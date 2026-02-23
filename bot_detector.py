"""
bot_detector.py — Multi-signal bot probability scoring and cluster detection
"""
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
from collections import defaultdict
import numpy as np
from loguru import logger
from parser import ParsedTrade


# Known bot-associated program IDs and MEV signatures
KNOWN_BOT_PROGRAMS = {
    "MEV1111111111111111111111111111111111111111",
    "jito111111111111111111111111111111111111111",
}

# Funding wallets known to deploy bot farms (add as you discover them)
KNOWN_BOT_FUNDERS: Set[str] = set()


@dataclass
class BotAnalysis:
    wallet_address: str
    bot_score: float          # 0.0 = definitely human, 1.0 = definitely bot
    is_bot: bool
    signals: Dict[str, float] # breakdown of which signals fired
    cluster_id: Optional[str] = None


def analyse_wallet_for_bot(
    wallet_address: str,
    trades: List[ParsedTrade],
    funder_address: Optional[str] = None,
) -> BotAnalysis:
    """
    Compute a bot probability score for a wallet based on its trade history.
    Returns BotAnalysis with score 0.0–1.0 and signal breakdown.
    """
    signals: Dict[str, float] = {}

    if not trades:
        return BotAnalysis(wallet_address, 0.5, False, {"no_data": 0.5})

    # ── 1. Sniper signal: avg blocks after mint ────────────────────────────
    snipe_blocks = [t.blocks_after_mint for t in trades if t.blocks_after_mint is not None]
    if snipe_blocks:
        avg_blocks = np.mean(snipe_blocks)
        if avg_blocks < 3:
            signals["ultra_sniper"] = 0.35
        elif avg_blocks < 15:
            signals["fast_sniper"] = 0.20
        elif avg_blocks < 50:
            signals["moderate_sniper"] = 0.08

    # ── 2. Hold time: very short average hold ─────────────────────────────
    hold_times = []
    token_buys: Dict[str, List[ParsedTrade]] = defaultdict(list)
    token_sells: Dict[str, List[ParsedTrade]] = defaultdict(list)
    for t in trades:
        if t.side == "buy":
            token_buys[t.token_address].append(t)
        else:
            token_sells[t.token_address].append(t)

    for token, sells in token_sells.items():
        buys = token_buys.get(token, [])
        for sell in sells:
            relevant_buys = [b for b in buys if b.block_time <= sell.block_time]
            if relevant_buys:
                earliest_buy = min(relevant_buys, key=lambda b: b.block_time)
                hold = (sell.block_time - earliest_buy.block_time).total_seconds()
                if hold >= 0:
                    hold_times.append(hold)

    if hold_times:
        avg_hold = np.mean(hold_times)
        if avg_hold < 30:
            signals["sub_30s_holds"] = 0.30
        elif avg_hold < 120:
            signals["sub_2min_holds"] = 0.18
        elif avg_hold < 300:
            signals["sub_5min_holds"] = 0.08

    # ── 3. Trade size variance (bots use consistent sizes) ────────────────
    amounts = [t.amount_sol for t in trades if t.amount_sol and t.amount_sol > 0]
    if len(amounts) >= 5:
        cv = np.std(amounts) / (np.mean(amounts) + 1e-9)
        if cv < 0.03:
            signals["identical_sizes"] = 0.25
        elif cv < 0.10:
            signals["very_uniform_sizes"] = 0.12

    # ── 4. Trade frequency ────────────────────────────────────────────────
    if len(trades) >= 10:
        time_range = (max(t.block_time for t in trades) - min(t.block_time for t in trades))
        days = max(time_range.days, 1)
        daily_rate = len(trades) / days
        if daily_rate > 200:
            signals["extreme_frequency"] = 0.20
        elif daily_rate > 80:
            signals["high_frequency"] = 0.12
        elif daily_rate > 40:
            signals["elevated_frequency"] = 0.05

    # ── 5. Win rate suspiciously perfect ─────────────────────────────────
    wins = sum(1 for t in trades if t.side == "sell")  # crude proxy
    total = len(trades)
    if total >= 100 and wins / total > 0.88:
        signals["perfect_win_rate"] = 0.12

    # ── 6. Jito usage across all trades ──────────────────────────────────
    jito_count = sum(1 for t in trades if t.used_jito)
    if len(trades) >= 10 and jito_count / len(trades) > 0.90:
        signals["always_jito"] = 0.10

    # ── 7. Token diversity: only buys new launches ────────────────────────
    unique_tokens = len({t.token_address for t in trades})
    buy_tokens = len({t.token_address for t in trades if t.side == "buy"})
    if buy_tokens > 0 and unique_tokens / buy_tokens < 1.05:
        signals["zero_diversity"] = 0.08

    # ── 8. Known funder ───────────────────────────────────────────────────
    if funder_address and funder_address in KNOWN_BOT_FUNDERS:
        signals["known_bot_funder"] = 0.45

    # ── 9. Activity gaps: bots don't sleep ───────────────────────────────
    if len(trades) >= 30:
        timestamps = sorted(t.block_time for t in trades)
        gaps = [(timestamps[i + 1] - timestamps[i]).total_seconds()
                for i in range(len(timestamps) - 1)]
        max_gap_hours = max(gaps) / 3600 if gaps else 0
        if max_gap_hours < 2:  # never more than 2 hour gap = no sleep
            signals["no_sleep_pattern"] = 0.10

    # ── Aggregate score ───────────────────────────────────────────────────
    total_score = min(sum(signals.values()), 1.0)
    from config import settings
    is_bot = total_score >= settings.bot_score_threshold

    return BotAnalysis(
        wallet_address=wallet_address,
        bot_score=round(total_score, 3),
        is_bot=is_bot,
        signals=signals,
    )


# ─── Cluster Detection ────────────────────────────────────────────────────────
def detect_bot_clusters(
    wallet_trade_map: Dict[str, List[ParsedTrade]],
    block_delta_threshold: int = 5,
    min_co_occurrences: int = 3,
) -> Dict[str, str]:
    """
    Identify clusters of wallets that consistently buy the same tokens
    within a few blocks of each other. Returns {wallet_address: cluster_id}.
    """
    # Build co-occurrence matrix: {(walletA, walletB): count}
    co_occur: Dict[tuple, int] = defaultdict(int)

    # Index trades by token
    token_buyers: Dict[str, List[tuple]] = defaultdict(list)  # token -> [(wallet, block_time)]
    for wallet, trades in wallet_trade_map.items():
        for trade in trades:
            if trade.side == "buy":
                token_buyers[trade.token_address].append((wallet, trade.block_time))

    # Find wallets that buy same token within threshold
    for token, buyers in token_buyers.items():
        buyers.sort(key=lambda x: x[1])  # sort by time
        for i, (w1, t1) in enumerate(buyers):
            for w2, t2 in buyers[i + 1:]:
                delta_secs = (t2 - t1).total_seconds()
                if delta_secs > block_delta_threshold * 0.4:  # ~0.4s per block
                    break
                if w1 != w2:
                    key = tuple(sorted([w1, w2]))
                    co_occur[key] += 1

    # Build adjacency list for connected components
    adjacency: Dict[str, Set[str]] = defaultdict(set)
    for (w1, w2), count in co_occur.items():
        if count >= min_co_occurrences:
            adjacency[w1].add(w2)
            adjacency[w2].add(w1)

    # BFS to find connected components
    visited: Set[str] = set()
    clusters: Dict[str, str] = {}
    cluster_id = 0

    for wallet in adjacency:
        if wallet not in visited:
            # BFS
            queue = [wallet]
            component = []
            while queue:
                node = queue.pop(0)
                if node in visited:
                    continue
                visited.add(node)
                component.append(node)
                queue.extend(adjacency[node] - visited)

            if len(component) >= 2:
                cid = f"cluster_{cluster_id:04d}"
                for w in component:
                    clusters[w] = cid
                cluster_id += 1

    return clusters


def add_known_bot_funder(address: str):
    """Dynamically add a discovered bot funder to the blocklist."""
    KNOWN_BOT_FUNDERS.add(address)
    logger.info(f"Added known bot funder: {address}")

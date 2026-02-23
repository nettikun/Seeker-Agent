"""
scorer.py — Compute and persist wallet scores; handle tier promotion/demotion
"""
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from collections import defaultdict
import numpy as np
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

from database import Wallet, Trade, WalletTier
from parser import ParsedTrade
from bot_detector import analyse_wallet_for_bot, BotAnalysis
from config import settings


@dataclass_score := __import__("dataclasses").dataclass


@dataclass_score
class WalletScore:
    wallet_address: str
    win_rate: float
    total_trades: int
    winning_trades: int
    total_pnl_usd: float
    avg_pnl_per_trade: float
    avg_hold_time_secs: float
    trade_size_cv: float
    bot_score: float
    recommended_tier: WalletTier


def compute_score_from_trades(
    wallet_address: str,
    trades: List[ParsedTrade],
    funder_address: Optional[str] = None,
) -> Tuple[WalletScore, BotAnalysis]:
    """
    Given a list of ParsedTrades, compute a full WalletScore and BotAnalysis.
    PnL is computed by matching sells against prior buys (FIFO per token).
    """
    bot_analysis = analyse_wallet_for_bot(wallet_address, trades, funder_address)

    # Organise by token
    token_buys: Dict[str, List[ParsedTrade]] = defaultdict(list)
    token_sells: Dict[str, List[ParsedTrade]] = defaultdict(list)
    for t in sorted(trades, key=lambda x: x.block_time):
        if t.side == "buy":
            token_buys[t.token_address].append(t)
        else:
            token_sells[t.token_address].append(t)

    # Compute per-trade PnL by token
    trade_pnls: List[float] = []
    hold_times: List[float] = []

    for token, sells in token_sells.items():
        buys = list(token_buys.get(token, []))  # FIFO queue
        buy_cost_queue: List[float] = [b.amount_usd for b in buys]
        buy_time_queue: List[datetime] = [b.block_time for b in buys]

        for sell in sells:
            # Find earliest buy before this sell
            prior_buys = [(cost, t) for cost, t in zip(buy_cost_queue, buy_time_queue)
                          if t <= sell.block_time]
            if prior_buys:
                cost, buy_time = prior_buys[0]
                pnl = sell.amount_usd - cost
                trade_pnls.append(pnl)
                hold = (sell.block_time - buy_time).total_seconds()
                if hold >= 0:
                    hold_times.append(hold)
                # Remove matched buy from queue
                buy_cost_queue = buy_cost_queue[1:]
                buy_time_queue = buy_time_queue[1:]

    total_trades = len(trade_pnls)
    winning_trades = sum(1 for p in trade_pnls if p > 0)
    win_rate = winning_trades / total_trades if total_trades > 0 else 0.0
    total_pnl = sum(trade_pnls)
    avg_pnl = total_pnl / total_trades if total_trades > 0 else 0.0
    avg_hold = float(np.mean(hold_times)) if hold_times else 0.0

    amounts = [t.amount_sol for t in trades if t.amount_sol and t.amount_sol > 0]
    trade_size_cv = float(np.std(amounts) / (np.mean(amounts) + 1e-9)) if len(amounts) >= 3 else 0.0

    # Tier recommendation
    if bot_analysis.is_bot:
        tier = WalletTier.EXILED
    elif total_trades < settings.min_trades or win_rate < settings.min_win_rate:
        tier = WalletTier.CANDIDATE
    elif win_rate >= 0.45 and total_pnl > 500 and total_trades >= 80:
        tier = WalletTier.TIER1
    else:
        tier = WalletTier.TIER2

    score = WalletScore(
        wallet_address=wallet_address,
        win_rate=round(win_rate, 4),
        total_trades=total_trades,
        winning_trades=winning_trades,
        total_pnl_usd=round(total_pnl, 2),
        avg_pnl_per_trade=round(avg_pnl, 2),
        avg_hold_time_secs=round(avg_hold, 1),
        trade_size_cv=round(trade_size_cv, 4),
        bot_score=bot_analysis.bot_score,
        recommended_tier=tier,
    )
    return score, bot_analysis


async def persist_score(db: AsyncSession, score: WalletScore):
    """Write computed score back to the Wallet row in DB."""
    await db.execute(
        update(Wallet)
        .where(Wallet.address == score.wallet_address)
        .values(
            win_rate=score.win_rate,
            total_trades=score.total_trades,
            winning_trades=score.winning_trades,
            total_pnl_usd=score.total_pnl_usd,
            avg_pnl_per_trade=score.avg_pnl_per_trade,
            avg_hold_time_secs=score.avg_hold_time_secs,
            trade_size_cv=score.trade_size_cv,
            bot_score=score.bot_score,
            tier=score.recommended_tier,
            last_scored=datetime.utcnow(),
        )
    )
    await db.commit()
    logger.debug(
        f"[SCORE] {score.wallet_address[:8]}… | WR={score.win_rate:.1%} "
        f"PnL=${score.total_pnl_usd:,.0f} | tier={score.recommended_tier} | bot={score.bot_score:.2f}"
    )


async def persist_trades(db: AsyncSession, wallet_address: str, trades: List[ParsedTrade]):
    """Upsert parsed trades into the trades table."""
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from database import Trade as TradeModel

    for t in trades:
        stmt = pg_insert(TradeModel).values(
            wallet_address=wallet_address,
            signature=t.signature,
            token_address=t.token_address,
            token_symbol=t.token_symbol,
            side=t.side,
            amount_sol=t.amount_sol,
            amount_usd=t.amount_usd,
            price_usd=t.price_usd,
            block_time=t.block_time,
            used_jito=t.used_jito,
            blocks_after_mint=t.blocks_after_mint,
        ).on_conflict_do_nothing(index_elements=["signature"])
        await db.execute(stmt)
    await db.commit()


async def get_wallets_due_for_rescore(db: AsyncSession, limit: int = 100) -> List[Wallet]:
    """Return wallets that haven't been scored recently."""
    cutoff = datetime.utcnow() - timedelta(hours=1)
    result = await db.execute(
        select(Wallet)
        .where(
            (Wallet.tier != WalletTier.EXILED) &
            (Wallet.tier != WalletTier.ARCHIVED) &
            ((Wallet.last_scored == None) | (Wallet.last_scored < cutoff))
        )
        .order_by(Wallet.last_scored.asc().nullsfirst())
        .limit(limit)
    )
    return result.scalars().all()

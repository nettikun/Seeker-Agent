"""
discovery.py — Autonomous wallet discovery via graph crawling and DEX leaderboards
"""
import asyncio
from datetime import datetime
from typing import List, Set, Dict
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from database import Wallet, WalletTier, WalletEdge
from helius import HeliusClient
from parser import parse_transactions_batch
from config import settings
import httpx


# ─── Seed wallets to bootstrap the graph ─────────────────────────────────────
SEED_WALLETS: List[str] = settings.seed_wallets or [
    # Add a few known good trader addresses here to kickstart discovery
    # e.g. "7xKX...your_seed_wallet_1",
]


async def ensure_wallet_exists(
    db: AsyncSession, address: str, source: str = "discovery"
) -> bool:
    """Insert wallet into DB if it doesn't exist. Returns True if newly added."""
    stmt = pg_insert(Wallet).values(
        address=address,
        tier=WalletTier.CANDIDATE,
        discovery_source=source,
        first_seen=datetime.utcnow(),
        last_active=datetime.utcnow(),
    ).on_conflict_do_nothing(index_elements=["address"])
    result = await db.execute(stmt)
    await db.commit()
    return result.rowcount > 0


async def record_edge(
    db: AsyncSession,
    source: str,
    target: str,
    shared_token: str,
    block_delta: int,
):
    """Record or increment a co-buyer edge in the graph."""
    stmt = (
        pg_insert(WalletEdge)
        .values(
            source_address=source,
            target_address=target,
            shared_token=shared_token,
            block_delta=block_delta,
            co_occurrences=1,
            first_seen=datetime.utcnow(),
            last_seen=datetime.utcnow(),
        )
        .on_conflict_do_update(
            index_elements=["source_address", "target_address"],
            set_={
                "co_occurrences": WalletEdge.co_occurrences + 1,
                "last_seen": datetime.utcnow(),
            },
        )
    )
    await db.execute(stmt)
    await db.commit()


async def discover_from_wallet(
    helius: HeliusClient,
    db: AsyncSession,
    wallet_address: str,
    already_seen: Set[str],
) -> List[str]:
    """
    Given a known wallet, find co-buyers for the tokens it traded.
    Returns list of newly discovered wallet addresses.
    """
    discovered: List[str] = []

    txns = await helius.get_parsed_transactions(wallet_address, limit=50)
    trades = parse_transactions_batch(txns, wallet_address)

    # Collect unique tokens this wallet bought
    bought_tokens = {t.token_address for t in trades if t.side == "buy" and t.token_address}

    for token in list(bought_tokens)[:10]:  # cap per wallet to avoid explosion
        if not token:
            continue
        try:
            # Get other wallets that hold/held this token
            co_buyers = await helius.get_token_accounts_by_owner(token, limit=30)
            for co_buyer in co_buyers:
                if co_buyer in already_seen or co_buyer == wallet_address:
                    continue
                already_seen.add(co_buyer)
                is_new = await ensure_wallet_exists(db, co_buyer, source=wallet_address)
                if is_new:
                    discovered.append(co_buyer)
                    logger.debug(f"[DISCOVERY] New wallet {co_buyer[:8]}… via {wallet_address[:8]}… on token {token[:8]}…")
                await record_edge(db, wallet_address, co_buyer, token, block_delta=0)
        except Exception as e:
            logger.warning(f"discover_from_wallet token {token[:8]}: {e}")
        await asyncio.sleep(0.1)

    return discovered


async def run_discovery_cycle(helius: HeliusClient, db: AsyncSession):
    """
    One full discovery pass:
    1. Pull current tier1/tier2 wallets
    2. Fan out and find co-buyers
    3. Add new candidates to DB
    """
    logger.info("[DISCOVERY] Starting discovery cycle…")

    # Get existing tracked wallets as seeds for this cycle
    result = await db.execute(
        select(Wallet.address)
        .where(Wallet.tier.in_([WalletTier.TIER1, WalletTier.TIER2]))
        .order_by(Wallet.last_active.desc())
        .limit(50)
    )
    active_wallets = [row[0] for row in result.fetchall()]

    # Also add static seeds if pool is small
    all_seeds = list(set(active_wallets + SEED_WALLETS))
    if not all_seeds:
        logger.warning("[DISCOVERY] No seed wallets configured. Add seeds to .env or DB.")
        return

    already_seen: Set[str] = set(all_seeds)
    total_discovered = 0

    for wallet in all_seeds[:20]:  # limit per cycle to control rate
        try:
            new_wallets = await discover_from_wallet(helius, db, wallet, already_seen)
            total_discovered += len(new_wallets)
        except Exception as e:
            logger.error(f"[DISCOVERY] Error on {wallet[:8]}: {e}")
        await asyncio.sleep(0.5)

    logger.info(f"[DISCOVERY] Cycle complete. {total_discovered} new candidates added.")


async def fetch_birdeye_top_traders(db: AsyncSession, api_key: str) -> int:
    """
    Pull top traders from Birdeye's leaderboard API and add them as candidates.
    Returns number of new wallets added.
    """
    if not api_key:
        return 0
    added = 0
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(
                "https://public-api.birdeye.so/v1/trader/gainers-losers",
                params={"type": "1W", "sort_by": "PnL", "sort_type": "desc", "limit": 50},
                headers={"X-API-KEY": api_key},
            )
            if r.status_code == 200:
                data = r.json().get("data", {}).get("items", [])
                for item in data:
                    addr = item.get("address")
                    if addr:
                        is_new = await ensure_wallet_exists(db, addr, source="birdeye_leaderboard")
                        if is_new:
                            added += 1
    except Exception as e:
        logger.warning(f"[DISCOVERY] Birdeye leaderboard fetch failed: {e}")
    return added

"""
discovery.py — Autonomous wallet discovery.
"""
import asyncio
from datetime import datetime
from typing import List, Set, Dict
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
import httpx

from database import Wallet, WalletTier, WalletEdge
from helius import HeliusClient
from parser import parse_transactions_batch
from config import settings, HELIUS_API


async def ensure_wallet_exists(db, address, source="discovery"):
    if not address or len(address) < 32 or len(address) > 44:
        return False
    try:
        await db.rollback()
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
    except Exception as e:
        await db.rollback()
        return False


async def record_edge(db, source, target, shared_token, block_delta=0):
    try:
        await db.rollback()
        stmt = pg_insert(WalletEdge).values(
            source_address=source,
            target_address=target,
            shared_token=shared_token,
            block_delta=block_delta,
            co_occurrences=1,
            first_seen=datetime.utcnow(),
            last_seen=datetime.utcnow(),
        ).on_conflict_do_nothing()
        await db.execute(stmt)
        await db.commit()
    except Exception:
        await db.rollback()

async def get_wallets_from_enhanced_txns(helius, token_mint, limit=30):
    wallets = []
    try:
        txns = await helius._get(
            f"/addresses/{token_mint}/transactions",
            {"limit": limit, "type": "SWAP"}
        )
        if not txns:
            return []
        for txn in txns:
            fee_payer = txn.get("feePayer")
            if fee_payer and len(fee_payer) >= 32:
                wallets.append(fee_payer)
            for acc in txn.get("accountData", []):
                addr = acc.get("account", "")
                if addr and 32 <= len(addr) <= 44:
                    wallets.append(addr)
    except Exception as e:
        logger.debug(f"get_wallets_from_enhanced_txns failed for {token_mint[:8]}: {e}")
    return list(set(wallets))


async def get_token_recent_buyers(helius, token_mint, limit=20):
    wallets = []
    try:
        result = await helius._rpc("getSignaturesForAddress", [
            token_mint,
            {"limit": limit, "commitment": "confirmed"}
        ])
        if not result:
            return []
        for sig_info in result:
            sig = sig_info.get("signature")
            if not sig:
                continue
            try:
                tx = await helius._rpc("getTransaction", [
                    sig,
                    {"encoding": "json", "maxSupportedTransactionVersion": 0}
                ])
                if tx and tx.get("transaction"):
                    account_keys = tx["transaction"].get("message", {}).get("accountKeys", [])
                    if account_keys:
                        signer = account_keys[0] if isinstance(account_keys[0], str) else account_keys[0].get("pubkey")
                        if signer and len(signer) >= 32:
                            wallets.append(signer)
            except Exception:
                pass
            await asyncio.sleep(0.05)
    except Exception as e:
        logger.debug(f"get_token_recent_buyers failed for {token_mint[:8]}: {e}")
    return list(set(wallets))


async def discover_from_wallet(helius, db, wallet_address, already_seen):
    discovered = []
    try:
        txns = await helius.get_parsed_transactions(wallet_address, limit=100)
        logger.info(f"[DISCOVERY] {wallet_address[:8]} — fetched {len(txns)} raw txns")
        if not txns:
            return []

        trades = parse_transactions_batch(txns, wallet_address)
        logger.info(f"[DISCOVERY] {wallet_address[:8]} — parsed {len(trades)} trades")
        if not trades:
            # Fallback: extract feePayer directly from raw transactions
            token_mints = set()
            for txn in txns:
                for transfer in txn.get("tokenTransfers", []):
                    mint = transfer.get("mint", "")
                    if mint and len(mint) >= 32:
                        token_mints.add(mint)
            logger.info(f"[DISCOVERY] {wallet_address[:8]} — fallback found {len(token_mints)} token mints")
        else:
            token_mints = {
                t.token_address for t in trades
                if t.token_address and len(t.token_address) >= 32
            }
            logger.info(f"[DISCOVERY] {wallet_address[:8]} — found {len(token_mints)} unique tokens")

        bought_tokens = list(token_mints)

        for token in bought_tokens[:8]:
            try:
                co_traders = await get_wallets_from_enhanced_txns(helius, token, limit=25)
                logger.info(f"[DISCOVERY] token {token[:8]} — found {len(co_traders)} co-traders")
                if not co_traders:
                    co_traders = await get_token_recent_buyers(helius, token, limit=20)
                new_count = 0
                for co_wallet in co_traders:
                    if co_wallet in already_seen or co_wallet == wallet_address:
                        continue
                    if len(co_wallet) < 32 or len(co_wallet) > 44:
                        continue
                    already_seen.add(co_wallet)
                    is_new = await ensure_wallet_exists(db, co_wallet, source=wallet_address)
                    if is_new:
                        discovered.append(co_wallet)
                        new_count += 1
                    await record_edge(db, wallet_address, co_wallet, token)
                if new_count > 0:
                    logger.info(f"[DISCOVERY] +{new_count} wallets from token {token[:8]}")
            except Exception as e:
                logger.warning(f"[DISCOVERY] token {token[:8]} error: {e}")
            await asyncio.sleep(0.2)

    except Exception as e:
        logger.warning(f"[DISCOVERY] discover_from_wallet {wallet_address[:8]}: {e}")
    return discovered


async def run_discovery_cycle(helius, db):
    logger.info("[DISCOVERY] Starting discovery cycle...")
    result = await db.execute(
        select(Wallet.address)
        .where(Wallet.tier.in_([WalletTier.TIER1, WalletTier.TIER2, WalletTier.CANDIDATE]))
        .order_by(Wallet.last_active.desc())
        .limit(30)
    )
    active_wallets = [row[0] for row in result.fetchall()]
    if not active_wallets:
        logger.warning("[DISCOVERY] No seed wallets configured. Add seeds to .env or DB.")
        return
    already_seen = set(active_wallets)
    total_discovered = 0
    for wallet in active_wallets[:15]:
        try:
            new_wallets = await discover_from_wallet(helius, db, wallet, already_seen)
            total_discovered += len(new_wallets)
            if new_wallets:
                logger.info(f"[DISCOVERY] {wallet[:8]} -> +{len(new_wallets)} new candidates")
        except Exception as e:
            logger.error(f"[DISCOVERY] Error on {wallet[:8]}: {e}")
        await asyncio.sleep(0.5)
    logger.info(f"[DISCOVERY] Cycle complete. {total_discovered} new candidates added.")


async def fetch_birdeye_top_traders(db, api_key):
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
        logger.warning(f"[DISCOVERY] Birdeye fetch failed: {e}")
    return added

"""
orchestrator.py — The autonomous brain.

Runs four perpetual async loops:
  1. Discovery Loop  — finds new wallet candidates continuously
  2. Scoring Loop    — scores/re-scores all wallets, promotes/demotes tiers
  3. Webhook Loop    — manages Helius webhook registrations for Tier 1
  4. Health Loop     — posts heartbeat stats, prunes inactive wallets

Plus a webhook event handler that fires on every live trade signal.
"""
import asyncio
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update

from config import settings
from database import (
    AsyncSessionLocal, Wallet, WalletTier, AgentHealth, create_all_tables
)
from helius import HeliusClient
from parser import parse_transactions_batch
from scorer import compute_score_from_trades, persist_score, persist_trades, get_wallets_due_for_rescore
from discovery import run_discovery_cycle, ensure_wallet_exists, fetch_birdeye_top_traders
from alerts import (
    TradeAlert, send_alert, send_heartbeat,
    send_tier_change_alert, send_bot_exile_alert,
)
from bot_detector import detect_bot_clusters


# ─── Runtime counters ─────────────────────────────────────────────────────────
class Counters:
    def __init__(self):
        self.trades_processed = 0
        self.alerts_sent = 0
        self.errors = 0
        self._hourly_trades: deque = deque()
        self._hourly_alerts: deque = deque()

    def record_trade(self):
        now = datetime.utcnow()
        self.trades_processed += 1
        self._hourly_trades.append(now)
        self._trim(self._hourly_trades)

    def record_alert(self):
        now = datetime.utcnow()
        self.alerts_sent += 1
        self._hourly_alerts.append(now)
        self._trim(self._hourly_alerts)

    def _trim(self, q: deque):
        cutoff = datetime.utcnow() - timedelta(hours=1)
        while q and q[0] < cutoff:
            q.popleft()

    @property
    def trades_last_hour(self) -> int:
        return len(self._hourly_trades)

    @property
    def alerts_last_hour(self) -> int:
        return len(self._hourly_alerts)


counters = Counters()

# Store a reference to the webhook_id we registered with Helius
_webhook_id: Optional[str] = None
# Track which addresses are currently registered on the webhook
_webhook_addresses: Set[str] = set()


# ─── Live Trade Handler (called by webhook server) ────────────────────────────
async def handle_live_transaction(txn: dict):
    """
    Called for every incoming Helius webhook event.
    Parses the trade, checks the wallet score, and fires an alert.
    """
    try:
        async with AsyncSessionLocal() as db:
            # Determine which of our tracked wallets fired this event
            account_data = txn.get("accountData", [])
            wallet_address = None
            for acc in account_data:
                if acc.get("nativeBalanceChange") != 0:
                    wallet_address = acc.get("account")
                    break
            if not wallet_address:
                return

            # Fetch wallet record
            result = await db.execute(
                select(Wallet).where(Wallet.address == wallet_address)
            )
            wallet = result.scalar_one_or_none()
            if not wallet or wallet.tier == WalletTier.EXILED:
                return

            trade = parse_enhanced_transaction(txn, wallet_address)
            if not trade:
                return

            counters.record_trade()

            # Update last_active
            await db.execute(
                update(Wallet)
                .where(Wallet.address == wallet_address)
                .values(last_active=datetime.utcnow())
            )
            await db.commit()

            # Only alert on buys for Tier 1 wallets; sells if we have PnL data
            is_copy_eligible = (
                wallet.tier == WalletTier.TIER1
                and trade.side == "buy"
                and wallet.win_rate >= settings.min_win_rate
                and not wallet.bot_score >= settings.bot_score_threshold
            )

            alert = TradeAlert(
                wallet=wallet,
                trade=trade,
                is_copy_eligible=is_copy_eligible,
            )
            await send_alert(alert)
            counters.record_alert()
            logger.info(
                f"[ALERT] {wallet_address[:8]}… {trade.side.upper()} {trade.token_symbol} "
                f"${trade.amount_usd:,.0f} | WR={wallet.win_rate:.1%}"
            )
    except Exception as e:
        counters.errors += 1
        logger.error(f"handle_live_transaction error: {e}")


# ─── Loop 1: Discovery ───────────────────────────────────────────────────────
async def discovery_loop(helius: HeliusClient):
    logger.info("[ORCHESTRATOR] Discovery loop started.")
    while True:
        try:
            async with AsyncSessionLocal() as db:
                await run_discovery_cycle(helius, db)
                if settings.birdeye_api_key:
                    added = await fetch_birdeye_top_traders(db, settings.birdeye_api_key)
                    if added:
                        logger.info(f"[DISCOVERY] +{added} from Birdeye leaderboard")
        except Exception as e:
            counters.errors += 1
            logger.error(f"[DISCOVERY] Loop error: {e}")
        await asyncio.sleep(settings.discovery_interval_secs)


# ─── Loop 2: Scoring ─────────────────────────────────────────────────────────
async def scoring_loop(helius: HeliusClient):
    logger.info("[ORCHESTRATOR] Scoring loop started.")
    while True:
        try:
            async with AsyncSessionLocal() as db:
                wallets = await get_wallets_due_for_rescore(db, limit=50)
                if wallets:
                    logger.info(f"[SCORING] Processing {len(wallets)} wallets…")

                for wallet in wallets:
                    try:
                        txns = await helius.get_all_transactions(wallet.address, max_txns=500)
                        trades = parse_transactions_batch(txns, wallet.address)

                      if not trades:
                            await db.execute(
                                update(Wallet)
                                .where(Wallet.address == wallet.address)
                                .values(last_scored=datetime.utcnow())
                            )
                            await db.commit()
                            continue

                        score, bot_analysis = compute_score_from_trades(wallet.address, trades)

                        # Log tier change + alert
                        if score.recommended_tier != wallet.tier:
                            logger.info(
                                f"[TIER CHANGE] {wallet.address[:8]}… "
                                f"{wallet.tier} → {score.recommended_tier} "
                                f"(WR={score.win_rate:.1%}, bot={bot_analysis.bot_score:.2f})"
                            )
                            # Fire Telegram alert for notable promotions
                            await send_tier_change_alert(wallet, wallet.tier, score.recommended_tier)
                            # Extra exile alert for newly detected bots
                            if score.recommended_tier == WalletTier.EXILED:
                                await send_bot_exile_alert(
                                    wallet.address,
                                    bot_analysis.bot_score,
                                    bot_analysis.signals,
                                )

                        await persist_score(db, score)
                    except Exception as e:
                        counters.errors += 1
                        logger.error(f"[SCORING] Error on {wallet.address[:8]}: {e}")
                    await asyncio.sleep(0.3)

            # Run cluster bot detection periodically
            await run_cluster_detection()

        except Exception as e:
            counters.errors += 1
            logger.error(f"[SCORING] Loop error: {e}")
        await asyncio.sleep(60)  # re-check every minute, scoring filters by last_scored


async def run_cluster_detection():
    """Pull recent trades and look for bot clusters across all scored wallets."""
    try:
        async with AsyncSessionLocal() as db:
            from database import Trade as TradeModel
            from parser import ParsedTrade as PT

            # Get recent trades across all wallets for clustering analysis
            result = await db.execute(
                select(TradeModel)
                .where(TradeModel.block_time >= datetime.utcnow() - timedelta(hours=24))
                .limit(10000)
            )
            db_trades = result.scalars().all()

            wallet_trade_map: Dict[str, List] = defaultdict(list)
            for t in db_trades:
                wallet_trade_map[t.wallet_address].append(t)

            # Convert to ParsedTrade-like objects for clustering
            parsed_map = {}
            for addr, db_trade_list in wallet_trade_map.items():
                parsed_list = []
                for t in db_trade_list:
                    pt = PT(
                        signature=t.signature,
                        wallet_address=t.wallet_address,
                        token_address=t.token_address or "",
                        token_symbol=t.token_symbol or "",
                        side=t.side,
                        amount_sol=t.amount_sol or 0,
                        amount_usd=t.amount_usd or 0,
                        price_usd=t.price_usd or 0,
                        block_time=t.block_time,
                        used_jito=t.used_jito or False,
                    )
                    parsed_list.append(pt)
                parsed_map[addr] = parsed_list

            clusters = detect_bot_clusters(parsed_map)
            if clusters:
                logger.info(f"[CLUSTER] Detected {len(set(clusters.values()))} bot clusters, {len(clusters)} wallets")
                # Exile cluster members
                for addr in clusters:
                    await db.execute(
                        update(Wallet)
                        .where(Wallet.address == addr)
                        .values(tier=WalletTier.EXILED, notes="bot cluster detected")
                    )
                await db.commit()
    except Exception as e:
        logger.error(f"[CLUSTER] Detection error: {e}")


# ─── Loop 3: Webhook Management ──────────────────────────────────────────────
async def webhook_management_loop(helius: HeliusClient, public_webhook_url: str):
    """
    Keeps the Helius webhook up-to-date with the current Tier 1 wallet list.
    Runs every 5 minutes to sync additions/removals.
    """
    global _webhook_id, _webhook_addresses
    logger.info("[ORCHESTRATOR] Webhook management loop started.")

    while True:
        try:
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    select(Wallet.address)
                    .where(Wallet.tier == WalletTier.TIER1)
                    .limit(settings.tier1_max_wallets)
                )
                tier1_addresses = {row[0] for row in result.fetchall()}

            if tier1_addresses == _webhook_addresses:
                await asyncio.sleep(300)
                continue

            # Sync webhook
            if not _webhook_id:
                # Create new webhook
                resp = await helius.create_webhook(public_webhook_url, list(tier1_addresses))
                if resp:
                    _webhook_id = resp.get("webhookID")
                    _webhook_addresses = tier1_addresses
                    logger.info(f"[WEBHOOK] Created webhook {_webhook_id} for {len(tier1_addresses)} wallets")
            else:
                success = await helius.edit_webhook(_webhook_id, list(tier1_addresses), public_webhook_url)
                if success:
                    _webhook_addresses = tier1_addresses
                    logger.info(f"[WEBHOOK] Updated webhook — now tracking {len(tier1_addresses)} Tier 1 wallets")

        except Exception as e:
            counters.errors += 1
            logger.error(f"[WEBHOOK] Management error: {e}")

        await asyncio.sleep(300)


# ─── Loop 4: Health + Pruning ─────────────────────────────────────────────────
async def health_loop():
    logger.info("[ORCHESTRATOR] Health loop started.")
    while True:
        try:
            async with AsyncSessionLocal() as db:
                # Count by tier
                counts = {}
                for tier in WalletTier:
                    result = await db.execute(
                        select(func.count()).where(Wallet.tier == tier)
                    )
                    counts[tier] = result.scalar() or 0

                # Prune inactive wallets
                prune_cutoff = datetime.utcnow() - timedelta(days=settings.prune_inactive_days)
                await db.execute(
                    update(Wallet)
                    .where(
                        (Wallet.last_active < prune_cutoff) &
                        (Wallet.tier.in_([WalletTier.TIER2, WalletTier.CANDIDATE]))
                    )
                    .values(tier=WalletTier.ARCHIVED)
                )
                await db.commit()

                # Log heartbeat
                db.add(AgentHealth(
                    wallets_tracked=sum(counts.values()),
                    tier1_count=counts.get(WalletTier.TIER1, 0),
                    tier2_count=counts.get(WalletTier.TIER2, 0),
                    candidates_count=counts.get(WalletTier.CANDIDATE, 0),
                    trades_processed_last_hour=counters.trades_last_hour,
                    alerts_sent_last_hour=counters.alerts_last_hour,
                    errors_last_hour=counters.errors,
                ))
                await db.commit()

                logger.info(
                    f"[HEALTH] T1={counts.get(WalletTier.TIER1, 0)} | "
                    f"T2={counts.get(WalletTier.TIER2, 0)} | "
                    f"Candidates={counts.get(WalletTier.CANDIDATE, 0)} | "
                    f"Exiled={counts.get(WalletTier.EXILED, 0)} | "
                    f"Trades/hr={counters.trades_last_hour} | "
                    f"Alerts/hr={counters.alerts_last_hour}"
                )

                await send_heartbeat(
                    tier1=counts.get(WalletTier.TIER1, 0),
                    tier2=counts.get(WalletTier.TIER2, 0),
                    candidates=counts.get(WalletTier.CANDIDATE, 0),
                    exiled=counts.get(WalletTier.EXILED, 0),
                    trades_last_hour=counters.trades_last_hour,
                    alerts_last_hour=counters.alerts_last_hour,
                    errors_last_hour=counters.errors,
                )

        except Exception as e:
            counters.errors += 1
            logger.error(f"[HEALTH] Loop error: {e}")

        await asyncio.sleep(3600)  # hourly


# ─── Main Entry Point ─────────────────────────────────────────────────────────
async def run_agent(public_webhook_url: str = "https://your-server.com/webhook/helius"):
    """
    Bootstrap and run all agent loops + the webhook HTTP server concurrently.
    `public_webhook_url` must be a publicly reachable HTTPS URL pointing to
    your webhook_server.py /webhook/helius endpoint.
    """
    logger.info("=" * 60)
    logger.info("  SOLANA WALLET AGENT — Starting up")
    logger.info("=" * 60)

    # Init DB
    await create_all_tables()
    logger.info("[INIT] Database tables verified.")

   # Seed initial wallets from config
    from config import settings as _settings
    SEED_WALLETS = _settings.seed_wallets
    async with AsyncSessionLocal() as db:
        for addr in SEED_WALLETS:
            await ensure_wallet_exists(db, addr, source="config_seed")
    if SEED_WALLETS:
        logger.info(f"[INIT] Seeded {len(SEED_WALLETS)} initial wallets.")
      
    # Register live trade handler with webhook server
    from webhook_server import register_handler
    register_handler(handle_live_transaction)

    # Start FastAPI webhook server in background
    import uvicorn
    from webhook_server import app as webhook_app

    server_config = uvicorn.Config(
        webhook_app,
        host="0.0.0.0",
        port=settings.webhook_server_port,
        log_level="warning",
    )
    server = uvicorn.Server(server_config)

    async with HeliusClient() as helius:
        await asyncio.gather(
            server.serve(),
            discovery_loop(helius),
            scoring_loop(helius),
            webhook_management_loop(helius, public_webhook_url),
            health_loop(),
        )

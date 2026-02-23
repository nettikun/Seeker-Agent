"""
alerts.py — Telegram-only alert delivery for live trade signals.

Messages use Telegram HTML formatting with emoji-rich layouts.
Supports:
  - Trade alerts (buy / sell)
  - Bot exile notifications
  - Tier promotion notifications
  - Hourly heartbeat reports
"""
import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import httpx
from loguru import logger
from config import settings
from parser import ParsedTrade
from database import Wallet, WalletTier


# ─── Telegram API helper ──────────────────────────────────────────────────────
TG_API = f"https://api.telegram.org/bot{{token}}/{{method}}"


async def _tg_post(method: str, payload: dict, retries: int = 3) -> bool:
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        return False
    url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/{method}"
    for attempt in range(retries):
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.post(url, json=payload)
                if r.status_code == 200:
                    return True
                elif r.status_code == 429:
                    # Rate limited — respect retry_after
                    retry_after = r.json().get("parameters", {}).get("retry_after", 5)
                    logger.warning(f"[TG] Rate limited. Waiting {retry_after}s…")
                    await asyncio.sleep(retry_after)
                else:
                    logger.warning(f"[TG] {method} failed {r.status_code}: {r.text[:200]}")
                    return False
        except Exception as e:
            logger.error(f"[TG] {method} error (attempt {attempt+1}): {e}")
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
    return False


async def _send_message(text: str, disable_preview: bool = True) -> bool:
    return await _tg_post("sendMessage", {
        "chat_id": settings.telegram_chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": disable_preview,
    })


# ─── Message formatters ───────────────────────────────────────────────────────
def _tier_badge(tier: WalletTier) -> str:
    return {
        WalletTier.TIER1: "🥇 T1",
        WalletTier.TIER2: "🥈 T2",
        WalletTier.CANDIDATE: "🔍 CAND",
        WalletTier.EXILED: "💀 EXILED",
        WalletTier.ARCHIVED: "📦 ARCH",
    }.get(tier, tier.value)


def _pnl_emoji(pnl: float) -> str:
    if pnl > 500:  return "💰💰💰"
    if pnl > 100:  return "💰💰"
    if pnl > 0:    return "💰"
    if pnl > -100: return "🩸"
    return "🩸🩸"


def format_buy_alert(wallet: Wallet, trade: ParsedTrade, is_copy_eligible: bool) -> str:
    copy_banner = "\n⚡ <b>COPY ELIGIBLE — ACT FAST</b> ⚡\n" if is_copy_eligible else ""
    short_addr = f"{wallet.address[:6]}…{wallet.address[-4:]}"
    short_token = f"{trade.token_address[:6]}…{trade.token_address[-4:]}"

    return (
        f"🟢 <b>BUY DETECTED</b>{copy_banner}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👛 <a href='https://solscan.io/account/{wallet.address}'>{short_addr}</a>  {_tier_badge(wallet.tier)}\n"
        f"📊 WR: <b>{wallet.win_rate:.1%}</b>  |  PnL: <b>${wallet.total_pnl_usd:,.0f}</b>  |  Trades: {wallet.total_trades}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 Token: <code>{trade.token_symbol or short_token}</code>\n"
        f"   <code>{trade.token_address}</code>\n"
        f"💵 Amount: <b>{trade.amount_sol:.3f} SOL</b>  (~${trade.amount_usd:,.0f})\n"
        f"💲 Price:  <code>${trade.price_usd:.10f}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔗 <a href='https://solscan.io/tx/{trade.signature}'>View Tx</a>  |  "
        f"<a href='https://dexscreener.com/solana/{trade.token_address}'>Chart</a>  |  "
        f"<a href='https://birdeye.so/token/{trade.token_address}?chain=solana'>Birdeye</a>\n"
        f"⏰ {datetime.utcnow().strftime('%H:%M:%S UTC')}"
    )


def format_sell_alert(wallet: Wallet, trade: ParsedTrade, pnl_usd: Optional[float]) -> str:
    short_addr = f"{wallet.address[:6]}…{wallet.address[-4:]}"
    pnl_str = (
        f"💰 PnL:    <b>${pnl_usd:+,.2f}</b>  {_pnl_emoji(pnl_usd)}\n" if pnl_usd is not None else ""
    )

    return (
        f"🔴 <b>SELL DETECTED</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👛 <a href='https://solscan.io/account/{wallet.address}'>{short_addr}</a>  {_tier_badge(wallet.tier)}\n"
        f"📊 WR: <b>{wallet.win_rate:.1%}</b>  |  PnL: <b>${wallet.total_pnl_usd:,.0f}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 Token: <code>{trade.token_symbol or trade.token_address[:16]}…</code>\n"
        f"💵 Sold:  <b>{trade.amount_sol:.3f} SOL</b>  (~${trade.amount_usd:,.0f})\n"
        f"{pnl_str}"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔗 <a href='https://solscan.io/tx/{trade.signature}'>View Tx</a>  |  "
        f"<a href='https://dexscreener.com/solana/{trade.token_address}'>Chart</a>\n"
        f"⏰ {datetime.utcnow().strftime('%H:%M:%S UTC')}"
    )


def format_tier_promotion(wallet: Wallet, old_tier: WalletTier, new_tier: WalletTier) -> str:
    short_addr = f"{wallet.address[:6]}…{wallet.address[-4:]}"
    direction = "📈 PROMOTED" if (
        [WalletTier.CANDIDATE, WalletTier.TIER2, WalletTier.TIER1].index(new_tier) >
        [WalletTier.CANDIDATE, WalletTier.TIER2, WalletTier.TIER1].index(old_tier)
    ) else "📉 DEMOTED"

    return (
        f"🔔 <b>TIER CHANGE — {direction}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👛 <a href='https://solscan.io/account/{wallet.address}'>{short_addr}</a>\n"
        f"📊 WR: <b>{wallet.win_rate:.1%}</b>  |  PnL: <b>${wallet.total_pnl_usd:,.0f}</b>  |  Trades: {wallet.total_trades}\n"
        f"🏷️ {_tier_badge(old_tier)}  →  {_tier_badge(new_tier)}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{'✅ Now receiving REAL-TIME alerts' if new_tier == WalletTier.TIER1 else ''}"
        f"{'⏬ Moved to daily scoring' if new_tier == WalletTier.TIER2 else ''}"
    )


def format_bot_exile(wallet_address: str, bot_score: float, top_signals: dict) -> str:
    short_addr = f"{wallet_address[:6]}…{wallet_address[-4:]}"
    signals_str = "\n".join(
        f"  • {k}: <b>{v:.2f}</b>" for k, v in list(top_signals.items())[:5]
    )
    return (
        f"🤖 <b>BOT EXILED</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👛 <code>{wallet_address}</code>\n"
        f"☠️ Bot Score: <b>{bot_score:.2f}</b>  (threshold: {settings.bot_score_threshold})\n"
        f"📡 Signals fired:\n{signals_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔕 Wallet permanently exiled from tracking."
    )


def format_heartbeat(
    tier1: int, tier2: int, candidates: int, exiled: int,
    trades_last_hour: int, alerts_last_hour: int, errors_last_hour: int,
) -> str:
    total = tier1 + tier2 + candidates
    bar_filled = int((tier1 / max(settings.tier1_max_wallets, 1)) * 10)
    bar = "█" * bar_filled + "░" * (10 - bar_filled)

    return (
        f"🤖 <b>AGENT HEARTBEAT</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🥇 Tier 1 (live):   <b>{tier1}</b>  [{bar}] {tier1}/{settings.tier1_max_wallets}\n"
        f"🥈 Tier 2 (daily):  <b>{tier2}</b>\n"
        f"🔍 Candidates:      <b>{candidates}</b>\n"
        f"💀 Exiled (bots):   <b>{exiled}</b>\n"
        f"📦 Total DB:        <b>{total}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 Trades/hr:  <b>{trades_last_hour}</b>\n"
        f"🔔 Alerts/hr:  <b>{alerts_last_hour}</b>\n"
        f"⚠️ Errors/hr:  <b>{errors_last_hour}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
    )


# ─── Public send functions ────────────────────────────────────────────────────
@dataclass
class TradeAlert:
    wallet: Wallet
    trade: ParsedTrade
    pnl_usd: Optional[float] = None
    is_copy_eligible: bool = False


async def send_alert(alert: TradeAlert):
    """Send a buy or sell alert to Telegram."""
    if alert.trade.side == "buy":
        text = format_buy_alert(alert.wallet, alert.trade, alert.is_copy_eligible)
    else:
        text = format_sell_alert(alert.wallet, alert.trade, alert.pnl_usd)
    await _send_message(text)


async def send_tier_change_alert(wallet: Wallet, old_tier: WalletTier, new_tier: WalletTier):
    """Alert when a wallet moves between tiers."""
    # Only notify on promotions to Tier 1 or Tier 2 (avoid spam)
    if new_tier in (WalletTier.TIER1, WalletTier.TIER2):
        text = format_tier_promotion(wallet, old_tier, new_tier)
        await _send_message(text)


async def send_bot_exile_alert(wallet_address: str, bot_score: float, signals: dict):
    """Alert when a wallet is detected and exiled as a bot."""
    text = format_bot_exile(wallet_address, bot_score, signals)
    await _send_message(text)


async def send_heartbeat(
    tier1: int, tier2: int, candidates: int, exiled: int,
    trades_last_hour: int, alerts_last_hour: int, errors_last_hour: int = 0,
):
    """Send hourly health report to Telegram."""
    text = format_heartbeat(tier1, tier2, candidates, exiled,
                            trades_last_hour, alerts_last_hour, errors_last_hour)
    await _send_message(text)

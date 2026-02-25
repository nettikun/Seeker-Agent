from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict
from loguru import logger

SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
STABLE_MINTS = {USDC_MINT, USDT_MINT}

@dataclass
class ParsedTrade:
    signature: str
    wallet_address: str
    token_address: str
    token_symbol: str
    side: str
    amount_sol: float
    amount_usd: float
    price_usd: float
    block_time: datetime
    used_jito: bool = False
    blocks_after_mint: Optional[int] = None
    hold_time_secs: Optional[float] = None
    raw: Dict = field(default_factory=dict)

def _safe_float(val, default=0.0):
    try:
        return float(val) if val is not None else default
    except:
        return default

def parse_enhanced_transaction(txn, wallet_address):
    try:
        sig = txn.get("signature", "")
        ts = txn.get("timestamp", 0)
        block_time = datetime.utcfromtimestamp(ts) if ts else datetime.utcnow()
        transfers = txn.get("tokenTransfers", [])
        sent = [t for t in transfers if t.get("fromUserAccount") == wallet_address]
        received = [t for t in transfers if t.get("toUserAccount") == wallet_address]
        native_change = 0
        for acc in txn.get("accountData", []):
            if acc.get("account") == wallet_address:
                native_change = acc.get("nativeBalanceChange", 0)
                break
        def is_base(mint):
            return mint in (SOL_MINT, "") or mint in STABLE_MINTS
        sent_mints = {t.get("mint", "") for t in sent}
        received_mints = {t.get("mint", "") for t in received}
        traded_sent = [m for m in sent_mints if not is_base(m)]
        traded_received = [m for m in received_mints if not is_base(m)]
        if traded_received:
            token_mint = traded_received[0]
            tt = next((t for t in received if t.get("mint") == token_mint), {})
            sol_spent = abs(native_change) / 1e9 if native_change < 0 else 0
            usd_val = sol_spent * 150
            return ParsedTrade(signature=sig, wallet_address=wallet_address,
                token_address=token_mint, token_symbol=tt.get("symbol", token_mint[:8]),
                side="buy", amount_sol=sol_spent, amount_usd=usd_val,
                price_usd=usd_val / max(_safe_float(tt.get("tokenAmount", 1)), 1),
                block_time=block_time, used_jito="jito" in str(txn).lower())
        elif traded_sent:
            token_mint = traded_sent[0]
            tt = next((t for t in sent if t.get("mint") == token_mint), {})
            sol_received = native_change / 1e9 if native_change > 0 else 0
            usd_val = sol_received * 150
            return ParsedTrade(signature=sig, wallet_address=wallet_address,
                token_address=token_mint, token_symbol=tt.get("symbol", token_mint[:8]),
                side="sell", amount_sol=sol_received, amount_usd=usd_val,
                price_usd=usd_val / max(_safe_float(tt.get("tokenAmount", 1)), 1),
                block_time=block_time, used_jito="jito" in str(txn).lower())
        swap = txn.get("events", {}).get("swap", {})
        if swap:
            ni = swap.get("nativeInput") or {}
            no = swap.get("nativeOutput") or {}
            ti = swap.get("tokenInputs", [])
            to = swap.get("tokenOutputs", [])
            if ni and to:
                sol = _safe_float(ni.get("amount", 0)) / 1e9
                t = to[0]
                return ParsedTrade(signature=sig, wallet_address=wallet_address,
                    token_address=t.get("mint",""), token_symbol=t.get("symbol","?"),
                    side="buy", amount_sol=sol, amount_usd=sol*150, price_usd=0,
                    block_time=block_time, used_jito="jito" in str(txn).lower())
            elif ti and no:
                sol = _safe_float(no.get("amount", 0)) / 1e9
                t = ti[0]
                return ParsedTrade(signature=sig, wallet_address=wallet_address,
                    token_address=t.get("mint",""), token_symbol=t.get("symbol","?"),
                    side="sell", amount_sol=sol, amount_usd=sol*150, price_usd=0,
                    block_time=block_time, used_jito="jito" in str(txn).lower())
        return None
    except Exception as e:
        logger.debug(f"parse error: {e}")
        return None
def parse_enhanced_transaction(txn, wallet_address):
    try:
        sig = txn.get("signature", "")
        ts = txn.get("timestamp", 0)
        block_time = datetime.utcfromtimestamp(ts) if ts else datetime.utcnow()
        transfers = txn.get("tokenTransfers", [])
        sent = [t for t in transfers if t.get("fromUserAccount") == wallet_address]
        received = [t for t in transfers if t.get("toUserAccount") == wallet_address]
        native_change = 0
        for acc in txn.get("accountData", []):
            if acc.get("account") == wallet_address:
                native_change = acc.get("nativeBalanceChange", 0)
                break
        def is_base(mint):
            return mint in (SOL_MINT, "") or mint in STABLE_MINTS
        traded_sent = [t.get("mint","") for t in sent if not is_base(t.get("mint",""))]
        traded_received = [t.get("mint","") for t in received if not is_base(t.get("mint",""))]
        if traded_received:
            token_mint = traded_received[0]
            tt = next((t for t in received if t.get("mint") == token_mint), {})
            sol_spent = abs(native_change) / 1e9 if native_change < 0 else 0
            usd_val = sol_spent * 150
            return ParsedTrade(signature=sig, wallet_address=wallet_address,
                token_address=token_mint, token_symbol=tt.get("symbol", token_mint[:8]),
                side="buy", amount_sol=sol_spent, amount_usd=usd_val, price_usd=0,
                block_time=block_time, used_jito="jito" in str(txn).lower())
        elif traded_sent:
            token_mint = traded_sent[0]
            tt = next((t for t in sent if t.get("mint") == token_mint), {})
            sol_received = native_change / 1e9 if native_change > 0 else 0
            usd_val = sol_received * 150
            return ParsedTrade(signature=sig, wallet_address=wallet_address,
                token_address=token_mint, token_symbol=tt.get("symbol", token_mint[:8]),
                side="sell", amount_sol=sol_received, amount_usd=usd_val, price_usd=0,
                block_time=block_time, used_jito="jito" in str(txn).lower())
        return None
    except Exception as e:
        logger.debug(f"parse error: {e}")
        return None
def parse_transactions_batch(txns, wallet_address):
    trades = []
    for txn in txns:
        t = parse_enhanced_transaction(txn, wallet_address)
        if t and t.token_address and t.token_address not in STABLE_MINTS:
            trades.append(t)
    return trades

def calculate_trade_pnl(buys, sell):
    if not buys:
        return 0.0
    return sell.amount_usd - (sum(b.amount_usd for b in buys) / len(buys))

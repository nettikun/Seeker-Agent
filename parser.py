"""
parser.py — Parse Helius enhanced transactions into ParsedTrade objects.
Handles multiple Helius response formats.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any
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
    side: str                        # "buy" | "sell"
    amount_sol: float
    amount_usd: float
    price_usd: float
    block_time: datetime
    used_jito: bool = False
    blocks_after_mint: Optional[int] = None
    hold_time_secs: Optional[float] = None
    raw: Dict = field(default_factory=dict)


def _safe_float(val, default=0.0) -> float:
    try:
        return float(val) if val is not None else default
    except (TypeError, ValueError):
        return default


def _parse_via_token_transfers(txn: Dict, wallet_address: str) -> Optional[ParsedTrade]:
    """
    Parse using tokenTransfers array — most reliable Helius format.
    A swap = one transfer IN + one transfer OUT for the same wallet.
    """
    sig = txn.get("signature", "")
    ts = txn.get("timestamp", 0)
    block_time = datetime.utcfromtimestamp(ts) if ts else datetime.utcnow()

    transfers = txn.get("tokenTransfers", [])
    if not transfers:
        return None

    # Find transfers where our wallet is sender or receiver
    sent = [t for t in transfers if t.get("fromUserAccount") == wallet_address]
    received = [t for t in transfers if t.get("toUserAccount") == wallet_address]

    if not sent and not received:
        return None

    # Determine side based on what was sent vs received
    # BUY = sent SOL/stable, received token
    # SELL = sent token, received SOL/stable

    sent_mints = {t.get("mint", "") for t in sent}
    received_mints = {t.get("mint", "") for t in received}

    # Find the non-SOL, non-stable token
    def is_base(mint):
        return mint in (SOL_MINT, "") or mint in STABLE_MINTS

    traded_tokens_sent = [m for m in sent_mints if not is_base(m)]
    traded_tokens_received = [m for m in received_mints if not is_base(m)]

    # Also check native SOL balance change
    native_change = 0
    for acc in txn.get("accountData", []):
        if acc.get("account") == wallet_address:
            native_change = acc.get("nativeBalanceChange", 0)
            break

    if traded_tokens_received:
        # BUY — received a non-SOL token
        token_mint = traded_tokens_received[0]
        token_transfer = next((t for t in received if t.get("mint") == token_mint), {})
        token_symbol = token_transfer.get("symbol", token_mint[:8])
        token_amount = _safe_float(token_transfer.get("tokenAmount", 0))

        # Sol spent = negative native change or sent stable
        sol_spent = abs(native_change) / 1e9 if native_change < 0 else 0
        if not sol_spent:
            # Check sent stables
            for s in sent:
                if s.get("mint") in STABLE_MINTS:
                    sol_spent = _safe_float(s.get("tokenAmount", 0)) / 150

        usd_val = sol_spent * 150
        price = usd_val / token_amount if token_amount > 0 else 0

        return ParsedTrade(
            signature=sig,
            wallet_address=wallet_address,
            token_address=token_mint,
            token_symbol=token_symbol,
            side="buy",
            amount_sol=sol_spent,
            amount_usd=usd_val,
            price_usd=price,
            block_time=block_time,
            used_jito="jito" in str(txn).lower(),
            raw=txn,
        )

    elif traded_tokens_sent:
        # SELL — sent a non-SOL token
        token_mint = traded_tokens_sent[0]
        token_transfer = next((t for t in sent if t.get("mint") == token_mint), {})
        token_symbol = token_transfer.get("symbol", token_mint[:8])
        token_amount = _safe_float(token_transfer.get("tokenAmount", 0))

        # SOL received = positive native change
        sol_received = native_change / 1e9 if native_change > 0 else 0
        if not sol_received:
            for r in received:
                if r.get("mint") in STABLE_MINTS:
                    sol_received = _safe_float(r.get("tokenAmount", 0)) / 150

        usd_val = sol_received * 150
        price = usd_val / token_amount if token_amount > 0 else 0

        return ParsedTrade(
            signature=sig,
            wallet_address=wallet_address,
            token_address=token_mint,
            token_symbol=token_symbol,
            side="sell",
            amount_sol=sol_received,
            amount_usd=usd_val,
            price_usd=price,
            block_time=block_time,
            used_jito="jito" in str(txn).lower(),
            raw=txn,
        )

    return None


def _parse_via_swap_event(txn, wallet_address):
    sig = txn.get("signature", "")
    ts = txn.get("timestamp", 0)
    block_time = datetime.utcfromtimestamp(ts) if ts else datetime.utcnow()
    swap = txn.get("events", {}).get("swap", {})
    if not swap:
        return None
    native_input = swap.get("nativeInput") or {}
    native_output = swap.get("nativeOutput") or {}
    token_inputs = swap.get("tokenInputs", [])
    token_outputs = swap.get("tokenOutputs", [])
    if native_input and token_outputs:
        sol_amount = _safe_float(native_input.get("amount", 0)) / 1e9
        token_out = token_outputs[0]
        token_mint = token_out.get("mint", "")
        token_symbol = token_out.get("symbol", token_mint[:8])
        usd_val = sol_amount * 150
        token_amount = _safe_float(token_out.get("rawTokenAmount", {}).get("tokenAmount", 1))
        return ParsedTrade(signature=sig, wallet_address=wallet_address, token_address=token_mint,
            token_symbol=token_symbol, side="buy", amount_sol=sol_amount, amount_usd=usd_val,
            price_usd=usd_val / max(token_amount, 1), block_time=block_time,
            used_jito="jito" in str(txn).lower(), raw=txn)
    elif token_inputs and native_output:
        sol_amount = _safe_float(native_output.get("amount", 0)) / 1e9
        token_in = token_inputs[0]
        token_mint = token_in.get("mint", "")
        token_symbol = token_in.get("symbol", token_mint[:8])
        usd_val = sol_amount * 150
        token_amount = _safe_float(token_in.get("rawTokenAmount", {}).get("tokenAmount", 1))
        return ParsedTrade(signature=sig, wallet_address=wallet_address, token_address=token_mint,
            token_symbol=token_symbol, side="sell", amount_sol=sol_amount, amount_usd=usd_val,
            price_usd=usd_val / max(token_amount, 1), block_time=block_time,
            used_jito="jito" in str(txn).lower(), raw=txn)
    elif token_inputs and token_outputs:
        token_in = token_inputs[0]
        token_out = token_outputs[0]
        in_mint = token_in.get("mint", "")
        out_mint = token_out.get("mint", "")
        if in_mint in STABLE_MINTS or in_mint == SOL_MINT:
            usd_val = _safe_float(token_in.get("rawTokenAmount", {}).get("tokenAmount", 0))
            token_mint = out_mint

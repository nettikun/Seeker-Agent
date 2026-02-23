"""
parser.py — Parse Helius enhanced transactions into structured Trade objects
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any
from loguru import logger

SOL_MINT = "So11111111111111111111111111111111111111112"


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
    raw: Dict = field(default_factory=dict)


def parse_enhanced_transaction(txn: Dict, wallet_address: str) -> Optional[ParsedTrade]:
    """
    Convert a single Helius enhanced transaction dict into a ParsedTrade.
    Returns None if the transaction is not a relevant swap.
    """
    try:
        if txn.get("type") not in ("SWAP", "TOKEN_SWAP"):
            return None

        sig = txn.get("signature", "")
        ts = txn.get("timestamp", 0)
        block_time = datetime.utcfromtimestamp(ts) if ts else datetime.utcnow()

        # Extract swap events
        events = txn.get("events", {})
        swap = events.get("swap") or {}

        token_in = swap.get("tokenInputs", [{}])[0] if swap.get("tokenInputs") else {}
        token_out = swap.get("tokenOutputs", [{}])[0] if swap.get("tokenOutputs") else {}

        if not token_in or not token_out:
            # Try native SOL swaps
            native_in = swap.get("nativeInput") or {}
            native_out = swap.get("nativeOutput") or {}
            if native_in and token_out:
                # Buying token with SOL
                sol_amount = native_in.get("amount", 0) / 1e9
                token_mint = token_out.get("mint", "")
                token_sym = token_out.get("symbol", token_mint[:8])
                usd_val = swap.get("innerSwaps", [{}])[0].get("amountUSD", sol_amount * 150) if sol_amount else 0
                return ParsedTrade(
                    signature=sig,
                    wallet_address=wallet_address,
                    token_address=token_mint,
                    token_symbol=token_sym,
                    side="buy",
                    amount_sol=sol_amount,
                    amount_usd=usd_val,
                    price_usd=usd_val / (token_out.get("rawTokenAmount", {}).get("tokenAmount", 1) or 1),
                    block_time=block_time,
                    used_jito="jito" in str(txn).lower(),
                    raw=txn,
                )
            elif token_in and native_out:
                # Selling token for SOL
                sol_amount = native_out.get("amount", 0) / 1e9
                token_mint = token_in.get("mint", "")
                token_sym = token_in.get("symbol", token_mint[:8])
                usd_val = sol_amount * 150  # rough, price oracle would improve this
                return ParsedTrade(
                    signature=sig,
                    wallet_address=wallet_address,
                    token_address=token_mint,
                    token_symbol=token_sym,
                    side="sell",
                    amount_sol=sol_amount,
                    amount_usd=usd_val,
                    price_usd=usd_val / (token_in.get("rawTokenAmount", {}).get("tokenAmount", 1) or 1),
                    block_time=block_time,
                    used_jito="jito" in str(txn).lower(),
                    raw=txn,
                )
            return None

        # Token-to-token swap
        in_mint = token_in.get("mint", "")
        out_mint = token_out.get("mint", "")

        if in_mint == SOL_MINT or in_mint == "":
            side = "buy"
            token_mint = out_mint
            token_sym = token_out.get("symbol", out_mint[:8])
            sol_amount = token_in.get("rawTokenAmount", {}).get("tokenAmount", 0) / 1e9
        else:
            side = "sell"
            token_mint = in_mint
            token_sym = token_in.get("symbol", in_mint[:8])
            sol_amount = token_out.get("rawTokenAmount", {}).get("tokenAmount", 0) / 1e9

        # USD value — Helius sometimes provides this
        usd_val = swap.get("amountUSD", sol_amount * 150)

        return ParsedTrade(
            signature=sig,
            wallet_address=wallet_address,
            token_address=token_mint,
            token_symbol=token_sym,
            side=side,
            amount_sol=sol_amount,
            amount_usd=float(usd_val),
            price_usd=float(usd_val) / max(1, token_out.get("rawTokenAmount", {}).get("tokenAmount", 1)),
            block_time=block_time,
            used_jito="jito" in str(txn).lower(),
            raw=txn,
        )

    except Exception as e:
        logger.debug(f"parse_enhanced_transaction error ({txn.get('signature', '?')}): {e}")
        return None


def parse_transactions_batch(txns: List[Dict], wallet_address: str) -> List[ParsedTrade]:
    trades = []
    for txn in txns:
        t = parse_enhanced_transaction(txn, wallet_address)
        if t:
            trades.append(t)
    return trades


def calculate_trade_pnl(buys: List[ParsedTrade], sell: ParsedTrade) -> float:
    """
    Match a sell against prior buys (FIFO) to compute realised PnL in USD.
    """
    if not buys:
        return 0.0
    avg_buy_price = sum(b.amount_usd for b in buys) / len(buys)
    return sell.amount_usd - avg_buy_price

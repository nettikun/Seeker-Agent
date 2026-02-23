"""
webhook_server.py — FastAPI server that receives Helius real-time webhook events
and dispatches them into the processing pipeline.
"""
import asyncio
import hashlib
import hmac
import json
from typing import List, Callable, Awaitable
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from loguru import logger
from config import settings

app = FastAPI(title="Solana Wallet Agent — Webhook Receiver")

# Callback registry: list of async functions to call with each transaction payload
_handlers: List[Callable[[dict], Awaitable[None]]] = []


def register_handler(fn: Callable[[dict], Awaitable[None]]):
    """Register an async function that will be called for every incoming webhook event."""
    _handlers.append(fn)


@app.post("/webhook/helius")
async def helius_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Helius posts enhanced transaction events here whenever a tracked wallet
    fires a SWAP transaction.
    """
    body = await request.body()

    # Verify webhook signature if secret is set
    if settings.helius_webhook_secret:
        signature = request.headers.get("helius-signature", "")
        expected = hmac.new(
            settings.helius_webhook_secret.encode(),
            body,
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(signature, expected):
            raise HTTPException(status_code=401, detail="Invalid webhook signature")

    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # Dispatch to all registered handlers in background so we return 200 fast
    for txn in (payload if isinstance(payload, list) else [payload]):
        for handler in _handlers:
            background_tasks.add_task(handler, txn)

    return {"status": "ok"}


@app.get("/health")
async def health():
    return {"status": "running"}

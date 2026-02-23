"""
main.py — Entry point. Run with: python main.py
"""
import asyncio
import sys
from loguru import logger
from orchestrator import run_agent

# ─── Logging Setup ────────────────────────────────────────────────────────────
logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
    level="INFO",
    colorize=True,
)
logger.add(
    "logs/agent.log",
    rotation="100 MB",
    retention="30 days",
    compression="gz",
    level="DEBUG",
    enqueue=True,
)

if __name__ == "__main__":
    import os

    # ── Public URL for Helius webhooks ────────────────────────────────────────
    # This must be a publicly reachable HTTPS URL.
    # Options:
    #   - Deploy on Railway/Fly.io and use the assigned URL
    #   - Use ngrok for local testing: ngrok http 8080 → paste the https URL here
    #   - Set PUBLIC_WEBHOOK_URL env var
    public_webhook_url = os.getenv(
        "PUBLIC_WEBHOOK_URL",
        "https://your-server.com/webhook/helius"  # ← CHANGE THIS
    )

    logger.info(f"Webhook URL: {public_webhook_url}")
    logger.info("Starting agent... press Ctrl+C to stop.")

    try:
        asyncio.run(run_agent(public_webhook_url))
    except KeyboardInterrupt:
        logger.info("Agent stopped by user.")
    except Exception as e:
        logger.critical(f"Agent crashed: {e}")
        sys.exit(1)

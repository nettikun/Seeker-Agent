"""
config.py — Centralised settings loaded from environment variables.
Handles Railway's automatic DATABASE_URL and REDIS_URL injection.
"""
import os
from pydantic_settings import BaseSettings
from typing import List


def _get_database_url() -> str:
    url = (
        os.getenv("DATABASE_URL") or
        os.getenv("POSTGRESQL_URL") or
        os.getenv("POSTGRES_URL") or
        ""
    )
    if not url:
        raise RuntimeError(
            "No database URL found. In Railway: make sure you added a "
            "PostgreSQL plugin to your project."
        )
    url = url.replace("postgres://", "postgresql+asyncpg://")
    url = url.replace("postgresql://", "postgresql+asyncpg://")
    return url


def _get_redis_url() -> str:
    return (
        os.getenv("REDIS_URL") or
        os.getenv("REDIS_PRIVATE_URL") or
        "redis://localhost:6379/0"
    )


class Settings(BaseSettings):
    helius_api_key: str = "32d9767f-1961-4fa4-a0ac-8de5b50fe7e7"
    helius_webhook_secret: str = ""
    birdeye_api_key: str = ""

    database_url: str = ""
    redis_url: str = ""

    telegram_bot_token: str = "8394789655:AAEUGczu5_M0vN2qUV9Hk5XAnWVM9jeBcGU"
    telegram_chat_id: str = ""

    min_win_rate: float = 0.30
    min_trades: int = 50
    bot_score_threshold: float = 0.55
    tier1_max_wallets: int = 200
    tier2_max_wallets: int = 1000
    discovery_interval_secs: int = 300
    rescore_interval_secs: int = 3600
    prune_inactive_days: int = 30
    webhook_server_port: int = 8080

    seed_wallets: List[str] = []

    class Config:
        env_file = ".env"
        env_fi

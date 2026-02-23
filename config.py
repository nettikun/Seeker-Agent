"""
config.py — Centralised settings loaded from .env
"""
from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    # API keys
    helius_api_key: str = "32d9767f-1961-4fa4-a0ac-8de5b50fe7e7"
    helius_webhook_secret: str = ""
    birdeye_api_key: str = ""

    # Infra
    database_url: str
    redis_url: str = "redis://localhost:6379/0"

    # Telegram Alerts (only alert channel)
    telegram_bot_token: str = "8394789655:AAEUGczu5_M0vN2qUV9Hk5XAnWVM9jeBcGU"
    telegram_chat_id: str = ""

    # Agent thresholds
    min_win_rate: float = 0.30
    min_trades: int = 50
    bot_score_threshold: float = 0.55
    tier1_max_wallets: int = 200
    tier2_max_wallets: int = 1000
    discovery_interval_secs: int = 300
    rescore_interval_secs: int = 3600
    prune_inactive_days: int = 30
    webhook_server_port: int = 8080

    # Seeds — known good wallets to bootstrap discovery
    seed_wallets: List[str] = []

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()

# Helius base URLs
HELIUS_RPC = f"https://mainnet.helius-rpc.com/?api-key={settings.helius_api_key}"
HELIUS_API = "https://api.helius.xyz/v0"
BIRDEYE_API = "https://public-api.birdeye.so/v1"

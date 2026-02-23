"""
database.py — SQLAlchemy async models + session factory
"""
from datetime import datetime
from typing import Optional, AsyncGenerator
from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, Float, Index,
    Integer, String, Text, ForeignKey, Enum as SAEnum
)
from sqlalchemy.orm import DeclarativeBase, relationship
from sqlalchemy.ext.asyncio import (
    AsyncSession, async_sessionmaker, create_async_engine
)
import enum
from config import settings

# ─── Engine ──────────────────────────────────────────────────────────────────
engine = create_async_engine(settings.database_url, pool_size=20, max_overflow=40, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


# ─── Enums ───────────────────────────────────────────────────────────────────
class WalletTier(str, enum.Enum):
    CANDIDATE = "candidate"   # not yet vetted
    TIER2 = "tier2"           # daily scoring
    TIER1 = "tier1"           # real-time webhook
    EXILED = "exiled"         # bot / bad actor
    ARCHIVED = "archived"     # inactive / low score


class Base(DeclarativeBase):
    pass


# ─── Models ──────────────────────────────────────────────────────────────────
class Wallet(Base):
    __tablename__ = "wallets"

    id = Column(Integer, primary_key=True, autoincrement=True)
    address = Column(String(64), unique=True, nullable=False, index=True)
    tier = Column(SAEnum(WalletTier), default=WalletTier.CANDIDATE, nullable=False, index=True)

    # Scoring
    win_rate = Column(Float, default=0.0)
    total_trades = Column(Integer, default=0)
    winning_trades = Column(Integer, default=0)
    total_pnl_usd = Column(Float, default=0.0)
    avg_pnl_per_trade = Column(Float, default=0.0)
    avg_hold_time_secs = Column(Float, default=0.0)
    trade_size_cv = Column(Float, default=0.0)   # coefficient of variation
    bot_score = Column(Float, default=0.0)

    # Meta
    first_seen = Column(DateTime, default=datetime.utcnow)
    last_active = Column(DateTime, default=datetime.utcnow)
    last_scored = Column(DateTime, nullable=True)
    discovery_source = Column(String(128), nullable=True)  # which wallet led here
    webhook_registered = Column(Boolean, default=False)
    notes = Column(Text, nullable=True)

    trades = relationship("Trade", back_populates="wallet", lazy="dynamic")
    edges_from = relationship("WalletEdge", foreign_keys="WalletEdge.source_address",
                               primaryjoin="Wallet.address == WalletEdge.source_address",
                               lazy="dynamic")

    __table_args__ = (
        Index("ix_wallets_tier_win_rate", "tier", "win_rate"),
        Index("ix_wallets_last_active", "last_active"),
    )


class Trade(Base):
    __tablename__ = "trades"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    wallet_address = Column(String(64), ForeignKey("wallets.address"), nullable=False, index=True)
    signature = Column(String(128), unique=True, nullable=False)

    token_address = Column(String(64), nullable=True, index=True)
    token_symbol = Column(String(32), nullable=True)
    side = Column(String(8), nullable=False)      # "buy" | "sell"
    amount_sol = Column(Float, nullable=True)
    amount_usd = Column(Float, nullable=True)
    price_usd = Column(Float, nullable=True)
    pnl_usd = Column(Float, nullable=True)        # filled on sell
    is_profitable = Column(Boolean, nullable=True)

    block_time = Column(DateTime, nullable=False, index=True)
    blocks_after_mint = Column(Integer, nullable=True)  # for sniper detection
    hold_time_secs = Column(Float, nullable=True)       # buy-to-sell duration
    used_jito = Column(Boolean, default=False)

    wallet = relationship("Wallet", back_populates="trades")


class WalletEdge(Base):
    """Graph edges: wallet A and B bought the same token within N blocks."""
    __tablename__ = "wallet_edges"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    source_address = Column(String(64), ForeignKey("wallets.address"), nullable=False, index=True)
    target_address = Column(String(64), nullable=False, index=True)
    shared_token = Column(String(64), nullable=True)
    block_delta = Column(Integer, nullable=True)   # blocks between their buys
    co_occurrences = Column(Integer, default=1)
    first_seen = Column(DateTime, default=datetime.utcnow)
    last_seen = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_edges_source_target", "source_address", "target_address"),
    )


class AgentHealth(Base):
    """Heartbeat log for monitoring."""
    __tablename__ = "agent_health"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    wallets_tracked = Column(Integer, default=0)
    tier1_count = Column(Integer, default=0)
    tier2_count = Column(Integer, default=0)
    candidates_count = Column(Integer, default=0)
    trades_processed_last_hour = Column(Integer, default=0)
    alerts_sent_last_hour = Column(Integer, default=0)
    errors_last_hour = Column(Integer, default=0)


async def create_all_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

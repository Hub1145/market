from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Float, ForeignKey, String, Text, JSON
from sqlalchemy.orm import Mapped, mapped_column

from packages.db.base import Base

class RawTradeEvent(Base):
    __tablename__ = "raw_trade_events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    external_id: Mapped[str] = mapped_column(String, index=True, unique=True) # transaction hash or event ID
    source: Mapped[str] = mapped_column(String) # clob, indexer, stream
    raw_payload: Mapped[dict] = mapped_column(JSON)
    
    received_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

class Trade(Base):
    __tablename__ = "trades"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    market_id: Mapped[str] = mapped_column(ForeignKey("markets.id"))
    outcome_id: Mapped[int] = mapped_column(ForeignKey("outcomes.id"))
    trader_address: Mapped[str] = mapped_column(ForeignKey("trader_wallets.address"))
    
    side: Mapped[str] = mapped_column(String) # buy, sell
    price: Mapped[float] = mapped_column(Float)
    size: Mapped[float] = mapped_column(Float)
    notional: Mapped[float] = mapped_column(Float)
    
    transaction_hash: Mapped[str] = mapped_column(String, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, index=True)
    
    # Analytics enrichment
    is_reprice: Mapped[bool] = mapped_column(default=False)
    clv_score: Mapped[Optional[float]] = mapped_column(Float)

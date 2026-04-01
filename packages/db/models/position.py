from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Float, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from packages.db.base import Base

class PositionSnapshot(Base):
    __tablename__ = "position_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    trader_address: Mapped[str] = mapped_column(ForeignKey("trader_wallets.address"))
    market_id: Mapped[str] = mapped_column(ForeignKey("markets.id"))
    outcome_id: Mapped[int] = mapped_column(ForeignKey("outcomes.id"))
    
    current_size: Mapped[float] = mapped_column(Float)
    avg_entry_price: Mapped[float] = mapped_column(Float)
    unrealized_pnl: Mapped[float] = mapped_column(Float)
    
    snapshot_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

class ClosedPosition(Base):
    __tablename__ = "closed_positions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    trader_address: Mapped[str] = mapped_column(ForeignKey("trader_wallets.address"))
    market_id: Mapped[str] = mapped_column(ForeignKey("markets.id"))
    outcome_id: Mapped[int] = mapped_column(ForeignKey("outcomes.id"))
    
    buy_size: Mapped[float] = mapped_column(Float)
    buy_avg_price: Mapped[float] = mapped_column(Float)
    sell_size: Mapped[float] = mapped_column(Float)
    sell_avg_price: Mapped[float] = mapped_column(Float)
    
    realized_pnl: Mapped[float] = mapped_column(Float)
    realized_edge: Mapped[float] = mapped_column(Float) # PnL normalized by notional or probability
    
    closed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

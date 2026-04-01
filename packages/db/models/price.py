from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from packages.db.base import Base

class PriceSnapshot(Base):
    __tablename__ = "price_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    market_id: Mapped[str] = mapped_column(ForeignKey("markets.id"))
    outcome_id: Mapped[int] = mapped_column(ForeignKey("outcomes.id"))
    
    best_bid: Mapped[float] = mapped_column(Float)
    best_ask: Mapped[float] = mapped_column(Float)
    mid_price: Mapped[float] = mapped_column(Float)
    
    timestamp: Mapped[datetime] = mapped_column(DateTime, index=True)

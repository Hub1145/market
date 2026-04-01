from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Float, ForeignKey, String, Text, JSON
from sqlalchemy.orm import Mapped, mapped_column

from packages.db.base import Base

class TraderScoreSnapshot(Base):
    __tablename__ = "trader_score_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    address: Mapped[str] = mapped_column(ForeignKey("trader_wallets.address"))
    
    repricing_score: Mapped[float] = mapped_column(Float) # near-term predictive skill
    resolution_score: Mapped[float] = mapped_column(Float) # final resolution predictive skill
    composite_score: Mapped[float] = mapped_column(Float)
    
    topic: Mapped[Optional[str]] = mapped_column(String, index=True) # tag-specific score
    
    sample_size: Mapped[int] = mapped_column(default=0)
    calculated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

class MarketSignalSnapshot(Base):
    __tablename__ = "market_signal_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    market_id: Mapped[str] = mapped_column(ForeignKey("markets.id"))
    outcome_id: Mapped[int] = mapped_column(ForeignKey("outcomes.id"))
    
    signal_type: Mapped[str] = mapped_column(String) # repricing, final_resolution
    signal_strength: Mapped[float] = mapped_column(Float)
    directional_bias: Mapped[str] = mapped_column(String) # YES, NO
    
    # Explainability
    explanation: Mapped[dict] = mapped_column(JSON) # Structured reasoning
    top_traders: Mapped[dict] = mapped_column(JSON) # List of contributing skilled traders
    
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Float, String, Text, JSON
from sqlalchemy.orm import Mapped, mapped_column

from packages.db.base import Base

class TraderWallet(Base):
    __tablename__ = "trader_wallets"

    address: Mapped[str] = mapped_column(String, primary_key=True) # proxyWallet address
    owner_address: Mapped[Optional[str]] = mapped_column(String, index=True) # EOA if known
    ens_name: Mapped[Optional[str]] = mapped_column(String)
    
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class TraderProfile(Base):
    __tablename__ = "trader_profiles"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    address: Mapped[str] = mapped_column(String, index=True)
    
    # Statistical summaries
    total_trades: Mapped[int] = mapped_column(default=0)
    win_rate: Mapped[float] = mapped_column(default=0.0)
    profit_loss: Mapped[float] = mapped_column(default=0.0)
    
    # Alpha features
    avg_clv: Mapped[float] = mapped_column(default=0.0)
    median_clv: Mapped[float] = mapped_column(default=0.0)
    directional_purity: Mapped[float] = mapped_column(default=1.0)
    gamma_score: Mapped[float] = mapped_column(default=0.0)
    
    last_updated: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

class TraderClassification(Base):
    __tablename__ = "trader_classifications"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    address: Mapped[str] = mapped_column(String, index=True)
    
    label: Mapped[str] = mapped_column(String) # whale, MM, copier, directional_discretionary
    confidence: Mapped[float] = mapped_column(default=1.0)
    reasoning: Mapped[Optional[str]] = mapped_column(Text)
    
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

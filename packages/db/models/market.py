from datetime import datetime
from typing import List, Optional

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from packages.db.base import Base

class Event(Base):
    __tablename__ = "events"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    title: Mapped[str] = mapped_column(String)
    description: Mapped[Optional[str]] = mapped_column(Text)
    category: Mapped[Optional[str]] = mapped_column(String)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    closed: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    markets: Mapped[List["Market"]] = relationship("Market", back_populates="event", lazy="selectin")

class Market(Base):
    __tablename__ = "markets"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    event_id: Mapped[str] = mapped_column(ForeignKey("events.id"))
    question: Mapped[str] = mapped_column(String)
    slug: Mapped[str] = mapped_column(String, unique=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    closed: Mapped[bool] = mapped_column(Boolean, default=False)
    resolution_source: Mapped[Optional[str]] = mapped_column(String)
    end_date_iso: Mapped[Optional[datetime]] = mapped_column(DateTime)
    market_type: Mapped[str] = mapped_column(String) # binary, categorical
    
    event: Mapped["Event"] = relationship("Event", back_populates="markets", lazy="selectin")
    outcomes: Mapped[List["Outcome"]] = relationship("Outcome", back_populates="market", lazy="selectin")
    tags: Mapped[List["MarketTag"]] = relationship("MarketTag", back_populates="market", lazy="selectin")

class Outcome(Base):
    __tablename__ = "outcomes"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    market_id: Mapped[str] = mapped_column(ForeignKey("markets.id"))
    name: Mapped[str] = mapped_column(String)
    asset_id: Mapped[str] = mapped_column(String) # CLOB asset ID
    
    market: Mapped["Market"] = relationship("Market", back_populates="outcomes", lazy="selectin")

class MarketTag(Base):
    __tablename__ = "market_tags"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    market_id: Mapped[str] = mapped_column(ForeignKey("markets.id"))
    tag: Mapped[str] = mapped_column(String, index=True)
    
    market: Mapped["Market"] = relationship("Market", back_populates="tags", lazy="selectin")

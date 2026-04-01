import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from packages.db.models.market import Market, Outcome
from packages.db.models.trade import Trade
from packages.db.models.scoring import MarketSignalSnapshot

logger = logging.getLogger(__name__)

def calculate_pnl(entry_price: float, exit_price: float, size: float, side: str) -> float:
    """
    Compute theoretical PnL for a signal.
    """
    if side.lower() == "yes":
        return (exit_price - entry_price) * size
    else:
        return (entry_price - exit_price) * size

async def simulate_alpha(
    session: AsyncSession, 
    start_date: datetime, 
    end_date: datetime
) -> List[Dict[str, Any]]:
    """
    Replay historical signals and calculate their 'precision' (profitability).
    """
    logger.info(f"Simulating alpha from {start_date} to {end_date}...")
    
    # 1. Fetch signal snapshots in range
    stmt = (
        select(MarketSignalSnapshot)
        .where(MarketSignalSnapshot.created_at >= start_date)
        .where(MarketSignalSnapshot.created_at <= end_date)
        .order_by(MarketSignalSnapshot.created_at.asc())
    )
    snapshots = (await session.execute(stmt)).scalars().all()
    
    results = []
    for snap in snapshots:
        # 2. Look ahead for price movement (Convergence)
        # We check the average trade price 1 hour after the signal
        lookback_limit = snap.created_at + timedelta(hours=1)
        
        trade_stmt = (
            select(Trade)
            .where(Trade.market_id == snap.market_id)
            .where(Trade.outcome_id == snap.outcome_id)
            .where(Trade.timestamp > snap.created_at)
            .where(Trade.timestamp <= lookback_limit)
        )
        post_trades = (await session.execute(trade_stmt)).scalars().all()
        
        if not post_trades:
            continue
            
        entry_price = post_trades[0].price # First trade after signal
        exit_price = post_trades[-1].price # Last trade in the 1h window
        
        pnl = calculate_pnl(entry_price, exit_price, 100, snap.directional_bias)
        
        results.append({
            "timestamp": snap.created_at,
            "market_id": snap.market_id,
            "strength": snap.signal_strength,
            "bias": snap.directional_bias,
            "pnl": pnl,
            "is_correct": pnl > 0
        })
        
    return results

import logging
from typing import List, Optional

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from packages.db.models.price import PriceSnapshot

logger = logging.getLogger(__name__)

async def compute_clv(
    session: AsyncSession, 
    market_id: str, 
    outcome_id: int, 
    entry_price: float, 
    entry_time: pd.Timestamp,
    horizons: List[str] = ["1h", "4h", "24h"]
) -> dict:
    """
    Compute Closing Line Value (CLV) for a specific trade.
    CLV = Later Price - Entry Price  (absolute probability change, not percentage).
    Prediction-market tokens are probabilities on [0,1].  Dividing by entry_price
    would inflate CLV for cheap tokens (0.05->0.10 is +5pp of edge, not +100%).
    """
    results = {}
    
    # Fetch price snapshots for this market/outcome after entry_time
    stmt = select(PriceSnapshot).where(
        PriceSnapshot.market_id == market_id,
        PriceSnapshot.outcome_id == outcome_id,
        PriceSnapshot.timestamp > entry_time
    ).order_by(PriceSnapshot.timestamp.asc())
    
    result = await session.execute(stmt)
    snapshots = result.scalars().all()
    
    if not snapshots:
        return results
        
    df = pd.DataFrame([
        {"timestamp": s.timestamp, "mid_price": s.mid_price} 
        for s in snapshots
    ])
    df.set_index("timestamp", inplace=True)
    
    for horizon in horizons:
        # Find price at entry_time + horizon
        target_time = entry_time + pd.Timedelta(horizon)
        # Find closest snapshot to target_time
        idx = df.index.get_indexer([target_time], method='nearest')[0]
        if idx != -1:
            later_price = df.iloc[idx]["mid_price"]
            clv = later_price - entry_price  # absolute probability-point change
            results[f"clv_{horizon}"] = clv
            
    return results

def compute_lateness_penalty(entry_price: float, previous_price: float, max_move: float = 0.05) -> float:
    """
    Compute a penalty for entering after price has already moved significantly.
    Uses absolute probability-point change (not percentage) consistent with CLV.
    If the price already moved > max_move probability points, the signal is stale.
    """
    move = abs(entry_price - previous_price)  # absolute pp move on [0,1]
    if move > max_move:
        return min(1.0, (move - max_move) / max_move)
    return 0.0

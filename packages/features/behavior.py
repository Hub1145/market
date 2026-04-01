import logging
from typing import List

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from packages.db.models.trade import Trade

logger = logging.getLogger(__name__)

def compute_directional_purity(trades: List[Trade]) -> float:
    """
    Compute the directional conviction of a trader.
    Purity = |Sum(Size * Side)| / Sum(|Size|)
    where Side is +1 for Buy and -1 for Sell.
    High value (near 1.0) means directional conviction.
    Low value (near 0.0) means market-making or wash trading.
    """
    if not trades:
        return 0.0
        
    numerator = 0.0
    denominator = 0.0
    
    for trade in trades:
        side_mult = 1.0 if trade.side.lower() == "buy" else -1.0
        numerator += (trade.size * side_mult)
        denominator += trade.size
        
    if denominator == 0:
        return 0.0
        
    return abs(numerator) / denominator

def compute_exposure_churn(trades: List[Trade], window_minutes: int = 60) -> float:
    """
    Measure how often a trader flips their position within a short window.
    High churn is characteristic of market makers.
    """
    if len(trades) < 2:
        return 0.0
        
    df = pd.DataFrame([
        {"timestamp": t.timestamp, "side": t.side.lower(), "size": t.size}
        for t in trades
    ])
    df.set_index("timestamp", inplace=True)
    df.sort_index(inplace=True)
    
    # Simple churn metric: count number of side flips within windows
    df['side_num'] = df['side'].map({'buy': 1, 'sell': -1})
    df['flip'] = df['side_num'].diff().abs() > 0
    
    # Churn = flips / total_trades
    return df['flip'].sum() / len(trades)

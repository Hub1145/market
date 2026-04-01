import logging
from typing import Dict, List

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from packages.db.models.market import Market, MarketTag
from packages.db.models.trade import Trade

logger = logging.getLogger(__name__)

async def aggregate_topic_skill(
    session: AsyncSession, 
    trader_address: str
) -> Dict[str, float]:
    """
    Compute median CLV per Gamma tag for a specific trader.
    Used to identify topic specialists (e.g., 'Politics expert').
    """
    # Join trades with market tags
    stmt = (
        select(MarketTag.tag, func.avg(Trade.clv_score).label("avg_clv"))
        .join(Market, Trade.market_id == Market.id)
        .join(MarketTag, Market.id == MarketTag.market_id)
        .where(Trade.trader_address == trader_address)
        .where(Trade.clv_score != None)
        .group_by(MarketTag.tag)
    )
    
    result = await session.execute(stmt)
    rows = result.all()
    
    return {row.tag: float(row.avg_clv) for row in rows}

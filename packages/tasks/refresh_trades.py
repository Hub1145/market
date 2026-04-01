import asyncio
import logging
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from packages.db.session import AsyncSessionLocal
from packages.services.trader_service import TraderService
from packages.db.models.market import Market

logger = logging.getLogger(__name__)

async def refresh_trades(session: AsyncSession):
    """
    Refresh trades for all active markets.
    """
    logger.info("Refreshing trades for active markets...")
    # Get active markets to sync
    result = await session.execute(select(Market).where(Market.active == True).limit(200))
    markets = result.scalars().all()
    
    service = TraderService(session)
    for market in markets:
        logger.info(f"Syncing trades for market: {market.id}")
        await service.sync_trades_for_market(market.id)
    
    await service.cleanup_ghost_positions()
    await service.close()
    
    logger.info("Trade refresh complete.")

if __name__ == "__main__":
    async def run():
        async with AsyncSessionLocal() as session:
            await refresh_trades(session)
            
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())

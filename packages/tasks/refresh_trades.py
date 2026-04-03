import asyncio
import logging
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from packages.db.session import AsyncSessionLocal
from packages.services.trader_service import TraderService
from packages.db.models.market import Market  # noqa: F401 — used in select(Market.id)

logger = logging.getLogger(__name__)

async def refresh_trades(session: AsyncSession):
    """
    Refresh trades for all active markets.
    """
    logger.info("Refreshing trades for active markets...")
    # Fetch only the ID column — no need to load full Market ORM objects
    result = await session.execute(
        select(Market.id).where(Market.active == True, Market.closed == False).limit(500)
    )
    market_ids = result.scalars().all()

    service = TraderService(session)
    for market_id in market_ids:
        logger.debug(f"Syncing trades for market: {market_id}")
        await service.sync_trades_for_market(market_id)
    
    await service.cleanup_ghost_positions()
    await service.close()
    
    logger.info("Trade refresh complete.")

if __name__ == "__main__":
    async def run():
        async with AsyncSessionLocal() as session:
            await refresh_trades(session)
            
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())

import asyncio
import logging
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from packages.db.session import AsyncSessionLocal
from packages.db.models.trader import TraderProfile, TraderClassification
from packages.classification.rules import classify_trader

logger = logging.getLogger(__name__)

async def classify_all_traders(session: AsyncSession):
    """Apply heuristic classification rules to all trader profiles."""
    logger.info("Classifying all traders...")

    result = await session.execute(select(TraderProfile))
    profiles = result.scalars().all()

    for profile in profiles:
        label, confidence, reasoning = classify_trader(profile)

        # Upsert: update the existing row for this address, or insert exactly one.
        # TraderClassification uses an autoincrement PK so session.merge() with id=None
        # always INSERTs — accumulating duplicate rows and causing cartesian products in
        # the signal aggregation JOIN every sync cycle.
        existing = (
            await session.execute(
                select(TraderClassification)
                .where(TraderClassification.address == profile.address)
                .limit(1)
            )
        ).scalar_one_or_none()

        if existing:
            existing.label      = label
            existing.confidence = confidence
            existing.reasoning  = reasoning
        else:
            session.add(TraderClassification(
                address=profile.address,
                label=label,
                confidence=confidence,
                reasoning=reasoning,
            ))

    await session.commit()
    logger.info(f"Classified {len(profiles)} traders.")

if __name__ == "__main__":
    async def run():
        async with AsyncSessionLocal() as session:
            await classify_all_traders(session)
    
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())

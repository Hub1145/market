import asyncio
import logging
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from packages.db.session import AsyncSessionLocal
from packages.services.market_service import MarketService
from packages.ingestion.clients.polymarket_http import GammaClient
from packages.db.models.market import Market, Outcome

logger = logging.getLogger(__name__)

# Tag IDs that are critical for external-data strategies — always fetched.
# Discovered from Polymarket's Gamma API /tags endpoint.
PRIORITY_TAG_IDS = [
    84,      # Weather
    103038,  # Earthquakes
    496,     # Natural Disasters
    92,      # Global Warming / Climate
    74,      # Science
]


async def _fetch_all_tags(gc: GammaClient) -> list:
    """Fetch all available tag IDs from the Gamma /tags endpoint."""
    try:
        import httpx
        async with httpx.AsyncClient(base_url="https://gamma-api.polymarket.com", timeout=10.0) as c:
            r = await c.get("/tags")
            r.raise_for_status()
            tags = r.json()
            return [t.get("id") for t in tags if t.get("id")]
    except Exception as e:
        logger.warning(f"Could not fetch tag list: {e}. Using priority tags only.")
        return PRIORITY_TAG_IDS


async def refresh_markets(session: AsyncSession):
    """
    Refresh active markets from Polymarket with database lifecycle management.
    
    Research Section 1/2: A strategy is only as good as its data coverage.
    This task ensures that markets tagged for the current strategy are always fresh,
    while pruning long-resolved data to maintain database health.
    """
    from packages.core.config import settings
    strategy = settings.strategy
    logger.info(f"Refreshing active markets for strategy '{strategy}'...")
    service = MarketService(session)

    # ------------------------------------------------------------------ #
    # 0. Cleanup: Prune outdated resolved markets (7-day window)         #
    # ------------------------------------------------------------------ #
    await service.prune_resolved_markets(older_than_days=7)

    # ------------------------------------------------------------------ #
    # Pass 1: Global volume-based sync (top by volume)                 #
    # Tag-based pass 2 covers the long tail — 500 here is sufficient.  #
    # ------------------------------------------------------------------ #
    await service.refresh_active_markets(limit=500)

    # ------------------------------------------------------------------ #
    # Pass 2: tag-based comprehensive fetch (+ priority tags)             #
    # ------------------------------------------------------------------ #
    gc = GammaClient()
    try:
        # Determine priority tags for current strategy
        from packages.tasks.compute_signals import _STRATEGY_TAG_FILTER
        strategy_priority_tags = _STRATEGY_TAG_FILTER.get(strategy, set())
        
        all_tag_ids = await _fetch_all_tags(gc)
        
        # Always process priority IDs first (hardcoded for reliability)
        # Combine with strategy-specific tags if they aren't already included.
        ordered_tags = PRIORITY_TAG_IDS + [
            t for t in all_tag_ids if t not in PRIORITY_TAG_IDS
        ]

        seen_event_ids: set = set()
        total_new = 0
        for tag_id in ordered_tags:
            try:
                # Per-tag depth increased for exhaustive strategy coverage
                tag_events = await gc.get_events_by_tag(tag_id, max_events=500)
                new_events = [e for e in tag_events
                               if e.get("id") not in seen_event_ids]
                seen_event_ids.update(e.get("id") for e in tag_events)
                for raw_event in new_events:
                    try:
                        await service.upsert_event(raw_event)
                        total_new += 1
                    except Exception as e:
                        logger.warning(
                            f"Tag upsert failed (tag={tag_id}, "
                            f"event={raw_event.get('id')}): {e}"
                        )
            except Exception as e:
                logger.warning(f"Tag fetch failed for tag_id={tag_id}: {e}")

        logger.info(f"Tag-based pass: {total_new} new events across {len(ordered_tags)} tags.")
    finally:
        await gc.close()

    # ------------------------------------------------------------------ #
    # Pass 3: gap fill for orphan markets (no outcomes after passes 1+2)  #
    # ------------------------------------------------------------------ #
    # ... (rest of the code remains the same)
    orphan_event_ids = (await session.execute(
        select(Market.event_id)
        .outerjoin(Outcome, Outcome.market_id == Market.id)
        .where(Market.active == True, Market.closed == False)
        .group_by(Market.event_id)
        .having(func.count(Outcome.id) == 0)
    )).scalars().all()

    if orphan_event_ids:
        logger.info(
            f"Gap-fill: {len(orphan_event_ids)} events with missing outcomes."
        )
        gc3 = GammaClient()
        filled = 0
        try:
            # Gap-fill up to 200 events per cycle
            for event_id in orphan_event_ids[:200]:
                try:
                    raw_event = await gc3.get_event(str(event_id))
                    await service.upsert_event(raw_event)
                    filled += 1
                except Exception as e:
                    logger.warning(f"Gap-fill skipped event {event_id}: {e}")
        finally:
            await gc3.close()
        logger.info(f"Gap-fill complete: recovered {filled} events.")

    logger.info("Market refresh complete.")



if __name__ == "__main__":
    async def run():
        async with AsyncSessionLocal() as session:
            await refresh_markets(session)

    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())

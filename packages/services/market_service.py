import logging
import traceback
from datetime import datetime, timedelta
from typing import List

from sqlalchemy import select, func as sqlfunc
from sqlalchemy.ext.asyncio import AsyncSession

from packages.ingestion.clients.polymarket_http import GammaClient
from packages.ingestion.normalize.markets import normalize_gamma_event
from packages.db.models.market import Market, Outcome, MarketTag
from packages.db.models.price import PriceSnapshot

logger = logging.getLogger(__name__)


class MarketService:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.gamma_client = GammaClient()

    async def refresh_active_markets(self, limit: int = 200):
        """
        Fetch active events from Gamma via pagination, normalize, and upsert.
        Research emphasises casting a wide net: skilled traders sometimes operate in
        thin / low-profile markets that a 20-event cap would miss entirely.
        """
        logger.info(f"Refreshing up to {limit} active events from Gamma...")
        try:
            raw_events = await self.gamma_client.get_events_paginated(max_events=limit)

            # Collect all price data from all events in one pass
            all_price_data: List[dict] = []

            batch_size = 50
            # Track (market_id, tag) pairs added in this session to avoid
            # double-adds that cause the SAWarning identity-map collision.
            # The Market.tags lazy="selectin" fires on session.merge(market)
            # and reloads flushed rows, colliding with pending objects if the
            # same tag was session.add()-ed earlier in the same batch.
            seen_tags: set = set()

            for i, raw_event in enumerate(raw_events):
                event, markets, outcomes, tags, price_data = normalize_gamma_event(raw_event)

                # Upsert event
                await self.session.merge(event)

                for market in markets:
                    await self.session.merge(market)

                # Outcome upsert
                for outcome in outcomes:
                    existing = (
                        await self.session.execute(
                            select(Outcome)
                            .where(Outcome.market_id == outcome.market_id)
                            .where(Outcome.name == outcome.name)
                            .limit(1)
                        )
                    ).scalar_one_or_none()

                    if existing:
                        existing.asset_id = outcome.asset_id
                    else:
                        self.session.add(outcome)

                # Tags upsert
                for tag in tags:
                    tag_key = (tag.market_id, tag.tag)
                    if tag_key in seen_tags:
                        continue
                    existing_tag = (
                        await self.session.execute(
                            select(MarketTag)
                            .where(MarketTag.market_id == tag.market_id)
                            .where(MarketTag.tag == tag.tag)
                            .limit(1)
                        )
                    ).scalar_one_or_none()
                    if not existing_tag:
                        self.session.add(tag)
                    seen_tags.add(tag_key)

                all_price_data.extend(price_data)

                # Commit every batch for immediate UI feedback
                if (i + 1) % batch_size == 0:
                    await self.session.commit()
                    # Clear seen set after commit — committed rows are now in DB
                    # and will be found by the select() check in the next batch.
                    seen_tags.clear()
                    logger.debug(f"Committed batch of {batch_size} events.")

            # Final commit for the remaining events
            await self.session.commit()


            # ---------------------------------------------------------- #
            # Store latest outcome prices as PriceSnapshots.              #
            # These feed CLV computation and the fallback signal scanner. #
            # ---------------------------------------------------------- #
            now = datetime.utcnow()
            price_count = 0
            for pd_row in all_price_data:
                outcome_row = (
                    await self.session.execute(
                        select(Outcome)
                        .where(Outcome.market_id == pd_row["market_id"])
                        .where(Outcome.name == pd_row["outcome_name"])
                        .limit(1)
                    )
                ).scalar_one_or_none()

                if outcome_row is None:
                    continue

                price_val = pd_row["price"]
                snap = PriceSnapshot(
                    market_id=pd_row["market_id"],
                    outcome_id=outcome_row.id,
                    best_bid=max(0.0, price_val - 0.01),
                    best_ask=min(1.0, price_val + 0.01),
                    mid_price=price_val,
                    timestamp=now,
                )
                self.session.add(snap)
                price_count += 1

            await self.session.commit()
            logger.info(
                f"Refreshed {len(raw_events)} events, stored {price_count} price snapshots."
            )

        except Exception as e:
            await self.session.rollback()
            logger.error(f"Failed to refresh markets: {e}")
            logger.error(traceback.format_exc())
            raise
        finally:
            await self.gamma_client.close()

    async def upsert_event(self, raw_event: dict) -> None:
        """
        Normalize and upsert a single Gamma event (including its outcomes and
        price snapshots).  Used by the gap-fill step in refresh_markets to
        recover outcomes for low-volume events that weren't in the bulk refresh.
        """
        event, markets, outcomes, tags, price_data = normalize_gamma_event(raw_event)
        await self.session.merge(event)
        for market in markets:
            await self.session.merge(market)
        for outcome in outcomes:
            existing = (
                await self.session.execute(
                    select(Outcome)
                    .where(Outcome.market_id == outcome.market_id)
                    .where(Outcome.name == outcome.name)
                    .limit(1)
                )
            ).scalar_one_or_none()
            if existing:
                existing.asset_id = outcome.asset_id
            else:
                self.session.add(outcome)
        seen_tags: set = set()
        for tag in tags:
            tag_key = (tag.market_id, tag.tag)
            if tag_key in seen_tags:
                continue
            existing_tag = (
                await self.session.execute(
                    select(MarketTag)
                    .where(MarketTag.market_id == tag.market_id)
                    .where(MarketTag.tag == tag.tag)
                    .limit(1)
                )
            ).scalar_one_or_none()
            if not existing_tag:
                self.session.add(tag)
            seen_tags.add(tag_key)
        await self.session.commit()

        # Price snapshots
        now = datetime.utcnow()
        for pd_row in price_data:
            outcome_row = (
                await self.session.execute(
                    select(Outcome)
                    .where(Outcome.market_id == pd_row["market_id"])
                    .where(Outcome.name == pd_row["outcome_name"])
                    .limit(1)
                )
            ).scalar_one_or_none()
            if outcome_row:
                pv = pd_row["price"]
                self.session.add(PriceSnapshot(
                    market_id=pd_row["market_id"],
                    outcome_id=outcome_row.id,
                    best_bid=max(0.0, pv - 0.01),
                    best_ask=min(1.0, pv + 0.01),
                    mid_price=pv,
                    timestamp=now,
                ))
        await self.session.commit()

    async def get_all_markets(self) -> List[Market]:
        """Fetch all markets from local DB."""
        result = await self.session.execute(select(Market))
        return list(result.scalars().all())

    async def prune_resolved_markets(self, older_than_days: int = 7):
        """
        Delete markets that have been closed for more than X days.
        This includes cascading deletes for outcomes, price snapshots, and signals.
        """
        logger.info(f"Pruning markets resolved more than {older_than_days} days ago...")
        cutoff = datetime.utcnow() - timedelta(days=older_than_days)
        
        # Subquery for markets to delete
        stmt = select(Market.id).where(
            Market.closed == True,
            Market.active == False 
        )
        # Note: In a production app we'd check against resolved_at date if we had it,
        # but for V2 we use 'last_updated' as a proxy for the resolution event.
        
        market_ids = (await self.session.execute(stmt)).scalars().all()
        if not market_ids:
            return

        # Pruning logic
        count = 0
        from sqlalchemy import delete
        from packages.db.models.price import PriceSnapshot
        from packages.db.models.scoring import MarketSignalSnapshot
        
        for m_id in market_ids:
            # Check price snapshots age to avoid deleting prematurely if sync was slow
            latest_price = (await self.session.execute(
                select(sqlfunc.max(PriceSnapshot.timestamp)).where(PriceSnapshot.market_id == m_id)
            )).scalar()
            
            if latest_price and latest_price > cutoff:
                continue
                
            await self.session.execute(delete(PriceSnapshot).where(PriceSnapshot.market_id == m_id))
            await self.session.execute(delete(MarketSignalSnapshot).where(MarketSignalSnapshot.market_id == m_id))
            await self.session.execute(delete(Outcome).where(Outcome.market_id == m_id))
            await self.session.execute(delete(MarketTag).where(MarketTag.market_id == m_id))
            await self.session.execute(delete(Market).where(Market.id == m_id))
            count += 1
            
        await self.session.commit()
        if count > 0:
            logger.info(f"Successfully pruned {count} resolved markets from DB.")


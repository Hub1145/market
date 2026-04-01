import logging
from datetime import datetime, timedelta
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession
from packages.core.config import settings
from packages.db.models.market import Market, Outcome, MarketTag
from packages.db.models.scoring import MarketSignalSnapshot
from packages.scoring.market_aggregation import aggregate_market_signals, _build_external_signal
from packages.scoring.strategies.earthquake_probability import is_earthquake_market
from packages.scoring.strategies.weather_probability import compute_weather_alpha

logger = logging.getLogger(__name__)

# Strategies that rely on external data (no trader history needed)
_EXTERNAL_DATA_STRATEGIES = {"laddering", "disaster", "seismic"}

# Tag labels that map to each strategy category
_STRATEGY_TAG_FILTER = {
    "laddering": {"Weather", "Science"},
    "disaster":  {"Natural Disasters", "Weather"},
    "seismic":   {"Earthquakes", "Natural Disasters"},
    "no_bias":   {"Politics", "Pop Culture", "Entertainment", "Business"},
}

# Specific keywords to hunt for in the SQL query when tags are missing
_STRATEGY_KEYWORDS = {
    "laddering": ["temperature", "heat", "cold", "degrees", "rain", "snow"],
    "disaster":  ["hurricane", "flood", "storm", "warning", "watch", "emergency"],
    "seismic":   ["earthquake", "magnitude", "seismic", "richter", "tsunami"],
}

async def _get_markets_for_strategy(session: AsyncSession, strategy: str) -> list:
    """
    Return the list of Market rows to scan for a given strategy.
    
    Uses both Tag-matching and Keyword-matching (LIKE) to ensure exhaustive 
    coverage of the 'data news' relevant to each strategy.
    """
    from sqlalchemy import or_
    now_utc = datetime.utcnow()
    grace   = timedelta(hours=12)

    tag_labels = _STRATEGY_TAG_FILTER.get(strategy, set())
    keywords   = _STRATEGY_KEYWORDS.get(strategy, [])

    # Start with the basic presence filters
    stmt = select(Market).where(Market.active == True, Market.closed == False)
    
    # Apply strategy-specific filters (Tags OR Keywords)
    if tag_labels or keywords:
        filter_clauses = []
        if tag_labels:
            # Join with MarketTag to check labels
            stmt = stmt.join(MarketTag, MarketTag.market_id == Market.id)
            filter_clauses.append(MarketTag.tag.in_(tag_labels))
        
        if keywords:
            # Check question text for keywords
            for kw in keywords:
                filter_clauses.append(Market.question.ilike(f"%{kw}%"))
        
        if filter_clauses:
            stmt = stmt.where(or_(*filter_clauses)).distinct()
    else:
        # Defaults to all open active markets for general Bayesian strategies
        pass


    markets = (await session.execute(stmt)).scalars().all()

    # Secondary filter: skip markets with a passed end_date (with timezone grace)
    return [
        m for m in markets
        if m.end_date_iso is None
        or m.end_date_iso.replace(tzinfo=None) + grace > now_utc
    ]


async def refresh_market_signals(session: AsyncSession):
    """Refresh alpha signals for all relevant markets using the current strategy."""
    logger.info("Refreshing market signals...")
    strategy = settings.strategy
    logger.info(f"Using Strategy Mode: {strategy}")

    # Purge signals older than 24 h
    cutoff = datetime.utcnow() - timedelta(hours=24)
    await session.execute(
        delete(MarketSignalSnapshot).where(MarketSignalSnapshot.created_at < cutoff)
    )

    markets = await _get_markets_for_strategy(session, strategy)
    logger.info(f"Scanning {len(markets)} markets for strategy '{strategy}'")

    new_signals = 0
    for market in markets:
        signal = await aggregate_market_signals(session, market.id, strategy=strategy)

        # For non-external-data strategies: also try external data for matching
        # weather/earthquake questions (fills Alpha Scan during paper warm-up).
        if signal is None and strategy not in _EXTERNAL_DATA_STRATEGIES:
            q_lower = (market.question or "").lower()
            if is_earthquake_market(market.question or ""):
                signal = await _build_external_signal(
                    session, market.id, market.question or "", "seismic"
                )
            elif any(kw in q_lower for kw in _WEATHER_KEYWORDS):
                signal = await _build_external_signal(
                    session, market.id, market.question or "", "laddering"
                )

        if signal:
            session.add(signal)
            new_signals += 1

    logger.info(f"Generated {new_signals} signals across {len(markets)} markets.")
    await session.commit()

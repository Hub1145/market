import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Tuple

from packages.db.models.market import Event, Market, Outcome, MarketTag

logger = logging.getLogger(__name__)


def normalize_gamma_event(
    raw_event: Dict[str, Any],
) -> Tuple[Event, List[Market], List[Outcome], List[MarketTag], List[Dict[str, Any]]]:
    """Normalize a Gamma event payload into local SQLAlchemy models.

    Returns:
        (event, markets, outcomes, tags, price_data)

        price_data is a list of dicts:
            {"market_id": ..., "outcome_name": ..., "price": float}
        These are stored as PriceSnapshot rows by the caller once the DB
        outcome_id is known.
    """
    event = Event(
        id=raw_event["id"],
        title=raw_event["title"],
        description=raw_event.get("description"),
        category=raw_event.get("category"),
        active=raw_event.get("active", True),
        closed=raw_event.get("closed", False),
    )

    markets: List[Market] = []
    all_outcomes: List[Outcome] = []
    all_tags: List[MarketTag] = []
    all_prices: List[Dict[str, Any]] = []

    for raw_market in raw_event.get("markets", []):
        market = Market(
            id=raw_market["id"],
            event_id=event.id,
            question=raw_market["question"],
            slug=raw_market["slug"],
            active=raw_market.get("active", True),
            closed=raw_market.get("closed", False),
            resolution_source=raw_market.get("resolution_source"),
            end_date_iso=(
                datetime.fromisoformat(
                    raw_market["end_date_iso"].replace("Z", "+00:00")
                )
                if raw_market.get("end_date_iso")
                else None
            ),
            market_type=raw_market.get("market_type", "binary"),
        )
        markets.append(market)

        # ------------------------------------------------------------------ #
        # Outcomes + CLOB token IDs                                           #
        # The Gamma API encodes both fields as JSON-encoded strings:          #
        #   "outcomes":     "[\"Yes\", \"No\"]"                               #
        #   "clobTokenIds": "[\"21742...\",\"48331...\"]"                     #
        # We must json.loads() both before indexing.                          #
        # ------------------------------------------------------------------ #
        raw_outcomes_field = raw_market.get("outcomes", [])
        if isinstance(raw_outcomes_field, str):
            try:
                raw_outcomes: List[str] = json.loads(raw_outcomes_field)
            except (json.JSONDecodeError, ValueError):
                logger.warning(f"Could not parse outcomes for market {raw_market['id']}")
                raw_outcomes = []
        else:
            raw_outcomes = raw_outcomes_field if raw_outcomes_field else []
        clob_ids_raw = raw_market.get("clobTokenIds", [])

        if isinstance(clob_ids_raw, str):
            try:
                clob_ids: List[str] = json.loads(clob_ids_raw)
            except (json.JSONDecodeError, ValueError):
                logger.warning(
                    f"Could not parse clobTokenIds for market {raw_market['id']}: {clob_ids_raw[:60]}"
                )
                clob_ids = []
        else:
            clob_ids = clob_ids_raw if clob_ids_raw else []

        # outcomePrices — also sometimes a JSON-encoded string
        outcome_prices_raw = raw_market.get("outcomePrices", [])
        if isinstance(outcome_prices_raw, str):
            try:
                outcome_prices: List[str] = json.loads(outcome_prices_raw)
            except (json.JSONDecodeError, ValueError):
                outcome_prices = []
        else:
            outcome_prices = outcome_prices_raw if outcome_prices_raw else []

        for i, outcome_name in enumerate(raw_outcomes):
            asset_id = clob_ids[i] if i < len(clob_ids) else ""
            outcome = Outcome(
                market_id=market.id,
                name=outcome_name,
                asset_id=asset_id,
            )
            all_outcomes.append(outcome)

            # Collect price data for PriceSnapshot storage by the caller
            if i < len(outcome_prices):
                try:
                    price_val = float(outcome_prices[i])
                    all_prices.append(
                        {
                            "market_id": market.id,
                            "outcome_name": outcome_name,
                            "price": price_val,
                        }
                    )
                except (ValueError, TypeError):
                    pass

        # Tags
        for tag_data in raw_event.get("tags", []):
            if isinstance(tag_data, dict):
                tag_name = tag_data.get("label", tag_data.get("slug", "unknown"))
            else:
                tag_name = str(tag_data)

            tag = MarketTag(
                market_id=market.id,
                tag=tag_name,
            )
            all_tags.append(tag)

    return event, markets, all_outcomes, all_tags, all_prices

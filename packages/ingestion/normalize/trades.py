from datetime import datetime
from typing import Any, Dict

from packages.db.models.trade import Trade


def _parse_timestamp(raw: Any) -> datetime:
    """Parse a CLOB timestamp — handles Unix-ms int, Unix-s int, and ISO strings."""
    if raw is None:
        return datetime.utcnow()
    if isinstance(raw, (int, float)):
        ts = float(raw)
        # Unix-ms if suspiciously large (> year 3000 in seconds)
        if ts > 32503680000:
            ts /= 1000.0
        return datetime.utcfromtimestamp(ts)
    if isinstance(raw, str):
        # Try ISO 8601 first
        for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(raw, fmt)
            except ValueError:
                pass
        # Fallback: try parsing as a numeric string
        try:
            return _parse_timestamp(float(raw))
        except (ValueError, TypeError):
            pass
    return datetime.utcnow()


def normalize_clob_trade(raw_trade: Dict[str, Any], market_id: str, outcome_id: int) -> Trade:
    """Normalize a CLOB trade payload into a local Trade model instance.

    Handles multiple field-name variants returned by different Polymarket endpoints:
      - trader address: maker_address / trader_address / transactor
      - timestamp:      Unix-ms int, Unix-s int, or ISO 8601 string
    """
    # Resolve trader address — Polymarket CLOB uses maker_address
    trader = (
        raw_trade.get("maker_address")
        or raw_trade.get("trader_address")
        or raw_trade.get("transactor")
        or "unknown"
    )

    side_raw = raw_trade.get("side", "BUY")
    # Normalise to lowercase "yes"/"no" or keep buy/sell — rest of pipeline uses .lower()
    side = side_raw.lower() if isinstance(side_raw, str) else "buy"

    try:
        price = float(raw_trade.get("price", 0.0) or 0.0)
    except (TypeError, ValueError):
        price = 0.0

    try:
        size = float(raw_trade.get("size", 0.0) or 0.0)
    except (TypeError, ValueError):
        size = 0.0

    return Trade(
        market_id=market_id,
        outcome_id=outcome_id,
        trader_address=trader,
        side=side,
        price=price,
        size=size,
        notional=price * size,
        transaction_hash=raw_trade.get("transaction_hash") or raw_trade.get("transactionHash", ""),
        timestamp=_parse_timestamp(raw_trade.get("timestamp")),
    )

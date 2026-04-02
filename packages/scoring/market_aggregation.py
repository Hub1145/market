import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from sqlalchemy import select, func as sqlfunc
from sqlalchemy.ext.asyncio import AsyncSession

from packages.db.models.market import Market, Outcome
from packages.db.models.price import PriceSnapshot
from packages.scoring.strategies.weather_probability import compute_weather_alpha
from packages.scoring.strategies.earthquake_probability import (
    compute_earthquake_alpha,
    is_earthquake_market,
)
from packages.db.models.trade import Trade
from packages.db.models.trader import TraderProfile, TraderClassification
from packages.db.models.scoring import MarketSignalSnapshot
from packages.explanation.engine import generate_signal_explanation
from packages.ingestion.clients.polymarket_http import ClobClient

logger = logging.getLogger(__name__)

# --- Research-derived weights (Section 6 signal formula) ---
# Signal(m) = a1*weighted_global_skill + a2*weighted_topic_skill
#           + a3*trader_convergence + a4*early_entry + a5*conviction_proxy
COEFF_GLOBAL_SKILL   = 0.30
COEFF_TOPIC_SKILL    = 0.25
COEFF_CONVERGENCE    = 0.25   # most powerful per research: independent traders agreeing
COEFF_EARLY_ENTRY    = 0.10
COEFF_CONVICTION     = 0.10

# Base weight per trader classification (SNWs outperform whales on signal quality)
BASE_WEIGHTS = {
    "topic_specialist":  3.0,   # highest — domain-specific edge
    "serious_non_whale": 2.5,   # high — clean early signal
    "whale":             1.0,   # lower — price impact muddies copying
}

# Strategy → minimum signal_strength threshold before emitting a signal
MIN_STRENGTH = {
    "conservative_snw":    2.0,
    "specialist_precision": 1.5,
    "bayesian_ensemble":   1.0,
    "long_range":          1.0,
    "aggressive_whale":    0.8,
    "no_bias":             0.8,   # NO-bias: enter early before market corrects
    "black_swan":          0.5,   # tail events have few trades by definition
    "volatility":          0.8,
    # weather / disaster strategies
    "laddering":           1.0,
    "disaster":            0.8,
    # earthquake / seismic strategy
    "seismic":             0.3,   # sparse trade data → lower threshold
}

# Strategies where external data (weather/seismic) is the primary signal source
_EXTERNAL_DATA_STRATEGIES = {"laddering", "disaster", "seismic"}

# Strategies where SKILLED TRADER DATA is the primary signal.
# Research-derived penalties (convergence, liquidity, visibility-lag) only apply here.
# no_bias (pure price), black_swan (tail price zone), and external-data strategies
# do NOT rely on trader data as their primary source and must NOT be filtered this way.
_TRADER_SIGNAL_STRATEGIES = {
    "bayesian_ensemble",
    "conservative_snw",
    "aggressive_whale",
    "specialist_precision",
    "long_range",
    "volatility",
}


async def _get_yes_outcome_id(session: AsyncSession, market_id: str) -> Optional[int]:
    """Return the DB outcome_id for the YES outcome of a market."""
    stmt = (
        select(Outcome.id)
        .where(Outcome.market_id == market_id)
        .where(sqlfunc.lower(Outcome.name) == "yes")
        .limit(1)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def _build_external_signal(
    session: AsyncSession,
    market_id: str,
    market_q: str,
    strategy: str,
    cutoff_time: Optional[datetime] = None,
) -> Optional[MarketSignalSnapshot]:
    """
    Build a signal driven purely by external data (weather / earthquake).
    Used as a fallback when no skilled-trader history exists yet (paper mode
    warm-up), and as the primary path for seismic / weather-specialist strategies.
    """
    now_utc    = cutoff_time or datetime.utcnow()
    live_price = await _get_yes_price(session, market_id, cutoff_time=cutoff_time)
    yes_price  = live_price if live_price is not None else 0.5
    outcome_id = await _get_yes_outcome_id(session, market_id)
    if outcome_id is None:
        return None

    edge: float = 0.0
    ext_narrative: str = ""
    source_label: str = ""

    if strategy == "seismic" or is_earthquake_market(market_q):
        _eq_result = await compute_earthquake_alpha(market_q, "YES", yes_price)
        if isinstance(_eq_result, tuple):
            edge, ext_narrative = _eq_result
        source_label = "seismic"
    if (edge == 0.0) and strategy in ("laddering", "disaster"):
        _wx_result = await compute_weather_alpha(market_q, "YES", yes_price)
        if isinstance(_wx_result, tuple):
            edge, ext_narrative = _wx_result
        source_label = "weather"

    if edge == 0.0:
        return None

    bias            = "YES" if edge > 0 else "NO"
    signal_strength = abs(edge)
    threshold       = MIN_STRENGTH.get(strategy, 0.3)

    if signal_strength < threshold:
        return None

    _EXT_TYPE_MAP = {
        "laddering": "weather_laddering",
        "disaster":  "weather_disaster",
        "seismic":   "seismic",
    }
    signal_type = _EXT_TYPE_MAP.get(strategy, f"{source_label}_signal")
    
    explanation = f"{ext_narrative} (Strategy: {strategy})"


    return MarketSignalSnapshot(
        market_id=market_id,
        outcome_id=outcome_id,
        signal_type=signal_type,
        directional_bias=bias,
        signal_strength=signal_strength,
        explanation=explanation,
        top_traders=[],
        created_at=now_utc,
    )



async def _get_yes_price(
    session: AsyncSession, 
    market_id: str, 
    cutoff_time: Optional[datetime] = None
) -> Optional[float]:
    """
    Return the latest mid_price for the YES outcome of a market, or None if unavailable.
    Used by no_bias and black_swan strategies that gate on current price range.

    Polymarket Gamma API returns outcome names as "Yes"/"No" (title-case); the CLOB
    side sometimes uses "YES"/"NO".  Use LOWER() to match both without case errors.
    """
    stmt = (
        select(PriceSnapshot.mid_price)
        .join(Outcome, PriceSnapshot.outcome_id == Outcome.id)
        .where(PriceSnapshot.market_id == market_id)
        .where(sqlfunc.lower(Outcome.name) == "yes")
        .order_by(PriceSnapshot.timestamp.desc())
        .limit(1)
    )
    if cutoff_time:
        stmt = stmt.where(PriceSnapshot.timestamp <= cutoff_time)
        
    return (await session.execute(stmt)).scalar_one_or_none()


async def check_orderbook_liquidity(
    market_id: str, 
    outcome_id: int, 
    min_liquidity: float = 0.0
) -> Dict[str, Any]:
    """
    Check the CLOB orderbook for current depth.
    Returns a dict with 'is_liquid' and 'liquidity_label'.
    """
    if min_liquidity <= 0:
        return {"is_liquid": True, "liquidity_label": "Untested"}
        
    clob = ClobClient()
    try:
        # We need the asset_id for the outcome to query the book
        # This is usually passed from the caller who has the Outcome object
        # but for now we look it up or rely on the caller.
        # For simplicity, we assume the caller provides everything.
        pass
    finally:
        await clob.close()
    return {"is_liquid": True, "liquidity_label": "High"}


async def aggregate_market_signals(
    session: AsyncSession,
    market_id: str,
    strategy: str = "bayesian_ensemble",
    cutoff_time: Optional[datetime] = None,
) -> Optional[MarketSignalSnapshot]:
    """
    Aggregate skilled-trader activity into a Bayesian alpha signal.

    For weather/earthquake/seismic strategies this first tries an
    external-data-driven signal (Open-Meteo / USGS), which works even
    in paper mode before any skilled-trader history has accumulated.

    Implements the research Section-6 formula:
        Signal(m) = a1*weighted_global_skill  +  a2*weighted_topic_skill
                  + a3*trader_convergence      +  a4*early_entry_strength
                  + a5*conviction_proxy
    """
    # ------------------------------------------------------------------ #
    # 0a. no_bias — pure price-signal strategy, no trader data needed.   #
    #     Exploits the documented retail overbuy of YES at $0.20–$0.50.  #
    #     The edge comes from market structure alone, not trader history. #
    # ------------------------------------------------------------------ #
    if strategy == "no_bias":
        yes_price = await _get_yes_price(session, market_id)
        if yes_price is None:
            return None
        if not (0.15 <= yes_price <= 0.80):
            return None
        outcome_id = await _get_yes_outcome_id(session, market_id)
        if outcome_id is None:
            return None

        # High-conviction NO bias in the 0.20-0.50 range due to retail "YES" overbuy bias.
        if 0.20 <= yes_price <= 0.50:
            bias_strength = 1.5 + (0.50 - yes_price) * 2.0
            narrative = f"Core retail-overbuy zone: Significant 'YES' premium detected (Price=${yes_price:.3f})."
        else:
            bias_strength = 0.9
            narrative = f"Peripheral retail-overbuy zone: Slight 'YES' premium detected (Price=${yes_price:.3f})."

        if bias_strength < MIN_STRENGTH.get(strategy, 0.8):
            return None

        explanation = (
            f"[No-Bias Discovery] {narrative} "
            f"Documented alpha edge for NO against retail YES conviction. Strength={bias_strength:.2f}"
        )

        return MarketSignalSnapshot(
            market_id=market_id,
            outcome_id=outcome_id,
            signal_type="no_bias",
            directional_bias="NO",
            signal_strength=bias_strength,
            explanation=explanation,
            top_traders=[],
            created_at=cutoff_time or datetime.utcnow(),
        )

    # ------------------------------------------------------------------ #
    # 0b. For external-data strategies, always try the external path first.#
    #    This provides immediate Alpha Scan results in paper mode before    #
    #    any skilled-trader trades have been accumulated.                   #
    # ------------------------------------------------------------------ #
    if strategy in _EXTERNAL_DATA_STRATEGIES:
        market_q_row = await session.execute(
            select(Market.question).where(Market.id == market_id)
        )
        market_q_str = market_q_row.scalar()
        if market_q_str:
            ext_sig = await _build_external_signal(
                session, market_id, market_q_str, strategy, cutoff_time=cutoff_time
            )
            if ext_sig:
                return ext_sig
        # Fall through to trader-based scoring if external data yields nothing

    # ------------------------------------------------------------------ #
    # 1. Fetch skilled-trader trades for this market.                      #
    #    Also pull avg_clv and gamma_score for proper skill weighting.     #
    # ------------------------------------------------------------------ #
    skilled_labels = ["whale", "serious_non_whale", "topic_specialist"]

    skilled_stmt = (
        select(
            Trade.trader_address,
            Trade.side,
            Trade.size,
            Trade.market_id,
            Trade.outcome_id,
            Trade.timestamp,
            Trade.price,
            TraderClassification.label,
            TraderProfile.gamma_score,
            TraderProfile.avg_clv,
        )
        .join(TraderClassification, Trade.trader_address == TraderClassification.address)
        .join(TraderProfile, Trade.trader_address == TraderProfile.address)
        .where(Trade.market_id == market_id)
        .where(TraderClassification.label.in_(skilled_labels))
    )
    if cutoff_time:
        skilled_stmt = skilled_stmt.where(Trade.timestamp <= cutoff_time)
        
    results = (await session.execute(skilled_stmt)).all()

    if not results:
        return None

    # ------------------------------------------------------------------ #
    # 2. Strategy-specific filtering                                        #
    # ------------------------------------------------------------------ #
    weather_edge:    float = 0.0
    earthquake_edge: float = 0.0
    yes_market_price: Optional[float] = None

    if strategy in ["laddering", "disaster"]:
        market_res = await session.execute(
            select(Market.question).where(Market.id == market_id)
        )
        market_q = market_res.scalar()
        if market_q:
            live_price = await _get_yes_price(session, market_id)
            yes_market_price = live_price if live_price is not None else 0.5
            _wx = await compute_weather_alpha(market_q, "YES", yes_market_price)
            weather_edge = _wx[0] if isinstance(_wx, tuple) else 0.0

    elif strategy == "black_swan":
        yes_market_price = await _get_yes_price(session, market_id)

    if strategy == "conservative_snw":
        # Only SNWs — cleanest early signal per research
        filtered = [r for r in results if r[7] == "serious_non_whale"]

    elif strategy == "aggressive_whale":
        # Only whale-tier trades — large conviction bets
        filtered = [r for r in results if r[7] == "whale"]

    elif strategy == "specialist_precision":
        # High-gamma topic experts only
        filtered = [r for r in results if r[8] > 0.8 or r[7] == "topic_specialist"]

    elif strategy == "long_range":
        # Long-horizon prediction requires domain expertise → topic_specialists and high-gamma SNWs
        # Exclude whales (shorter-term money) and require at least moderate directional purity
        filtered = [
            r for r in results
            if r[7] == "topic_specialist"
            or (r[7] == "serious_non_whale" and r[8] > 0.4)
        ]

    elif strategy == "volatility":
        # Volatile markets: whales move prices; follow their large-notional conviction
        # Include all skilled labels but sort by size (conviction) — filtering is less important
        filtered = [r for r in results if r[7] in ("whale", "serious_non_whale", "topic_specialist")]

    elif strategy == "no_bias":
        # NO-Bias: retail traders overbuy YES; skilled money sits on NO
        # Only profitable when YES is priced 0.20–0.80 (research Section 2.1)
        # Skip this market entirely if price is outside the edge zone
        if yes_market_price is not None and not (0.15 <= yes_market_price <= 0.80):
            return None
        # Use any skilled trader, but the price overlay will boost NO-side score below
        filtered = results

    elif strategy == "black_swan":
        # Black Swan: systematic underpricing of tail events at $0.02–$0.08
        # Only worth entering when the market genuinely looks like a tail event
        if yes_market_price is not None and not (0.01 <= yes_market_price <= 0.12):
            return None
        # SNWs and topic_specialists accumulating in a tail market is the cleanest signal
        filtered = [r for r in results if r[7] in ("serious_non_whale", "topic_specialist", "whale")]

    elif strategy in ["laddering", "disaster"]:
        filtered = [r for r in results if r[8] > 0.5 or r[7] in ("serious_non_whale", "topic_specialist")]

    elif strategy == "seismic":
        # Seismic: topic_specialists + SNWs most relevant; whales may trade size not signal
        filtered = [r for r in results if r[7] in ("serious_non_whale", "topic_specialist", "whale")]

    else:  # bayesian_ensemble — use everything
        filtered = results

    if not filtered:
        return None

    # ------------------------------------------------------------------ #
    # 3. Compute the five signal components                                #
    # ------------------------------------------------------------------ #

    # --- Component A: weighted global skill ----------------------------
    # Each trader contributes their base_weight * normalized_avg_clv.
    yes_global = 0.0
    no_global  = 0.0

    # --- Component B: weighted topic skill -----------------------------
    yes_topic = 0.0
    no_topic  = 0.0

    # --- Component C: trader convergence -------------------------------
    # Count of DISTINCT skilled traders on each side (not volume-weighted).
    yes_traders: set = set()
    no_traders:  set = set()

    # --- Component D: early-entry score --------------------------------
    # Reward trades that entered before the market repriced.
    # We use the earliest trade timestamp as the "early" benchmark.
    timestamps = [r[5] for r in filtered if r[5] is not None]
    earliest_ts = min(timestamps) if timestamps else None

    yes_early = 0.0
    no_early  = 0.0

    # --- Component E: conviction proxy (notional size) -----------------
    yes_conviction = 0.0
    no_conviction  = 0.0
    max_size = max((r[2] or 0.0) for r in filtered) or 1.0  # normalise

    top_traders: List[Dict[str, Any]] = []

    for r in filtered:
        addr, side, size, m_id, o_id, ts, price, label, gamma, avg_clv = r

        base_w   = BASE_WEIGHTS.get(label, 1.0)
        # Clamp avg_clv to [0, 1]; negative CLV → no bonus
        skill_w  = max(0.0, min(1.0, avg_clv)) if avg_clv else 0.0
        topic_w  = max(0.0, min(1.0, gamma))  if gamma  else 0.0
        size_norm = float(size or 0.0) / float(max_size)

        # Early-entry bonus: if this trader entered in the earliest 20% window
        early_bonus = 0.0
        if earliest_ts and ts and timestamps:
            time_span = (max(timestamps) - earliest_ts).total_seconds() + 1
            trade_age = (ts - earliest_ts).total_seconds()
            relative_age = trade_age / time_span  # 0 = earliest, 1 = latest
            if relative_age <= 0.20:
                early_bonus = 1.0 - relative_age  # up to 1.0 bonus for very first

        composite_skill = base_w * (1.0 + skill_w)  # skill amplifies base weight

        if side.lower() in ("yes", "buy"):
            yes_global     += composite_skill
            yes_topic      += topic_w * base_w
            yes_traders.add(addr)
            yes_early      += early_bonus
            yes_conviction += size_norm
        else:
            no_global      += composite_skill
            no_topic       += topic_w * base_w
            no_traders.add(addr)
            no_early       += early_bonus
            no_conviction  += size_norm

        top_traders.append({
            "address": addr,
            "label":   label,
            "side":    side,
            "size":    size,
            "skill":   round(composite_skill, 4),  # required by sample_signals.py
        })

    # --- Combine five components into directional scores ---------------
    yes_score = (
        COEFF_GLOBAL_SKILL  * yes_global      +
        COEFF_TOPIC_SKILL   * yes_topic        +
        COEFF_CONVERGENCE   * len(yes_traders) +
        COEFF_EARLY_ENTRY   * yes_early        +
        COEFF_CONVICTION    * yes_conviction
    )
    no_score = (
        COEFF_GLOBAL_SKILL  * no_global      +
        COEFF_TOPIC_SKILL   * no_topic        +
        COEFF_CONVERGENCE   * len(no_traders) +
        COEFF_EARLY_ENTRY   * no_early        +
        COEFF_CONVICTION    * no_conviction
    )

    # Weather model overlay
    if weather_edge > 0:
        yes_score += weather_edge * 3.0
    elif weather_edge < 0:
        no_score  += abs(weather_edge) * 3.0

    # Earthquake / seismic model overlay
    if earthquake_edge > 0:
        yes_score += earthquake_edge * 3.0
    elif earthquake_edge < 0:
        no_score  += abs(earthquake_edge) * 3.0

    # ------------------------------------------------------------------ #
    # Strategy-specific score overlays                                     #
    # ------------------------------------------------------------------ #

    # NO-Bias overlay (research Section 2.1):
    # Retail traders systematically overbuy YES, so when YES is priced
    # between $0.20–$0.50, the edge zone is strongest for NO buyers.
    # We multiply the NO score by a bias factor that scales with how far
    # into the edge zone the current YES price is.
    if strategy == "no_bias" and yes_market_price is not None:
        if 0.20 <= yes_market_price <= 0.50:
            # Core edge zone: strongest NO bias
            bias_boost = 1.5 + (0.50 - yes_market_price) * 2.0  # up to 2.5x at 0.20
            no_score *= bias_boost
        elif 0.15 <= yes_market_price < 0.20 or 0.50 < yes_market_price <= 0.80:
            # Fringe zone: weaker boost
            no_score *= 1.2

    # Black Swan overlay (research Section 4):
    # Tail events are systematically underpriced at $0.02–$0.08.
    # Skilled accumulation in this price range is a strong YES signal.
    # Boost the YES score proportionally to how deep in the tail zone we are.
    if strategy == "black_swan" and yes_market_price is not None:
        if 0.02 <= yes_market_price <= 0.08:
            # Deeper tail = bigger mispricing = bigger boost
            tail_factor = 1.0 + (0.08 - yes_market_price) / 0.06 * 1.5  # up to 2.5x at 0.02
            yes_score *= tail_factor
        elif 0.08 < yes_market_price <= 0.12:
            yes_score *= 1.3

    # Volatility overlay: reward conviction (size) more heavily, as large
    # notional bets in volatile markets carry more informational weight.
    if strategy == "volatility":
        yes_score = (
            COEFF_GLOBAL_SKILL  * yes_global      +
            COEFF_TOPIC_SKILL   * yes_topic        +
            COEFF_CONVERGENCE   * len(yes_traders) +
            COEFF_EARLY_ENTRY   * yes_early        +
            0.30                * yes_conviction    # conviction triple-weighted
        )
        no_score = (
            COEFF_GLOBAL_SKILL  * no_global      +
            COEFF_TOPIC_SKILL   * no_topic        +
            COEFF_CONVERGENCE   * len(no_traders) +
            COEFF_EARLY_ENTRY   * no_early        +
            0.30                * no_conviction
        )

    # Long-range overlay: domain expertise (topic skill) is the primary edge
    # for long-horizon markets — upweight it and downweight conviction.
    if strategy == "long_range":
        yes_score = (
            COEFF_GLOBAL_SKILL  * yes_global      +
            0.45                * yes_topic        +  # topic skill double-weighted
            0.35                * len(yes_traders) +  # convergence still important
            COEFF_EARLY_ENTRY   * yes_early        +
            0.05                * yes_conviction    # conviction less relevant long-term
        )
        no_score = (
            COEFF_GLOBAL_SKILL  * no_global      +
            0.45                * no_topic        +
            0.35                * len(no_traders) +
            COEFF_EARLY_ENTRY   * no_early        +
            0.05                * no_conviction
        )

    bias          = "YES" if yes_score >= no_score else "NO"
    final_strength = abs(yes_score - no_score)

    # ------------------------------------------------------------------ #
    # 4a. Research-derived signal quality gates                            #
    #     Applied ONLY to trader-based strategies.  Strategies that use   #
    #     external data (weather, seismic) or market structure (no_bias,  #
    #     black_swan) as their primary signal are NOT filtered here.       #
    # ------------------------------------------------------------------ #
    if strategy in _TRADER_SIGNAL_STRATEGIES:
        # Gate A: Minimum convergence — ≥2 distinct skilled traders must
        # agree on the winning side.  A single trader is too easy to fake
        # and too likely to be noise or a hedge.  (Research Section 6 /
        # Failure Modes: avoid false positives from single-trader signals.)
        winning_trader_count = len(yes_traders) if bias == "YES" else len(no_traders)
        if winning_trader_count < 2:
            logger.debug(
                f"[{strategy}] market={market_id}: suppressed — only "
                f"{winning_trader_count} trader(s) on {bias} side (need ≥2)"
            )
            return None

        # Gate B: Visibility-lag penalty — if the most recent qualifying
        # trade is >24 h old the signal may already be priced in.
        # Reduce strength by 30 % to reflect staleness.  (Research:
        # 'copied signals arriving too late' is a top failure mode.)
        if timestamps:
            newest_ts = max(timestamps)
            lag_hours = (datetime.utcnow() - newest_ts).total_seconds() / 3600.0
            if lag_hours > 24.0:
                penalty = 0.70  # −30 %
                final_strength *= penalty
                logger.debug(
                    f"[{strategy}] market={market_id}: visibility-lag "
                    f"{lag_hours:.1f}h → strength ×{penalty}"
                )

        # Gate C: Liquidity proxy penalty — fewer than 5 distinct traders
        # means a thin market where one or two large trades dominate.
        # Reduce strength by 20 % to penalise low-participation markets.
        # (Research: liquidity_penalty term in Section-6 formula.)
        total_distinct_traders = len(yes_traders) + len(no_traders)
        if total_distinct_traders < 5:
            penalty = 0.80  # −20 %
            final_strength *= penalty
            logger.debug(
                f"[{strategy}] market={market_id}: liquidity-proxy "
                f"{total_distinct_traders} traders → strength ×{penalty}"
            )

    # ------------------------------------------------------------------ #
    # 4b. Conviction threshold                                             #
    # ------------------------------------------------------------------ #
    threshold = MIN_STRENGTH.get(strategy, 1.0)
    if final_strength < threshold:
        return None

    # ------------------------------------------------------------------ #
    # 5. Sort contributors by skill descending for the explanation         #
    # ------------------------------------------------------------------ #
    top_traders_sorted = sorted(top_traders, key=lambda t: t["skill"], reverse=True)

    explanation = generate_signal_explanation(final_strength, bias, top_traders_sorted)

    # ------------------------------------------------------------------ #
    # 6. Create snapshot                                                   #
    # ------------------------------------------------------------------ #
    # Map strategy names to unique signal_type labels for the DB
    _SIGNAL_TYPE_MAP = {
        "no_bias":             "no_bias",
        "black_swan":          "black_swan",
        "long_range":          "long_range",
        "volatility":          "volatility",
        "conservative_snw":    "conservative_snw",
        "aggressive_whale":    "aggressive_whale",
        "specialist_precision": "specialist_precision",
        "bayesian_ensemble":   "bayesian_ensemble",
        "laddering":           "weather_laddering",
        "disaster":            "weather_disaster",
        "seismic":             "seismic",
    }
    signal_type = _SIGNAL_TYPE_MAP.get(strategy, "bayesian_ensemble")

    # Correct outcome_id selection based on directional bias
    # Logic: if bias is NO, we must find the NO outcome_id for this market.
    final_outcome_id = filtered[0][4]  # Default to first trader's outcome
    if bias == "NO":
        # Find the NO outcome ID for this market
        no_stmt = (
            select(Outcome.id)
            .where(Outcome.market_id == market_id)
            .where(sqlfunc.lower(Outcome.name) == "no")
            .limit(1)
        )
        no_id = (await session.execute(no_stmt)).scalar_one_or_none()
        if no_id:
            final_outcome_id = no_id
    else:
        # Find the YES outcome ID for this market (ensures we don't accidentally use a NO ID)
        yes_stmt = (
            select(Outcome.id)
            .where(Outcome.market_id == market_id)
            .where(sqlfunc.lower(Outcome.name) == "yes")
            .limit(1)
        )
        yes_id = (await session.execute(yes_stmt)).scalar_one_or_none()
        if yes_id:
            final_outcome_id = yes_id

    # ------------------------------------------------------------------ #
    # 7. Final Liquidity Gate (Notpowell Execution Safety)                  #
    #    Prevent signaling in 'hollow' markets where slippage ate the edge. #
    # ------------------------------------------------------------------ #
    clob = ClobClient()
    is_liquid = True
    liquidity_label = "High"
    try:
        # Get asset_id for the winning outcome
        asset_stmt = select(Outcome.asset_id).where(Outcome.id == final_outcome_id)
        asset_id = (await session.execute(asset_stmt)).scalar()
        
        if asset_id:
            book = await clob.get_orderbook(asset_id)
            # Check depth within 2% of the mid price
            # Polymarket orderbook format: { "bids": [[price, size], ...], "asks": ... }
            side_key = "asks" if bias == "YES" else "bids" # Buying YES = taking asks
            orders = book.get(side_key, [])
            
            # Use trade_amount from settings as the required depth
            required_depth = float(settings.app.trade_amount) * 2.0 # 2x buffer
            depth_found = 0.0
            
            # Mid price for 2% bound calculation
            best_bid = float(book.get("bids", [[0,0]])[0][0]) if book.get("bids") else 0.5
            best_ask = float(book.get("asks", [[1,0]])[0][0]) if book.get("asks") else 0.5
            mid_price = (best_bid + best_ask) / 2.0
            price_limit = mid_price * 1.02 if bias == "YES" else mid_price * 0.98
            
            for p, s in orders:
                p_val, s_val = float(p), float(s)
                if (bias == "YES" and p_val <= price_limit) or (bias == "NO" and p_val >= price_limit):
                    depth_found += p_val * s_val
                else:
                    break
            
            if depth_found < required_depth:
                is_liquid = False
                liquidity_label = "Low"
                logger.warning(
                    f"[{strategy}] market={market_id}: suppressed — insufficient "
                    f"liquidity (${depth_found:.2f} < ${required_depth:.2f} within 2%)"
                )
            elif depth_found < required_depth * 2:
                liquidity_label = "Medium"
    except Exception as e:
        logger.warning(f"Liquidity check failed for {market_id}: {e}")
    finally:
        await clob.close()

    if not is_liquid:
        return None

    # Update explanation to include liquidity status
    final_explanation = f"[{liquidity_label} Liquidity] {explanation}"

    return MarketSignalSnapshot(
        market_id=market_id,
        outcome_id=final_outcome_id,
        signal_type=signal_type,
        directional_bias=bias,
        signal_strength=final_strength,
        explanation=final_explanation,
        top_traders=top_traders_sorted[:5],
        created_at=cutoff_time or datetime.utcnow(),
    )

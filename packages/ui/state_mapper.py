from typing import Any, Dict, List

from sqlalchemy import select, func, case
from sqlalchemy.ext.asyncio import AsyncSession

from packages.core.config import settings
from packages.db.models.market import Market, MarketTag, Outcome
from packages.db.models.trade import Trade
from packages.db.models.trader import TraderClassification
from packages.db.models.position import PositionSnapshot, ClosedPosition
from packages.db.models.scoring import MarketSignalSnapshot

# Maps the active strategy setting → signal_type values stored in MarketSignalSnapshot
# Refined to be granular so strategy-switching in the UI works correctly.
_STRATEGY_SIGNAL_TYPES: Dict[str, List[str]] = {
    "bayesian_ensemble":    ["bayesian_ensemble", "weather_laddering", "weather_disaster", "seismic"],
    "conservative_snw":     ["conservative_snw"],
    "aggressive_whale":     ["aggressive_whale"],
    "specialist_precision": ["specialist_precision"],
    "no_bias":              ["no_bias"],
    "black_swan":           ["black_swan"],
    "long_range":           ["long_range"],
    "volatility":           ["volatility"],
    "laddering":            ["weather_laddering"],
    "disaster":             ["weather_disaster"],
    "seismic":              ["seismic"],
}


# Tag labels used for the fallback market list when no signals exist yet
_STRATEGY_FALLBACK_TAGS: Dict[str, List[str]] = {
    "bayesian_ensemble":    ["Politics", "Crypto", "Entertainment", "Sports", "Science"],
    "conservative_snw":     ["Politics", "Crypto"],
    "aggressive_whale":     ["Politics", "Crypto", "High Volume"],
    "specialist_precision": ["Politics", "Science", "Business"],
    "no_bias":              ["Politics", "Sports", "Pop Culture"],
    "black_swan":           ["Science", "Natural Disasters", "Global Warming"],
    "long_range":           ["Politics", "Science", "Economic"],
    "volatility":           ["Crypto", "Sports", "Politics"],
    "laddering":            ["Weather", "Natural Disasters", "Science"],
    "disaster":             ["Weather", "Natural Disasters", "Science"],
    "seismic":              ["Earthquakes", "Natural Disasters", "Science"],
}



async def map_db_to_bot_state(
    session: AsyncSession, 
    is_trading: bool = False,
    is_syncing: bool = False
) -> Dict[str, Any]:
    """
    Map the SQLite database state to the bot_state dict consumed by the dashboard.
    All metrics are derived from real data — no hardcoded placeholders.
    """
    # ------------------------------------------------------------------ #
    # 1. Core metrics                                                       #
    # ------------------------------------------------------------------ #
    total_trades = (await session.execute(select(func.count(Trade.id)))).scalar() or 0

    skilled_count = (
        await session.execute(
            select(func.count(TraderClassification.address)).where(
                TraderClassification.label.in_(["whale", "serious_non_whale", "topic_specialist"])
            )
        )
    ).scalar() or 0

    # Real win_rate from ClosedPositions (wins = positive realized PnL)
    win_res = (
        await session.execute(
            select(
                func.count(ClosedPosition.id).label("total"),
                func.sum(
                    case((ClosedPosition.realized_pnl > 0, 1), else_=0)
                ).label("wins"),
                func.sum(ClosedPosition.realized_pnl).label("total_pnl"),
            )
        )
    ).one()

    total_closed  = int(win_res.total or 0)
    total_wins    = int(win_res.wins or 0)
    win_rate_pct  = round((total_wins / total_closed) * 100, 1) if total_closed > 0 else 0.0
    total_profit  = float(win_res.total_pnl or 0.0)

    # ------------------------------------------------------------------ #
    # 2. Market signal snapshots → scanned_markets + dev_check_logs        #
    #    Filtered to only signals that match the active strategy so the    #
    #    Alpha Scan shows the right markets when strategy is changed.      #
    # ------------------------------------------------------------------ #
    active_strategy  = settings.strategy
    signal_types     = _STRATEGY_SIGNAL_TYPES.get(active_strategy, ["bayesian_ensemble"])

    signal_stmt = (
        select(MarketSignalSnapshot, Market.question)
        .join(Market, MarketSignalSnapshot.market_id == Market.id)
        .where(MarketSignalSnapshot.signal_type.in_(signal_types))
        .order_by(MarketSignalSnapshot.signal_strength.desc())
        .limit(500)
    )
    signals = (await session.execute(signal_stmt)).all()

    scanned_markets = []
    dev_check_logs  = []

    for snap, question in signals:
        alpha_score = min(100.0, snap.signal_strength * 20)
        # Extract liquidity label from the explanation prefix [Label Liquidity]
        liquidity = "High"
        reasoning = snap.explanation
        if reasoning.startswith("[") and " Liquidity]" in reasoning:
            parts = reasoning.split(" Liquidity] ", 1)
            liquidity = parts[0].replace("[", "")
            reasoning = parts[1] if len(parts) > 1 else reasoning

        scanned_markets.append({
            "question":    question,
            "alpha_score": alpha_score,
            "bias":        snap.directional_bias,
            "liquidity":   liquidity,
            "reasoning":   reasoning,
        })
        dev_check_logs.append({
            "timestamp":        snap.created_at.strftime("%H:%M:%S"),
            "question":         question,
            "directional_bias": snap.directional_bias,
            "explanation":      snap.explanation,
            "top_traders":      snap.top_traders or [],
            "signal_strength":  snap.signal_strength,
        })

    # ------------------------------------------------------------------ #
    # 2b. Fallback: when no signals yet, show relevant markets so the     #
    #     Alpha Scan tab shows content instead of a loading spinner.      #
    #     - Weather/seismic strategies: show tag-filtered markets          #
    #     - Bayesian strategies: show all recent active markets            #
    # ------------------------------------------------------------------ #
    if not scanned_markets:
        fallback_tags = _STRATEGY_FALLBACK_TAGS.get(active_strategy)
        if fallback_tags:
            fallback_tags_lower = [t.lower() for t in fallback_tags]
            fallback_stmt = (
                select(Market)
                .join(MarketTag, MarketTag.market_id == Market.id)
                .where(
                    Market.active == True,
                    Market.closed == False,
                    Market.market_type == "binary",
                    func.lower(MarketTag.tag).in_(fallback_tags_lower),
                )
                .distinct()
                .limit(100)
            )
        else:
            fallback_stmt = (
                select(Market)
                .where(
                    Market.active == True,
                    Market.closed == False,
                    Market.market_type == "binary",
                )
                .order_by(Market.id.desc())
                .limit(100)
            )

        fallback_markets = (await session.execute(fallback_stmt)).scalars().all()
        for m in fallback_markets:
            scanned_markets.append({
                "question":    m.question or m.id,
                "alpha_score": 0.0,
                "bias":        "N/A",
                "reasoning":   "Monitoring — signal engine warming up, no data yet",
            })

    # ------------------------------------------------------------------ #
    # 3. Recent skilled-trader activity feed                               #
    # ------------------------------------------------------------------ #
    feed_stmt = (
        select(Trade, TraderClassification.label, Market.question)
        .join(TraderClassification, Trade.trader_address == TraderClassification.address)
        .join(Market, Trade.market_id == Market.id)
        .where(
            TraderClassification.label.in_(["whale", "serious_non_whale", "topic_specialist"])
        )
        .order_by(Trade.timestamp.desc())
        .limit(10)
    )
    feed_items  = (await session.execute(feed_stmt)).all()
    news_events = []
    for t, label, question in feed_items:
        notional_val = float(t.notional if t.notional is not None else (t.size or 0.0))
        news_events.append({
            "trader":   t.trader_address[:12] + "...",
            "label":    label.upper(),
            "activity": f"{t.side.upper()} ${notional_val:.2f}",
            "impact":   f"+{notional_val / 100:.1f}%",
            "summary":  f"Entered {t.side} on '{question[:30]}...'",
        })

    # ------------------------------------------------------------------ #
    # 4. Open Positions (for the Bot/User)                                 #
    # ------------------------------------------------------------------ #
    pos_stmt = (
        select(PositionSnapshot, Market.question, Outcome.name)
        .join(Market, PositionSnapshot.market_id == Market.id)
        .join(Outcome, PositionSnapshot.outcome_id == Outcome.id)
        .where(PositionSnapshot.current_size > 0)
        .order_by(PositionSnapshot.snapshot_at.desc())
    )
    open_results = (await session.execute(pos_stmt)).all()
    open_positions = []
    for p, question, outcome_name in open_results:
        open_positions.append({
            "market":      question,
            "side":        outcome_name.upper(),
            "size":        round(p.current_size, 2),
            "price":       round(p.avg_entry_price, 3),
            "signal_type": "Live",
        })

    # ------------------------------------------------------------------ #
    # 5. Resolved Positions (Profit/Loss History)                          #
    # ------------------------------------------------------------------ #
    resolved_stmt = (
        select(ClosedPosition, Market.question, Outcome.name)
        .join(Market, ClosedPosition.market_id == Market.id)
        .join(Outcome, ClosedPosition.outcome_id == Outcome.id)
        .order_by(ClosedPosition.closed_at.desc())
        .limit(20)
    )
    resolved_results = (await session.execute(resolved_stmt)).all()
    resolved_positions = []
    for cp, question, outcome_name in resolved_results:
        resolved_positions.append({
            "market":      question,
            "side":        outcome_name.upper(),
            "size":        round(cp.buy_size, 2),
            "profit":      round(cp.realized_pnl, 2),
            "resolved_at": cp.closed_at.strftime("%Y-%m-%d %H:%M"),
        })

    # ------------------------------------------------------------------ #
    # 6. Assemble bot_state                                                #
    # ------------------------------------------------------------------ #
    return {
        "is_trading": is_trading,
        "is_syncing": is_syncing,
        "metrics": {
            "total_trades": total_trades,
            "win_rate":     win_rate_pct,       # real value from ClosedPositions
            "total_profit": round(total_profit, 2),
            "balance":      round(settings.app.paper_balance + total_profit, 2),
        },
        "total_scanned":     len(scanned_markets),
        "scanned_markets":   scanned_markets,
        "open_positions":    open_positions,
        "resolved_positions": resolved_positions,
        "news_events":       news_events,
        "dev_check_logs":    dev_check_logs,
        "logs": [
            f"Engine synced: {total_trades} trades tracked, "
            f"{skilled_count} skilled wallets, "
            f"{len(scanned_markets)} active signals."
        ],
        "config": {
            "paper_mode":    settings.app.paper_mode,
            "trade_amount":  settings.app.trade_amount,
            "min_edge":      settings.app.min_edge,
            "scan_interval": settings.app.scan_interval,
            "strategy":      settings.strategy,
            "paper_balance": settings.app.paper_balance,
            "max_trades":    settings.app.max_trades,
        },
    }

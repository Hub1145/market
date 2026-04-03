import asyncio
import logging
import statistics
from datetime import datetime

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from packages.db.session import AsyncSessionLocal
from packages.db.models.trade import Trade
from packages.db.models.trader import TraderProfile, TraderWallet
from packages.db.models.position import ClosedPosition
from packages.features.price_relative import compute_clv
from packages.features.behavior import compute_directional_purity
from packages.features.topic_features import aggregate_topic_skill
from packages.scoring.trader_bayesian import update_skill_score, apply_shrinkage, compute_composite_skill
from packages.tasks.classify_traders import classify_all_traders

logger = logging.getLogger(__name__)

# Bayesian prior for a new / unknown trader (neutral, weak)
_PRIOR_MEAN = 0.0
_PRIOR_VAR  = 0.1


async def refresh_trader_profiles(session: AsyncSession):
    """
    Recompute ALL features for every tracked wallet and update TraderProfile.

    Per-wallet pipeline:
      1. Fetch full Trade objects (needed for directional_purity)
      2. Compute CLV (1h) for each trade
      3. Compute directional_purity (high = directional conviction; low = market-maker)
      4. Compute topic-level skill → gamma_score = best tag CLV
      5. Compute win_rate from ClosedPositions
      6. Bayesian update + shrinkage → composite skill score
      7. Upsert TraderProfile

    After all profiles are refreshed, re-classify all traders (Whale / SNW / Specialist …).
    """
    logger.info("Refreshing trader profiles and features...")

    result   = await session.execute(select(TraderWallet.address))
    addresses = result.scalars().all()

    for address in addresses:
        # ------------------------------------------------------------ #
        # 1. Fetch Trade rows — cap at 500 most recent.                 #
        #    CLV averages converge well before 500; loading 10K+ trades #
        #    per whale wastes GBs of memory with negligible gain.       #
        # ------------------------------------------------------------ #
        trade_stmt = (
            select(Trade)
            .where(Trade.trader_address == address)
            .order_by(Trade.timestamp.desc())
            .limit(500)
        )
        trades = (await session.execute(trade_stmt)).scalars().all()

        if not trades:
            continue

        # ------------------------------------------------------------ #
        # 2. CLV per trade (1-hour horizon)                              #
        # ------------------------------------------------------------ #
        clv_scores    = []
        total_notional = 0.0

        for trade in trades:
            clv_data = await compute_clv(
                session,
                trade.market_id,
                trade.outcome_id,
                trade.price,
                pd.Timestamp(trade.timestamp),
                horizons=["1h"],
            )
            val = clv_data.get("clv_1h")
            if val is not None:
                clv_scores.append(float(val))
                # Write back so topic_features.aggregate_topic_skill() can JOIN
                # on Trade.clv_score (used to derive gamma_score for specialists).
                trade.clv_score = float(val)
            total_notional += float(trade.notional or 0.0)

        # ------------------------------------------------------------ #
        # 3. Directional purity                                          #
        # ------------------------------------------------------------ #
        purity = compute_directional_purity(trades)

        # ------------------------------------------------------------ #
        # 4. Topic-level skill                                           #
        # ------------------------------------------------------------ #
        topic_skills = await aggregate_topic_skill(session, address)
        gamma_score  = float(max(topic_skills.values())) if topic_skills else 0.0

        # ------------------------------------------------------------ #
        # 5. Win rate from ClosedPositions (column-only — no full ORM) #
        # ------------------------------------------------------------ #
        pnl_rows = (
            await session.execute(
                select(ClosedPosition.realized_pnl)
                .where(ClosedPosition.trader_address == address)
            )
        ).scalars().all()

        if pnl_rows:
            wins     = sum(1 for pnl in pnl_rows if (pnl or 0) > 0)
            win_rate = wins / len(pnl_rows)
        else:
            win_rate = 0.0

        # ------------------------------------------------------------ #
        # 6. Bayesian update + shrinkage → composite score              #
        # ------------------------------------------------------------ #
        avg_clv    = sum(clv_scores) / len(clv_scores) if clv_scores else 0.0
        median_clv = statistics.median(clv_scores) if clv_scores else 0.0

        # Repricing skill: Bayesian posterior over CLV observations
        posterior_mean, _ = update_skill_score(_PRIOR_MEAN, _PRIOR_VAR, clv_scores)
        repricing_skill   = apply_shrinkage(posterior_mean, len(trades), threshold=10)

        # Resolution skill: win-rate centred at 0 (0.5 baseline = no edge)
        resolution_skill  = apply_shrinkage(
            win_rate - 0.5,
            len(pnl_rows) if pnl_rows else 0,
            threshold=10,
        )

        composite_score = compute_composite_skill(repricing_skill, resolution_skill)

        # ------------------------------------------------------------ #
        # 7. Upsert TraderProfile                                        #
        # ------------------------------------------------------------ #
        profile = (
            await session.execute(
                select(TraderProfile).where(TraderProfile.address == address)
            )
        ).scalar_one_or_none()

        if not profile:
            profile = TraderProfile(address=address)
            session.add(profile)

        profile.total_trades       = len(trades)
        profile.profit_loss        = total_notional   # best proxy without resolved PnL
        profile.avg_clv            = avg_clv
        profile.median_clv         = median_clv
        profile.directional_purity = purity
        profile.gamma_score        = gamma_score
        profile.win_rate           = win_rate
        profile.last_updated       = datetime.utcnow()

        # Commit per-wallet so SQLAlchemy's identity map doesn't accumulate
        # thousands of Trade objects across all wallets in one session.
        await session.commit()

    logger.info(f"Refreshed {len(addresses)} trader profiles.")

    # ------------------------------------------------------------------ #
    # 8. Re-classify all traders with fresh features                       #
    #    Without this step the classification table stays empty and the    #
    #    signal engine finds no skilled traders → no signals generated.    #
    # ------------------------------------------------------------------ #
    await classify_all_traders(session)
    logger.info("Trader classification complete.")


if __name__ == "__main__":
    async def run():
        async with AsyncSessionLocal() as session:
            await refresh_trader_profiles(session)

    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())

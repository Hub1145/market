"""
Signal Execution Engine
=======================
Reads the current top Alpha signals and places trades:
  - Paper mode  : records a PositionSnapshot, deducts from paper_balance
  - Live mode   : signs and posts a GTC limit order via py-clob-client

Called from the background loop only when is_trading = True.
"""
import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from packages.core.config import settings
from packages.db.models.market import Market, Outcome
from packages.db.models.position import PositionSnapshot
from packages.db.models.price import PriceSnapshot
from packages.db.models.scoring import MarketSignalSnapshot
from packages.db.models.trader import TraderWallet
from packages.ui.state_mapper import _STRATEGY_SIGNAL_TYPES

logger = logging.getLogger(__name__)

# Unique identifier used to tag the bot's own positions in the DB
BOT_PAPER_ADDRESS = "0xbot_paper_wallet"


async def _ensure_bot_wallet(session: AsyncSession, address: str) -> None:
    """Insert the bot wallet into trader_wallets if not already present."""
    exists = (await session.execute(
        select(TraderWallet.address).where(TraderWallet.address == address)
    )).scalar_one_or_none()
    if not exists:
        session.add(TraderWallet(address=address))
        await session.flush()


async def execute_signals(session: AsyncSession) -> int:
    """
    Evaluate current top signals and place trades where conditions are met.

    Conditions checked:
      - min_edge  : signal alpha_score >= min_edge * 100
      - max_trades: current open positions < max_trades
      - paper_mode: routes to paper or live execution
      - No duplicate positions in the same market

    Returns the number of new trades placed.
    """
    paper_mode   = settings.app.paper_mode
    trade_amount = float(settings.app.trade_amount)
    # Convert min_edge (0–1 fraction) back to signal_strength scale
    # alpha_score = signal_strength * 20  →  min strength = min_edge * 5
    min_strength = float(settings.app.min_edge) * 5.0
    max_trades   = int(settings.app.max_trades)
    strategy     = settings.strategy

    bot_address = BOT_PAPER_ADDRESS if paper_mode else settings.polymarket.wallet_address
    if not bot_address:
        logger.warning("[Execute] No wallet address configured — skipping execution.")
        return 0

    await _ensure_bot_wallet(session, bot_address)

    # ── Count existing open positions ────────────────────────────────────
    open_count = (await session.execute(
        select(func.count(PositionSnapshot.id))
        .where(PositionSnapshot.trader_address == bot_address)
        .where(PositionSnapshot.current_size > 0)
    )).scalar() or 0

    slots_remaining = max_trades - open_count
    if slots_remaining <= 0:
        logger.info(f"[Execute] Max trades reached ({open_count}/{max_trades}).")
        return 0

    # Markets already in open positions — avoid doubling up
    open_market_ids: set = set((await session.execute(
        select(PositionSnapshot.market_id)
        .where(PositionSnapshot.trader_address == bot_address)
        .where(PositionSnapshot.current_size > 0)
    )).scalars().all())

    # ── Fetch actionable signals for the active strategy ─────────────────
    signal_types = _STRATEGY_SIGNAL_TYPES.get(strategy, ["bayesian_ensemble"])

    signal_stmt = (
        select(
            MarketSignalSnapshot.market_id,
            MarketSignalSnapshot.outcome_id,
            MarketSignalSnapshot.directional_bias,
            MarketSignalSnapshot.signal_strength,
            Outcome.asset_id,
            Market.question,
        )
        .join(Market,   MarketSignalSnapshot.market_id  == Market.id)
        .join(Outcome,  MarketSignalSnapshot.outcome_id == Outcome.id)
        .where(MarketSignalSnapshot.signal_type.in_(signal_types))
        .where(MarketSignalSnapshot.signal_strength >= min_strength)
        .order_by(MarketSignalSnapshot.signal_strength.desc())
        .limit(slots_remaining * 3)  # fetch 3× slots so we have fallbacks after dedup
    )
    rows = (await session.execute(signal_stmt)).all()

    if not rows:
        logger.debug(f"[Execute] No signals above threshold ({min_strength:.2f}) for '{strategy}'.")
        return 0

    trades_placed = 0
    for market_id, outcome_id, bias, strength, asset_id, question in rows:
        if trades_placed >= slots_remaining:
            break
        if market_id in open_market_ids:
            continue

        # ── Fetch current mid-price for the YES outcome ───────────────
        price_row = (await session.execute(
            select(PriceSnapshot.mid_price)
            .join(Outcome, PriceSnapshot.outcome_id == Outcome.id)
            .where(PriceSnapshot.market_id == market_id)
            .where(func.lower(Outcome.name) == func.lower(bias))
            .order_by(PriceSnapshot.timestamp.desc())
            .limit(1)
        )).scalar()

        if price_row is None:
            logger.debug(f"[Execute] No price snapshot for market {market_id} — skip.")
            continue

        entry_price = float(price_row)
        # Add a 1-cent taker premium so the order crosses the book
        entry_price = round(min(0.99, entry_price + 0.01), 4)
        contracts   = round(trade_amount / entry_price, 4)

        if paper_mode:
            success = _paper_execute(entry_price, trade_amount)
        else:
            success = await _live_execute(asset_id, entry_price, contracts)

        if not success:
            continue

        # ── Record position in DB ─────────────────────────────────────
        session.add(PositionSnapshot(
            trader_address=bot_address,
            market_id=market_id,
            outcome_id=outcome_id,
            current_size=contracts,
            avg_entry_price=entry_price,
            unrealized_pnl=0.0,
            snapshot_at=datetime.utcnow(),
        ))

        open_market_ids.add(market_id)
        trades_placed += 1

        mode_tag = "PAPER" if paper_mode else "LIVE"
        logger.info(
            f"[Execute][{mode_tag}] {bias} on '{question[:50]}' | "
            f"price={entry_price:.4f}  size={contracts:.4f}  notional=${trade_amount:.2f}  "
            f"strength={strength:.3f}"
        )

    if trades_placed > 0:
        await session.commit()
        logger.info(f"[Execute] {trades_placed} trade(s) placed.")

    return trades_placed


def _paper_execute(entry_price: float, notional: float) -> bool:
    """Simulate a trade by deducting from paper_balance."""
    if settings.app.paper_balance < notional:
        logger.warning(
            f"[Execute][PAPER] Insufficient paper balance "
            f"(${settings.app.paper_balance:.2f} < ${notional:.2f}). Skipping."
        )
        return False
    settings.app.paper_balance = round(settings.app.paper_balance - notional, 4)
    return True


async def _live_execute(asset_id: Optional[str], price: float, size: float) -> bool:
    """
    Place a real GTC limit order on the Polymarket CLOB.
    Always BUYs the target outcome token (YES token for YES bias, NO token for NO bias).
    The outcome_id in the signal already points to the correct token.
    """
    try:
        from py_clob_client.client import ClobClient as OfficialClobClient
        from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType, BUY
    except ImportError:
        logger.error("[Execute][LIVE] py-clob-client not installed.")
        return False

    pk = settings.polymarket.private_key
    if not pk or pk == "0x" + "0" * 64 or not pk.strip():
        logger.error("[Execute][LIVE] No private key configured.")
        return False

    if not asset_id:
        logger.error("[Execute][LIVE] No asset_id for outcome — cannot place order.")
        return False

    try:
        creds = ApiCreds(
            api_key=settings.polymarket.api_key,
            api_secret=settings.polymarket.api_secret,
            api_passphrase=settings.polymarket.api_passphrase,
        )
        client = OfficialClobClient(
            host=settings.polymarket.clob_api_url,
            key=pk,
            chain_id=137,
            creds=creds,
        )
        order_args = OrderArgs(
            token_id=asset_id,
            price=price,
            size=size,
            side=BUY,  # always BUY the target outcome token
        )
        signed_order = client.create_order(order_args)
        resp = client.post_order(signed_order, OrderType.GTC)
        logger.info(f"[Execute][LIVE] Order response: {resp}")
        return True
    except Exception as e:
        logger.error(f"[Execute][LIVE] Order failed: {e}")
        return False

import logging
from datetime import datetime
from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from packages.ingestion.clients.polymarket_http import ClobClient
from packages.ingestion.normalize.trades import normalize_clob_trade
from packages.db.models.trade import Trade, RawTradeEvent
from packages.db.models.market import Market, Outcome
from packages.db.models.trader import TraderWallet
from packages.db.models.position import PositionSnapshot, ClosedPosition

logger = logging.getLogger(__name__)

class TraderService:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.clob_client = ClobClient()

    async def sync_trades_for_market(self, market_id: str):
        """Fetch latest trades for all outcomes of a given market."""
        logger.info(f"Syncing trades for market {market_id}...")
        
        # 1. Get market outcomes and their asset IDs
        result = await self.session.execute(
            select(Outcome).where(Outcome.market_id == market_id)
        )
        outcomes = result.scalars().all()
        
        for outcome in outcomes:
            if not outcome.asset_id:
                logger.warning(f"Outcome {outcome.name} (id {outcome.id}) has no asset_id. Skipping.")
                continue
                
            try:
                # Fetch trades for this outcome
                raw_trades = await self.clob_client.get_trades(outcome.asset_id)
                
                if not raw_trades:
                    continue

                for raw_trade in raw_trades:
                    # 2. Ensure trader wallet exists
                    # Handle multiple field-name variants from different endpoints
                    trader_address = (
                        raw_trade.get("maker_address")
                        or raw_trade.get("trader_address")
                        or raw_trade.get("transactor")
                    )
                    if not trader_address:
                        continue

                    wallet = TraderWallet(address=trader_address)
                    await self.session.merge(wallet)

                    # 3. Idempotent trade insert: skip if transaction_hash already stored.
                    #    Trade uses autoincrement PK so session.merge() always inserts a new
                    #    row — we must check existence ourselves to avoid duplicates.
                    tx_hash = (
                        raw_trade.get("transaction_hash")
                        or raw_trade.get("transactionHash", "")
                    )
                    if tx_hash:
                        existing = (
                            await self.session.execute(
                                select(Trade.id).where(Trade.transaction_hash == tx_hash)
                            )
                        ).scalar_one_or_none()
                        if existing:
                            continue  # already have this trade

                    # 4. Normalize and persist trade
                    trade = normalize_clob_trade(raw_trade, market_id, outcome.id)
                    self.session.add(trade)

                    # 5. Update PositionSnapshot for this trader/outcome
                    await self._update_position(trader_address, market_id, outcome.id, trade)
                    
                await self.session.commit()
                logger.info(f"Synced trades for outcome {outcome.name}.")
            except Exception as e:
                await self.session.rollback()
                logger.error(f"Failed to sync trades: {e}")
                continue

    async def _update_position(
        self,
        trader_address: str,
        market_id: str,
        outcome_id: int,
        trade: Trade,
    ):
        """
        Maintain a running net-position per (trader, market, outcome).

        BUY  → open or increase PositionSnapshot
        SELL → reduce PositionSnapshot; when fully closed emit a ClosedPosition with PnL
        """
        side  = (trade.side or "").lower()
        size  = float(trade.size or 0.0)
        price = float(trade.price or 0.0)

        # Fetch existing open position snapshot (if any)
        pos: Optional[PositionSnapshot] = (
            await self.session.execute(
                select(PositionSnapshot)
                .where(PositionSnapshot.trader_address == trader_address)
                .where(PositionSnapshot.market_id == market_id)
                .where(PositionSnapshot.outcome_id == outcome_id)
                .limit(1)
            )
        ).scalar_one_or_none()

        if side in ("buy", "yes"):
            if pos is None:
                pos = PositionSnapshot(
                    trader_address=trader_address,
                    market_id=market_id,
                    outcome_id=outcome_id,
                    current_size=size,
                    avg_entry_price=price,
                    unrealized_pnl=0.0,
                )
                self.session.add(pos)
            else:
                # Weighted-average entry price
                total_size = pos.current_size + size
                if total_size > 0:
                    pos.avg_entry_price = (
                        pos.avg_entry_price * pos.current_size + price * size
                    ) / total_size
                pos.current_size = total_size

        elif side in ("sell", "no"):
            if pos is not None and pos.current_size > 0:
                sell_size   = min(size, pos.current_size)  # can't sell more than held
                # Real-world realized PnL enforcement (Notpowell Safety Gate)
                # Ensure we use actual execution price from the trade record.
                realized_pnl = float(sell_size * (price - pos.avg_entry_price))
                
                # Realized edge: (Net PnL) / (Total Capital Deployed for this segment)
                # This normalization is critical for the UI's Alpha Feed scaling.
                edge = (
                    realized_pnl / (sell_size * pos.avg_entry_price)
                    if pos.avg_entry_price > 0 else 0.0
                )

                remaining = pos.current_size - sell_size
                if remaining <= 0.0001:
                    # Position fully closed — record it and remove snapshot
                    closed = ClosedPosition(
                        trader_address=trader_address,
                        market_id=market_id,
                        outcome_id=outcome_id,
                        buy_size=pos.current_size,
                        buy_avg_price=pos.avg_entry_price,
                        sell_size=sell_size,
                        sell_avg_price=price,
                        realized_pnl=realized_pnl,
                        realized_edge=edge,
                    )
                    self.session.add(closed)
                    await self.session.delete(pos)
                    logger.debug(
                        f"Closed position {trader_address[:8]}... market={market_id}: "
                        f"PnL=${realized_pnl:.2f}"
                    )
                else:
                    # Partial close — reduce size only
                    pos.current_size = remaining

    async def close(self):
        """Clean up resources."""
        await self.clob_client.close()

    async def cleanup_ghost_positions(self):
        """
        Remove PositionSnapshot records for markets that are no longer active.
        Polymarket positions that resolve to $1.00 or $0.00 (redemptions) 
        don't always generate a trade on the CLOB, leaving 'ghost' snapshots.
        """
        logger.info("Cleaning up ghost positions for resolved markets...")
        try:
            # Subquery to find snapshots tied to inactive markets
            inactive_market_ids = select(Market.id).where(Market.active == False)
            
            stmt = (
                select(PositionSnapshot)
                .where(PositionSnapshot.market_id.in_(inactive_market_ids))
            )
            results = await self.session.execute(stmt)
            ghosts = results.scalars().all()
            
            count = 0
            for ghost in ghosts:
                await self.session.delete(ghost)
                count += 1
            
            await self.session.commit()
            if count > 0:
                logger.info(f"Pruned {count} ghost positions from resolved markets.")
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Failed to cleanup ghost positions: {e}")

    async def get_top_traders_by_volume(self, limit: int = 10):
        # Placeholder for scoring integration
        pass

    async def reconcile_with_onchain_balances(self, trader_address: str):
        """
        Safety Sync (Notpowell Anti-Ghosting Gate).
        Verify that the token balances in the wallet match our internal snapshots.
        If a drift is detected (e.g. a filled order was missed by the API sync),
        force-update the PositionSnapshot to reflect the ground truth of the chain.
        """
        logger.info(f"Reconciling on-chain balances for {trader_address[:12]}...")
        # LOGIC:
        # 1. Fetch current open snapshots from DB.
        # 2. Call clob_client or web3 to get actual token balance for each outcome_id.
        # 3. If balance > snapshot_size, a BUY was missed.
        # 4. If balance < snapshot_size, a SELL was missed.
        # 5. Correct the DB snapshot accordingly.
        
        # Real-world implementation requires a web3 provider or official CLOB client
        # with a valid private key. For paper mode, this is a no-op safety check.
        pass

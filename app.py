import asyncio
import logging
import time
from typing import Dict, Any

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi_socketio import SocketManager

from sqlalchemy import select
from packages.db.session import AsyncSessionLocal, init_db
from packages.db.models.market import Market
from packages.ui.state_mapper import map_db_to_bot_state
from packages.core.config import settings
from packages.tasks.refresh_markets import refresh_markets
from packages.tasks.refresh_trades import refresh_trades
from packages.tasks.recompute_features import refresh_trader_profiles
from packages.tasks.compute_signals import refresh_market_signals
from packages.tasks.execute_signals import execute_signals

logger = logging.getLogger("polymarket_alpha_ui")

app = FastAPI()
sio = SocketManager(app=app, cors_allowed_origins="*", mount_location="/socket.io")

# Mount static files and templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Globals for bot state
is_trading = False
is_syncing = False
bot_state: Dict[str, Any] = {}
db_lock = asyncio.Lock()

# How often each layer runs:
#   SIGNAL_CYCLE  — recompute scores on cached DB markets (no API). Fast, runs often.
#   TRADE_CYCLE   — refresh trade history + positions from CLOB (only when trading).
#   MARKET_CYCLE  — fetch new markets from Polymarket API (heavy, runs infrequently).
SIGNAL_CYCLE_SECONDS = 30          # process cached markets every 30 s
TRADE_CYCLE_SECONDS  = 60          # refresh positions every 60 s while trading
MARKET_CYCLE_MIN     = 30          # minimum minutes between market API fetches

async def update_state_loop():
    """Periodically refresh the bot state from the database and broadcast via SocketIO."""
    while True:
        try:
            async with db_lock:
                async with AsyncSessionLocal() as session:
                    global bot_state
                    bot_state = await map_db_to_bot_state(session, is_trading=is_trading, is_syncing=is_syncing)
                    await sio.emit("bot_status", bot_state)
        except Exception as e:
            logger.error(f"Error in state update loop: {e}")
        
        await asyncio.sleep(2)

# Global sync trigger to wake up background worker on config change
_force_sync_event = asyncio.Event()

# Track which strategy markets were last fetched for (to detect strategy changes)
_last_fetched_strategy: str = ""
_last_market_fetch_time: float = 0.0


async def background_workers_loop():
    """
    Three-layer background engine (memory-efficient, nothing missed):

      Layer 1 — Signal cycle (every 30 s):
        Re-score all cached markets already in the DB. No Polymarket API calls.
        Keeps Alpha Scan current at all times regardless of bot state.

      Layer 2 — Trade/Position cycle (every 60 s, only when bot is running):
        Refresh trade history and re-compute trader profiles from CLOB.
        Positions stay in sync with live market data while trading.

      Layer 3 — Market fetch cycle (every scan_interval min, min 30):
        Pull new markets from Polymarket Gamma API for the active strategy.
        Heavy — runs infrequently to save memory and bandwidth.
        Triggered immediately on strategy change or force-sync from UI.
    """
    global is_syncing, _last_fetched_strategy, _last_market_fetch_time
    logger.info("Starting background worker loop...")

    # Track last trade refresh separately
    last_trade_refresh: float = 0.0

    # First-run: if DB is empty, fetch markets before the first signal cycle
    try:
        async with AsyncSessionLocal() as session:
            from sqlalchemy import func as sqlfunc
            count_stmt = select(sqlfunc.count(Market.id))
            market_count = (await session.execute(count_stmt)).scalar() or 0
            if market_count == 0:
                logger.info("DB empty — running initial market sync before first signal cycle...")
                async with db_lock:
                    await refresh_markets(session)
                    await refresh_market_signals(session)
                _last_fetched_strategy = settings.strategy
                _last_market_fetch_time = time.monotonic()
                last_trade_refresh = time.monotonic()
    except Exception as e:
        logger.error(f"Startup sync failed: {e}")

    while True:
        try:
            has_force_sync = _force_sync_event.is_set()
            _force_sync_event.clear()

            now              = time.monotonic()
            current_strategy = settings.strategy
            strategy_changed = current_strategy != _last_fetched_strategy
            market_interval  = max(MARKET_CYCLE_MIN, settings.app.scan_interval) * 60
            market_due       = (now - _last_market_fetch_time) >= market_interval
            trade_due        = is_trading and (now - last_trade_refresh) >= TRADE_CYCLE_SECONDS

            is_syncing = True

            async with db_lock:
                async with AsyncSessionLocal() as session:

                    # ── Layer 3: Heavy market fetch ──────────────────────────────
                    if strategy_changed or has_force_sync or market_due:
                        reason = ("strategy change" if strategy_changed
                                  else "force sync"  if has_force_sync
                                  else "scheduled interval")
                        logger.info(f"[Market Fetch] Starting ({reason})...")
                        await refresh_markets(session)
                        _last_fetched_strategy = current_strategy
                        _last_market_fetch_time = time.monotonic()
                        logger.info("[Market Fetch] Done.")

                    # ── Layer 2: Trade / position refresh ────────────────────────
                    if trade_due or (has_force_sync and is_trading):
                        logger.info("[Trade Refresh] Updating positions...")
                        await refresh_trades(session)
                        await refresh_trader_profiles(session)
                        last_trade_refresh = time.monotonic()
                        logger.info("[Trade Refresh] Done.")

                    # ── Layer 1: Signal recompute on cached markets ───────────────
                    logger.info(f"[Signal Cycle] Scoring cached markets (strategy={current_strategy})...")
                    await refresh_market_signals(session)
                    logger.info("[Signal Cycle] Done.")

                    # ── Layer 0: Execute trades on top signals ────────────────────
                    if is_trading:
                        await execute_signals(session)

            is_syncing = False

        except Exception as e:
            is_syncing = False
            logger.error(f"Background sync error: {e}")
            import traceback
            logger.error(traceback.format_exc())

        # Sleep until next 30-second signal cycle, or wake immediately on force-sync
        try:
            await asyncio.wait_for(_force_sync_event.wait(), timeout=SIGNAL_CYCLE_SECONDS)
        except asyncio.TimeoutError:
            pass


@app.on_event("startup")
async def startup_event():
    # Initialize the database within the event loop managed by Uvicorn
    init_db()
    asyncio.create_task(update_state_loop())
    asyncio.create_task(background_workers_loop())

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")

@app.post("/api/control")
async def control(request: Request):
    global is_trading
    data = await request.json()
    action = data.get("action")
    
    if action == "start":
        is_trading = True
    elif action == "stop":
        is_trading = False
        
    logger.info(f"Bot state changed: is_trading={is_trading}")
    return {"status": "success", "is_trading": is_trading}

@app.post("/api/config")
async def update_config(request: Request):
    """
    Save strategy and trading settings from the dashboard Settings tab.
    Persists to settings.json so the next sync cycle picks up the new strategy.
    """
    import json
    from pathlib import Path
    from packages.core.config import SETTINGS_FILE

    data = await request.json()
    settings_path = SETTINGS_FILE

    # Load existing settings (or start fresh)
    current = {}
    if settings_path.exists():
        try:
            with open(settings_path) as f:
                current = json.load(f)
        except Exception:
            current = {}

    # Merge in the new values sent from the UI
    allowed_keys = {
        "strategy", "paper_mode", "trade_amount",
        "min_edge", "scan_interval", "paper_balance", "max_trades",
    }
    for key in allowed_keys:
        if key in data:
            current[key] = data[key]

    # Handle private key separately
    pk = data.get("private_key", "").strip()
    if pk and pk != "0x" + "0" * 64:
        env_path = Path(".env")
        env_lines = env_path.read_text().splitlines() if env_path.exists() else []
        env_lines = [l for l in env_lines
                     if not l.startswith("PK=") and not l.startswith("POLYMARKET__PRIVATE_KEY=")]
        env_lines.append(f"POLYMARKET__PRIVATE_KEY={pk}")
        env_path.write_text("\n".join(env_lines) + "\n")
        settings.polymarket.private_key = pk

    with open(settings_path, "w") as f:
        json.dump(current, f, indent=2)

    # Hot-reload with strict type-casting for live objects
    if "paper_mode" in data:
        settings.app.paper_mode = str(data["paper_mode"]).lower() == "true"
    if "trade_amount" in data:
        settings.app.trade_amount = float(data["trade_amount"])
    if "min_edge" in data:
        settings.app.min_edge = float(data["min_edge"])
    if "scan_interval" in data:
        settings.app.scan_interval = int(data["scan_interval"])
    if "paper_balance" in data:
        settings.app.paper_balance = float(data["paper_balance"])
    if "max_trades" in data:
        settings.app.max_trades = int(data["max_trades"])
    
    if "strategy" in data:
        old_strategy = settings.strategy
        new_strategy = str(data["strategy"])
        if old_strategy != new_strategy:
            logger.info(f"Strategy changed from {old_strategy} to {new_strategy}. Triggering market re-fetch...")
            settings.strategy = new_strategy
            # Reset the last-fetched marker so the background loop treats this as a strategy change
            global _last_fetched_strategy
            _last_fetched_strategy = ""
            _force_sync_event.set()

    logger.info(f"Settings updated: {current}")
    return {"status": "saved", "settings": current}


@app.get("/api/config")
async def get_config():
    """Return current settings so the frontend can populate the form on page load."""
    return JSONResponse({
        "paper_mode":    settings.app.paper_mode,
        "trade_amount":  settings.app.trade_amount,
        "min_edge":      settings.app.min_edge,
        "scan_interval": settings.app.scan_interval,
        "strategy":      settings.strategy,
        "paper_balance": settings.app.paper_balance,
        "max_trades":    settings.app.max_trades,
    })




@app.sio.on("request_update")
async def handle_request_update(sid, *args, **kwargs):
    await sio.emit("bot_status", bot_state, to=sid)

if __name__ == "__main__":
    import uvicorn
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=5000,
        proxy_headers=True,       # trust X-Forwarded-Proto from Render's proxy
        forwarded_allow_ips="*",  # accept forwarded headers from any upstream IP
    )

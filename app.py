import asyncio
import logging
import os
from typing import Dict, Any

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
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

async def background_workers_loop():
    """Run all background data and quants tasks in a loop."""
    logger.info("Starting background worker loop...")
    
    # First-run check: if DB is empty, run a market sync immediately
    try:
        async with AsyncSessionLocal() as session:
            from sqlalchemy import func as sqlfunc
            count_stmt = select(sqlfunc.count(Market.id))
            market_count = (await session.execute(count_stmt)).scalar() or 0
            if market_count == 0:
                logger.info("Database empty on startup. Triggering initial market sync...")
                await refresh_markets(session)
    except Exception as e:
        logger.error(f"Startup sync failed: {e}")

    while True:
        try:
            # Wake-up logic: wait for trading to start OR a force-sync trigger
            has_force_sync = _force_sync_event.is_set()
            if not is_trading and not has_force_sync:
                try:
                    await asyncio.wait_for(_force_sync_event.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pass
                has_force_sync = _force_sync_event.is_set()
                if not is_trading and not has_force_sync:
                    continue
            
            # Clear the trigger if it was set
            _force_sync_event.clear()
            
            global is_syncing
            is_syncing = True
            logger.info(f"--- Sync Cycle Starting (ForceSync={has_force_sync}) ---")
            
            async with db_lock:
                async with AsyncSessionLocal() as session:
                    # 1. Market Refresh (Always done, priority tags used internally)
                    await refresh_markets(session)
                    
                    # 2. Detailed Data Refresh (Skipped on strategy-change to ensure 'immediate' signals)
                    if not has_force_sync:
                        await refresh_trades(session)
                        await refresh_trader_profiles(session)
                    else:
                        logger.info("Strategy change detected: performing high-priority signal refresh...")
                        
                    # 3. Alpha Signal Generation (Always refreshed)
                    await refresh_market_signals(session)
                
            logger.info("--- Sync Cycle Complete ---")
            is_syncing = False
            
        except Exception as e:
            is_syncing = False
            logger.error(f"Error in background sync: {e}")
            import traceback
            logger.error(traceback.format_exc())
            
        # Wait for the interval OR a force-sync (e.g. strategy change)
        scan_seconds = max(30, settings.app.scan_interval * 60)
        try:
            await asyncio.wait_for(_force_sync_event.wait(), timeout=scan_seconds)
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
            logger.info(f"Strategy changed from {old_strategy} to {new_strategy}. Triggering sync...")
            settings.strategy = new_strategy
            _force_sync_event.set()

    logger.info(f"Settings updated: {current}")
    return {"status": "saved", "settings": current}




@app.sio.on("request_update")
async def handle_request_update(sid, *args, **kwargs):
    await sio.emit("bot_status", bot_state, to=sid)

if __name__ == "__main__":
    import uvicorn
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="0.0.0.0", port=5000)

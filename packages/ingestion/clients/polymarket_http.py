import base64
import hashlib
import hmac
import logging
import asyncio
import time
from typing import Any, Dict, List, Optional

import httpx
from packages.core.config import settings


def _build_l2_headers(method: str, path: str) -> Dict[str, str]:
    """Build Polymarket CLOB L2 HMAC auth headers from settings credentials."""
    api_key        = settings.polymarket.api_key
    api_secret     = settings.polymarket.api_secret
    api_passphrase = settings.polymarket.api_passphrase
    wallet_address = settings.polymarket.wallet_address  # derived from private_key
    if not all([api_key, api_secret, api_passphrase, wallet_address]):
        return {}
    timestamp = str(int(time.time()))
    message   = timestamp + method.upper() + path
    try:
        secret_bytes = base64.b64decode(api_secret)
        sig = base64.b64encode(
            hmac.new(secret_bytes, message.encode("utf-8"), hashlib.sha256).digest()
        ).decode("utf-8")
    except Exception:
        return {}
    return {
        "POLY_ADDRESS":   wallet_address,
        "POLY_SIGNATURE": sig,
        "POLY_TIMESTAMP": timestamp,
        "POLY_NONCE":     "0",
        "Content-Type":   "application/json",
    }

try:
    from py_clob_client.client import ClobClient as OfficialClobClient
    from py_clob_client.clob_types import ApiCreds
    HAS_OFFICIAL_CLIENT = True
except ImportError:
    HAS_OFFICIAL_CLIENT = False

logger = logging.getLogger(__name__)

class GammaClient:
    """Client for the Polymarket Gamma API (Market Metadata)."""
    
    def __init__(self, base_url: str = settings.polymarket.gamma_api_url):
        self.base_url = base_url
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=30.0)

    async def get_events(
        self,
        limit: int = 50,
        offset: int = 0,
        active: bool = True,
        order: str = "volume24hr",
        ascending: bool = False,
    ) -> List[Dict[str, Any]]:
        """Fetch events from Gamma, sorted by 24-hour volume descending by default."""
        params = {
            "limit":     limit,
            "offset":    offset,
            "active":    str(active).lower(),
            "closed":    "false",
            "order":     order,
            "ascending": str(ascending).lower(),
        }
        response = await self.client.get("/events", params=params)
        response.raise_for_status()
        return response.json()

    async def get_events_paginated(self, max_events: int = 2000) -> List[Dict[str, Any]]:
        """
        Fetch up to max_events active events across multiple pages.
        Casting a very wide net to ensure no skilled trade activity is missed.
        """
        all_events: List[Dict[str, Any]] = []
        page_size = 50
        offset    = 0
        while len(all_events) < max_events:
            batch = await self.get_events(limit=page_size, offset=offset)
            if not batch:
                break
            all_events.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size
        return all_events[:max_events]

    async def get_events_by_tag(self, tag_id: int, max_events: int = 2000) -> List[Dict[str, Any]]:
        """
        Fetch active events filtered by Polymarket tag ID.
        Supports up to 2000 events per tag for exhaustive coverage.
        """
        all_events: List[Dict[str, Any]] = []
        page_size = 50
        offset    = 0
        while len(all_events) < max_events:
            params = {
                "limit":    page_size,
                "offset":   offset,
                "active":   "true",
                "closed":   "false",
                "tag_id":   tag_id,
                "order":    "volume24hr",
                "ascending": "false",
            }
            response = await self.client.get("/events", params=params)
            response.raise_for_status()
            batch = response.json()
            if not batch:
                break
            all_events.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size
        return all_events[:max_events]


    async def get_event(self, event_id: str) -> Dict[str, Any]:
        """Fetch a single event by ID."""
        response = await self.client.get(f"/events/{event_id}")
        response.raise_for_status()
        return response.json()

    async def close(self):
        await self.client.aclose()

class ClobClient:
    """
    Client for the Polymarket CLOB API. 
    Enhanced to use official py_clob_client if keys are provided.
    """
    
    def __init__(self, base_url: str = settings.polymarket.clob_api_url):
        self.base_url = base_url
        self.http_client = httpx.AsyncClient(base_url=self.base_url, timeout=30.0)
        self.official_client = None
        
        pk = settings.polymarket.private_key
        if HAS_OFFICIAL_CLIENT and pk and pk != "0x" + "0"*64 and pk != "":
            try:
                creds = ApiCreds(
                    api_key=settings.polymarket.api_key,
                    api_secret=settings.polymarket.api_secret,
                    api_passphrase=settings.polymarket.api_passphrase
                )
                self.official_client = OfficialClobClient(
                    host=self.base_url,
                    key=pk,
                    chain_id=137, # Polygon
                    creds=creds
                )
                logger.info("Official Polymarket CLOB client initialized with credentials.")
            except Exception as e:
                logger.warning(f"Failed to init official client: {e}. Falling back to HTTP.")

    async def get_trades(self, asset_id: str, limit: int = 500) -> List[Dict[str, Any]]:
        """Fetch recent trades for a specific asset (CLOB token ID).

        Polymarket CLOB authenticated endpoint:
            GET /data/trades?asset_id=<token_id>&limit=N
            Requires L2 API-key auth (api_key / api_secret / api_passphrase + wallet address).

        Falls back gracefully to an empty list when:
          - The official client is unavailable (no valid private key configured)
          - The endpoint returns 401 (auth required) or 404
        """
        if not asset_id:
            return []

        if self.official_client:
            try:
                resp = await asyncio.to_thread(
                    self.official_client.get_trades, asset_id=asset_id
                )
                if isinstance(resp, list):
                    return resp
                if isinstance(resp, dict):
                    return resp.get("data", resp.get("history", []))
            except Exception as e:
                logger.warning(f"Official client get_trades failed: {e}. Falling back to HTTP.")

        # Attempt the authenticated /data/trades endpoint with L2 HMAC headers.
        # Falls back silently to empty list on 401/403 (no credentials configured).
        try:
            path = f"/data/trades?asset_id={asset_id}&limit={limit}"
            auth_headers = _build_l2_headers("GET", path)
            response = await self.http_client.get(
                "/data/trades",
                params={"asset_id": asset_id, "limit": limit},
                headers=auth_headers,
            )
            if response.status_code in (401, 403, 404):
                logger.debug(
                    f"CLOB /data/trades returned {response.status_code} for asset {asset_id[:16]}... "
                    f"— API credentials required. Returning empty trade list."
                )
                return []
            response.raise_for_status()
            data = response.json()
            if isinstance(data, dict):
                return data.get("data", data.get("history", []))
            return data if isinstance(data, list) else []
        except httpx.HTTPStatusError:
            return []
        except Exception as e:
            logger.warning(f"CLOB get_trades request failed for {asset_id[:16]}...: {e}")
            return []

    async def get_orderbook(self, token_id: str) -> Dict[str, Any]:
        """Fetch current orderbook for a specific token."""
        if self.official_client:
            try:
                resp = await asyncio.to_thread(self.official_client.get_market_orderbook, token_id=token_id)
                return resp
            except Exception as e:
                logger.warning(f"Official client get_orderbook failed: {e}. Falling back to HTTP.")

        response = await self.http_client.get(f"/book", params={"token_id": token_id})
        response.raise_for_status()
        return response.json()

    async def close(self):
        await self.http_client.aclose()

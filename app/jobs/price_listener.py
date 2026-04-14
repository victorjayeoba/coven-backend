"""
AVE Data API WebSocket — `price` topic listener.

Subscribes to live price/volume/tvl updates for the trending + pump-phase
tokens. Each update is normalized and published to the event bus, where
the SSE handler forwards it to the frontend.

Endpoint: wss://wss.ave-api.xyz
Subscribe shape (per AVE docs):
  {"jsonrpc":"2.0","method":"subscribe","params":["price",[<id1>,<id2>,...]],"id":N}
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed

from app.config import settings
from app.services.ave_client import AveClient
from app.services.event_bus import PRICE_UPDATE, bus

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

WSS_URL = getattr(settings, "ave_wss_url", None) or "wss://wss.ave-api.xyz"
AVE_API_KEY = settings.ave_api_key

CHAINS = ["solana", "bsc"]
TRENDING_PER_CHAIN = 30
PUMP_NEW_LIMIT = 80
PUMP_HOT_LIMIT = 40
SUBSCRIPTION_REFRESH_SECONDS = 5 * 60
PING_INTERVAL = 20
RECONNECT_DELAY = 5

_stop = asyncio.Event()


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _coerce_list(data) -> list[dict]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for key in ("list", "tokens", "data", "items", "result"):
            inner = data.get(key)
            if isinstance(inner, list):
                return [x for x in inner if isinstance(x, dict)]
    return []


async def _fetch_token_ids() -> list[str]:
    """Same set as ws_listener — pump tokens + trending."""
    seen: set[str] = set()
    tids: list[str] = []

    async def _add(chain: str, contract: str | None) -> None:
        if not contract:
            return
        tid = f"{contract}-{chain}".lower()
        if tid not in seen:
            seen.add(tid)
            tids.append(f"{contract}-{chain}")

    async with AveClient() as ave:
        for tag, limit in (("pump_in_new", PUMP_NEW_LIMIT), ("pump_in_hot", PUMP_HOT_LIMIT)):
            try:
                raw = await ave.tokens_platform(tag=tag, limit=limit)
            except Exception as e:
                print(f"[price_listener] tokens_platform '{tag}' failed: {e}")
                continue
            for t in _coerce_list(raw):
                await _add(t.get("chain"), t.get("token") or t.get("token_address"))

        for chain in CHAINS:
            try:
                raw = await ave.trending(chain, page=0, page_size=TRENDING_PER_CHAIN)
            except Exception as e:
                print(f"[price_listener] trending failed for {chain}: {e}")
                continue
            for t in _coerce_list(raw):
                await _add(chain, t.get("token"))

    return tids


def _f(v: Any) -> float | None:
    try:
        f = float(v)
        return f if f == f else None  # filter NaN
    except (TypeError, ValueError):
        return None


def _normalize(p: dict) -> dict | None:
    """Turn an AVE price payload into the same shape the frontend uses."""
    target = p.get("target_token")
    chain = p.get("chain")
    if not target or not chain:
        return None
    token_id = f"{target}-{chain}"
    return {
        "token_id": token_id,
        "chain": chain,
        "price_usd": _f(p.get("uprice")),
        "price_change_24h": _f(p.get("price_change_24h")),
        "price_change_1h": _f(p.get("price_change")),  # AVE only sends short-term here
        "tvl": _f(p.get("tvl")),
        "volume_24h": _f(p.get("volume_24_u")),
        "tx_count_24h": _f(p.get("tx_count_24h")),
        "makers_24h": _f(p.get("makers_24h")),
        "is_main_pair": p.get("is_main_pair"),
        "time": _f(p.get("time")),
    }


# ---------------------------------------------------------------------
# Loop
# ---------------------------------------------------------------------

_event_count = 0
_last_report = 0.0


async def _ping_loop(ws) -> None:
    pid = 2_000_000
    while not _stop.is_set():
        try:
            await asyncio.wait_for(_stop.wait(), timeout=PING_INTERVAL)
        except asyncio.TimeoutError:
            pass
        try:
            pid += 1
            await ws.send(json.dumps({"jsonrpc": "2.0", "method": "ping", "id": pid}))
        except Exception:
            return


async def _subscribe_chunked(ws, token_ids: list[str]) -> None:
    """
    Subscribe in chunks — AVE accepts an array of identifiers per request.
    Chunk to avoid huge payloads.
    """
    chunk = 50
    msg_id = 1
    for i in range(0, len(token_ids), chunk):
        slice_ids = token_ids[i : i + chunk]
        msg = {
            "jsonrpc": "2.0",
            "method": "subscribe",
            "params": ["price", slice_ids],
            "id": msg_id,
        }
        msg_id += 1
        try:
            await ws.send(json.dumps(msg))
        except Exception as e:
            print(f"[price_listener] subscribe send failed: {e}")
            return
    print(f"[price_listener] subscribed to {len(token_ids)} token prices")


async def _handle_message(raw: str) -> None:
    global _event_count, _last_report

    if isinstance(raw, str) and raw.strip().lower() == "pong":
        return
    try:
        payload = json.loads(raw)
    except Exception:
        return

    result = payload.get("result") or payload.get("params")
    if not isinstance(result, dict):
        return
    if result.get("topic") != "price":
        return

    prices = result.get("prices") or []
    if not isinstance(prices, list):
        return

    for p in prices:
        if not isinstance(p, dict):
            continue
        norm = _normalize(p)
        if norm:
            await bus.publish(PRICE_UPDATE, norm)
            _event_count += 1

    now = time.time()
    if now - _last_report >= 60:
        print(f"[price_listener] {_event_count} price updates in last minute")
        _event_count = 0
        _last_report = now


async def _run_once() -> None:
    print(f"[price_listener] connecting to {WSS_URL}")
    async with websockets.connect(
        WSS_URL,
        additional_headers={"X-API-KEY": AVE_API_KEY} if AVE_API_KEY else None,
        ping_interval=None,
        open_timeout=15,
    ) as ws:
        print("[price_listener] connected")

        token_ids = await _fetch_token_ids()
        if not token_ids:
            print("[price_listener] no targets — closing")
            return

        await _subscribe_chunked(ws, token_ids)
        ping_task = asyncio.create_task(_ping_loop(ws))
        refresh_deadline = time.time() + SUBSCRIPTION_REFRESH_SECONDS

        try:
            while not _stop.is_set():
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=30)
                except asyncio.TimeoutError:
                    if time.time() > refresh_deadline:
                        return  # reconnect to re-seed subscriptions
                    continue
                await _handle_message(raw)
        finally:
            ping_task.cancel()


async def run() -> None:
    if not AVE_API_KEY:
        print("[price_listener] AVE_API_KEY missing — skipping")
        return

    print("[price_listener] starting")
    while not _stop.is_set():
        try:
            await _run_once()
        except ConnectionClosed as e:
            print(f"[price_listener] connection closed: {e}")
        except Exception as e:
            print(f"[price_listener] error: {type(e).__name__}: {e}")

        if _stop.is_set():
            break
        try:
            await asyncio.wait_for(_stop.wait(), timeout=RECONNECT_DELAY)
        except asyncio.TimeoutError:
            pass
    print("[price_listener] stopped")


def stop() -> None:
    _stop.set()

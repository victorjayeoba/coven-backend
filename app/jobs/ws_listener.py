"""
AVE Data API WebSocket listener.

Connects to wss://wss.ave-api.xyz and subscribes to real-time swap events
for a rotating set of trending tokens. Normalizes each event and publishes
to the in-process event bus — the contagion detector picks it up from there.

Subscribe format (from AVE docs):
  {"jsonrpc":"2.0","method":"subscribe","params":["multi_tx","<token>","<chain>"],"id":N}

Ping/pong:
  {"jsonrpc":"2.0","method":"ping","id":N}  ->  "pong"
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
from app.services.event_bus import SWAP_EVENT, bus

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

WSS_URL = getattr(settings, "ave_wss_url", None) or "wss://wss.ave-api.xyz"
AVE_API_KEY = settings.ave_api_key

CHAINS = ["solana", "bsc"]
TRENDING_PER_CHAIN = 15          # fewer trending tokens
PUMP_NEW_LIMIT = 40              # tokens currently entering pump phase (prime entry targets)
PUMP_HOT_LIMIT = 20              # tokens hot & pumping
SUBSCRIPTION_REFRESH_SECONDS = 15 * 60  # re-seed every 15 min
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


async def _fetch_subscription_targets() -> list[tuple[str, str, str | None]]:
    """
    Build the watchlist:
      - tokens currently entering a pump (pump_in_new)  -> best entry signals
      - hot pumping tokens (pump_in_hot)                -> confirmation pumps
      - per-chain trending                              -> baseline coverage
    Deduped by (chain, contract).
    """
    seen: set[tuple[str, str]] = set()
    targets: list[tuple[str, str, str | None]] = []

    async def _add(chain: str, contract: str | None, symbol: str | None) -> None:
        if not contract:
            return
        key = (chain, contract)
        if key in seen:
            return
        seen.add(key)
        targets.append((chain, contract, symbol))

    async with AveClient() as ave:
        # Global pump-phase feeds (not chain-scoped in the API)
        for tag, limit in (("pump_in_new", PUMP_NEW_LIMIT), ("pump_in_hot", PUMP_HOT_LIMIT)):
            try:
                raw = await ave.tokens_platform(tag=tag, limit=limit)
            except Exception as e:
                print(f"[ws_listener] tokens_platform '{tag}' failed: {e}")
                continue
            for t in _coerce_list(raw):
                chain = t.get("chain")
                contract = t.get("token") or t.get("token_address")
                if chain and contract:
                    await _add(chain, contract, t.get("symbol"))

        # Per-chain trending as a baseline
        for chain in CHAINS:
            try:
                raw = await ave.trending(chain, page=0, page_size=TRENDING_PER_CHAIN)
            except Exception as e:
                print(f"[ws_listener] trending failed for {chain}: {e}")
                continue
            for t in _coerce_list(raw):
                contract = t.get("token")
                if contract:
                    await _add(chain, contract, t.get("symbol"))

    return targets


def _normalize_event(tx: dict) -> dict | None:
    """
    Turn an AVE WSS 'tx' payload into the standard event bus event.
    Shape (from real stream):
      {
        amm, amount_eth, amount_usd, block_number, chain, direction,
        from_address, from_amount, from_symbol, from_reserve,
        to_address, to_amount, to_symbol, to_reserve,
        pair_address, wallet_address, sender, time, tx_time, id
      }
    """
    wallet = tx.get("wallet_address") or tx.get("sender") or tx.get("maker")
    if not wallet:
        return None

    chain = tx.get("chain")
    direction = (tx.get("direction") or "").lower()
    side = "buy" if direction == "buy" else "sell" if direction == "sell" else None
    if side is None:
        return None

    # The "target" token is what the trader ended up holding
    if side == "buy":
        token_contract = tx.get("to_address")
        symbol = tx.get("to_symbol")
    else:
        token_contract = tx.get("from_address")
        symbol = tx.get("from_symbol")

    if not token_contract or not chain:
        return None

    ts_raw = tx.get("time") or tx.get("tx_time") or tx.get("block_time")
    try:
        ts = float(ts_raw) if ts_raw else time.time()
    except (TypeError, ValueError):
        ts = time.time()

    return {
        "wallet": wallet,
        "token_id": f"{token_contract}-{chain}",
        "chain": chain,
        "symbol": symbol,
        "timestamp": ts,
        "side": side,
        "tx_hash": tx.get("transaction") or tx.get("id"),
        "amount_usd": tx.get("amount_usd"),
    }


# ---------------------------------------------------------------------
# Core WS loop
# ---------------------------------------------------------------------

async def _ping_loop(ws) -> None:
    """Send periodic pings to keep the connection alive."""
    pid = 1_000_000
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


async def _subscribe_all(ws, targets: list[tuple[str, str, str | None]]) -> None:
    """Send a subscription message for each (chain, token)."""
    for idx, (chain, token, _sym) in enumerate(targets, start=1):
        msg = {
            "jsonrpc": "2.0",
            "method": "subscribe",
            "params": ["multi_tx", token, chain],
            "id": idx,
        }
        try:
            await ws.send(json.dumps(msg))
        except Exception as e:
            print(f"[ws_listener] subscribe send failed: {e}")
            return
    print(f"[ws_listener] subscribed to {len(targets)} tokens")


_event_count = 0
_raw_count = 0
_last_report = 0.0


async def _handle_message(raw: str) -> None:
    """Parse one WS frame and publish normalized events to the bus."""
    global _event_count, _raw_count, _last_report

    _raw_count += 1

    # Ping response
    if isinstance(raw, str) and raw.strip().lower() == "pong":
        return

    try:
        payload = json.loads(raw)
    except Exception:
        return

    # Subscription ACK / error
    if "method" not in payload and "result" in payload and "id" in payload:
        if isinstance(payload["result"], (dict, str)) and "topic" not in (payload.get("result") or {}):
            return

    # Event push — actual shape: result.tx for transactions
    result = payload.get("result") or payload.get("params")
    if not isinstance(result, dict):
        return

    tx = result.get("tx") or result.get("msg") or result.get("data")
    if not isinstance(tx, dict):
        return

    event = _normalize_event(tx)
    if event:
        await bus.publish(SWAP_EVENT, event)
        _event_count += 1

    now = time.time()
    if now - _last_report >= 60:
        print(f"[ws_listener] raw={_raw_count} events={_event_count} in last minute")
        _raw_count = 0
        _event_count = 0
        _last_report = now


async def _run_once() -> None:
    """One connect → subscribe → consume cycle."""
    url = f"{WSS_URL}?X-API-KEY={AVE_API_KEY}"  # header auth isn't always honored on WSS — also try query
    print(f"[ws_listener] connecting to {WSS_URL}")

    async with websockets.connect(
        url,
        additional_headers={"X-API-KEY": AVE_API_KEY} if AVE_API_KEY else None,
        ping_interval=None,  # we handle ping ourselves
        open_timeout=15,
    ) as ws:
        print("[ws_listener] connected")
        targets = await _fetch_subscription_targets()
        if not targets:
            print("[ws_listener] no targets — closing")
            return

        await _subscribe_all(ws, targets)

        ping_task = asyncio.create_task(_ping_loop(ws))
        refresh_deadline = time.time() + SUBSCRIPTION_REFRESH_SECONDS

        try:
            while not _stop.is_set():
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=30)
                except asyncio.TimeoutError:
                    if time.time() > refresh_deadline:
                        return  # force reconnect to re-seed subscriptions
                    continue
                await _handle_message(raw)
        finally:
            ping_task.cancel()


# ---------------------------------------------------------------------
# Supervisor — reconnect forever
# ---------------------------------------------------------------------

async def run() -> None:
    if not AVE_API_KEY:
        print("[ws_listener] AVE_API_KEY missing — skipping")
        return

    print("[ws_listener] starting")
    while not _stop.is_set():
        try:
            await _run_once()
        except ConnectionClosed as e:
            print(f"[ws_listener] connection closed: {e}")
        except Exception as e:
            print(f"[ws_listener] error: {type(e).__name__}: {e}")

        if _stop.is_set():
            break
        try:
            await asyncio.wait_for(_stop.wait(), timeout=RECONNECT_DELAY)
        except asyncio.TimeoutError:
            pass
    print("[ws_listener] stopped")


def stop() -> None:
    _stop.set()

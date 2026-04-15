"""
Wallet watcher — dedicated AVE WSS connection tracking copy-bot target wallets.

Why a second WSS (in addition to ws_listener)?
  - ws_listener subscribes to `multi_tx` for a rotating set of trending / pump
    tokens. That's the wrong shape for copy bots: a target wallet can buy ANY
    token, not just the ones we're currently tracking for cluster detection.
  - This watcher subscribes per-wallet, so a copy bot fires on whatever token
    their target buys next — not limited to the trending watchlist.

Subscription format (AVE wss):
    {"jsonrpc":"2.0","method":"subscribe",
     "params":["<TOPIC>","<wallet_address>","<chain>"], "id":<n>}

The topic string is configurable — AVE uses different names across docs
(`user`, `address`, `wallet`, `tx_by_address`). If one doesn't ACK, try the
next. We log subscription ACK / error frames so mismatches are obvious.

Parser is the same as ws_listener._normalize_event (AVE tx shape), plus a
wallet-side filter: we ONLY forward events whose wallet_address matches one
of the currently-watched wallets (belt-and-suspenders in case the upstream
topic leaks unrelated txs).
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed

from app.config import settings
from app.db import mongo
from app.services.event_bus import SWAP_EVENT, bus

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

WSS_URL = getattr(settings, "ave_wss_url", None) or "wss://wss.ave-api.xyz"
AVE_API_KEY = settings.ave_api_key

# AVE's wallet-level subscription topic. Adjust if your AVE plan exposes a
# different name — the sub message is logged on subscribe so you'll see any
# {"error": ...} response from the server.
WALLET_TOPIC = "tx"

REFRESH_SECONDS = 30           # re-read active bots and diff subscriptions
PING_INTERVAL = 20
RECONNECT_DELAY = 5

_stop = asyncio.Event()
_subscribed: set[tuple[str, str]] = set()   # (wallet_lc, chain)
_watched: set[tuple[str, str]] = set()      # current desired set


# ---------------------------------------------------------------------
# DB -> desired watch set
# ---------------------------------------------------------------------

async def _load_targets() -> set[tuple[str, str]]:
    """Unique (wallet_lc, chain) from every active copy bot with a target."""
    db = mongo.db()
    out: set[tuple[str, str]] = set()
    cursor = db.bots.find(
        {
            "type": "copy",
            "status": "active",
            "target_wallet_lc": {"$exists": True, "$ne": ""},
            "chain": {"$in": ["solana", "bsc"]},
        },
        {"target_wallet_lc": 1, "chain": 1},
    )
    async for b in cursor:
        wallet = (b.get("target_wallet_lc") or "").strip().lower()
        chain = b.get("chain")
        if wallet and chain:
            out.add((wallet, chain))
    return out


# ---------------------------------------------------------------------
# Parser (same AVE tx shape as multi_tx)
# ---------------------------------------------------------------------

def _normalize(tx: dict) -> dict | None:
    wallet = (tx.get("wallet_address") or tx.get("sender") or tx.get("maker") or "").lower()
    if not wallet:
        return None

    chain = tx.get("chain")
    direction = (tx.get("direction") or "").lower()
    side = "buy" if direction == "buy" else "sell" if direction == "sell" else None
    if side is None:
        return None

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
# WSS machinery
# ---------------------------------------------------------------------

_sub_id = 100_000


async def _send_subscribe(ws, wallet: str, chain: str) -> None:
    global _sub_id
    _sub_id += 1
    msg = {
        "jsonrpc": "2.0",
        "method": "subscribe",
        "params": [WALLET_TOPIC, wallet, chain],
        "id": _sub_id,
    }
    await ws.send(json.dumps(msg))
    print(f"[wallet_watcher] subscribe {WALLET_TOPIC} {wallet[:8]}… {chain}")


async def _send_unsubscribe(ws, wallet: str, chain: str) -> None:
    global _sub_id
    _sub_id += 1
    msg = {
        "jsonrpc": "2.0",
        "method": "unsubscribe",
        "params": [WALLET_TOPIC, wallet, chain],
        "id": _sub_id,
    }
    try:
        await ws.send(json.dumps(msg))
        print(f"[wallet_watcher] unsubscribe {wallet[:8]}… {chain}")
    except Exception:
        pass


async def _reconcile(ws) -> None:
    """Diff the DB target set against what we're currently subscribed to."""
    desired = await _load_targets()
    global _watched
    _watched = desired

    to_add = desired - _subscribed
    to_drop = _subscribed - desired

    for wallet, chain in to_add:
        try:
            await _send_subscribe(ws, wallet, chain)
            _subscribed.add((wallet, chain))
        except Exception as e:
            print(f"[wallet_watcher] sub failed: {e}")

    for wallet, chain in to_drop:
        await _send_unsubscribe(ws, wallet, chain)
        _subscribed.discard((wallet, chain))


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


async def _reconcile_loop(ws) -> None:
    while not _stop.is_set():
        try:
            await _reconcile(ws)
        except Exception as e:
            print(f"[wallet_watcher] reconcile error: {e}")
        try:
            await asyncio.wait_for(_stop.wait(), timeout=REFRESH_SECONDS)
        except asyncio.TimeoutError:
            pass


async def _handle_message(raw: str) -> None:
    if isinstance(raw, str) and raw.strip().lower() == "pong":
        return
    try:
        payload = json.loads(raw)
    except Exception:
        return

    # Subscription ACK / error — log errors, drop acks silently
    if "method" not in payload and "id" in payload and "result" in payload:
        result = payload["result"]
        if isinstance(result, dict) and result.get("error"):
            print(f"[wallet_watcher] sub error: {result}")
        # Plain ack — skip
        if not isinstance(result, dict) or ("tx" not in result and "msg" not in result):
            return

    result = payload.get("result") or payload.get("params")
    if not isinstance(result, dict):
        return
    tx = result.get("tx") or result.get("msg") or result.get("data")
    if not isinstance(tx, dict):
        return

    event = _normalize(tx)
    if not event:
        return

    # Belt-and-suspenders: only forward if this wallet is actually watched.
    if (event["wallet"], event["chain"]) not in _watched:
        return

    await bus.publish(SWAP_EVENT, event)
    print(
        f"[wallet_watcher] {event['side']} {event['symbol'] or event['token_id']} "
        f"by {event['wallet'][:8]}… (${event.get('amount_usd') or '?'})"
    )


async def _run_once() -> None:
    _subscribed.clear()
    url = f"{WSS_URL}?X-API-KEY={AVE_API_KEY}"
    print(f"[wallet_watcher] connecting to {WSS_URL}")

    async with websockets.connect(
        url,
        additional_headers={"X-API-KEY": AVE_API_KEY} if AVE_API_KEY else None,
        ping_interval=None,
        open_timeout=15,
    ) as ws:
        print("[wallet_watcher] connected")

        # Initial reconcile so we're subscribed before the first messages arrive
        await _reconcile(ws)

        ping_task = asyncio.create_task(_ping_loop(ws))
        reconcile_task = asyncio.create_task(_reconcile_loop(ws))

        try:
            while not _stop.is_set():
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=45)
                except asyncio.TimeoutError:
                    continue
                await _handle_message(raw)
        finally:
            ping_task.cancel()
            reconcile_task.cancel()


async def run() -> None:
    if not AVE_API_KEY:
        print("[wallet_watcher] AVE_API_KEY missing — skipping")
        return

    print("[wallet_watcher] starting")
    while not _stop.is_set():
        try:
            await _run_once()
        except ConnectionClosed as e:
            print(f"[wallet_watcher] connection closed: {e}")
        except Exception as e:
            print(f"[wallet_watcher] error: {type(e).__name__}: {e}")

        if _stop.is_set():
            break
        try:
            await asyncio.wait_for(_stop.wait(), timeout=RECONNECT_DELAY)
        except asyncio.TimeoutError:
            pass
    print("[wallet_watcher] stopped")


def stop() -> None:
    _stop.set()

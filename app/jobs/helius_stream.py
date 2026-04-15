"""
Helius WebSocket streamer — real-time copy-trade coverage for Solana.

Why this beats polling:
  - `logsSubscribe` fires within ~1s of a tx confirming — we don't wait for the
    next 10s tick.
  - Standard Solana JSON-RPC WS method, available on every Helius tier.
  - Free tier allows one WS connection; we multiplex all wallet subscriptions
    over that single connection.

Flow:
  1. Connect to wss://mainnet.helius-rpc.com/?api-key=<KEY>
  2. For each active copy bot's target wallet on Solana:
       send logsSubscribe with `mentions: [wallet]`
  3. On notification: extract signature → fetch parsed swap from Helius REST
     (`/v0/transactions/?commitment=confirmed&sigs=<sig>`) → emit SWAP_EVENT
  4. Every 30s: reconcile subscriptions against the DB (add new, drop removed)

Bot runner already de-dupes open positions by (bot_id, token_id), so if the
REST poller happens to also pick up a signal, the second one noops.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import httpx
import websockets
from websockets.exceptions import ConnectionClosed

from app.config import settings
from app.db import mongo
from app.services.event_bus import SWAP_EVENT, bus

# ---------------------------------------------------------------------

SOLANA_SKIP = {
    "So11111111111111111111111111111111111111112",
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
}

REFRESH_SECONDS = 30
RECONNECT_DELAY = 5

_stop = asyncio.Event()
_startup_ts = time.time() - 30

# wallet -> logsSubscribe subscription id (returned by Helius)
_subs: dict[str, int] = {}
# pending correlation id -> wallet (used to map subscribe response -> wallet)
_pending: dict[int, str] = {}
_req_id = 10_000
# signatures we've already dispatched (de-dupe burst logs)
_seen_sigs: set[str] = set()


def _wss_url() -> str:
    return f"wss://mainnet.helius-rpc.com/?api-key={settings.helius_api_key}"


def _rest_parse_url(sig: str) -> str:
    return f"{settings.helius_base_url}/v0/transactions/?api-key={settings.helius_api_key}"


# ---------------------------------------------------------------------
# Target set
# ---------------------------------------------------------------------

async def _load_solana_targets() -> set[str]:
    db = mongo.db()
    out: set[str] = set()
    cursor = db.bots.find(
        {
            "type": "copy",
            "status": "active",
            "chain": "solana",
            "target_wallet": {"$exists": True, "$ne": ""},
        },
        {"target_wallet": 1},
    )
    async for b in cursor:
        w = (b.get("target_wallet") or "").strip()
        if w:
            out.add(w)
    return out


# ---------------------------------------------------------------------
# Parser (Helius enhanced REST response)
# ---------------------------------------------------------------------

def _normalize_helius_swap(tx: dict, wallet: str) -> dict | None:
    """
    Fast path uses `events.swap` (Jupiter/Raydium/Orca). Fallback reconstructs
    from `tokenTransfers` so Pump.fun/Meteora/UNKNOWN-tagged swaps come through.
    """
    ts = int(tx.get("timestamp") or time.time())

    events = (tx.get("events") or {}).get("swap")
    if isinstance(events, dict):
        def _filter_non_stable(toks: list[dict]) -> dict | None:
            for t in toks or []:
                mint = t.get("mint")
                if mint and mint not in SOLANA_SKIP:
                    return t
            return None

        out_token = _filter_non_stable(events.get("tokenOutputs"))
        in_token = _filter_non_stable(events.get("tokenInputs"))
        side = None
        token = None
        if out_token:
            side, token = "buy", out_token
        elif in_token:
            side, token = "sell", in_token
        if side and token:
            mint = token.get("mint")
            if mint and mint not in SOLANA_SKIP:
                return {
                    "wallet": wallet,
                    "token_id": f"{mint}-solana",
                    "chain": "solana",
                    "symbol": None,
                    "timestamp": float(ts),
                    "side": side,
                    "tx_hash": tx.get("signature"),
                    "amount_usd": None,
                }

    transfers = tx.get("tokenTransfers") or []
    if not isinstance(transfers, list):
        return None

    received: dict | None = None
    sent: dict | None = None
    for t in transfers:
        if not isinstance(t, dict):
            continue
        mint = t.get("mint")
        if not mint or mint in SOLANA_SKIP:
            continue
        if t.get("toUserAccount") == wallet and received is None:
            received = t
        elif t.get("fromUserAccount") == wallet and sent is None:
            sent = t

    if received:
        side, token = "buy", received
    elif sent:
        side, token = "sell", sent
    else:
        return None

    return {
        "wallet": wallet,
        "token_id": f"{token['mint']}-solana",
        "chain": "solana",
        "symbol": None,
        "timestamp": float(ts),
        "side": side,
        "tx_hash": tx.get("signature"),
        "amount_usd": None,
    }


# ---------------------------------------------------------------------
# REST fetch + dispatch
# ---------------------------------------------------------------------

async def _fetch_and_dispatch(client: httpx.AsyncClient, sig: str, wallet: str) -> None:
    if sig in _seen_sigs:
        return
    _seen_sigs.add(sig)
    if len(_seen_sigs) > 2000:
        # keep the set bounded
        _seen_sigs.clear()
        _seen_sigs.add(sig)

    url = (
        f"{settings.helius_base_url}/v0/transactions"
        f"?api-key={settings.helius_api_key}"
    )
    try:
        r = await client.post(url, json={"transactions": [sig]}, timeout=10)
    except Exception as e:
        print(f"[helius_stream] REST error for {sig[:8]}…: {e}")
        return
    if r.status_code >= 400:
        print(f"[helius_stream] REST {r.status_code} for {sig[:8]}…: {r.text[:160]}")
        return
    try:
        items = r.json()
    except Exception:
        return
    if not isinstance(items, list) or not items:
        return

    tx = items[0]
    # Don't gate on type=="SWAP": Helius tags Pump.fun / Meteora / newer AMMs
    # as UNKNOWN. The normalizer returns None for genuine non-swaps.
    ts = int(tx.get("timestamp") or 0)
    if ts and ts < int(_startup_ts):
        return

    event = _normalize_helius_swap(tx, wallet)
    if not event:
        return
    await bus.publish(SWAP_EVENT, event)
    print(
        f"[helius_stream] SOL {event['side']} "
        f"{event['token_id'][:10]}… by {wallet[:8]}…"
    )


# ---------------------------------------------------------------------
# WSS subscribe / unsubscribe
# ---------------------------------------------------------------------

async def _subscribe(ws, wallet: str) -> None:
    global _req_id
    _req_id += 1
    rid = _req_id
    _pending[rid] = wallet
    msg = {
        "jsonrpc": "2.0",
        "id": rid,
        "method": "logsSubscribe",
        "params": [
            {"mentions": [wallet]},
            {"commitment": "confirmed"},
        ],
    }
    await ws.send(json.dumps(msg))
    print(f"[helius_stream] subscribe logs mentions={wallet[:8]}…")


async def _unsubscribe(ws, wallet: str) -> None:
    sub_id = _subs.pop(wallet, None)
    if sub_id is None:
        return
    global _req_id
    _req_id += 1
    msg = {
        "jsonrpc": "2.0",
        "id": _req_id,
        "method": "logsUnsubscribe",
        "params": [sub_id],
    }
    try:
        await ws.send(json.dumps(msg))
        print(f"[helius_stream] unsubscribe {wallet[:8]}…")
    except Exception:
        pass


async def _reconcile(ws) -> None:
    desired = await _load_solana_targets()
    current = set(_subs.keys()) | set(_pending.values())
    for w in desired - current:
        try:
            await _subscribe(ws, w)
        except Exception as e:
            print(f"[helius_stream] subscribe failed: {e}")
    for w in current - desired:
        await _unsubscribe(ws, w)


# ---------------------------------------------------------------------
# Frame handler
# ---------------------------------------------------------------------

async def _handle(raw: str, client: httpx.AsyncClient) -> None:
    try:
        msg = json.loads(raw)
    except Exception:
        return

    # Subscribe response: {"jsonrpc":"2.0","result":<subId>,"id":<rid>}
    if "result" in msg and "id" in msg and "method" not in msg:
        rid = msg.get("id")
        wallet = _pending.pop(rid, None) if rid is not None else None
        sub_id = msg.get("result")
        if wallet and isinstance(sub_id, int):
            _subs[wallet] = sub_id
            print(f"[helius_stream] subscribed {wallet[:8]}… sub_id={sub_id}")
        return

    # Notification: {"method":"logsNotification","params":{"result":{...},"subscription":<subId>}}
    if msg.get("method") != "logsNotification":
        return
    params = msg.get("params") or {}
    sub_id = params.get("subscription")
    result = (params.get("result") or {}).get("value") or {}
    sig = result.get("signature")
    if not sig or not isinstance(sub_id, int):
        return
    # Err logs → skip
    if result.get("err"):
        return

    # Reverse-lookup wallet from sub_id
    wallet = next((w for w, s in _subs.items() if s == sub_id), None)
    if not wallet:
        return

    # Parsed tx fetch is fire-and-forget so we don't block the socket
    asyncio.create_task(_fetch_and_dispatch(client, sig, wallet))


# ---------------------------------------------------------------------
# Supervisor
# ---------------------------------------------------------------------

async def _reconcile_loop(ws) -> None:
    while not _stop.is_set():
        try:
            await _reconcile(ws)
        except Exception as e:
            print(f"[helius_stream] reconcile error: {e}")
        try:
            await asyncio.wait_for(_stop.wait(), timeout=REFRESH_SECONDS)
        except asyncio.TimeoutError:
            pass


async def _run_once() -> None:
    _subs.clear()
    _pending.clear()
    print("[helius_stream] connecting to Helius WSS")
    async with httpx.AsyncClient() as client:
        async with websockets.connect(
            _wss_url(), ping_interval=30, open_timeout=15
        ) as ws:
            print("[helius_stream] connected")
            await _reconcile(ws)
            reconcile_task = asyncio.create_task(_reconcile_loop(ws))
            try:
                while not _stop.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=60)
                    except asyncio.TimeoutError:
                        continue
                    await _handle(raw, client)
            finally:
                reconcile_task.cancel()


async def run() -> None:
    if not settings.helius_api_key:
        print("[helius_stream] HELIUS_API_KEY missing — skipping")
        return
    print("[helius_stream] starting")
    while not _stop.is_set():
        try:
            await _run_once()
        except ConnectionClosed as e:
            print(f"[helius_stream] connection closed: {e}")
        except Exception as e:
            print(f"[helius_stream] error: {type(e).__name__}: {e}")
        if _stop.is_set():
            break
        try:
            await asyncio.wait_for(_stop.wait(), timeout=RECONNECT_DELAY)
        except asyncio.TimeoutError:
            pass
    print("[helius_stream] stopped")


def stop() -> None:
    _stop.set()

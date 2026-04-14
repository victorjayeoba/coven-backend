"""
Transaction poller.

Periodically pulls recent swap transactions for trending tokens from AVE REST,
normalizes each swap into a generic 'swap' event, and publishes to the bus.

The detector subscribes to these and fires signals.

This is the primary feed until/unless we wire WSS. The bus abstraction means
when we add a WS source later, nothing downstream has to change.
"""

import asyncio
from typing import Any

from app.db import mongo
from app.services.ave_client import AveClient
from app.services.event_bus import SWAP_EVENT, bus

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

CHAINS = ["solana", "bsc"]
TRENDING_PER_CHAIN = 30        # top N trending tokens per chain
TXS_PER_PAIR = 30              # pull last N swaps per pair each cycle
POLL_INTERVAL_SECONDS = 60     # how often to loop
PARALLEL = 10                  # concurrent API calls


# ---------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------

def _normalize_swap(tx: dict, chain: str, token_id: str, symbol: str | None) -> dict | None:
    """
    Turn an AVE swap_transactions record into a standard event shape.
    Field names follow AVE WSS 'multi_tx' schema.
    """
    wallet = (
        tx.get("wallet_address")
        or tx.get("sender")
        or tx.get("from_address")
        or tx.get("maker")
    )
    if not wallet:
        return None

    # tx_swap_type is AVE's canonical side indicator
    swap_type = str(tx.get("tx_swap_type") or tx.get("side") or "").lower()
    if "buy" in swap_type or swap_type == "1":
        side = "buy"
    elif "sell" in swap_type or swap_type == "2":
        side = "sell"
    else:
        side = "buy"

    ts = tx.get("time") or tx.get("block_time") or tx.get("timestamp")

    return {
        "wallet": wallet,
        "token_id": token_id,
        "chain": chain,
        "symbol": symbol,
        "timestamp": float(ts) if ts else None,
        "side": side,
        "tx_hash": tx.get("transaction") or tx.get("tx_hash") or tx.get("hash"),
        "amount_usd": tx.get("amount_usd"),
    }


# ---------------------------------------------------------------------
# Poll cycle
# ---------------------------------------------------------------------

def _coerce_list(data) -> list[dict]:
    """
    AVE sometimes wraps list payloads. Accept any of:
      [ {...}, {...} ]
      { "list": [ ... ] }
      { "tokens": [ ... ] }
      { "data": [ ... ] }
    Return an empty list otherwise.
    """
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for key in ("list", "tokens", "data", "items", "result"):
            inner = data.get(key)
            if isinstance(inner, list):
                return [x for x in inner if isinstance(x, dict)]
    return []


async def _get_tracked_pairs(ave: AveClient) -> list[tuple[str, str, str, str | None]]:
    """
    Build the list of (chain, token_id, pair_id, symbol) to poll.
    Start from trending tokens per chain.
    """
    results: list[tuple[str, str, str, str | None]] = []
    for chain in CHAINS:
        try:
            raw = await ave.trending(chain, page=0, page_size=TRENDING_PER_CHAIN)
        except Exception as e:
            print(f"[tx_poller] trending failed for {chain}: {e}")
            continue

        items = _coerce_list(raw)
        if not items:
            print(f"[tx_poller] trending '{chain}' returned no items (raw type: {type(raw).__name__})")
            if isinstance(raw, dict):
                print(f"[tx_poller]   top-level keys: {list(raw.keys())[:10]}")
            continue

        for t in items:
            token_contract = t.get("token")
            pair_contract = t.get("main_pair")
            if not token_contract or not pair_contract:
                continue
            token_id = f"{token_contract}-{chain}"
            pair_id = f"{pair_contract}-{chain}"
            results.append((chain, token_id, pair_id, t.get("symbol")))
    return results


async def _poll_one_pair(
    ave: AveClient,
    chain: str,
    token_id: str,
    pair_id: str,
    symbol: str | None,
    sem: asyncio.Semaphore,
) -> int:
    async with sem:
        try:
            raw = await ave.swap_transactions(pair_id, limit=TXS_PER_PAIR, sort="desc")
        except Exception as e:
            # surface the first failure each cycle so we see what's happening
            global _swap_err_logged
            if not _swap_err_logged:
                _swap_err_logged = True
                print(f"[tx_poller] swap_transactions error: {type(e).__name__}: {e}")
            return 0

    items = _coerce_list(raw)
    published = 0
    for tx in items:
        event = _normalize_swap(tx, chain, token_id, symbol)
        if event is None:
            continue
        await bus.publish(SWAP_EVENT, event)
        published += 1
    return published


_swap_err_logged = False


async def poll_once() -> dict:
    """Single pass: fetch trending, pull swaps, publish events."""
    global _swap_err_logged
    _swap_err_logged = False  # reset each cycle so we see recurring errors
    async with AveClient() as ave:
        pairs = await _get_tracked_pairs(ave)
        if not pairs:
            return {"pairs": 0, "events": 0}

        sem = asyncio.Semaphore(PARALLEL)
        counts = await asyncio.gather(
            *[_poll_one_pair(ave, c, t, p, s, sem) for c, t, p, s in pairs]
        )

    return {"pairs": len(pairs), "events": sum(counts)}


# ---------------------------------------------------------------------
# Long-running loop
# ---------------------------------------------------------------------

_stop = asyncio.Event()


async def run() -> None:
    """Forever loop until stop() is called."""
    print("[tx_poller] starting")
    while not _stop.is_set():
        try:
            stats = await poll_once()
            print(f"[tx_poller] cycle: pairs={stats['pairs']} events={stats['events']}")
        except Exception as e:
            print(f"[tx_poller] cycle error: {e}")

        try:
            await asyncio.wait_for(_stop.wait(), timeout=POLL_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            continue
    print("[tx_poller] stopped")


def stop() -> None:
    _stop.set()

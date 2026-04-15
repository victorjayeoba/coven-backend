"""
Bot position monitor — keeps open bot_trades priced using DexScreener.

Why this exists:
  - `position_monitor` batches prices for open SYSTEM trades via AVE. Those
    tokens are ones our contagion detector fired on, which are almost always
    in AVE's index.
  - Bot trades (copy trades) are on whatever the whale bought — often fresh
    memes AVE doesn't track at all. Without this, open bot positions sit at
    their entry price forever, showing 0% P&L.

What it does:
  Every N seconds:
    1. Pull every open bot_trade
    2. Group token_ids by chain (DexScreener expects `chainId` matching)
    3. Batch-fetch from DexScreener's free /tokens/{addr1,addr2,...} endpoint
       (up to 30 tokens per call, no auth, no rate-limit signup)
    4. Update each trade's current_price_usd + unrealized fields
    5. Publish PRICE_UPDATE so frontend SSE patches the bot detail row live

DexScreener is the right source here because:
  - It indexes every Solana/BSC DEX, including fresh memes
  - Same data the user sees in the token-page embed, so numbers stay consistent
  - Free, no key, 300 req/min plenty for our scale
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

import httpx

from app.db import mongo
from app.services.event_bus import PRICE_UPDATE, bus

INTERVAL_SECONDS = 15
BATCH_SIZE = 30   # DexScreener hard cap

DS_CHAIN_MAP = {
    "solana": "solana",
    "bsc": "bsc",
    "eth": "ethereum",
    "ethereum": "ethereum",
    "base": "base",
    "arbitrum": "arbitrum",
    "polygon": "polygon",
}

_stop = asyncio.Event()


def _split_token_id(token_id: str) -> tuple[str, str]:
    if "-" not in token_id:
        return token_id, ""
    idx = token_id.rfind("-")
    return token_id[:idx], token_id[idx + 1:].lower()


def _pct(current: float, entry: float) -> float:
    if not entry:
        return 0.0
    return (current / entry - 1.0) * 100.0


async def _fetch_prices(
    client: httpx.AsyncClient, addresses: list[str], chain: str
) -> dict[str, float]:
    """Return {address_lower: priceUsd} for the chain, picking the best pair per token."""
    if not addresses:
        return {}
    out: dict[str, float] = {}
    ds_chain = DS_CHAIN_MAP.get(chain, chain)

    for i in range(0, len(addresses), BATCH_SIZE):
        batch = addresses[i:i + BATCH_SIZE]
        joined = ",".join(batch)
        try:
            r = await client.get(
                f"https://api.dexscreener.com/latest/dex/tokens/{joined}",
                timeout=10,
            )
        except Exception as e:
            print(f"[bot_position_monitor] DS fetch failed: {e}")
            continue
        if r.status_code >= 400:
            continue
        try:
            data = r.json()
        except Exception:
            continue

        pairs = data.get("pairs") or []
        # Pick the highest-volume pair per token on the requested chain
        best: dict[str, dict] = {}
        for p in pairs:
            if not isinstance(p, dict):
                continue
            if p.get("chainId") != ds_chain:
                continue
            base = (p.get("baseToken") or {}).get("address", "").lower()
            if not base:
                continue
            prev = best.get(base)
            prev_vol = float((prev.get("volume") or {}).get("h24") or 0) if prev else -1
            cur_vol = float((p.get("volume") or {}).get("h24") or 0)
            if cur_vol > prev_vol:
                best[base] = p

        for addr, p in best.items():
            try:
                price = float(p.get("priceUsd"))
                if price > 0:
                    out[addr] = price
            except (TypeError, ValueError):
                pass

    return out


async def _cycle(client: httpx.AsyncClient) -> dict:
    db = mongo.db()
    opens = await db.bot_trades.find(
        {"status": "open"},
        {"token_id": 1, "entry": 1, "chain": 1},
    ).to_list(500)
    if not opens:
        return {"opens": 0}

    # Group addresses by chain
    by_chain: dict[str, list[str]] = {}
    by_chain_lookup: dict[str, dict[str, str]] = {}  # chain -> {addr_lower: token_id}
    for t in opens:
        tid: str = t.get("token_id") or ""
        addr, chain_from_id = _split_token_id(tid)
        chain = (t.get("chain") or chain_from_id or "").lower()
        if not addr or not chain:
            continue
        addr_lc = addr.lower()
        by_chain.setdefault(chain, []).append(addr)
        by_chain_lookup.setdefault(chain, {})[addr_lc] = tid

    # Fetch per chain in parallel
    tasks = [
        _fetch_prices(client, list(set(addrs)), chain)
        for chain, addrs in by_chain.items()
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Flatten into {token_id: price}
    price_map: dict[str, float] = {}
    for chain, addrs_res in zip(by_chain.keys(), results):
        if isinstance(addrs_res, Exception) or not isinstance(addrs_res, dict):
            continue
        for addr_lc, price in addrs_res.items():
            tid = by_chain_lookup.get(chain, {}).get(addr_lc)
            if tid:
                price_map[tid] = price

    updated = 0
    now = datetime.utcnow()
    for t in opens:
        price = price_map.get(t.get("token_id"))
        if price is None:
            continue
        entry = t.get("entry") or {}
        entry_price = float(entry.get("price_usd") or 0)
        amount = float(entry.get("amount_tokens") or 0)
        unrealized = (price - entry_price) * amount if entry_price else 0.0
        await db.bot_trades.update_one(
            {"_id": t["_id"]},
            {
                "$set": {
                    "current_price_usd": price,
                    "unrealized_pnl_usd": round(unrealized, 2),
                    "unrealized_pnl_pct": round(_pct(price, entry_price), 2),
                    "last_updated": now,
                }
            },
        )
        # Fan out to frontend SSE for live bot row updates
        await bus.publish(
            PRICE_UPDATE,
            {"token_id": t.get("token_id"), "price_usd": price},
        )
        updated += 1

    return {"opens": len(opens), "priced": len(price_map), "updated": updated}


async def run() -> None:
    print("[bot_position_monitor] starting")
    async with httpx.AsyncClient() as client:
        while not _stop.is_set():
            try:
                stats = await _cycle(client)
                if stats.get("opens"):
                    print(f"[bot_position_monitor] cycle: {stats}")
            except Exception as e:
                print(f"[bot_position_monitor] cycle error: {e}")
            try:
                await asyncio.wait_for(_stop.wait(), timeout=INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                continue
    print("[bot_position_monitor] stopped")


def stop() -> None:
    _stop.set()

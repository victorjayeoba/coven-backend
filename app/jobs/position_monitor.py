"""
Position monitor.

Every N seconds, batch-fetches current prices for all open paper trades
and updates current_price_usd + unrealized pnl fields.

Uses /v2/tokens/price which batches up to 200 tokens in a single call.
"""

from __future__ import annotations

import asyncio
from datetime import datetime

from app.db import mongo
from app.services.ave_client import AveClient

INTERVAL_SECONDS = 30
BATCH_SIZE = 100  # AVE batch price supports 200, be polite

_stop = asyncio.Event()


def _pct(current: float, entry: float) -> float:
    if not entry:
        return 0.0
    return (current / entry - 1.0) * 100.0


async def _cycle() -> dict:
    db = mongo.db()
    opens = await db.trades.find({"status": "open"}, {"token_id": 1, "entry": 1}).to_list(500)
    if not opens:
        return {"opens": 0}

    token_ids = list({t["token_id"] for t in opens})

    async with AveClient() as ave:
        price_map: dict[str, float] = {}
        for i in range(0, len(token_ids), BATCH_SIZE):
            batch = token_ids[i:i + BATCH_SIZE]
            try:
                data = await ave.batch_prices(batch)
            except Exception:
                continue
            if isinstance(data, dict):
                for tid, info in data.items():
                    if isinstance(info, dict):
                        try:
                            price_map[tid] = float(info.get("current_price_usd"))
                        except (TypeError, ValueError):
                            pass

    updated = 0
    for t in opens:
        price = price_map.get(t["token_id"])
        if price is None:
            continue
        entry_price = float((t.get("entry") or {}).get("price_usd") or 0)
        amount = float((t.get("entry") or {}).get("amount_tokens") or 0)
        unrealized = (price - entry_price) * amount if entry_price else 0.0
        await db.trades.update_one(
            {"_id": t["_id"]},
            {"$set": {
                "current_price_usd": price,
                "unrealized_pnl_usd": round(unrealized, 2),
                "unrealized_pnl_pct": round(_pct(price, entry_price), 2),
                "last_updated": datetime.utcnow(),
            }},
        )
        updated += 1

    return {"opens": len(opens), "priced": len(price_map), "updated": updated}


async def run() -> None:
    print("[position_monitor] starting")
    while not _stop.is_set():
        try:
            stats = await _cycle()
            if stats["opens"]:
                print(f"[position_monitor] cycle: {stats}")
        except Exception as e:
            print(f"[position_monitor] cycle error: {e}")
        try:
            await asyncio.wait_for(_stop.wait(), timeout=INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            continue
    print("[position_monitor] stopped")


def stop() -> None:
    _stop.set()

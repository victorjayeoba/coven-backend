"""
Exit monitor.

Subscribes to every SWAP event. For any SELL by a wallet that was in
entry_wallets of an OPEN paper trade on that token, marks it as distribution.
When a threshold fraction of entry wallets have sold, we close the trade.

This is the behavioral exit — not price-based. We exit because the smart
money that got us in is getting out.
"""

from __future__ import annotations

from datetime import datetime

from app.db import mongo
from app.models.trade import TradeExit
from app.services.event_bus import SWAP_EVENT, TRADE_CLOSED, bus

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

EXIT_DISTRIBUTION_FRACTION = 0.5    # exit when 50% of entry wallets have sold


# ---------------------------------------------------------------------
# Event handler
# ---------------------------------------------------------------------

async def on_swap(event: dict) -> None:
    if event.get("side") != "sell":
        return

    wallet = event.get("wallet")
    token_id = event.get("token_id")
    if not wallet or not token_id:
        return

    db = mongo.db()
    # Find any open trade on this token where the seller was one of our entry wallets
    trade = await db.trades.find_one({
        "token_id": token_id,
        "status": "open",
        "entry_wallets": wallet,
    })
    if not trade:
        return

    # Track distributed wallets (set-like via $addToSet)
    await db.trades.update_one(
        {"_id": trade["_id"]},
        {
            "$addToSet": {"distributed_wallets": wallet},
            "$set": {"last_updated": datetime.utcnow()},
        },
    )

    # Re-fetch to see the distributed set now
    fresh = await db.trades.find_one({"_id": trade["_id"]})
    entry_count = len(fresh.get("entry_wallets") or [])
    distributed_count = len(fresh.get("distributed_wallets") or [])
    if entry_count == 0:
        return

    fraction = distributed_count / entry_count
    if fraction < EXIT_DISTRIBUTION_FRACTION:
        return  # not enough cluster members have left yet

    # CLOSE — use latest known price as exit price
    exit_price = float(
        fresh.get("current_price_usd")
        or fresh.get("entry", {}).get("price_usd")
        or 0
    )
    entry_price = float(fresh.get("entry", {}).get("price_usd") or 0)
    size_usd = float(fresh.get("entry", {}).get("size_usd") or 0)
    amount = float(fresh.get("entry", {}).get("amount_tokens") or 0)

    pnl_usd = (exit_price - entry_price) * amount if entry_price else 0.0
    pnl_pct = (exit_price / entry_price - 1.0) * 100 if entry_price else 0.0

    exit_obj = TradeExit(
        price_usd=exit_price,
        amount_tokens=amount,
        timestamp=datetime.utcnow(),
        reason="cluster_distribution",
    )

    await db.trades.update_one(
        {"_id": trade["_id"]},
        {"$set": {
            "exit": exit_obj.model_dump(mode="json"),
            "pnl_usd": round(pnl_usd, 2),
            "pnl_pct": round(pnl_pct, 2),
            "status": "closed",
            "closed_at": datetime.utcnow(),
            "last_updated": datetime.utcnow(),
        }},
    )

    print(
        f"[TRADE CLOSE] {fresh.get('symbol') or token_id} "
        f"exit=${exit_price:.6f} "
        f"pnl={pnl_pct:+.2f}% (${pnl_usd:+.2f}) "
        f"reason=cluster_distribution"
    )

    await bus.publish(TRADE_CLOSED, {
        "trade_id": str(trade["_id"]),
        "token_id": token_id,
        "pnl_usd": round(pnl_usd, 2),
        "pnl_pct": round(pnl_pct, 2),
        "reason": "cluster_distribution",
    })


# ---------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------

def register() -> None:
    bus.subscribe(SWAP_EVENT, on_swap)

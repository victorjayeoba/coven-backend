"""
Execution engine (paper trading).

Subscribes to SIGNAL_SCORED events. For signals with status=exec/partial,
opens a paper trade: fetches current price, sizes the position, writes to
MongoDB, and publishes TRADE_OPENED.

Nothing hits a chain or a trading API — this is simulated. The architecture
is wired for real execution: swap `_get_entry_price()` + `_persist_trade()`
for AVE Trading API calls to go live.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from app.db import mongo
from app.models.trade import Trade, TradeEntry
from app.services.ave_client import AveClient
from app.services.event_bus import SIGNAL_SCORED, TRADE_OPENED, bus

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

DEFAULT_SIZE_USD = 200.0             # base position size for conviction >= exec
PARTIAL_FRACTION = 0.5               # half size for "partial"
MAX_POSITION_TVL_RATIO = 0.005       # never size > 0.5% of token TVL

# Single-user for hackathon demo
USER_ID = "demo"


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

async def _already_open(token_id: str) -> bool:
    """Don't open a second position on a token we're already holding."""
    db = mongo.db()
    existing = await db.trades.find_one({
        "user_id": USER_ID,
        "token_id": token_id,
        "status": "open",
    })
    return existing is not None


async def _get_entry_price(token_id: str, ave: AveClient) -> float | None:
    """Fetch current USD price for the token."""
    try:
        data = await ave.token_detail(token_id)
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    token = data.get("token") if isinstance(data.get("token"), dict) else data
    try:
        return float(token.get("current_price_usd"))
    except (TypeError, ValueError):
        return None


def _size_for_status(status: str, tvl_usd: float | None) -> float:
    base = DEFAULT_SIZE_USD
    if status == "partial":
        base *= PARTIAL_FRACTION

    # Cap at a fraction of TVL so we don't imagine trading more than the pool can absorb
    if tvl_usd and tvl_usd > 0:
        cap = tvl_usd * MAX_POSITION_TVL_RATIO
        base = min(base, cap)

    return round(max(0.0, base), 2)


# ---------------------------------------------------------------------
# Event handler
# ---------------------------------------------------------------------

async def on_signal_scored(signal: dict) -> None:
    status = signal.get("status")
    if status not in {"exec", "partial"}:
        return

    token_id = signal.get("token_id")
    if not token_id:
        return

    if await _already_open(token_id):
        return

    async with AveClient() as ave:
        entry_price = await _get_entry_price(token_id, ave)
    if not entry_price or entry_price <= 0:
        return

    size_usd = _size_for_status(status, signal.get("tvl_usd"))
    if size_usd <= 0:
        return

    amount = size_usd / entry_price

    trade = Trade(
        user_id=USER_ID,
        token_id=token_id,
        chain=signal.get("chain") or "",
        symbol=signal.get("symbol"),
        cluster_id=signal.get("cluster_id"),
        entry_wallets=signal.get("wallets_involved") or [],
        conviction_score=int(signal.get("conviction_score") or 0),
        risk_score=int(signal.get("risk_score") or 0),
        entry=TradeEntry(
            price_usd=entry_price,
            size_usd=size_usd,
            amount_tokens=amount,
            timestamp=datetime.utcnow(),
        ),
        current_price_usd=entry_price,
        is_paper=True,
        status="open",
    )

    db = mongo.db()
    doc = trade.model_dump(mode="json")
    result = await db.trades.insert_one(doc)
    doc["_id"] = str(result.inserted_id)

    await bus.publish(TRADE_OPENED, doc)

    print(
        f"[TRADE OPEN] {signal.get('symbol') or token_id} "
        f"@ ${entry_price:.6f} size=${size_usd:.2f} "
        f"conviction={trade.conviction_score} status={status}"
    )


# ---------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------

def register() -> None:
    bus.subscribe(SIGNAL_SCORED, on_signal_scored)

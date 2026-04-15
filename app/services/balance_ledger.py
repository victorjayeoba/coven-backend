"""
Atomic paper-wallet balance ops.

Trades open against a (user, chain) bucket. Before opening:
    ok = await try_debit(user_id, chain, size_usd)
    if not ok: refuse trade

On close:
    await credit(user_id, chain, size_usd + pnl_usd)   # = exit value

Both ops use Mongo-side atomic updates so two concurrent trades can't
both pass the check and overdraw the wallet. No transactions needed.
"""

from __future__ import annotations

from datetime import datetime

from bson import ObjectId

from app.db import mongo

SUPPORTED_CHAINS = {"solana", "bsc"}


def _oid(user_id: str) -> ObjectId | None:
    try:
        return ObjectId(user_id)
    except Exception:
        return None


async def try_debit(user_id: str, chain: str, amount: float) -> bool:
    """
    Atomically subtract `amount` from users.paper_balances[chain] iff the
    balance is enough. Returns True on success, False if insufficient or
    if user/chain is invalid.
    """
    if amount <= 0:
        return True              # no-op
    if chain not in SUPPORTED_CHAINS:
        return False
    oid = _oid(user_id)
    if oid is None:
        return False
    db = mongo.db()
    key = f"paper_balances.{chain}"
    res = await db.users.find_one_and_update(
        {"_id": oid, key: {"$gte": float(amount)}},
        {
            "$inc": {key: -float(amount)},
            "$set": {"paper_balances_updated_at": datetime.utcnow()},
        },
    )
    return res is not None


async def credit(user_id: str, chain: str, amount: float) -> None:
    """
    Add `amount` to users.paper_balances[chain]. Used on trade close to
    return original size + PnL. Negative amounts allowed (e.g. closing
    underwater) — the field can go to 0 but not below for normal flows
    because we always credit the exit value, not just the PnL.
    """
    if chain not in SUPPORTED_CHAINS:
        return
    oid = _oid(user_id)
    if oid is None:
        return
    db = mongo.db()
    key = f"paper_balances.{chain}"
    await db.users.update_one(
        {"_id": oid},
        {
            "$inc": {key: float(amount)},
            "$set": {"paper_balances_updated_at": datetime.utcnow()},
        },
    )


async def get_balance(user_id: str, chain: str) -> float:
    oid = _oid(user_id)
    if oid is None:
        return 0.0
    db = mongo.db()
    doc = await db.users.find_one({"_id": oid}, {"paper_balances": 1})
    pb = (doc or {}).get("paper_balances") or {}
    try:
        return float(pb.get(chain) or 0)
    except (TypeError, ValueError):
        return 0.0

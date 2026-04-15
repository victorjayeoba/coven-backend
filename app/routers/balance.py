"""
Paper wallet balances — per user, per network.

Before this was in the browser's localStorage (Zustand persist), which meant
Telegram couldn't touch it. Now the source of truth is Mongo:

  users.paper_balances = { "solana": 1000.0, "bsc": 500.0 }

Endpoints are thin; mutation is additive (deposit, reset). No enforcement of
balance-on-trade yet — bot trades still open freely; balance is a UX signal.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.auth.dependencies import get_current_user
from app.db import mongo

router = APIRouter(prefix="/api/balance", tags=["balance"])

Network = Literal["solana", "bsc"]
SUPPORTED: tuple[Network, ...] = ("solana", "bsc")


def _empty() -> dict[str, float]:
    return {n: 0.0 for n in SUPPORTED}


async def _read_balances(user_id: str) -> dict[str, float]:
    db = mongo.db()
    doc = await db.users.find_one(
        {"_id": ObjectId(user_id)}, {"paper_balances": 1}
    )
    raw = (doc or {}).get("paper_balances") or {}
    out = _empty()
    for n in SUPPORTED:
        try:
            out[n] = float(raw.get(n, 0) or 0)
        except (TypeError, ValueError):
            out[n] = 0.0
    return out


def _total(balances: dict[str, float]) -> float:
    return round(sum(balances.values()), 2)


@router.get("")
async def get_balances(user: dict = Depends(get_current_user)):
    balances = await _read_balances(user["id"])
    return {"balances": balances, "total": _total(balances)}


class DepositBody(BaseModel):
    network: Network
    amount: float = Field(gt=0)


@router.post("/deposit")
async def deposit(body: DepositBody, user: dict = Depends(get_current_user)):
    if body.network not in SUPPORTED:
        raise HTTPException(status_code=400, detail="Unsupported network")
    db = mongo.db()
    key = f"paper_balances.{body.network}"
    await db.users.update_one(
        {"_id": ObjectId(user["id"])},
        {
            "$inc": {key: float(body.amount)},
            "$set": {"paper_balances_updated_at": datetime.utcnow()},
        },
        upsert=False,
    )
    balances = await _read_balances(user["id"])
    return {"balances": balances, "total": _total(balances)}


@router.post("/reset")
async def reset(user: dict = Depends(get_current_user)):
    db = mongo.db()
    await db.users.update_one(
        {"_id": ObjectId(user["id"])},
        {"$set": {"paper_balances": _empty()}},
    )
    return {"balances": _empty(), "total": 0.0}

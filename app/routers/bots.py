"""
Bot CRUD + trade listing. All endpoints scoped to the current user.
The bot_runner service executes them live off the event bus.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.auth.dependencies import get_current_user
from app.db import mongo

router = APIRouter(prefix="/api/bots", tags=["bots"])


BotType = Literal["copy", "signal"]
BotStatus = Literal["active", "paused"]
Chain = Literal["solana", "bsc"]
SizeMode = Literal["fixed", "multiplier", "percent"]


class BotCreate(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    type: BotType
    chain: Chain

    size_usd: float = 100
    size_mode: SizeMode = "fixed"
    multiplier: float = 1
    percent_of_target: float = 5

    target_wallet: str = ""
    target_label: str = ""
    copy_exits: bool = True

    min_conviction: int = 70
    cluster_filter: str = ""

    take_profit_pct: float = 50
    stop_loss_pct: float = 20
    trailing_stop_pct: float = 0

    max_slippage_pct: float = 3
    max_concurrent: int = 5
    daily_loss_limit_usd: float = 500
    min_liquidity_usd: float = 10_000
    cooldown_min: int = 15
    anti_rug: bool = True


class BotUpdate(BaseModel):
    name: str | None = None
    status: BotStatus | None = None
    size_usd: float | None = None
    size_mode: SizeMode | None = None
    multiplier: float | None = None
    percent_of_target: float | None = None
    target_wallet: str | None = None
    target_label: str | None = None
    copy_exits: bool | None = None
    min_conviction: int | None = None
    cluster_filter: str | None = None
    take_profit_pct: float | None = None
    stop_loss_pct: float | None = None
    trailing_stop_pct: float | None = None
    max_slippage_pct: float | None = None
    max_concurrent: int | None = None
    daily_loss_limit_usd: float | None = None
    min_liquidity_usd: float | None = None
    cooldown_min: int | None = None
    anti_rug: bool | None = None


def _serialize(bot: dict) -> dict:
    out = dict(bot)
    out["id"] = str(out.pop("_id"))
    out.pop("target_wallet_lc", None)
    for k in ("created_at", "last_updated"):
        if isinstance(out.get(k), datetime):
            out[k] = out[k].isoformat()
    return out


@router.get("")
async def list_bots(user: dict = Depends(get_current_user)):
    db = mongo.db()
    out: list[dict] = []
    async for b in db.bots.find({"user_id": user["id"]}).sort("created_at", -1):
        out.append(_serialize(b))
    return out


@router.post("", status_code=201)
async def create_bot(body: BotCreate, user: dict = Depends(get_current_user)):
    now = datetime.utcnow()
    doc = body.model_dump()
    doc.update({
        "user_id": user["id"],
        "status": "active",
        "target_wallet_lc": (doc.get("target_wallet") or "").lower(),
        "stats": {"pnl_usd": 0.0, "trades": 0, "wins": 0},
        "created_at": now,
        "last_updated": now,
    })
    db = mongo.db()
    res = await db.bots.insert_one(doc)
    doc["_id"] = res.inserted_id
    return _serialize(doc)


def _oid(id_: str) -> ObjectId:
    try:
        return ObjectId(id_)
    except Exception:
        raise HTTPException(status_code=404, detail="Bot not found")


@router.get("/{bot_id}")
async def get_bot(bot_id: str, user: dict = Depends(get_current_user)):
    db = mongo.db()
    bot = await db.bots.find_one({"_id": _oid(bot_id), "user_id": user["id"]})
    if not bot:
        raise HTTPException(status_code=404, detail="Bot not found")
    return _serialize(bot)


@router.patch("/{bot_id}")
async def update_bot(
    bot_id: str, body: BotUpdate, user: dict = Depends(get_current_user)
):
    patch = {k: v for k, v in body.model_dump().items() if v is not None}
    if not patch:
        raise HTTPException(status_code=400, detail="No fields to update")
    if "target_wallet" in patch:
        patch["target_wallet_lc"] = (patch["target_wallet"] or "").lower()
    patch["last_updated"] = datetime.utcnow()

    db = mongo.db()
    res = await db.bots.find_one_and_update(
        {"_id": _oid(bot_id), "user_id": user["id"]},
        {"$set": patch},
        return_document=True,
    )
    if not res:
        raise HTTPException(status_code=404, detail="Bot not found")
    return _serialize(res)


@router.delete("/{bot_id}")
async def delete_bot(bot_id: str, user: dict = Depends(get_current_user)):
    db = mongo.db()
    oid = _oid(bot_id)
    res = await db.bots.delete_one({"_id": oid, "user_id": user["id"]})
    if res.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Bot not found")
    await db.bot_trades.delete_many({"bot_id": bot_id})
    return {"ok": True}


@router.get("/{bot_id}/trades")
async def bot_trades(
    bot_id: str,
    status: Literal["open", "closed", "all"] = Query("all"),
    limit: int = Query(100, ge=1, le=500),
    user: dict = Depends(get_current_user),
):
    db = mongo.db()
    # Auth check
    bot = await db.bots.find_one({"_id": _oid(bot_id), "user_id": user["id"]})
    if not bot:
        raise HTTPException(status_code=404, detail="Bot not found")

    q: dict = {"bot_id": bot_id}
    if status != "all":
        q["status"] = status

    out: list[dict] = []
    async for t in db.bot_trades.find(q).sort("opened_at", -1).limit(limit):
        t["id"] = str(t.pop("_id"))
        for k in ("opened_at", "closed_at", "last_updated"):
            if isinstance(t.get(k), datetime):
                t[k] = t[k].isoformat()
        entry = t.get("entry") or {}
        if isinstance(entry.get("timestamp"), datetime):
            entry["timestamp"] = entry["timestamp"].isoformat()
        xit = t.get("exit") or {}
        if isinstance(xit, dict) and isinstance(xit.get("timestamp"), datetime):
            xit["timestamp"] = xit["timestamp"].isoformat()
        out.append(t)
    return out

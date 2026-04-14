from datetime import datetime, timedelta

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query

from app.auth.dependencies import get_current_user
from app.db import mongo

router = APIRouter(prefix="/api/signals", tags=["signals"])


def _clean(doc: dict) -> dict:
    doc["id"] = str(doc.pop("_id"))
    return doc


@router.get("")
async def list_signals(
    _: dict = Depends(get_current_user),
    status: str | None = None,
    min_conviction: int = Query(0, ge=0, le=100),
    chain: str | None = None,
    limit: int = Query(50, ge=1, le=200),
):
    db = mongo.db()
    q: dict = {"conviction_score": {"$gte": min_conviction}}
    if status:
        q["status"] = status
    if chain:
        q["chain"] = chain
    cursor = db.signals.find(q).sort("detected_at", -1).limit(limit)
    return [_clean(d) async for d in cursor]


@router.get("/live")
async def live_signals(
    _: dict = Depends(get_current_user),
    minutes: int = Query(60, ge=1, le=24 * 60),
    limit: int = Query(50, ge=1, le=200),
):
    db = mongo.db()
    cutoff = (datetime.utcnow() - timedelta(minutes=minutes)).isoformat()
    cursor = (
        db.signals.find({"detected_at": {"$gte": cutoff}})
        .sort("detected_at", -1)
        .limit(limit)
    )
    return [_clean(d) async for d in cursor]


@router.get("/{signal_id}")
async def get_signal(signal_id: str, _: dict = Depends(get_current_user)):
    db = mongo.db()
    try:
        doc = await db.signals.find_one({"_id": ObjectId(signal_id)})
    except Exception:
        doc = None
    if not doc:
        raise HTTPException(status_code=404, detail="Signal not found")
    return _clean(doc)

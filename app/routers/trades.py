from fastapi import APIRouter, Depends, HTTPException, Query

from bson import ObjectId

from app.auth.dependencies import get_current_user
from app.db import mongo

router = APIRouter(prefix="/api/trades", tags=["trades"])


def _clean(doc: dict) -> dict:
    doc["id"] = str(doc.pop("_id"))
    return doc


@router.get("")
async def list_trades(
    _: dict = Depends(get_current_user),
    status: str | None = None,
    limit: int = Query(50, ge=1, le=200),
):
    db = mongo.db()
    q: dict = {}
    if status:
        q["status"] = status
    cursor = db.trades.find(q).sort("opened_at", -1).limit(limit)
    return [_clean(d) async for d in cursor]


@router.get("/active")
async def active_trades(_: dict = Depends(get_current_user)):
    db = mongo.db()
    cursor = db.trades.find({"status": "open"}).sort("opened_at", -1)
    return [_clean(d) async for d in cursor]


@router.get("/history")
async def trade_history(
    _: dict = Depends(get_current_user),
    limit: int = Query(100, ge=1, le=500),
):
    db = mongo.db()
    cursor = db.trades.find({"status": "closed"}).sort("closed_at", -1).limit(limit)
    return [_clean(d) async for d in cursor]


@router.get("/pnl")
async def pnl_summary(_: dict = Depends(get_current_user)):
    """Aggregate stats across all trades."""
    db = mongo.db()
    pipeline = [
        {"$match": {"status": "closed"}},
        {
            "$group": {
                "_id": None,
                "total_pnl_usd": {"$sum": "$pnl_usd"},
                "trade_count": {"$sum": 1},
                "wins": {"$sum": {"$cond": [{"$gt": ["$pnl_usd", 0]}, 1, 0]}},
                "losses": {"$sum": {"$cond": [{"$lte": ["$pnl_usd", 0]}, 1, 0]}},
                "avg_pnl_pct": {"$avg": "$pnl_pct"},
                "best_trade": {"$max": "$pnl_pct"},
                "worst_trade": {"$min": "$pnl_pct"},
            }
        },
    ]
    cursor = db.trades.aggregate(pipeline)
    summary = next(iter([d async for d in cursor]), None) or {}

    # Open positions stats
    unrealized_pipeline = [
        {"$match": {"status": "open"}},
        {
            "$group": {
                "_id": None,
                "open_count": {"$sum": 1},
                "unrealized_pnl_usd": {"$sum": "$unrealized_pnl_usd"},
            }
        },
    ]
    cursor2 = db.trades.aggregate(unrealized_pipeline)
    unrealized = next(iter([d async for d in cursor2]), None) or {}

    total_trades = (summary.get("trade_count") or 0)
    wins = summary.get("wins") or 0
    win_rate = (wins / total_trades * 100.0) if total_trades else 0.0

    return {
        "total_realized_pnl_usd": round(summary.get("total_pnl_usd") or 0.0, 2),
        "trade_count": total_trades,
        "wins": wins,
        "losses": summary.get("losses") or 0,
        "win_rate_pct": round(win_rate, 2),
        "avg_pnl_pct": round(summary.get("avg_pnl_pct") or 0.0, 2),
        "best_trade_pct": round(summary.get("best_trade") or 0.0, 2),
        "worst_trade_pct": round(summary.get("worst_trade") or 0.0, 2),
        "open_positions": unrealized.get("open_count") or 0,
        "unrealized_pnl_usd": round(unrealized.get("unrealized_pnl_usd") or 0.0, 2),
    }


@router.get("/{trade_id}")
async def get_trade(trade_id: str, _: dict = Depends(get_current_user)):
    db = mongo.db()
    try:
        doc = await db.trades.find_one({"_id": ObjectId(trade_id)})
    except Exception:
        doc = None
    if not doc:
        raise HTTPException(status_code=404, detail="Trade not found")
    return _clean(doc)

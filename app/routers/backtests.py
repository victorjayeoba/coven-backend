from fastapi import APIRouter, Depends, HTTPException, Query

from bson import ObjectId

from app.auth.dependencies import get_current_user
from app.db import mongo

router = APIRouter(prefix="/api/backtests", tags=["backtests"])


def _clean(doc: dict) -> dict:
    doc["id"] = str(doc.pop("_id"))
    return doc


@router.get("")
async def list_backtests(
    _: dict = Depends(get_current_user),
    limit: int = Query(50, ge=1, le=200),
    min_pnl_pct: float | None = None,
):
    db = mongo.db()
    q: dict = {}
    if min_pnl_pct is not None:
        q["peak_pnl_pct"] = {"$gte": min_pnl_pct}
    cursor = db.backtests.find(q).sort("peak_pnl_pct", -1).limit(limit)
    return [_clean(d) async for d in cursor]


@router.get("/summary")
async def summary(_: dict = Depends(get_current_user)):
    """Aggregate stats across all backtests — the demo headline numbers."""
    db = mongo.db()
    pipeline = [
        {
            "$group": {
                "_id": None,
                "tokens_tested": {"$addToSet": "$token_id"},
                "total_firings": {"$sum": 1},
                "avg_pnl_pct": {"$avg": "$realistic_pnl_pct"},
                "avg_peak_pnl_pct": {"$avg": "$peak_pnl_pct"},
                "best_pnl_pct": {"$max": "$peak_pnl_pct"},
                "best_realistic_pnl_pct": {"$max": "$realistic_pnl_pct"},
                "wins": {"$sum": {"$cond": [{"$gt": ["$realistic_pnl_pct", 0]}, 1, 0]}},
            }
        },
        {
            "$project": {
                "_id": 0,
                "tokens_tested": {"$size": "$tokens_tested"},
                "total_firings": 1,
                "avg_pnl_pct": {"$round": ["$avg_pnl_pct", 2]},
                "avg_peak_pnl_pct": {"$round": ["$avg_peak_pnl_pct", 2]},
                "best_pnl_pct": {"$round": ["$best_pnl_pct", 2]},
                "best_realistic_pnl_pct": {"$round": ["$best_realistic_pnl_pct", 2]},
                "wins": 1,
            }
        },
    ]
    cursor = db.backtests.aggregate(pipeline)
    result = [doc async for doc in cursor]
    return result[0] if result else {
        "tokens_tested": 0,
        "total_firings": 0,
        "avg_pnl_pct": 0,
        "avg_peak_pnl_pct": 0,
        "best_pnl_pct": 0,
        "best_realistic_pnl_pct": 0,
        "wins": 0,
    }


@router.get("/{backtest_id}")
async def get_backtest(backtest_id: str, _: dict = Depends(get_current_user)):
    db = mongo.db()
    try:
        doc = await db.backtests.find_one({"_id": ObjectId(backtest_id)})
    except Exception:
        doc = None
    if not doc:
        raise HTTPException(status_code=404, detail="Backtest not found")
    return _clean(doc)

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from bson import ObjectId

from app.auth.dependencies import get_current_user
from app.db import mongo
from app.services.ave_client import AveClient
from app.services.balance_ledger import credit, try_debit
from app.services.event_bus import BOT_TRADE_OPENED, TRADE_CLOSED, bus

router = APIRouter(prefix="/api/trades", tags=["trades"])


def _clean(doc: dict) -> dict:
    doc["id"] = str(doc.pop("_id"))
    return doc


def _clean_bot(doc: dict) -> dict:
    """Shape a bot_trade row so it matches the main trades response."""
    doc["id"] = str(doc.pop("_id"))
    doc["source"] = "bot"
    # Older bot_trades stored user_id / bot_id as ObjectId — stringify so
    # FastAPI's JSON serializer doesn't choke.
    if isinstance(doc.get("user_id"), ObjectId):
        doc["user_id"] = str(doc["user_id"])
    if isinstance(doc.get("bot_id"), ObjectId):
        doc["bot_id"] = str(doc["bot_id"])
    # Unrealized P&L for open bot trades (trades collection has this materialized
    # by position_monitor; bot_trades rely on current_price_usd + entry).
    entry = doc.get("entry") or {}
    entry_price = float(entry.get("price_usd") or 0)
    amount = float(entry.get("amount_tokens") or 0)
    curr = float(doc.get("current_price_usd") or entry_price or 0)
    if doc.get("status") == "open" and entry_price > 0:
        doc["unrealized_pnl_usd"] = round((curr - entry_price) * amount, 2)
        doc["unrealized_pnl_pct"] = round(
            (curr / entry_price - 1.0) * 100.0, 2
        )
    return doc


def _compute_hold_sec(doc: dict) -> dict:
    """Add hold_duration_sec for closed trades."""
    opened = doc.get("opened_at")
    closed = doc.get("closed_at")
    if opened and closed:
        try:
            o = opened if isinstance(opened, datetime) else datetime.fromisoformat(str(opened).replace("Z", ""))
            c = closed if isinstance(closed, datetime) else datetime.fromisoformat(str(closed).replace("Z", ""))
            doc["hold_duration_sec"] = int((c - o).total_seconds())
        except Exception:
            pass
    return doc


@router.get("")
async def list_trades(
    user: dict = Depends(get_current_user),
    status: str | None = None,
    limit: int = Query(50, ge=1, le=200),
):
    db = mongo.db()
    q: dict = {}
    if status:
        q["status"] = status

    sys_trades = [
        _clean(d)
        async for d in db.trades.find(q).sort("opened_at", -1).limit(limit)
    ]
    for t in sys_trades:
        t["source"] = t.get("source") or "signal"

    bot_q = dict(q)
    bot_q["user_id"] = user["id"]
    uid_str = user["id"]
    uid_variants: list = [uid_str]
    try:
        uid_variants.append(ObjectId(uid_str))
    except Exception:
        pass
    bot_q["user_id"] = {"$in": uid_variants}
    bot_trades = [
        _clean_bot(d)
        async for d in db.bot_trades.find(bot_q).sort("opened_at", -1).limit(limit)
    ]

    merged = sys_trades + bot_trades
    merged.sort(key=lambda t: str(t.get("opened_at") or ""), reverse=True)
    return merged[:limit]


@router.get("/active")
async def active_trades(user: dict = Depends(get_current_user)):
    db = mongo.db()
    sys_trades = [
        _clean(d)
        async for d in db.trades.find({"status": "open"}).sort("opened_at", -1)
    ]
    for t in sys_trades:
        t["source"] = t.get("source") or "signal"

    # Match user_id as string OR ObjectId — older bot_trades may have either form.
    uid_str = user["id"]
    uid_variants: list = [uid_str]
    try:
        uid_variants.append(ObjectId(uid_str))
    except Exception:
        pass

    bot_trades = [
        _clean_bot(d)
        async for d in db.bot_trades.find(
            {"status": "open", "user_id": {"$in": uid_variants}}
        ).sort("opened_at", -1)
    ]
    merged = sys_trades + bot_trades
    merged.sort(key=lambda t: str(t.get("opened_at") or ""), reverse=True)
    return merged


@router.get("/history")
async def trade_history(
    user: dict = Depends(get_current_user),
    limit: int = Query(100, ge=1, le=500),
):
    db = mongo.db()
    sys_trades = [
        _compute_hold_sec(_clean(d))
        async for d in db.trades.find({"status": "closed"})
        .sort("closed_at", -1)
        .limit(limit)
    ]
    for t in sys_trades:
        t["source"] = t.get("source") or "signal"
    uid_str = user["id"]
    uid_variants: list = [uid_str]
    try:
        uid_variants.append(ObjectId(uid_str))
    except Exception:
        pass
    bot_trades = [
        _compute_hold_sec(_clean_bot(d))
        async for d in db.bot_trades.find(
            {"status": "closed", "user_id": {"$in": uid_variants}}
        )
        .sort("closed_at", -1)
        .limit(limit)
    ]
    merged = sys_trades + bot_trades
    merged.sort(key=lambda t: str(t.get("closed_at") or ""), reverse=True)
    return merged[:limit]


@router.get("/pnl")
async def pnl_summary(user: dict = Depends(get_current_user)):
    """Aggregate stats across system trades + this user's bot trades."""
    db = mongo.db()

    closed_pipeline = [
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

    async def _agg(collection, extra_match: dict | None = None) -> dict:
        pipe = list(closed_pipeline)
        if extra_match:
            pipe[0] = {"$match": {**pipe[0]["$match"], **extra_match}}
        cursor = collection.aggregate(pipe)
        return next(iter([d async for d in cursor]), None) or {}

    uid_str = user["id"]
    uid_variants: list = [uid_str]
    try:
        uid_variants.append(ObjectId(uid_str))
    except Exception:
        pass

    sys_closed = await _agg(db.trades)
    bot_closed = await _agg(db.bot_trades, {"user_id": {"$in": uid_variants}})

    total_pnl = (sys_closed.get("total_pnl_usd") or 0.0) + (
        bot_closed.get("total_pnl_usd") or 0.0
    )
    total_trades = (sys_closed.get("trade_count") or 0) + (
        bot_closed.get("trade_count") or 0
    )
    wins = (sys_closed.get("wins") or 0) + (bot_closed.get("wins") or 0)
    losses = (sys_closed.get("losses") or 0) + (bot_closed.get("losses") or 0)

    # avg_pnl_pct — weighted by trade count
    sys_avg = sys_closed.get("avg_pnl_pct") or 0.0
    bot_avg = bot_closed.get("avg_pnl_pct") or 0.0
    sys_ct = sys_closed.get("trade_count") or 0
    bot_ct = bot_closed.get("trade_count") or 0
    avg_pnl_pct = (
        (sys_avg * sys_ct + bot_avg * bot_ct) / total_trades if total_trades else 0.0
    )

    best = max(
        sys_closed.get("best_trade") or float("-inf"),
        bot_closed.get("best_trade") or float("-inf"),
    )
    worst = min(
        sys_closed.get("worst_trade") or float("inf"),
        bot_closed.get("worst_trade") or float("inf"),
    )
    if best == float("-inf"):
        best = 0.0
    if worst == float("inf"):
        worst = 0.0

    win_rate = (wins / total_trades * 100.0) if total_trades else 0.0

    # Open positions — include bot_trades unrealized P&L by computing on the fly
    open_count = await db.trades.count_documents({"status": "open"})
    bot_open_count = await db.bot_trades.count_documents(
        {"status": "open", "user_id": {"$in": uid_variants}}
    )

    # Sys unrealized is already materialized on trade docs
    sys_unrealized_cur = db.trades.aggregate([
        {"$match": {"status": "open"}},
        {"$group": {"_id": None, "u": {"$sum": "$unrealized_pnl_usd"}}},
    ])
    sys_unrealized = (next(iter([d async for d in sys_unrealized_cur]), None) or {}).get("u") or 0.0

    # Bot unrealized — compute per-row since we don't materialize it
    bot_unrealized = 0.0
    async for d in db.bot_trades.find(
        {"status": "open", "user_id": {"$in": uid_variants}},
        {"entry.price_usd": 1, "entry.amount_tokens": 1, "current_price_usd": 1},
    ):
        entry = d.get("entry") or {}
        entry_price = float(entry.get("price_usd") or 0)
        amount = float(entry.get("amount_tokens") or 0)
        curr = float(d.get("current_price_usd") or entry_price or 0)
        if entry_price > 0:
            bot_unrealized += (curr - entry_price) * amount

    return {
        "total_realized_pnl_usd": round(total_pnl, 2),
        "trade_count": total_trades,
        "wins": wins,
        "losses": losses,
        "win_rate_pct": round(win_rate, 2),
        "avg_pnl_pct": round(avg_pnl_pct, 2),
        "best_trade_pct": round(best, 2),
        "worst_trade_pct": round(worst, 2),
        "open_positions": open_count + bot_open_count,
        "unrealized_pnl_usd": round(sys_unrealized + bot_unrealized, 2),
    }


class OpenTradeBody(BaseModel):
    token_id: str = Field(..., description="`<address>-<chain>` form used everywhere else")
    size_usd: float = Field(..., gt=0)
    symbol: str | None = None


@router.post("/open")
async def open_manual_trade(
    body: OpenTradeBody, user: dict = Depends(get_current_user)
):
    """User-initiated paper swap (the wallet Swap button in the TopBar)."""
    token_id = body.token_id.strip()
    if "-" not in token_id:
        raise HTTPException(status_code=400, detail="token_id must be <address>-<chain>")
    chain = token_id.rsplit("-", 1)[1]
    if chain not in ("solana", "bsc"):
        raise HTTPException(status_code=400, detail=f"unsupported chain: {chain}")

    # Reuse bot_runner's price resolver (AVE → Jupiter fallback) so manual
    # swaps can hit Pump.fun / Raydium tokens too.
    from app.services.bot_runner import _entry_price
    entry_price = await _entry_price(token_id)
    if not entry_price or entry_price <= 0:
        raise HTTPException(status_code=502, detail="Could not fetch price for token")

    size_usd = float(body.size_usd)
    debited = await try_debit(user["id"], chain, size_usd)
    if not debited:
        raise HTTPException(status_code=400, detail=f"Insufficient {chain} balance")

    amount_tokens = size_usd / entry_price
    now = datetime.utcnow()
    doc = {
        "bot_id": None,                # manual trade, no parent bot
        "user_id": user["id"],
        "token_id": token_id,
        "chain": chain,
        "symbol": body.symbol,
        "entry": {
            "price_usd": entry_price,
            "size_usd": round(size_usd, 2),
            "amount_tokens": amount_tokens,
            "timestamp": now,
            "trigger": {"source": "manual"},
        },
        "exit": None,
        "current_price_usd": entry_price,
        "peak_price_usd": entry_price,
        "take_profit_pct": 0.0,
        "stop_loss_pct": 0.0,
        "trailing_stop_pct": 0.0,
        "status": "open",
        "is_paper": True,
        "source": "manual",
        "opened_at": now,
        "closed_at": None,
        "last_updated": now,
    }

    db = mongo.db()
    try:
        res = await db.bot_trades.insert_one(doc)
    except Exception as e:
        await credit(user["id"], chain, size_usd)
        raise HTTPException(status_code=500, detail=f"Trade insert failed: {e}")

    doc["_id"] = str(res.inserted_id)
    await bus.publish(
        BOT_TRADE_OPENED,
        {"trade_id": doc["_id"], "token_id": token_id, "source": "manual"},
    )
    return _clean_bot(doc)


@router.post("/{trade_id}/close")
async def close_trade(
    trade_id: str, user: dict = Depends(get_current_user)
):
    """Manually close an open paper trade (system or bot) at the latest known price."""
    db = mongo.db()
    try:
        oid = ObjectId(trade_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Trade not found")

    # Check system trades first
    trade = await db.trades.find_one({"_id": oid})
    collection = db.trades
    is_bot_trade = False
    if not trade:
        trade = await db.bot_trades.find_one({"_id": oid, "user_id": user["id"]})
        collection = db.bot_trades
        is_bot_trade = True
    if not trade:
        raise HTTPException(status_code=404, detail="Trade not found")
    if trade.get("status") != "open":
        raise HTTPException(status_code=400, detail="Trade is not open")

    # Use current_price_usd if we have it, else fetch live from AVE.
    exit_price = float(trade.get("current_price_usd") or 0)
    if exit_price <= 0:
        async with AveClient() as ave:
            try:
                data = await ave.token_detail(trade["token_id"])
            except Exception:
                data = None
        tok = (
            data.get("token")
            if isinstance(data, dict) and isinstance(data.get("token"), dict)
            else data
        )
        try:
            exit_price = float(tok.get("current_price_usd")) if tok else 0.0
        except (TypeError, ValueError):
            exit_price = 0.0
    if exit_price <= 0:
        exit_price = float(trade.get("entry", {}).get("price_usd") or 0)
    if exit_price <= 0:
        raise HTTPException(status_code=502, detail="Could not determine exit price")

    entry = trade.get("entry") or {}
    entry_price = float(entry.get("price_usd") or 0)
    amount = float(entry.get("amount_tokens") or 0)
    pnl_usd = (exit_price - entry_price) * amount if entry_price else 0.0
    pnl_pct = (exit_price / entry_price - 1.0) * 100 if entry_price else 0.0
    now = datetime.utcnow()

    await collection.update_one(
        {"_id": oid},
        {
            "$set": {
                "exit": {
                    "price_usd": exit_price,
                    "amount_tokens": amount,
                    "timestamp": now,
                    "reason": "manual",
                },
                "current_price_usd": exit_price,
                "pnl_usd": round(pnl_usd, 2),
                "pnl_pct": round(pnl_pct, 2),
                "status": "closed",
                "closed_at": now,
                "last_updated": now,
            }
        },
    )

    # If this is a bot trade, credit exit value back to the user's wallet
    # and bump the parent bot's rollup stats
    if is_bot_trade:
        from app.services.balance_ledger import credit
        chain = trade.get("chain")
        user_id = trade.get("user_id")
        exit_value = max(0.0, float(entry.get("size_usd") or 0) + pnl_usd)
        if user_id and chain and exit_value > 0:
            await credit(user_id, chain, exit_value)

    if is_bot_trade and trade.get("bot_id"):
        try:
            bot_oid = ObjectId(trade["bot_id"])
        except Exception:
            bot_oid = None
        if bot_oid is not None:
            inc: dict = {"stats.pnl_usd": round(pnl_usd, 2)}
            if pnl_usd > 0:
                inc["stats.wins"] = 1
            await db.bots.update_one({"_id": bot_oid}, {"$inc": inc})

    await bus.publish(
        TRADE_CLOSED,
        {
            "trade_id": trade_id,
            "token_id": trade.get("token_id"),
            "pnl_usd": round(pnl_usd, 2),
            "pnl_pct": round(pnl_pct, 2),
            "reason": "manual",
            "source": "bot" if is_bot_trade else "signal",
        },
    )

    fresh = await collection.find_one({"_id": oid})
    return _clean_bot(fresh) if is_bot_trade else _clean(fresh)


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

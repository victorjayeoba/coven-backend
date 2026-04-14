import asyncio
from datetime import datetime, timedelta
from typing import Any

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query

from app.auth.dependencies import get_current_user
from app.db import mongo
from app.services.ave_client import AveClient

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
    doc = await _find_signal_or_backtest(db, signal_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Signal not found")
    return _clean(doc)


async def _find_signal_or_backtest(db, signal_id: str):
    """Drawer can be opened from either live signals or backtests — try both."""
    for coll in ("signals", "backtests"):
        try:
            doc = await db[coll].find_one({"_id": ObjectId(signal_id)})
            if doc:
                return doc
        except Exception:
            continue
    return None


def _parse_tx_time(tx: dict) -> float | None:
    v = tx.get("time") or tx.get("tx_time") or tx.get("block_time") or tx.get("timestamp")
    if v is None:
        return None
    try:
        f = float(v)
        return f / 1000.0 if f > 1e12 else f
    except (TypeError, ValueError):
        pass
    if isinstance(v, str):
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00")).timestamp()
        except Exception:
            return None
    return None


@router.get("/{signal_id}/cluster-trades")
async def cluster_trades(signal_id: str, _: dict = Depends(get_current_user)):
    """
    Returns every buy/sell by the signal's cluster wallets on the signal's token.
    Used by the token detail drawer to show coordination in action.
    """
    db = mongo.db()
    signal = await _find_signal_or_backtest(db, signal_id)
    if not signal:
        raise HTTPException(status_code=404, detail="Signal not found")

    token_id = signal.get("token_id")
    chain = signal.get("chain") or (token_id.rsplit("-", 1)[-1] if "-" in (token_id or "") else None)
    wallets = signal.get("wallets_involved") or []
    if not token_id or not chain or not wallets:
        return {"trades": [], "entries": [], "exits": []}

    token_contract = token_id.rsplit("-", 1)[0]
    target_lower = token_contract.lower()

    async with AveClient() as ave:
        async def _for_wallet(addr: str):
            try:
                raw = await ave.wallet_token_tx(
                    wallet_address=addr,
                    chain=chain,
                    token_address=token_contract,
                    page_size=100,
                )
            except Exception:
                return addr, []
            items = (
                raw.get("result") if isinstance(raw, dict) else raw
            ) or []
            if not isinstance(items, list):
                items = []
            return addr, items

        pairs = await asyncio.gather(*[_for_wallet(w) for w in wallets])

    trades: list[dict[str, Any]] = []
    for wallet_addr, txs in pairs:
        for tx in txs:
            if not isinstance(tx, dict):
                continue
            from_addr = (tx.get("from_address") or "").lower()
            to_addr = (tx.get("to_address") or "").lower()
            side = None
            if from_addr == target_lower:
                side = "sell"
            elif to_addr == target_lower:
                side = "buy"
            if not side:
                continue

            ts = _parse_tx_time(tx)
            if ts is None:
                continue

            def _f(v):
                try:
                    return float(v) if v is not None else None
                except (TypeError, ValueError):
                    return None

            # USD value — compute from {amount × price} on either side
            amount_usd_f = _f(tx.get("amount_usd"))
            if amount_usd_f is None:
                from_amt = _f(tx.get("from_amount"))
                from_price = _f(tx.get("from_price_usd"))
                if from_amt is not None and from_price is not None:
                    amount_usd_f = from_amt * from_price
                else:
                    to_amt = _f(tx.get("to_amount"))
                    to_price = _f(tx.get("to_price_usd"))
                    if to_amt is not None and to_price is not None:
                        amount_usd_f = to_amt * to_price

            # Target-token price at trade time
            if side == "buy":
                price_f = _f(tx.get("to_price_usd"))
            else:
                price_f = _f(tx.get("from_price_usd"))

            trades.append({
                "wallet": wallet_addr,
                "side": side,
                "amount_usd": amount_usd_f,
                "price_usd": price_f,
                "timestamp": ts,
                "tx_hash": tx.get("transaction"),
            })

    trades.sort(key=lambda t: t["timestamp"])

    entries = [t for t in trades if t["side"] == "buy"]
    exits = [t for t in trades if t["side"] == "sell"]

    return {
        "signal_id": signal_id,
        "token_id": token_id,
        "chain": chain,
        "cluster_id": signal.get("cluster_id"),
        "wallets": wallets,
        "trades": trades,
        "entries": entries,
        "exits": exits,
    }

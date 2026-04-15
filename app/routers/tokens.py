import asyncio

from fastapi import APIRouter, Body, Depends, HTTPException, Query

from app.auth.dependencies import get_current_user
from app.db import mongo
from app.services.ave_client import AveClient

router = APIRouter(prefix="/api/tokens", tags=["tokens"])


@router.get("/{token_id}/signals")
async def token_signals(token_id: str, _: dict = Depends(get_current_user)):
    """Every live signal and backtest record that fired on this token."""
    db = mongo.db()
    out: list[dict] = []

    async for s in db.signals.find({"token_id": token_id}).sort("detected_at", -1):
        s["id"] = str(s.pop("_id"))
        s["source"] = "live"
        out.append(s)
    async for b in db.backtests.find({"token_id": token_id}).sort(
        "first_entry_at", -1
    ):
        b["id"] = str(b.pop("_id"))
        b["source"] = "backtest"
        b["detected_at"] = b.pop("first_entry_at", None)
        out.append(b)

    return out


@router.get("/{token_id}/smart-holders")
async def token_smart_holders(
    token_id: str, _: dict = Depends(get_current_user)
):
    """Wallets in our graph that currently hold this token."""
    db = mongo.db()
    out: list[dict] = []
    async for w in db.wallets.find(
        {"tokens.token_id": token_id},
        {"_id": 0, "address": 1, "chain": 1, "alpha_score": 1,
         "total_profit": 1, "tokens": 1},
    ):
        tokens = w.get("tokens") or []
        # Find the matching token entry for balance/pnl
        match = next((t for t in tokens if t.get("token_id") == token_id), {})

        # Cluster lookup
        cluster = await db.clusters.find_one(
            {"wallet_addresses": w["address"]},
            {"_id": 0, "cluster_id": 1},
        )
        out.append({
            "address": w["address"],
            "chain": w.get("chain"),
            "alpha_score": w.get("alpha_score", 0.0),
            "total_profit": w.get("total_profit"),
            "balance_usd": match.get("balance_usd"),
            "total_profit_token": match.get("total_profit"),
            "unrealized_profit_token": match.get("unrealized_profit"),
            "cluster_id": cluster.get("cluster_id") if cluster else None,
        })

    # Sort by balance size
    out.sort(key=lambda h: float(h.get("balance_usd") or 0), reverse=True)
    return out


@router.post("/batch")
async def batch_details(
    payload: dict = Body(..., example={"token_ids": ["0xabc-bsc"]}),
    _: dict = Depends(get_current_user),
):
    """
    Given a list of token_ids, fetch their details in parallel and return
    a map { token_id: <detail_dict> }. Trims to the fields the dashboard
    needs so the response stays small.
    """
    token_ids = payload.get("token_ids") or []
    if not isinstance(token_ids, list):
        raise HTTPException(status_code=400, detail="token_ids must be a list")
    token_ids = [str(t) for t in token_ids if t][:50]
    if not token_ids:
        return {}

    async with AveClient() as ave:
        results = await asyncio.gather(
            *[ave.token_detail(tid) for tid in token_ids],
            return_exceptions=True,
        )

    out: dict = {}
    for tid, data in zip(token_ids, results):
        if isinstance(data, Exception) or not isinstance(data, dict):
            continue
        tok = data.get("token") if isinstance(data.get("token"), dict) else data
        if not isinstance(tok, dict):
            continue

        def _num(k):
            v = tok.get(k)
            try:
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        out[tid] = {
            "symbol": tok.get("symbol"),
            "name": tok.get("name"),
            "chain": tok.get("chain"),
            "logo_url": tok.get("logo_url"),
            "price_usd": _num("current_price_usd"),
            "price_change_1h": _num("token_price_change_1h"),
            "price_change_24h": _num("token_price_change_24h"),
            "market_cap": _num("market_cap"),
            "fdv": _num("fdv"),
            "tvl": _num("main_pair_tvl") or _num("tvl"),
            "volume_24h": _num("token_tx_volume_usd_24h"),
            "tx_count_24h": _num("token_tx_count_24h"),
            "makers_24h": _num("token_makers_24h"),
            "launch_at": tok.get("launch_at"),
            "risk_score": tok.get("risk_score"),
        }
    return out


@router.get("/search")
async def search(
    q: str = Query(..., min_length=1),
    chain: str | None = None,
    limit: int = Query(20, ge=1, le=100),
    _: dict = Depends(get_current_user),
):
    async with AveClient() as ave:
        try:
            return await ave.search_tokens(q, chain=chain, limit=limit)
        except Exception as e:
            raise HTTPException(status_code=502, detail=str(e))


@router.get("/trending")
async def trending(
    chain: str = Query(..., description="solana, bsc, eth, ..."),
    page: int = Query(0, ge=0),
    page_size: int = Query(50, ge=1, le=100),
    _: dict = Depends(get_current_user),
):
    async with AveClient() as ave:
        try:
            return await ave.trending(chain, page=page, page_size=page_size)
        except Exception as e:
            raise HTTPException(status_code=502, detail=str(e))


@router.get("/{token_id}")
async def token_detail(token_id: str, _: dict = Depends(get_current_user)):
    async with AveClient() as ave:
        try:
            return await ave.token_detail(token_id)
        except Exception as e:
            raise HTTPException(status_code=502, detail=str(e))


@router.get("/{token_id}/candles")
async def token_candles(
    token_id: str,
    interval: int = Query(60, description="minutes: 1,5,15,30,60,240,1440"),
    limit: int = Query(100, ge=1, le=1000),
    _: dict = Depends(get_current_user),
):
    async with AveClient() as ave:
        try:
            return await ave.klines_by_token(token_id, interval=interval, limit=limit)
        except Exception as e:
            raise HTTPException(status_code=502, detail=str(e))


@router.get("/{token_id}/txs")
async def token_txs(
    token_id: str,
    limit: int = Query(50, ge=1, le=200),
    _: dict = Depends(get_current_user),
):
    """Recent swap transactions on this token's main pair."""
    async with AveClient() as ave:
        try:
            detail = await ave.token_detail(token_id)
        except Exception as e:
            raise HTTPException(status_code=502, detail=str(e))

        tok = detail.get("token") if isinstance(detail, dict) and isinstance(detail.get("token"), dict) else detail
        if not isinstance(tok, dict):
            return []

        chain = tok.get("chain") or (token_id.split("-")[-1] if "-" in token_id else None)
        pair_addr = (
            tok.get("main_pair_address")
            or tok.get("main_pair")
            or tok.get("pair_address")
        )
        if not pair_addr and isinstance(tok.get("pairs"), list) and tok["pairs"]:
            first = tok["pairs"][0]
            if isinstance(first, dict):
                pair_addr = first.get("pair") or first.get("pair_address")
        if not pair_addr or not chain:
            return []

        pair_id = f"{pair_addr}-{chain}"
        try:
            return await ave.swap_transactions(pair_id, limit=limit)
        except Exception as e:
            raise HTTPException(status_code=502, detail=str(e))


@router.get("/{token_id}/risk")
async def token_risk(token_id: str, _: dict = Depends(get_current_user)):
    async with AveClient() as ave:
        try:
            return await ave.contract_risk(token_id)
        except Exception as e:
            raise HTTPException(status_code=502, detail=str(e))

import asyncio

from fastapi import APIRouter, Body, Depends, HTTPException, Query

from app.auth.dependencies import get_current_user
from app.services.ave_client import AveClient

router = APIRouter(prefix="/api/tokens", tags=["tokens"])


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


@router.get("/{token_id}/risk")
async def token_risk(token_id: str, _: dict = Depends(get_current_user)):
    async with AveClient() as ave:
        try:
            return await ave.contract_risk(token_id)
        except Exception as e:
            raise HTTPException(status_code=502, detail=str(e))

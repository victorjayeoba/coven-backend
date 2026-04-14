from fastapi import APIRouter, Depends, HTTPException, Query

from app.auth.dependencies import get_current_user
from app.services.ave_client import AveClient

router = APIRouter(prefix="/api/tokens", tags=["tokens"])


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

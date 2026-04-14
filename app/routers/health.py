import time

from fastapi import APIRouter

from app.db import mongo
from app.services.ave_client import AveClient

router = APIRouter(tags=["health"])

_started_at = time.time()


@router.get("/health")
async def health():
    mongo_ok = await mongo.ping()
    async with AveClient() as ave:
        ave_ok = await ave.ping()
    return {
        "status": "ok" if (mongo_ok and ave_ok) else "degraded",
        "mongo": "connected" if mongo_ok else "unreachable",
        "ave": "reachable" if ave_ok else "unreachable",
        "uptime_seconds": int(time.time() - _started_at),
    }

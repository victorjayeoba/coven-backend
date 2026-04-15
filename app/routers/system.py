"""
Lightweight runtime status endpoints for the frontend (countdown timers,
poller heartbeats, etc.). Public — no auth.
"""

from datetime import datetime, timezone

from fastapi import APIRouter

from app.jobs import rank_poller

router = APIRouter(prefix="/api/system", tags=["system"])


@router.get("/next-scan-at")
def next_scan_at() -> dict:
    ts = rank_poller.next_fire_at()
    if ts is None:
        return {"next_scan_at": None}
    return {
        "next_scan_at": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
        "interval_seconds": rank_poller.POLL_INTERVAL,
    }

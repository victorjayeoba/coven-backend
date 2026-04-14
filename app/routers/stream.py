"""
Server-Sent Events (SSE) endpoint for pushing live signals to the frontend.

Any connected client receives every SIGNAL_FIRED and SIGNAL_SCORED event
as soon as they happen — no polling needed.
"""

import asyncio
import json
from typing import AsyncGenerator

from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse

from app.auth.dependencies import get_current_user
from app.services.event_bus import (
    PRICE_UPDATE,
    SIGNAL_FIRED,
    SIGNAL_SCORED,
    bus,
)

router = APIRouter(prefix="/api/stream", tags=["stream"])


def _format_sse(event: str, data: dict) -> bytes:
    payload = json.dumps(data, default=str)
    return f"event: {event}\ndata: {payload}\n\n".encode("utf-8")


async def _signal_stream(request: Request) -> AsyncGenerator[bytes, None]:
    queue: asyncio.Queue[tuple[str, dict]] = asyncio.Queue(maxsize=2000)

    async def _on_fired(payload: dict) -> None:
        try:
            queue.put_nowait(("signal.fired", payload))
        except asyncio.QueueFull:
            pass

    async def _on_scored(payload: dict) -> None:
        try:
            queue.put_nowait(("signal.scored", payload))
        except asyncio.QueueFull:
            pass

    async def _on_price(payload: dict) -> None:
        try:
            queue.put_nowait(("price.update", payload))
        except asyncio.QueueFull:
            pass

    bus.subscribe(SIGNAL_FIRED, _on_fired)
    bus.subscribe(SIGNAL_SCORED, _on_scored)
    bus.subscribe(PRICE_UPDATE, _on_price)

    yield _format_sse("connected", {"status": "ok"})

    try:
        while True:
            if await request.is_disconnected():
                break
            try:
                event_name, payload = await asyncio.wait_for(
                    queue.get(), timeout=20.0
                )
                yield _format_sse(event_name, payload)
            except asyncio.TimeoutError:
                yield b": heartbeat\n\n"
    finally:
        bus.unsubscribe(SIGNAL_FIRED, _on_fired)
        bus.unsubscribe(SIGNAL_SCORED, _on_scored)
        bus.unsubscribe(PRICE_UPDATE, _on_price)


@router.get("/signals")
async def stream_signals(
    request: Request,
    _: dict = Depends(get_current_user),
):
    """SSE stream of live signal events."""
    return StreamingResponse(
        _signal_stream(request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@router.post("/test-signal")
async def emit_test_signal(_: dict = Depends(get_current_user)):
    """
    Debug helper: publish a fake signal so you can verify the
    stream → frontend pipeline without waiting for a real one.
    """
    from datetime import datetime
    from uuid import uuid4

    fake = {
        "id": f"test-{uuid4().hex[:12]}",
        "signal_type": "alpha",
        "alpha_tier": "elite",
        "token_id": "0xtest-bsc",
        "chain": "bsc",
        "symbol": "TEST",
        "cluster_id": None,
        "wallets_involved": ["0xTEST_WALLET"],
        "cluster_size_total": 0,
        "cluster_active_count": 1,
        "wallet_alpha_scores": [12.34],
        "avg_alpha_score": 12.34,
        "time_window_seconds": 0,
        "first_entry_at": datetime.utcnow().isoformat(),
        "last_entry_at": datetime.utcnow().isoformat(),
        "detected_at": datetime.utcnow().isoformat(),
        "conviction_score": 80,
        "status": "exec",
    }
    await bus.publish(SIGNAL_FIRED, fake)
    return {"ok": True, "id": fake["id"]}

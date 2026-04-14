"""
Signal enricher.

Subscribes to raw ContagionSignal events emitted by the detector, fetches
risk + token context from AVE, scores the signal, and persists the enriched
version to MongoDB.

This is the bridge between Phase 3 (detection) and Phases 5+ (execution).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from app.db import mongo
from app.services import conviction_scorer, risk_checker
from app.services.ave_client import AveClient
from app.services.event_bus import SIGNAL_FIRED, SIGNAL_SCORED, bus


# ---------------------------------------------------------------------
# Enrichment
# ---------------------------------------------------------------------

async def _fetch_token_context(token_id: str, ave: AveClient) -> dict:
    """Pull TVL + age data for scoring. Best-effort."""
    ctx: dict[str, Any] = {"tvl_usd": None, "age_hours": None}
    try:
        data = await ave.token_detail(token_id)
    except Exception:
        return ctx
    if not isinstance(data, dict):
        return ctx

    # TVL may be on main_pair_tvl or direct tvl
    tvl = data.get("main_pair_tvl") or data.get("tvl")
    try:
        ctx["tvl_usd"] = float(tvl) if tvl is not None else None
    except (TypeError, ValueError):
        pass

    launch = data.get("launch_at") or data.get("created_at")
    if launch:
        try:
            launched_ts = float(launch)
            now_ts = datetime.utcnow().timestamp()
            ctx["age_hours"] = max(0.0, (now_ts - launched_ts) / 3600.0)
        except (TypeError, ValueError):
            pass

    return ctx


async def _wallet_alpha_scores(addresses: list[str]) -> list[float]:
    """Look up alpha_score values for involved wallets from MongoDB."""
    if not addresses:
        return []
    db = mongo.db()
    cursor = db.wallets.find(
        {"address": {"$in": addresses}},
        {"alpha_score": 1, "_id": 0},
    )
    scores: list[float] = []
    async for doc in cursor:
        val = doc.get("alpha_score") or 0.0
        try:
            scores.append(float(val))
        except (TypeError, ValueError):
            pass
    return scores


# ---------------------------------------------------------------------
# Event handler
# ---------------------------------------------------------------------

async def on_signal_fired(signal: dict) -> None:
    """Enrich a raw signal: run risk + scoring, update its DB record."""
    token_id = signal.get("token_id")
    if not token_id:
        return

    async with AveClient() as ave:
        risk_result = await risk_checker.check_token(token_id, ave=ave)
        ctx = await _fetch_token_context(token_id, ave)

    alpha_scores = await _wallet_alpha_scores(signal.get("wallets_involved") or [])

    scored = conviction_scorer.score(
        cluster_active=signal.get("cluster_active_count", 0),
        cluster_total=signal.get("cluster_size_total", 0),
        wallet_alpha_scores=alpha_scores,
        risk_score=risk_result["risk_score"],
        tvl_usd=ctx.get("tvl_usd"),
        token_age_hours=ctx.get("age_hours"),
    )

    # Block if risk check failed (honeypot, mint, etc.)
    final_status = "blocked" if not risk_result["passed"] else scored["status"]

    enriched: dict[str, Any] = {
        "conviction_score": scored["conviction_score"],
        "conviction_breakdown": scored["breakdown"],
        "status": final_status,
        "risk_score": risk_result["risk_score"],
        "risk_passed": risk_result["passed"],
        "risk_issues": risk_result["issues"],
        "risk_details": risk_result["details"],
        "tvl_usd": ctx.get("tvl_usd"),
        "token_age_hours": ctx.get("age_hours"),
        "enriched_at": datetime.utcnow().isoformat(),
    }

    # Update the latest signal document for this (token, cluster)
    db = mongo.db()
    await db.signals.update_one(
        {
            "token_id": token_id,
            "cluster_id": signal.get("cluster_id"),
            "detected_at": signal.get("detected_at"),
        },
        {"$set": enriched},
    )

    # Fan out enriched signal so downstream (execution) can subscribe
    await bus.publish(SIGNAL_SCORED, {**signal, **enriched})

    print(
        f"[SCORED] cluster #{signal.get('cluster_id')} on "
        f"{signal.get('symbol') or token_id} → "
        f"conviction={scored['conviction_score']} risk={risk_result['risk_score']} "
        f"status={final_status.upper()}"
    )
    if risk_result["issues"]:
        print(f"         issues: {', '.join(risk_result['issues'])}")


# ---------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------

def register() -> None:
    bus.subscribe(SIGNAL_FIRED, on_signal_fired)

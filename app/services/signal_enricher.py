"""
Signal enricher.

For CLUSTER signals: existing formula (cluster-weighted).
For ALPHA signals: single-wallet conviction that weights wallet alpha and
token momentum heavily (since there's no cluster coordination to lean on).
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any

from app.db import mongo
from app.services import conviction_scorer, risk_checker
from app.services.ave_client import AveClient
from app.services.event_bus import SIGNAL_FIRED, SIGNAL_SCORED, bus


# ---------------------------------------------------------------------
# Momentum score — token-level, 0..1
# ---------------------------------------------------------------------

def _num(v: Any) -> float | None:
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def compute_momentum(token: dict | None) -> float:
    """
    Returns a 0..1 score describing how much a token has momentum right now.
    Uses AVE's built-in fields on the token detail response.
    """
    if not isinstance(token, dict):
        return 0.0

    change_24h = _num(token.get("token_price_change_24h")) or 0.0
    change_1h = _num(token.get("token_price_change_1h")) or 0.0
    vol_1h = _num(token.get("token_tx_volume_usd_1h")) or 0.0
    vol_4h = _num(token.get("token_tx_volume_usd_4h")) or 0.0
    buyers_1h = _num(token.get("token_buyers_1h")) or 0.0
    buyers_4h = _num(token.get("token_buyers_4h")) or 0.0

    # 1) Price momentum: reward positive 24h + 1h change, log-scaled
    def _pct_factor(pct: float) -> float:
        if pct <= 0:
            return 0.0
        return min(1.0, math.log1p(pct) / math.log1p(100))  # 0% → 0, 100% → 1

    price_factor = 0.6 * _pct_factor(change_24h) + 0.4 * _pct_factor(change_1h)

    # 2) Volume acceleration: 1h vs 4h/4
    avg_per_hour_4h = vol_4h / 4.0 if vol_4h > 0 else 0.0
    accel = vol_1h / avg_per_hour_4h if avg_per_hour_4h > 0 else 0.0
    accel_factor = min(1.0, accel / 3.0)  # 3x = full

    # 3) Buyers growth
    avg_buyers_per_hour_4h = buyers_4h / 4.0 if buyers_4h > 0 else 0.0
    buyer_ratio = (
        buyers_1h / avg_buyers_per_hour_4h
        if avg_buyers_per_hour_4h > 0
        else 0.0
    )
    buyer_factor = min(1.0, buyer_ratio / 2.5)

    score = (
        price_factor * 0.5
        + accel_factor * 0.3
        + buyer_factor * 0.2
    )
    return round(max(0.0, min(1.0, score)), 3)


# ---------------------------------------------------------------------
# Context fetch
# ---------------------------------------------------------------------

async def _fetch_token_context(token_id: str, ave: AveClient) -> dict:
    ctx: dict[str, Any] = {"tvl_usd": None, "age_hours": None, "token": None}
    try:
        data = await ave.token_detail(token_id)
    except Exception:
        return ctx
    if not isinstance(data, dict):
        return ctx

    tok = data.get("token") if isinstance(data.get("token"), dict) else data
    if not isinstance(tok, dict):
        return ctx

    ctx["token"] = tok
    ctx["tvl_usd"] = _num(tok.get("main_pair_tvl") or tok.get("tvl"))
    launch = _num(tok.get("launch_at") or tok.get("created_at"))
    if launch and launch > 0:
        ctx["age_hours"] = max(0.0, (datetime.utcnow().timestamp() - launch) / 3600.0)
    return ctx


async def _wallet_alpha_scores(addresses: list[str]) -> list[float]:
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
# Alpha-type scoring
# ---------------------------------------------------------------------

def _score_alpha_signal(
    *,
    wallet_alpha: float,
    momentum: float,
    risk_score: int,
    tvl_usd: float | None,
    age_hours: float | None,
) -> dict:
    """
    Alpha-solo conviction: wallet alpha and token momentum dominate.

    weights:
      alpha       0.35
      momentum    0.30
      risk        0.20
      liquidity   0.10
      age         0.05
    """
    # Alpha factor — log-squash so 1.0 → 0.5, 3.0 → 0.75, 10.0 → 1.0
    alpha_factor = min(1.0, math.log1p(wallet_alpha) / math.log1p(10)) if wallet_alpha > 0 else 0.0
    momentum_factor = max(0.0, min(1.0, momentum))
    risk_factor = max(0.0, min(1.0, risk_score / 100.0))
    liq_factor = 0.0
    if tvl_usd and tvl_usd > 10_000:
        liq_factor = min(1.0, tvl_usd / 100_000)
    age_factor = 0.5
    if age_hours is not None:
        if age_hours < 6:
            age_factor = age_hours / 6.0
        elif age_hours > 72:
            decay = max(0.0, 1.0 - ((age_hours - 72) / 288))
            age_factor = max(0.5, decay)
        else:
            age_factor = 1.0

    weighted = (
        alpha_factor * 0.35
        + momentum_factor * 0.30
        + risk_factor * 0.20
        + liq_factor * 0.10
        + age_factor * 0.05
    )
    conviction = int(round(weighted * 100))

    if conviction >= 70:
        status = "exec"
    elif conviction >= 40:
        status = "partial"
    else:
        status = "watch"

    return {
        "conviction_score": conviction,
        "conviction_breakdown": {
            "alpha": round(alpha_factor, 3),
            "momentum": round(momentum_factor, 3),
            "risk": round(risk_factor, 3),
            "liquidity": round(liq_factor, 3),
            "age": round(age_factor, 3),
        },
        "status": status,
    }


# ---------------------------------------------------------------------
# Event handler
# ---------------------------------------------------------------------

async def on_signal_fired(signal: dict) -> None:
    token_id = signal.get("token_id")
    if not token_id:
        return

    signal_type = signal.get("signal_type") or "cluster"

    # rank_stack signals self-score in rank_poller and publish SIGNAL_SCORED
    # directly. The cluster/alpha formula here needs cluster wallet counts
    # which they don't have — would overwrite their conviction with garbage.
    if signal_type == "rank_stack":
        return

    async with AveClient() as ave:
        risk_result = await risk_checker.check_token(token_id, ave=ave)
        ctx = await _fetch_token_context(token_id, ave)

    momentum = compute_momentum(ctx.get("token"))

    if signal_type == "alpha":
        wallet_alpha = (
            (signal.get("wallet_alpha_scores") or [0.0])[0]
            if signal.get("wallet_alpha_scores")
            else 0.0
        )
        scored = _score_alpha_signal(
            wallet_alpha=float(wallet_alpha or 0.0),
            momentum=momentum,
            risk_score=risk_result["risk_score"],
            tvl_usd=ctx.get("tvl_usd"),
            age_hours=ctx.get("age_hours"),
        )

        # Tiered quality gate:
        # "strong" alpha (α 1.0–3.0) requires real momentum to execute.
        # Elite alpha (α ≥ 3.0) passes regardless.
        tier = signal.get("alpha_tier")
        if tier == "strong" and momentum < 0.3:
            scored["status"] = "watch"
    else:
        # Cluster scoring (existing formula)
        alpha_scores = (
            signal.get("wallet_alpha_scores")
            or await _wallet_alpha_scores(signal.get("wallets_involved") or [])
        )
        scored = conviction_scorer.score(
            cluster_active=signal.get("cluster_active_count", 0),
            cluster_total=signal.get("cluster_size_total", 0),
            wallet_alpha_scores=alpha_scores,
            risk_score=risk_result["risk_score"],
            tvl_usd=ctx.get("tvl_usd"),
            token_age_hours=ctx.get("age_hours"),
        )

    final_status = (
        "blocked" if not risk_result["passed"] else scored["status"]
    )

    enriched: dict[str, Any] = {
        "conviction_score": scored["conviction_score"],
        "conviction_breakdown": scored["breakdown"]
        if "breakdown" in scored
        else scored.get("conviction_breakdown"),
        "momentum_score": momentum,
        "status": final_status,
        "risk_score": risk_result["risk_score"],
        "risk_passed": risk_result["passed"],
        "risk_issues": risk_result["issues"],
        "risk_details": risk_result["details"],
        "tvl_usd": ctx.get("tvl_usd"),
        "token_age_hours": ctx.get("age_hours"),
        "enriched_at": datetime.utcnow().isoformat(),
    }
    if signal.get("alpha_tier"):
        enriched["alpha_tier"] = signal["alpha_tier"]

    db = mongo.db()
    await db.signals.update_one(
        {
            "token_id": token_id,
            "cluster_id": signal.get("cluster_id"),
            "detected_at": signal.get("detected_at"),
        },
        {"$set": enriched},
    )

    await bus.publish(SIGNAL_SCORED, {**signal, **enriched})

    print(
        f"[SCORED {signal_type}] "
        f"{signal.get('symbol') or token_id} → "
        f"conviction={scored['conviction_score']} "
        f"momentum={momentum:.2f} "
        f"risk={risk_result['risk_score']} "
        f"status={final_status.upper()}"
    )


def register() -> None:
    bus.subscribe(SIGNAL_FIRED, on_signal_fired)

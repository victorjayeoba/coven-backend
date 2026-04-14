"""
Conviction scorer.

Given a raw cluster signal plus enrichment (risk report, token detail),
produces a 0-100 conviction score and a status tag.

Formula (all weighted 0..1, then summed × 100):

  cluster_size_factor  × 0.25    how many cluster wallets joined vs total
  alpha_factor         × 0.25    avg alpha_score of involved wallets
  risk_factor          × 0.25    contract risk check score
  liquidity_factor     × 0.15    token liquidity depth (TVL)
  age_factor           × 0.10    token recency (newer = riskier but bigger upside)
"""

from __future__ import annotations

import time
from typing import Any, Literal

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

LIQUIDITY_TARGET_USD = 100_000     # 100k TVL = fully healthy liquidity
AGE_OPTIMAL_HOURS = 6              # 6h old = sweet spot (new enough, not zero-history)
AGE_DECAY_HOURS = 72               # tokens older than 72h get a tiny penalty

THRESHOLDS = {
    "exec": 70,       # >= 70 → auto-execute full
    "partial": 40,    # 40-69 → half size + alert
    # < 40 → watch only
}


Status = Literal["exec", "partial", "watch", "blocked"]


# ---------------------------------------------------------------------
# Per-factor scoring
# ---------------------------------------------------------------------

def _cluster_size_factor(active: int, total: int) -> float:
    """More cluster members involved = higher confidence."""
    if total <= 0:
        return 0.0
    # Cap at 5 active wallets = full credit.
    # 2/5 = 0.40, 3/5 = 0.60, 5/5 = 1.00
    return min(1.0, active / min(5, total))


def _alpha_factor(alpha_scores: list[float]) -> float:
    """
    Average alpha_score across involved wallets, mapped into [0,1].
    AVE's profit_rate can be 0..many — we squash logarithmically.
    """
    if not alpha_scores:
        return 0.0
    avg = sum(alpha_scores) / len(alpha_scores)
    if avg <= 0:
        return 0.0
    # 0.5 profit_rate → ~0.35, 1.0 → 0.50, 2.0 → 0.67, 5.0 → 0.85, 10.0 → 1.0
    import math
    return min(1.0, math.log1p(avg) / math.log1p(10.0))


def _risk_factor(risk_score: int) -> float:
    """risk_score is already 0..100 — just normalize."""
    return max(0.0, min(1.0, risk_score / 100.0))


def _liquidity_factor(tvl_usd: float | None) -> float:
    """Below $10k = 0. At $100k+ = 1.0. Linear scale between."""
    if not tvl_usd or tvl_usd <= 10_000:
        return 0.0
    return min(1.0, tvl_usd / LIQUIDITY_TARGET_USD)


def _age_factor(token_age_hours: float | None) -> float:
    """
    Newer tokens get slightly less credit (higher risk).
    Optimal age = ~6 hours; full credit beyond that.
    Very old tokens lose a tiny bit (less momentum).
    """
    if token_age_hours is None:
        return 0.5
    if token_age_hours < AGE_OPTIMAL_HOURS:
        return token_age_hours / AGE_OPTIMAL_HOURS
    if token_age_hours > AGE_DECAY_HOURS:
        decay = max(0.0, 1.0 - ((token_age_hours - AGE_DECAY_HOURS) / (AGE_DECAY_HOURS * 4)))
        return max(0.5, decay)
    return 1.0


# ---------------------------------------------------------------------
# Main scorer
# ---------------------------------------------------------------------

def score(
    *,
    cluster_active: int,
    cluster_total: int,
    wallet_alpha_scores: list[float],
    risk_score: int,
    tvl_usd: float | None,
    token_age_hours: float | None,
) -> dict:
    """
    Returns:
      {
        "conviction_score": int 0..100,
        "breakdown": { factor: float 0..1, ... },
        "status": "exec" | "partial" | "watch",
      }
    """
    size = _cluster_size_factor(cluster_active, cluster_total)
    alpha = _alpha_factor(wallet_alpha_scores)
    risk = _risk_factor(risk_score)
    liq = _liquidity_factor(tvl_usd)
    age = _age_factor(token_age_hours)

    weighted = (
        size * 0.25 +
        alpha * 0.25 +
        risk * 0.25 +
        liq * 0.15 +
        age * 0.10
    )
    conviction = int(round(weighted * 100))

    if conviction >= THRESHOLDS["exec"]:
        status: Status = "exec"
    elif conviction >= THRESHOLDS["partial"]:
        status = "partial"
    else:
        status = "watch"

    return {
        "conviction_score": conviction,
        "breakdown": {
            "cluster_size": round(size, 3),
            "alpha": round(alpha, 3),
            "risk": round(risk, 3),
            "liquidity": round(liq, 3),
            "age": round(age, 3),
        },
        "status": status,
    }

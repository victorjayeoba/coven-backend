"""
Risk checker.

For a given token, fetches AVE's contract risk report and applies a set
of rules. Returns a pass/fail decision plus a 0-100 risk score (higher = safer).
"""

from __future__ import annotations

from typing import Any

from app.services.ave_client import AveClient

# ---------------------------------------------------------------------
# Config — tunable thresholds
# ---------------------------------------------------------------------

MAX_BUY_TAX = 0.10          # 10% buy tax
MAX_SELL_TAX = 0.10         # 10% sell tax


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _truthy(val: Any) -> bool:
    """AVE uses 1/0, true/false, strings — normalize."""
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return val != 0
    if isinstance(val, str):
        return val.lower() in {"1", "true", "yes"}
    return False


def _as_float(val: Any, default: float = 0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------
# Rule evaluation
# ---------------------------------------------------------------------

def evaluate(risk_data: dict | None) -> dict:
    """
    Given AVE's /contracts/{id} response data, return:
      {
        "passed":       bool,
        "risk_score":   0-100 (higher = safer),
        "issues":       list of human strings,
        "details":      raw subset for storage,
      }
    """
    if not risk_data or not isinstance(risk_data, dict):
        return {
            "passed": False,
            "risk_score": 0,
            "issues": ["no risk data available"],
            "details": {},
        }

    issues: list[str] = []
    deductions = 0

    # Extract — AVE's field naming has drifted; try variants
    is_honeypot = _truthy(
        risk_data.get("is_honeypot") or risk_data.get("honeypot")
    )
    has_mint = _truthy(
        risk_data.get("has_mint_method") or risk_data.get("mintable")
    )
    lp_locked = _truthy(
        risk_data.get("lp_locked")
        or (not _truthy(risk_data.get("is_lp_not_locked")))
    )
    buy_tax = _as_float(risk_data.get("buy_tax") or risk_data.get("buy_tax_pct"))
    sell_tax = _as_float(risk_data.get("sell_tax") or risk_data.get("sell_tax_pct"))
    owner_renounced = _truthy(
        risk_data.get("owner_renounced")
        or (not _truthy(risk_data.get("has_not_renounced")))
    )

    # Rule checks
    if is_honeypot:
        issues.append("honeypot detected")
        deductions += 100  # hard block
    if has_mint:
        issues.append("mint method enabled")
        deductions += 40
    if not lp_locked:
        issues.append("LP not locked")
        deductions += 30
    if buy_tax > MAX_BUY_TAX:
        issues.append(f"buy tax {buy_tax * 100:.1f}% exceeds limit")
        deductions += 20
    if sell_tax > MAX_SELL_TAX:
        issues.append(f"sell tax {sell_tax * 100:.1f}% exceeds limit")
        deductions += 25
    if not owner_renounced:
        issues.append("owner not renounced")
        deductions += 10  # soft, not blocking

    risk_score = max(0, 100 - deductions)

    # Passed if no hard failures
    hard_fail = is_honeypot or has_mint or not lp_locked
    passed = not hard_fail

    return {
        "passed": passed,
        "risk_score": risk_score,
        "issues": issues,
        "details": {
            "is_honeypot": is_honeypot,
            "has_mint_method": has_mint,
            "lp_locked": lp_locked,
            "buy_tax": buy_tax,
            "sell_tax": sell_tax,
            "owner_renounced": owner_renounced,
        },
    }


async def check_token(token_id: str, ave: AveClient | None = None) -> dict:
    """Fetch risk from AVE and evaluate. Handles client lifecycle if not provided."""
    if ave is not None:
        try:
            data = await ave.contract_risk(token_id)
        except Exception as e:
            return {
                "passed": False,
                "risk_score": 0,
                "issues": [f"risk fetch failed: {type(e).__name__}"],
                "details": {},
            }
        return evaluate(data)

    # No client provided — manage one ourselves
    async with AveClient() as owned:
        try:
            data = await owned.contract_risk(token_id)
        except Exception as e:
            return {
                "passed": False,
                "risk_score": 0,
                "issues": [f"risk fetch failed: {type(e).__name__}"],
                "details": {},
            }
        return evaluate(data)

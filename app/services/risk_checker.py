"""
Risk checker.

Fetches AVE's /contracts/{id} report and evaluates using the REAL fields
AVE returns. Returns pass/fail + a 0-100 safety score (higher = safer).

Key AVE conventions:
  - Boolean flags arrive as int (0/1) OR string ("0"/"1")
  - A value of -1 on honeypot-type fields means UNTESTED, not dangerous.
  - `cannot_buy` / `cannot_sell_all` == "1" are the real honeypot signals.
  - `trust_list` == "1" → verified safe token.
  - `ai_report.summary.risk_level` ∈ {"low", "medium", "high"}.
"""

from __future__ import annotations

from typing import Any

from app.services.ave_client import AveClient

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

MAX_BUY_TAX = 0.10
MAX_SELL_TAX = 0.10


# ---------------------------------------------------------------------
# Parsers (AVE uses mixed types for flags)
# ---------------------------------------------------------------------

def _flag(val: Any) -> bool | None:
    """
    Normalize AVE flag to bool | None.
      "1", 1, True   → True
      "0", 0, False  → False
      "-1", -1       → None (untested)
      other / None   → None
    """
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        if val == 0:
            return False
        if val == 1:
            return True
        return None  # -1 or other → unknown
    if isinstance(val, str):
        s = val.strip()
        if s in {"1", "true", "yes"}:
            return True
        if s in {"0", "false", "no"}:
            return False
        return None
    return None


def _as_float(val: Any, default: float = 0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------

def evaluate(risk_data: dict | None) -> dict:
    if not risk_data or not isinstance(risk_data, dict):
        return {
            "passed": False,
            "risk_score": 0,
            "issues": ["no risk data available"],
            "details": {},
        }

    # -- read flags --
    cannot_buy       = _flag(risk_data.get("cannot_buy"))
    cannot_sell_all  = _flag(risk_data.get("cannot_sell_all"))
    is_honeypot      = _flag(risk_data.get("is_honeypot"))
    has_mint         = _flag(risk_data.get("has_mint_method"))
    hidden_owner     = _flag(risk_data.get("hidden_owner"))
    transfer_pause   = _flag(risk_data.get("transfer_pausable"))
    take_back        = _flag(risk_data.get("can_take_back_ownership"))
    has_black        = _flag(risk_data.get("has_black_method"))
    anti_whale_mod   = _flag(risk_data.get("anti_whale_modifiable"))
    slippage_mod     = _flag(risk_data.get("slippage_modifiable"))
    trust_list       = _flag(risk_data.get("trust_list"))

    buy_tax   = _as_float(risk_data.get("buy_tax"))
    sell_tax  = _as_float(risk_data.get("sell_tax"))
    transfer_tax = _as_float(risk_data.get("transfer_tax"))

    # AVE's risk signals
    ai_level = (
        (risk_data.get("ai_report") or {})
        .get("summary", {})
        .get("risk_level")
    )
    if isinstance(ai_level, str):
        ai_level = ai_level.lower()

    # -- rule engine --
    issues: list[str] = []
    hard_block = False

    if cannot_buy is True:
        issues.append("cannot buy")
        hard_block = True
    if cannot_sell_all is True:
        issues.append("cannot sell all (honeypot-like)")
        hard_block = True
    if is_honeypot is True:
        issues.append("honeypot confirmed")
        hard_block = True
    if has_mint is True:
        issues.append("mint method enabled")
        hard_block = True
    if hidden_owner is True:
        issues.append("hidden owner")
        hard_block = True
    if transfer_pause is True:
        issues.append("transfer pausable")
        hard_block = True
    if take_back is True:
        issues.append("ownership can be reclaimed")
        hard_block = True
    if buy_tax > MAX_BUY_TAX:
        issues.append(f"buy tax {buy_tax*100:.1f}% too high")
        hard_block = True
    if sell_tax > MAX_SELL_TAX:
        issues.append(f"sell tax {sell_tax*100:.1f}% too high")
        hard_block = True

    # -- soft issues (no block, score reduction) --
    soft_deductions = 0
    if has_black is True:
        issues.append("blacklist method present")
        soft_deductions += 15
    if anti_whale_mod is True:
        issues.append("anti-whale modifiable")
        soft_deductions += 10
    if slippage_mod is True:
        issues.append("slippage modifiable")
        soft_deductions += 10
    if transfer_tax and transfer_tax > 0:
        issues.append(f"transfer tax {transfer_tax*100:.1f}%")
        soft_deductions += 10

    # -- compute safety score --
    if trust_list is True:
        base_score = 95
    elif ai_level == "low":
        base_score = 85
    elif ai_level == "medium":
        base_score = 60
    elif ai_level == "high":
        base_score = 30
    else:
        # Fall back to AVE's numeric risk_score (higher = riskier)
        ave_risk = _as_float(risk_data.get("risk_score"), default=50.0)
        base_score = max(0, 100 - int(ave_risk))

    risk_score = base_score - soft_deductions
    if hard_block:
        risk_score = 0
    risk_score = max(0, min(100, int(risk_score)))

    return {
        "passed": not hard_block,
        "risk_score": risk_score,
        "issues": issues,
        "details": {
            "ai_risk_level": ai_level,
            "trust_list": trust_list,
            "cannot_buy": cannot_buy,
            "cannot_sell_all": cannot_sell_all,
            "is_honeypot": is_honeypot,
            "has_mint_method": has_mint,
            "hidden_owner": hidden_owner,
            "transfer_pausable": transfer_pause,
            "can_take_back_ownership": take_back,
            "has_black_method": has_black,
            "buy_tax": buy_tax,
            "sell_tax": sell_tax,
            "transfer_tax": transfer_tax,
        },
    }


async def check_token(token_id: str, ave: AveClient | None = None) -> dict:
    """Fetch risk from AVE and evaluate."""
    async def _run(client: AveClient) -> dict:
        try:
            data = await client.contract_risk(token_id)
        except Exception as e:
            return {
                "passed": False,
                "risk_score": 0,
                "issues": [f"risk fetch failed: {type(e).__name__}"],
                "details": {},
            }
        return evaluate(data)

    if ave is not None:
        return await _run(ave)
    async with AveClient() as owned:
        return await _run(owned)

"""
Telegram link + preferences endpoints.

Data model lives on the `users` document:
  users.telegram = {
    chat_id: int,
    username: str | None,
    first_name: str | None,
    linked_at: datetime,
    prefs: {
      signals: bool,      # SIGNAL_FIRED notifications
      trades: bool,       # BOT_TRADE_OPENED / CLOSED notifications
      mute_until: datetime | None,
    }
  }

Pending link codes live in `telegram_link_codes`:
  { code, user_id, created_at, expires_at }
  indexed by code (unique) and expires_at (TTL)
"""

from __future__ import annotations

import secrets
from datetime import datetime, timedelta

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.auth.dependencies import get_current_user
from app.config import settings
from app.db import mongo

router = APIRouter(prefix="/api/telegram", tags=["telegram"])

LINK_CODE_TTL_MINUTES = 10


def _generate_code() -> str:
    # 6 chars, upper alphanumeric, avoid 0/O/1/I
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(secrets.choice(alphabet) for _ in range(6))


def _serialize_link(tg: dict | None) -> dict | None:
    if not tg:
        return None
    out: dict = {}
    for k in ("chat_id", "username", "first_name"):
        if k in tg:
            out[k] = tg[k]
    if isinstance(tg.get("linked_at"), datetime):
        out["linked_at"] = tg["linked_at"].isoformat()
    prefs = tg.get("prefs") or {}
    out["prefs"] = {
        "signals": bool(prefs.get("signals", True)),
        "trades": bool(prefs.get("trades", True)),
        "mute_until": prefs.get("mute_until").isoformat()
        if isinstance(prefs.get("mute_until"), datetime)
        else None,
    }
    return out


@router.get("/config")
async def telegram_config(_: dict = Depends(get_current_user)):
    """Public-ish bot handle so the frontend can build the deep link."""
    return {
        "configured": bool(settings.telegram_bot_token),
        "bot_username": settings.telegram_bot_username,
    }


@router.get("/status")
async def telegram_status(user: dict = Depends(get_current_user)):
    db = mongo.db()
    fresh = await db.users.find_one(
        {"_id": ObjectId(user["id"])}, {"telegram": 1}
    )
    link = (fresh or {}).get("telegram")
    return {
        "linked": bool(link and link.get("chat_id")),
        "link": _serialize_link(link),
    }


class StartLinkResponse(BaseModel):
    code: str
    deep_link: str | None
    expires_at: str
    bot_username: str | None


@router.post("/start-link", response_model=StartLinkResponse)
async def start_link(user: dict = Depends(get_current_user)):
    if not settings.telegram_bot_token:
        raise HTTPException(
            status_code=503,
            detail="Telegram bot not configured. Set TELEGRAM_BOT_TOKEN.",
        )
    db = mongo.db()

    # Invalidate any older pending codes for this user
    await db.telegram_link_codes.delete_many({"user_id": user["id"]})

    now = datetime.utcnow()
    exp = now + timedelta(minutes=LINK_CODE_TTL_MINUTES)
    # Try a few times to avoid a collision with an active code
    code = None
    for _ in range(5):
        candidate = _generate_code()
        existing = await db.telegram_link_codes.find_one({"code": candidate})
        if not existing:
            code = candidate
            break
    if not code:
        raise HTTPException(status_code=500, detail="Could not generate unique code")

    await db.telegram_link_codes.insert_one({
        "code": code,
        "user_id": user["id"],
        "created_at": now,
        "expires_at": exp,
    })

    bot = settings.telegram_bot_username
    deep_link = f"https://t.me/{bot}?start={code}" if bot else None

    return StartLinkResponse(
        code=code,
        deep_link=deep_link,
        expires_at=exp.isoformat(),
        bot_username=bot,
    )


@router.post("/unlink")
async def unlink(user: dict = Depends(get_current_user)):
    db = mongo.db()
    await db.users.update_one(
        {"_id": ObjectId(user["id"])},
        {"$unset": {"telegram": ""}},
    )
    await db.telegram_link_codes.delete_many({"user_id": user["id"]})
    return {"ok": True}


class PrefsBody(BaseModel):
    signals: bool | None = None
    trades: bool | None = None
    mute_until: str | None = None   # ISO or null to clear


@router.post("/test")
async def send_test(
    body: dict, user: dict = Depends(get_current_user)
):
    """
    Send a mock notification of each type directly to this user's chat.
    Great for previewing templates without waiting for a real event.
    body: { "type": "signal" | "trade_open" | "trade_close" | "all" }
    """
    if not settings.telegram_bot_token:
        raise HTTPException(status_code=503, detail="Telegram bot not configured")

    db = mongo.db()
    fresh = await db.users.find_one(
        {"_id": ObjectId(user["id"])}, {"telegram": 1}
    )
    tg_link = (fresh or {}).get("telegram") or {}
    chat_id = tg_link.get("chat_id")
    if not chat_id:
        raise HTTPException(status_code=400, detail="Telegram not linked")

    kind = (body or {}).get("type") or "all"

    from app.services.telegram_client import TelegramClient
    from app.services.telegram_dispatcher import (
        _fmt_signal,
        _fmt_trade_open,
        _fmt_trade_close,
        TELEGRAM_QUICK_BUY_USD,
    )

    now_iso = datetime.utcnow().isoformat()
    fake_signal = {
        "id": "test-signal-0000",
        "signal_type": "cluster",
        "token_id": "0xTEST-bsc",
        "chain": "bsc",
        "symbol": "TESTCOIN",
        "cluster_id": 7,
        "cluster_active_count": 4,
        "wallets_involved": ["0xA1", "0xB2", "0xC3", "0xD4"],
        "conviction_score": 78,
        "status": "exec",
        "detected_at": now_iso,
    }
    fake_trade_open = {
        "id": "test-trade-open",
        "user_id": user["id"],
        "token_id": "0xTEST-bsc",
        "chain": "bsc",
        "symbol": "TESTCOIN",
        "entry": {
            "price_usd": 0.000123,
            "size_usd": float(TELEGRAM_QUICK_BUY_USD),
            "amount_tokens": 813_008,
            "timestamp": now_iso,
            "trigger": {
                "type": "copy_entry",
                "source_wallet": "0xWhaleABC123",
            },
        },
        "status": "open",
    }
    fake_trade_close = {
        "id": "test-trade-close",
        "token_id": "0xTEST-bsc",
        "symbol": "TESTCOIN",
        "pnl_usd": 42.17,
        "pnl_pct": 42.17,
        "reason": "take_profit",
    }

    messages = []
    if kind in ("signal", "all"):
        keyboard = {
            "inline_keyboard": [[
                {"text": f"💰 Buy ${TELEGRAM_QUICK_BUY_USD}", "callback_data": "b:test-signal-0000"},
                {"text": "📊 View", "callback_data": "v:test-signal-0000"},
            ]],
        }
        messages.append((_fmt_signal(fake_signal), keyboard))
    if kind in ("trade_open", "all"):
        messages.append((_fmt_trade_open(fake_trade_open), None))
    if kind in ("trade_close", "all"):
        messages.append((_fmt_trade_close(fake_trade_close), None))

    if not messages:
        raise HTTPException(status_code=400, detail="Unknown test type")

    sent = 0
    try:
        async with TelegramClient() as tg:
            for text, kb in messages:
                await tg.send_message(chat_id, text, reply_markup=kb)
                sent += 1
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Telegram send failed: {e}")

    return {"ok": True, "sent": sent}


@router.patch("/prefs")
async def update_prefs(
    body: PrefsBody, user: dict = Depends(get_current_user)
):
    patch: dict = {}
    if body.signals is not None:
        patch["telegram.prefs.signals"] = body.signals
    if body.trades is not None:
        patch["telegram.prefs.trades"] = body.trades
    if body.mute_until is not None:
        try:
            patch["telegram.prefs.mute_until"] = (
                datetime.fromisoformat(body.mute_until.replace("Z", "+00:00"))
                if body.mute_until
                else None
            )
        except Exception:
            raise HTTPException(status_code=400, detail="Bad mute_until format")
    if not patch:
        raise HTTPException(status_code=400, detail="Nothing to update")

    db = mongo.db()
    await db.users.update_one({"_id": ObjectId(user["id"])}, {"$set": patch})
    fresh = await db.users.find_one(
        {"_id": ObjectId(user["id"])}, {"telegram": 1}
    )
    return {"link": _serialize_link((fresh or {}).get("telegram"))}

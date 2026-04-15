"""
Telegram notification dispatcher.

Subscribes to the in-process event bus and fans out formatted messages to every
user who has linked Telegram AND opted in for that notification type.

Event → notification mapping:
  SIGNAL_FIRED     → "New signal" card (prefs.signals)
  BOT_TRADE_OPENED → "Bot opened position" (prefs.trades)
  BOT_TRADE_CLOSED → "Bot closed position" with P&L (prefs.trades)

Each send is non-blocking and swallows errors — one user's bad chat_id should
never break the dispatcher for everyone else.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

from app.config import settings
from app.db import mongo
from app.services.event_bus import (
    BOT_TRADE_CLOSED,
    BOT_TRADE_OPENED,
    SIGNAL_SCORED,
    bus,
)
from app.services.telegram_client import TelegramClient, send_to

# Absolute floor — even if a user sets threshold=0 we won't spam them with
# garbage signals. Individual users can raise this via preferences.conviction_threshold.
SIGNAL_NOTIFY_HARD_FLOOR = 40

# Default buy size for the inline "Buy $100" button.
TELEGRAM_QUICK_BUY_USD = 100


def _fmt_usd(v: Any, places: int = 2) -> str:
    try:
        n = float(v)
    except (TypeError, ValueError):
        return "—"
    if n >= 1_000_000:
        return f"${n / 1_000_000:.2f}M"
    if n >= 1_000:
        return f"${n / 1_000:.2f}K"
    if abs(n) < 0.01:
        return f"${n:.6f}"
    return f"${n:,.{places}f}"


def _fmt_pct(v: Any) -> str:
    try:
        n = float(v)
    except (TypeError, ValueError):
        return "—"
    sign = "+" if n > 0 else ""
    return f"{sign}{n:.2f}%"


async def _linked_chats(pref_key: str) -> list[tuple[int, int]]:
    """
    Returns (chat_id, conviction_threshold) for every user who opted in for
    this notification type. Callers filter against the signal's conviction.
    """
    db = mongo.db()
    out: list[tuple[int, int]] = []
    now = datetime.utcnow()
    cursor = db.users.find(
        {
            "telegram.chat_id": {"$exists": True},
            f"telegram.prefs.{pref_key}": {"$ne": False},
        },
        {"telegram": 1, "preferences": 1},
    )
    async for u in cursor:
        tg = u.get("telegram") or {}
        mute = (tg.get("prefs") or {}).get("mute_until")
        if isinstance(mute, datetime) and mute > now:
            continue
        chat_id = tg.get("chat_id")
        if not isinstance(chat_id, int):
            continue
        prefs = u.get("preferences") or {}
        try:
            threshold = int(prefs.get("conviction_threshold") or 70)
        except (TypeError, ValueError):
            threshold = 70
        out.append((chat_id, max(SIGNAL_NOTIFY_HARD_FLOOR, threshold)))
    return out


async def _broadcast(
    chats: list[int],
    text: str,
    reply_markup: dict | None = None,
) -> None:
    if not chats:
        return

    async def _one(chat_id: int) -> None:
        if not settings.telegram_bot_token:
            return
        try:
            async with TelegramClient() as tg:
                await tg.send_message(chat_id, text, reply_markup=reply_markup)
        except Exception as e:
            print(f"[telegram] send failed to {chat_id}: {e}")

    await asyncio.gather(*(_one(cid) for cid in chats), return_exceptions=True)


def _chain_flag(chain: str | None) -> str:
    return {"solana": "◎", "bsc": "🟡", "eth": "Ξ", "base": "🔵"}.get(
        (chain or "").lower(), "•"
    )


# ---------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------

def _fmt_signal(sig: dict) -> str:
    symbol = sig.get("symbol") or (sig.get("token_id") or "")[:10]
    chain = sig.get("chain") or ""
    conviction = int(sig.get("conviction_score") or 0)
    status = (sig.get("status") or "watch").upper()
    cluster_id = sig.get("cluster_id")
    sig_type = sig.get("signal_type") or "cluster"
    wallets = sig.get("cluster_active_count") or len(sig.get("wallets_involved") or [])

    if sig_type == "alpha":
        detail = (
            f"α-wallet: {sig.get('avg_alpha_score', '?'):.2f}"
            if isinstance(sig.get("avg_alpha_score"), (int, float))
            else "alpha wallet"
        )
    else:
        detail = f"Cabal #{cluster_id} · {wallets} wallets piling in"

    return (
        f"⚡ <b>{status}</b> · conviction <b>{conviction}</b>\n"
        f"{_chain_flag(chain)} <b>${symbol}</b> <code>{chain}</code>\n"
        f"<i>{detail}</i>"
    )


def _fmt_trade_open(t: dict) -> str:
    symbol = t.get("symbol") or (t.get("token_id") or "")[:10]
    chain = t.get("chain") or ""
    entry = t.get("entry") or {}
    price = float(entry.get("price_usd") or 0)
    size = float(entry.get("size_usd") or 0)
    trigger = entry.get("trigger") or {}
    trig_line = ""
    if trigger.get("type") == "copy_entry":
        src = (trigger.get("source_wallet") or "")[:8]
        trig_line = f"\n<i>Copied {src}…</i>"
    elif trigger.get("type") == "signal":
        trig_line = f"\n<i>Signal · conviction {trigger.get('conviction', '—')}</i>"
    return (
        f"🟢 <b>Bot OPENED</b>\n"
        f"{_chain_flag(chain)} <b>${symbol}</b> "
        f"· size <b>{_fmt_usd(size, 0)}</b>\n"
        f"entry @ <code>{_fmt_usd(price, 6)}</code>"
        f"{trig_line}"
    )


def _fmt_trade_close(t: dict) -> str:
    symbol = t.get("symbol") or (t.get("token_id") or "")[:10]
    pnl_usd = float(t.get("pnl_usd") or 0)
    pnl_pct = float(t.get("pnl_pct") or 0)
    reason = (t.get("reason") or "").replace("_", " ") or "closed"
    emoji = "🟩" if pnl_pct >= 0 else "🟥"
    return (
        f"{emoji} <b>Bot CLOSED</b> · <i>{reason}</i>\n"
        f"<b>${symbol}</b> · "
        f"<b>{_fmt_pct(pnl_pct)}</b> ({_fmt_usd(pnl_usd)})"
    )


# ---------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------

async def on_signal_fired(payload: dict) -> None:
    conviction = int(payload.get("conviction_score") or 0)
    if conviction < SIGNAL_NOTIFY_HARD_FLOOR:
        return  # below the absolute floor — don't ping anyone
    all_chats = await _linked_chats("signals")
    # Per-user threshold: a user with conviction_threshold=80 won't see a 60 signal.
    chats = [cid for cid, thresh in all_chats if conviction >= thresh]
    if not chats:
        return

    # Inline keyboard — callback_data is size-limited (64 bytes). We stash
    # the signal id which is a Mongo ObjectId (24 chars) — plenty of room.
    sig_id = payload.get("id") or payload.get("_id")
    keyboard: dict | None = None
    if sig_id:
        keyboard = {
            "inline_keyboard": [[
                {
                    "text": f"💰 Buy ${TELEGRAM_QUICK_BUY_USD}",
                    "callback_data": f"b:{sig_id}",
                },
                {
                    "text": "📊 View",
                    "callback_data": f"v:{sig_id}",
                },
            ]],
        }
    await _broadcast(chats, _fmt_signal(payload), reply_markup=keyboard)


async def on_bot_opened(payload: dict) -> None:
    chats = await _linked_chats("trades")
    if not chats:
        return
    await _broadcast([cid for cid, _ in chats], _fmt_trade_open(payload))


async def on_bot_closed(payload: dict) -> None:
    chats = await _linked_chats("trades")
    if not chats:
        return
    await _broadcast([cid for cid, _ in chats], _fmt_trade_close(payload))


# ---------------------------------------------------------------------

def register() -> None:
    if not settings.telegram_bot_token:
        print("[telegram_dispatcher] bot token missing — notifications disabled")
        return
    bus.subscribe(SIGNAL_SCORED, on_signal_fired)
    bus.subscribe(BOT_TRADE_OPENED, on_bot_opened)
    bus.subscribe(BOT_TRADE_CLOSED, on_bot_closed)
    print("[telegram_dispatcher] registered")

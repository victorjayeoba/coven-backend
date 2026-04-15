"""
Telegram long-poller + command dispatcher.

Uses `getUpdates` (long-poll) rather than a webhook so it works on localhost
with no public URL. Each update is parsed, commands are dispatched:

  /start <code>   → bind this chat to the user holding the code
  /start          → friendly welcome, tell user to get code from the site
  /status         → show connection state
  /unlink         → disconnect
  /trades         → last 5 bot trades
  /balance        → paper wallet balances (placeholder)
  /pause          → pause all bots
  /resume         → resume all bots

Runs continuously once TELEGRAM_BOT_TOKEN is set; silent no-op otherwise.
"""

from __future__ import annotations

import asyncio
from datetime import datetime

from bson import ObjectId

from app.config import settings
from app.db import mongo
from app.services.balance_ledger import credit, get_balance, try_debit
from app.services.event_bus import BOT_TRADE_OPENED, bus
from app.services.telegram_client import TelegramClient

# Default size for the inline "Buy $100" button on signal alerts.
TELEGRAM_QUICK_BUY_USD = 100

_stop = asyncio.Event()


def _fmt_ts(dt: datetime | str | None) -> str:
    if not dt:
        return "—"
    if isinstance(dt, datetime):
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    return str(dt)


# ---------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------

async def _cmd_start(tg: TelegramClient, chat: dict, args: list[str]) -> None:
    chat_id = chat["id"]
    username = chat.get("username")
    first_name = chat.get("first_name")

    if not args:
        await tg.send_message(
            chat_id,
            "👋 <b>Welcome to Coven</b>\n\n"
            "This bot delivers live cluster signals + paper-trade updates to you on Telegram.\n\n"
            "To connect your Coven account, go to the website → <b>Notifications</b> tab "
            "and copy your 6-character link code, then reply here with:\n\n"
            "<code>/start YOURCODE</code>",
        )
        return

    code = args[0].upper().strip()
    db = mongo.db()
    link_doc = await db.telegram_link_codes.find_one({"code": code})
    if not link_doc:
        await tg.send_message(
            chat_id,
            "❌ That code isn't valid or has already been used.\n"
            "Generate a fresh one from the <b>Notifications</b> page.",
        )
        return

    if link_doc.get("expires_at") and link_doc["expires_at"] < datetime.utcnow():
        await db.telegram_link_codes.delete_one({"_id": link_doc["_id"]})
        await tg.send_message(
            chat_id,
            "⏱ That code has expired. Generate a new one and try again.",
        )
        return

    user_id = link_doc["user_id"]
    await db.users.update_one(
        {"_id": ObjectId(user_id)},
        {
            "$set": {
                "telegram": {
                    "chat_id": chat_id,
                    "username": username,
                    "first_name": first_name,
                    "linked_at": datetime.utcnow(),
                    "prefs": {
                        "signals": True,
                        "trades": True,
                        "mute_until": None,
                    },
                }
            }
        },
    )
    await db.telegram_link_codes.delete_one({"_id": link_doc["_id"]})

    await tg.send_message(
        chat_id,
        "✅ <b>Connected!</b>\n\n"
        f"You'll now receive live cluster signals and bot-trade updates.\n\n"
        "Type <code>/status</code> anytime, or <code>/unlink</code> to disconnect.",
    )


async def _user_from_chat(chat_id: int) -> dict | None:
    db = mongo.db()
    return await db.users.find_one({"telegram.chat_id": chat_id})


async def _cmd_status(tg: TelegramClient, chat: dict, _: list[str]) -> None:
    user = await _user_from_chat(chat["id"])
    if not user:
        await tg.send_message(
            chat["id"],
            "🔌 Not connected. Get a code from the site's Notifications page.",
        )
        return
    linked_at = user.get("telegram", {}).get("linked_at")
    await tg.send_message(
        chat["id"],
        f"✅ <b>Connected</b>\n"
        f"Account: <code>{user.get('email', user.get('username', 'you'))}</code>\n"
        f"Linked: <code>{_fmt_ts(linked_at)}</code>\n\n"
        f"Use <code>/unlink</code> to disconnect.",
    )


async def _cmd_unlink(tg: TelegramClient, chat: dict, _: list[str]) -> None:
    db = mongo.db()
    res = await db.users.update_one(
        {"telegram.chat_id": chat["id"]},
        {"$unset": {"telegram": ""}},
    )
    if res.modified_count:
        await tg.send_message(
            chat["id"],
            "🔌 Disconnected. You can re-link anytime from the site.",
        )
    else:
        await tg.send_message(chat["id"], "You weren't connected.")


SUPPORTED_NETWORKS = ("solana", "bsc")
NETWORK_ALIASES = {
    "sol": "solana",
    "solana": "solana",
    "bnb": "bsc",
    "bsc": "bsc",
    "bnbchain": "bsc",
}
NETWORK_LABEL = {"solana": "Solana", "bsc": "BNB Smart Chain"}


def _fmt_usd(n: float) -> str:
    if n >= 1_000_000:
        return f"${n/1_000_000:.2f}M"
    if n >= 1_000:
        return f"${n/1_000:.2f}K"
    return f"${n:,.2f}"


async def _cmd_balance(tg: TelegramClient, chat: dict, _: list[str]) -> None:
    user = await _user_from_chat(chat["id"])
    if not user:
        await tg.send_message(chat["id"], "Connect first — see the Notifications page.")
        return
    balances = (user.get("paper_balances") or {})
    total = 0.0
    lines = []
    for net in SUPPORTED_NETWORKS:
        v = float(balances.get(net) or 0)
        total += v
        lines.append(f"• <b>{NETWORK_LABEL[net]}</b> · <code>{_fmt_usd(v)}</code>")
    await tg.send_message(
        chat["id"],
        "<b>💼 Paper Wallet</b>\n"
        + "\n".join(lines)
        + f"\n\n<b>Total</b>: <code>{_fmt_usd(total)}</code>\n\n"
        "Add funds: <code>/fund 500 sol</code>",
    )


async def _cmd_fund(tg: TelegramClient, chat: dict, args: list[str]) -> None:
    user = await _user_from_chat(chat["id"])
    if not user:
        await tg.send_message(chat["id"], "Connect first — see the Notifications page.")
        return

    if not args:
        await tg.send_message(
            chat["id"],
            "<b>Usage</b>: <code>/fund &lt;amount&gt; [network]</code>\n"
            "Examples:\n"
            "<code>/fund 500</code> — $500 to Solana (default)\n"
            "<code>/fund 1000 bsc</code> — $1,000 to BNB Smart Chain\n"
            "<code>/fund 250 sol</code> — $250 to Solana",
        )
        return

    # Parse amount
    try:
        amount = float(args[0].replace(",", "").replace("$", ""))
    except ValueError:
        await tg.send_message(chat["id"], "❌ Amount must be a number. e.g. <code>/fund 500</code>")
        return
    if amount <= 0:
        await tg.send_message(chat["id"], "❌ Amount must be positive.")
        return
    if amount > 1_000_000:
        await tg.send_message(chat["id"], "❌ Amount too large — max $1M at a time.")
        return

    # Parse network (default solana)
    raw_net = (args[1].lower() if len(args) > 1 else "solana")
    network = NETWORK_ALIASES.get(raw_net)
    if not network:
        await tg.send_message(
            chat["id"],
            f"❌ Unknown network '{args[1]}'. Use <code>sol</code> or <code>bsc</code>.",
        )
        return

    db = mongo.db()
    key = f"paper_balances.{network}"
    await db.users.update_one(
        {"_id": user["_id"]},
        {
            "$inc": {key: float(amount)},
            "$set": {"paper_balances_updated_at": datetime.utcnow()},
        },
    )

    fresh = await db.users.find_one({"_id": user["_id"]}, {"paper_balances": 1})
    balances = (fresh or {}).get("paper_balances") or {}
    net_balance = float(balances.get(network) or 0)
    total = sum(float(balances.get(n) or 0) for n in SUPPORTED_NETWORKS)

    await tg.send_message(
        chat["id"],
        f"✅ <b>Deposited {_fmt_usd(amount)}</b> to {NETWORK_LABEL[network]}\n"
        f"{NETWORK_LABEL[network]} balance: <code>{_fmt_usd(net_balance)}</code>\n"
        f"Total paper wallet: <code>{_fmt_usd(total)}</code>",
    )


async def _cmd_signals(tg: TelegramClient, chat: dict, _: list[str]) -> None:
    """
    Pull-mode: the top 5 recent live signals as individual cards, each with
    its own Buy $100 / View buttons so the user can trigger any of them.
    """
    user = await _user_from_chat(chat["id"])
    if not user:
        await tg.send_message(chat["id"], "Connect first — see the Notifications page.")
        return

    from app.services.telegram_dispatcher import (
        _fmt_signal,
        TELEGRAM_QUICK_BUY_USD,
    )

    db = mongo.db()
    rows: list[dict] = []
    cursor = (
        db.signals.find({})
        .sort([("conviction_score", -1), ("detected_at", -1)])
        .limit(5)
    )
    async for s in cursor:
        rows.append(s)

    if not rows:
        await tg.send_message(
            chat["id"],
            "📭 No live signals yet. The detector is watching — I'll ping you the moment a cabal piles in.",
        )
        return

    # Header
    await tg.send_message(
        chat["id"],
        f"⚡ <b>Top {len(rows)} live signals</b> · tap <i>Buy ${TELEGRAM_QUICK_BUY_USD}</i> on any card",
    )

    # Individual cards so each Buy button works independently
    for s in rows:
        sig_id = str(s.get("_id"))
        payload = {**s, "id": sig_id}
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
        try:
            await tg.send_message(
                chat["id"],
                _fmt_signal(payload),
                reply_markup=keyboard,
            )
        except Exception as e:
            print(f"[telegram_poller] /signals send failed: {e}")


# ---------------------------------------------------------------------
# Copy-trade tracking
# ---------------------------------------------------------------------

import re

_BSC_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
_SOL_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")  # base58, no 0/O/I/l


def _detect_chain(addr: str) -> str | None:
    """Return 'bsc' / 'solana' / None based on address shape."""
    addr = (addr or "").strip()
    if _BSC_RE.match(addr):
        return "bsc"
    if _SOL_RE.match(addr):
        return "solana"
    return None


async def _cmd_track(tg: TelegramClient, chat: dict, args: list[str]) -> None:
    """
    /track <wallet> [chain] [size]
    Creates a copy-trade bot pointed at <wallet>. Defaults: $100 size,
    TP +50%, SL -20%. Chain auto-detected from address shape.
    """
    user = await _user_from_chat(chat["id"])
    if not user:
        await tg.send_message(chat["id"], "Connect first — see the Notifications page.")
        return

    if not args:
        await tg.send_message(
            chat["id"],
            "<b>Track a wallet · spin up a copy bot</b>\n\n"
            "<code>/track &lt;address&gt;</code> — auto-detect chain, $100/trade\n"
            "<code>/track &lt;address&gt; bsc</code> — force chain\n"
            "<code>/track &lt;address&gt; sol 250</code> — set size\n\n"
            "Defaults: TP <b>+50%</b>, SL <b>−20%</b>, copy exits on. "
            "Manage with <code>/open</code> or stop with <code>/untrack &lt;address&gt;</code>.",
        )
        return

    wallet = args[0].strip()
    chain = _detect_chain(wallet)
    explicit = None
    size = float(TELEGRAM_QUICK_BUY_USD)

    # Optional chain + size args
    for a in args[1:]:
        a_low = a.lower()
        if a_low in NETWORK_ALIASES:
            explicit = NETWORK_ALIASES[a_low]
        else:
            try:
                size = float(a.replace(",", "").replace("$", ""))
            except ValueError:
                pass

    if explicit:
        chain = explicit
    if not chain:
        await tg.send_message(
            chat["id"],
            "❌ Couldn't tell which chain that address is on. "
            "Add it: <code>/track ADDR sol</code> or <code>/track ADDR bsc</code>.",
        )
        return

    if size <= 0 or size > 1_000_000:
        await tg.send_message(chat["id"], "❌ Size must be $1 – $1,000,000.")
        return

    db = mongo.db()
    user_id = str(user["_id"])

    # Dup-guard: one active copy bot per (user, wallet, chain)
    wallet_lc = wallet.lower()
    dup = await db.bots.find_one({
        "user_id": user_id,
        "type": "copy",
        "chain": chain,
        "target_wallet_lc": wallet_lc,
        "status": "active",
    })
    if dup:
        await tg.send_message(
            chat["id"],
            f"You're already tracking <code>{wallet[:8]}…</code> on "
            f"<b>{NETWORK_LABEL[chain]}</b>. Use <code>/untrack {wallet}</code> to stop.",
        )
        return

    now = datetime.utcnow()
    bot_doc = {
        "user_id": user_id,
        "name": f"TG · {wallet[:4]}…{wallet[-4:]}",
        "type": "copy",
        "status": "active",
        "chain": chain,
        "size_usd": float(size),
        "size_mode": "fixed",
        "multiplier": 1.0,
        "percent_of_target": 5.0,
        "target_wallet": wallet,
        "target_wallet_lc": wallet_lc,
        "target_label": "",
        "copy_exits": True,
        "min_conviction": 70,
        "cluster_filter": "",
        "take_profit_pct": 50.0,
        "stop_loss_pct": 20.0,
        "trailing_stop_pct": 0.0,
        "max_slippage_pct": 3.0,
        "max_concurrent": 5,
        "daily_loss_limit_usd": 500.0,
        "min_liquidity_usd": 10_000.0,
        "cooldown_min": 15,
        "anti_rug": True,
        "stats": {"pnl_usd": 0.0, "trades": 0, "wins": 0},
        "created_at": now,
        "last_updated": now,
    }
    res = await db.bots.insert_one(bot_doc)
    bot_id = str(res.inserted_id)

    # Balance heads-up
    bal = await get_balance(user_id, chain)
    bal_line = (
        f"\n💼 {NETWORK_LABEL[chain]} balance: <code>{_fmt_usd(bal)}</code>"
        if bal >= size
        else f"\n⚠️ Low balance: <code>{_fmt_usd(bal)}</code> on {NETWORK_LABEL[chain]} — "
        f"run <code>/fund {int(size)} {chain[:3]}</code> to enable trades."
    )

    await tg.send_message(
        chat["id"],
        f"✅ <b>Tracking <code>{wallet[:6]}…{wallet[-4:]}</code></b>\n"
        f"chain: <b>{NETWORK_LABEL[chain]}</b> · size: <b>{_fmt_usd(size)}</b>/trade\n"
        f"TP +50% · SL −20% · copy exits ON"
        f"{bal_line}",
    )


async def _cmd_untrack(tg: TelegramClient, chat: dict, args: list[str]) -> None:
    user = await _user_from_chat(chat["id"])
    if not user:
        await tg.send_message(chat["id"], "Connect first.")
        return

    if not args:
        # List currently tracked wallets
        db = mongo.db()
        rows = []
        async for b in db.bots.find(
            {"user_id": str(user["_id"]), "type": "copy", "status": "active"},
            {"target_wallet": 1, "chain": 1},
        ):
            w = b.get("target_wallet") or ""
            rows.append(f"• <code>{w[:6]}…{w[-4:]}</code> · {b.get('chain', '?').upper()}")
        if not rows:
            await tg.send_message(chat["id"], "You're not tracking any wallets.")
            return
        await tg.send_message(
            chat["id"],
            "<b>Currently tracking</b>\n" + "\n".join(rows) +
            "\n\nStop one: <code>/untrack ADDR</code>",
        )
        return

    wallet = args[0].strip()
    wallet_lc = wallet.lower()
    db = mongo.db()
    res = await db.bots.delete_many({
        "user_id": str(user["_id"]),
        "type": "copy",
        "target_wallet_lc": wallet_lc,
    })
    if res.deleted_count:
        await tg.send_message(
            chat["id"],
            f"🛑 Stopped tracking <code>{wallet[:6]}…{wallet[-4:]}</code> "
            f"({res.deleted_count} bot{'s' if res.deleted_count > 1 else ''} removed).",
        )
    else:
        await tg.send_message(chat["id"], "No matching tracked wallet.")


async def _cmd_open(tg: TelegramClient, chat: dict, _: list[str]) -> None:
    """
    List every open bot position for this user. Each row is an inline button —
    tap one to get a full card (entry / live / PnL) with Close + Buy again.
    """
    user = await _user_from_chat(chat["id"])
    if not user:
        await tg.send_message(chat["id"], "Connect first — see the Notifications page.")
        return

    db = mongo.db()
    opens: list[dict] = []
    cursor = (
        db.bot_trades.find(
            {"user_id": str(user["_id"]), "status": "open"},
            {
                "symbol": 1,
                "token_id": 1,
                "entry": 1,
                "current_price_usd": 1,
                "opened_at": 1,
            },
        )
        .sort("opened_at", -1)
        .limit(12)
    )
    async for t in cursor:
        opens.append(t)

    if not opens:
        await tg.send_message(
            chat["id"],
            "📭 No open positions. Tap <i>💰 Buy</i> on a signal card to start one.",
        )
        return

    # Build a keyboard with one button per trade — label shows symbol + live PnL.
    rows = []
    for t in opens:
        entry = t.get("entry") or {}
        ep = float(entry.get("price_usd") or 0)
        cp = float(t.get("current_price_usd") or ep or 0)
        pct = (cp / ep - 1) * 100 if ep > 0 else 0.0
        arrow = "🟢" if pct >= 0 else "🔴"
        sym = t.get("symbol") or (t.get("token_id") or "")[:10]
        sign = "+" if pct >= 0 else ""
        label = f"{arrow} ${sym}  {sign}{pct:.2f}%"
        rows.append([{"text": label, "callback_data": f"o:{t['_id']}"}])

    await tg.send_message(
        chat["id"],
        f"💼 <b>Open positions</b> · {len(opens)}\nTap any to manage.",
        reply_markup={"inline_keyboard": rows},
    )


async def _cmd_trades(tg: TelegramClient, chat: dict, _: list[str]) -> None:
    user = await _user_from_chat(chat["id"])
    if not user:
        await tg.send_message(
            chat["id"], "Connect first — see the Notifications page."
        )
        return
    db = mongo.db()
    rows = []
    cursor = db.bot_trades.find(
        {"user_id": str(user["_id"])}
    ).sort("opened_at", -1).limit(5)
    async for t in cursor:
        entry = t.get("entry") or {}
        status = t.get("status", "?")
        sym = t.get("symbol") or t.get("token_id", "")[:8]
        pnl_pct = t.get("pnl_pct")
        rows.append(
            f"• <b>${sym}</b> · {status} · "
            f"entry ${float(entry.get('price_usd') or 0):.6f}"
            + (f" · pnl {pnl_pct:+.2f}%" if pnl_pct is not None else "")
        )
    if not rows:
        await tg.send_message(chat["id"], "No bot trades yet.")
        return
    await tg.send_message(
        chat["id"], "<b>Last bot trades</b>\n" + "\n".join(rows)
    )


async def _cmd_pause(tg: TelegramClient, chat: dict, _: list[str]) -> None:
    user = await _user_from_chat(chat["id"])
    if not user:
        await tg.send_message(chat["id"], "Connect first.")
        return
    db = mongo.db()
    res = await db.bots.update_many(
        {"user_id": str(user["_id"]), "status": "active"},
        {"$set": {"status": "paused"}},
    )
    await tg.send_message(
        chat["id"], f"⏸ Paused {res.modified_count} active bot(s)."
    )


async def _cmd_resume(tg: TelegramClient, chat: dict, _: list[str]) -> None:
    user = await _user_from_chat(chat["id"])
    if not user:
        await tg.send_message(chat["id"], "Connect first.")
        return
    db = mongo.db()
    res = await db.bots.update_many(
        {"user_id": str(user["_id"]), "status": "paused"},
        {"$set": {"status": "active"}},
    )
    await tg.send_message(
        chat["id"], f"▶️ Resumed {res.modified_count} bot(s)."
    )


async def _cmd_help(tg: TelegramClient, chat: dict, _: list[str]) -> None:
    await tg.send_message(
        chat["id"],
        "<b>Coven bot commands</b>\n"
        "<code>/signals</code> — top 5 live signals · tap to buy\n"
        "<code>/open</code> — open positions · tap to manage\n"
        "<code>/track ADDR</code> — start a copy bot on a wallet\n"
        "<code>/untrack ADDR</code> — stop one (no arg = list)\n"
        "<code>/balance</code> — show paper wallet\n"
        "<code>/fund 500 sol</code> — deposit paper funds\n"
        "<code>/trades</code> — last 5 bot trades\n"
        "<code>/pause</code> · <code>/resume</code> — toggle all bots\n"
        "<code>/status</code> — link state · <code>/unlink</code> — disconnect",
    )


COMMANDS = {
    "/start": _cmd_start,
    "/status": _cmd_status,
    "/signals": _cmd_signals,
    "/live": _cmd_signals,   # alias
    "/track": _cmd_track,
    "/copy": _cmd_track,     # alias
    "/untrack": _cmd_untrack,
    "/open": _cmd_open,
    "/positions": _cmd_open,  # alias
    "/balance": _cmd_balance,
    "/fund": _cmd_fund,
    "/deposit": _cmd_fund,   # alias
    "/unlink": _cmd_unlink,
    "/trades": _cmd_trades,
    "/pause": _cmd_pause,
    "/resume": _cmd_resume,
    "/help": _cmd_help,
}


# ---------------------------------------------------------------------
# Inline-button callback handling (the "Buy $100" button on signal alerts)
# ---------------------------------------------------------------------

async def _resolve_entry_price(token_id: str) -> float | None:
    """Reuse bot_runner's price resolution (AVE → Jupiter fallback)."""
    try:
        from app.services.bot_runner import _entry_price  # local import to avoid cycle
        return await _entry_price(token_id)
    except Exception as e:
        print(f"[telegram_poller] price resolve failed for {token_id}: {e}")
        return None


async def _execute_quick_buy(
    tg: TelegramClient,
    cb_id: str,
    chat_id: int,
    message_id: int | None,
    signal_id: str,
) -> None:
    db = mongo.db()
    user = await db.users.find_one({"telegram.chat_id": chat_id})
    if not user:
        await tg.answer_callback_query(cb_id, "You're not linked yet.", show_alert=True)
        return

    # Look up the signal this button was attached to
    try:
        sig = await db.signals.find_one({"_id": ObjectId(signal_id)})
    except Exception:
        sig = None
    if not sig:
        await tg.answer_callback_query(cb_id, "Signal expired or not found.", show_alert=True)
        return

    token_id = sig.get("token_id")
    chain = sig.get("chain") or ""
    symbol = sig.get("symbol")
    if not token_id:
        await tg.answer_callback_query(cb_id, "Signal missing token.", show_alert=True)
        return

    user_id = str(user["_id"])

    # Dup-guard: refuse if we already have an open trade on this token for this user
    dup = await db.bot_trades.find_one({
        "user_id": user_id,
        "token_id": token_id,
        "status": "open",
    })
    if dup:
        await tg.answer_callback_query(cb_id, "You already hold this token.", show_alert=True)
        return

    price = await _resolve_entry_price(token_id)
    if not price or price <= 0:
        await tg.answer_callback_query(cb_id, "Couldn't price this token.", show_alert=True)
        return

    size_usd = float(TELEGRAM_QUICK_BUY_USD)

    # Balance gate
    debited = await try_debit(user_id, chain, size_usd)
    if not debited:
        bal = await get_balance(user_id, chain)
        await tg.answer_callback_query(
            cb_id,
            f"Insufficient {chain.upper()} balance (${bal:.2f}). Run /fund {int(size_usd)} {chain[:3]}.",
            show_alert=True,
        )
        return

    amount = size_usd / price
    now = datetime.utcnow()

    doc = {
        "bot_id": None,   # not tied to a bot — manual from Telegram
        "user_id": user_id,
        "token_id": token_id,
        "chain": chain,
        "symbol": symbol,
        "entry": {
            "price_usd": price,
            "size_usd": round(size_usd, 2),
            "amount_tokens": amount,
            "timestamp": now,
            "trigger": {
                "type": "telegram_manual",
                "signal_id": signal_id,
                "conviction": int(sig.get("conviction_score") or 0),
            },
        },
        "exit": None,
        "current_price_usd": price,
        "peak_price_usd": price,
        "take_profit_pct": 50.0,    # sensible defaults for a tap-to-buy
        "stop_loss_pct": 20.0,
        "trailing_stop_pct": 0.0,
        "status": "open",
        "is_paper": True,
        "opened_at": now,
        "closed_at": None,
        "last_updated": now,
    }
    try:
        res = await db.bot_trades.insert_one(doc)
    except Exception as e:
        # Refund debit on insert failure
        await credit(user_id, chain, size_usd)
        await tg.answer_callback_query(cb_id, f"Trade failed: {e}", show_alert=True)
        return
    trade_id = str(res.inserted_id)

    # Publish so SSE + bot_position_monitor + frontend caches update
    await bus.publish(BOT_TRADE_OPENED, {
        "id": trade_id,
        "bot_id": None,
        "user_id": user_id,
        "token_id": token_id,
        "chain": chain,
        "symbol": symbol,
        "entry": {
            "price_usd": price,
            "size_usd": round(size_usd, 2),
            "amount_tokens": amount,
            "timestamp": now.isoformat(),
            "trigger": doc["entry"]["trigger"],
        },
        "status": "open",
    })

    await tg.answer_callback_query(cb_id, f"Bought ${TELEGRAM_QUICK_BUY_USD} of ${symbol or 'token'}")

    # Update the original message to show it's been acted on
    if message_id is not None:
        try:
            await tg.edit_message_text(
                chat_id,
                message_id,
                (
                    f"✅ <b>Bought ${TELEGRAM_QUICK_BUY_USD}</b> of ${symbol or token_id[:10]}\n"
                    f"entry @ <code>${price:.6f}</code>\n"
                    f"<i>TP +50% · SL −20% · paper mode</i>"
                ),
                reply_markup={"inline_keyboard": []},
            )
        except Exception:
            pass


def _fmt_position_card(t: dict) -> str:
    """Detail card for a single open/closed bot_trade."""
    sym = t.get("symbol") or (t.get("token_id") or "")[:10]
    chain = t.get("chain") or ""
    entry = t.get("entry") or {}
    ep = float(entry.get("price_usd") or 0)
    amount = float(entry.get("amount_tokens") or 0)
    size = float(entry.get("size_usd") or 0)
    cp = float(t.get("current_price_usd") or ep or 0)
    tp = float(t.get("take_profit_pct") or 0)
    sl = float(t.get("stop_loss_pct") or 0)
    status = t.get("status", "open")

    if status == "closed":
        exit_obj = t.get("exit") or {}
        xp = float(exit_obj.get("price_usd") or cp or 0)
        pnl_usd = float(t.get("pnl_usd") or (xp - ep) * amount)
        pnl_pct = float(t.get("pnl_pct") or ((xp / ep - 1) * 100 if ep else 0))
        reason = (exit_obj.get("reason") or "").replace("_", " ") or "closed"
        emoji = "🟩" if pnl_pct >= 0 else "🟥"
        return (
            f"{emoji} <b>${sym}</b> <code>{chain}</code> · <i>{reason}</i>\n"
            f"entry <code>${ep:.6f}</code> → exit <code>${xp:.6f}</code>\n"
            f"P&amp;L <b>{'+' if pnl_pct >= 0 else ''}{pnl_pct:.2f}%</b> "
            f"({'+' if pnl_usd >= 0 else ''}${pnl_usd:.2f}) · size ${size:.0f}"
        )

    pnl_pct = (cp / ep - 1) * 100 if ep > 0 else 0.0
    pnl_usd = (cp - ep) * amount
    tone = "🟢" if pnl_pct >= 0 else "🔴"
    trig = (entry.get("trigger") or {})
    trig_line = ""
    if trig.get("type") == "telegram_manual":
        trig_line = "\n<i>Opened from Telegram</i>"
    elif trig.get("type") == "copy_entry":
        trig_line = f"\n<i>Copied {(trig.get('source_wallet') or '')[:8]}…</i>"
    elif trig.get("type") == "signal":
        trig_line = f"\n<i>Signal · conviction {trig.get('conviction', '—')}</i>"

    sign = "+" if pnl_pct >= 0 else ""
    return (
        f"{tone} <b>${sym}</b> <code>{chain}</code> · OPEN\n"
        f"entry <code>${ep:.6f}</code>  →  live <code>${cp:.6f}</code>\n"
        f"P&amp;L <b>{sign}{pnl_pct:.2f}%</b> ({sign}${pnl_usd:.2f}) · size ${size:.0f}\n"
        f"TP <b>+{tp:.0f}%</b> · SL <b>−{sl:.0f}%</b>"
        f"{trig_line}"
    )


async def _execute_close(
    tg: TelegramClient, cb_id: str, chat_id: int, message_id: int | None, trade_id: str
) -> None:
    db = mongo.db()
    user = await db.users.find_one({"telegram.chat_id": chat_id})
    if not user:
        await tg.answer_callback_query(cb_id, "You're not linked.", show_alert=True)
        return

    try:
        oid = ObjectId(trade_id)
    except Exception:
        await tg.answer_callback_query(cb_id, "Bad trade id.", show_alert=True)
        return

    trade = await db.bot_trades.find_one({"_id": oid, "user_id": str(user["_id"])})
    if not trade:
        await tg.answer_callback_query(cb_id, "Trade not found.", show_alert=True)
        return
    if trade.get("status") != "open":
        await tg.answer_callback_query(cb_id, "Already closed.", show_alert=True)
        return

    entry = trade.get("entry") or {}
    ep = float(entry.get("price_usd") or 0)
    amount = float(entry.get("amount_tokens") or 0)
    cp = float(trade.get("current_price_usd") or ep or 0)

    if cp <= 0:
        # Last-ditch fetch
        cp = await _resolve_entry_price(trade.get("token_id", "")) or ep
    if cp <= 0:
        await tg.answer_callback_query(cb_id, "Couldn't price right now.", show_alert=True)
        return

    pnl_usd = (cp - ep) * amount if ep else 0.0
    pnl_pct = (cp / ep - 1) * 100 if ep else 0.0
    now = datetime.utcnow()

    await db.bot_trades.update_one(
        {"_id": oid},
        {
            "$set": {
                "exit": {
                    "price_usd": cp,
                    "amount_tokens": amount,
                    "timestamp": now,
                    "reason": "manual_telegram",
                },
                "current_price_usd": cp,
                "pnl_usd": round(pnl_usd, 2),
                "pnl_pct": round(pnl_pct, 2),
                "status": "closed",
                "closed_at": now,
                "last_updated": now,
            }
        },
    )

    # Credit exit value back to the user's chain wallet
    chain = trade.get("chain")
    size_usd = float((trade.get("entry") or {}).get("size_usd") or 0)
    exit_value = max(0.0, size_usd + pnl_usd)
    if chain and exit_value > 0:
        await credit(str(user["_id"]), chain, exit_value)

    # Bump parent bot stats if this trade was tied to one
    bot_id = trade.get("bot_id")
    if bot_id:
        try:
            bot_oid = ObjectId(bot_id)
            inc = {"stats.pnl_usd": round(pnl_usd, 2)}
            if pnl_usd > 0:
                inc["stats.wins"] = 1
            await db.bots.update_one({"_id": bot_oid}, {"$inc": inc})
        except Exception:
            pass

    from app.services.event_bus import BOT_TRADE_CLOSED
    await bus.publish(
        BOT_TRADE_CLOSED,
        {
            "id": trade_id,
            "bot_id": bot_id,
            "token_id": trade.get("token_id"),
            "symbol": trade.get("symbol"),
            "pnl_usd": round(pnl_usd, 2),
            "pnl_pct": round(pnl_pct, 2),
            "reason": "manual_telegram",
        },
    )

    await tg.answer_callback_query(
        cb_id,
        f"Closed {(trade.get('symbol') or '')} · {'+' if pnl_pct >= 0 else ''}{pnl_pct:.2f}%",
    )

    # Replace the card with the closed version
    if message_id is not None:
        fresh = await db.bot_trades.find_one({"_id": oid})
        try:
            await tg.edit_message_text(
                chat_id,
                message_id,
                _fmt_position_card(fresh or trade),
                reply_markup={
                    "inline_keyboard": [[
                        {"text": "🔁 Buy again", "callback_data": f"r:{trade_id}"},
                    ]],
                },
            )
        except Exception:
            pass


async def _execute_rebuy(
    tg: TelegramClient, cb_id: str, chat_id: int, message_id: int | None, trade_id: str
) -> None:
    """Buy-in again — open a fresh $100 position on the same token."""
    db = mongo.db()
    user = await db.users.find_one({"telegram.chat_id": chat_id})
    if not user:
        await tg.answer_callback_query(cb_id, "You're not linked.", show_alert=True)
        return
    user_id = str(user["_id"])

    try:
        oid = ObjectId(trade_id)
    except Exception:
        await tg.answer_callback_query(cb_id, "Bad trade id.", show_alert=True)
        return

    ref = await db.bot_trades.find_one({"_id": oid, "user_id": user_id})
    if not ref:
        await tg.answer_callback_query(cb_id, "Trade not found.", show_alert=True)
        return

    token_id = ref.get("token_id")
    chain = ref.get("chain") or ""
    symbol = ref.get("symbol")
    if not token_id:
        await tg.answer_callback_query(cb_id, "Missing token.", show_alert=True)
        return

    # Dup-guard: if they already have a fresh open position on this token, bail
    dup = await db.bot_trades.find_one(
        {"user_id": user_id, "token_id": token_id, "status": "open"}
    )
    if dup:
        await tg.answer_callback_query(cb_id, "You already hold this token.", show_alert=True)
        return

    price = await _resolve_entry_price(token_id)
    if not price or price <= 0:
        await tg.answer_callback_query(cb_id, "Couldn't price this token.", show_alert=True)
        return

    size_usd = float(TELEGRAM_QUICK_BUY_USD)

    # Balance gate
    debited = await try_debit(user_id, chain, size_usd)
    if not debited:
        bal = await get_balance(user_id, chain)
        await tg.answer_callback_query(
            cb_id,
            f"Insufficient {chain.upper()} balance (${bal:.2f}). Run /fund {int(size_usd)} {chain[:3]}.",
            show_alert=True,
        )
        return

    amount = size_usd / price
    now = datetime.utcnow()

    doc = {
        "bot_id": None,
        "user_id": user_id,
        "token_id": token_id,
        "chain": chain,
        "symbol": symbol,
        "entry": {
            "price_usd": price,
            "size_usd": round(size_usd, 2),
            "amount_tokens": amount,
            "timestamp": now,
            "trigger": {"type": "telegram_manual", "rebuy_of": trade_id},
        },
        "exit": None,
        "current_price_usd": price,
        "peak_price_usd": price,
        "take_profit_pct": 50.0,   # defaults per user spec
        "stop_loss_pct": 20.0,
        "trailing_stop_pct": 0.0,
        "status": "open",
        "is_paper": True,
        "opened_at": now,
        "closed_at": None,
        "last_updated": now,
    }
    try:
        res = await db.bot_trades.insert_one(doc)
    except Exception as e:
        # Refund the debit since the trade never made it in
        await credit(user_id, chain, size_usd)
        await tg.answer_callback_query(cb_id, f"Trade failed: {e}", show_alert=True)
        return
    new_id = str(res.inserted_id)

    await bus.publish(BOT_TRADE_OPENED, {
        "id": new_id,
        "bot_id": None,
        "user_id": user_id,
        "token_id": token_id,
        "chain": chain,
        "symbol": symbol,
        "entry": {
            "price_usd": price,
            "size_usd": round(size_usd, 2),
            "amount_tokens": amount,
            "timestamp": now.isoformat(),
            "trigger": doc["entry"]["trigger"],
        },
        "status": "open",
    })

    await tg.answer_callback_query(cb_id, f"Bought ${TELEGRAM_QUICK_BUY_USD} of ${symbol or 'token'}")

    # Show a fresh card for the new position
    fresh = await db.bot_trades.find_one({"_id": res.inserted_id})
    if fresh:
        keyboard = {
            "inline_keyboard": [[
                {"text": "❌ Close", "callback_data": f"c:{new_id}"},
                {"text": "🔁 Buy again", "callback_data": f"r:{new_id}"},
            ]],
        }
        await tg.send_message(
            chat_id,
            _fmt_position_card(fresh),
            reply_markup=keyboard,
        )


async def _handle_callback(tg: TelegramClient, cb: dict) -> None:
    cb_id = cb.get("id")
    data = cb.get("data") or ""
    msg = cb.get("message") or {}
    chat = msg.get("chat") or {}
    chat_id = chat.get("id")
    message_id = msg.get("message_id")
    if not cb_id or not chat_id:
        return

    if data.startswith("b:"):
        signal_id = data[2:]
        await _execute_quick_buy(tg, cb_id, chat_id, message_id, signal_id)
        return

    if data.startswith("o:"):
        trade_id = data[2:]
        # Open card for a position
        db = mongo.db()
        user = await db.users.find_one({"telegram.chat_id": chat_id})
        if not user:
            await tg.answer_callback_query(cb_id, "You're not linked.", show_alert=True)
            return
        try:
            t = await db.bot_trades.find_one(
                {"_id": ObjectId(trade_id), "user_id": str(user["_id"])}
            )
        except Exception:
            t = None
        if not t:
            await tg.answer_callback_query(cb_id, "Trade not found.", show_alert=True)
            return
        await tg.answer_callback_query(cb_id)
        buttons = []
        if t.get("status") == "open":
            buttons.append({"text": "❌ Close", "callback_data": f"c:{trade_id}"})
        buttons.append({"text": "🔁 Buy again", "callback_data": f"r:{trade_id}"})
        await tg.send_message(
            chat_id,
            _fmt_position_card(t),
            reply_markup={"inline_keyboard": [buttons]},
        )
        return

    if data.startswith("c:"):
        trade_id = data[2:]
        await _execute_close(tg, cb_id, chat_id, message_id, trade_id)
        return

    if data.startswith("r:"):
        trade_id = data[2:]
        await _execute_rebuy(tg, cb_id, chat_id, message_id, trade_id)
        return

    if data.startswith("v:"):
        signal_id = data[2:]
        db = mongo.db()
        try:
            sig = await db.signals.find_one({"_id": ObjectId(signal_id)})
        except Exception:
            sig = None
        if not sig:
            await tg.answer_callback_query(cb_id, "Signal expired.", show_alert=True)
            return
        token_id = sig.get("token_id") or ""
        await tg.answer_callback_query(cb_id)
        await tg.send_message(
            chat_id,
            f"📊 <b>${sig.get('symbol') or token_id[:10]}</b>\n"
            f"token_id: <code>{token_id}</code>\n"
            f"conviction: <b>{int(sig.get('conviction_score') or 0)}</b>\n"
            f"status: <b>{(sig.get('status') or '').upper()}</b>",
        )
        return

    await tg.answer_callback_query(cb_id)


async def _handle_message(tg: TelegramClient, msg: dict) -> None:
    text = (msg.get("text") or "").strip()
    chat = msg.get("chat") or {}
    if not text or not chat:
        return

    # Strip bot mention (e.g. /start@CovenAlphaBot CODE)
    parts = text.split()
    cmd = parts[0]
    if "@" in cmd:
        cmd = cmd.split("@", 1)[0]
    cmd = cmd.lower()

    handler = COMMANDS.get(cmd)
    if handler:
        try:
            await handler(tg, chat, parts[1:])
        except Exception as e:
            print(f"[telegram_poller] command {cmd} failed: {e}")
        return

    # Any non-command message: send help once.
    await tg.send_message(
        chat["id"],
        "I don't know that one. Try <code>/help</code>.",
    )


# ---------------------------------------------------------------------
# Long-poll loop
# ---------------------------------------------------------------------

async def run() -> None:
    if not settings.telegram_bot_token:
        print("[telegram_poller] TELEGRAM_BOT_TOKEN missing — skipping")
        return

    # Sanity check the token + auto-detect bot username if not configured.
    try:
        async with TelegramClient() as tg:
            me = await tg.get_me()
            name = me.get("username")
            if name and not settings.telegram_bot_username:
                # In-process override so start-link returns a working deep link
                settings.telegram_bot_username = name
            print(f"[telegram_poller] connected as @{name}")
    except Exception as e:
        print(f"[telegram_poller] bot token check failed: {e} — skipping")
        return

    offset: int | None = None
    while not _stop.is_set():
        try:
            async with TelegramClient() as tg:
                updates = await tg.get_updates(offset=offset, timeout=25)
                for upd in updates:
                    offset = max(offset or 0, int(upd.get("update_id", 0)) + 1)
                    msg = upd.get("message")
                    if msg:
                        await _handle_message(tg, msg)
                        continue
                    cb = upd.get("callback_query")
                    if cb:
                        await _handle_callback(tg, cb)
        except Exception as e:
            print(f"[telegram_poller] loop error: {e}")
            try:
                await asyncio.wait_for(_stop.wait(), timeout=3)
            except asyncio.TimeoutError:
                pass

    print("[telegram_poller] stopped")


def stop() -> None:
    _stop.set()

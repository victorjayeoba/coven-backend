"""
Bot runner — paper execution for user-created bots.

Subscribes to the live event bus and reacts per bot type:

  - Copy bots (type=copy):
      * SWAP_EVENT where wallet == target_wallet & side=buy  -> open position
      * SWAP_EVENT where wallet == target_wallet & side=sell -> close (if copy_exits)

  - Signal bots (type=signal):
      * SIGNAL_SCORED where conviction >= min_conviction     -> open position

All bots also react to:
  - PRICE_UPDATE  -> mark open positions; close on TP / SL / trailing stop.

Bots are stored per-user in `bots`. Paper trades they produce go in `bot_trades`
(kept separate from the system-wide `trades` collection so bot P&L is isolated).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from bson import ObjectId

from app.db import mongo
from app.services.ave_client import AveClient
from app.services.balance_ledger import credit, try_debit
from app.services.event_bus import (
    BOT_TRADE_CLOSED,
    BOT_TRADE_OPENED,
    BOT_UPDATED,
    PRICE_UPDATE,
    SIGNAL_SCORED,
    SWAP_EVENT,
    bus,
)

# ---------------------------------------------------------------------
# In-memory last-price cache fed by PRICE_UPDATE — lets us size entries
# without hitting /tokens REST on every swap.
# ---------------------------------------------------------------------
_last_price: dict[str, float] = {}


async def _entry_price(token_id: str) -> float | None:
    """
    Resolve USD price for a token. Order:
      1. In-memory cache (populated by PRICE_UPDATE stream)
      2. AVE (indexed meme tokens + all major)
      3. Jupiter Price API (covers any Solana token with a tradeable route) ← fallback
    """
    cached = _last_price.get(token_id)
    if cached and cached > 0:
        return cached

    # --- 1. AVE ---
    try:
        async with AveClient() as ave:
            data = await ave.token_detail(token_id)
        tok = (
            data.get("token")
            if isinstance(data, dict) and isinstance(data.get("token"), dict)
            else data
        )
        if isinstance(tok, dict):
            try:
                p = float(tok.get("current_price_usd"))
                if p > 0:
                    _last_price[token_id] = p
                    return p
            except (TypeError, ValueError):
                pass
    except Exception:
        pass

    # --- 2. Jupiter fallback (Solana only) ---
    if token_id.endswith("-solana"):
        mint = token_id.rsplit("-", 1)[0]
        try:
            import httpx
            async with httpx.AsyncClient(timeout=5) as client:
                r = await client.get(
                    f"https://price.jup.ag/v6/price?ids={mint}"
                )
            if r.status_code == 200:
                js = r.json()
                data = (js.get("data") or {}).get(mint)
                if isinstance(data, dict):
                    p = float(data.get("price") or 0)
                    if p > 0:
                        _last_price[token_id] = p
                        return p
        except Exception:
            pass

    return None


def _compute_size_usd(bot: dict, target_amount_usd: float | None) -> float:
    mode = bot.get("size_mode", "fixed")
    if bot.get("type") == "signal":
        return float(bot.get("size_usd") or 0)
    if mode == "fixed":
        return float(bot.get("size_usd") or 0)
    if mode == "multiplier" and target_amount_usd:
        return float(target_amount_usd) * float(bot.get("multiplier") or 1)
    if mode == "percent" and target_amount_usd:
        return float(target_amount_usd) * float(bot.get("percent_of_target") or 0) / 100.0
    return float(bot.get("size_usd") or 0)


async def _open_count(bot_id: str) -> int:
    db = mongo.db()
    return await db.bot_trades.count_documents({"bot_id": bot_id, "status": "open"})


async def _open_position(
    bot: dict,
    token_id: str,
    chain: str,
    symbol: str | None,
    trigger: dict,
    target_amount_usd: float | None = None,
) -> None:
    db = mongo.db()
    bot_id = str(bot["_id"])
    bot_name = bot.get("name") or bot_id[:6]
    tag = symbol or token_id[:12]

    # Respect max_concurrent
    max_conc = int(bot.get("max_concurrent") or 0)
    if max_conc > 0 and await _open_count(bot_id) >= max_conc:
        print(f"[bot_runner] {bot_name} SKIP {tag}: max_concurrent={max_conc} reached")
        return

    # Don't double-enter the same token for this bot
    dup = await db.bot_trades.find_one(
        {"bot_id": bot_id, "token_id": token_id, "status": "open"}
    )
    if dup:
        print(f"[bot_runner] {bot_name} SKIP {tag}: already open on this token")
        return

    entry_price = await _entry_price(token_id)
    if not entry_price or entry_price <= 0:
        print(
            f"[bot_runner] {bot_name} SKIP {tag}: no entry price "
            f"(AVE doesn't index {token_id}) — swap was detected but bot can't size a trade"
        )
        return

    size_usd = _compute_size_usd(bot, target_amount_usd)
    if size_usd <= 0:
        print(f"[bot_runner] {bot_name} SKIP {tag}: computed size_usd={size_usd}")
        return

    # Balance gate: refuse to open if the user's chain wallet can't cover it.
    user_id = bot.get("user_id")
    if user_id and chain:
        debited = await try_debit(user_id, chain, size_usd)
        if not debited:
            print(
                f"[bot_runner] {bot_name} SKIP {tag}: insufficient {chain} "
                f"balance for ${size_usd:.2f} — fund wallet to enable"
            )
            return

    amount_tokens = size_usd / entry_price
    now = datetime.utcnow()

    doc = {
        "bot_id": bot_id,
        "user_id": bot["user_id"],
        "token_id": token_id,
        "chain": chain,
        "symbol": symbol,
        "entry": {
            "price_usd": entry_price,
            "size_usd": round(size_usd, 2),
            "amount_tokens": amount_tokens,
            "timestamp": now,
            "trigger": trigger,
        },
        "exit": None,
        "current_price_usd": entry_price,
        "peak_price_usd": entry_price,
        "take_profit_pct": float(bot.get("take_profit_pct") or 0),
        "stop_loss_pct": float(bot.get("stop_loss_pct") or 0),
        "trailing_stop_pct": float(bot.get("trailing_stop_pct") or 0),
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
        if user_id and chain:
            await credit(user_id, chain, size_usd)
        print(f"[bot_runner] insert failed for {tag}, refunded: {e}")
        return
    doc["_id"] = str(res.inserted_id)
    doc["id"] = doc["_id"]

    await db.bots.update_one(
        {"_id": bot["_id"]},
        {"$inc": {"stats.trades": 1}, "$set": {"last_updated": now}},
    )

    await bus.publish(BOT_TRADE_OPENED, _json_safe(doc))
    await _emit_bot_update(bot_id)
    print(
        f"[BOT OPEN] {bot.get('name')} {symbol or token_id} "
        f"@ ${entry_price:.6f} size=${size_usd:.2f} trigger={trigger.get('type')}"
    )


async def _close_position(
    trade: dict,
    exit_price: float,
    reason: str,
) -> None:
    db = mongo.db()
    entry_price = float(trade.get("entry", {}).get("price_usd") or 0)
    amount = float(trade.get("entry", {}).get("amount_tokens") or 0)
    size_usd = float(trade.get("entry", {}).get("size_usd") or 0)
    if entry_price <= 0:
        return

    pnl_usd = (exit_price - entry_price) * amount
    pnl_pct = (exit_price / entry_price - 1.0) * 100.0
    now = datetime.utcnow()

    await db.bot_trades.update_one(
        {"_id": trade["_id"]},
        {
            "$set": {
                "exit": {
                    "price_usd": exit_price,
                    "timestamp": now,
                    "reason": reason,
                },
                "pnl_usd": round(pnl_usd, 2),
                "pnl_pct": round(pnl_pct, 2),
                "current_price_usd": exit_price,
                "status": "closed",
                "closed_at": now,
                "last_updated": now,
            }
        },
    )

    # Credit the exit value back to the user's chain wallet (size + pnl).
    user_id = trade.get("user_id")
    chain = trade.get("chain")
    exit_value = max(0.0, size_usd + pnl_usd)
    if user_id and chain and exit_value > 0:
        await credit(user_id, chain, exit_value)

    bot_id = trade.get("bot_id")
    if bot_id:
        inc = {"stats.pnl_usd": round(pnl_usd, 2)}
        if pnl_usd > 0:
            inc["stats.wins"] = 1
        try:
            await db.bots.update_one({"_id": ObjectId(bot_id)}, {"$inc": inc})
        except Exception:
            pass

    payload = {
        "id": str(trade["_id"]),
        "bot_id": bot_id,
        "token_id": trade.get("token_id"),
        "symbol": trade.get("symbol"),
        "exit_price": exit_price,
        "pnl_usd": round(pnl_usd, 2),
        "pnl_pct": round(pnl_pct, 2),
        "reason": reason,
    }
    await bus.publish(BOT_TRADE_CLOSED, payload)
    await _emit_bot_update(bot_id)

    print(
        f"[BOT CLOSE] trade={payload['id'][:8]} "
        f"exit=${exit_price:.6f} pnl={pnl_pct:+.2f}% reason={reason}"
    )


async def _emit_bot_update(bot_id: str) -> None:
    db = mongo.db()
    bot = await db.bots.find_one({"_id": ObjectId(bot_id)})
    if bot:
        bot["id"] = str(bot.pop("_id"))
        await bus.publish(BOT_UPDATED, _json_safe(bot))


def _json_safe(doc: dict) -> dict:
    out: dict = {}
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            out[k] = str(v)
        elif isinstance(v, datetime):
            out[k] = v.isoformat()
        elif isinstance(v, dict):
            out[k] = _json_safe(v)
        elif isinstance(v, list):
            out[k] = [_json_safe(x) if isinstance(x, dict) else x for x in v]
        else:
            out[k] = v
    return out


# ---------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------

async def on_swap(event: dict) -> None:
    """Copy-bots: buy matches -> open; sell matches -> close (if copy_exits)."""
    wallet = (event.get("wallet") or "").lower()
    chain = event.get("chain")
    token_id = event.get("token_id")
    side = event.get("side")
    if not wallet or not token_id or side not in {"buy", "sell"}:
        return

    db = mongo.db()

    if side == "buy":
        # Match copy bots watching this wallet+chain
        cursor = db.bots.find({
            "type": "copy",
            "status": "active",
            "chain": chain,
            "target_wallet_lc": wallet,
        })
        async for bot in cursor:
            try:
                await _open_position(
                    bot,
                    token_id=token_id,
                    chain=chain,
                    symbol=event.get("symbol"),
                    trigger={
                        "type": "copy_entry",
                        "source_wallet": wallet,
                        "tx_hash": event.get("tx_hash"),
                    },
                    target_amount_usd=(
                        float(event["amount_usd"])
                        if event.get("amount_usd") is not None
                        else None
                    ),
                )
            except Exception as e:
                print(f"[bot_runner] open failed for bot={bot.get('name')}: {e}")
        return

    # side == "sell"
    cursor = db.bots.find({
        "type": "copy",
        "status": "active",
        "chain": chain,
        "target_wallet_lc": wallet,
        "copy_exits": True,
    })
    async for bot in cursor:
        open_trade = await db.bot_trades.find_one(
            {"bot_id": str(bot["_id"]), "token_id": token_id, "status": "open"}
        )
        if not open_trade:
            continue
        exit_price = _last_price.get(token_id) or float(
            open_trade.get("current_price_usd") or 0
        )
        if exit_price <= 0:
            continue
        try:
            await _close_position(open_trade, exit_price, reason="target_sold")
        except Exception as e:
            print(f"[bot_runner] close failed: {e}")


async def on_signal(signal: dict) -> None:
    """Signal bots: open when a scored signal meets threshold."""
    status = signal.get("status")
    if status not in {"exec", "partial"}:
        return
    conviction = int(signal.get("conviction_score") or 0)
    chain = signal.get("chain")
    token_id = signal.get("token_id")
    if not token_id:
        return

    db = mongo.db()
    cursor = db.bots.find({
        "type": "signal",
        "status": "active",
        "chain": chain,
    })
    async for bot in cursor:
        min_c = int(bot.get("min_conviction") or 0)
        if conviction < min_c:
            continue
        cf = (bot.get("cluster_filter") or "").strip()
        if cf and str(signal.get("cluster_id") or "") != cf:
            continue
        try:
            await _open_position(
                bot,
                token_id=token_id,
                chain=chain or "",
                symbol=signal.get("symbol"),
                trigger={
                    "type": "signal",
                    "signal_id": signal.get("id"),
                    "conviction": conviction,
                    "cluster_id": signal.get("cluster_id"),
                },
            )
        except Exception as e:
            print(f"[bot_runner] signal-open failed for bot={bot.get('name')}: {e}")


async def on_price(payload: dict) -> None:
    """Patch open bot trades + enforce TP / SL / trailing stop."""
    token_id = payload.get("token_id")
    price = payload.get("price_usd")
    if not token_id or price is None:
        return
    try:
        price = float(price)
    except (TypeError, ValueError):
        return
    if price <= 0:
        return

    _last_price[token_id] = price

    db = mongo.db()
    # Update peak for trailing + current_price on every open position
    await db.bot_trades.update_many(
        {"token_id": token_id, "status": "open"},
        [
            {
                "$set": {
                    "current_price_usd": price,
                    "peak_price_usd": {
                        "$max": ["$peak_price_usd", price],
                    },
                    "last_updated": datetime.utcnow(),
                }
            }
        ],
    )

    # Check TP / SL / trailing for each open trade on this token
    async for t in db.bot_trades.find({"token_id": token_id, "status": "open"}):
        entry_price = float(t.get("entry", {}).get("price_usd") or 0)
        if entry_price <= 0:
            continue
        change_pct = (price / entry_price - 1.0) * 100.0

        tp = float(t.get("take_profit_pct") or 0)
        sl = float(t.get("stop_loss_pct") or 0)
        trail = float(t.get("trailing_stop_pct") or 0)
        peak = float(t.get("peak_price_usd") or entry_price)

        if tp > 0 and change_pct >= tp:
            await _close_position(t, price, reason="take_profit")
            continue
        if sl > 0 and change_pct <= -sl:
            await _close_position(t, price, reason="stop_loss")
            continue
        if trail > 0 and peak > 0:
            drawdown_pct = (price / peak - 1.0) * 100.0
            if drawdown_pct <= -trail and price > entry_price:
                await _close_position(t, price, reason="trailing_stop")


# ---------------------------------------------------------------------

def register() -> None:
    bus.subscribe(SWAP_EVENT, on_swap)
    bus.subscribe(SIGNAL_SCORED, on_signal)
    bus.subscribe(PRICE_UPDATE, on_price)

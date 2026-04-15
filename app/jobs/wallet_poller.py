"""
Wallet poller — REST-driven wallet activity tracking for copy bots.

Why this exists:
  AVE's WSS `multi_tx` only streams tokens we've explicitly subscribed to (the
  ~145 trending/pump tokens). A copy bot's target wallet can trade ANYTHING,
  so AVE misses most swaps. Our `wallet_watcher` tried a `tx` wallet-level
  topic that AVE's feed doesn't actually populate.

What this does:
  - Solana: poll Helius parsed transactions API (`/v0/addresses/<a>/transactions?type=SWAP`)
    — returns decoded swap events with tokens + amounts for ANY DEX
    (Jupiter, Raydium, DFlow, Orca, Meteora, Pump.fun).
  - BSC: poll BSCScan ERC-20 transfer list (`module=account&action=tokentx`)
    and reconstruct swaps from transfer pairs within the same tx.

  Every detected swap is normalized into the same SWAP_EVENT shape that
  `bot_runner.on_swap` already consumes. Zero changes needed downstream.

Cursors:
  In-memory `(wallet, chain) -> last_signature_or_timestamp_seen`. First poll
  after a bot is added only processes swaps strictly newer than "now" minus a
  grace window — prevents backfilling stale trades.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx

from app.config import settings
from app.db import mongo
from app.services.event_bus import SWAP_EVENT, bus

# ---------------------------------------------------------------------

POLL_INTERVAL = max(5, int(settings.wallet_poll_interval_seconds))

# Solana stables / natives we don't want copy bots to chase
SOLANA_SKIP = {
    "So11111111111111111111111111111111111111112",  # wSOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
}
BSC_SKIP = {
    "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",  # WBNB
    "0x55d398326f99059ff775485246999027b3197955",  # USDT
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",  # USDC
    "0xe9e7cea3dedca5984780bafc599bd69add087d56",  # BUSD
    "0x2170ed0880ac9a755fd29b2688956bd959f933f8",  # WETH
}

_stop = asyncio.Event()

# Startup wall-clock — any event strictly older than this (minus a small grace)
# is considered backfill and dropped, so we never copy stale trades.
_startup_ts = time.time() - 30

# Cursors per (wallet, chain)
_sol_cursor: dict[tuple[str, str], str] = {}   # wallet_lc -> last signature seen
_bsc_cursor: dict[tuple[str, str], int] = {}   # wallet_lc -> last blockNumber seen


# ---------------------------------------------------------------------
# Load active target set from DB
# ---------------------------------------------------------------------

async def _load_targets() -> list[tuple[str, str]]:
    """List of (wallet_lc, chain) for every active copy bot."""
    db = mongo.db()
    out: list[tuple[str, str]] = []
    cursor = db.bots.find(
        {
            "type": "copy",
            "status": "active",
            "target_wallet_lc": {"$exists": True, "$ne": ""},
            "chain": {"$in": ["solana", "bsc"]},
        },
        {"target_wallet": 1, "target_wallet_lc": 1, "chain": 1},
    )
    async for b in cursor:
        # Helius keeps case for Solana; BSCScan wants the lower-case hex.
        addr = (
            b.get("target_wallet") if b.get("chain") == "solana"
            else (b.get("target_wallet_lc") or b.get("target_wallet"))
        )
        chain = b.get("chain")
        if addr and chain:
            out.append((addr, chain))
    # dedupe while preserving insertion order
    seen: set[tuple[str, str]] = set()
    deduped: list[tuple[str, str]] = []
    for pair in out:
        if pair not in seen:
            seen.add(pair)
            deduped.append(pair)
    return deduped


# ---------------------------------------------------------------------
# Solana — Helius parsed transactions
# ---------------------------------------------------------------------

async def _poll_solana(client: httpx.AsyncClient, wallet: str) -> None:
    key = (wallet, "solana")
    # No `type=SWAP` filter — Helius tags Pump.fun / Meteora as UNKNOWN, and
    # we'd rather reconstruct from tokenTransfers than miss those trades.
    params = {"api-key": settings.helius_api_key, "limit": 25}
    url = f"{settings.helius_base_url}/v0/addresses/{wallet}/transactions"

    try:
        r = await client.get(url, params=params, timeout=15)
    except Exception as e:
        print(f"[wallet_poller] helius error for {wallet[:8]}…: {e}")
        return
    if r.status_code >= 400:
        print(f"[wallet_poller] helius {r.status_code} for {wallet[:8]}…: {r.text[:200]}")
        return

    try:
        items = r.json()
    except Exception:
        return
    if not isinstance(items, list):
        return

    last_sig = _sol_cursor.get(key)
    new_events: list[dict] = []

    # Helius returns newest-first
    for tx in items:
        sig = tx.get("signature")
        ts = int(tx.get("timestamp") or 0)
        if last_sig and sig == last_sig:
            break
        if ts and ts < int(_startup_ts):
            # skip anything older than when we started
            continue
        new_events.append(tx)

    if items:
        _sol_cursor[key] = items[0].get("signature") or last_sig

    # Emit oldest-first so bot_runner sees them chronologically
    for tx in reversed(new_events):
        event = _normalize_helius_swap(tx, wallet)
        if event:
            await bus.publish(SWAP_EVENT, event)
            print(
                f"[wallet_poller] SOL {event['side']} "
                f"{event['symbol'] or event['token_id'][:10]} "
                f"by {wallet[:8]}… (${event.get('amount_usd') or '?'})"
            )


def _normalize_helius_swap(tx: dict, wallet: str) -> dict | None:
    """
    Fast path: Helius's enhanced parser filled in `events.swap` (Jupiter,
    Raydium, Orca). Fallback: reconstruct from `tokenTransfers` so we also
    catch Pump.fun, Meteora, DFlow, and anything Helius tags as UNKNOWN.

    Buy = wallet received a non-stable token. Sell = wallet sent one.
    """
    ts = int(tx.get("timestamp") or time.time())

    events = (tx.get("events") or {}).get("swap")
    if isinstance(events, dict):
        def _filter_non_stable(toks: list[dict]) -> dict | None:
            for t in toks or []:
                mint = t.get("mint")
                if mint and mint not in SOLANA_SKIP:
                    return t
            return None

        out_token = _filter_non_stable(events.get("tokenOutputs"))
        in_token = _filter_non_stable(events.get("tokenInputs"))
        side = None
        token = None
        if out_token:
            side, token = "buy", out_token
        elif in_token:
            side, token = "sell", in_token
        if side and token:
            mint = token.get("mint")
            if mint and mint not in SOLANA_SKIP:
                return {
                    "wallet": wallet,
                    "token_id": f"{mint}-solana",
                    "chain": "solana",
                    "symbol": None,
                    "timestamp": float(ts),
                    "side": side,
                    "tx_hash": tx.get("signature"),
                    "amount_usd": None,
                }

    return _reconstruct_from_transfers(tx, wallet, ts)


def _reconstruct_from_transfers(tx: dict, wallet: str, ts: int) -> dict | None:
    """Derive buy/sell from raw tokenTransfers when events.swap is missing."""
    transfers = tx.get("tokenTransfers") or []
    if not isinstance(transfers, list):
        return None

    received: dict | None = None
    sent: dict | None = None
    for t in transfers:
        if not isinstance(t, dict):
            continue
        mint = t.get("mint")
        if not mint or mint in SOLANA_SKIP:
            continue
        if t.get("toUserAccount") == wallet and received is None:
            received = t
        elif t.get("fromUserAccount") == wallet and sent is None:
            sent = t

    side = None
    token = None
    if received:
        side, token = "buy", received
    elif sent:
        side, token = "sell", sent
    else:
        return None

    return {
        "wallet": wallet,
        "token_id": f"{token['mint']}-solana",
        "chain": "solana",
        "symbol": None,
        "timestamp": float(ts),
        "side": side,
        "tx_hash": tx.get("signature"),
        "amount_usd": None,
    }


# ---------------------------------------------------------------------
# BSC — BSCScan token transfer list
# ---------------------------------------------------------------------

async def _poll_bsc(client: httpx.AsyncClient, wallet: str) -> None:
    key = (wallet, "bsc")
    # Etherscan V2 — same endpoint works for BSC + 60+ EVM chains via chainid.
    params = {
        "chainid": settings.bscscan_chain_id,
        "module": "account",
        "action": "tokentx",
        "address": wallet,
        "sort": "desc",
        "apikey": settings.bscscan_api_key or "",
    }
    try:
        r = await client.get(settings.bscscan_base_url, params=params, timeout=15)
    except Exception as e:
        print(f"[wallet_poller] etherscan-v2 error for {wallet[:8]}…: {e}")
        return
    if r.status_code >= 400:
        print(f"[wallet_poller] etherscan-v2 {r.status_code} for {wallet[:8]}…")
        return

    try:
        data = r.json()
    except Exception:
        return
    if str(data.get("status")) != "1":
        # BSCScan returns status=0 + "No transactions found" when the wallet
        # has no txs. Don't spam logs for that case.
        return
    rows: list[dict] = data.get("result") or []
    if not isinstance(rows, list):
        return

    last_block = _bsc_cursor.get(key) or 0
    new_rows = []
    max_block = last_block

    for row in rows:
        try:
            block = int(row.get("blockNumber") or 0)
            ts = int(row.get("timeStamp") or 0)
        except ValueError:
            continue
        if block <= last_block:
            break
        if ts and ts < int(_startup_ts):
            continue
        new_rows.append(row)
        if block > max_block:
            max_block = block

    if max_block > last_block:
        _bsc_cursor[key] = max_block

    # Group transfers by txHash and reconstruct swaps
    by_tx: dict[str, list[dict]] = {}
    for row in new_rows:
        tx_hash = row.get("hash")
        if not tx_hash:
            continue
        by_tx.setdefault(tx_hash, []).append(row)

    # Chronological: oldest first
    for tx_hash, transfers in sorted(
        by_tx.items(),
        key=lambda kv: int(kv[1][0].get("timeStamp") or 0),
    ):
        event = _normalize_bscscan_swap(transfers, wallet)
        if event:
            await bus.publish(SWAP_EVENT, event)
            print(
                f"[wallet_poller] BSC {event['side']} "
                f"{event['symbol'] or event['token_id'][:10]} "
                f"by {wallet[:8]}… (${event.get('amount_usd') or '?'})"
            )


def _normalize_bscscan_swap(
    transfers: list[dict], wallet: str
) -> dict | None:
    """
    A swap shows up on BSCScan as two token transfers in the same txHash —
    one OUT of the wallet (sent) and one IN to the wallet (received).
    We label it buy/sell based on which side is the non-stable token.
    """
    wallet_lc = wallet.lower()
    incoming: list[dict] = []
    outgoing: list[dict] = []
    for t in transfers:
        to_addr = (t.get("to") or "").lower()
        from_addr = (t.get("from") or "").lower()
        if to_addr == wallet_lc:
            incoming.append(t)
        elif from_addr == wallet_lc:
            outgoing.append(t)

    if not incoming and not outgoing:
        return None

    def _pick_non_stable(tfs: list[dict]) -> dict | None:
        for t in tfs:
            addr = (t.get("contractAddress") or "").lower()
            if addr and addr not in BSC_SKIP:
                return t
        return None

    # Prefer the non-stable side
    got = _pick_non_stable(incoming)  # received → buy
    gave = _pick_non_stable(outgoing)  # sent → sell

    side = None
    token = None
    if got:
        side = "buy"
        token = got
    elif gave:
        side = "sell"
        token = gave
    else:
        return None

    contract = (token.get("contractAddress") or "").lower()
    symbol = token.get("tokenSymbol")
    ts = int(token.get("timeStamp") or time.time())
    return {
        "wallet": wallet_lc,
        "token_id": f"{contract}-bsc",
        "chain": "bsc",
        "symbol": symbol,
        "timestamp": float(ts),
        "side": side,
        "tx_hash": token.get("hash"),
        "amount_usd": None,
    }


# ---------------------------------------------------------------------
# Supervisor
# ---------------------------------------------------------------------

async def run() -> None:
    # Always run BSC polling (no WSS equivalent). Solana polling only runs if
    # explicitly enabled — otherwise helius_stream (WSS) is doing that job.
    solana_enabled = bool(settings.helius_api_key) and settings.enable_wallet_poller
    bsc_enabled = bool(settings.bscscan_api_key)
    if not solana_enabled and not bsc_enabled:
        print("[wallet_poller] disabled (solana -> helius_stream, bsc -> no key)")
        return

    print(
        f"[wallet_poller] starting · "
        f"solana={'yes (fallback)' if solana_enabled else 'handled by helius_stream'} "
        f"bsc={'yes' if bsc_enabled else 'no'} "
        f"interval={POLL_INTERVAL}s"
    )

    async with httpx.AsyncClient() as client:
        while not _stop.is_set():
            try:
                targets = await _load_targets()
                tasks = []
                for wallet, chain in targets:
                    if chain == "solana" and solana_enabled:
                        tasks.append(_poll_solana(client, wallet))
                    elif chain == "bsc" and bsc_enabled:
                        tasks.append(_poll_bsc(client, wallet))
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as e:
                print(f"[wallet_poller] loop error: {e}")

            try:
                await asyncio.wait_for(_stop.wait(), timeout=POLL_INTERVAL)
            except asyncio.TimeoutError:
                pass

    print("[wallet_poller] stopped")


def stop() -> None:
    _stop.set()

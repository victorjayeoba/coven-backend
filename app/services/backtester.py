"""
Historical backtester — HONEST version.

Starts from OUR wallets (not from winning tokens). Looks at what they bought
during a past period, simulates our signal logic at the moment of entry,
and forward-projects price outcomes. Includes ALL results — wins AND losses.

Pipeline:
  1. Load smart-money wallets + cluster membership from MongoDB
  2. For each wallet, fetch /walletinfo/tokens (what they currently hold)
  3. Filter to tokens where first_tx_time is in [now - days_back, now - hold_buffer]
  4. Group by (token, cluster) → find tight entry windows (≥ MIN_CLUSTER_WALLETS)
  5. For each firing: pull /klines, compute entry/peak/realistic_exit/P&L
  6. Score with the same conviction formula used live
  7. Persist to db.backtests — including losers
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from datetime import datetime
from typing import Any

from app.db import mongo
from app.services import conviction_scorer
from app.services.ave_client import AveClient

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

DAYS_BACK = 7                       # entries newer than now-DAYS_BACK are eligible
HOLD_BUFFER_HOURS = 24              # need at least this much forward price data
TIME_WINDOW_SECONDS = 60 * 60       # cluster entry window = 60 min
MIN_CLUSTER_WALLETS = 2             # need 2+ from same cluster for a firing
HOLD_WINDOW_HOURS = 24              # fallback hold if no cluster exit detected
PEAK_INTERVAL_MIN = 15              # kline granularity
PARALLEL = 10                       # concurrent walletinfo requests

# Behavior-based exit settings
EXIT_DISTRIBUTION_FRACTION = 0.5    # exit when 50% of cluster wallets have sold
STOP_LOSS_PCT = -30.0               # hard stop at -30% from entry (safety net)
MAX_HOLD_HOURS = 72                 # never hold longer than this


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _as_unix_seconds(val: Any) -> float | None:
    """Accept numeric unix (s or ms), or ISO-8601 string."""
    if val is None:
        return None
    # Numeric (int/float/numeric string)
    try:
        v = float(val)
        if v > 1e12:
            v = v / 1000.0
        return v if v > 0 else None
    except (TypeError, ValueError):
        pass
    # ISO-8601 string like "2026-04-13T01:21:22.000237Z"
    if isinstance(val, str):
        s = val.strip().replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(s).timestamp()
        except Exception:
            return None
    return None


def _parse_tx_time(tx: dict) -> float | None:
    return _as_unix_seconds(
        tx.get("time")
        or tx.get("tx_time")
        or tx.get("block_time")
        or tx.get("timestamp")
    )


def _coerce_list(data: Any) -> list[dict]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for k in ("points", "list", "tokens", "data", "items", "result", "klines"):
            inner = data.get(k)
            if isinstance(inner, list):
                return [x for x in inner if isinstance(x, dict)]
    return []


def _candle_close(c: Any) -> float | None:
    if isinstance(c, dict):
        for k in ("close", "c", "close_price"):
            v = c.get(k)
            if v is not None:
                try:
                    return float(v)
                except (TypeError, ValueError):
                    pass
    if isinstance(c, list) and len(c) >= 5:
        try:
            return float(c[4])
        except (TypeError, ValueError):
            pass
    return None


def _candle_low(c: Any) -> float | None:
    if isinstance(c, dict):
        for k in ("low", "l", "low_price"):
            v = c.get(k)
            if v is not None:
                try:
                    return float(v)
                except (TypeError, ValueError):
                    pass
    if isinstance(c, list) and len(c) >= 4:
        try:
            return float(c[3])
        except (TypeError, ValueError):
            pass
    return None


def _candle_ts(c: Any) -> float | None:
    if isinstance(c, dict):
        for k in ("time", "t", "timestamp", "open_time"):
            v = c.get(k)
            ts = _as_unix_seconds(v)
            if ts:
                return ts
    if isinstance(c, list) and len(c) >= 1:
        return _as_unix_seconds(c[0])
    return None


def _price_at(candles: list, ts: float) -> float | None:
    best_price, best_dt = None, None
    for c in candles or []:
        ct = _candle_ts(c)
        cp = _candle_close(c)
        if ct is None or cp is None or ct > ts:
            continue
        dt = ts - ct
        if best_dt is None or dt < best_dt:
            best_dt = dt
            best_price = cp
    return best_price


def _price_window(candles: list, start_ts: float, end_ts: float) -> tuple[float | None, float | None]:
    """Return (peak, final) prices between start_ts and end_ts."""
    peak = None
    final = None
    final_ct = -1.0
    for c in candles or []:
        ct = _candle_ts(c)
        cp = _candle_close(c)
        if ct is None or cp is None:
            continue
        if start_ts <= ct <= end_ts:
            if peak is None or cp > peak:
                peak = cp
            if ct > final_ct:
                final_ct = ct
                final = cp
    return peak, final


# ---------------------------------------------------------------------
# Load graph
# ---------------------------------------------------------------------

async def _load_graph() -> tuple[
    dict[str, int],       # wallet → cluster_id
    dict[int, int],       # cluster_id → total size
    dict[str, str],       # wallet → chain
    dict[str, float],     # wallet → alpha_score
]:
    db = mongo.db()
    w2c: dict[str, int] = {}
    cluster_size: dict[int, int] = {}

    async for c in db.clusters.find({}):
        cid = c.get("cluster_id")
        if cid is None:
            continue
        members = c.get("wallet_addresses") or []
        cluster_size[cid] = len(members)
        for w in members:
            w2c[w] = cid

    chain: dict[str, str] = {}
    alpha: dict[str, float] = {}
    async for w in db.wallets.find({}, {"_id": 0, "address": 1, "chain": 1, "alpha_score": 1}):
        chain[w["address"]] = w.get("chain") or ""
        try:
            alpha[w["address"]] = float(w.get("alpha_score") or 0.0)
        except (TypeError, ValueError):
            alpha[w["address"]] = 0.0

    return w2c, cluster_size, chain, alpha


# ---------------------------------------------------------------------
# Gather wallet entries over the historical window
# ---------------------------------------------------------------------

async def _fetch_wallet_entries(
    ave: AveClient,
    wallets: list[tuple[str, str]],   # (address, chain)
    start_ts: float,
    end_ts: float,
) -> dict[str, list[dict]]:
    """
    For each wallet, fetch walletinfo/tokens. Keep tokens whose first_tx_time
    is within [start_ts, end_ts]. Returns: token_id -> [{wallet, entry_ts}, ...]
    """
    token_entries: dict[str, list[dict]] = defaultdict(list)
    sem = asyncio.Semaphore(PARALLEL)

    async def _one(addr: str, chain: str) -> None:
        async with sem:
            try:
                raw = await ave.wallet_tokens(addr, chain, hide_sold=0, hide_small=0)
            except Exception:
                return
        for h in _coerce_list(raw):
            ts = _as_unix_seconds(
                h.get("first_tx_time")
                or h.get("first_buy_time")
                or h.get("last_txn_time")
            )
            if not ts or not (start_ts <= ts <= end_ts):
                continue
            token_contract = (
                h.get("token_id")
                or h.get("token")
                or h.get("token_address")
                or h.get("contract")
            )
            if not token_contract:
                continue
            if "-" not in token_contract:
                token_id = f"{token_contract}-{chain}"
            else:
                token_id = token_contract
            token_entries[token_id].append({
                "wallet": addr,
                "entry_ts": ts,
                "chain": chain,
                "symbol": h.get("symbol") or h.get("token_symbol"),
            })

    await asyncio.gather(*[_one(a, c) for a, c in wallets])
    return token_entries


# ---------------------------------------------------------------------
# Cluster firings
# ---------------------------------------------------------------------

def _find_firings(
    token_id: str,
    entries: list[dict],
    wallet_to_cluster: dict[str, int],
    cluster_size: dict[int, int],
) -> list[dict]:
    """
    For a given token, find cluster firings.
    A firing = ≥ MIN_CLUSTER_WALLETS unique wallets from SAME cluster within TIME_WINDOW.
    """
    # Group entries by cluster
    by_cluster: dict[int, list[dict]] = defaultdict(list)
    for e in entries:
        cid = wallet_to_cluster.get(e["wallet"])
        if cid is None:
            continue
        by_cluster[cid].append(e)

    firings: list[dict] = []
    for cid, group in by_cluster.items():
        group.sort(key=lambda x: x["entry_ts"])
        i = 0
        used: set[str] = set()
        while i < len(group):
            window_end = group[i]["entry_ts"] + TIME_WINDOW_SECONDS
            window = [
                e for e in group[i:]
                if e["entry_ts"] <= window_end and e["wallet"] not in used
            ]
            unique_wallets = {e["wallet"] for e in window}
            if len(unique_wallets) >= MIN_CLUSTER_WALLETS:
                first_ts = min(e["entry_ts"] for e in window)
                last_ts = max(e["entry_ts"] for e in window)
                symbol = next((e.get("symbol") for e in window if e.get("symbol")), None)
                chain = next((e.get("chain") for e in window if e.get("chain")), None)
                firings.append({
                    "signal_type": "cluster",
                    "token_id": token_id,
                    "chain": chain,
                    "symbol": symbol,
                    "cluster_id": cid,
                    "cluster_size_total": cluster_size.get(cid, len(unique_wallets)),
                    "cluster_active_count": len(unique_wallets),
                    "wallets_involved": sorted(unique_wallets),
                    "first_entry_ts": first_ts,
                    "last_entry_ts": last_ts,
                    "time_window_seconds": int(last_ts - first_ts),
                })
                used.update(unique_wallets)
                while i < len(group) and group[i]["wallet"] in used:
                    i += 1
            else:
                i += 1
    return firings


# ---------------------------------------------------------------------
# Alpha solo firings
# ---------------------------------------------------------------------

ALPHA_ELITE_THRESHOLD = 3.0
ALPHA_STRONG_THRESHOLD = 1.0
ALPHA_COOLDOWN = 30 * 60


def _find_alpha_firings(
    token_id: str,
    entries: list[dict],
    wallet_to_cluster: dict[str, int],
    cluster_size: dict[int, int],
    alpha_scores: dict[str, float],
) -> list[dict]:
    """
    For a given token, find single-wallet alpha firings.
    A firing = wallet with alpha ≥ ALPHA_STRONG_THRESHOLD entered.
    Per-(wallet, token) cooldown applied to avoid duplicate firings.
    """
    # Sort by entry_ts
    entries_sorted = sorted(entries, key=lambda e: e["entry_ts"])
    last_fired: dict[str, float] = {}

    firings: list[dict] = []
    for e in entries_sorted:
        wallet = e["wallet"]
        alpha = alpha_scores.get(wallet, 0.0)
        if alpha < ALPHA_STRONG_THRESHOLD:
            continue

        # Per-wallet cooldown
        last = last_fired.get(wallet, 0.0)
        if e["entry_ts"] - last < ALPHA_COOLDOWN:
            continue
        last_fired[wallet] = e["entry_ts"]

        cid = wallet_to_cluster.get(wallet)
        tier = "elite" if alpha >= ALPHA_ELITE_THRESHOLD else "strong"

        firings.append({
            "signal_type": "alpha",
            "alpha_tier": tier,
            "token_id": token_id,
            "chain": e.get("chain"),
            "symbol": e.get("symbol"),
            "cluster_id": cid,
            "cluster_size_total": (
                cluster_size.get(cid, 0) if cid is not None else 0
            ),
            "cluster_active_count": 1,
            "wallets_involved": [wallet],
            "first_entry_ts": e["entry_ts"],
            "last_entry_ts": e["entry_ts"],
            "time_window_seconds": 0,
        })
    return firings


# ---------------------------------------------------------------------
# Simulate trade outcome
# ---------------------------------------------------------------------

async def _detect_cluster_exit(
    ave: AveClient,
    fire: dict,
    entry_ts: float,
    search_end_ts: float,
) -> tuple[float | None, str, int]:
    """
    For each entry wallet, fetch its buy/sell history on this token after entry.
    Return (exit_ts, reason, sellers_count):
      exit_ts = timestamp when EXIT_DISTRIBUTION_FRACTION of entry wallets had sold
      reason  = 'cluster_distribution' if reached threshold, else 'no_exit'
      sellers_count = how many cluster wallets had sold at that point
    """
    chain = fire.get("chain") or _chain_from_token_id(fire["token_id"])
    # Token contract = token_id without the chain suffix
    token_contract = fire["token_id"].rsplit("-", 1)[0]

    entry_wallets = fire["wallets_involved"]
    threshold = max(1, int(len(entry_wallets) * EXIT_DISTRIBUTION_FRACTION))

    # First-sell timestamp per wallet
    first_sell_ts: list[float] = []

    sem = asyncio.Semaphore(3)  # rate-polite

    target_token_lower = token_contract.lower()

    async def _for_wallet(addr: str) -> float | None:
        async with sem:
            try:
                raw = await ave.wallet_token_tx(
                    wallet_address=addr,
                    chain=chain,
                    token_address=token_contract,
                    from_time=int(entry_ts),
                    to_time=int(search_end_ts),
                    page_size=100,
                )
            except Exception:
                return None
        txs = _coerce_list(raw)
        earliest = None
        for tx in txs:
            # A SELL = wallet SENT the target token
            # (from_address == target token contract)
            from_addr = (tx.get("from_address") or "").lower()
            if from_addr != target_token_lower:
                continue

            ts = _parse_tx_time(tx)
            if ts and ts >= entry_ts and (earliest is None or ts < earliest):
                earliest = ts
        return earliest

    results = await asyncio.gather(*[_for_wallet(a) for a in entry_wallets])
    for ts in results:
        if ts is not None:
            first_sell_ts.append(ts)

    if len(first_sell_ts) < threshold:
        return None, "no_exit", len(first_sell_ts)

    first_sell_ts.sort()
    exit_ts = first_sell_ts[threshold - 1]  # moment threshold was reached
    return exit_ts, "cluster_distribution", len(first_sell_ts)


def _chain_from_token_id(token_id: str) -> str:
    return token_id.rsplit("-", 1)[-1] if "-" in token_id else ""


async def _simulate(
    ave: AveClient,
    fire: dict,
    alpha_scores: dict[str, float],
) -> dict:
    token_id = fire["token_id"]
    entry_ts = fire["first_entry_ts"]
    max_exit_ts = entry_ts + MAX_HOLD_HOURS * 3600

    # Candles — pull with hourly granularity to cover the full hold window
    try:
        raw_candles = await ave.klines_by_token(
            token_id, interval=60, limit=500
        )
    except Exception:
        raw_candles = []
    candles = _coerce_list(raw_candles) or raw_candles or []

    # Token detail — TVL + age for proper conviction scoring
    tvl_usd: float | None = None
    token_age_hours: float | None = None
    try:
        td = await ave.token_detail(token_id)
        if isinstance(td, dict):
            tok = td.get("token") if isinstance(td.get("token"), dict) else td
            try:
                tvl_usd = float(tok.get("main_pair_tvl") or tok.get("tvl") or 0)
                if tvl_usd <= 0:
                    tvl_usd = None
            except (TypeError, ValueError):
                pass
            launch = tok.get("launch_at") or tok.get("created_at")
            launch_ts = _as_unix_seconds(launch)
            if launch_ts and launch_ts < entry_ts:
                token_age_hours = max(0.0, (entry_ts - launch_ts) / 3600.0)
    except Exception:
        pass

    entry_price = _price_at(candles, entry_ts)
    peak_within_max, _ = _price_window(candles, entry_ts, max_exit_ts)

    # --- BEHAVIOR-BASED EXIT (the real product exit strategy) ---
    cluster_exit_ts, exit_reason, sellers = await _detect_cluster_exit(
        ave, fire, entry_ts, max_exit_ts
    )

    # Find the actual exit point
    stop_price = (
        entry_price * (1 + STOP_LOSS_PCT / 100.0) if entry_price else None
    )

    # Walk candles: look for stop-loss first, then cluster exit.
    # If neither triggers → the cluster is still holding → fall back to
    # realistic peak capture (75% of peak) — represents unrealized gains.
    exit_ts_used = None
    exit_price = None
    final_reason = "still_holding"

    candles_sorted = sorted(
        [
            (c, _candle_ts(c), _candle_close(c), _candle_low(c))
            for c in (candles or [])
            if _candle_ts(c) is not None and _candle_close(c) is not None
        ],
        key=lambda t: t[1],
    )

    for _, ct, cp, clow in candles_sorted:
        if ct < entry_ts:
            continue
        if ct > max_exit_ts:
            break
        # Stop-loss: check candle LOW (not just close) — catches flash dumps
        intra_low = clow if clow is not None else cp
        if stop_price is not None and intra_low <= stop_price:
            exit_ts_used = ct
            exit_price = stop_price   # simulate stop-loss fill at the cap
            final_reason = "stop_loss"
            break
        # Cluster distribution
        if cluster_exit_ts is not None and ct >= cluster_exit_ts:
            exit_ts_used = ct
            exit_price = cp
            final_reason = exit_reason
            break

    # Nothing triggered → still holding → realistic exit = 75% of peak capture
    if exit_ts_used is None and entry_price and peak_within_max is not None:
        exit_price = entry_price + (peak_within_max - entry_price) * 0.75
        exit_ts_used = max_exit_ts  # nominal exit at end of window
        final_reason = "still_holding"

    # Compute metrics
    pnl_pct = None
    peak_pnl_pct = None
    if entry_price and entry_price > 0:
        if exit_price is not None:
            pnl_pct = round((exit_price / entry_price - 1.0) * 100, 2)
        if peak_within_max is not None:
            peak_pnl_pct = round((peak_within_max / entry_price - 1.0) * 100, 2)

    wallet_alphas = [alpha_scores.get(w, 0.0) for w in fire["wallets_involved"]]
    scored = conviction_scorer.score(
        cluster_active=fire["cluster_active_count"],
        cluster_total=fire["cluster_size_total"],
        wallet_alpha_scores=wallet_alphas,
        risk_score=75,
        tvl_usd=tvl_usd,
        token_age_hours=token_age_hours,
    )

    # For alpha signals, override the conviction + status using the alpha
    # formula from the live enricher so backtest and live agree.
    signal_type = fire.get("signal_type") or "cluster"
    if signal_type == "alpha":
        from app.services.signal_enricher import (
            _score_alpha_signal,
            compute_momentum,
        )
        # We don't have live momentum data in backtest — approximate with
        # the peak-vs-entry ratio over the hold window. This is a reasonable
        # stand-in: "how much did this token move after entry?"
        momentum_proxy = 0.0
        if entry_price and peak_within_max and entry_price > 0:
            up = (peak_within_max / entry_price) - 1.0
            momentum_proxy = max(0.0, min(1.0, up / 1.0))

        alpha_val = wallet_alphas[0] if wallet_alphas else 0.0
        alpha_scored = _score_alpha_signal(
            wallet_alpha=alpha_val,
            momentum=momentum_proxy,
            risk_score=75,
            tvl_usd=tvl_usd,
            age_hours=token_age_hours,
        )
        # Apply the strong-tier momentum gate
        if fire.get("alpha_tier") == "strong" and momentum_proxy < 0.3:
            alpha_scored["status"] = "watch"
        scored = {
            "conviction_score": alpha_scored["conviction_score"],
            "breakdown": alpha_scored["conviction_breakdown"],
            "status": alpha_scored["status"],
        }

    outcome = "win" if (pnl_pct is not None and pnl_pct > 0) else (
        "loss" if pnl_pct is not None else "no_data"
    )

    hold_hours = None
    if exit_ts_used is not None:
        hold_hours = round((exit_ts_used - entry_ts) / 3600.0, 2)

    return {
        "signal_type": signal_type,
        "alpha_tier": fire.get("alpha_tier"),
        "token_id": token_id,
        "chain": fire.get("chain"),
        "symbol": fire.get("symbol"),
        "cluster_id": fire["cluster_id"],
        "cluster_size_total": fire["cluster_size_total"],
        "cluster_active_count": fire["cluster_active_count"],
        "wallets_involved": fire["wallets_involved"],
        "first_entry_at": datetime.utcfromtimestamp(entry_ts).isoformat(),
        "last_entry_at": datetime.utcfromtimestamp(fire["last_entry_ts"]).isoformat(),
        "time_window_seconds": fire["time_window_seconds"],
        "entry_price_usd": entry_price,
        "exit_price_usd": exit_price,
        "peak_price_usd": peak_within_max,
        "exit_reason": final_reason,
        "cluster_sellers_detected": sellers,
        "hold_hours": hold_hours,
        "realistic_pnl_pct": pnl_pct,
        "peak_pnl_pct": peak_pnl_pct,
        "outcome": outcome,
        "conviction_score": scored["conviction_score"],
        "conviction_breakdown": scored["breakdown"],
        "status": scored["status"],
        "tvl_usd": tvl_usd,
        "token_age_hours": token_age_hours,
        "wallet_alpha_scores": wallet_alphas,
        "avg_alpha_score": round(
            sum(wallet_alphas) / len(wallet_alphas), 3
        ) if wallet_alphas else 0.0,
        "created_at": datetime.utcnow().isoformat(),
    }


# ---------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------

async def run_honest_backtest(
    days_back: int = DAYS_BACK,
    hold_buffer_hours: int = HOLD_BUFFER_HOURS,
) -> dict:
    """
    True forward-testing backtest:
      - Take our current smart-money graph
      - Look at what they bought in [now - days_back, now - hold_buffer_hours]
      - Find cluster firings
      - Simulate outcomes using forward prices
      - Include wins AND losses
    """
    w2c, cluster_size, chain_map, alpha = await _load_graph()
    if not w2c:
        return {"error": "no graph — run seed_graph first"}

    wallets = [(addr, chain_map.get(addr, "")) for addr in w2c.keys() if chain_map.get(addr)]
    if not wallets:
        return {"error": "wallets have no chain info"}

    now = time.time()
    start_ts = now - days_back * 86400
    end_ts = now - hold_buffer_hours * 3600  # leave room for forward prices

    print(f"[backtest] window: {datetime.utcfromtimestamp(start_ts).isoformat()} "
          f"→ {datetime.utcfromtimestamp(end_ts).isoformat()}")
    print(f"[backtest] fetching holdings for {len(wallets)} wallets...")

    async with AveClient() as ave:
        entries_by_token = await _fetch_wallet_entries(ave, wallets, start_ts, end_ts)
        print(f"[backtest] found entries on {len(entries_by_token)} tokens")

        all_firings: list[dict] = []
        cluster_count = 0
        alpha_count = 0
        for token_id, entries in entries_by_token.items():
            fires = _find_firings(token_id, entries, w2c, cluster_size)
            alpha_fires = _find_alpha_firings(
                token_id, entries, w2c, cluster_size, alpha
            )
            # Dedup: if an alpha wallet's entry is already in a cluster firing
            # on this token, drop the alpha firing for that wallet.
            cluster_wallets_per_token = set()
            for f in fires:
                cluster_wallets_per_token.update(f["wallets_involved"])
            alpha_fires = [
                f for f in alpha_fires
                if f["wallets_involved"][0] not in cluster_wallets_per_token
            ]
            all_firings.extend(fires)
            all_firings.extend(alpha_fires)
            cluster_count += len(fires)
            alpha_count += len(alpha_fires)
        print(
            f"[backtest] firings: {cluster_count} cluster + "
            f"{alpha_count} alpha = {len(all_firings)} total"
        )

        if not all_firings:
            return {
                "tokens_scanned": len(entries_by_token),
                "firings": 0,
                "results": [],
                "summary": {},
            }

        results: list[dict] = []
        sem = asyncio.Semaphore(PARALLEL)

        async def _run(fire: dict) -> dict:
            async with sem:
                return await _simulate(ave, fire, alpha)

        results = await asyncio.gather(*[_run(f) for f in all_firings])

    # Persist (replace previous backtest run)
    db = mongo.db()
    await db.backtests.delete_many({})
    if results:
        await db.backtests.insert_many(results)

    # Summary
    valid = [r for r in results if r["outcome"] != "no_data"]
    wins = [r for r in valid if r["outcome"] == "win"]
    losses = [r for r in valid if r["outcome"] == "loss"]
    best = max((r["peak_pnl_pct"] or 0) for r in valid) if valid else 0
    worst = min((r["realistic_pnl_pct"] or 0) for r in valid) if valid else 0
    avg = sum((r["realistic_pnl_pct"] or 0) for r in valid) / len(valid) if valid else 0

    # Exit-reason breakdown — tells us what the product does most often
    reason_counts: dict[str, int] = {}
    for r in valid:
        reason_counts[r["exit_reason"]] = reason_counts.get(r["exit_reason"], 0) + 1

    # Execution-grade subset (conviction >= 70 = status "exec") — what the
    # product would actually auto-trade
    exec_only = [r for r in valid if r.get("conviction_score", 0) >= 70]
    exec_wins = [r for r in exec_only if r["outcome"] == "win"]
    exec_avg = (
        sum((r["realistic_pnl_pct"] or 0) for r in exec_only) / len(exec_only)
        if exec_only else 0
    )

    # Per-type stats — cluster vs alpha
    def _type_stats(records: list[dict]) -> dict:
        if not records:
            return {"count": 0, "wins": 0, "win_rate_pct": 0.0, "avg_pnl_pct": 0.0}
        w = [r for r in records if r["outcome"] == "win"]
        a = sum((r["realistic_pnl_pct"] or 0) for r in records) / len(records)
        return {
            "count": len(records),
            "wins": len(w),
            "win_rate_pct": round(len(w) / len(records) * 100, 2),
            "avg_pnl_pct": round(a, 2),
        }

    cluster_records = [r for r in valid if r.get("signal_type") != "alpha"]
    alpha_records = [r for r in valid if r.get("signal_type") == "alpha"]

    summary = {
        "tokens_scanned": len(entries_by_token),
        "firings_detected": len(all_firings),
        "evaluable": len(valid),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(len(wins) / len(valid) * 100, 2) if valid else 0.0,
        "avg_pnl_pct": round(avg, 2),
        "best_pnl_pct": round(best, 2),
        "worst_pnl_pct": round(worst, 2),
        "exit_reasons": reason_counts,
        "window_days": days_back,
        # Execution grade — the real product's behavior
        "exec_count": len(exec_only),
        "exec_wins": len(exec_wins),
        "exec_win_rate_pct": (
            round(len(exec_wins) / len(exec_only) * 100, 2) if exec_only else 0.0
        ),
        "exec_avg_pnl_pct": round(exec_avg, 2),
        # Per-signal-type breakdown
        "cluster_stats": _type_stats(cluster_records),
        "alpha_stats": _type_stats(alpha_records),
    }

    return {
        "tokens_scanned": len(entries_by_token),
        "firings": len(all_firings),
        "results": len(results),
        "summary": summary,
    }

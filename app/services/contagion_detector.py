"""
Contagion detector — dual-mode.

MODE 1: Cluster signal
  2+ wallets from the same cluster enter a token inside a time window.

MODE 2: Alpha signal
  A single high-alpha wallet (score >= ALPHA_SOLO_THRESHOLD) enters a token,
  regardless of cluster membership. The enricher decides whether the token's
  momentum justifies promoting this to an executable signal.

Both write to the same `signals` collection, tagged with signal_type.
"""

from __future__ import annotations

import time
from collections import defaultdict
from datetime import datetime

from app.db import mongo
from app.models.signal import ContagionSignal
from app.services.event_bus import SIGNAL_FIRED, SWAP_EVENT, bus

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

TIME_WINDOW_SECONDS = 60 * 60
MIN_CLUSTER_WALLETS_FOR_SIGNAL = 2
SIGNAL_COOLDOWN_SECONDS = 15 * 60

# Tiered alpha quality gates
ALPHA_ELITE_THRESHOLD = 3.0    # always fires (truly top wallets)
ALPHA_STRONG_THRESHOLD = 1.0   # fires; enricher requires momentum to promote
ALPHA_SOLO_COOLDOWN = 30 * 60  # dedup per (wallet, token)

# Drop wallets that haven't traded recently — alpha ages out
WALLET_RECENCY_DAYS = 30


# ---------------------------------------------------------------------
# In-memory indexes
# ---------------------------------------------------------------------

_wallet_to_cluster: dict[str, int] = {}
_cluster_members: dict[int, set[str]] = {}
_wallet_alpha: dict[str, float] = {}
_wallet_last_trade_ts: dict[str, float] = {}

_recent_entries: dict[tuple[str, int], list[tuple[str, float]]] = defaultdict(list)
_last_fired: dict[tuple[str, int], float] = {}
_alpha_last_fired: dict[tuple[str, str], float] = {}  # (wallet, token_id)


def _is_recent(wallet: str) -> bool:
    """True if wallet has no recency data OR traded within WALLET_RECENCY_DAYS."""
    ts = _wallet_last_trade_ts.get(wallet)
    if ts is None or ts <= 0:
        # No recency info available → don't block (fail-open).
        return True
    return (time.time() - ts) <= WALLET_RECENCY_DAYS * 86400


async def load_graph_index() -> dict:
    global _wallet_to_cluster, _cluster_members, _wallet_alpha
    global _wallet_last_trade_ts
    _wallet_to_cluster = {}
    _cluster_members = {}
    _wallet_alpha = {}
    _wallet_last_trade_ts = {}

    db = mongo.db()
    async for cluster in db.clusters.find({}):
        cid = cluster.get("cluster_id")
        wallets = cluster.get("wallet_addresses", [])
        if cid is None or not wallets:
            continue
        _cluster_members[cid] = set(wallets)
        for w in wallets:
            _wallet_to_cluster[w] = cid

    async for w in db.wallets.find(
        {},
        {"_id": 0, "address": 1, "alpha_score": 1, "last_trade_time": 1},
    ):
        addr = w.get("address")
        if not addr:
            continue
        try:
            _wallet_alpha[addr] = float(w.get("alpha_score") or 0)
        except (TypeError, ValueError):
            _wallet_alpha[addr] = 0.0
        try:
            lt = w.get("last_trade_time")
            if lt is not None:
                _wallet_last_trade_ts[addr] = float(lt)
        except (TypeError, ValueError):
            pass

    return {
        "clusters_loaded": len(_cluster_members),
        "wallets_indexed": len(_wallet_to_cluster),
        "alpha_scores_indexed": len(_wallet_alpha),
        "wallets_with_recency": len(_wallet_last_trade_ts),
    }


# ---------------------------------------------------------------------
# Event handler
# ---------------------------------------------------------------------

async def on_swap(event: dict) -> None:
    wallet = event.get("wallet")
    token_id = event.get("token_id")
    side = event.get("side", "buy")
    if not wallet or not token_id or side != "buy":
        return

    ts = float(event.get("timestamp") or time.time())
    alpha = _wallet_alpha.get(wallet)

    # Wallet is in the graph at all?
    if alpha is None:
        return

    # --- MODE 1: Cluster signal ---
    cluster_id = _wallet_to_cluster.get(wallet)
    fired_cluster = False
    if cluster_id is not None:
        fired_cluster = await _try_fire_cluster(
            wallet=wallet,
            token_id=token_id,
            cluster_id=cluster_id,
            ts=ts,
            event=event,
        )

    # --- MODE 2: Alpha solo signal (tiered) ---
    # Elite wallets (α ≥ 3.0) fire unconditionally.
    # Strong wallets (α ≥ 1.0) fire too, but the enricher requires token
    # momentum ≥ 0.3 to promote beyond "watch". See signal_enricher.
    # Sub-strong wallets (α < 1.0) never fire solo — cluster only.
    if (
        not fired_cluster
        and alpha >= ALPHA_STRONG_THRESHOLD
        and _is_recent(wallet)
    ):
        tier = "elite" if alpha >= ALPHA_ELITE_THRESHOLD else "strong"
        await _try_fire_alpha(
            wallet=wallet,
            alpha=alpha,
            tier=tier,
            token_id=token_id,
            cluster_id=cluster_id,
            ts=ts,
            event=event,
        )


async def _try_fire_cluster(
    *,
    wallet: str,
    token_id: str,
    cluster_id: int,
    ts: float,
    event: dict,
) -> bool:
    key = (token_id, cluster_id)

    entries = _recent_entries[key]
    entries.append((wallet, ts))
    cutoff = ts - TIME_WINDOW_SECONDS
    entries = [(w, t) for w, t in entries if t >= cutoff]
    _recent_entries[key] = entries

    unique_wallets = {w for w, _ in entries}
    if len(unique_wallets) < MIN_CLUSTER_WALLETS_FOR_SIGNAL:
        return False

    last = _last_fired.get(key, 0.0)
    if ts - last < SIGNAL_COOLDOWN_SECONDS:
        return False
    _last_fired[key] = ts

    cluster_total = len(_cluster_members.get(cluster_id, set()))
    first_ts = min(t for _, t in entries)
    last_ts = max(t for _, t in entries)

    alpha_scores = [
        _wallet_alpha.get(w, 0.0) for w in sorted(unique_wallets)
    ]
    avg_alpha = (
        sum(alpha_scores) / len(alpha_scores) if alpha_scores else 0.0
    )

    signal = ContagionSignal(
        signal_type="cluster",
        token_id=token_id,
        chain=event.get("chain") or _chain_of(token_id),
        symbol=event.get("symbol"),
        cluster_id=cluster_id,
        wallets_involved=sorted(unique_wallets),
        cluster_size_total=cluster_total,
        cluster_active_count=len(unique_wallets),
        wallet_alpha_scores=alpha_scores,
        avg_alpha_score=round(avg_alpha, 3),
        time_window_seconds=int(last_ts - first_ts),
        first_entry_at=datetime.utcfromtimestamp(first_ts),
        last_entry_at=datetime.utcfromtimestamp(last_ts),
    )

    payload = signal.model_dump(mode="json")
    db = mongo.db()
    result = await db.signals.insert_one(payload)
    payload["id"] = str(result.inserted_id)
    payload.pop("_id", None)

    await bus.publish(SIGNAL_FIRED, payload)

    print(
        f"[CONTAGION cluster] #{cluster_id} on {signal.symbol or token_id} "
        f"({len(unique_wallets)}/{cluster_total} wallets, α={avg_alpha:.2f})"
    )
    return True


async def _try_fire_alpha(
    *,
    wallet: str,
    alpha: float,
    tier: str,  # "elite" or "strong"
    token_id: str,
    cluster_id: int | None,
    ts: float,
    event: dict,
) -> bool:
    key = (wallet, token_id)
    last = _alpha_last_fired.get(key, 0.0)
    if ts - last < ALPHA_SOLO_COOLDOWN:
        return False
    _alpha_last_fired[key] = ts

    signal = ContagionSignal(
        signal_type="alpha",
        token_id=token_id,
        chain=event.get("chain") or _chain_of(token_id),
        symbol=event.get("symbol"),
        cluster_id=cluster_id,
        wallets_involved=[wallet],
        cluster_size_total=len(_cluster_members.get(cluster_id, set()))
        if cluster_id is not None
        else 0,
        cluster_active_count=1,
        wallet_alpha_scores=[alpha],
        avg_alpha_score=round(alpha, 3),
        time_window_seconds=0,
        first_entry_at=datetime.utcfromtimestamp(ts),
        last_entry_at=datetime.utcfromtimestamp(ts),
    )

    payload = signal.model_dump(mode="json")
    payload["alpha_tier"] = tier  # enricher + frontend read this

    db = mongo.db()
    result = await db.signals.insert_one(payload)
    payload["id"] = str(result.inserted_id)
    payload.pop("_id", None)

    await bus.publish(SIGNAL_FIRED, payload)

    print(
        f"[CONTAGION alpha/{tier}] {wallet[:10]}… α={alpha:.2f} on "
        f"{signal.symbol or token_id}"
    )
    return True


async def _persist(signal: ContagionSignal) -> None:
    db = mongo.db()
    await db.signals.insert_one(signal.model_dump(mode="json"))


def _chain_of(token_id: str) -> str:
    return token_id.rsplit("-", 1)[-1] if "-" in token_id else "unknown"


# ---------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------

def register() -> None:
    bus.subscribe(SWAP_EVENT, on_swap)

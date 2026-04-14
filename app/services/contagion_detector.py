"""
Contagion detector.

Subscribes to swap events from the event bus. For each incoming swap:
  1. Is the buyer's wallet in our graph?
  2. Which cluster do they belong to?
  3. Have OTHER wallets from the same cluster bought this token
     within the configured time window?
  4. If yes: fire a ContagionSignal.

State is kept in-memory and indexed for O(1) lookups.
It's rebuilt from MongoDB on startup via `load_graph_index()`.
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

TIME_WINDOW_SECONDS = 60 * 60            # 60 minutes
MIN_CLUSTER_WALLETS_FOR_SIGNAL = 2       # at least N wallets from the cluster in the window
SIGNAL_COOLDOWN_SECONDS = 15 * 60        # don't re-fire same (token, cluster) within 15 min


# ---------------------------------------------------------------------
# In-memory indexes (populated from Mongo)
# ---------------------------------------------------------------------

# wallet_address -> cluster_id
_wallet_to_cluster: dict[str, int] = {}

# cluster_id -> set of wallet addresses
_cluster_members: dict[int, set[str]] = {}

# (token_id, cluster_id) -> list of (wallet, timestamp)
_recent_entries: dict[tuple[str, int], list[tuple[str, float]]] = defaultdict(list)

# (token_id, cluster_id) -> last time we fired a signal (for cooldown)
_last_fired: dict[tuple[str, int], float] = {}


async def load_graph_index() -> dict:
    """Load clusters from Mongo into in-memory indexes."""
    global _wallet_to_cluster, _cluster_members
    _wallet_to_cluster = {}
    _cluster_members = {}

    db = mongo.db()
    async for cluster in db.clusters.find({}):
        cid = cluster.get("cluster_id")
        wallets = cluster.get("wallet_addresses", [])
        if cid is None or not wallets:
            continue
        _cluster_members[cid] = set(wallets)
        for w in wallets:
            _wallet_to_cluster[w] = cid

    return {
        "clusters_loaded": len(_cluster_members),
        "wallets_indexed": len(_wallet_to_cluster),
    }


# ---------------------------------------------------------------------
# Event handler
# ---------------------------------------------------------------------

async def on_swap(event: dict) -> None:
    """
    Handle an incoming swap event.
    Expected payload:
      {
        "wallet": "0x...",
        "token_id": "contract-chain",
        "chain": "bsc",
        "symbol": "WIF",
        "timestamp": 1712345678   # unix epoch seconds
        "side": "buy"             # "buy" | "sell"
      }
    """
    wallet = event.get("wallet")
    token_id = event.get("token_id")
    side = event.get("side", "buy")
    if not wallet or not token_id or side != "buy":
        return

    cluster_id = _wallet_to_cluster.get(wallet)
    if cluster_id is None:
        return  # wallet not in our graph — ignore

    ts = float(event.get("timestamp") or time.time())
    key = (token_id, cluster_id)

    # Append and prune by time window
    entries = _recent_entries[key]
    entries.append((wallet, ts))
    cutoff = ts - TIME_WINDOW_SECONDS
    entries = [(w, t) for w, t in entries if t >= cutoff]
    _recent_entries[key] = entries

    # How many UNIQUE cluster wallets entered in the window?
    unique_wallets = {w for w, _ in entries}
    if len(unique_wallets) < MIN_CLUSTER_WALLETS_FOR_SIGNAL:
        return

    # Cooldown check
    last = _last_fired.get(key, 0.0)
    if ts - last < SIGNAL_COOLDOWN_SECONDS:
        return
    _last_fired[key] = ts

    cluster_total = len(_cluster_members.get(cluster_id, set()))
    first_ts = min(t for _, t in entries)
    last_ts = max(t for _, t in entries)

    signal = ContagionSignal(
        token_id=token_id,
        chain=event.get("chain") or _chain_of(token_id),
        symbol=event.get("symbol"),
        cluster_id=cluster_id,
        wallets_involved=sorted(unique_wallets),
        cluster_size_total=cluster_total,
        cluster_active_count=len(unique_wallets),
        time_window_seconds=int(last_ts - first_ts),
        first_entry_at=datetime.utcfromtimestamp(first_ts),
        last_entry_at=datetime.utcfromtimestamp(last_ts),
    )

    await _persist(signal)
    await bus.publish(SIGNAL_FIRED, signal.model_dump(mode="json"))

    print(
        f"[CONTAGION] cluster #{cluster_id} on {signal.symbol or token_id} "
        f"({len(unique_wallets)}/{cluster_total} wallets, "
        f"window={signal.time_window_seconds}s)"
    )


async def _persist(signal: ContagionSignal) -> None:
    db = mongo.db()
    await db.signals.insert_one(signal.model_dump(mode="json"))


def _chain_of(token_id: str) -> str:
    return token_id.rsplit("-", 1)[-1] if "-" in token_id else "unknown"


# ---------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------

def register() -> None:
    """Call once on startup to wire the handler into the bus."""
    bus.subscribe(SWAP_EVENT, on_swap)

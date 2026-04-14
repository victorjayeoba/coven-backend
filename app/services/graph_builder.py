"""
Wallet graph builder (v2) — AVE-native edition.

Uses AVE's curated smart money ranking instead of inferring smartness from
/top100 holders. Much cleaner, less noisy, and AVE verifies the profit.

Pipeline:
  1. /v2/address/smart_wallet/list          -> ranked smart wallets per chain
  2. /v2/address/walletinfo/tokens          -> current holdings per wallet
  3. Flip data: token -> set of smart wallets holding it
  4. Wallets sharing 2+ tokens -> connected (Union-Find)
  5. Save wallets, edges, clusters to MongoDB
"""

from __future__ import annotations

import asyncio
from collections import Counter, defaultdict
from datetime import datetime
from itertools import combinations

from pymongo import UpdateOne

from app.db import mongo
from app.services.ave_client import AveClient

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

CHAINS = ["solana", "bsc"]
SMART_WALLETS_PER_CHAIN = 200       # take top N by profit rate per chain
MIN_APPEARANCES = 2                 # wallet must hold 2+ tokens to be interesting
MIN_SHARED_TOKENS = 2               # pairs need 2+ shared tokens to be connected
PARALLEL_REQUESTS = 10              # concurrent /walletinfo/tokens calls
HIDE_SOLD = 1                       # skip tokens the wallet already exited
HIDE_SMALL = 1                      # skip dust holdings


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _coerce_list(data) -> list[dict]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for key in ("list", "tokens", "data", "items", "result", "wallets"):
            inner = data.get(key)
            if isinstance(inner, list):
                return [x for x in inner if isinstance(x, dict)]
    return []


def _wallet_addr(item: dict) -> str | None:
    return (
        item.get("wallet_address")
        or item.get("address")
        or item.get("wallet")
    )


def _token_id(item: dict, chain: str) -> str | None:
    tid = item.get("token_id") or item.get("id")
    if tid:
        return tid
    contract = (
        item.get("token")
        or item.get("token_address")
        or item.get("contract")
        or item.get("address")
    )
    if contract:
        return f"{contract}-{chain}"
    return None


# ---------------------------------------------------------------------
# Union-Find
# ---------------------------------------------------------------------

class UnionFind:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        root = self.parent.setdefault(x, x)
        while root != self.parent[root]:
            self.parent[root] = self.parent[self.parent[root]]
            root = self.parent[root]
        self.parent[x] = root
        return root

    def union(self, x: str, y: str) -> None:
        rx, ry = self.find(x), self.find(y)
        if rx != ry:
            self.parent[rx] = ry


# ---------------------------------------------------------------------
# Phase 1 — fetch smart wallets
# ---------------------------------------------------------------------

async def fetch_smart_wallets(
    ave: AveClient,
    chain: str,
    limit: int,
) -> list[dict]:
    """Return up to `limit` smart wallets for a chain, sorted by profit rate."""
    try:
        raw = await ave.smart_wallet_list(
            chain=chain, sort="total_profit_rate", sort_dir="desc"
        )
    except Exception as e:
        print(f"[graph] smart_wallet_list failed for {chain}: {e}")
        return []
    items = _coerce_list(raw)
    return items[:limit]


# ---------------------------------------------------------------------
# Phase 2 — fetch holdings per wallet
# ---------------------------------------------------------------------

async def fetch_holdings_for_wallets(
    ave: AveClient,
    wallets: list[tuple[str, str, dict]],
) -> dict[str, list[dict]]:
    """
    For each (wallet, chain, meta) fetch current holdings.
    Returns: { wallet_address: [holding_dict, ...] }
    """
    holdings: dict[str, list[dict]] = {}
    sem = asyncio.Semaphore(PARALLEL_REQUESTS)

    async def _one(addr: str, chain: str) -> None:
        async with sem:
            try:
                raw = await ave.wallet_tokens(
                    addr,
                    chain,
                    hide_sold=HIDE_SOLD,
                    hide_small=HIDE_SMALL,
                )
                holdings[addr] = _coerce_list(raw)
            except Exception:
                holdings[addr] = []

    await asyncio.gather(*[_one(a, c) for a, c, _ in wallets])
    return holdings


# ---------------------------------------------------------------------
# Phase 3 — build wallet -> tokens map
# ---------------------------------------------------------------------

def build_wallet_map(
    wallets: list[tuple[str, str, dict]],
    holdings: dict[str, list[dict]],
) -> tuple[dict[str, list[dict]], dict[str, dict]]:
    """
    Return:
      wallet_map:   wallet_address -> [token appearances]
      wallet_meta:  wallet_address -> {chain, alpha_score, raw}
    """
    wallet_map: dict[str, list[dict]] = defaultdict(list)
    wallet_meta: dict[str, dict] = {}

    for addr, chain, meta in wallets:
        profit_rate = (
            meta.get("total_profit_rate")
            or meta.get("profit_rate")
            or 0.0
        )
        try:
            alpha = float(profit_rate)
        except (TypeError, ValueError):
            alpha = 0.0

        wallet_meta[addr] = {
            "chain": chain,
            "alpha_score": alpha,
            "total_profit": meta.get("total_profit"),
            "total_trades": meta.get("total_trades"),
            "total_volume": meta.get("total_volume"),
            "last_trade_time": meta.get("last_trade_time"),
        }

        for h in holdings.get(addr, []):
            tid = _token_id(h, chain)
            if not tid:
                continue
            wallet_map[addr].append({
                "token_id": tid,
                "symbol": h.get("symbol") or h.get("token_symbol"),
                "chain": chain,
                "balance_usd": h.get("balance_usd"),
                "balance_amount": h.get("balance_amount"),
                "total_profit": h.get("total_profit"),
                "unrealized_profit": h.get("unrealized_profit"),
                "last_txn_time": h.get("last_txn_time"),
            })

    return wallet_map, wallet_meta


# ---------------------------------------------------------------------
# Phase 4 — cluster via shared holdings
# ---------------------------------------------------------------------

def compute_pair_shared_counts(
    wallet_map: dict[str, list[dict]],
) -> Counter:
    """
    Inverted-index approach: for every token, enumerate wallet pairs holding it.
    """
    token_to_wallets: dict[str, list[str]] = defaultdict(list)
    for wallet, appearances in wallet_map.items():
        if len(appearances) < MIN_APPEARANCES:
            continue
        for a in appearances:
            token_to_wallets[a["token_id"]].append(wallet)

    pair_counts: Counter = Counter()
    for _, wallets in token_to_wallets.items():
        for a, b in combinations(sorted(set(wallets)), 2):
            pair_counts[(a, b)] += 1

    return pair_counts


def cluster_wallets(
    pair_counts: Counter,
) -> tuple[list[set[str]], list[dict]]:
    """Union-Find over connected pairs -> clusters + edges."""
    uf = UnionFind()
    edges: list[dict] = []

    for (a, b), count in pair_counts.items():
        if count < MIN_SHARED_TOKENS:
            continue
        uf.union(a, b)
        edges.append({
            "wallet_a": a,
            "wallet_b": b,
            "shared_count": count,
        })

    groups: dict[str, set[str]] = defaultdict(set)
    for node in list(uf.parent.keys()):
        groups[uf.find(node)].add(node)

    clusters = [g for g in groups.values() if len(g) >= 2]
    clusters.sort(key=len, reverse=True)
    return clusters, edges


# ---------------------------------------------------------------------
# Phase 5 — persist
# ---------------------------------------------------------------------

async def save_graph(
    wallet_map: dict[str, list[dict]],
    wallet_meta: dict[str, dict],
    edges: list[dict],
    clusters: list[set[str]],
) -> dict:
    db = mongo.db()
    now = datetime.utcnow()

    # --- wallets ---
    wallet_ops: list[UpdateOne] = []
    for address, appearances in wallet_map.items():
        meta = wallet_meta.get(address, {})
        wallet_ops.append(UpdateOne(
            {"address": address},
            {
                "$set": {
                    "address": address,
                    "chain": meta.get("chain"),
                    "alpha_score": meta.get("alpha_score", 0.0),
                    "total_profit": meta.get("total_profit"),
                    "total_trades": meta.get("total_trades"),
                    "tokens": appearances,
                    "last_updated": now,
                },
                "$setOnInsert": {"first_seen": now},
            },
            upsert=True,
        ))
    if wallet_ops:
        await db.wallets.bulk_write(wallet_ops)

    # --- clusters (replace) ---
    await db.clusters.delete_many({})
    cluster_docs = []
    for idx, group in enumerate(clusters, start=1):
        chains = {wallet_meta.get(w, {}).get("chain") for w in group}
        chains.discard(None)
        cluster_docs.append({
            "cluster_id": idx,
            "wallet_addresses": sorted(group),
            "size": len(group),
            "chain": ",".join(sorted(chains)) or "unknown",
            "created_at": now,
            "last_updated": now,
        })
    if cluster_docs:
        await db.clusters.insert_many(cluster_docs)

    # --- edges (replace) ---
    await db.wallet_edges.delete_many({})
    if edges:
        await db.wallet_edges.insert_many(edges)

    return {
        "wallets_upserted": len(wallet_ops),
        "clusters": len(cluster_docs),
        "edges": len(edges),
    }


# ---------------------------------------------------------------------
# Top-level entry
# ---------------------------------------------------------------------

async def build_and_save(
    chains: list[str] = None,
    wallets_per_chain: int = SMART_WALLETS_PER_CHAIN,
) -> dict:
    chains = chains or CHAINS

    async with AveClient() as ave:
        # (addr, chain, meta)
        wallets: list[tuple[str, str, dict]] = []
        for chain in chains:
            smart = await fetch_smart_wallets(ave, chain, wallets_per_chain)
            print(f"[graph] {chain}: {len(smart)} smart wallets")
            for item in smart:
                addr = _wallet_addr(item)
                if addr:
                    wallets.append((addr, chain, item))

        if not wallets:
            return {"error": "no smart wallets fetched"}

        print(f"[graph] fetching holdings for {len(wallets)} wallets "
              f"(parallelism={PARALLEL_REQUESTS})")
        holdings = await fetch_holdings_for_wallets(ave, wallets)

    wallet_map, wallet_meta = build_wallet_map(wallets, holdings)
    pair_counts = compute_pair_shared_counts(wallet_map)
    clusters, edges = cluster_wallets(pair_counts)

    stats = await save_graph(wallet_map, wallet_meta, edges, clusters)
    stats.update({
        "smart_wallets_fetched": len(wallets),
        "wallets_with_holdings": len([w for w in wallet_map if wallet_map[w]]),
        "connected_pairs": len(edges),
        "largest_cluster": len(clusters[0]) if clusters else 0,
    })
    return stats

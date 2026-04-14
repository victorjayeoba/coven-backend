"""
Phase 0 — Thesis Validation Script

Goal: Prove that wallet clusters exist in AVE data before building anything else.

What it does:
  1. Pulls top gainer tokens from AVE's ranks endpoint
  2. Fetches top 100 holders for each
  3. Cross-references wallet addresses across tokens
  4. Prints wallets that appear in multiple winners (the clusters)
  5. Prints connection strengths (which wallets co-appear most)

If this script finds strong clusters → go build the full project.
If it finds only random wallets → the approach needs adjusting.

Run:
  python validate_ave.py
"""

import os
import sys
import time
import json
from collections import defaultdict, Counter
from itertools import combinations

import requests
from dotenv import load_dotenv

# Force UTF-8 stdout on Windows for emoji/unicode token symbols
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

load_dotenv()

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------

AVE_API_KEY = os.getenv("AVE_API_KEY")
AVE_BASE_URL = "https://prod.ave-api.com/v2"

# Which categories of tokens to analyze (we'll pull from multiple)
RANK_TOPICS = ["hot", "meme", "gainer"]    # try multiple to build a richer sample
TOKEN_LIMIT_PER_TOPIC = 50                 # how many per topic
REQUEST_DELAY_SEC = 0.25                   # polite delay between API calls

# Minimum wallet appearances across tokens to be considered "interesting"
MIN_APPEARANCES = 2

# Minimum shared tokens between two wallets to be considered "connected"
MIN_SHARED_TOKENS = 2

# Known infrastructure addresses to exclude (burn, AMM routers, etc.)
INFRA_ADDRESSES = {
    "0x0000000000000000000000000000000000000000",
    "0x000000000000000000000000000000000000dead",
    "0x000000000000000000000000000000000000dEaD",
    "0xdead000000000000000042069420694206942069",
}

# Addresses shorter than this or matching pool-like patterns are likely not user wallets
MIN_ADDRESS_LEN = 32

HEADERS = {"X-API-KEY": AVE_API_KEY} if AVE_API_KEY else {}

# -----------------------------------------------------------------------------
# AVE API WRAPPERS
# -----------------------------------------------------------------------------

def get_top_tokens(topic: str, limit: int) -> list:
    """Fetch ranked tokens from AVE."""
    url = f"{AVE_BASE_URL}/ranks"
    params = {"topic": topic, "limit": limit}
    print(f"[*] Fetching top {limit} tokens from topic='{topic}'...")
    r = requests.get(url, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json().get("data", [])
    print(f"[+] Got {len(data)} tokens")
    return data


def get_top_holders(token_id: str) -> list:
    """Fetch top 100 holders for a given token_id ({contract}-{chain})."""
    url = f"{AVE_BASE_URL}/tokens/top100/{token_id}"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json().get("data", [])


# -----------------------------------------------------------------------------
# CORE LOGIC
# -----------------------------------------------------------------------------

def is_infra_address(addr: str) -> bool:
    """Filter out known infrastructure addresses (burn, pools, etc.)."""
    if not addr:
        return True
    lower = addr.lower()
    if lower in {a.lower() for a in INFRA_ADDRESSES}:
        return True
    # Common burn pattern: 0x000...dead
    if lower.startswith("0x0000000000") and lower.endswith("dead"):
        return True
    return False


def build_wallet_map(tokens: list) -> dict:
    """
    For each token, fetch top100 holders and build a map:
      wallet_address -> list of tokens they appear in
    """
    wallet_to_tokens = defaultdict(list)

    for i, token in enumerate(tokens, 1):
        # token_id format depends on AVE response shape; try common keys
        token_id = (
            token.get("token_id")
            or token.get("id")
            or f"{token.get('token')}-{token.get('chain')}"
        )
        raw_symbol = token.get("symbol", "?")
        symbol = raw_symbol.encode("ascii", "replace").decode("ascii")

        print(f"[{i}/{len(tokens)}] Fetching top100 for {symbol} ({token_id})...")
        try:
            holders = get_top_holders(token_id)
        except Exception as e:
            print(f"    [!] Failed: {e}")
            continue

        for h in holders:
            wallet = h.get("address") or h.get("wallet") or h.get("holder_address")
            if not wallet:
                continue
            if is_infra_address(wallet):
                continue
            # Skip wallets that hold a massive % of supply (likely AMM pools, team wallets)
            ratio = h.get("balance_ratio") or h.get("percent") or 0
            try:
                if float(ratio) > 0.10:  # skip anyone with >10% of supply
                    continue
            except (TypeError, ValueError):
                pass

            wallet_to_tokens[wallet].append({
                "token_id": token_id,
                "symbol": symbol,
                "balance_ratio": ratio,
                "realized_profit": h.get("realized_profit") or h.get("realized_pnl"),
                "unrealized_profit": h.get("unrealized_profit") or h.get("unrealized_pnl"),
            })

        time.sleep(REQUEST_DELAY_SEC)

    return wallet_to_tokens


def find_repeat_wallets(wallet_map: dict, min_appearances: int) -> list:
    """Wallets appearing in multiple tokens."""
    repeats = [
        (wallet, entries)
        for wallet, entries in wallet_map.items()
        if len(entries) >= min_appearances
    ]
    return sorted(repeats, key=lambda x: len(x[1]), reverse=True)


def find_connections(repeat_wallets: list, min_shared: int) -> list:
    """
    For every pair of repeat wallets, count shared tokens.
    Returns pairs sorted by shared count.
    """
    pairs = []
    for (w1, entries1), (w2, entries2) in combinations(repeat_wallets, 2):
        tokens1 = {e["token_id"] for e in entries1}
        tokens2 = {e["token_id"] for e in entries2}
        shared = tokens1 & tokens2
        if len(shared) >= min_shared:
            pairs.append({
                "wallet_a": w1,
                "wallet_b": w2,
                "shared_count": len(shared),
                "shared_tokens": list(shared),
            })
    return sorted(pairs, key=lambda x: x["shared_count"], reverse=True)


def identify_clusters(connections: list) -> list:
    """
    Simple cluster detection:
    Build adjacency, find connected components via BFS.
    """
    graph = defaultdict(set)
    for c in connections:
        graph[c["wallet_a"]].add(c["wallet_b"])
        graph[c["wallet_b"]].add(c["wallet_a"])

    visited = set()
    clusters = []
    for node in graph:
        if node in visited:
            continue
        # BFS
        cluster = set()
        queue = [node]
        while queue:
            curr = queue.pop(0)
            if curr in visited:
                continue
            visited.add(curr)
            cluster.add(curr)
            queue.extend(graph[curr] - visited)
        if len(cluster) >= 2:
            clusters.append(cluster)

    return sorted(clusters, key=len, reverse=True)


# -----------------------------------------------------------------------------
# REPORTING
# -----------------------------------------------------------------------------

def print_report(wallet_map, repeat_wallets, connections, clusters):
    print("\n" + "=" * 70)
    print("                   PHASE 0 — THESIS VALIDATION RESULTS")
    print("=" * 70)

    print(f"\n[STATS]")
    print(f"  Total unique wallets seen: {len(wallet_map)}")
    print(f"  Wallets in {MIN_APPEARANCES}+ tokens:  {len(repeat_wallets)}")
    print(f"  Wallet pairs sharing {MIN_SHARED_TOKENS}+ tokens: {len(connections)}")
    print(f"  Clusters identified:         {len(clusters)}")

    print(f"\n[TOP 10 REPEAT WALLETS]")
    print(f"  {'Wallet':<46} {'Tokens':>8}")
    print(f"  {'-'*46} {'-'*8}")
    for wallet, entries in repeat_wallets[:10]:
        print(f"  {wallet[:42]:<46} {len(entries):>8}")

    print(f"\n[TOP 10 WALLET CONNECTIONS]")
    for i, c in enumerate(connections[:10], 1):
        print(f"  #{i}  Shared: {c['shared_count']} tokens")
        print(f"      A: {c['wallet_a'][:42]}")
        print(f"      B: {c['wallet_b'][:42]}")
        print(f"      Tokens: {', '.join(c['shared_tokens'][:3])}...")
        print()

    print(f"[CLUSTERS FOUND]")
    for i, cluster in enumerate(clusters[:5], 1):
        print(f"  Cluster #{i} — {len(cluster)} wallets:")
        for w in list(cluster)[:8]:
            print(f"    • {w[:42]}")
        if len(cluster) > 8:
            print(f"    ... and {len(cluster) - 8} more")
        print()

    # VERDICT
    print("=" * 70)
    if len(clusters) >= 3 and len(connections) >= 10:
        print("  ✅ THESIS VALIDATED")
        print("  Clusters exist in the data. The Contagion Engine approach is viable.")
        print("  → PROCEED TO PHASE 1")
    elif len(connections) >= 3:
        print("  ⚠️  PARTIAL VALIDATION")
        print("  Some clusters found but signal is weaker than expected.")
        print("  → Consider widening the token pool, trying different rank topics,")
        print("    or lowering MIN_SHARED_TOKENS threshold.")
    else:
        print("  ❌ THESIS WEAK")
        print("  Very few wallet connections found in this sample.")
        print("  → Try a different RANK_TOPIC, larger TOKEN_LIMIT,")
        print("    or pivot to a different approach.")
    print("=" * 70 + "\n")


def save_raw_data(wallet_map, connections, clusters):
    """Save raw data for inspection."""
    out = {
        "wallets": {
            w: entries for w, entries in wallet_map.items() if len(entries) >= MIN_APPEARANCES
        },
        "connections": connections[:50],
        "clusters": [list(c) for c in clusters],
    }
    with open("validate_output.json", "w") as f:
        json.dump(out, f, indent=2, default=str)
    print("[*] Raw data saved to validate_output.json")


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def main():
    if not AVE_API_KEY:
        print("[!] AVE_API_KEY not found in environment.")
        print("    Create a .env file in this folder with:")
        print("      AVE_API_KEY=your_key_here")
        sys.exit(1)

    # 1. Get top tokens from multiple topics (dedupe by token_id)
    all_tokens = []
    seen_ids = set()
    for topic in RANK_TOPICS:
        try:
            topic_tokens = get_top_tokens(topic, TOKEN_LIMIT_PER_TOPIC)
        except Exception as e:
            print(f"[!] Failed topic '{topic}': {e}")
            continue
        for t in topic_tokens:
            tid = (
                t.get("token_id")
                or t.get("id")
                or f"{t.get('token')}-{t.get('chain')}"
            )
            if tid and tid not in seen_ids:
                seen_ids.add(tid)
                all_tokens.append(t)

    print(f"\n[+] Total unique tokens across topics: {len(all_tokens)}\n")

    if not all_tokens:
        print("[!] No tokens returned. Check API key.")
        sys.exit(1)

    # 2. Build wallet -> tokens map
    wallet_map = build_wallet_map(all_tokens)

    # 3. Find wallets appearing in multiple tokens
    repeat_wallets = find_repeat_wallets(wallet_map, MIN_APPEARANCES)

    # 4. Find connected pairs
    connections = find_connections(repeat_wallets, MIN_SHARED_TOKENS)

    # 5. Identify clusters
    clusters = identify_clusters(connections)

    # 6. Report
    print_report(wallet_map, repeat_wallets, connections, clusters)
    save_raw_data(wallet_map, connections, clusters)


if __name__ == "__main__":
    main()

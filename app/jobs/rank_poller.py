"""
AVE rank poller — turns leaderboard data into trading signals.

Algorithm (multi-topic stacking):
  Every POLL_INTERVAL, fetch a whitelist of AVE rank topics (top gainers,
  volume spikes, momentum, etc). For each solana/bsc token, count how many
  topics it appears on right now and how far it climbed since last snapshot.

  A token on ≥2 topics at once = rare alignment → fire a "rank_stack" signal.
  Conviction is computed in-process so the enricher doesn't have to re-score
  (the cluster/alpha formula doesn't apply here).

Dispatched the same way as cluster/alpha signals — writes to `db.signals` and
publishes `SIGNAL_SCORED` so the existing telegram dispatcher picks it up
unchanged.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime
from typing import Any

from app.config import settings
from app.db import mongo
from app.services.ave_client import AveClient
from app.services.event_bus import SIGNAL_FIRED, SIGNAL_SCORED, bus

# ---------------------------------------------------------------------

SUPPORTED_CHAINS = ("solana", "bsc")

POLL_INTERVAL = 5 * 60           # seconds between polls
TOPICS_PER_POLL = 100            # rows per topic
MIN_TOPICS_FOR_SIGNAL = 2        # token must appear in ≥ this many topics
RANK_JUMP_BONUS_MIN = 10         # climbing ≥ N positions adds bonus conviction
MAX_TOPICS_WHITELIST = 6         # keep polling cost bounded

# Topic slugs we care about. AVE doesn't publish a stable manifest, so we
# loosely match on human names (see _pick_interesting_topics).
TOPIC_HINTS = (
    "gainer",      # "Top Gainers 1h", "Top Gainers 24h"
    "volume",      # "Top Volume"
    "momentum",    # "Momentum"
    "trending",    # "Trending"
    "heat",        # "Heat / hot"
    "breakout",
)

_stop = asyncio.Event()

# topic_slug -> { token_id -> rank }
_snapshots: dict[str, dict[str, int]] = {}
# topic_slug -> human-readable label (for logging / message formatting)
_topic_labels: dict[str, str] = {}


# ---------------------------------------------------------------------
# Topic discovery
# ---------------------------------------------------------------------

def _coerce_list(data: Any) -> list[dict]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for key in ("list", "data", "items", "result", "topics"):
            inner = data.get(key)
            if isinstance(inner, list):
                return [x for x in inner if isinstance(x, dict)]
    return []


def _pick_interesting_topics(raw: list[dict]) -> list[tuple[str, str]]:
    """
    Return [(slug, label), ...] for topics whose name contains any keyword
    in TOPIC_HINTS. Capped to MAX_TOPICS_WHITELIST.
    """
    picked: list[tuple[str, str]] = []
    for t in raw:
        slug = (
            t.get("topic")
            or t.get("slug")
            or t.get("name")
            or t.get("id")
        )
        label = t.get("label") or t.get("title") or t.get("name") or slug
        if not slug:
            continue
        lower = str(label).lower()
        if not any(h in lower for h in TOPIC_HINTS):
            continue
        picked.append((str(slug), str(label)))
        if len(picked) >= MAX_TOPICS_WHITELIST:
            break
    return picked


# ---------------------------------------------------------------------
# Per-poll: stack tokens across topics
# ---------------------------------------------------------------------

def _normalize_token(row: dict) -> tuple[str, str, str] | None:
    """(token_id, chain, symbol) — drops anything off-chain or unsupported."""
    chain = (row.get("chain") or row.get("chain_name") or "").lower()
    if chain not in SUPPORTED_CHAINS:
        return None
    contract = row.get("token") or row.get("token_address") or row.get("address")
    if not contract:
        return None
    symbol = row.get("symbol") or row.get("token_symbol") or ""
    return (f"{contract}-{chain}", chain, symbol)


async def _poll_topic(ave: AveClient, slug: str) -> list[tuple[str, str, str, int]]:
    """Returns [(token_id, chain, symbol, rank), ...] for this topic."""
    # Virtual topic: "trending_<chain>" uses AVE's trending feed instead of ranks.
    # Lets us stack a token's movers-presence with named rank topics.
    if slug.startswith("trending_"):
        chain = slug.split("_", 1)[1]
        try:
            raw = await ave.trending(chain, page=0, page_size=min(50, TOPICS_PER_POLL))
        except Exception as e:
            print(f"[rank_poller] trending({chain}) fetch failed: {e}")
            return []
    else:
        try:
            raw = await ave.ranks(slug, limit=TOPICS_PER_POLL)
        except Exception as e:
            print(f"[rank_poller] '{slug}' ranks fetch failed: {e}")
            return []

    rows = _coerce_list(raw)
    out: list[tuple[str, str, str, int]] = []
    for idx, row in enumerate(rows, start=1):
        norm = _normalize_token(row)
        if norm:
            tid, chain, symbol = norm
            out.append((tid, chain, symbol, idx))
    return out


def _compute_conviction(topics_count: int, best_rank_jump: int) -> int:
    """
    Stack score: more topics = more conviction.
      2 topics → 55
      3 topics → 72
      4 topics → 85
      5+ topics → 95
    Plus up to +10 for jumping ≥ RANK_JUMP_BONUS_MIN positions on any topic.
    """
    base = {2: 55, 3: 72, 4: 85}.get(topics_count, 95 if topics_count >= 5 else 0)
    bonus = 0
    if best_rank_jump >= RANK_JUMP_BONUS_MIN:
        bonus = min(10, best_rank_jump // 5)  # +1 per 5 positions jumped, capped
    return min(99, base + bonus)


# ---------------------------------------------------------------------
# Persistence + dispatch
# ---------------------------------------------------------------------

async def _fire_signal(
    token_id: str,
    chain: str,
    symbol: str | None,
    topics_hit: list[tuple[str, str, int, int]],   # (slug, label, rank, jump)
    conviction: int,
) -> None:
    """Write signal to db.signals and publish SIGNAL_SCORED."""
    now = datetime.utcnow()
    topic_summary = [
        {"topic": slug, "label": label, "rank": rank, "rank_jump": jump}
        for slug, label, rank, jump in topics_hit
    ]
    payload = {
        "signal_type": "rank_stack",
        "token_id": token_id,
        "chain": chain,
        "symbol": symbol or None,
        "conviction_score": conviction,
        "status": "watch" if conviction < 75 else "execute",
        "topics": topic_summary,
        "topics_count": len(topics_hit),
        "best_rank_jump": max((j for _, _, _, j in topics_hit), default=0),
        "first_entry_at": now,
        "last_entry_at": now,
        "detected_at": now,
        "updated_at": now,
    }

    db = mongo.db()
    filter_ = {"signal_type": "rank_stack", "token_id": token_id}
    update_fields = {k: v for k, v in payload.items() if k != "first_entry_at"}
    insert_only = {"first_entry_at": now, "created_at": now}
    res = await db.signals.find_one_and_update(
        filter_,
        {
            "$set": update_fields,
            "$setOnInsert": insert_only,
            "$inc": {"fire_count": 1},
        },
        upsert=True,
        return_document=True,
    )

    out = dict(res or payload)
    out["id"] = str(out.pop("_id")) if "_id" in out else None
    for k in ("first_entry_at", "last_entry_at", "created_at", "updated_at", "detected_at"):
        if isinstance(out.get(k), datetime):
            out[k] = out[k].isoformat()

    # Publish SIGNAL_FIRED so anything subscribed to new-signal creation sees
    # it, plus SIGNAL_SCORED (conviction already computed) so the telegram
    # dispatcher fans it out immediately — no enricher roundtrip needed.
    await bus.publish(SIGNAL_FIRED, out)
    await bus.publish(SIGNAL_SCORED, out)

    topic_names = ", ".join(label for _, label, _, _ in topics_hit)
    print(
        f"[rank_poller] STACK {symbol or token_id[:10]} on {len(topics_hit)} "
        f"topics ({topic_names}) · conviction={conviction}"
    )


# ---------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------

async def _discover_topics(ave: AveClient) -> list[tuple[str, str]]:
    try:
        raw = await ave.rank_topics()
    except Exception as e:
        print(f"[rank_poller] rank_topics fetch failed: {e}")
        raw = None

    topics = _pick_interesting_topics(_coerce_list(raw)) if raw else []

    # Always include per-chain trending as virtual topics. Guarantees we get
    # signals even when AVE's named leaderboards are empty or unavailable.
    for chain in SUPPORTED_CHAINS:
        slug = f"trending_{chain}"
        label = f"Trending {chain.upper()}"
        if slug not in [s for s, _ in topics]:
            topics.append((slug, label))

    if topics:
        print(
            f"[rank_poller] watching {len(topics)} topics: "
            + ", ".join(label for _, label in topics)
        )
        for slug, label in topics:
            _topic_labels[slug] = label
    return topics


async def _one_pass(ave: AveClient, topics: list[tuple[str, str]]) -> None:
    # Fetch every topic in parallel — AVE is fine with this and it keeps
    # the 5-min cadence tight.
    results = await asyncio.gather(
        *[_poll_topic(ave, slug) for slug, _ in topics],
        return_exceptions=True,
    )

    # token_id -> list[(slug, label, rank, rank_jump)]
    per_token: dict[str, list[tuple[str, str, int, int]]] = {}
    # token_id -> (chain, symbol)
    meta: dict[str, tuple[str, str]] = {}

    for (slug, label), res in zip(topics, results):
        if isinstance(res, Exception) or not res:
            continue
        prev_snap = _snapshots.get(slug) or {}
        new_snap: dict[str, int] = {}
        for tid, chain, symbol, rank in res:
            new_snap[tid] = rank
            prev_rank = prev_snap.get(tid)
            jump = (prev_rank - rank) if prev_rank is not None else 0  # lower rank = higher
            per_token.setdefault(tid, []).append((slug, label, rank, jump))
            meta.setdefault(tid, (chain, symbol))
        _snapshots[slug] = new_snap

    # Emit stacked signals
    for tid, hits in per_token.items():
        if len(hits) < MIN_TOPICS_FOR_SIGNAL:
            continue
        chain, symbol = meta[tid]
        best_jump = max((j for _, _, _, j in hits), default=0)
        conviction = _compute_conviction(len(hits), best_jump)
        if conviction <= 0:
            continue
        try:
            await _fire_signal(tid, chain, symbol, hits, conviction)
        except Exception as e:
            print(f"[rank_poller] fire_signal failed for {tid}: {e}")


async def run() -> None:
    if not settings.ave_api_key:
        print("[rank_poller] AVE_API_KEY missing — skipping")
        return

    print("[rank_poller] starting · interval=%ds" % POLL_INTERVAL)

    # Discover topics once; rediscover every hour in case AVE rotates catalog.
    topics: list[tuple[str, str]] = []
    last_discover = 0.0

    while not _stop.is_set():
        try:
            async with AveClient() as ave:
                now = time.time()
                if not topics or now - last_discover > 3600:
                    topics = await _discover_topics(ave) or topics
                    last_discover = now
                if topics:
                    await _one_pass(ave, topics)
        except Exception as e:
            print(f"[rank_poller] loop error: {e}")

        try:
            await asyncio.wait_for(_stop.wait(), timeout=POLL_INTERVAL)
        except asyncio.TimeoutError:
            pass

    print("[rank_poller] stopped")


def stop() -> None:
    _stop.set()

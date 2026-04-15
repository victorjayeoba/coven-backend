"""
One-shot cleanup for the `signals` collection.

What it does:
  1. Backfill `signal_type` on legacy docs that pre-date the field
     ('cluster' if cluster_id present; 'alpha' otherwise).
  2. Dedupe cluster signals by (token_id, cluster_id) — keep the strongest,
     drop the rest.
  3. Dedupe alpha signals by (token_id, first_wallet) — same logic.
  4. Optionally drop docs with neither cluster_id nor wallets_involved
     (truly orphaned legacy junk).

Run:
  cd backend
  python -m scripts.clean_signals          # safe — reports what it would do
  python -m scripts.clean_signals --apply  # actually mutates Mongo

Idempotent. Re-running after an --apply pass should be a no-op.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.db import mongo


def _docs_summary(docs: list[dict]) -> str:
    """Compact line per dup group for the dry-run report."""
    parts = []
    for d in docs:
        sym = d.get("symbol") or ""
        conv = d.get("conviction_score", 0)
        parts.append(f"{str(d['_id'])[-6:]}({sym} c={conv})")
    return " | ".join(parts)


async def _backfill(db, apply: bool) -> dict:
    cluster_filter = {
        "signal_type": {"$exists": False},
        "cluster_id": {"$ne": None, "$exists": True},
    }
    alpha_filter = {
        "signal_type": {"$exists": False},
        "$or": [
            {"cluster_id": None},
            {"cluster_id": {"$exists": False}},
        ],
    }

    cluster_n = await db.signals.count_documents(cluster_filter)
    alpha_n = await db.signals.count_documents(alpha_filter)

    if apply:
        if cluster_n:
            await db.signals.update_many(
                cluster_filter, {"$set": {"signal_type": "cluster"}}
            )
        if alpha_n:
            await db.signals.update_many(
                alpha_filter, {"$set": {"signal_type": "alpha"}}
            )

    return {"cluster": cluster_n, "alpha": alpha_n}


async def _dedupe_cluster(db, apply: bool) -> dict:
    pipeline = [
        {"$match": {"signal_type": "cluster"}},
        {
            "$group": {
                "_id": {
                    "token_id": "$token_id",
                    "cluster_id": {
                        "$toString": {"$ifNull": ["$cluster_id", ""]}
                    },
                },
                "docs": {
                    "$push": {
                        "_id": "$_id",
                        "symbol": "$symbol",
                        "conviction_score": {
                            "$ifNull": ["$conviction_score", 0]
                        },
                        "detected_at": "$detected_at",
                    }
                },
                "n": {"$sum": 1},
            }
        },
        {"$match": {"n": {"$gt": 1}}},
    ]

    groups: list[dict] = []
    async for g in db.signals.aggregate(pipeline):
        groups.append(g)

    deleted = 0
    for g in groups:
        docs = g["docs"]
        docs.sort(
            key=lambda d: (
                int(d.get("conviction_score") or 0),
                d.get("detected_at") or "",
            ),
            reverse=True,
        )
        keep = docs[0]
        drop = [d["_id"] for d in docs[1:]]
        token = g["_id"]["token_id"]
        cluster = g["_id"]["cluster_id"]
        print(
            f"  cluster · token={token[:20]}… cabal={cluster}\n"
            f"    {_docs_summary(docs)}\n"
            f"    keep {str(keep['_id'])[-6:]}, drop {len(drop)}"
        )
        if apply and drop:
            res = await db.signals.delete_many({"_id": {"$in": drop}})
            deleted += int(res.deleted_count or 0)

    return {"groups": len(groups), "deleted": deleted}


async def _dedupe_alpha(db, apply: bool) -> dict:
    pipeline = [
        {"$match": {"signal_type": "alpha"}},
        {
            "$group": {
                "_id": {
                    "token_id": "$token_id",
                    "wallet": {"$arrayElemAt": ["$wallets_involved", 0]},
                },
                "docs": {
                    "$push": {
                        "_id": "$_id",
                        "symbol": "$symbol",
                        "conviction_score": {
                            "$ifNull": ["$conviction_score", 0]
                        },
                        "detected_at": "$detected_at",
                    }
                },
                "n": {"$sum": 1},
            }
        },
        {"$match": {"n": {"$gt": 1}}},
    ]

    groups: list[dict] = []
    async for g in db.signals.aggregate(pipeline):
        groups.append(g)

    deleted = 0
    for g in groups:
        docs = g["docs"]
        docs.sort(
            key=lambda d: (
                int(d.get("conviction_score") or 0),
                d.get("detected_at") or "",
            ),
            reverse=True,
        )
        keep = docs[0]
        drop = [d["_id"] for d in docs[1:]]
        token = g["_id"]["token_id"]
        wallet = (g["_id"].get("wallet") or "")[:10]
        print(
            f"  alpha · token={token[:20]}… wallet={wallet}…\n"
            f"    {_docs_summary(docs)}\n"
            f"    keep {str(keep['_id'])[-6:]}, drop {len(drop)}"
        )
        if apply and drop:
            res = await db.signals.delete_many({"_id": {"$in": drop}})
            deleted += int(res.deleted_count or 0)

    return {"groups": len(groups), "deleted": deleted}


async def _drop_orphans(db, apply: bool) -> dict:
    """Docs with no cluster_id AND no wallets — junk we can't classify."""
    f = {
        "$or": [
            {"cluster_id": None},
            {"cluster_id": {"$exists": False}},
        ],
        "$and": [
            {
                "$or": [
                    {"wallets_involved": {"$exists": False}},
                    {"wallets_involved": {"$size": 0}},
                ]
            }
        ],
    }
    n = await db.signals.count_documents(f)
    if apply and n:
        res = await db.signals.delete_many(f)
        return {"deleted": int(res.deleted_count or 0)}
    return {"deleted": 0, "found": n}


async def main(apply: bool) -> None:
    mongo.connect()
    if not await mongo.ping():
        print("[!] MongoDB unreachable. Check MONGO_URI in .env.")
        return
    db = mongo.db()

    mode = "APPLY" if apply else "DRY-RUN (no changes)"
    print(f"\n=== Coven signals cleanup · {mode} ===\n")

    total = await db.signals.count_documents({})
    print(f"Total signals before: {total}\n")

    print("[1/4] Backfill signal_type on legacy docs")
    bf = await _backfill(db, apply)
    print(f"  cluster backfilled: {bf['cluster']}")
    print(f"  alpha   backfilled: {bf['alpha']}\n")

    print("[2/4] Dedupe cluster signals (group by token + cluster_id)")
    c = await _dedupe_cluster(db, apply)
    if c["groups"] == 0:
        print("  no duplicate cluster groups")
    print(f"  groups merged: {c['groups']}, docs removed: {c['deleted']}\n")

    print("[3/4] Dedupe alpha signals (group by token + first wallet)")
    a = await _dedupe_alpha(db, apply)
    if a["groups"] == 0:
        print("  no duplicate alpha groups")
    print(f"  groups merged: {a['groups']}, docs removed: {a['deleted']}\n")

    print("[4/4] Drop orphan docs (no cluster_id and no wallets)")
    o = await _drop_orphans(db, apply)
    print(f"  {o}\n")

    after = await db.signals.count_documents({})
    delta = total - after
    print(f"Total signals after:  {after}  (Δ -{delta})")
    if not apply:
        print("\n(Dry run — re-run with --apply to actually write changes.)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean the signals collection.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually mutate Mongo. Without this, only reports what it would do.",
    )
    args = parser.parse_args()
    asyncio.run(main(args.apply))

"""
One-shot cleanup for duplicate signal documents created before the upsert
change landed. Runs at startup — idempotent, safe to re-run.

Cluster signals: group by (signal_type='cluster', token_id, cluster_id).
Alpha signals:   group by (signal_type='alpha', token_id, wallets_involved[0]).

In each group, keep the document with the highest `conviction_score`
(tiebreak on most recent `detected_at`) and delete the rest.
"""

from __future__ import annotations

from app.db import mongo


async def run() -> dict:
    db = mongo.db()
    stats = {
        "cluster_merged": 0,
        "alpha_merged": 0,
        "deleted": 0,
        "backfilled_cluster": 0,
        "backfilled_alpha": 0,
    }

    # --- 0. Backfill signal_type on legacy docs ------------------------
    # Older signals (created before signal_type was a field) silently break
    # the dedupe + upsert filters. Tag them based on cluster_id presence:
    #   cluster_id present → cluster signal
    #   wallets_involved.size == 1 and no cluster_id → alpha signal
    res = await db.signals.update_many(
        {"signal_type": {"$exists": False}, "cluster_id": {"$ne": None}},
        {"$set": {"signal_type": "cluster"}},
    )
    stats["backfilled_cluster"] = int(res.modified_count or 0)

    res = await db.signals.update_many(
        {
            "signal_type": {"$exists": False},
            "$or": [{"cluster_id": None}, {"cluster_id": {"$exists": False}}],
        },
        {"$set": {"signal_type": "alpha"}},
    )
    stats["backfilled_alpha"] = int(res.modified_count or 0)

    # --- Cluster dupes -------------------------------------------------
    # Coerce cluster_id to string in the group key — otherwise Mongo treats
    # `1` (int) and `"1"` (string) as different buckets and won't merge them.
    pipeline_cluster = [
        {"$match": {"signal_type": "cluster"}},
        {
            "$group": {
                "_id": {
                    "token_id": "$token_id",
                    "cluster_id": {"$toString": {"$ifNull": ["$cluster_id", ""]}},
                },
                "docs": {
                    "$push": {
                        "_id": "$_id",
                        "conviction": {"$ifNull": ["$conviction_score", 0]},
                        "detected_at": "$detected_at",
                    }
                },
                "n": {"$sum": 1},
            }
        },
        {"$match": {"n": {"$gt": 1}}},
    ]
    async for grp in db.signals.aggregate(pipeline_cluster):
        docs = grp.get("docs") or []
        if len(docs) < 2:
            continue
        # Sort: highest conviction first, then most recent
        docs.sort(
            key=lambda d: (
                int(d.get("conviction") or 0),
                d.get("detected_at") or 0,
            ),
            reverse=True,
        )
        keep_id = docs[0]["_id"]
        drop_ids = [d["_id"] for d in docs[1:]]
        if drop_ids:
            print(
                f"[signal_dedupe] cluster dup on token={grp['_id'].get('token_id')} "
                f"cluster={grp['_id'].get('cluster_id')} · "
                f"keep={keep_id} drop={len(drop_ids)}"
            )
            res = await db.signals.delete_many({"_id": {"$in": drop_ids}})
            stats["cluster_merged"] += 1
            stats["deleted"] += int(res.deleted_count or 0)

    # --- Alpha dupes ---------------------------------------------------
    pipeline_alpha = [
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
                        "conviction": {"$ifNull": ["$conviction_score", 0]},
                        "detected_at": "$detected_at",
                    }
                },
                "n": {"$sum": 1},
            }
        },
        {"$match": {"n": {"$gt": 1}}},
    ]
    async for grp in db.signals.aggregate(pipeline_alpha):
        docs = grp.get("docs") or []
        if len(docs) < 2:
            continue
        docs.sort(
            key=lambda d: (
                int(d.get("conviction") or 0),
                d.get("detected_at") or 0,
            ),
            reverse=True,
        )
        drop_ids = [d["_id"] for d in docs[1:]]
        if drop_ids:
            res = await db.signals.delete_many({"_id": {"$in": drop_ids}})
            stats["alpha_merged"] += 1
            stats["deleted"] += int(res.deleted_count or 0)

    return stats

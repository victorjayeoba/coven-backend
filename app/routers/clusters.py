from fastapi import APIRouter, Depends, HTTPException

from app.auth.dependencies import get_current_user
from app.db import mongo

router = APIRouter(prefix="/api/clusters", tags=["clusters"])


@router.get("")
async def list_clusters(_: dict = Depends(get_current_user)):
    db = mongo.db()
    out = []
    async for c in db.clusters.find().sort("size", 1):
        out.append({
            "cluster_id": c["cluster_id"],
            "size": c.get("size", 0),
            "chain": c.get("chain"),
            "wallet_addresses": c.get("wallet_addresses") or [],
            "created_at": c.get("created_at"),
        })
    return out


@router.get("/{cluster_id}")
async def get_cluster(cluster_id: int, _: dict = Depends(get_current_user)):
    db = mongo.db()
    c = await db.clusters.find_one({"cluster_id": cluster_id}, {"_id": 0})
    if not c:
        raise HTTPException(status_code=404, detail="Cluster not found")

    # Enrich with member wallet stats
    members = []
    for addr in c.get("wallet_addresses", []):
        w = await db.wallets.find_one(
            {"address": addr},
            {"_id": 0, "address": 1, "chain": 1, "alpha_score": 1, "total_profit": 1, "tokens": 1},
        )
        if w:
            members.append({
                "address": w["address"],
                "chain": w.get("chain"),
                "alpha_score": w.get("alpha_score", 0.0),
                "total_profit": w.get("total_profit"),
                "token_count": len(w.get("tokens") or []),
            })
    c["members"] = members
    return c

from fastapi import APIRouter, Depends, HTTPException, Query

from app.auth.dependencies import get_current_user
from app.db import mongo

router = APIRouter(prefix="/api/wallets", tags=["wallets"])


@router.get("/graph")
async def wallet_graph(
    _: dict = Depends(get_current_user),
    chain: str | None = None,
    min_alpha: float = Query(0.0, ge=0.0),
    limit_nodes: int = Query(500, ge=10, le=2000),
):
    """
    Returns { nodes: [...], edges: [...], clusters: [...] } for the graph view.
    """
    db = mongo.db()
    q: dict = {"alpha_score": {"$gte": min_alpha}}
    if chain:
        q["chain"] = chain

    nodes = []
    async for w in db.wallets.find(q).sort("alpha_score", -1).limit(limit_nodes):
        nodes.append({
            "address": w["address"],
            "chain": w.get("chain"),
            "alpha_score": w.get("alpha_score", 0.0),
            "total_profit": w.get("total_profit"),
            "token_count": len(w.get("tokens") or []),
        })

    node_addresses = {n["address"] for n in nodes}
    edges = []
    async for e in db.wallet_edges.find(
        {"wallet_a": {"$in": list(node_addresses)}, "wallet_b": {"$in": list(node_addresses)}}
    ):
        edges.append({
            "source": e["wallet_a"],
            "target": e["wallet_b"],
            "weight": e.get("shared_count", 1),
        })

    clusters = []
    async for c in db.clusters.find().sort("size", -1):
        clusters.append({
            "cluster_id": c["cluster_id"],
            "size": c.get("size", 0),
            "chain": c.get("chain"),
            "wallet_addresses": c.get("wallet_addresses") or [],
        })

    return {"nodes": nodes, "edges": edges, "clusters": clusters}


@router.get("/top")
async def top_wallets(
    _: dict = Depends(get_current_user),
    chain: str | None = None,
    limit: int = Query(25, ge=1, le=100),
):
    db = mongo.db()
    q: dict = {}
    if chain:
        q["chain"] = chain
    out = []
    async for w in db.wallets.find(q).sort("alpha_score", -1).limit(limit):
        out.append({
            "address": w["address"],
            "chain": w.get("chain"),
            "alpha_score": w.get("alpha_score", 0.0),
            "total_profit": w.get("total_profit"),
            "total_trades": w.get("total_trades"),
            "token_count": len(w.get("tokens") or []),
        })
    return out


@router.get("/{address}")
async def get_wallet(address: str, _: dict = Depends(get_current_user)):
    db = mongo.db()
    w = await db.wallets.find_one({"address": address}, {"_id": 0})
    if not w:
        raise HTTPException(status_code=404, detail="Wallet not in graph")
    return w

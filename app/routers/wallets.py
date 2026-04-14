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

    # Enrich with cluster info
    cluster = await db.clusters.find_one(
        {"wallet_addresses": address}, {"_id": 0}
    )
    if cluster:
        w["cluster"] = {
            "cluster_id": cluster.get("cluster_id"),
            "size": cluster.get("size"),
            "chain": cluster.get("chain"),
            "wallet_addresses": cluster.get("wallet_addresses") or [],
        }

    # Find connected wallets (edges)
    neighbors: list[dict] = []
    async for e in db.wallet_edges.find(
        {"$or": [{"wallet_a": address}, {"wallet_b": address}]}
    ):
        other = e["wallet_b"] if e["wallet_a"] == address else e["wallet_a"]
        neighbors.append({
            "address": other,
            "shared_count": e.get("shared_count", 0),
        })
    neighbors.sort(key=lambda x: -(x["shared_count"] or 0))
    w["neighbors"] = neighbors[:20]

    # Recent signals this wallet has been part of (live or backtest)
    recent_signals: list[dict] = []
    async for s in db.signals.find(
        {"wallets_involved": address}, {"_id": 1, "token_id": 1, "symbol": 1,
                                         "chain": 1, "cluster_id": 1,
                                         "conviction_score": 1, "status": 1,
                                         "detected_at": 1}
    ).sort("detected_at", -1).limit(10):
        s["id"] = str(s.pop("_id"))
        recent_signals.append(s)

    async for s in db.backtests.find(
        {"wallets_involved": address}, {"_id": 1, "token_id": 1, "symbol": 1,
                                         "chain": 1, "cluster_id": 1,
                                         "conviction_score": 1, "status": 1,
                                         "first_entry_at": 1, "peak_pnl_pct": 1,
                                         "realistic_pnl_pct": 1}
    ).sort("first_entry_at", -1).limit(10):
        s["id"] = str(s.pop("_id"))
        s["detected_at"] = s.pop("first_entry_at", None)
        recent_signals.append(s)

    w["recent_signals"] = recent_signals[:10]
    return w

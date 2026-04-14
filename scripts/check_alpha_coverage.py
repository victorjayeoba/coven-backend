"""
Count how many of our seeded wallets qualify for alpha-solo signals.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.db import mongo


ALPHA_THRESHOLD = 1.0


async def main() -> None:
    mongo.connect()
    db = mongo.db()

    total = await db.wallets.count_documents({})
    alpha_elite = await db.wallets.count_documents(
        {"alpha_score": {"$gte": ALPHA_THRESHOLD}}
    )
    alpha_strong = await db.wallets.count_documents(
        {"alpha_score": {"$gte": 0.5}}
    )

    pipeline = [
        {"$group": {
            "_id": None,
            "max": {"$max": "$alpha_score"},
            "avg": {"$avg": "$alpha_score"},
        }},
    ]
    agg = [doc async for doc in db.wallets.aggregate(pipeline)]
    max_alpha = agg[0]["max"] if agg else 0
    avg_alpha = agg[0]["avg"] if agg else 0

    print(f"\nTotal wallets indexed:  {total}")
    print(f"Alpha >= 1.0 (elite):   {alpha_elite}   <- these fire ALPHA signals")
    print(f"Alpha >= 0.5 (strong):  {alpha_strong}")
    print(f"Max alpha in graph:     {max_alpha:.2f}")
    print(f"Avg alpha in graph:     {avg_alpha:.2f}\n")

    # Show top 10 highest alpha wallets
    print("Top 10 alpha wallets:")
    async for w in db.wallets.find({}, {
        "_id": 0, "address": 1, "chain": 1, "alpha_score": 1, "total_profit": 1,
    }).sort("alpha_score", -1).limit(10):
        addr = w["address"]
        print(f"  {addr[:10]}…  α={w.get('alpha_score', 0):.2f}  "
              f"profit=${w.get('total_profit', 0):,.0f}  {w.get('chain')}")

    await mongo.disconnect()


if __name__ == "__main__":
    asyncio.run(main())

"""
Build the Coven wallet graph from AVE's smart money ranking.

Run:
  cd backend
  python -m scripts.seed_graph
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.db import mongo
from app.services.graph_builder import build_and_save

CHAINS = ["solana", "bsc"]
WALLETS_PER_CHAIN = 200


async def main() -> None:
    mongo.connect()
    if not await mongo.ping():
        print("[!] MongoDB unreachable. Check MONGO_URI in .env.")
        return

    print(f"[*] Seeding graph from AVE smart_wallet/list "
          f"({CHAINS}, {WALLETS_PER_CHAIN}/chain)")
    stats = await build_and_save(CHAINS, WALLETS_PER_CHAIN)

    print("\n[+] Graph built:")
    for k, v in stats.items():
        print(f"    {k:26} {v}")

    await mongo.disconnect()


if __name__ == "__main__":
    asyncio.run(main())

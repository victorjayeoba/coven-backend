"""
Prints the current tokens the WSS listener would subscribe to.
Run:
  cd backend
  python -m scripts.show_watchlist
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.jobs.ws_listener import _fetch_subscription_targets


async def main() -> None:
    targets = await _fetch_subscription_targets()
    print(f"\n[+] {len(targets)} tokens on current watchlist\n")

    # Group by chain
    by_chain: dict[str, list] = {}
    for chain, contract, symbol in targets:
        by_chain.setdefault(chain, []).append((symbol or "?", contract))

    for chain, rows in sorted(by_chain.items()):
        print(f"— {chain.upper()} ({len(rows)} tokens) —")
        for symbol, contract in rows:
            safe_symbol = symbol.encode("ascii", "replace").decode("ascii")
            print(f"  {safe_symbol:20}  {contract}")
        print()


if __name__ == "__main__":
    asyncio.run(main())

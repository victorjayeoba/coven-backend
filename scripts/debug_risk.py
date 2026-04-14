"""Dump FULL /contracts and /tokens responses."""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.services.ave_client import AveClient

TOKENS = [
    "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c-bsc",  # WBNB (safe)
]


async def main() -> None:
    async with AveClient() as ave:
        for tid in TOKENS:
            print("=" * 60)
            print(f"/v2/contracts/{tid}")
            print("=" * 60)
            try:
                data = await ave.contract_risk(tid)
                # Show ALL keys + values, truncated per field
                if isinstance(data, dict):
                    for k, v in data.items():
                        s = json.dumps(v, default=str)
                        print(f"  {k}: {s[:200]}")
                else:
                    print(type(data), data)
            except Exception as e:
                print(f"ERROR: {type(e).__name__}: {e}")


if __name__ == "__main__":
    asyncio.run(main())

"""Dump the actual shape of a klines response."""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.services.ave_client import AveClient

TOKEN_ID = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c-bsc"


async def main() -> None:
    async with AveClient() as ave:
        data = await ave.klines_by_token(TOKEN_ID, interval=15, limit=5)
        print(f"Type: {type(data).__name__}")
        print(f"Preview: {json.dumps(data, default=str)[:1500]}")
        print()
        if isinstance(data, list) and data:
            print(f"First item type: {type(data[0]).__name__}")
            print(f"First item: {json.dumps(data[0], default=str, indent=2)}")
        elif isinstance(data, dict):
            print(f"Keys: {list(data.keys())}")
            for k, v in data.items():
                if isinstance(v, list) and v:
                    print(f"\n  {k}[0]: {json.dumps(v[0], default=str)[:500]}")


if __name__ == "__main__":
    asyncio.run(main())

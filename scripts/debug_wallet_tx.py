"""Test the wallet_token_tx endpoint to see if it actually returns sells."""

import asyncio
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.db import mongo
from app.services.ave_client import AveClient


async def main() -> None:
    mongo.connect()
    db = mongo.db()

    # Pick first backtest record to test with
    rec = await db.backtests.find_one({"outcome": {"$in": ["win", "loss"]}})
    if not rec:
        print("No backtest records to test with. Run run_backtest first.")
        return

    wallet = rec["wallets_involved"][0]
    token_id = rec["token_id"]
    chain = rec.get("chain") or token_id.rsplit("-", 1)[-1]
    token_contract = token_id.rsplit("-", 1)[0]

    print(f"Testing wallet_token_tx for:")
    print(f"  wallet:   {wallet}")
    print(f"  chain:    {chain}")
    print(f"  token:    {token_contract}")
    print(f"  symbol:   {rec.get('symbol')}")

    async with AveClient() as ave:
        try:
            data = await ave.wallet_token_tx(
                wallet_address=wallet,
                chain=chain,
                token_address=token_contract,
                from_time=int(time.time() - 30 * 86400),  # last 30 days
            )
        except Exception as e:
            print(f"\nERROR: {type(e).__name__}: {e}")
            return

    print(f"\nResponse type: {type(data).__name__}")
    if isinstance(data, dict):
        print(f"Keys: {list(data.keys())}")
        # Find the list of txs
        for k, v in data.items():
            if isinstance(v, list):
                print(f"\n'{k}' list has {len(v)} items")
                if v:
                    print(f"First item keys: {list(v[0].keys()) if isinstance(v[0], dict) else type(v[0]).__name__}")
                    print(f"First item: {json.dumps(v[0], default=str, indent=2)[:800]}")
                break
    elif isinstance(data, list):
        print(f"List with {len(data)} items")
        if data:
            print(f"First: {json.dumps(data[0], default=str, indent=2)[:800]}")
    else:
        print(f"Raw: {data}")


if __name__ == "__main__":
    asyncio.run(main())

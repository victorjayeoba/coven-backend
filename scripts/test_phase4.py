"""
Test Phase 4 end-to-end without waiting for a live signal.

Synthesizes a fake ContagionSignal for a real token, pushes it through
the signal_enricher, and prints the full scoring + risk result.

Run:
  cd backend
  python -m scripts.test_phase4
"""

import asyncio
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.db import mongo
from app.services import signal_enricher
from app.services.ave_client import AveClient


# ---------------------------------------------------------------------
# Pick a real token to test against
# ---------------------------------------------------------------------

TEST_TOKENS = [
    # (token_id, symbol) — try a few real ones
    ("0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c-bsc", "WBNB"),   # should pass risk
    ("0x55d398326f99059ff775485246999027b3197955-bsc", "USDT"),   # should pass risk
]


async def pick_live_pump_token(ave: AveClient) -> tuple[str, str] | None:
    """Grab a live pump_in_hot token — more realistic scoring test."""
    try:
        data = await ave.tokens_platform(tag="pump_in_hot", limit=5)
    except Exception:
        return None
    items = data if isinstance(data, list) else (data or {}).get("list") or []
    for t in items:
        if not isinstance(t, dict):
            continue
        chain = t.get("chain")
        contract = t.get("token") or t.get("token_address")
        symbol = t.get("symbol")
        if chain and contract:
            return f"{contract}-{chain}", symbol
    return None


async def run_test(token_id: str, symbol: str | None) -> None:
    print(f"\n{'='*60}")
    print(f"TESTING: {symbol or '?'}  ({token_id})")
    print(f"{'='*60}")

    # Build a fake signal — looks exactly like what the detector emits
    fake_signal = {
        "token_id": token_id,
        "chain": token_id.rsplit("-", 1)[-1],
        "symbol": symbol,
        "cluster_id": 999,               # fake cluster id
        "wallets_involved": [],           # empty → alpha_scores will be 0
        "cluster_size_total": 4,
        "cluster_active_count": 3,
        "time_window_seconds": 180,
        "first_entry_at": datetime.utcnow().isoformat(),
        "last_entry_at": datetime.utcnow().isoformat(),
        "detected_at": datetime.utcnow().isoformat(),
    }

    # Ensure a matching doc exists so the enricher's $update has a target
    db = mongo.db()
    await db.signals.update_one(
        {"token_id": fake_signal["token_id"], "cluster_id": 999, "detected_at": fake_signal["detected_at"]},
        {"$set": fake_signal},
        upsert=True,
    )

    # Run the enricher directly
    await signal_enricher.on_signal_fired(fake_signal)

    # Read back and print the enriched record
    doc = await db.signals.find_one(
        {"token_id": fake_signal["token_id"], "cluster_id": 999, "detected_at": fake_signal["detected_at"]},
        {"_id": 0},
    )
    print("\nEnriched signal in MongoDB:")
    import json
    print(json.dumps(doc, indent=2, default=str))


async def main() -> None:
    mongo.connect()
    if not await mongo.ping():
        print("[!] MongoDB unreachable.")
        return

    tokens: list[tuple[str, str | None]] = list(TEST_TOKENS)

    async with AveClient() as ave:
        live = await pick_live_pump_token(ave)
    if live:
        tokens.insert(0, live)
        print(f"[+] Including a live pump_in_hot token: {live[1]}")

    for token_id, symbol in tokens:
        await run_test(token_id, symbol)

    # Clean up fake test docs
    db = mongo.db()
    await db.signals.delete_many({"cluster_id": 999})
    print("\n[+] Cleaned up test signals.")

    await mongo.disconnect()


if __name__ == "__main__":
    asyncio.run(main())

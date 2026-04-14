"""
Honest backtester runner.

Takes our current smart-money graph, looks at what they bought in the past
N days (without cherry-picking winners), and simulates forward outcomes.
Includes WINS AND LOSSES.

Run:
  cd backend
  python -m scripts.run_backtest
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.db import mongo
from app.services.backtester import run_honest_backtest

DAYS_BACK = 7              # look at entries from last 7 days
HOLD_BUFFER_HOURS = 24     # need at least 24h of forward prices


async def main() -> None:
    mongo.connect()
    if not await mongo.ping():
        print("[!] MongoDB unreachable.")
        return

    result = await run_honest_backtest(
        days_back=DAYS_BACK,
        hold_buffer_hours=HOLD_BUFFER_HOURS,
    )

    if result.get("error"):
        print(f"[!] {result['error']}")
        await mongo.disconnect()
        return

    s = result.get("summary") or {}
    print("\n" + "=" * 60)
    print("  HONEST BACKTEST — cluster firings over the past window")
    print("=" * 60)
    print(f"  window:              last {s.get('window_days', DAYS_BACK)} days")
    print(f"  tokens scanned:      {s.get('tokens_scanned', 0)}")
    print(f"  firings detected:    {s.get('firings_detected', 0)}")
    print(f"  evaluable:           {s.get('evaluable', 0)} (with price data)")
    print(f"  wins / losses:       {s.get('wins', 0)} / {s.get('losses', 0)}")
    print(f"  win rate:            {s.get('win_rate_pct', 0)}%")
    print(f"  avg P&L (realistic): {s.get('avg_pnl_pct', 0):+.2f}%")
    print(f"  best peak P&L:       {s.get('best_pnl_pct', 0):+.2f}%")
    print(f"  worst final P&L:     {s.get('worst_pnl_pct', 0):+.2f}%")
    print("=" * 60)

    await mongo.disconnect()


if __name__ == "__main__":
    asyncio.run(main())

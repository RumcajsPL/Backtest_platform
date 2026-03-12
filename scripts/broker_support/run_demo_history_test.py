#!/usr/bin/env python
"""
Empirical demo trade history test — Phase 0, Step 1 (MANDATORY).

Run this ONCE after bug fixes are applied.
Result determines the Step 3 enrichment architecture:

  RESULT A — trades returned:
    Demo trades appear in the real-account history endpoint.
    → Step 3: implement get_trade_details(position_id) in EToroClient,
      use it in TradeEnricher to fill exit_price + profit_loss after snapshot closure.

  RESULT B — empty list returned (or 403/404):
    Demo trades do NOT appear in the real-account history endpoint.
    → Step 3: use GET /market-data/instruments/rates at closure detection time
      as price approximation. PositionTracker snapshot approach is permanent.

Do NOT architect close-price enrichment before running this test.

Usage:
    python scripts/broker_support/run_demo_history_test.py
    python scripts/broker_support/run_demo_history_test.py --min-date 2026-01-01
"""
import argparse
import json
from datetime import datetime, timedelta

from loguru import logger

from src.broker_support.client.client import EToroClient


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Empirical test: do demo trades appear in real-account history?'
    )
    parser.add_argument(
        '--min-date',
        default=(datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d'),
        help='minDate param for history endpoint (YYYY-MM-DD). Default: 90 days ago.',
    )
    args = parser.parse_args()

    client = EToroClient()
    logger.info(f"Running empirical demo history test with minDate={args.min_date}")

    try:
        trades = client.fetch_closed_trades(
            from_date=datetime.strptime(args.min_date, '%Y-%m-%d')
        )
    except Exception as exc:
        logger.error(f"Request failed: {exc}")
        print(f"\n RESULT B — endpoint error: {exc}")
        print("  → Demo trades do NOT appear. PositionTracker snapshot approach is permanent.")
        print("  → Step 3: use /market-data/instruments/rates for exit price approximation.")
        return

    if not trades:
        print(f"\n RESULT B — endpoint returned empty list for minDate={args.min_date}")
        print("  → No demo trades found. PositionTracker snapshot approach is permanent.")
        print("  → Step 3: use /market-data/instruments/rates for exit price approximation.")
    else:
        print(f"\n RESULT A — {len(trades)} trade(s) returned!")
        print("  → Demo trades appear in real-account history endpoint.")
        print("  → Step 3: implement get_trade_details(position_id) for enrichment.")
        print("\n  Sample trade (first record):")
        print(json.dumps(trades[0], indent=4, default=str))

        # Show all field names to confirm schema matches Trade model aliases
        print("\n  Field names in response:")
        if isinstance(trades[0], dict):
            for k in sorted(trades[0].keys()):
                print(f"    {k}: {type(trades[0][k]).__name__}")


if __name__ == '__main__':
    main()
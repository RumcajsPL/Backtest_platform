#!/usr/bin/env python
"""
Manual one-shot position tracker.
Run this script directly to execute a single tracking cycle.

Usage:
    python scripts/broker_support/run_tracker.py
    python scripts/broker_support/run_tracker.py --journal outputs/broker_support/journal/trades.csv
    python scripts/broker_support/run_tracker.py --verbose
"""
import argparse
from pathlib import Path

from loguru import logger

from broker_support.tracking.position_tracker import PositionTracker


def main() -> None:
    parser = argparse.ArgumentParser(description='Run one eToro position tracking cycle.')
    parser.add_argument(
        '--journal',
        default='outputs/broker_support/journal/trades.csv',
        help='Path to closed-trades CSV journal.',
    )
    parser.add_argument(
        '--snapshots',
        default='outputs/broker_support/snapshots',
        help='Directory for position snapshots.',
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable DEBUG-level logging.',
    )
    args = parser.parse_args()

    if args.verbose:
        logger.remove()
        logger.add(lambda msg: print(msg, end=''), level='DEBUG')

    tracker = PositionTracker(
        journal_path=Path(args.journal),
        snapshots_dir=Path(args.snapshots),
    )

    new_trades = tracker.track()
    logger.info(f"Tracking complete — {new_trades} new closed trade(s) recorded.")

    df = tracker.journal.load_all()
    if not df.empty:
        logger.info(f"Journal total: {len(df)} trades.")


if __name__ == '__main__':
    main()
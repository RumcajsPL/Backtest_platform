#!/usr/bin/env python
"""
Script to run the position tracker manually or via scheduler.
"""
import argparse
from pathlib import Path
from datetime import datetime
from loguru import logger

from broker_support.tracker.position_tracker import PositionTracker


def main():
    parser = argparse.ArgumentParser(description='Track eToro positions')
    parser.add_argument('--journal', default='data/trading_journal.csv',
                       help='Path to journal CSV')
    parser.add_argument('--snapshots', default='data/snapshots',
                       help='Directory for snapshots')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Verbose output')
    
    args = parser.parse_args()
    
    # Configure logging
    if args.verbose:
        logger.remove()
        logger.add(lambda msg: print(msg), level="DEBUG")
    
    # Initialize tracker
    tracker = PositionTracker(
        journal_path=Path(args.journal),
        snapshots_dir=Path(args.snapshots)
    )
    
    # Run tracking
    new_trades = tracker.track()
    
    print(f"\n✅ Tracking complete. Recorded {new_trades} new closed trades.")
    
    # Show journal stats
    df = tracker.journal.load_all()
    if not df.empty:
        print(f"\n📊 Journal now has {len(df)} total trades")


if __name__ == "__main__":
    main()
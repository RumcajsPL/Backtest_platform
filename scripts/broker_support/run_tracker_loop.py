#!/usr/bin/env python
"""
Polling tracker loop — runs position tracking on a fixed interval during
DAX trading hours (08:00–22:00 CET/CEST).

Features:
  - Polls every POLL_INTERVAL_MINUTES (default 5) during trading hours.
  - Sleeps until next market open when outside trading hours.
  - Graceful shutdown on Ctrl-C or SIGTERM.
  - Structured log file at outputs/broker_support/logs/tracker.log.
  - --once flag for manual / cron single-cycle execution (same as run_tracker.py
    but with full logging setup and trading-hours awareness).

Usage:
    # Continuous loop (leave running in background / Task Scheduler)
    python scripts/broker_support/run_tracker_loop.py

    # Single cycle (cron / manual test)
    python scripts/broker_support/run_tracker_loop.py --once

    # Custom interval and paths
    python scripts/broker_support/run_tracker_loop.py --interval 10 --verbose

    # Ignore trading hours guard (runs 24/7 — useful for testing)
    python scripts/broker_support/run_tracker_loop.py --no-hours-guard
"""
import argparse
import signal
import sys
import time
from pathlib import Path

from loguru import logger

from src.broker_support.tracking.position_tracker import PositionTracker
from src.broker_support.utils.time_utils import is_trading_hours, seconds_until_open, now_berlin

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

POLL_INTERVAL_MINUTES: int = 5
DEFAULT_JOURNAL    = "outputs/broker_support/journal/trades.csv"
DEFAULT_SNAPSHOTS  = "outputs/broker_support/snapshots"
DEFAULT_LOG_DIR    = "outputs/broker_support/logs"
DEFAULT_INSTRUMENT_MAP = "configs/broker_support/instrument_map.yaml"

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

_shutdown_requested = False


def _handle_signal(signum, frame) -> None:
    global _shutdown_requested
    logger.info(f"Signal {signum} received — shutting down after current cycle.")
    _shutdown_requested = True


signal.signal(signal.SIGTERM, _handle_signal)
# SIGINT (Ctrl-C) is handled via KeyboardInterrupt in the main loop.

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _configure_logging(log_dir: str, verbose: bool) -> None:
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    level = "DEBUG" if verbose else "INFO"

    # Console sink
    logger.remove()
    logger.add(sys.stderr, level=level, colorize=True,
               format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                      "<level>{level: <8}</level> | {message}")

    # Rotating file sink
    logger.add(
        str(log_path / "tracker_{time:YYYY-MM-DD}.log"),
        level="DEBUG",
        rotation="00:00",      # new file each day
        retention="30 days",
        encoding="utf-8",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{line} | {message}",
    )

# ---------------------------------------------------------------------------
# Single cycle
# ---------------------------------------------------------------------------

def run_cycle(tracker: PositionTracker) -> int:
    """Execute one tracking cycle. Returns number of new trades written."""
    try:
        return tracker.track()
    except Exception as exc:
        logger.error(f"Cycle failed with unhandled exception: {exc}")
        return 0

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="eToro position tracker — polling loop with trading hours guard."
    )
    parser.add_argument(
        "--once", action="store_true",
        help="Run a single tracking cycle and exit.",
    )
    parser.add_argument(
        "--interval", type=int, default=POLL_INTERVAL_MINUTES, metavar="MINUTES",
        help=f"Poll interval in minutes (default: {POLL_INTERVAL_MINUTES}).",
    )
    parser.add_argument(
        "--journal", default=DEFAULT_JOURNAL,
        help="Path to closed-trades CSV journal.",
    )
    parser.add_argument(
        "--snapshots", default=DEFAULT_SNAPSHOTS,
        help="Directory for position snapshots.",
    )
    parser.add_argument(
        "--instrument-map", default=DEFAULT_INSTRUMENT_MAP,
        help="Path to instrument_map.yaml.",
    )
    parser.add_argument(
        "--log-dir", default=DEFAULT_LOG_DIR,
        help="Directory for log files.",
    )
    parser.add_argument(
        "--no-hours-guard", action="store_true",
        help="Disable trading hours guard — run 24/7 (useful for testing).",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable DEBUG-level console output.",
    )
    args = parser.parse_args()

    _configure_logging(args.log_dir, args.verbose)

    logger.info("=" * 60)
    logger.info("eToro Position Tracker starting")
    logger.info(f"  mode           : {'once' if args.once else 'loop'}")
    logger.info(f"  interval       : {args.interval} min")
    logger.info(f"  hours guard    : {'disabled' if args.no_hours_guard else 'enabled (08:00-22:00 CET)'}")
    logger.info(f"  journal        : {args.journal}")
    logger.info(f"  snapshots      : {args.snapshots}")
    logger.info(f"  instrument map : {args.instrument_map}")
    logger.info("=" * 60)

    tracker = PositionTracker(
        journal_path=Path(args.journal),
        snapshots_dir=Path(args.snapshots),
        instrument_map_path=Path(args.instrument_map),
    )

    poll_seconds = args.interval * 60

    # ------------------------------------------------------------------
    # Single-cycle mode (--once)
    # ------------------------------------------------------------------
    if args.once:
        if not args.no_hours_guard and not is_trading_hours():
            berlin_now = now_berlin().strftime("%H:%M %Z")
            logger.warning(
                f"Outside trading hours ({berlin_now}). "
                f"Use --no-hours-guard to force execution."
            )
            sys.exit(0)

        new_trades = run_cycle(tracker)
        df = tracker.journal.load_all()
        logger.info(f"Cycle complete — {new_trades} new trade(s). "
                    f"Journal total: {len(df)}.")
        sys.exit(0)

    # ------------------------------------------------------------------
    # Continuous loop
    # ------------------------------------------------------------------
    logger.info("Entering polling loop. Press Ctrl-C to stop.")
    total_cycles = 0
    total_trades = 0

    try:
        while not _shutdown_requested:

            # Trading hours check
            if not args.no_hours_guard and not is_trading_hours():
                sleep_sec = seconds_until_open()
                berlin_now = now_berlin().strftime("%H:%M %Z")
                logger.info(
                    f"Outside trading hours ({berlin_now}). "
                    f"Sleeping {sleep_sec // 3600}h "
                    f"{(sleep_sec % 3600) // 60}m until market open."
                )
                # Sleep in short chunks so SIGTERM / Ctrl-C is responsive
                _interruptible_sleep(sleep_sec)
                continue

            # Run cycle
            berlin_now = now_berlin().strftime("%H:%M %Z")
            logger.info(f"--- Cycle {total_cycles + 1} at {berlin_now} ---")
            new_trades = run_cycle(tracker)
            total_cycles += 1
            total_trades += new_trades

            df = tracker.journal.load_all()
            logger.info(
                f"Cycle {total_cycles} complete — {new_trades} new trade(s). "
                f"Journal total: {len(df)}."
            )

            if _shutdown_requested:
                break

            logger.info(f"Next cycle in {args.interval} minute(s).")
            _interruptible_sleep(poll_seconds)

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received.")

    logger.info(
        f"Tracker stopped. Total cycles: {total_cycles}, "
        f"total trades recorded: {total_trades}."
    )


def _interruptible_sleep(seconds: int) -> None:
    """
    Sleep for `seconds` total, waking every 5s to check for shutdown signal.
    Ensures Ctrl-C and SIGTERM are handled promptly.
    """
    elapsed = 0
    chunk = 5
    while elapsed < seconds and not _shutdown_requested:
        time.sleep(min(chunk, seconds - elapsed))
        elapsed += chunk


if __name__ == "__main__":
    main()
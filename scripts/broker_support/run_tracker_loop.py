#!/usr/bin/env python
"""
run_tracker_loop.py — polling tracker loop with instance isolation.

Tracks open positions and journals closed trades to the instance-scoped
journal directory. Runs during DAX trading hours (08:00–22:00 CET/CEST).

Instance isolation:
  --instance <id> scopes all paths to the instance's journal directory:
    Journal  : outputs/broker_support/journal/<id>/trades.csv
    Log file : outputs/broker_support/logs/tracker_<id>_YYYY-MM-DD.log
  Without --instance, falls back to the legacy root paths for backward
  compatibility (outputs/broker_support/journal/trades.csv).

Features:
  - Polls every POLL_INTERVAL_MINUTES (default 5) during trading hours.
  - Sleeps until next market open when outside trading hours.
  - Graceful shutdown on Ctrl-C or SIGTERM.
  - Structured log file per instance, rotated daily.
  - --once flag for manual / cron single-cycle execution.

Usage:
    # Continuous loop — named instance (recommended)
    python scripts/broker_support/run_tracker_loop.py --instance c424
    python scripts/broker_support/run_tracker_loop.py --instance 240166

    # Single cycle
    python scripts/broker_support/run_tracker_loop.py --instance 240166 --once

    # Legacy root path (no instance)
    python scripts/broker_support/run_tracker_loop.py

    # Ignore trading hours guard (testing)
    python scripts/broker_support/run_tracker_loop.py --instance 240166 --no-hours-guard

Kill switch:
    The tracker loop does not consume kill switch files — it is a read-only
    observer. Stop it with Ctrl-C or SIGTERM.
"""
import argparse
import signal
import sys
import time
from pathlib import Path

from loguru import logger

# ---------------------------------------------------------------------------
# Path bootstrap
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.broker_support.tracking.position_tracker import PositionTracker
from src.broker_support.utils.time_utils import is_trading_hours, seconds_until_open, now_berlin

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

POLL_INTERVAL_MINUTES: int = 5
JOURNAL_BASE_DIR   = "outputs/broker_support/journal"
DEFAULT_SNAPSHOTS  = "outputs/broker_support/snapshots"
DEFAULT_LOG_DIR    = "outputs/broker_support/logs"
DEFAULT_INSTRUMENT_MAP = "configs/broker_support/instrument_map.yaml"

# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------

def _resolve_journal_path(instance_id: str | None) -> Path:
    """
    Resolve instance-scoped journal path.

    With instance_id : outputs/broker_support/journal/<id>/trades.csv
    Without          : outputs/broker_support/journal/trades.csv  (legacy)
    """
    base = _PROJECT_ROOT / JOURNAL_BASE_DIR
    if instance_id:
        return base / instance_id / "trades.csv"
    return base / "trades.csv"


def _resolve_log_filename(instance_id: str | None) -> str:
    """
    Return loguru log filename pattern for this instance.

    With instance_id : tracker_<id>_{time:YYYY-MM-DD}.log
    Without          : tracker_{time:YYYY-MM-DD}.log  (legacy)
    """
    if instance_id:
        return f"tracker_{instance_id}_{{time:YYYY-MM-DD}}.log"
    return "tracker_{time:YYYY-MM-DD}.log"

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

_shutdown_requested = False


def _handle_signal(signum, frame) -> None:
    global _shutdown_requested
    logger.info(f"Signal {signum} received — shutting down after current cycle.")
    _shutdown_requested = True


signal.signal(signal.SIGTERM, _handle_signal)

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _configure_logging(log_dir: str, instance_id: str | None, verbose: bool) -> None:
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    level = "DEBUG" if verbose else "INFO"
    log_filename = _resolve_log_filename(instance_id)

    logger.remove()
    logger.add(
        sys.stderr,
        level=level,
        colorize=True,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
               "<level>{level: <8}</level> | {message}",
    )
    logger.add(
        str(log_path / log_filename),
        level="DEBUG",
        rotation="00:00",
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
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="eToro position tracker — polling loop with instance isolation."
    )
    parser.add_argument(
        "--instance", "-i",
        default=None,
        help=(
            "Instance ID for parallel loop isolation (e.g. c424, 240166). "
            "Scopes journal path and log filename to the instance directory. "
            "Omit only for legacy single-loop operation."
        ),
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

    instance_id: str | None = args.instance
    instance_label = instance_id or "default"
    journal_path = _resolve_journal_path(instance_id)

    _configure_logging(args.log_dir, instance_id, args.verbose)

    logger.info("=" * 60)
    logger.info(f"eToro Position Tracker — instance [{instance_label}]")
    logger.info(f"  mode           : {'once' if args.once else 'loop'}")
    logger.info(f"  interval       : {args.interval} min")
    logger.info(f"  hours guard    : {'disabled' if args.no_hours_guard else 'enabled (08:00-22:00 CET)'}")
    logger.info(f"  journal        : {journal_path}")
    logger.info(f"  snapshots      : {args.snapshots}")
    logger.info(f"  instrument map : {args.instrument_map}")
    logger.info("=" * 60)

    tracker = PositionTracker(
        journal_path=journal_path,
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
        logger.info(
            f"[{instance_label}] Cycle complete — {new_trades} new trade(s). "
            f"Journal total: {len(df)}."
        )
        sys.exit(0)

    # ------------------------------------------------------------------
    # Continuous loop
    # ------------------------------------------------------------------
    logger.info("Entering polling loop. Press Ctrl-C to stop.")
    total_cycles = 0
    total_trades = 0

    try:
        while not _shutdown_requested:

            if not args.no_hours_guard and not is_trading_hours():
                sleep_sec = seconds_until_open()
                berlin_now = now_berlin().strftime("%H:%M %Z")
                logger.info(
                    f"[{instance_label}] Outside trading hours ({berlin_now}). "
                    f"Sleeping {sleep_sec // 3600}h "
                    f"{(sleep_sec % 3600) // 60}m until market open."
                )
                _interruptible_sleep(sleep_sec)
                continue

            berlin_now = now_berlin().strftime("%H:%M %Z")
            logger.info(f"── [{instance_label}] Cycle {total_cycles + 1} at {berlin_now} ──")
            new_trades = run_cycle(tracker)
            total_cycles += 1
            total_trades += new_trades

            df = tracker.journal.load_all()
            logger.info(
                f"[{instance_label}] Cycle {total_cycles} complete — "
                f"{new_trades} new trade(s). Journal total: {len(df)}."
            )

            if _shutdown_requested:
                break

            logger.info(f"[{instance_label}] Next cycle in {args.interval} minute(s).")
            _interruptible_sleep(poll_seconds)

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received.")

    logger.info(
        f"[{instance_label}] Tracker stopped. "
        f"Total cycles: {total_cycles}, total trades recorded: {total_trades}."
    )


def _interruptible_sleep(seconds: int) -> None:
    elapsed = 0
    chunk = 5
    while elapsed < seconds and not _shutdown_requested:
        time.sleep(min(chunk, seconds - elapsed))
        elapsed += chunk


if __name__ == "__main__":
    main()
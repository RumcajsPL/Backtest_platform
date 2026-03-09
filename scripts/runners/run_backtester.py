"""
scripts/runners/run_backtester.py
----------------------------------
Thin runner for the backtesting & optimization pipeline.

Usage (from project root, with venv active):
    python scripts/runners/run_backtester.py
    python scripts/runners/run_backtester.py --config path/to/other_config.yaml
    python scripts/runners/run_backtester.py --config configs/backtesting/backtest_template.yaml --log-level DEBUG
    python scripts/runners/run_backtester.py --no-clean          # skip pre-run clean
    python scripts/runners/run_backtester.py --clean-db          # also delete backtester.db (fresh start)

Pre-run cleaning (B9O-002):
    By default, the runner cleans stale artifacts before every pipeline run:
      - ~/.wbws_data_cache/*.pkl  (data cache — avoids OOM from stale full-file pkls)
      - temp/backtesting/*.yaml   (temp candidate YAMLs — avoids NoneType parse errors)
    The database (backtester.db) is NOT cleaned by default.
    Pass --clean-db for a fully fresh start (deletes all run history).
    Pass --no-clean to skip all cleaning (e.g. resuming a checkpointed run).

Logging:
    Writes to both console (stdout) and a dated log file in the run output dir.
    Log file path: <output_dir>/pipeline_<YYYYMMDD_HHMMSS>.log
    The output_dir is read from backtest_template.yaml (run.output_dir).

Exit codes:
    0 — pipeline completed (checkpoint = COMPLETE)
    1 — pipeline failed with exception
    2 — invalid arguments or config not found
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path


# ── Ensure project root is on sys.path ────────────────────────────────────────
# Works when invoked as:
#   python scripts/runners/run_backtester.py   (from project root)
#   python -m scripts.runners.run_backtester   (as module)
_SCRIPT_DIR = Path(__file__).resolve()
_PROJECT_ROOT = _SCRIPT_DIR.parents[2]   # scripts/runners/run_backtester.py → up 2 → project root
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def _setup_logging(log_level: str, output_dir: Path) -> Path:
    """
    Configure root logger with console + file handlers.
    Returns the path to the log file created.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = output_dir / f"pipeline_{timestamp}.log"

    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    fmt = logging.Formatter(
        fmt="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(numeric_level)
    ch.setFormatter(fmt)
    root_logger.addHandler(ch)

    # File handler
    fh = logging.FileHandler(str(log_file), encoding="utf-8")
    fh.setLevel(numeric_level)
    fh.setFormatter(fmt)
    root_logger.addHandler(fh)

    return log_file


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the backtesting & optimization pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/backtesting/backtest_1st_run.yaml"),
        help="Path to backtest config YAML (default: configs/backtesting/backtest_template.yaml)",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default=None,
        help="Override log level from config (default: use config run.log_level)",
    )
    parser.add_argument(
        "--no-clean",
        action="store_true",
        default=False,
        help=(
            "Skip pre-run cleaning of data cache and temp YAMLs. "
            "Use when resuming a checkpointed run to avoid clearing state."
        ),
    )
    parser.add_argument(
        "--clean-db",
        action="store_true",
        default=False,
        help=(
            "Also delete backtester.db before running — completely fresh start. "
            "WARNING: permanently deletes all run history."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    config_path = args.config.resolve()

    if not config_path.exists():
        # Minimal logging before full setup — just stderr
        print(f"ERROR: Config file not found: {config_path}", file=sys.stderr)
        return 2

    # ── Read output_dir, temp_dir, and log_level from config ─────────────────
    import yaml
    try:
        with open(config_path, encoding="utf-8") as f:
            raw_config = yaml.safe_load(f)
    except Exception as exc:
        print(f"ERROR: Failed to parse config YAML: {exc}", file=sys.stderr)
        return 2

    run_cfg = raw_config.get("run", {})
    output_dir = Path(run_cfg.get("output_dir", "outputs/backtesting"))
    temp_dir = Path(run_cfg.get("temp_dir", "temp/backtesting"))
    config_log_level = run_cfg.get("log_level", "INFO")
    log_level = args.log_level or config_log_level

    log_file = _setup_logging(log_level, output_dir)
    logger = logging.getLogger(__name__)

    logger.info("=" * 70)
    logger.info("Backtesting & Optimization Pipeline")
    logger.info("Config:   %s", config_path)
    logger.info("Log file: %s", log_file)
    logger.info("=" * 70)

    # ── B9O-002: Pre-run environment clean ────────────────────────────────────
    # Clears stale data cache pkl files and leftover temp YAMLs before each run.
    # This prevents OOM crashes (stale full-size pkls) and YAML NoneType errors
    # (truncated temp YAMLs from crashed workers in previous runs).
    # Skipped when --no-clean is passed (e.g. resuming a checkpointed run).
    if args.no_clean:
        logger.info("Pre-run clean skipped (--no-clean flag set)")
    else:
        if args.clean_db:
            logger.warning(
                "Pre-run clean: --clean-db set — backtester.db will be deleted. "
                "All run history will be lost."
            )
        try:
            from src.utils.run_cleaner import clean_environment
            clean_stats = clean_environment(
                clean_cache=True,
                clean_temp=True,
                clean_db=args.clean_db,
                temp_dir=temp_dir,
                output_dir=output_dir,
            )
            if clean_stats["errors"]:
                logger.warning(
                    "Pre-run clean completed with %d error(s) — "
                    "locked files were skipped. Pipeline will continue.",
                    len(clean_stats["errors"]),
                )
        except Exception as clean_exc:
            # Clean failure must NEVER block the pipeline
            logger.warning(
                "Pre-run clean raised an unexpected error: %s — "
                "pipeline will continue without cleaning.",
                clean_exc,
            )

    # ── Import and run ────────────────────────────────────────────────────────
    try:
        from src.backtesting.orchestrator import run as run_pipeline
    except ImportError as exc:
        logger.error("Failed to import orchestrator: %s", exc)
        logger.error("Ensure you are running from the project root with venv active.")
        return 2

    t_start = time.perf_counter()
    try:
        run_pipeline(config_path)
        elapsed = time.perf_counter() - t_start
        logger.info("=" * 70)
        logger.info("Pipeline completed successfully in %.1f seconds", elapsed)
        logger.info("=" * 70)
        return 0

    except KeyboardInterrupt:
        elapsed = time.perf_counter() - t_start
        logger.warning("Pipeline interrupted by user after %.1f seconds", elapsed)
        logger.warning("Run can be resumed — checkpoint is preserved in the database.")
        return 1

    except Exception as exc:
        elapsed = time.perf_counter() - t_start
        logger.error("=" * 70)
        logger.error("Pipeline FAILED after %.1f seconds", elapsed)
        logger.error("Error: %s", exc, exc_info=True)
        logger.error("=" * 70)
        logger.error("To resume from last checkpoint: re-run with --no-clean --config <path>")
        logger.error("To start fresh: re-run with --clean-db")
        return 1


if __name__ == "__main__":
    sys.exit(main())
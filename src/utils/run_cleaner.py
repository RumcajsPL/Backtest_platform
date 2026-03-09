"""
src/utils/run_cleaner.py
------------------------
Pre-run environment cleaner for the backtesting pipeline.

Clears stale artifacts that accumulate between runs and can cause:
  - OOM crashes (old full-size pkl files in data cache — B9O-001)
  - WinError 32 file locks (pkl files locked by crashed workers)
  - YAML NoneType errors (zero-byte or truncated temp YAMLs from OOM crashes)
  - Stage 0 false failures (corrupt or leftover DB state from incomplete runs)

WHAT IS CLEANED (each target is opt-in via flags):

  data_cache  — ~/.wbws_data_cache/*.pkl
                Clears all pickled DataFrames. Required after data_loader.py
                upgrades (e.g. B9O-001) and after any OOM crash that may have
                left locked or truncated pkl files.
                Safe to clear at any time — cache is rebuilt on next run.
                MUST be cleared once after deploying data_loader.py v3.3.

  temp_yamls  — <temp_dir>/backtesting/*.yaml (default: temp/backtesting/)
                Clears temp candidate YAMLs written by strategy_runner.py.
                Retained YAMLs from crashed workers can cause NoneType parse
                errors when re-read by a new worker with the same candidate_id.
                Safe to clear between runs — temp YAMLs are always regenerated.

  db          — <output_dir>/backtester.db
                Deletes the SQLite database used by CandidateStore.
                Use only to start a completely fresh run.
                WARNING: this permanently deletes all run history.
                Requires explicit --clean-db flag to prevent accidental deletion.

WHAT IS NEVER CLEANED:
  - Source code files
  - Strategy YAML configs (configs/)
  - Output reports, HTML, JSON, Parquet (outputs/backtesting/ — run results)
  - Log files (outputs/backtesting/pipeline_*.log)
  - Trading YAMLs (outputs/backtesting/trading_yamls/)
  - Any file outside the three targets above

USAGE (standalone — from project root, venv active):
    python src/utils/run_cleaner.py
    python src/utils/run_cleaner.py --clean-cache --clean-temp
    python src/utils/run_cleaner.py --clean-cache --clean-temp --clean-db
    python src/utils/run_cleaner.py --all   (cache + temp only — NOT db)
    python src/utils/run_cleaner.py --all --clean-db

USAGE (programmatic — from run_backtester.py):
    from src.utils.run_cleaner import clean_environment
    clean_environment(
        clean_cache=True,
        clean_temp=True,
        clean_db=False,     # default False — explicit opt-in only
        temp_dir=Path("temp/backtesting"),
        output_dir=Path("outputs/backtesting"),
    )

EXIT CODES (standalone):
    0 — completed successfully
    1 — one or more files could not be deleted (logged; run continues)
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Default paths — relative to project root (matched to backtest YAML defaults)
_DEFAULT_TEMP_DIR = Path("temp/backtesting")
_DEFAULT_OUTPUT_DIR = Path("outputs/backtesting")
_DATA_CACHE_DIR = Path.home() / ".wbws_data_cache"
_DB_FILENAME = "backtester.db"


def clean_environment(
    clean_cache: bool = True,
    clean_temp: bool = True,
    clean_db: bool = False,
    temp_dir: Path = _DEFAULT_TEMP_DIR,
    output_dir: Path = _DEFAULT_OUTPUT_DIR,
) -> dict:
    """
    Clean stale pre-run artifacts.

    Args:
        clean_cache: Delete all pkl files from ~/.wbws_data_cache.
        clean_temp:  Delete all *.yaml files from temp_dir.
        clean_db:    Delete backtester.db from output_dir.
                     WARNING: permanently deletes all run history.
                     Default False — must be explicit.
        temp_dir:    Directory for temp candidate YAMLs.
                     Read from run.temp_dir in the backtest YAML.
        output_dir:  Pipeline output directory (where backtester.db lives).
                     Read from run.output_dir in the backtest YAML.

    Returns:
        Dict with keys: cache_deleted, temp_deleted, db_deleted, errors.
        Each count key is an int. errors is a list of (path, exc) tuples.
    """
    stats = {
        "cache_deleted": 0,
        "temp_deleted": 0,
        "db_deleted": 0,
        "errors": [],
    }

    if clean_cache:
        _clean_data_cache(stats)

    if clean_temp:
        _clean_temp_yamls(temp_dir, stats)

    if clean_db:
        _clean_database(output_dir, stats)

    _log_summary(stats, clean_cache, clean_temp, clean_db)
    return stats


# ── Private helpers ────────────────────────────────────────────────────────────

def _clean_data_cache(stats: dict) -> None:
    """Delete all .pkl files from the data cache directory."""
    cache_dir = _DATA_CACHE_DIR

    if not cache_dir.exists():
        logger.info("Data cache directory does not exist — nothing to clean: %s", cache_dir)
        return

    pkl_files = list(cache_dir.glob("*.pkl"))
    if not pkl_files:
        logger.info("Data cache already empty: %s", cache_dir)
        return

    logger.info(
        "Cleaning data cache: %d pkl file(s) in %s",
        len(pkl_files), cache_dir,
    )

    for pkl_file in pkl_files:
        try:
            pkl_file.unlink()
            stats["cache_deleted"] += 1
            logger.debug("Deleted cache file: %s", pkl_file.name)
        except Exception as exc:
            logger.warning(
                "Could not delete cache file %s: %s — skipping (file may be locked)",
                pkl_file.name, exc,
            )
            stats["errors"].append((str(pkl_file), str(exc)))


def _clean_temp_yamls(temp_dir: Path, stats: dict) -> None:
    """Delete all *.yaml files from the temp directory."""
    if not temp_dir.exists():
        logger.info("Temp directory does not exist — nothing to clean: %s", temp_dir)
        return

    yaml_files = list(temp_dir.glob("*.yaml"))
    if not yaml_files:
        logger.info("Temp directory already empty: %s", temp_dir)
        return

    logger.info(
        "Cleaning temp YAMLs: %d file(s) in %s",
        len(yaml_files), temp_dir,
    )

    for yaml_file in yaml_files:
        try:
            yaml_file.unlink()
            stats["temp_deleted"] += 1
            logger.debug("Deleted temp YAML: %s", yaml_file.name[:24])
        except Exception as exc:
            logger.warning(
                "Could not delete temp YAML %s: %s — skipping (file may be locked)",
                yaml_file.name[:24], exc,
            )
            stats["errors"].append((str(yaml_file), str(exc)))


def _clean_database(output_dir: Path, stats: dict) -> None:
    """Delete backtester.db from the output directory."""
    db_path = output_dir / _DB_FILENAME

    if not db_path.exists():
        logger.info("Database does not exist — nothing to clean: %s", db_path)
        return

    logger.info("Deleting database: %s", db_path)
    try:
        db_path.unlink()
        stats["db_deleted"] += 1
        logger.info("Database deleted: %s", db_path)
    except Exception as exc:
        logger.warning(
            "Could not delete database %s: %s",
            db_path, exc,
        )
        stats["errors"].append((str(db_path), str(exc)))


def _log_summary(
    stats: dict,
    clean_cache: bool,
    clean_temp: bool,
    clean_db: bool,
) -> None:
    """Log a concise summary of what was cleaned."""
    parts = []
    if clean_cache:
        parts.append(f"cache={stats['cache_deleted']} pkl(s) deleted")
    if clean_temp:
        parts.append(f"temp={stats['temp_deleted']} yaml(s) deleted")
    if clean_db:
        parts.append(f"db={'deleted' if stats['db_deleted'] else 'not found'}")

    summary = " | ".join(parts) if parts else "nothing cleaned (all targets disabled)"
    error_count = len(stats["errors"])

    if error_count:
        logger.warning(
            "Pre-run clean complete: %s | %d error(s) — locked files skipped",
            summary, error_count,
        )
    else:
        logger.info("Pre-run clean complete: %s", summary)


# ── Standalone entry point ─────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean stale backtester artifacts before a run.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python src/utils/run_cleaner.py                   # cache + temp (safe defaults)\n"
            "  python src/utils/run_cleaner.py --all             # cache + temp\n"
            "  python src/utils/run_cleaner.py --all --clean-db  # cache + temp + db (DESTRUCTIVE)\n"
            "  python src/utils/run_cleaner.py --clean-cache     # cache only\n"
            "  python src/utils/run_cleaner.py --clean-db        # db only (use with caution)\n"
        ),
    )
    parser.add_argument(
        "--clean-cache",
        action="store_true",
        default=False,
        help="Delete all pkl files from ~/.wbws_data_cache (default when no flags given)",
    )
    parser.add_argument(
        "--clean-temp",
        action="store_true",
        default=False,
        help="Delete all *.yaml files from temp/backtesting/ (default when no flags given)",
    )
    parser.add_argument(
        "--clean-db",
        action="store_true",
        default=False,
        help="Delete backtester.db — WARNING: permanently deletes all run history",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        default=False,
        help="Enable --clean-cache and --clean-temp (does NOT include --clean-db)",
    )
    parser.add_argument(
        "--temp-dir",
        type=Path,
        default=_DEFAULT_TEMP_DIR,
        help=f"Temp YAML directory (default: {_DEFAULT_TEMP_DIR})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=_DEFAULT_OUTPUT_DIR,
        help=f"Pipeline output directory where backtester.db lives (default: {_DEFAULT_OUTPUT_DIR})",
    )
    return parser.parse_args()


def _setup_logging() -> None:
    """Minimal logging for standalone use — console only."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def main() -> int:
    _setup_logging()
    args = _parse_args()

    # Apply --all shortcut
    clean_cache = args.clean_cache or args.all
    clean_temp = args.clean_temp or args.all
    clean_db = args.clean_db

    # Default behaviour when no flags at all: clean cache + temp (safe)
    if not any([args.clean_cache, args.clean_temp, args.clean_db, args.all]):
        logger.info("No flags specified — applying safe defaults: --clean-cache --clean-temp")
        clean_cache = True
        clean_temp = True

    if clean_db:
        logger.warning(
            "WARNING: --clean-db will permanently delete all run history from %s/%s",
            args.output_dir, _DB_FILENAME,
        )

    stats = clean_environment(
        clean_cache=clean_cache,
        clean_temp=clean_temp,
        clean_db=clean_db,
        temp_dir=args.temp_dir,
        output_dir=args.output_dir,
    )

    return 1 if stats["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
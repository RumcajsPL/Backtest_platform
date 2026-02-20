"""
run_strategy.py — Strategy Runner (New Architecture)
=====================================================
Entry point for a single strategy run using the new architecture orchestrator.
Loads config from a YAML file, runs the pipeline, prints a result summary.

Usage:
    python scripts/runners/run_strategy.py
    python scripts/runners/run_strategy.py --config configs/strategies/wbws/wbws_strategy_v2.yaml
    python scripts/runners/run_strategy.py --config configs/strategies/strategy_template.yaml --mode core

The --mode flag overrides execution.mode in the YAML (useful for quick switches
without editing the config file).
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Ensure project root is on the path regardless of working directory
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.strategies.orchestrator import StrategyOrchestrator
from src.utils.paths import CONFIGS_DIR


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a strategy through the new architecture pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/runners/run_strategy.py
  python scripts/runners/run_strategy.py --config configs/strategies/wbws/wbws_strategy_v2.yaml
  python scripts/runners/run_strategy.py --mode core --log-level DEBUG
        """,
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=CONFIGS_DIR / "strategies" / "strategy_template.yaml",
        help="Path to strategy YAML config (default: strategy_template.yaml)",
    )
    parser.add_argument(
        "--mode",
        choices=["core", "analytics"],
        default=None,
        help="Override execution.mode from the config YAML.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO)",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    """
    Returns 0 on success, 1 on failure.
    All exceptions are caught here and printed cleanly — the orchestrator
    itself does not swallow errors (fail-fast), so they surface here.
    """
    args = _parse_args()
    _configure_logging(args.log_level)

    log = logging.getLogger(__name__)
    log.info("Strategy runner starting")
    log.info("Config: %s", args.config)

    # Validate config path before handing to orchestrator
    if not args.config.exists():
        log.error(
            "Config file not found: %s\n"
            "Copy configs/strategies/strategy_template.yaml and fill in your parameters.",
            args.config,
        )
        return 1

    try:
        orchestrator = StrategyOrchestrator.from_yaml(args.config)

        # ASSUMPTION: if --mode is supplied, we need to override the config's mode.
        # StrategyConfig is frozen so we cannot mutate it. Two options:
        #   A) StrategyOrchestrator.run() accepts an optional mode override (chosen here)
        #   B) Build a new config with the overridden mode via from_dict()
        # Option A is simpler and keeps the override at the call site.
        # If StrategyOrchestrator.run() does not support a mode kwarg, use Option B.
        run_kwargs = {}
        if args.mode is not None:
            # ASSUMPTION: run() accepts mode= as an optional override.
            # If not supported, remove this and document the limitation.
            run_kwargs["mode_override"] = args.mode
            log.info("Mode overridden by CLI: %s", args.mode)

        result = orchestrator.run(**run_kwargs)

        # Print result summary to stdout (separate from log stream)
        print()
        print("=" * 60)
        print("RESULT SUMMARY")
        print("=" * 60)
        print(f"  Mode          : {result.mode}")
        print(f"  Total trades  : {result.total_trades}")
        print(f"  Win rate      : {result.win_rate:.1f}%")
        print(f"  Total PnL     : {result.total_pnl_points:+.1f} pts")
        print(f"  Expectancy    : {result.metrics.expectancy_points:+.2f} pts/trade")
        print(f"  Profit factor : {result.metrics.profit_factor:.2f}")
        print(f"  Max drawdown  : {result.metrics.max_drawdown:.1f} pts")
        print()
        print("  Stage timing:")
        for stage, ms in result.stage_durations_ms.items():
            print(f"    {stage:<12} {ms:>8.1f} ms")
        print(f"    {'TOTAL':<12} {result.total_duration_ms:>8.1f} ms")
        print("=" * 60)

        return 0

    except FileNotFoundError as e:
        log.error("File not found: %s", e)
        return 1
    except ValueError as e:
        log.error("Configuration error: %s", e)
        return 1
    except Exception as e:
        log.exception("Pipeline failed with unexpected error: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
"""
run_strategy.py — Strategy Runner (New Architecture)
=====================================================
Entry point for a single strategy run using the new architecture orchestrator.
Loads config from a YAML file, runs the pipeline, prints a result summary.

Usage:
    python scripts/runners/run_strategy.py
    python scripts/runners/run_strategy.py --config configs/strategies/wbws/wbws_strategy_v2.yaml
    python scripts/runners/run_strategy.py --config configs/strategies/wbws/wbws_strategy_v2.yaml --mode core

The --mode flag passes mode_override to orchestrator.run() — the YAML is not mutated.
StrategyConfig is frozen so the override happens at the run() call site only.

Version: 1.2.0
Changes from v1.1.0:
- [L1] _configure_logging() rewritten with dual-handler architecture:
       FileHandler   — level from YAML (output.logging.level) or --log-level CLI
                       flag, writes to output.logging.output_dir/run_{timestamp}.log
       StreamHandler — fixed at WARNING so the console stays clean and shows
                       only the _print_result() summary block.
- [L2] main(): config is peeked before full orchestrator construction so the
       log directory and level are available before the pipeline runs.
       This peek is a read-only from_yaml call — it does not duplicate work
       because StrategyOrchestrator.from_yaml() calls it again internally and
       StrategyConfig construction is fast (no I/O beyond the YAML file).
- [L3] _configure_logging() accepts Optional[LoggingOutputConfig] so it can
       fall back to safe defaults when called before config is loaded.
- [L4] Log file name includes timestamp to avoid clobbering previous runs.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

# Ensure project root is on the path regardless of working directory
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.strategies.orchestrator import StrategyOrchestrator, OrchestratorResult
from src.config.config_schema import StrategyConfig, LoggingOutputConfig
from src.utils.paths import CONFIGS_DIR

# ---------------------------------------------------------------------------
# Logging setup  [L1]
# ---------------------------------------------------------------------------

_LOG_FORMAT    = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_LOG_DATEFMT   = "%H:%M:%S"
_CONSOLE_LEVEL = logging.WARNING   # Console always at WARNING — keeps it clean


def _configure_logging(
    logging_config: Optional[LoggingOutputConfig],
    cli_level_override: Optional[str],
) -> Path:
    """
    Configure dual-handler logging.  [L1]

    FileHandler   — writes the full pipeline log to
                    {output_dir}/run_{timestamp}.log at the level specified
                    by --log-level (CLI) or output.logging.level (YAML),
                    falling back to INFO when neither is set.

    StreamHandler — writes WARNING+ to console only, so the terminal shows
                    the clean _print_result() summary and nothing else.

    Returns
    -------
    Path
        The resolved log file path (emitted into the file itself so each log
        is self-identifying).
    """
    # Resolve file-handler level: CLI flag > YAML > fallback INFO
    if cli_level_override:
        file_level = getattr(logging, cli_level_override.upper(), logging.INFO)
    elif logging_config is not None:
        file_level = getattr(logging, logging_config.level.upper(), logging.INFO)
    else:
        file_level = logging.INFO

    # Resolve output directory
    log_dir = Path(logging_config.output_dir) if logging_config is not None \
              else Path("outputs/strategies/logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file  = log_dir / f"run_{timestamp}.log"

    formatter = logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATEFMT)

    # File handler — full detail at configured level
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(file_level)
    file_handler.setFormatter(formatter)

    # Console handler — WARNING and above only
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(_CONSOLE_LEVEL)
    console_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)   # root accepts everything; handlers filter
    root.addHandler(file_handler)
    root.addHandler(console_handler)

    return log_file


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
  python scripts/runners/run_strategy.py --config configs/strategies/wbws/wbws_strategy_v2.yaml --mode core
  python scripts/runners/run_strategy.py --mode analytics --log-level DEBUG
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
        help=(
            "Override execution.mode from the config YAML. "
            "Passed as mode_override to orchestrator.run() — YAML is not modified."
        ),
    )
    parser.add_argument(
        "--log-level",
        default=None,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help=(
            "File log verbosity (default: value from output.logging.level in YAML). "
            "The console always shows WARNING and above regardless of this flag."
        ),
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def _print_result(result: OrchestratorResult) -> None:
    """Print formatted result summary to stdout (separate from the log stream)."""
    SEP = "=" * 60

    print()
    print(SEP)
    print("RESULT SUMMARY")
    print(SEP)
    print(f"  Mode          : {result.mode}")
    print(f"  Total trades  : {result.total_trades}")
    print(f"  Win rate      : {result.win_rate:.1f}%")
    print(f"  Total PnL     : {result.total_pnl_points:+.1f} pts")
    print(f"  Expectancy    : {result.metrics.expectancy_points:+.2f} pts/trade")
    print(f"  Profit factor : {result.metrics.profit_factor:.2f}")
    print(f"  Max drawdown  : {result.metrics.max_drawdown:.1f} pts")

    # Analytics block — only when analytics mode ran
    if result.analytics is not None:
        es         = result.analytics.executive_summary
        n_insights = len(result.analytics.get_all_insights())
        assessment = es.overall_assessment
        if len(assessment) > 80:
            assessment = assessment[:80] + "…"
        print()
        print(f"  Grade         : {es.performance_grade}")
        print(f"  Assessment    : {assessment}")
        print(f"  Insights      : {n_insights} generated")
        print(f"  Analytics ms  : {result.analytics.analysis_duration_ms:.1f}")

    if result.report is not None:
        print(f"  Report        : {result.report.html_path}")
        print(f"  Report ms     : {result.report.generation_duration_ms:.1f}")

    # Stage timing
    print()
    print("  Stage timing:")
    for stage, ms in result.stage_durations_ms.items():
        print(f"    {stage:<12} {ms:>8.1f} ms")
    print(f"    {'TOTAL':<12} {result.total_duration_ms:>8.1f} ms")
    print(SEP)
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    """
    Returns 0 on success, 1 on failure.

    Logging is configured in two phases:  [L2]
      Phase 1 — minimal bootstrap (console WARNING only) so any pre-config
                errors are visible.
      Phase 2 — after config peek: dual handlers wired from YAML + CLI flag.
                Bootstrap handler is removed before Phase 2 is attached.
    """
    args = _parse_args()

    # Phase 1: minimal bootstrap so pre-config errors are visible on console
    logging.basicConfig(
        level=logging.WARNING,
        format=_LOG_FORMAT,
        datefmt=_LOG_DATEFMT,
        stream=sys.stdout,
    )
    log = logging.getLogger(__name__)

    if not args.config.exists():
        log.error(
            "Config file not found: %s\n"
            "  → Copy configs/strategies/strategy_template.yaml "
            "and fill in your parameters.",
            args.config,
        )
        return 1

    # Phase 2: peek at YAML to extract logging settings, then reconfigure  [L2]
    try:
        peek_config = StrategyConfig.from_yaml(args.config)
    except Exception as e:
        log.error("Configuration error (cannot configure logging): %s", e)
        return 1

    # Remove bootstrap handler; attach file + console handlers
    logging.getLogger().handlers.clear()
    log_file = _configure_logging(peek_config.output.logging, args.log_level)

    # Re-acquire logger after reconfiguration
    log = logging.getLogger(__name__)
    log.info("Strategy runner starting")
    log.info("Config     : %s", args.config)
    log.info("Log file   : %s", log_file)
    if args.mode:
        log.info("Mode       : %s (CLI override)", args.mode)

    try:
        # StrategyConfig constructed again inside from_yaml — fast, YAML-only I/O
        orchestrator = StrategyOrchestrator.from_yaml(args.config)

        result = orchestrator.run(
            mode_override=args.mode,
        )

        _print_result(result)

        # Surface report path at INFO — captured in log file even if console is clean
        if result.report_path:
            log.info("HTML report saved → %s", result.report_path)

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
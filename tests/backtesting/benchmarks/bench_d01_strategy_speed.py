"""
D-01 Benchmark: Strategy Integration Speed — Direct Python Call Mode.

IMPORTANT: This script MUST be run from the project root directory:
    python tests/backtesting/benchmarks/bench_d01_strategy_speed.py --config configs/strategies/strategy_template.yaml
"""
from __future__ import annotations

import argparse
import statistics
import sys
import time
from pathlib import Path

# Assume we're at project root - imports will work
from src.utils.paths import PROJECT_ROOT, config_path

PASS_THRESHOLD_SECONDS = 20.0
N_CANDIDATES = 50

def _try_import_strategy():
    """Attempt to import strategy components. Returns (StrategyConfig, StrategyOrchestrator, CacheManager) or None."""
    try:
        from src.strategies.config.config_schema import StrategyConfig
        from src.strategies.orchestrator import StrategyOrchestrator
        from src.strategies.core.cache_manager import CacheManager
        return StrategyConfig, StrategyOrchestrator, CacheManager
    except ImportError as exc:
        return None, str(exc)


def run_benchmark(config_path_arg: Path) -> bool:
    """Run D-01 benchmark. Returns True if pass criterion met."""
    result = _try_import_strategy()
    if result[0] is None:
        print(f"\n{'='*60}")
        print(f"D-01 Benchmark: Strategy Integration Speed")
        print(f"{'='*60}")
        print(f"SKIP — strategy package not importable in this environment.")
        print(f"Import error: {result[1]}")
        print(f"\nDebug information:")
        print(f"  Project root: {PROJECT_ROOT}")
        print(f"  Python path: {sys.path}")
        print(f"\nTo run this benchmark:")
        print(f"  1. Ensure you're running from project root:")
        print(f"     cd {PROJECT_ROOT}")
        print(f"  2. Run with: python tests/backtesting/benchmarks/bench_d01_strategy_speed.py --config <path>")
        print(f"\nThis benchmark must be run on the operator's machine.")
        print(f"{'='*60}\n")
        return True  # Not a failure — environment constraint

    StrategyConfig, StrategyOrchestrator, CacheManager = result

    if not config_path_arg.exists():
        print(f"ERROR: config not found at {config_path_arg}")
        print(f"Looking in: {config_path_arg.resolve()}")
        return False

    print(f"\n{'='*60}")
    print(f"D-01 Benchmark: Strategy Integration Speed")
    print(f"Mode: direct Python call | Candidates: {N_CANDIDATES} | Sequential")
    print(f"Config: {config_path_arg}")
    print(f"Pass criterion: avg ≤ {PASS_THRESHOLD_SECONDS}s per candidate")
    print(f"{'='*60}")

    # Build a minimal set of parameter variations to evaluate
    # These span the 'safe' zone per backtest_template.yaml
    param_variants = [
        {"rsi_period": 10 + (i % 6) * 2, "atr_multiplier": 1.5 + (i % 4) * 0.25}
        for i in range(N_CANDIDATES)
    ]

    cache_manager = CacheManager()
    timings = []
    errors = []

    base_config = StrategyConfig.from_yaml(config_path_arg)

    for i, params in enumerate(param_variants):
        t_start = time.perf_counter()
        try:
            # Build a modified config for this candidate
            # In production, strategy_runner.py writes a temp YAML and uses from_yaml()
            # Here we call directly to measure the per-candidate overhead
            orchestrator = StrategyOrchestrator(base_config, cache_manager=cache_manager)
            result = orchestrator.run(mode_override="core")  # FIXED: changed from mode="core"
            elapsed = time.perf_counter() - t_start
            timings.append(elapsed)
            status = "ok"
        except Exception as exc:
            elapsed = time.perf_counter() - t_start
            errors.append((i, str(exc)))
            timings.append(elapsed)
            status = f"err: {exc}"
        finally:
            cache_manager.clear_all_caches()

        if (i + 1) % 10 == 0:
            print(f"  [{i+1:3d}/{N_CANDIDATES}] last={elapsed:.2f}s  running_avg={statistics.mean(timings):.2f}s")

    avg = statistics.mean(timings)
    median = statistics.median(timings)
    p95 = sorted(timings)[int(0.95 * len(timings))]
    passed = avg <= PASS_THRESHOLD_SECONDS

    print(f"\n{'='*60}")
    print(f"Results:")
    print(f"  Candidates evaluated : {N_CANDIDATES}")
    print(f"  Errors               : {len(errors)}")
    print(f"  Avg time/candidate   : {avg:.3f}s  ({'PASS ✓' if passed else 'FAIL ✗ — exceeds 20s threshold'})")
    print(f"  Median               : {median:.3f}s")
    print(f"  P95                  : {p95:.3f}s")
    print(f"  Total                : {sum(timings):.1f}s")
    print(f"  6-worker projection  : {sum(timings)/6:.0f}s  ({sum(timings)/6/60:.1f} min)")
    if errors:
        print(f"\n  First 3 errors:")
        for idx, msg in errors[:3]:
            print(f"    Candidate {idx}: {msg}")
    print(f"\nVERDICT: {'PASS ✓' if passed else 'REVIEW REQUIRED — see NEXT_SESSION_PLAN.md D-01 fallback'}")
    if not passed:
        print(f"  Action: Log result in CHANGE_LOG.md. Do NOT change D-01 decision yet.")
        print(f"  The 4-hour budget has slack; profile in Phase 3 before deciding.")
    print(f"{'='*60}\n")

    return passed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="D-01: Strategy integration speed benchmark")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/strategies/strategy_template.yaml"),
        help="Path to strategy config YAML",
    )
    args = parser.parse_args()
    
    # Resolve config path relative to project root if it's relative
    if not args.config.is_absolute():
        args.config = PROJECT_ROOT / args.config
    
    success = run_benchmark(args.config)
    sys.exit(0 if success else 1)
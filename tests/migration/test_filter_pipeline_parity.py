"""
FilterPipeline Parity Test - Old vs New Architecture

Tests:
1. Signal location parity (exact same signals pass/reject)
2. Stats parity (raw → time → technical → final counts)
3. Performance comparison
4. Both modes (core + debug)

Author: Migration Project
Version: 1.0.0
Date: 2025-02-13
Session: 5
"""

import sys
from pathlib import Path
from time import perf_counter
import pandas as pd
import numpy as np
import yaml

# ------------------------------------------------------------
# Project root
# ------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ------------------------------------------------------------
# OLD Architecture Imports
# ------------------------------------------------------------
from src.strategies.core.filter_pipeline import FilterPipeline as OldFilterPipeline
from src.backtesting.tools.filter_pipeline_cache import FilterPipelineCache as OldCache

# ------------------------------------------------------------
# NEW Architecture Imports
# ------------------------------------------------------------
from src.strategies.specific.modules.data_loader import DataLoader
from src.strategies.specific.modules.signal_generator import SignalGenerator
from src.strategies.specific.modules.filter_pipeline import FilterPipeline as NewFilterPipeline
from src.strategies.contracts.cache import FilterPipelineCache as NewCache
from src.strategies.contracts.signal_contracts import SignalFrame, SignalType


# ------------------------------------------------------------
# Utilities
# ------------------------------------------------------------
def load_config(name="wbws_strategy.yaml"):
    """Load strategy configuration."""
    path = PROJECT_ROOT / f"configs/strategies/wbws/{name}"
    with open(path, "r") as f:
        return yaml.safe_load(f)


def convert_signalframe_to_series(signal_frame: SignalFrame) -> pd.Series:
    """
    Convert SignalFrame to legacy pd.Series format.
    
    Maps:
    - Code 1 → "BUY"
    - Code 2 → "SELL"
    - Code 0 → pd.NA
    """
    signals = signal_frame.signals.copy()
    
    # Create mapping
    result = pd.Series(pd.NA, index=signals.index, dtype=object)
    result[signals == 1] = "BUY"
    result[signals == 2] = "SELL"
    
    return result


def compare_signals(old_series: pd.Series, new_series: pd.Series, stage: str) -> dict:
    """
    Compare two signal series and return detailed comparison.
    
    Args:
        old_series: Legacy pd.Series with "BUY"/"SELL"/NA
        new_series: New pd.Series with "BUY"/"SELL"/NA
        stage: Description of comparison stage
    
    Returns:
        Dict with comparison results
    """
    # Extract signal timestamps
    old_signals = old_series.dropna()
    new_signals = new_series.dropna()
    
    old_timestamps = set(old_signals.index)
    new_timestamps = set(new_signals.index)
    
    # Find differences
    only_old = old_timestamps - new_timestamps
    only_new = new_timestamps - old_timestamps
    common = old_timestamps & new_timestamps
    
    # Check signal types match for common timestamps
    type_mismatches = []
    for ts in common:
        if old_signals[ts] != new_signals[ts]:
            type_mismatches.append((ts, old_signals[ts], new_signals[ts]))
    
    # Counts
    old_buy = (old_signals == "BUY").sum()
    old_sell = (old_signals == "SELL").sum()
    new_buy = (new_signals == "BUY").sum()
    new_sell = (new_signals == "SELL").sum()
    
    parity = len(only_old) == 0 and len(only_new) == 0 and len(type_mismatches) == 0
    
    return {
        "stage": stage,
        "parity": parity,
        "old_count": len(old_signals),
        "new_count": len(new_signals),
        "old_buy": old_buy,
        "old_sell": old_sell,
        "new_buy": new_buy,
        "new_sell": new_sell,
        "only_old": len(only_old),
        "only_new": len(only_new),
        "type_mismatches": len(type_mismatches),
        "only_old_timestamps": list(only_old)[:5] if only_old else [],
        "only_new_timestamps": list(only_new)[:5] if only_new else [],
        "type_mismatch_details": type_mismatches[:5] if type_mismatches else [],
    }


def compare_stats(old_stats: dict, new_result, stage: str) -> dict:
    """
    Compare old stats dict with new FilterPipelineResult.
    
    Args:
        old_stats: Legacy stats dict
        new_result: FilterPipelineResult or similar
        stage: Description of comparison stage
    
    Returns:
        Dict with comparison results
    """
    # Extract counts from old stats
    old_raw = old_stats["raw"]["total"]
    old_time = old_stats["time_filtered"]["total"]
    old_tech = old_stats["technical"]["total"]
    old_final = old_stats["final"]["total"]
    
    # Extract counts from new result
    if hasattr(new_result, "raw_count"):
        # FilterPipelineResult
        new_raw = new_result.raw_count
        new_time = new_result.time_filtered_count
        new_tech = new_result.technical_filtered_count
        new_final = new_result.final_count
    else:
        # Fallback to signal frame counts
        new_raw = new_result.count_by_type()["total"]
        new_time = new_raw
        new_tech = new_raw
        new_final = new_raw
    
    # Compare
    raw_match = old_raw == new_raw
    time_match = old_time == new_time
    tech_match = old_tech == new_tech
    final_match = old_final == new_final
    
    parity = raw_match and time_match and tech_match and final_match
    
    return {
        "stage": stage,
        "parity": parity,
        "raw": {"old": old_raw, "new": new_raw, "match": raw_match},
        "time": {"old": old_time, "new": new_time, "match": time_match},
        "technical": {"old": old_tech, "new": new_tech, "match": tech_match},
        "final": {"old": old_final, "new": new_final, "match": final_match},
    }


# ------------------------------------------------------------
# Test Functions
# ------------------------------------------------------------
def test_parity_core_mode():
    """Test parity in core mode (minimal metadata)."""
    print("\n" + "="*80)
    print("TEST 1: PARITY - CORE MODE")
    print("="*80)
    
    # Load config
    config = load_config("wbws_strategy.yaml")
    
    # Ensure core mode
    config["execution"] = {"mode": "core"}
    
    # Load data (shared)
    loader = DataLoader(str(PROJECT_ROOT / "configs/strategies/wbws/wbws_strategy.yaml"))
    loader.load_config()
    data_bundle = loader.load_data()
    df = data_bundle.strategy
    
    print(f"✓ Data loaded: {len(df):,} bars")
    
    # Generate signals (shared)
    gen = SignalGenerator(htf_period="1H", mode="core")
    signal_frame = gen.generate_signals(data_bundle)
    raw_count = signal_frame.count_by_type()["total"]
    
    print(f"✓ Signals generated: {raw_count} total")
    
    # Convert to old format
    raw_signals_old = convert_signalframe_to_series(signal_frame)
    
    # ----------------------------------------------------------------
    # OLD Pipeline
    # ----------------------------------------------------------------
    print("\n--- OLD FilterPipeline ---")
    old_start = perf_counter()
    
    old_pipeline = OldFilterPipeline(config, cache=OldCache())
    old_filtered, old_stats = old_pipeline.apply_filters(df, raw_signals_old)
    
    old_time = (perf_counter() - old_start) * 1000
    
    print(f"Raw:       {old_stats['raw']['total']}")
    print(f"Time:      {old_stats['time_filtered']['total']}")
    print(f"Technical: {old_stats['technical']['total']}")
    print(f"Final:     {old_stats['final']['total']}")
    print(f"Time:      {old_time:.2f}ms")
    
    # ----------------------------------------------------------------
    # NEW Pipeline
    # ----------------------------------------------------------------
    print("\n--- NEW FilterPipeline ---")
    new_start = perf_counter()
    
    new_pipeline = NewFilterPipeline(config, cache=NewCache())
    new_result = new_pipeline.apply_filters(signal_frame, df, mode="core")
    
    new_time = (perf_counter() - new_start) * 1000
    
    print(f"Raw:       {new_result.raw_count}")
    print(f"Time:      {new_result.time_filtered_count}")
    print(f"Technical: {new_result.technical_filtered_count}")
    print(f"Final:     {new_result.final_count}")
    print(f"Time:      {new_time:.2f}ms")
    
    # ----------------------------------------------------------------
    # Compare
    # ----------------------------------------------------------------
    print("\n--- COMPARISON ---")
    
    # Convert new signals to old format for comparison
    new_filtered = convert_signalframe_to_series(new_result.final_signals)
    
    # Signal location comparison
    signal_comp = compare_signals(old_filtered, new_filtered, "final_signals")
    
    print(f"Signal Parity: {'✅ PASS' if signal_comp['parity'] else '❌ FAIL'}")
    print(f"  Old signals: {signal_comp['old_count']}")
    print(f"  New signals: {signal_comp['new_count']}")
    if not signal_comp['parity']:
        print(f"  Only old: {signal_comp['only_old']}")
        print(f"  Only new: {signal_comp['only_new']}")
        print(f"  Type mismatches: {signal_comp['type_mismatches']}")
    
    # Stats comparison
    stats_comp = compare_stats(old_stats, new_result, "pipeline_stats")
    
    print(f"\nStats Parity: {'✅ PASS' if stats_comp['parity'] else '❌ FAIL'}")
    for stage in ["raw", "time", "technical", "final"]:
        s = stats_comp[stage]
        match_str = "✓" if s["match"] else "✗"
        print(f"  {stage:10s}: old={s['old']:4d}, new={s['new']:4d} {match_str}")
    
    # Performance comparison
    speedup = old_time / new_time if new_time > 0 else 0
    regression = (new_time / old_time - 1) * 100 if old_time > 0 else 0
    
    print(f"\nPerformance:")
    print(f"  Old: {old_time:.2f}ms")
    print(f"  New: {new_time:.2f}ms")
    print(f"  Speedup: {speedup:.2f}x")
    print(f"  Regression: {regression:+.1f}%")
    
    # Overall result
    parity_pass = signal_comp['parity'] and stats_comp['parity']
    perf_pass = regression <= 10.0  # Allow up to 10% regression
    
    print(f"\n{'='*80}")
    print(f"CORE MODE: {'✅ PASS' if (parity_pass and perf_pass) else '❌ FAIL'}")
    print(f"  Parity: {'✅' if parity_pass else '❌'}")
    print(f"  Performance: {'✅' if perf_pass else '❌'} (regression: {regression:+.1f}%)")
    print(f"{'='*80}")
    
    return parity_pass and perf_pass


def test_parity_debug_mode():
    """Test parity in debug mode (full metadata)."""
    print("\n" + "="*80)
    print("TEST 2: PARITY - DEBUG MODE")
    print("="*80)
    
    # Load config
    config = load_config("wbws_strategy_debug.yaml")
    
    # Ensure debug mode
    config["execution"] = {"mode": "debug"}
    
    # Load data (shared)
    loader = DataLoader(str(PROJECT_ROOT / "configs/strategies/wbws/wbws_strategy.yaml"))
    loader.load_config()
    data_bundle = loader.load_data()
    df = data_bundle.strategy
    
    print(f"✓ Data loaded: {len(df):,} bars")
    
    # Generate signals (shared)
    gen = SignalGenerator(htf_period="1H", mode="debug")
    signal_frame = gen.generate_signals(data_bundle)
    raw_count = signal_frame.count_by_type()["total"]
    
    print(f"✓ Signals generated: {raw_count} total")
    
    # Convert to old format
    raw_signals_old = convert_signalframe_to_series(signal_frame)
    
    # ----------------------------------------------------------------
    # OLD Pipeline
    # ----------------------------------------------------------------
    print("\n--- OLD FilterPipeline ---")
    old_start = perf_counter()
    
    old_pipeline = OldFilterPipeline(config, cache=OldCache())
    old_filtered, old_stats = old_pipeline.apply_filters(df, raw_signals_old)
    
    old_time = (perf_counter() - old_start) * 1000
    
    print(f"Raw:       {old_stats['raw']['total']}")
    print(f"Time:      {old_stats['time_filtered']['total']}")
    print(f"Technical: {old_stats['technical']['total']}")
    print(f"Final:     {old_stats['final']['total']}")
    print(f"Time:      {old_time:.2f}ms")
    
    # ----------------------------------------------------------------
    # NEW Pipeline
    # ----------------------------------------------------------------
    print("\n--- NEW FilterPipeline ---")
    new_start = perf_counter()
    
    new_pipeline = NewFilterPipeline(config, cache=NewCache())
    new_result = new_pipeline.apply_filters(signal_frame, df, mode="debug")
    
    new_time = (perf_counter() - new_start) * 1000
    
    print(f"Raw:       {new_result.raw_count}")
    print(f"Time:      {new_result.time_filtered_count}")
    print(f"Technical: {new_result.technical_filtered_count}")
    print(f"Final:     {new_result.final_count}")
    print(f"Time:      {new_time:.2f}ms")
    
    # Check metadata collection
    print(f"\nMetadata collected:")
    print(f"  Filter results: {len(new_result.filter_results)}")
    print(f"  Rejection reasons: {len(new_result.rejection_reasons)}")
    print(f"  Execution time tracked: {'✓' if new_result.execution_time_ms else '✗'}")
    
    # ----------------------------------------------------------------
    # Compare
    # ----------------------------------------------------------------
    print("\n--- COMPARISON ---")
    
    # Convert new signals to old format for comparison
    new_filtered = convert_signalframe_to_series(new_result.final_signals)
    
    # Signal location comparison
    signal_comp = compare_signals(old_filtered, new_filtered, "final_signals")
    
    print(f"Signal Parity: {'✅ PASS' if signal_comp['parity'] else '❌ FAIL'}")
    print(f"  Old signals: {signal_comp['old_count']}")
    print(f"  New signals: {signal_comp['new_count']}")
    if not signal_comp['parity']:
        print(f"  Only old: {signal_comp['only_old']}")
        print(f"  Only new: {signal_comp['only_new']}")
        print(f"  Type mismatches: {signal_comp['type_mismatches']}")
    
    # Stats comparison
    stats_comp = compare_stats(old_stats, new_result, "pipeline_stats")
    
    print(f"\nStats Parity: {'✅ PASS' if stats_comp['parity'] else '❌ FAIL'}")
    for stage in ["raw", "time", "technical", "final"]:
        s = stats_comp[stage]
        match_str = "✓" if s["match"] else "✗"
        print(f"  {stage:10s}: old={s['old']:4d}, new={s['new']:4d} {match_str}")
    
    # Performance comparison
    speedup = old_time / new_time if new_time > 0 else 0
    regression = (new_time / old_time - 1) * 100 if old_time > 0 else 0
    
    print(f"\nPerformance:")
    print(f"  Old: {old_time:.2f}ms")
    print(f"  New: {new_time:.2f}ms")
    print(f"  Speedup: {speedup:.2f}x")
    print(f"  Regression: {regression:+.1f}%")
    
    # Overall result
    parity_pass = signal_comp['parity'] and stats_comp['parity']
    perf_pass = regression <= 10.0  # Allow up to 10% regression
    
    print(f"\n{'='*80}")
    print(f"DEBUG MODE: {'✅ PASS' if (parity_pass and perf_pass) else '❌ FAIL'}")
    print(f"  Parity: {'✅' if parity_pass else '❌'}")
    print(f"  Performance: {'✅' if perf_pass else '❌'} (regression: {regression:+.1f}%)")
    print(f"{'='*80}")
    
    return parity_pass and perf_pass


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
if __name__ == "__main__":
    print("\n" + "="*80)
    print("FILTERPIPELINE PARITY TEST SUITE")
    print("Comparing Old vs New Architecture")
    print("="*80)
    
    results = []
    
    # Test 1: Core mode
    try:
        results.append(("Core Mode", test_parity_core_mode()))
    except Exception as e:
        print(f"\n❌ Core mode test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        results.append(("Core Mode", False))
    
    # Test 2: Debug mode
    try:
        results.append(("Debug Mode", test_parity_debug_mode()))
    except Exception as e:
        print(f"\n❌ Debug mode test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        results.append(("Debug Mode", False))
    
    # Final summary
    print("\n" + "="*80)
    print("FINAL RESULTS")
    print("="*80)
    
    all_pass = all(result for _, result in results)
    
    for name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {name:20s}: {status}")
    
    print(f"\n{'='*80}")
    print(f"OVERALL: {'✅ ALL TESTS PASSED' if all_pass else '❌ SOME TESTS FAILED'}")
    print(f"{'='*80}\n")
    
    sys.exit(0 if all_pass else 1)
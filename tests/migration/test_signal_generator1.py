"""
SignalGenerator v2 Performance & Parity Check
Quick validation: Old vs New signal generation

Usage:
    python test_signal_generator_v2_simple.py

Expected Results:
    ✅ Signal parity: 100% match
    ✅ Performance (core): ≤25ms target
    ✅ Performance (debug): ≤30ms target
"""

import sys
from pathlib import Path
import time
import pandas as pd
import numpy as np

# Path resolution (same pattern as DataLoader test)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.strategies.core.signal_generator import SignalGenerator as OldSignalGenerator

try:
    from src.strategies.specific.modules.signal_generator import SignalGenerator as NewSignalGenerator
except ImportError:
    print("❌ New SignalGenerator not found at src/strategies/specific/modules/signal_generator.py")
    print("   Make sure you've copied signal_generator_v2.py to that location")
    sys.exit(1)

try:
    from src.strategies.specific.modules.data_loader import DataLoader as NewDataLoader
except ImportError:
    print("❌ New DataLoader not found - using old DataLoader for test data")
    from src.strategies.core.data_loader import DataLoader as DataLoaderFallback
    NEW_DATALOADER_AVAILABLE = False
else:
    NEW_DATALOADER_AVAILABLE = True

from src.strategies.contracts.signal_contracts import SignalType

# =============================================================================
# CONFIGURATION
# =============================================================================

config_path = PROJECT_ROOT / "configs/strategies/wbws/wbws_strategy_debug.yaml"
if not config_path.exists():
    print(f"❌ Config not found: {config_path}")
    sys.exit(1)

HTF_PERIOD = "1H"  # Adjust if your config uses different HTF

# =============================================================================
# LOAD TEST DATA
# =============================================================================

print("\n" + "="*70)
print("SIGNALGENERATOR V2 - VALIDATION CHECK")
print("="*70)

print(f"\n📂 Loading data from config: {config_path.name}")

if NEW_DATALOADER_AVAILABLE:
    # Use new DataLoader (recommended)
    loader = NewDataLoader(str(config_path))
    loader.load_config()
    data_bundle = loader.load_data()
    df_strategy = data_bundle.strategy
    df_htf = data_bundle.htf
    print(f"✅ Using DataLoader v2 (DataBundle)")
else:
    # Fallback to old DataLoader
    loader = DataLoaderFallback(str(config_path))
    loader.load_config()
    data = loader.load_data()
    df_strategy = data["strategy"]
    df_htf = data["htf"]
    data_bundle = None
    print(f"⚠️  Using old DataLoader (fallback)")

print(f"   Strategy bars: {len(df_strategy)}")
print(f"   HTF bars: {len(df_htf)}")

# =============================================================================
# TEST 1: SIGNAL PARITY (Old vs New) - FIXED
# =============================================================================

print("\n" + "="*70)
print("TEST 1: SIGNAL PARITY (Old vs New)")
print("="*70)

# Old generator (string-based)
old_gen = OldSignalGenerator(htf_period=HTF_PERIOD)
raw_signals_old, signals_df_old = old_gen.generate_signals(df_strategy, df_htf)

print(f"\n📊 Old SignalGenerator:")
print(f"   Total signals: {raw_signals_old.notna().sum():,}")
print(f"   BUY signals: {(raw_signals_old == 'BUY').sum():,}")
print(f"   SELL signals: {(raw_signals_old == 'SELL').sum():,}")

if data_bundle is not None:
    # New generator - Debug mode (for parity check)
    new_gen_debug = NewSignalGenerator(htf_period=HTF_PERIOD, mode="debug")
    signal_frame_new = new_gen_debug.generate_signals(data_bundle)
    
    print(f"\n📊 New SignalGenerator (v2.2.1 - int8):")
    print(f"   {signal_frame_new}")
    counts = signal_frame_new.count_by_type()
    print(f"   BUY signals: {counts['buy']:,}")
    print(f"   SELL signals: {counts['sell']:,}")
    print(f"   Storage dtype: {signal_frame_new.signals.dtype}")
    
    # FIX: Only compare rows where EITHER old or new has a signal
    # Get signal masks
    old_buy_mask = signals_df_old["we_buy"].values
    old_sell_mask = signals_df_old["we_sell"].values
    old_has_signal = old_buy_mask | old_sell_mask
    
    new_buy_mask = signal_frame_new.signals.values == 1
    new_sell_mask = signal_frame_new.signals.values == 2
    new_has_signal = new_buy_mask | new_sell_mask
    
    # Only compare rows where either has a signal
    compare_mask = old_has_signal | new_has_signal
    total_compare = compare_mask.sum()
    
    # Compare signals only on those rows
    buy_match = np.sum((old_buy_mask[compare_mask] == new_buy_mask[compare_mask]))
    sell_match = np.sum((old_sell_mask[compare_mask] == new_sell_mask[compare_mask]))
    
    buy_pct = (buy_match / total_compare * 100) if total_compare > 0 else 100.0
    sell_pct = (sell_match / total_compare * 100) if total_compare > 0 else 100.0
    
    print(f"\n✅ Parity Check (only rows with signals):")
    print(f"   Rows with signals: {total_compare:,}")
    print(f"   we_buy match: {buy_match:,}/{total_compare:,} ({buy_pct:.2f}%)")
    print(f"   we_sell match: {sell_match:,}/{total_compare:,} ({sell_pct:.2f}%)")
    
    # Also verify signal direction matches
    old_direction = np.where(old_buy_mask, 1, np.where(old_sell_mask, 2, 0))
    new_direction = np.where(new_buy_mask, 1, np.where(new_sell_mask, 2, 0))
    
    direction_match = np.sum(old_direction[compare_mask] == new_direction[compare_mask])
    direction_pct = (direction_match / total_compare * 100) if total_compare > 0 else 100.0
    
    print(f"   signal direction match: {direction_match:,}/{total_compare:,} ({direction_pct:.2f}%)")
    
    parity_pass = (buy_pct >= 99.9 and sell_pct >= 99.9 and direction_pct >= 99.9)
    print(f"\n{'✅ PASS' if parity_pass else '❌ FAIL'}: Signal parity {'≥' if parity_pass else '<'}99.9%")

# =============================================================================
# TEST 2: PERFORMANCE BENCHMARK
# =============================================================================

print("\n" + "="*70)
print("TEST 2: PERFORMANCE BENCHMARK")
print("="*70)

# Warm-up
old_gen.generate_signals(df_strategy, df_htf)

# Benchmark old generator
runs = 10
times_old = []
for _ in range(runs):
    start = time.perf_counter()
    old_gen.generate_signals(df_strategy, df_htf)
    elapsed = (time.perf_counter() - start) * 1000  # ms
    times_old.append(elapsed)

mean_old = np.mean(times_old)
std_old = np.std(times_old)

print(f"\n⏱️  Old SignalGenerator:")
print(f"   Mean: {mean_old:6.2f}ms (±{std_old:5.2f}ms) over {runs} runs")

if data_bundle is not None:
    # Benchmark new generator - Debug mode
    new_gen_debug = NewSignalGenerator(htf_period=HTF_PERIOD, mode="debug")
    new_gen_debug.generate_signals(data_bundle)  # Warm-up
    
    times_new_debug = []
    for _ in range(runs):
        start = time.perf_counter()
        new_gen_debug.generate_signals(data_bundle)
        elapsed = (time.perf_counter() - start) * 1000  # ms
        times_new_debug.append(elapsed)
    
    mean_debug = np.mean(times_new_debug)
    std_debug = np.std(times_new_debug)
    
    print(f"\n⏱️  New SignalGenerator (debug mode):")
    print(f"   Mean: {mean_debug:6.2f}ms (±{std_debug:5.2f}ms) over {runs} runs")
    
    # Benchmark new generator - Core mode
    new_gen_core = NewSignalGenerator(htf_period=HTF_PERIOD, mode="core")
    new_gen_core.generate_signals(data_bundle)  # Warm-up
    
    times_new_core = []
    for _ in range(runs):
        start = time.perf_counter()
        new_gen_core.generate_signals(data_bundle)
        elapsed = (time.perf_counter() - start) * 1000  # ms
        times_new_core.append(elapsed)
    
    mean_core = np.mean(times_new_core)
    std_core = np.std(times_new_core)
    
    print(f"\n⏱️  New SignalGenerator (core mode):")
    print(f"   Mean: {mean_core:6.2f}ms (±{std_core:5.2f}ms) over {runs} runs")
    
    # Analysis
    print(f"\n📊 Performance Analysis:")
    print(f"   Debug overhead: +{mean_debug - mean_core:.2f}ms ({(mean_debug/mean_core-1)*100:+.1f}%)")
    print(f"   Core vs old:    {mean_core - mean_old:+.2f}ms ({(mean_core/mean_old-1)*100:+.1f}%)")
    
    # Targets
    target_core = 25.0  # ms
    target_debug = 30.0  # ms
    
    print(f"\n📏 Target Compliance:")
    core_pass = mean_core <= target_core
    debug_pass = mean_debug <= target_debug
    
    print(f"   Core mode:  {mean_core:.2f}ms ≤ {target_core:.0f}ms? {'✅ PASS' if core_pass else '❌ FAIL'}")
    print(f"   Debug mode: {mean_debug:.2f}ms ≤ {target_debug:.0f}ms? {'✅ PASS' if debug_pass else '❌ FAIL'}")
    
    perf_pass = core_pass and debug_pass
else:
    print("\n⚠️  Skipping new generator benchmark (DataBundle not available)")
    perf_pass = True  # Don't fail if we can't test

# =============================================================================
# TEST 3: DUAL-MODE VERIFICATION (if DataBundle available)
# =============================================================================

if data_bundle is not None:
    print("\n" + "="*70)
    print("TEST 3: DUAL-MODE VERIFICATION")
    print("="*70)
    
    gen_debug = NewSignalGenerator(htf_period=HTF_PERIOD, mode="debug")
    signal_frame_debug = gen_debug.generate_signals(data_bundle)
    
    gen_core = NewSignalGenerator(htf_period=HTF_PERIOD, mode="core")
    signal_frame_core = gen_core.generate_signals(data_bundle)
    
    print(f"\n🔍 Debug Mode:")
    print(f"   Signals: {len(signal_frame_debug)}")
    print(f"   Indicator data: {'Present' if signal_frame_debug.indicator_data is not None else 'None'}")
    print(f"   Metadata: {signal_frame_debug.signal_metadata}")
    
    print(f"\n⚡ Core Mode:")
    print(f"   Signals: {len(signal_frame_core)}")
    print(f"   Indicator data: {'Present' if signal_frame_core.indicator_data is not None else 'None'}")
    print(f"   Metadata: {signal_frame_core.signal_metadata}")
    
    # Verify (use .equals() for Series with object dtype containing enums)
    signals_match = signal_frame_debug.signals.equals(signal_frame_core.signals)
    debug_has_metadata = signal_frame_debug.indicator_data is not None
    core_no_metadata = signal_frame_core.indicator_data is None
    
    print(f"\n✅ Verification:")
    print(f"   Signals match: {'✅ PASS' if signals_match else '❌ FAIL'}")
    print(f"   Debug has metadata: {'✅ PASS' if debug_has_metadata else '❌ FAIL'}")
    print(f"   Core no metadata: {'✅ PASS' if core_no_metadata else '❌ FAIL'}")
    
    dual_mode_pass = signals_match and debug_has_metadata and core_no_metadata
else:
    dual_mode_pass = True  # Skip if no DataBundle

# =============================================================================
# SUMMARY
# =============================================================================

print("\n" + "="*70)
print("TEST SUMMARY")
print("="*70)

results = {
    "Signal Parity": parity_pass,
    "Performance": perf_pass,
}

if data_bundle is not None:
    results["Dual-Mode"] = dual_mode_pass

for test_name, passed in results.items():
    status = "✅ PASS" if passed else "❌ FAIL"
    print(f"  {test_name:20s}: {status}")

all_passed = all(results.values())

print("\n" + "="*70)
if all_passed:
    print("✅ ALL TESTS PASSED - SignalGenerator v2 is ready!")
else:
    print("❌ SOME TESTS FAILED - Review issues above")
print("="*70 + "\n")

sys.exit(0 if all_passed else 1)
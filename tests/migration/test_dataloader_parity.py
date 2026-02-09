"""
DataLoader Migration Validation Test

This test compares the old DataLoader (dict-based 4-tuple return)
with the new DataLoader_v2 (DataBundle return) to ensure:

1. DataFrames are identical (column-wise, row-wise)
2. Performance is maintained or improved
3. All metadata is preserved

Usage:
    python tests/migration/test_dataloader_parity.py

Expected Result:
    ✅ All checks pass
    ⚡ New DataLoader performance ≤ 110% of old
"""

import sys
from pathlib import Path
import pandas as pd
import time
import numpy as np

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import old DataLoader
from src.strategies.core.data_loader import DataLoader as OldDataLoader

# Import new DataLoader
# Try direct import first, fall back to manual loading
try:
    from src.strategies.specific.modules.data_loader import DataLoader as NewDataLoader
except ImportError:
    # If the modules directory doesn't exist yet, try loading from file
    import importlib.util
    
    # Check multiple possible locations
    possible_paths = [
        PROJECT_ROOT / "src/strategies/specific/modules/data_loader.py",
        PROJECT_ROOT / "src/strategies/specific/data_loader.py",
        PROJECT_ROOT / "data_loader_v2.py",  # If you saved it at root temporarily
    ]
    
    new_loader_path = None
    for path in possible_paths:
        if path.exists():
            new_loader_path = path
            break
    
    if new_loader_path is None:
        print("\n❌ ERROR: Could not find new DataLoader file.")
        print("\nLooked in:")
        for path in possible_paths:
            print(f"  - {path}")
        print("\nPlease save data_loader_v2.py to one of these locations.")
        sys.exit(1)
    
    print(f"\nℹ️  Loading new DataLoader from: {new_loader_path}")
    
    spec = importlib.util.spec_from_file_location("new_data_loader", new_loader_path)
    new_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(new_module)
    NewDataLoader = new_module.DataLoader


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def compare_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, name: str) -> bool:
    """
    Compare two DataFrames for equality.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        name: Name for error messages
        
    Returns:
        True if DataFrames are equal (within floating point tolerance)
    """
    print(f"\n  Comparing {name}...")
    
    # Check both are None or both exist
    if df1 is None and df2 is None:
        print(f"    ✅ Both are None (expected for optional data)")
        return True
    
    if (df1 is None) != (df2 is None):
        print(f"    ❌ One is None, other is not")
        return False
    
    # Check shape
    if df1.shape != df2.shape:
        print(f"    ❌ Shape mismatch: {df1.shape} vs {df2.shape}")
        return False
    print(f"    ✅ Shape: {df1.shape}")
    
    # Check columns
    if not df1.columns.equals(df2.columns):
        print(f"    ❌ Column mismatch")
        print(f"       Old: {df1.columns.tolist()}")
        print(f"       New: {df2.columns.tolist()}")
        return False
    print(f"    ✅ Columns: {len(df1.columns)}")
    
    # Check index
    if not df1.index.equals(df2.index):
        print(f"    ❌ Index mismatch")
        if len(df1.index) != len(df2.index):
            print(f"       Length: {len(df1.index)} vs {len(df2.index)}")
        else:
            # Find first difference
            diff_mask = df1.index != df2.index
            if diff_mask.any():
                first_diff_idx = diff_mask.argmax()
                print(f"       First diff at position {first_diff_idx}")
                print(f"       Old: {df1.index[first_diff_idx]}")
                print(f"       New: {df2.index[first_diff_idx]}")
        return False
    print(f"    ✅ Index: {len(df1.index)} timestamps")
    
    # Check values (with floating point tolerance)
    for col in df1.columns:
        if df1[col].dtype in [np.float32, np.float64]:
            # Use floating point comparison with tolerance
            if not np.allclose(df1[col], df2[col], rtol=1e-5, atol=1e-8, equal_nan=True):
                print(f"    ❌ Values mismatch in column '{col}'")
                # Find first difference
                diff_mask = ~np.isclose(df1[col], df2[col], rtol=1e-5, atol=1e-8, equal_nan=True)
                if diff_mask.any():
                    first_diff_idx = diff_mask.argmax()
                    print(f"       First diff at row {first_diff_idx}")
                    print(f"       Old: {df1[col].iloc[first_diff_idx]}")
                    print(f"       New: {df2[col].iloc[first_diff_idx]}")
                return False
        else:
            # Exact comparison for non-float columns
            if not df1[col].equals(df2[col]):
                print(f"    ❌ Values mismatch in column '{col}'")
                return False
    
    print(f"    ✅ All values match (within tolerance)")
    return True


def benchmark(func, *args, **kwargs):
    """
    Benchmark a function and return result + elapsed time.
    
    Args:
        func: Function to benchmark
        *args, **kwargs: Function arguments
        
    Returns:
        (result, elapsed_seconds)
    """
    start = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = time.perf_counter() - start
    return result, elapsed


# =============================================================================
# MAIN TEST
# =============================================================================

def test_dataloader_parity():
    """
    Main test function comparing old and new DataLoader.
    """
    print("=" * 70)
    print("DataLoader Migration Parity Test")
    print("=" * 70)
    
    # Use the test config
    config_path = PROJECT_ROOT / "configs/strategies/wbws/wbws_strategy.yaml"
    
    if not config_path.exists():
        print(f"\n❌ Config file not found: {config_path}")
        return False
    
    print(f"\nConfig: {config_path.name}")
    
    # ==========================================================================
    # TEST 1: Load with OLD DataLoader
    # ==========================================================================
    print("\n" + "=" * 70)
    print("TEST 1: Old DataLoader (Baseline)")
    print("=" * 70)
    
    old_loader = OldDataLoader(str(config_path))
    old_loader.load_config()
    
    (df_full_old, df_strategy_old, df_htf_old, df_ltf_old), old_time = benchmark(
        old_loader.load_data
    )
    
    print(f"\n  ⏱️  Load time: {old_time*1000:.1f} ms")
    print(f"  📊 Full data: {len(df_full_old):,} bars")
    print(f"  📊 Strategy data: {len(df_strategy_old):,} bars")
    print(f"  📊 HTF data: {len(df_htf_old) if df_htf_old is not None else 0:,} bars")
    print(f"  📊 LTF data: {len(df_ltf_old) if df_ltf_old is not None else 0:,} bars")
    
    old_info = old_loader.get_data_info()
    old_validation = old_loader.validate_data()
    old_cache_stats = old_loader.get_cache_stats()
    
    print(f"\n  Validation: {'✅ VALID' if old_validation['is_valid'] else '❌ INVALID'}")
    print(f"  Cache: {old_cache_stats['hits']}/{old_cache_stats['hits'] + old_cache_stats['misses']} hits")
    
    # ==========================================================================
    # TEST 2: Load with NEW DataLoader
    # ==========================================================================
    print("\n" + "=" * 70)
    print("TEST 2: New DataLoader (DataBundle)")
    print("=" * 70)
    
    new_loader = NewDataLoader(str(config_path))
    new_loader.load_config()
    
    bundle, new_time = benchmark(new_loader.load_data)
    
    print(f"\n  ⏱️  Load time: {new_time*1000:.1f} ms")
    print(f"  📊 {bundle.info}")
    print(f"\n  Validation: {bundle.validation}")
    print(f"  Cache: {new_loader.cache_stats}")
    
    # ==========================================================================
    # TEST 3: Compare DataFrames
    # ==========================================================================
    print("\n" + "=" * 70)
    print("TEST 3: DataFrame Comparison")
    print("=" * 70)
    
    all_match = True
    
    # Compare full data
    all_match &= compare_dataframes(df_full_old, bundle.full, "Full Data")
    
    # Compare strategy data
    all_match &= compare_dataframes(df_strategy_old, bundle.strategy, "Strategy Data")
    
    # Compare HTF data
    all_match &= compare_dataframes(df_htf_old, bundle.htf, "HTF Data")
    
    # Compare LTF data
    all_match &= compare_dataframes(df_ltf_old, bundle.ltf, "LTF Data")
    
    # ==========================================================================
    # TEST 4: Compare Metadata
    # ==========================================================================
    print("\n" + "=" * 70)
    print("TEST 4: Metadata Comparison")
    print("=" * 70)
    
    metadata_match = True
    
    # Compare bar counts
    print("\n  Bar counts:")
    if old_info['full_bars'] != bundle.info.total_bars:
        print(f"    ❌ Full bars: {old_info['full_bars']} vs {bundle.info.total_bars}")
        metadata_match = False
    else:
        print(f"    ✅ Full bars: {bundle.info.total_bars:,}")
    
    if old_info['strategy_bars'] != bundle.info.strategy_bars:
        print(f"    ❌ Strategy bars: {old_info['strategy_bars']} vs {bundle.info.strategy_bars}")
        metadata_match = False
    else:
        print(f"    ✅ Strategy bars: {bundle.info.strategy_bars:,}")
    
    # Compare validation
    print("\n  Validation:")
    if old_validation['is_valid'] != bundle.validation.is_valid:
        print(f"    ❌ Validation status mismatch")
        metadata_match = False
    else:
        print(f"    ✅ Validation: {bundle.validation.is_valid}")
    
    # ==========================================================================
    # TEST 5: Performance Comparison
    # ==========================================================================
    print("\n" + "=" * 70)
    print("TEST 5: Performance Comparison")
    print("=" * 70)
    
    print(f"\n  Old DataLoader: {old_time*1000:.1f} ms")
    print(f"  New DataLoader: {new_time*1000:.1f} ms")
    
    speedup = old_time / new_time if new_time > 0 else 0
    slowdown_pct = ((new_time - old_time) / old_time * 100) if old_time > 0 else 0
    
    print(f"\n  Speedup: {speedup:.2f}x")
    print(f"  Slowdown: {slowdown_pct:+.1f}%")
    
    # Performance acceptance: new must be ≤ 110% of old
    performance_ok = new_time <= old_time * 1.10
    
    if performance_ok:
        print(f"  ✅ Performance acceptable (≤110% of baseline)")
    else:
        print(f"  ❌ Performance regression (>{old_time * 1.10 * 1000:.1f} ms threshold)")
    
    # ==========================================================================
    # FINAL RESULT
    # ==========================================================================
    print("\n" + "=" * 70)
    print("FINAL RESULT")
    print("=" * 70)
    
    print(f"\n  DataFrame comparison: {'✅ PASS' if all_match else '❌ FAIL'}")
    print(f"  Metadata comparison:  {'✅ PASS' if metadata_match else '❌ FAIL'}")
    print(f"  Performance test:     {'✅ PASS' if performance_ok else '❌ FAIL'}")
    
    overall_pass = all_match and metadata_match and performance_ok
    
    print(f"\n  {'='*70}")
    if overall_pass:
        print(f"  ✅ ALL TESTS PASSED - Migration is safe to proceed")
    else:
        print(f"  ❌ TESTS FAILED - Do not proceed with migration")
    print(f"  {'='*70}\n")
    
    return overall_pass


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    success = test_dataloader_parity()
    sys.exit(0 if success else 1)
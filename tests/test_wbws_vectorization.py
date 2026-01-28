# scripts/test_wbws_vectorization.py - FINAL VERSION
"""
Test harness to verify optimized WBWS trigger matches original exactly.
"""
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import time

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.indicators.wbws_trigger import WBWSTrigger as OriginalWBWS
from scripts.strategy_modules.wbws_trigger_optimized import WBWSTriggerOptimized as OptimizedWBWS

def create_test_data(n_bars: int = 1000) -> pd.DataFrame:
    """Create synthetic test data matching EXACT CSV format"""
    np.random.seed(42)
    
    dates = pd.date_range('2024-01-01', periods=n_bars, freq='1min')
    prices = 100 + np.cumsum(np.random.randn(n_bars) * 0.1)
    
    # Create EXACT CSV format
    df = pd.DataFrame({
        'timestamp': dates,
        'open': prices - np.random.rand(n_bars) * 0.5,
        'high': prices + np.random.rand(n_bars) * 0.5,
        'low': prices - np.random.rand(n_bars) * 0.5,
        'close': prices,
        'volume': np.random.randint(1000, 10000, n_bars)
    })
    
    return df

def prepare_data_like_dataloader(df: pd.DataFrame) -> pd.DataFrame:
    """Simulate EXACTLY what DataLoader does"""
    # 1. Parse dates (already done in our creation)
    # 2. Ensure lowercase columns
    df = df.copy()
    df.columns = df.columns.str.lower()
    
    # 3. Set timestamp as index and sort
    df = df.set_index('timestamp').sort_index()
    
    return df

def compare_results(orig_df: pd.DataFrame, opt_df: pd.DataFrame, tol: float = 1e-10):
    """Compare results from original and optimized versions"""
    
    # Both should have 'timestamp' column (from reset_index)
    assert 'timestamp' in orig_df.columns, "Original missing 'timestamp' column"
    assert 'timestamp' in opt_df.columns, "Optimized missing 'timestamp' column"
    
    # Set timestamp as index for comparison
    orig_compare = orig_df.set_index('timestamp').sort_index()
    opt_compare = opt_df.set_index('timestamp').sort_index()
    
    # Compare shapes
    assert orig_compare.shape == opt_compare.shape, f"Shape mismatch: {orig_compare.shape} vs {opt_compare.shape}"
    
    # Compare key columns
    key_columns = ['candle_type', 'rev_2d_2u', 'rev_2u_2d', 'we_buy', 'we_sell', 'htf_bull', 'htf_bear']
    
    mismatches = []
    for col in key_columns:
        if col in orig_compare.columns and col in opt_compare.columns:
            # Handle NaN values in comparison
            mask_orig_nan = orig_compare[col].isna()
            mask_opt_nan = opt_compare[col].isna()
            
            # Check NaN positions match
            nan_match = (mask_orig_nan == mask_opt_nan).all()
            if not nan_match:
                mismatches.append(f"{col}: NaN positions differ")
                continue
            
            # Compare non-NaN values
            mask_not_nan = ~mask_orig_nan
            if mask_not_nan.any():
                if col in ['candle_type']:
                    # Numeric comparison with tolerance
                    diff = (orig_compare.loc[mask_not_nan, col] - opt_compare.loc[mask_not_nan, col]).abs()
                    if (diff > tol).any():
                        mismatches.append(f"{col}: numeric mismatch max_diff={diff.max()}")
                else:
                    # Boolean comparison
                    bool_match = (orig_compare.loc[mask_not_nan, col] == opt_compare.loc[mask_not_nan, col]).all()
                    if not bool_match:
                        mismatches.append(f"{col}: boolean values differ")
    
    return mismatches, orig_compare, opt_compare

def run_single_test(wbws_instance, test_df, label=""):
    """Run a single test and return results"""
    try:
        result = wbws_instance.calculate_signals(test_df.copy(), verbose=False)
        print(f"    {label}: Success, shape={result.shape}")
        return result, True
    except Exception as e:
        print(f"    {label}: FAILED - {e}")
        return None, False

def run_validation():
    """Main validation routine"""
    print("=" * 60)
    print("WBWS TRIGGER VECTORIZATION VALIDATION")
    print("=" * 60)
    
    # Create test data in CSV format
    print("\n1. Creating test data (CSV format)...")
    raw_df = create_test_data(n_bars=5000)
    print(f"   Created {len(raw_df):,} test bars")
    print(f"   Raw columns: {list(raw_df.columns)}")
    print(f"   Sample timestamp: {raw_df['timestamp'].iloc[0]}")
    
    # Prepare data like DataLoader does
    print("\n2. Preparing data (simulating DataLoader)...")
    test_df = prepare_data_like_dataloader(raw_df)
    print(f"   Index name: '{test_df.index.name}'")
    print(f"   Index type: {type(test_df.index)}")
    print(f"   Columns: {list(test_df.columns)}")
    
    # Initialize both versions
    print("\n3. Initializing WBWS triggers...")
    original = OriginalWBWS(htf_period='60min')
    optimized = OptimizedWBWS(htf_period='60min')
    
    # First, test with small dataset
    print("\n4. Testing with small dataset (100 bars)...")
    small_raw = create_test_data(n_bars=100)
    small_df = prepare_data_like_dataloader(small_raw)
    
    orig_small, orig_ok = run_single_test(original, small_df, "Original")
    opt_small, opt_ok = run_single_test(optimized, small_df, "Optimized")
    
    if not (orig_ok and opt_ok):
        print("❌ One or both versions failed on small dataset")
        return False
    
    # Compare small results
    if orig_small is not None and opt_small is not None:
        mismatches, _, _ = compare_results(orig_small, opt_small)
        if mismatches:
            print(f"❌ Mismatches on small dataset: {mismatches}")
            return False
        else:
            print("✅ Small dataset results match!")
    
    # Now test performance with full dataset
    print("\n5. Performance testing with 5,000 bars...")
    
    # Warm up
    print("  Warming up...")
    _ = original.calculate_signals(test_df.copy(), verbose=False)
    _ = optimized.calculate_signals(test_df.copy(), verbose=False)
    
    # Time original
    print("  Timing original version...")
    start = time.perf_counter()
    orig_result = original.calculate_signals(test_df.copy(), verbose=False)
    orig_time = time.perf_counter() - start
    
    # Time optimized
    print("  Timing optimized version...")
    start = time.perf_counter()
    opt_result = optimized.calculate_signals(test_df.copy(), verbose=False)
    opt_time = time.perf_counter() - start
    
    print("\n6. Comparing results...")
    mismatches, orig_compare, opt_compare = compare_results(orig_result, opt_result)
    
    # Print results
    print("\n" + "=" * 60)
    print("VALIDATION RESULTS")
    print("=" * 60)
    
    print(f"\nPerformance:")
    print(f"  Original: {orig_time:.3f}s for {len(test_df):,} bars")
    print(f"  Optimized: {opt_time:.3f}s for {len(test_df):,} bars")
    print(f"  Speedup: {orig_time/opt_time:.1f}x faster")
    
    print(f"\nSignal counts:")
    print(f"  Original - Buy: {orig_result['we_buy'].sum():,}, Sell: {orig_result['we_sell'].sum():,}")
    print(f"  Optimized - Buy: {opt_result['we_buy'].sum():,}, Sell: {opt_result['we_sell'].sum():,}")
    
    print(f"\nDataFrame structure:")
    print(f"  Original shape: {orig_result.shape}")
    print(f"  Has 'timestamp' column: {'timestamp' in orig_result.columns}")
    print(f"  Optimized shape: {opt_result.shape}")
    print(f"  Has 'timestamp' column: {'timestamp' in opt_result.columns}")
    
    if mismatches:
        print(f"\n❌ MISMATCHES FOUND ({len(mismatches)}):")
        for mismatch in mismatches:
            print(f"  - {mismatch}")
        
        # Show first few differences
        for col in ['we_buy', 'we_sell', 'candle_type']:
            if col in orig_compare.columns and col in opt_compare.columns:
                diff_mask = orig_compare[col] != opt_compare[col]
                if diff_mask.any():
                    n_diffs = diff_mask.sum()
                    print(f"\n  Column '{col}' has {n_diffs} differences")
                    if n_diffs > 0:
                        sample_idx = orig_compare.index[diff_mask][:5]
                        print(f"  Sample differences at indices: {list(sample_idx)}")
                        for idx in sample_idx:
                            print(f"    {idx}: orig={orig_compare.loc[idx, col]}, opt={opt_compare.loc[idx, col]}")
        return False
    else:
        print(f"\n✅ PERFECT MATCH! All results identical.")
        
        # Show sample of signals
        print(f"\nSample of signals (first 3 of each):")
        
        buy_signals = orig_compare[orig_compare['we_buy']]
        if len(buy_signals) > 0:
            print(f"\nBUY signals ({len(buy_signals)} total):")
            print(buy_signals.head(3)[['candle_type', 'htf_bull', 'rev_2d_2u']].to_string())
        
        sell_signals = orig_compare[orig_compare['we_sell']]
        if len(sell_signals) > 0:
            print(f"\nSELL signals ({len(sell_signals)} total):")
            print(sell_signals.head(3)[['candle_type', 'htf_bear', 'rev_2u_2d']].to_string())
        
        return True

if __name__ == "__main__":
    try:
        success = run_validation()
        if success:
            print("\n" + "=" * 60)
            print("🎉 VALIDATION PASSED! Optimized version is ready for use.")
            print("=" * 60)
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ ERROR during validation: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
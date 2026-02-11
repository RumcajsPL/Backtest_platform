"""
TimeFilter Migration Parity Test

Validates that new TimeFilter produces identical results to old TimeManager.

Author: Migration Project
Date: 2025-02-11
Session: 4
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

# ------------------------------------------------------------
# CRITICAL: Add project root to path BEFORE importing from src
# ------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ------------------------------------------------------------
# UNIFIED PATH RESOLUTION USING paths.py
# ------------------------------------------------------------
from src.utils.paths import (
    PROJECT_ROOT as PATHS_PROJECT_ROOT,  # Rename to avoid confusion
    CONTRACTS_DIR,
    migration_test_path,
    ensure_dir
)

# Import old implementation (legacy path)
from src.strategies.filters.time_filter import TimeManager as OldTimeManager

# Import new implementation using path resolution
from src.strategies.specific.filters.time_filter import TimeFilter as NewTimeFilter

# Import contracts
from src.strategies.contracts.signal_contracts import SignalFrame


def create_test_signals(start_hour=0, end_hour=24, freq='15min'):
    """Create test SignalFrame with signals across different hours."""
    # Create timestamps across a full day
    timestamps = pd.date_range(
        '2024-01-01 00:00:00',
        '2024-01-01 23:59:00',
        freq=freq
    )
    
    # Create random BUY/SELL signals
    n = len(timestamps)
    signal_values = np.zeros(n, dtype=np.int8)
    
    # Generate signals at regular intervals
    for i in range(0, n, 4):  # Every 4th bar
        signal_values[i] = 1 if i % 8 == 0 else 2  # Alternate BUY/SELL
    
    signals = pd.Series(signal_values, index=timestamps, dtype='int8')
    
    return SignalFrame(
        signals=signals,
        indicator_data=None,
        signal_metadata={"source": "test"}
    )


def test_time_filter_parity():
    """Test that new TimeFilter matches old TimeManager behavior."""
    print("\n" + "="*60)
    print("TIME FILTER MIGRATION PARITY TEST")
    print("="*60)
    
    # Configuration (8:30 AM - 8:30 PM session)
    config = {
        'time_filter': {
            'enabled': True,
            'session_start': {'hour': 8, 'minute': 30},
            'session_end': {'hour': 20, 'minute': 30}
        }
    }
    
    # Create test signals
    signal_frame = create_test_signals()
    total_signals = signal_frame.count_by_type()["total"]
    
    print(f"\nInput: {total_signals} signals across full day (00:00-23:59)")
    
    # OLD IMPLEMENTATION
    print("\n" + "-"*60)
    print("OLD IMPLEMENTATION (TimeManager)")
    print("-"*60)
    
    old_filter = OldTimeManager(config)
    
    # Convert SignalFrame to old format (DataFrame with timestamp column)
    old_input = pd.DataFrame({
        'timestamp': signal_frame.signals.index,
        'signal': signal_frame.signals.map({1: "BUY", 2: "SELL"}).values
    })
    old_input = old_input[old_input['signal'].notna()]
    
    old_output = old_filter.filter_signals_by_time(old_input, 'timestamp')
    old_count = len(old_output)
    
    print(f"Output: {old_count} signals")
    print(f"Rejected: {total_signals - old_count}")
    
    # NEW IMPLEMENTATION
    print("\n" + "-"*60)
    print("NEW IMPLEMENTATION (TimeFilter)")
    print("-"*60)
    
    new_filter = NewTimeFilter(config, name="time_filter")
    
    # Create dummy df (not used by time filter)
    dummy_df = pd.DataFrame(index=signal_frame.signals.index)
    
    filter_result = new_filter.apply_filter(
        signal_frame=signal_frame,
        df=dummy_df,
        indicators={},
        ind_np={},
        mode="debug"
    )
    
    new_count = filter_result.signals_count
    
    print(f"Output: {new_count} signals")
    print(f"Rejected: {filter_result.metadata.signals_rejected}")
    print(f"\nMetadata:\n{filter_result.metadata}")
    
    # COMPARISON
    print("\n" + "="*60)
    print("COMPARISON")
    print("="*60)
    
    if old_count == new_count:
        print(f"✅ PASS: Signal counts match ({old_count} signals)")
        
        # Compare timestamps
        old_timestamps = set(old_output['timestamp'].values)
        new_timestamps = set(
            filter_result.signal_frame.signals[
                filter_result.signal_frame.signals != 0
            ].index
        )
        
        if old_timestamps == new_timestamps:
            print("✅ PASS: Timestamps match exactly")
        else:
            missing_in_new = old_timestamps - new_timestamps
            extra_in_new = new_timestamps - old_timestamps
            
            print(f"❌ FAIL: Timestamp mismatch")
            if missing_in_new:
                print(f"  Missing in new: {len(missing_in_new)}")
            if extra_in_new:
                print(f"  Extra in new: {len(extra_in_new)}")
            
            return False
        
        # Compare signal types
        old_buy_count = (old_output['signal'] == 'BUY').sum()
        old_sell_count = (old_output['signal'] == 'SELL').sum()
        new_counts = filter_result.signal_frame.count_by_type()
        
        if old_buy_count == new_counts['buy'] and old_sell_count == new_counts['sell']:
            print(f"✅ PASS: Signal types match (BUY: {old_buy_count}, SELL: {old_sell_count})")
        else:
            print(f"❌ FAIL: Signal type mismatch")
            print(f"  Old: BUY={old_buy_count}, SELL={old_sell_count}")
            print(f"  New: BUY={new_counts['buy']}, SELL={new_counts['sell']}")
            return False
        
        print("\n🎉 ALL TESTS PASSED - TimeFilter migration verified!")
        return True
        
    else:
        print(f"❌ FAIL: Signal count mismatch")
        print(f"  Old: {old_count}")
        print(f"  New: {new_count}")
        print(f"  Difference: {abs(old_count - new_count)}")
        return False


def test_disabled_filter():
    """Test that disabled filter passes all signals through."""
    print("\n" + "="*60)
    print("DISABLED FILTER TEST")
    print("="*60)
    
    config = {
        'time_filter': {
            'enabled': False
        }
    }
    
    signal_frame = create_test_signals()
    total_signals = signal_frame.count_by_type()["total"]
    
    new_filter = NewTimeFilter(config, name="time_filter")
    dummy_df = pd.DataFrame(index=signal_frame.signals.index)
    
    filter_result = new_filter.apply_filter(
        signal_frame=signal_frame,
        df=dummy_df,
        indicators={},
        ind_np={},
        mode="core"
    )
    
    if filter_result.signals_count == total_signals:
        print(f"✅ PASS: All {total_signals} signals passed (filter disabled)")
        print(f"Status: {filter_result.metadata.status.name}")
        return True
    else:
        print(f"❌ FAIL: Signal count changed when filter disabled")
        return False


def test_core_vs_debug_mode():
    """Test that core and debug modes produce same signal counts."""
    print("\n" + "="*60)
    print("CORE vs DEBUG MODE TEST")
    print("="*60)
    
    config = {
        'time_filter': {
            'enabled': True,
            'session_start': {'hour': 9, 'minute': 0},
            'session_end': {'hour': 17, 'minute': 0}
        }
    }
    
    signal_frame = create_test_signals()
    new_filter = NewTimeFilter(config)
    dummy_df = pd.DataFrame(index=signal_frame.signals.index)
    
    # Core mode
    core_result = new_filter.apply_filter(signal_frame, dummy_df, {}, {}, mode="core")
    core_count = core_result.signals_count
    
    # Debug mode
    debug_result = new_filter.apply_filter(signal_frame, dummy_df, {}, {}, mode="debug")
    debug_count = debug_result.signals_count
    
    if core_count == debug_count:
        print(f"✅ PASS: Both modes produce {core_count} signals")
        if core_result.metadata.execution_time_ms is not None:
            print(f"Core execution time: {core_result.metadata.execution_time_ms:.2f}ms")
        if debug_result.metadata.execution_time_ms is not None:
            print(f"Debug execution time: {debug_result.metadata.execution_time_ms:.2f}ms")
        return True
    else:
        print(f"❌ FAIL: Mode mismatch (core: {core_count}, debug: {debug_count})")
        return False


if __name__ == "__main__":
    results = []
    
    results.append(test_time_filter_parity())
    results.append(test_disabled_filter())
    results.append(test_core_vs_debug_mode())
    
    print("\n" + "="*60)
    print("FINAL RESULTS")
    print("="*60)
    
    if all(results):
        print("✅ ALL TESTS PASSED")
        sys.exit(0)
    else:
        print("❌ SOME TESTS FAILED")
        sys.exit(1)
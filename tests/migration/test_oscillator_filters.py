"""
Oscillator Filters (RSI + CCI) Migration Parity Test

Validates that new RSI and CCI filters produce identical results to old implementations.

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
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # Go up from tests/migration/
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ------------------------------------------------------------
# UNIFIED PATH RESOLUTION USING paths.py
# ------------------------------------------------------------
from src.utils.paths import (
    PROJECT_ROOT as PATHS_PROJECT_ROOT,  # Rename to avoid confusion
    SRC_DIR,
    CONTRACTS_DIR,
    MODULES_DIR,
    FILTERS_DIR,
    ensure_dir,
    contract_path,
    module_path,
    filter_path,
    migration_test_path  # Added this
)

# Import old implementations (legacy paths)
from src.strategies.filters.rsi_filter import RSIFilter as OldRSIFilter
from src.strategies.filters.cci_filter import CCIFilter as OldCCIFilter

#Filters not yet integrated in test
from src.strategies.filters.adx_filter import ADXFilter as OldADXFilter
from src.strategies.filters.bollinger_filter import BollingerFilter as OldBollingerFilter
from src.strategies.filters.choppiness_filter import ChoppinessFilter as OldChopinessFilter
from src.strategies.filters.dpo_filter import DPOFilter as OldDPOFilter
from src.strategies.filters.ma_filter import MAFilter as OldMAFilter
from src.strategies.filters.macd_filter import MACDFilter as OldMACDFilter
from src.strategies.filters.pivot_filter import PivotFilter as OldPivotFilter
from src.strategies.filters.supertrend_filter import SupertrendFilter as OldSupertrendFilter

# Import new implementations using path resolution
from src.strategies.specific.filters.rsi_filter import RSIFilter as NewRSIFilter
from src.strategies.specific.filters.cci_filter import CCIFilter as NewCCIFilter

#Filters not yet integrated in test
from src.strategies.specific.filters.adx_filter import ADXFilter as NewADXFilter
from src.strategies.specific.filters.bollinger_filter import BollingerFilter as NewBollingerFilter
from src.strategies.specific.filters.choppiness_filter import ChoppinessFilter as NewChopinessFilter
from src.strategies.specific.filters.dpo_filter import DPOFilter as NewDPOFilter
from src.strategies.specific.filters.ma_filter import MAFilter as NewMAFilter
from src.strategies.specific.filters.macd_filter import MACDFilter as NewMACDFilter
from src.strategies.specific.filters.pivot_filter import PivotFilter as NewPivotFilter
from src.strategies.specific.filters.supertrend_filter import SupertrendFilter as NewSupertrendFilter


# Import contracts using path resolution
from src.strategies.contracts.signal_contracts import SignalFrame


def create_test_data(n_bars=200):
    """Create realistic OHLCV test data."""
    np.random.seed(42)
    
    timestamps = pd.date_range('2024-01-01', periods=n_bars, freq='5min')
    
    # Generate realistic price action
    close = 100 + np.cumsum(np.random.randn(n_bars) * 0.5)
    high = close + np.abs(np.random.randn(n_bars) * 0.3)
    low = close - np.abs(np.random.randn(n_bars) * 0.3)
    open_ = close + np.random.randn(n_bars) * 0.2
    volume = np.random.randint(1000, 10000, n_bars)
    
    df = pd.DataFrame({
        'open': open_,
        'high': high,
        'low': low,
        'close': close,
        'volume': volume
    }, index=timestamps)
    
    return df.astype('float32')


def create_test_signals(df, signal_frequency=10):
    """Create test SignalFrame with BUY/SELL signals."""
    n = len(df)
    signal_values = np.zeros(n, dtype=np.int8)
    
    # Generate signals at regular intervals
    for i in range(0, n, signal_frequency):
        if i < n:
            signal_values[i] = 1 if i % (signal_frequency * 2) == 0 else 2
    
    signals = pd.Series(signal_values, index=df.index, dtype='int8')
    
    return SignalFrame(
        signals=signals,
        indicator_data=None,
        signal_metadata={"source": "test"}
    )


def test_rsi_filter_parity():
    """Test that new RSI filter matches old RSI filter behavior."""
    print("\n" + "="*60)
    print("RSI FILTER MIGRATION PARITY TEST")
    print("="*60)
    
    # Create test data
    df = create_test_data(n_bars=200)
    signal_frame = create_test_signals(df, signal_frequency=8)
    
    total_signals = signal_frame.count_by_type()["total"]
    buy_signals = signal_frame.count_by_type()["buy"]
    sell_signals = signal_frame.count_by_type()["sell"]
    
    print(f"\nInput: {total_signals} signals (BUY: {buy_signals}, SELL: {sell_signals})")
    
    # OLD IMPLEMENTATION
    print("\n" + "-"*60)
    print("OLD IMPLEMENTATION (RSIFilter)")
    print("-"*60)
    
    old_filter = OldRSIFilter(length=14, overbought=70.0, oversold=30.0, enabled=True)
    
    # Test BUY signals
    old_buy_mask = old_filter.apply_filter(df, is_long=True)
    old_buy_passed = old_buy_mask.sum()
    
    # Test SELL signals
    old_sell_mask = old_filter.apply_filter(df, is_long=False)
    old_sell_passed = old_sell_mask.sum()
    
    print(f"BUY signals passed: {old_buy_passed}/{len(df)}")
    print(f"SELL signals passed: {old_sell_passed}/{len(df)}")
    
    # NEW IMPLEMENTATION
    print("\n" + "-"*60)
    print("NEW IMPLEMENTATION (RSIFilter)")
    print("-"*60)
    
    new_filter = NewRSIFilter(length=14, overbought=70.0, oversold=30.0, enabled=True)
    
    # Compute indicators
    indicators = {}
    ind_np = {}
    new_filter.compute_indicators(df, indicators, ind_np)
    
    # Apply filter
    filter_result = new_filter.apply_filter(
        signal_frame=signal_frame,
        df=df,
        indicators=indicators,
        ind_np=ind_np,
        mode="debug"
    )
    
    new_counts = filter_result.signal_frame.count_by_type()
    
    print(f"Output: {filter_result.signals_count} signals")
    print(f"  BUY: {new_counts['buy']}")
    print(f"  SELL: {new_counts['sell']}")
    print(f"Rejected: {filter_result.metadata.signals_rejected}")
    print(f"\nMetadata:\n{filter_result.metadata}")
    
    # COMPARISON - Compare at signal locations
    print("\n" + "="*60)
    print("COMPARISON")
    print("="*60)
    
    # Extract signal locations
    signal_indices = np.where(signal_frame.signals.values != 0)[0]
    is_buy = signal_frame.signals.values[signal_indices] == 1
    is_sell = signal_frame.signals.values[signal_indices] == 2
    
    # Old filter results at signal locations
    old_buy_results = old_buy_mask.iloc[signal_indices[is_buy]]
    old_sell_results = old_sell_mask.iloc[signal_indices[is_sell]]
    
    old_passed_buy = old_buy_results.sum()
    old_passed_sell = old_sell_results.sum()
    old_total = old_passed_buy + old_passed_sell
    
    # New filter results
    new_passed_buy = new_counts['buy']
    new_passed_sell = new_counts['sell']
    new_total = new_counts['total']
    
    print(f"Old: {old_total} signals passed (BUY: {old_passed_buy}, SELL: {old_passed_sell})")
    print(f"New: {new_total} signals passed (BUY: {new_passed_buy}, SELL: {new_passed_sell})")
    
    if old_total == new_total and old_passed_buy == new_passed_buy and old_passed_sell == new_passed_sell:
        print(f"✅ PASS: Signal counts match exactly!")
        return True
    else:
        print(f"❌ FAIL: Signal count mismatch")
        print(f"  Total difference: {abs(old_total - new_total)}")
        print(f"  BUY difference: {abs(old_passed_buy - new_passed_buy)}")
        print(f"  SELL difference: {abs(old_passed_sell - new_passed_sell)}")
        return False


def test_cci_filter_parity():
    """Test that new CCI filter matches old CCI filter behavior."""
    print("\n" + "="*60)
    print("CCI FILTER MIGRATION PARITY TEST")
    print("="*60)
    
    # Create test data
    df = create_test_data(n_bars=200)
    signal_frame = create_test_signals(df, signal_frequency=8)
    
    total_signals = signal_frame.count_by_type()["total"]
    buy_signals = signal_frame.count_by_type()["buy"]
    sell_signals = signal_frame.count_by_type()["sell"]
    
    print(f"\nInput: {total_signals} signals (BUY: {buy_signals}, SELL: {sell_signals})")
    
    # OLD IMPLEMENTATION
    print("\n" + "-"*60)
    print("OLD IMPLEMENTATION (CCIFilter)")
    print("-"*60)
    
    old_filter = OldCCIFilter(length=20, overbought=100, oversold=-100, enabled=True)
    
    # Test BUY signals
    old_buy_mask = old_filter.apply_filter(df, is_long=True)
    old_buy_passed = old_buy_mask.sum()
    
    # Test SELL signals
    old_sell_mask = old_filter.apply_filter(df, is_long=False)
    old_sell_passed = old_sell_mask.sum()
    
    print(f"BUY signals passed: {old_buy_passed}/{len(df)}")
    print(f"SELL signals passed: {old_sell_passed}/{len(df)}")
    
    # NEW IMPLEMENTATION
    print("\n" + "-"*60)
    print("NEW IMPLEMENTATION (CCIFilter)")
    print("-"*60)
    
    new_filter = NewCCIFilter(length=20, overbought=100, oversold=-100, enabled=True)
    
    # Compute indicators
    indicators = {}
    ind_np = {}
    new_filter.compute_indicators(df, indicators, ind_np)
    
    # Apply filter
    filter_result = new_filter.apply_filter(
        signal_frame=signal_frame,
        df=df,
        indicators=indicators,
        ind_np=ind_np,
        mode="debug"
    )
    
    new_counts = filter_result.signal_frame.count_by_type()
    
    print(f"Output: {filter_result.signals_count} signals")
    print(f"  BUY: {new_counts['buy']}")
    print(f"  SELL: {new_counts['sell']}")
    print(f"Rejected: {filter_result.metadata.signals_rejected}")
    print(f"\nMetadata:\n{filter_result.metadata}")
    
    # COMPARISON - Compare at signal locations
    print("\n" + "="*60)
    print("COMPARISON")
    print("="*60)
    
    # Extract signal locations
    signal_indices = np.where(signal_frame.signals.values != 0)[0]
    is_buy = signal_frame.signals.values[signal_indices] == 1
    is_sell = signal_frame.signals.values[signal_indices] == 2
    
    # Old filter results at signal locations
    old_buy_results = old_buy_mask.iloc[signal_indices[is_buy]]
    old_sell_results = old_sell_mask.iloc[signal_indices[is_sell]]
    
    old_passed_buy = old_buy_results.sum()
    old_passed_sell = old_sell_results.sum()
    old_total = old_passed_buy + old_passed_sell
    
    # New filter results
    new_passed_buy = new_counts['buy']
    new_passed_sell = new_counts['sell']
    new_total = new_counts['total']
    
    print(f"Old: {old_total} signals passed (BUY: {old_passed_buy}, SELL: {old_passed_sell})")
    print(f"New: {new_total} signals passed (BUY: {new_passed_buy}, SELL: {new_passed_sell})")
    
    if old_total == new_total and old_passed_buy == new_passed_buy and old_passed_sell == new_passed_sell:
        print(f"✅ PASS: Signal counts match exactly!")
        return True
    else:
        print(f"❌ FAIL: Signal count mismatch")
        print(f"  Total difference: {abs(old_total - new_total)}")
        print(f"  BUY difference: {abs(old_passed_buy - new_passed_buy)}")
        print(f"  SELL difference: {abs(old_passed_sell - new_passed_sell)}")
        return False


def test_disabled_filters():
    """Test that disabled filters pass all signals through."""
    print("\n" + "="*60)
    print("DISABLED FILTERS TEST")
    print("="*60)
    
    df = create_test_data(n_bars=100)
    signal_frame = create_test_signals(df, signal_frequency=5)
    total_signals = signal_frame.count_by_type()["total"]
    
    results = []
    
    # Test RSI disabled
    rsi_filter = NewRSIFilter(enabled=False)
    rsi_filter.compute_indicators(df, {}, {})
    rsi_result = rsi_filter.apply_filter(signal_frame, df, {}, {}, mode="core")
    
    if rsi_result.signals_count == total_signals:
        print(f"✅ RSI: All {total_signals} signals passed (disabled)")
        results.append(True)
    else:
        print(f"❌ RSI: Signal count changed when disabled")
        results.append(False)
    
    # Test CCI disabled
    cci_filter = NewCCIFilter(enabled=False)
    cci_filter.compute_indicators(df, {}, {})
    cci_result = cci_filter.apply_filter(signal_frame, df, {}, {}, mode="core")
    
    if cci_result.signals_count == total_signals:
        print(f"✅ CCI: All {total_signals} signals passed (disabled)")
        results.append(True)
    else:
        print(f"❌ CCI: Signal count changed when disabled")
        results.append(False)
    
    return all(results)


def test_core_vs_debug_mode():
    """Test that core and debug modes produce same signal counts."""
    print("\n" + "="*60)
    print("CORE vs DEBUG MODE TEST")
    print("="*60)
    
    df = create_test_data(n_bars=150)
    signal_frame = create_test_signals(df, signal_frequency=6)
    
    results = []
    
    # Test RSI
    rsi_filter = NewRSIFilter(length=14, overbought=70, oversold=30)
    indicators = {}
    ind_np = {}
    rsi_filter.compute_indicators(df, indicators, ind_np)
    
    rsi_core = rsi_filter.apply_filter(signal_frame, df, indicators, ind_np, mode="core")
    rsi_debug = rsi_filter.apply_filter(signal_frame, df, indicators, ind_np, mode="debug")
    
    if rsi_core.signals_count == rsi_debug.signals_count:
        print(f"✅ RSI: Both modes produce {rsi_core.signals_count} signals")
        results.append(True)
    else:
        print(f"❌ RSI: Mode mismatch (core: {rsi_core.signals_count}, debug: {rsi_debug.signals_count})")
        results.append(False)
    
    # Test CCI
    cci_filter = NewCCIFilter(length=20, overbought=100, oversold=-100)
    indicators_cci = {}
    ind_np_cci = {}
    cci_filter.compute_indicators(df, indicators_cci, ind_np_cci)
    
    cci_core = cci_filter.apply_filter(signal_frame, df, indicators_cci, ind_np_cci, mode="core")
    cci_debug = cci_filter.apply_filter(signal_frame, df, indicators_cci, ind_np_cci, mode="debug")
    
    if cci_core.signals_count == cci_debug.signals_count:
        print(f"✅ CCI: Both modes produce {cci_core.signals_count} signals")
        results.append(True)
    else:
        print(f"❌ CCI: Mode mismatch (core: {cci_core.signals_count}, debug: {cci_debug.signals_count})")
        results.append(False)
    
    return all(results)


if __name__ == "__main__":
    results = []
    
    results.append(test_rsi_filter_parity())
    results.append(test_cci_filter_parity())
    results.append(test_disabled_filters())
    results.append(test_core_vs_debug_mode())
    
    print("\n" + "="*60)
    print("FINAL RESULTS - BATCH 1 (OSCILLATOR FILTERS)")
    print("="*60)
    
    if all(results):
        print("✅ ALL TESTS PASSED - Batch 1 migration verified!")
        sys.exit(0)
    else:
        print("❌ SOME TESTS FAILED")
        sys.exit(1)
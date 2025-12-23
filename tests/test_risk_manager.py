#!/usr/bin/env python3
#!/usr/bin/env python3
"""
Test script for Risk Manager module.
Validates SL/TP calculation and risk validation functionality.
"""

import sys
import os

# Add src to path (since tests/ is at same level as src/)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.strategies.trade_management.risk_manager import RiskManager

import pandas as pd
import numpy as np

def create_sample_ohlcv_data():
    """Create sample OHLCV data with realistic price movements."""
    np.random.seed(42)
    
    # Create date range
    dates = pd.date_range(start='2024-12-01', end='2025-12-15', freq='1h')
    
    # Generate price data with trend and volatility
    n_periods = len(dates)
    base_price = 18000.0
    returns = np.random.normal(0.0001, 0.005, n_periods)  # Small upward bias
    
    # Cumulative returns
    cumulative_returns = np.cumsum(returns)
    close_prices = base_price * (1 + cumulative_returns)
    
    # Create OHLCV data
    data = {
        'timestamp': dates,
        'open': close_prices * (1 + np.random.normal(0, 0.001, n_periods)),
        'high': close_prices * (1 + np.abs(np.random.normal(0.002, 0.001, n_periods))),
        'low': close_prices * (1 - np.abs(np.random.normal(0.002, 0.001, n_periods))),
        'close': close_prices,
        'volume': np.random.lognormal(10, 1, n_periods)
    }
    
    df = pd.DataFrame(data)
    df.set_index('timestamp', inplace=True)
    
    return df

def test_sl_tp_calculation(risk_manager):
    """Test SL/TP calculation with different scenarios."""
    print("\n" + "-" * 60)
    print("TESTING SL/TP CALCULATION")
    print("-" * 60)
    
    test_cases = [
        {
            'name': 'Long position, normal ATR',
            'entry_price': 18250.0,
            'is_long': True,
            'atr_value': 45.0,
            'expected_rr_ratio': 2.0
        },
        {
            'name': 'Short position, normal ATR',
            'entry_price': 18100.0,
            'is_long': False,
            'atr_value': 42.0,
            'expected_rr_ratio': 2.0
        },
        {
            'name': 'Long position, high volatility',
            'entry_price': 18300.0,
            'is_long': True,
            'atr_value': 85.0,
            'expected_rr_ratio': 2.0
        },
        {
            'name': 'Long position, very low ATR',
            'entry_price': 18200.0,
            'is_long': True,
            'atr_value': 5.0,
            'expected_rr_ratio': 2.0
        }
    ]
    
    for test in test_cases:
        print(f"\n🔹 Test: {test['name']}")
        print(f"   Entry: {test['entry_price']:.2f}")
        print(f"   Direction: {'Long' if test['is_long'] else 'Short'}")
        print(f"   ATR: {test['atr_value']:.2f}")
        
        sl, tp = risk_manager.calculate_sl_tp(
            test['entry_price'],
            test['is_long'],
            test['atr_value']
        )
        
        if sl is not None and tp is not None:
            # Calculate actual R:R ratio
            sl_distance = abs(test['entry_price'] - sl)
            tp_distance = abs(tp - test['entry_price'])
            actual_rr = tp_distance / sl_distance if sl_distance > 0 else 0
            
            print(f"✅ SL/TP calculated successfully")
            print(f"   Stop Loss: {sl:.2f}")
            print(f"   Take Profit: {tp:.2f}")
            print(f"   SL Distance: {sl_distance:.2f}")
            print(f"   TP Distance: {tp_distance:.2f}")
            print(f"   Actual R:R Ratio: 1:{actual_rr:.2f}")
            
            # Verify R:R ratio
            if abs(actual_rr - test['expected_rr_ratio']) < 0.1:
                print(f"✅ R:R ratio correct (expected 1:{test['expected_rr_ratio']})")
            else:
                print(f"⚠️  R:R ratio mismatch (expected 1:{test['expected_rr_ratio']})")
        else:
            print("❌ SL/TP calculation failed")

def test_risk_validation(risk_manager):
    """Test risk percentile validation."""
    print("\n" + "-" * 60)
    print("TESTING RISK PERCENTILE VALIDATION")
    print("-" * 60)
    
    # Assume annual range is ~2000 points for DAX
    test_cases = [
        {
            'name': 'Low risk (within limits)',
            'entry_price': 18250.0,
            'stop_loss': 18200.0,  # 50 point risk
            'is_long': True,
            'expected_result': True
        },
        {
            'name': 'High risk (exceeds limit)',
            'entry_price': 18250.0,
            'stop_loss': 17750.0,  # 500 point risk (~25% of 2000 range)
            'is_long': True,
            'expected_result': False  # Should be rejected
        },
        {
            'name': 'Short position, moderate risk',
            'entry_price': 18100.0,
            'stop_loss': 18150.0,  # 50 point risk
            'is_long': False,
            'expected_result': True
        }
    ]
    
    for test in test_cases:
        print(f"\n🔹 Test: {test['name']}")
        print(f"   Entry: {test['entry_price']:.2f}")
        print(f"   Stop Loss: {test['stop_loss']:.2f}")
        print(f"   Risk Distance: {abs(test['entry_price'] - test['stop_loss']):.2f}")
        
        can_trade, adjusted_sl, comment = risk_manager.validate_risk_percentile(
            test['entry_price'],
            test['stop_loss'],
            test['is_long']
        )
        
        print(f"   Result: {'✅ Can trade' if can_trade else '❌ Cannot trade'}")
        print(f"   Comment: {comment}")
        
        if adjusted_sl is not None and adjusted_sl != test['stop_loss']:
            print(f"   Adjusted SL: {adjusted_sl:.2f}")
        
        if can_trade == test['expected_result']:
            print(f"✅ Test passed")
        else:
            print(f"❌ Test failed (expected: {test['expected_result']})")

def test_risk_manager_disabled():
    """Test risk manager with disabled features."""
    print("\n" + "-" * 60)
    print("TESTING DISABLED RISK MANAGER")
    print("-" * 60)
    
    # Create configuration with disabled features
    config_disabled = {
        'sl_tp': {'enabled': False},
        'risk_management': {'enabled': False}
    }
    
    # Create minimal data
    sample_data = create_sample_ohlcv_data().iloc[:100]
    
    print("Initializing RiskManager with disabled features...")
    try:
        risk_manager_disabled = RiskManager(config_disabled, sample_data)
        
        # Test SL/TP calculation (should return None, None)
        sl, tp = risk_manager_disabled.calculate_sl_tp(18000.0, True, 40.0)
        
        if sl is None and tp is None:
            print("✅ SL/TP correctly disabled")
        else:
            print("❌ SL/TP should be disabled but returned values")
        
        # Test risk validation (should always allow)
        can_trade, adjusted_sl, comment = risk_manager_disabled.validate_risk_percentile(
            18000.0, 17900.0, True
        )
        
        if can_trade:
            print("✅ Risk validation correctly disabled (always allows)")
        else:
            print("❌ Risk validation disabled but rejected trade")
            
    except Exception as e:
        print(f"❌ Disabled risk manager test failed: {e}")

def test_risk_manager():
    """Run comprehensive tests on RiskManager."""
    print("=" * 60)
    print("TESTING RISK MANAGER MODULE")
    print("=" * 60)
    
    # Create sample OHLCV data
    print("\n1. Creating sample OHLCV data...")
    sample_data = create_sample_ohlcv_data()
    print(f"✅ Created {len(sample_data)} OHLCV records")
    print(f"   Date range: {sample_data.index[0]} to {sample_data.index[-1]}")
    print(f"   Price range: {sample_data['low'].min():.2f} - {sample_data['high'].max():.2f}")
    
    # Create configuration
    config = {
        'sl_tp': {
            'enabled': True,
            'atr_length': 14,
            'sl_multiplier': 1.4,
            'risk_to_reward_ratio': 2.0
        },
        'risk_management': {
            'enabled': True,
            'max_risk_percentile': 0.05,  # 5% of annual range
            'allow_exceed_limit': False
        }
    }
    
    print("\n2. Initializing RiskManager...")
    try:
        risk_manager = RiskManager(config, sample_data)
        print("✅ RiskManager initialized successfully")
        
        # Test SL/TP calculation
        test_sl_tp_calculation(risk_manager)
        
        # Test risk validation
        test_risk_validation(risk_manager)
        
        # Test disabled features
        test_risk_manager_disabled()
        
    except Exception as e:
        print(f"❌ RiskManager test failed: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("RISK MANAGER TEST COMPLETE")
    print("=" * 60)

def quick_smoke_test():
    """Quick smoke test for basic functionality."""
    print("\n🚀 QUICK SMOKE TEST")
    print("-" * 40)
    
    # Minimal test
    config = {
        'sl_tp': {'enabled': True, 'sl_multiplier': 1.4, 'risk_to_reward_ratio': 2.0},
        'risk_management': {'enabled': False}
    }
    
    # Create tiny dataset
    dates = pd.date_range(start='2025-12-01', periods=50, freq='1h')
    data = pd.DataFrame({
        'open': 18000 + np.random.randn(50) * 10,
        'high': 18050 + np.random.randn(50) * 10,
        'low': 17950 + np.random.randn(50) * 10,
        'close': 18000 + np.cumsum(np.random.randn(50) * 0.1),
        'volume': np.random.randint(1000, 10000, 50)
    }, index=dates)
    
    try:
        rm = RiskManager(config, data)
        
        # Quick calculation
        sl, tp = rm.calculate_sl_tp(18250.0, True, 35.0)
        
        if sl is not None and tp is not None:
            print("✅ Smoke test passed!")
            print(f"   SL: {sl:.2f}, TP: {tp:.2f}")
            print(f"   R:R Ratio: 1:{abs(tp-18250)/abs(sl-18250):.2f}")
        else:
            print("❌ Smoke test failed - no SL/TP calculated")
            
    except Exception as e:
        print(f"❌ Smoke test failed: {e}")

if __name__ == "__main__":
    # Run comprehensive test
    test_risk_manager()
    
    # Run quick smoke test
    quick_smoke_test()
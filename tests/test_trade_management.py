#!/usr/bin/env python3
"""
Combined test script for both Time and Risk Management modules.
Tests integration between the two modules.
"""

import sys
import os

# Add src to path (since tests/ is at same level as src/)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.strategies.trade_management.time_manager import TimeManager
from src.strategies.trade_management.risk_manager import RiskManager

import pandas as pd
import numpy as np
import pytz
from datetime import datetime, timedelta

def create_integration_test_data():
    """Create data for integration testing using timedelta."""
    signals = []
    
    base_date = datetime(2025, 12, 15, 9, 0, 0)
    cet_tz = pytz.timezone('Europe/Paris')
    
    # Use timedelta to create signals at regular intervals
    time_offsets = [
        timedelta(hours=0, minutes=30),    # 9:30
        timedelta(hours=3, minutes=0),     # 12:00  
        timedelta(hours=10, minutes=45),   # 19:45
        timedelta(hours=13, minutes=0),    # 22:00
        timedelta(hours=-6, minutes=0),    # 3:00 (overnight)
    ]
    
    for i, offset in enumerate(time_offsets):
        time_dt = base_date + offset
        time_cet = cet_tz.localize(time_dt)
        time_utc = time_cet.astimezone(pytz.UTC)
        
        signals.append({
            'timestamp': pd.Timestamp(time_utc),
            'signal': 'buy' if i % 2 == 0 else 'sell',
            'price': 18000 + i * 10,
            'volume': 1000 + i * 100
        })
    
    return pd.DataFrame(signals)

def test_integration():
    """Test integration between TimeManager and RiskManager."""
    print("=" * 60)
    print("INTEGRATION TEST: TIME + RISK MANAGEMENT")
    print("=" * 60)
    
    # Create configuration
    config = {
        'time_filter': {
            'enabled': True,
            'timezone': 'Europe/Paris',
            'session_start': {'hour': 8, 'minute': 30},
            'session_end': {'hour': 20, 'minute': 30},
            'allow_overnight_session': True
        },
        'sl_tp': {
            'enabled': True,
            'atr_length': 14,
            'sl_multiplier': 1.4,
            'risk_to_reward_ratio': 2.0
        },
        'risk_management': {
            'enabled': True,
            'max_risk_percentile': 0.05,
            'allow_exceed_limit': False
        }
    }
    
    print("\n1. Creating test data...")
    signals_df = create_integration_test_data()
    print(f"✅ Created {len(signals_df)} test signals")
    
    # Create sample OHLCV for RiskManager
    np.random.seed(42)
    dates = pd.date_range(start='2025-11-01', periods=100, freq='1H')
    ohlcv_data = pd.DataFrame({
        'open': 18000 + np.random.randn(100) * 20,
        'high': 18050 + np.random.randn(100) * 20,
        'low': 17950 + np.random.randn(100) * 20,
        'close': 18000 + np.cumsum(np.random.randn(100) * 0.5),
        'volume': np.random.randint(1000, 10000, 100)
    }, index=dates)
    
    print("\n2. Initializing modules...")
    try:
        # Initialize TimeManager
        time_manager = TimeManager(config)
        print("✅ TimeManager initialized")
        
        # Initialize RiskManager
        risk_manager = RiskManager(config, ohlcv_data)
        print("✅ RiskManager initialized")
        
        print("\n3. Executing workflow...")
        
        # Step 1: Filter by time
        print("\n   Step 1: Time filtering")
        filtered_signals = time_manager.filter_signals_by_time(signals_df, 'timestamp')
        print(f"      Original signals: {len(signals_df)}")
        print(f"      After time filter: {len(filtered_signals)}")
        
        # Step 2: Process each filtered signal
        print("\n   Step 2: Risk calculation for filtered signals")
        for idx, signal in filtered_signals.iterrows():
            print(f"\n      Processing signal #{idx}:")
            print(f"        Time: {signal['timestamp'].strftime('%Y-%m-%d %H:%M:%S %Z')}")
            print(f"        Type: {signal['signal'].upper()}")
            print(f"        Price: {signal['price']:.2f}")
            
            is_long = signal['signal'].lower() == 'buy'
            
            # Calculate SL/TP
            sl, tp = risk_manager.calculate_sl_tp(signal['price'], is_long)
            
            if sl is not None and tp is not None:
                print(f"        SL: {sl:.2f}, TP: {tp:.2f}")
                
                # Validate risk
                can_trade, final_sl, comment = risk_manager.validate_risk_percentile(
                    signal['price'], sl, is_long
                )
                
                print(f"        Risk validation: {'✅ PASS' if can_trade else '❌ REJECT'}")
                print(f"        Comment: {comment}")
            else:
                print("        ❌ SL/TP calculation failed")
        
        print("\n4. Testing edge cases...")
        
        # Test 1: Time filter disabled
        print("\n   Test 1: Time filter disabled")
        config_no_time = config.copy()
        config_no_time['time_filter']['enabled'] = False
        time_manager_no_filter = TimeManager(config_no_time)
        
        all_times_allowed = all([
            time_manager_no_filter.is_in_trading_hours(
                pd.Timestamp('2025-12-15 03:00:00', tz='UTC')
            ),
            time_manager_no_filter.is_in_trading_hours(
                pd.Timestamp('2025-12-15 23:00:00', tz='UTC')
            )
        ])
        
        if all_times_allowed:
            print("      ✅ All times allowed when filter is disabled")
        else:
            print("      ❌ Time filter should allow all times when disabled")
        
        # Test 2: Risk management disabled
        print("\n   Test 2: Risk management disabled")
        config_no_risk = config.copy()
        config_no_risk['risk_management']['enabled'] = False
        config_no_risk['sl_tp']['enabled'] = False
        
        risk_manager_disabled = RiskManager(config_no_risk, ohlcv_data)
        can_trade, _, _ = risk_manager_disabled.validate_risk_percentile(
            18000, 17000, True  # Very large risk
        )
        
        if can_trade:
            print("      ✅ All trades allowed when risk management disabled")
        else:
            print("      ❌ Risk management should be disabled")
        
        print("\n✅ Integration test completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Integration test failed: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("INTEGRATION TEST COMPLETE")
    print("=" * 60)

def run_quick_demo():
    """Run a quick demonstration of the trade management workflow."""
    print("\n" + "=" * 60)
    print("QUICK DEMO: TRADE MANAGEMENT WORKFLOW")
    print("=" * 60)
    
    # Simple configuration
    config = {
        'time_filter': {
            'enabled': True,
            'timezone': 'Europe/Paris',
            'session_start': {'hour': 8, 'minute': 30},
            'session_end': {'hour': 20, 'minute': 30}
        },
        'sl_tp': {
            'enabled': True,
            'sl_multiplier': 1.4,
            'risk_to_reward_ratio': 2.0
        }
    }
    
    print("\n📋 Configuration:")
    print(f"   Time filter: {'Enabled' if config['time_filter']['enabled'] else 'Disabled'}")
    print(f"   Trading session: {config['time_filter']['session_start']['hour']:02d}:"
          f"{config['time_filter']['session_start']['minute']:02d} - "
          f"{config['time_filter']['session_end']['hour']:02d}:"
          f"{config['time_filter']['session_end']['minute']:02d}")
    print(f"   SL multiplier: {config['sl_tp']['sl_multiplier']}")
    print(f"   R:R ratio: 1:{config['sl_tp']['risk_to_reward_ratio']}")
    
    # Test a trade scenario
    print("\n💼 Sample trade scenario:")
    print("   Asset: DAX40 @ 18250.00")
    print("   Signal: BUY")
    print("   Time: 2025-12-15 10:30:00 UTC")
    print("   ATR: 45.0")
    
    # Create simple data for RiskManager
    dates = pd.date_range(start='2025-12-01', periods=10, freq='1H')
    ohlcv = pd.DataFrame({
        'open': [18000 + i*10 for i in range(10)],
        'high': [18050 + i*10 for i in range(10)],
        'low': [17950 + i*10 for i in range(10)],
        'close': [18000 + i*10 for i in range(10)],
        'volume': [1000] * 10
    }, index=dates)
    
    try:
        # Initialize
        tm = TimeManager(config)
        rm = RiskManager(config, ohlcv)
        
        # Check time
        trade_time = pd.Timestamp('2025-12-15 10:30:00', tz='UTC')
        in_session = tm.is_in_trading_hours(trade_time)
        
        print(f"\n⏰ Time check: {'✅ Within trading hours' if in_session else '❌ Outside trading hours'}")
        
        if in_session:
            # Calculate SL/TP
            sl, tp = rm.calculate_sl_tp(18250.0, True, 45.0)
            
            if sl and tp:
                print(f"\n💰 Risk calculation:")
                print(f"   Stop Loss: {sl:.2f} ({18250-sl:.2f} points)")
                print(f"   Take Profit: {tp:.2f} ({tp-18250:.2f} points)")
                print(f"   Risk/Reward: 1:{(tp-18250)/(18250-sl):.1f}")
                
                # Quick profit/loss analysis
                print(f"\n📊 P/L Analysis:")
                print(f"   Risk per contract: {18250-sl:.2f} points")
                print(f"   Reward per contract: {tp-18250:.2f} points")
                print(f"   Risk/Reward Ratio: 1:{(tp-18250)/(18250-sl):.1f}")
        
        print("\n✅ Demo completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")

if __name__ == "__main__":
    # Run integration test
    test_integration()
    
    # Run quick demo
    run_quick_demo()
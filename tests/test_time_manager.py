#!/usr/bin/env python3
"""
Test script for Time Manager module.
Works with direct hour/minute without timezone conversion.
"""

import sys
import os

# Add src to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.strategies.trade_management.time_manager import TimeManager
import pandas as pd
from datetime import datetime

def create_sample_data():
    """Create sample timestamp data in CET (naive)."""
    base_date = datetime(2025, 12, 15)
    
    # Trading hours: 8:30-20:30
    test_cases = [
        # (hour, minute, should be in session)
        (9, 0, True, "9:00 - IN"),
        (12, 30, True, "12:30 - IN"),
        (20, 0, True, "20:00 - IN"),
        (20, 45, False, "20:45 - OUT (after 20:30)"),
        (8, 0, False, "8:00 - OUT (before 8:30)"),
        (21, 0, False, "21:00 - OUT"),
        (2, 0, False, "2:00 - OUT (overnight)"),
    ]
    
    print("Creating test cases:")
    for hour, minute, expected, description in test_cases:
        print(f"  {hour:02d}:{minute:02d} - {description}")
    
    times = []
    expected_results = []
    
    for hour, minute, expected, description in test_cases:
        time_dt = base_date.replace(hour=hour, minute=minute, second=0)
        times.append(time_dt)
        expected_results.append(expected)
    
    df = pd.DataFrame({
        'timestamp': times,
        'value': range(len(times)),
        'expected_in_session': expected_results
    })
    
    return df

def test_simple_time_manager():
    """Run tests on simplified TimeManager."""
    print("=" * 60)
    print("TESTING SIMPLIFIED TIME MANAGER")
    print("=" * 60)
    
    # Create sample data
    print("\n1. Creating sample data...")
    sample_df = create_sample_data()
    print(f"✅ Created {len(sample_df)} test timestamps")
    
    # Create configuration
    config = {
        'time_filter': {
            'enabled': True,
            'session_start': {'hour': 8, 'minute': 30},
            'session_end': {'hour': 20, 'minute': 30},
        }
    }
    
    print("\n2. Initializing TimeManager...")
    try:
        time_manager = TimeManager(config)
        print("✅ TimeManager initialized successfully")
        print(f"   Session: {config['time_filter']['session_start']['hour']:02d}:"
              f"{config['time_filter']['session_start']['minute']:02d} - "
              f"{config['time_filter']['session_end']['hour']:02d}:"
              f"{config['time_filter']['session_end']['minute']:02d}")
    except Exception as e:
        print(f"❌ TimeManager initialization failed: {e}")
        return
    
    print("\n3. Testing individual timestamps...")
    passed = 0
    failed = 0
    
    for idx, row in sample_df.iterrows():
        timestamp = pd.Timestamp(row['timestamp'])
        is_in_session = time_manager.is_in_trading_hours(timestamp)
        expected = row['expected_in_session']
        
        if is_in_session == expected:
            status = "✅ PASS"
            passed += 1
        else:
            status = "❌ FAIL"
            failed += 1
        
        # Get session info only if needed (for failed tests)
        if is_in_session != expected:
            session_info = time_manager.get_session_info(timestamp)
            print(f"\n{status} - Test #{idx+1}")
            print(f"   Time: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   Session info: {session_info}")
            print(f"   Expected: {'IN session' if expected else 'OUT of session'}")
            print(f"   Got: {'IN session' if is_in_session else 'OUT of session'}")
        else:
            print(f"\n{status} - Test #{idx+1}: {timestamp.strftime('%H:%M')} - "
                  f"{'IN' if is_in_session else 'OUT'} (as expected)")
    
    print(f"\n4. Individual tests: {passed} passed, {failed} failed")
    
    print("\n5. Testing batch filtering...")
    try:
        filtered_df = time_manager.filter_signals_by_time(sample_df, 'timestamp')
        
        print(f"✅ Batch filtering completed")
        print(f"   Original records: {len(sample_df)}")
        print(f"   Filtered records: {len(filtered_df)}")
        
        # Show which ones were filtered out
        if len(filtered_df) < len(sample_df):
            filtered_out = sample_df[~sample_df.index.isin(filtered_df.index)]
            print(f"   Filtered out: {len(filtered_out)} records")
            print("\n   Samples filtered out:")
            for idx, row in filtered_out.head(2).iterrows():
                print(f"     - {row['timestamp'].strftime('%H:%M')} (expected: {'IN' if row['expected_in_session'] else 'OUT'})")
        
        if not filtered_df.empty:
            print("\n   Sample filtered timestamps (kept):")
            for idx, row in filtered_df.head(2).iterrows():
                print(f"     - {row['timestamp'].strftime('%H:%M')}")
    except Exception as e:
        print(f"❌ Batch filtering failed: {e}")
    
    print("\n6. Testing edge cases...")
    
    # Test exactly at session boundaries
    edge_test_cases = [
        ('At session start', datetime(2025, 12, 15, 8, 30, 0), True),
        ('1 min before start', datetime(2025, 12, 15, 8, 29, 0), False),
        ('At session end', datetime(2025, 12, 15, 20, 30, 0), False),  # Exclusive
        ('1 min before end', datetime(2025, 12, 15, 20, 29, 0), True),
    ]
    
    edge_passed = 0
    edge_total = 0
    
    for name, time_dt, expected in edge_test_cases:
        edge_total += 1
        is_in_session = time_manager.is_in_trading_hours(pd.Timestamp(time_dt))
        if is_in_session == expected:
            status = "✅"
            edge_passed += 1
        else:
            status = "❌"
        
        print(f"   {status} {name}: {time_dt.strftime('%H:%M')} -> "
              f"{'IN' if is_in_session else 'OUT'} (expected: {'IN' if expected else 'OUT'})")
    
    print(f"   Edge cases: {edge_passed}/{edge_total} passed")
    
    print("\n7. Testing with time filter disabled...")
    config_disabled = config.copy()
    config_disabled['time_filter']['enabled'] = False
    
    try:
        time_manager_disabled = TimeManager(config_disabled)
        
        # Test times that should be outside session when enabled
        test_times = [
            pd.Timestamp(datetime(2025, 12, 15, 2, 0, 0)),   # Night
            pd.Timestamp(datetime(2025, 12, 15, 10, 0, 0)),  # Morning
            pd.Timestamp(datetime(2025, 12, 15, 22, 0, 0)),  # Evening
        ]
        
        all_allowed = all(time_manager_disabled.is_in_trading_hours(t) for t in test_times)
        
        if all_allowed:
            print("✅ Time filter disabled correctly - all times allowed")
        else:
            print("❌ Time filter should be disabled but isn't")
            
    except Exception as e:
        print(f"❌ Test with disabled filter failed: {e}")
    
    # REMOVED: Section 8 (overnight testing)
    
    print("\n" + "=" * 60)
    print("SIMPLIFIED TIME MANAGER TEST COMPLETE")
    print(f"✅ {passed} tests passed")
    if failed > 0:
        print(f"❌ {failed} tests failed")
    print("=" * 60)

if __name__ == "__main__":
    test_simple_time_manager()
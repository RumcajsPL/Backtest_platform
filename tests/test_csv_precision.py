# tests/test_csv_precision.py
import pandas as pd
from datetime import datetime
import tempfile
import os

def test_csv_datetime_precision():
    """Test if CSV preserves second precision"""
    
    # Create test data with second precision
    test_data = [
        {
            'trade_id': 1,
            'entry_time': pd.Timestamp('2025-12-24 15:35:00'),
            'exit_time': pd.Timestamp('2025-12-24 15:37:51'),  # Has seconds
            'exit_reason': 'STOP_LOSS',
            'pnl_points': -10.5
        },
        {
            'trade_id': 2,
            'entry_time': pd.Timestamp('2025-12-24 16:00:00'),
            'exit_time': pd.Timestamp('2025-12-24 16:05:30'),  # Has seconds
            'exit_reason': 'TAKE_PROFIT',
            'pnl_points': 15.2
        }
    ]
    
    df = pd.DataFrame(test_data)
    
    print("🔍 Original DataFrame:")
    print(f"  exit_time dtype: {df['exit_time'].dtype}")
    print(f"  Sample 1: {df['exit_time'].iloc[0]} (second={df['exit_time'].iloc[0].second})")
    print(f"  Sample 2: {df['exit_time'].iloc[1]} (second={df['exit_time'].iloc[1].second})")
    
    # Test 1: Default CSV save
    temp_file1 = tempfile.mktemp(suffix='_default.csv')
    df.to_csv(temp_file1, index=False)
    
    df_loaded1 = pd.read_csv(temp_file1, parse_dates=['entry_time', 'exit_time'])
    print(f"\n🔍 Test 1 - Default CSV:")
    print(f"  Loaded exit_time: {df_loaded1['exit_time'].iloc[0]}")
    print(f"  Has seconds: {df_loaded1['exit_time'].iloc[0].second == 51}")
    
    # Test 2: CSV with date_format
    temp_file2 = tempfile.mktemp(suffix='_with_format.csv')
    df.to_csv(temp_file2, index=False, date_format='%Y-%m-%d %H:%M:%S')
    
    df_loaded2 = pd.read_csv(temp_file2, parse_dates=['entry_time', 'exit_time'])
    print(f"\n🔍 Test 2 - CSV with date_format='%Y-%m-%d %H:%M:%S':")
    print(f"  Loaded exit_time: {df_loaded2['exit_time'].iloc[0]}")
    print(f"  Has seconds: {df_loaded2['exit_time'].iloc[0].second == 51}")
    
    # Cleanup
    os.unlink(temp_file1)
    os.unlink(temp_file2)
    
    return df_loaded1['exit_time'].iloc[0].second == 51 and df_loaded2['exit_time'].iloc[0].second == 51

if __name__ == "__main__":
    success = test_csv_datetime_precision()
    if success:
        print("\n✅ CSV preserves second precision")
    else:
        print("\n❌ CSV loses second precision")
import sys
import os
import yaml
import pandas as pd
import numpy as np

# -------------------------------------------------------------------------
# PATH SETUP
# -------------------------------------------------------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..', '..'))

if project_root not in sys.path:
    sys.path.append(project_root)

from src.strategies.filters.rsi_filter import RSIFilter

# -------------------------------------------------------------------------
# FUNCTIONS
# -------------------------------------------------------------------------

def load_config(config_relative_path):
    full_path = os.path.join(project_root, config_relative_path)
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"❌ Config file not found at: {full_path}")
    with open(full_path, 'r') as file:
        return yaml.safe_load(file)

def load_data(file_relative_path, date_range_cfg):
    full_path = os.path.join(project_root, file_relative_path)
    
    if not os.path.exists(full_path):
        print(f"⚠️  Data file not found at: {full_path}. Generating dummy data...")
        dates = pd.date_range(start=date_range_cfg['start'], end=date_range_cfg['end'], freq='1min')
        df = pd.DataFrame({
            'timestamp': dates,
            'open': 100, 'high': 105, 'low': 95, 
            'close': [100 + (np.sin(i/10) * 10) for i in range(len(dates))], 
            'volume': 1000
        })
    else:
        print(f"✅ Loading data from: {full_path}")
        # Load CSV and parse timestamps
        df = pd.read_csv(full_path)
        df.columns = df.columns.str.lower()
        df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Apply Date Range Filter
    start_dt = pd.to_datetime(date_range_cfg['start'])
    end_dt = pd.to_datetime(date_range_cfg['end'])
    
    mask = (df['timestamp'] >= start_dt) & (df['timestamp'] <= end_dt)
    df_filtered = df.loc[mask].copy()
    
    # Set index for technical analysis
    df_filtered.set_index('timestamp', inplace=True)
    df_filtered.sort_index(inplace=True)
    
    return df_filtered

def run_test():
    # 1. Load Configuration
    config_rel_path = os.path.join('src', 'config', 'WBWS', 'wbws_rsi_strategy.yaml')
    
    try:
        config = load_config(config_rel_path)
    except FileNotFoundError as e:
        print(e)
        return

    rsi_cfg = config['filters']['rsi_filter']
    data_cfg = config['data']
    date_range = data_cfg['date_range']

    print(f"\n--- Testing RSI Filter: {config['strategy']['name']} ---")
    print(f"   Settings: Length={rsi_cfg['length']}, OB={rsi_cfg['overbought']}, OS={rsi_cfg['oversold']}")
    print(f"   Period:   {date_range['start']} TO {date_range['end']}")

    # 2. Load and Filter Data
    df = load_data(data_cfg['file'], date_range)
    
    if df.empty:
        print("❌ Error: No data found for the specified date range.")
        return

    # 3. Initialize Filter
    rsi_filter = RSIFilter(
        length=rsi_cfg['length'],
        overbought=rsi_cfg['overbought'],
        oversold=rsi_cfg['oversold'],
        enabled=rsi_cfg['enabled']
    )

    # 4. Apply Filter
    long_signals_allowed = rsi_filter.apply_filter(df, is_long=True)
    short_signals_allowed = rsi_filter.apply_filter(df, is_long=False)

    # 5. Generate Report
    total_bars = len(df)
    long_allowed_count = long_signals_allowed.sum()
    short_allowed_count = short_signals_allowed.sum()
    
    overbought_count = total_bars - long_allowed_count
    oversold_count = total_bars - short_allowed_count

    print("\n--- Validation Report (Filtered by Date) ---")
    print(f"Total Bars in Range: {total_bars}")
    print("-" * 40)
    
    print(f"LONG TRADES (Bullish Context):")
    print(f"   ✅ Allowed (RSI < {rsi_cfg['overbought']}): {long_allowed_count} bars ({long_allowed_count/total_bars:.1%})")
    print(f"   ❌ Filtered (Overbought):  {overbought_count} bars")
    
    print("-" * 40)
    
    print(f"SHORT TRADES (Bearish Context):")
    print(f"   ✅ Allowed (RSI > {rsi_cfg['oversold']}): {short_allowed_count} bars ({short_allowed_count/total_bars:.1%})")
    print(f"   ❌ Filtered (Oversold):    {oversold_count} bars")

    print("-" * 40)
    print(f"First timestamp in range: {df.index[0]}")
    print(f"Last timestamp in range:  {df.index[-1]}")
    print(f"Last RSI Value: {df['rsi'].iloc[-1]:.4f}")

if __name__ == "__main__":
    run_test()
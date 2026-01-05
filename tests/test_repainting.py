# tests/test_repainting.py
"""
Repainting Risk Analysis for WBWS Trigger

Implements Phase 1: Pure trigger repaint analysis (indicator-level)

Usage:
    python tests/test_repainting.py src/config/WBWS/wbws_rsi_strategy.yaml

Output:
- CSV files in outputs/reports/WBWS/repainting/
  - repaint_summary_<htf>min_<timestamp>.csv: Overall metrics
  - signal_comparison_<htf>min_<timestamp>.csv: Detailed per-signal differences
  - repaint_position_analysis_<htf>min_<timestamp>.csv: Repaint rate by HTF position

Metrics Calculated:
- Signal counts (buy/sell/total) for lookahead_on and lookahead_off
- Stability metrics: stable, phantom, delayed percentages
- Asymmetry: buy vs sell repaint rates
- Repaint direction matrix
- Repaint rate by HTF progress bins (0-25%, 25-50%, 50-75%, 75-100%)
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime
import yaml
from typing import Dict, Tuple

# For IDE path resolution issues: The sys.path.append handles runtime import.
# If your IDE complains about unresolved import, you can ignore it or configure
# your IDE to recognize the project root as a source directory.

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src', 'indicators'))
from wbws_trigger import WBWSTrigger

def load_config(config_path: str) -> Dict:
    """Load YAML configuration"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def prepare_data(config: Dict) -> pd.DataFrame:
    """Load and prepare OHLCV data from config"""
    data_config = config['data']
    df = pd.read_csv(data_config['file'], parse_dates=['timestamp'])
    df.set_index('timestamp', inplace=True)
    
    # Apply date range
    start = pd.to_datetime(data_config['date_range']['start'])
    end = pd.to_datetime(data_config['date_range']['end'])
    df = df[(df.index >= start) & (df.index <= end)]
    
    # Validate schema
    required_cols = ['open', 'high', 'low', 'close', 'volume']
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"Missing columns: {set(required_cols) - set(df.columns)}")
    
    return df

def compute_signals_with_variant(trigger: WBWSTrigger, df: pd.DataFrame, lookahead: bool = True) -> pd.DataFrame:
    """
    Compute signals with lookahead variant.
    
    Args:
        trigger: WBWSTrigger instance
        df: Preprocessed OHLCV DataFrame
        lookahead: If True, use current implementation (lookahead_on)
                   If False, simulate lookahead_off by shifting HTF conditions
    """
    # Prepare HTF data (modified from original)
    df_htf = df.resample(trigger.htf_period).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    
    # HTF conditions
    df_htf['htf_bull'] = (df_htf['close'] > df_htf['open'])
    df_htf['htf_bear'] = (df_htf['close'] < df_htf['open'])
    
    if not lookahead:
        # Simulate lookahead_off: Use previous HTF bar's conditions
        # Use fill_value to avoid NaN and dtype change to object
        df_htf['htf_bull'] = df_htf['htf_bull'].shift(1, fill_value=False)
        df_htf['htf_bear'] = df_htf['htf_bear'].shift(1, fill_value=False)
    
    # Forward fill to base timeframe
    df_copy = df.copy()
    df_copy['htf_bull'] = df_htf['htf_bull'].reindex(df.index, method='ffill').fillna(False)
    df_copy['htf_bear'] = df_htf['htf_bear'].reindex(df.index, method='ffill').fillna(False)
    
    # Now run the rest of calculate_signals logic
    trigger._validate_input(df_copy)  # Private method, but assuming access
    
    df_signals = df_copy.reset_index()  # As in original
    
    # Classify candles (copy from original)
    candle_types = []
    for i in range(len(df_signals)):
        if i == 0:
            candle_types.append(np.nan)
        else:
            prev = df_signals.iloc[i-1]
            curr = df_signals.iloc[i]
            if (pd.isna(prev['high']) or pd.isna(prev['low']) or
                pd.isna(curr['high']) or pd.isna(curr['low'])):
                candle_types.append(np.nan)
                continue
            
            if (curr['high'] <= prev['high'] and curr['low'] >= prev['low']):
                candle_types.append(1)  # Inside
            elif (curr['high'] > prev['high'] and curr['low'] < prev['low']):
                candle_types.append(3)  # Outside
            elif (curr['high'] > prev['high'] and curr['low'] >= prev['low']):
                candle_types.append(2)  # 2u
            elif (curr['low'] < prev['low'] and curr['high'] <= prev['high']):
                candle_types.append(-2)  # 2d
            else:
                candle_types.append(np.nan)
    
    df_signals['candle_type'] = candle_types
    
    # Detect reversals
    df_signals['rev_2d_2u'] = (
        df_signals['candle_type'].notna() &
        df_signals['candle_type'].shift(1).notna() &
        (df_signals['candle_type'].shift(1) == -2) &
        (df_signals['candle_type'] == 2)
    )
    
    df_signals['rev_2u_2d'] = (
        df_signals['candle_type'].notna() &
        df_signals['candle_type'].shift(1).notna() &
        (df_signals['candle_type'].shift(1) == 2) &
        (df_signals['candle_type'] == -2)
    )
    
    # Generate signals
    df_signals['we_buy'] = df_signals['rev_2d_2u'] & df_signals['htf_bull']
    df_signals['we_sell'] = df_signals['rev_2u_2d'] & df_signals['htf_bear']
    
    return df_signals.set_index('timestamp')

def analyze_repainting(df_on: pd.DataFrame, df_off: pd.DataFrame, htf_period: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Compute repainting metrics"""
    # Combine for comparison
    df_comp = pd.DataFrame({
        'buy_on': df_on['we_buy'],
        'buy_off': df_off['we_buy'],
        'sell_on': df_on['we_sell'],
        'sell_off': df_off['we_sell']
    })
    
    # Signal stability
    df_comp['buy_stable'] = df_comp['buy_on'] == df_comp['buy_off']
    df_comp['sell_stable'] = df_comp['sell_on'] == df_comp['sell_off']
    
    # Repaint types for buys
    df_comp['buy_type'] = np.where(
        df_comp['buy_on'] & ~df_comp['buy_off'], 'phantom',
        np.where(~df_comp['buy_on'] & df_comp['buy_off'], 'delayed',
        np.where(df_comp['buy_on'] & df_comp['buy_off'], 'stable', 'none')
    ))
    
    # For sells
    df_comp['sell_type'] = np.where(
        df_comp['sell_on'] & ~df_comp['sell_off'], 'phantom',
        np.where(~df_comp['sell_on'] & df_comp['sell_off'], 'delayed',
        np.where(df_comp['sell_on'] & df_comp['sell_off'], 'stable', 'none')
    ))
    
    # Summary metrics
    summary = {
        'htf_period_min': int(htf_period.rstrip('min')),
        'total_bars': len(df_comp),
        'buy_on_count': int(df_comp['buy_on'].sum()),
        'buy_off_count': int(df_comp['buy_off'].sum()),
        'sell_on_count': int(df_comp['sell_on'].sum()),
        'sell_off_count': int(df_comp['sell_off'].sum()),
        'buy_stable_pct': (df_comp['buy_stable'].mean() * 100) if len(df_comp) > 0 else 0,
        'sell_stable_pct': (df_comp['sell_stable'].mean() * 100) if len(df_comp) > 0 else 0,
        'buy_phantom_count': int((df_comp['buy_type'] == 'phantom').sum()),
        'buy_delayed_count': int((df_comp['buy_type'] == 'delayed').sum()),
        'sell_phantom_count': int((df_comp['sell_type'] == 'phantom').sum()),
        'sell_delayed_count': int((df_comp['sell_type'] == 'delayed').sum()),
    }
    
    total_unique_buy = summary['buy_on_count'] + summary['buy_delayed_count']
    total_unique_sell = summary['sell_on_count'] + summary['sell_delayed_count']
    
    summary.update({
        'buy_repaint_pct': ((summary['buy_phantom_count'] + summary['buy_delayed_count']) / total_unique_buy * 100) if total_unique_buy > 0 else 0,
        'sell_repaint_pct': ((summary['sell_phantom_count'] + summary['sell_delayed_count']) / total_unique_sell * 100) if total_unique_sell > 0 else 0,
    })
    
    summary_df = pd.DataFrame([summary])
    
    # Signal comparison CSV (only rows with signals in either)
    signal_rows = df_comp[(df_comp['buy_on'] | df_comp['buy_off'] | df_comp['sell_on'] | df_comp['sell_off'])]
    signal_comp_df = signal_rows.copy()
    
    # HTF position analysis
    # Compute progress in HTF bar
    htf_freq = pd.Timedelta(htf_period)
    df_comp['htf_open'] = df_comp.index.floor(freq=htf_period)
    df_comp['progress'] = (df_comp.index - df_comp['htf_open']) / htf_freq
    df_comp['progress_bin'] = pd.cut(df_comp['progress'], bins=[0, 0.25, 0.5, 0.75, 1.0], labels=['0-25%', '25-50%', '50-75%', '75-100%'])
    
    # Repaint rate by bin (considering any repaint in buy or sell)
    df_comp['any_repaint'] = (df_comp['buy_type'].isin(['phantom', 'delayed'])) | (df_comp['sell_type'].isin(['phantom', 'delayed']))
    position_analysis = df_comp[(df_comp['buy_type'] != 'none') | (df_comp['sell_type'] != 'none')].groupby('progress_bin', observed=True).agg({
        'any_repaint': ['count', 'sum', 'mean']
    })
    position_analysis.columns = ['total_signals', 'repaint_count', 'repaint_rate']
    position_analysis['repaint_rate'] *= 100
    
    return summary_df, signal_comp_df, position_analysis.reset_index()

def main(config_path: str):
    config = load_config(config_path)
    df = prepare_data(config)
    
    output_dir = os.path.join(config['output']['outputs_dir'], config['output']['reports_dir'], 'repainting')
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    htf_periods = ['5min', '15min', '30min', '45min', '60min']
    all_summaries = []
    
    for htf in htf_periods:
        print(f"Processing HTF: {htf}")
        
        trigger = WBWSTrigger(htf_period=htf)
        
        # Compute with lookahead_on
        df_on = compute_signals_with_variant(trigger, df, lookahead=True)
        
        # Compute with lookahead_off
        df_off = compute_signals_with_variant(trigger, df, lookahead=False)
        
        # Analyze
        summary_df, signal_comp_df, position_df = analyze_repainting(df_on, df_off, htf)
        
        # Save CSVs
        summary_df.to_csv(os.path.join(output_dir, f'repaint_summary_{htf.rstrip("min")}min_{timestamp}.csv'), index=False)
        signal_comp_df.to_csv(os.path.join(output_dir, f'signal_comparison_{htf.rstrip("min")}min_{timestamp}.csv'))
        position_df.to_csv(os.path.join(output_dir, f'repaint_position_analysis_{htf.rstrip("min")}min_{timestamp}.csv'), index=False)
        
        all_summaries.append(summary_df)
    
    # Combined summary across HTFs
    combined_summary = pd.concat(all_summaries, ignore_index=True)
    combined_summary.to_csv(os.path.join(output_dir, f'repaint_combined_summary_{timestamp}.csv'), index=False)
    
    print(f"Analysis complete. Outputs saved to: {output_dir}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python tests/test_repainting.py <config_path>")
        sys.exit(1)
    
    main(sys.argv[1])
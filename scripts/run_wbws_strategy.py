"""
WBWS Strategy Runner - Integrated with RSI Filter and Detailed Reporting
"""
import sys
import pandas as pd
import numpy as np
import yaml
import json
from pathlib import Path
from datetime import datetime

# Get project root
project_root = Path(__file__).resolve().parent.parent

# Add to Python path
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
if str(project_root / 'src') not in sys.path:
    sys.path.insert(0, str(project_root / 'src'))

def run_wbws_strategy(config_path: str, verbose: bool = False):
    print("\n" + "="*70)
    print("🚀 WBWS STRATEGY WORKFLOW (Trigger + Filters)")
    print("="*70 + "\n")
    
    # 1. Load Configuration
    config_path_full = project_root / config_path if not Path(config_path).is_absolute() else Path(config_path)
    with open(config_path_full, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # 2. Load Data
    data_cfg = config.get('data', {})
    df = pd.read_csv(project_root / data_cfg['file'], parse_dates=['timestamp'])
    df.columns = df.columns.str.lower()
    df = df.set_index('timestamp').sort_index()
    
    # Apply Date Range
    dr = data_cfg.get('date_range', {})
    df = df[(df.index >= pd.to_datetime(dr['start'])) & (df.index <= pd.to_datetime(dr['end']))]
    print(f"📊 Data loaded: {len(df):,} bars")

    # 3. Generate Trigger Signals
    from src.indicators.wbws_trigger import WBWSTrigger
    indicator = WBWSTrigger(htf_period=config['indicator']['htf_period'])
    signals_df = indicator.calculate_signals(df)
    
    # Align Index
    if not signals_df.index.equals(df.index):
        signals_df.index = df.index
        
    raw_signals = pd.Series(index=df.index, dtype=object)
    raw_signals.loc[signals_df['we_buy'] == True] = 'BUY'
    raw_signals.loc[signals_df['we_sell'] == True] = 'SELL'
    
    raw_stats = {'buy': int((raw_signals == 'BUY').sum()), 'sell': int((raw_signals == 'SELL').sum())}
    print(f"⚡ Raw signals: {sum(raw_stats.values())} ({raw_stats['buy']} buy, {raw_stats['sell']} sell)")

    # 4. Apply RSI Filter
    from src.strategies.filters.rsi_filter import RSIFilter
    rsi_cfg = config['filters']['rsi_filter']
    rsi_logic = RSIFilter(enabled=True, length=rsi_cfg['length'], overbought=rsi_cfg['overbought'], oversold=rsi_cfg['oversold'])
    
    not_overbought = rsi_logic.apply_filter(df, is_long=True)
    not_oversold = rsi_logic.apply_filter(df, is_long=False)
    
    filtered_signals = raw_signals.copy()
    filtered_signals.loc[(raw_signals == 'BUY') & ~not_overbought] = None
    filtered_signals.loc[(raw_signals == 'SELL') & ~not_oversold] = None
    
    # 5. Compile Split & Samples
    final_buy_count = int((filtered_signals == 'BUY').sum())
    final_sell_count = int((filtered_signals == 'SELL').sum())
    
    # Get samples for the report
    buy_samples = filtered_signals[filtered_signals == 'BUY'].head(5).index.astype(str).tolist()
    sell_samples = filtered_signals[filtered_signals == 'SELL'].head(5).index.astype(str).tolist()

    # 6. Save Report
    out_cfg = config.get('output', {})
    report_dir = project_root / out_cfg.get('outputs_dir', 'outputs') / out_cfg.get('reports_dir', 'reports/WBWS')
    report_dir.mkdir(parents=True, exist_ok=True)
    
    report_data = {
        "execution_time": datetime.now().isoformat(),
        "counts": {"raw_buy": raw_stats['buy'], "raw_sell": raw_stats['sell'], "final_buy": final_buy_count, "final_sell": final_sell_count},
        "samples": {"buy_timestamps": buy_samples, "sell_timestamps": sell_samples}
    }
    
    report_path = report_dir / f"strategy_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_path, 'w') as f:
        json.dump(report_data, f, indent=4)

    # FINAL CONSOLE OUTPUT
    print("\n" + "="*40)
    print("📈 FINAL SIGNAL SPLIT")
    print("-" * 40)
    print(f"🟢 BUY Signals:  {final_buy_count} (Rejected: {raw_stats['buy'] - final_buy_count})")
    print(f"🔴 SELL Signals: {final_sell_count} (Rejected: {raw_stats['sell'] - final_sell_count})")
    print(f"📂 Report: {report_path.relative_to(project_root)}")
    print("="*40 + "\n")

    return df

if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_wbws_strategy(sys.argv[1])
    else:
        print("❌ Usage: python scripts/run_wbws_strategy.py <config_path>")
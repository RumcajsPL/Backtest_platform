"""
WBWS Strategy Runner - Integrated with TimeManager, RSI Filter, Risk Management and Detailed Reporting
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
    print("🚀 WBWS STRATEGY WORKFLOW (Trigger + Time + RSI + Risk Mgmt)")
    print("="*70 + "\n")
    
    # 1. Load Configuration
    config_path_full = project_root / config_path if not Path(config_path).is_absolute() else Path(config_path)
    with open(config_path_full, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # 2. Load Data - FULL DATASET FIRST (for indicators requiring history)
    data_cfg = config.get('data', {})
    df_full = pd.read_csv(project_root / data_cfg['file'], parse_dates=['timestamp'])
    df_full.columns = df_full.columns.str.lower()
    df_full = df_full.set_index('timestamp').sort_index()
    
    print(f"📊 Full dataset: {len(df_full):,} bars ({df_full.index[0]} to {df_full.index[-1]})")
    
    # Apply Date Range for Strategy Execution
    dr = data_cfg.get('date_range', {})
    df = df_full[(df_full.index >= pd.to_datetime(dr['start'])) & (df_full.index <= pd.to_datetime(dr['end']))]
    print(f"📊 Strategy period: {len(df):,} bars")
    print(f"📅 Backtest range: {df.index[0]} to {df.index[-1]}\n")

    # 3. Generate Raw Trigger Signals
    from src.indicators.wbws_trigger import WBWSTrigger
    indicator = WBWSTrigger(htf_period=config['indicator']['htf_period'])
    signals_df = indicator.calculate_signals(df)
    
    # Align Index
    if not signals_df.index.equals(df.index):
        signals_df.index = df.index
        
    raw_signals = pd.Series(index=df.index, dtype=object)
    raw_signals.loc[signals_df['we_buy'] == True] = 'BUY'
    raw_signals.loc[signals_df['we_sell'] == True] = 'SELL'
    
    raw_buy_count = int((raw_signals == 'BUY').sum())
    raw_sell_count = int((raw_signals == 'SELL').sum())
    raw_total = raw_buy_count + raw_sell_count
    
    print("="*70)
    print("⚡ STEP 1: RAW TRIGGER SIGNALS")
    print("-"*70)
    print(f"  🟢 BUY:   {raw_buy_count:>4}")
    print(f"  🔴 SELL:  {raw_sell_count:>4}")
    print(f"  📊 TOTAL: {raw_total:>4}")
    print("="*70 + "\n")

    # 4. Apply Time Filter
    from src.strategies.trade_management.time_manager import TimeManager
    
    time_mgr_cfg = config.get('trade_management', {})
    time_manager = TimeManager(time_mgr_cfg)
    
    # Create signals DataFrame for time filtering
    raw_signals_df = pd.DataFrame({
        'timestamp': df.index,
        'signal': raw_signals.values
    }).dropna(subset=['signal'])
    
    # Apply time filter
    time_filtered_df = time_manager.filter_signals_by_time(raw_signals_df, timestamp_col='timestamp')
    
    # Reconstruct time-filtered signals series
    time_filtered_signals = pd.Series(index=df.index, dtype=object)
    time_filtered_signals.loc[time_filtered_df['timestamp'].values] = time_filtered_df['signal'].values
    
    time_filtered_buy = int((time_filtered_signals == 'BUY').sum())
    time_filtered_sell = int((time_filtered_signals == 'SELL').sum())
    time_filtered_total = time_filtered_buy + time_filtered_sell
    
    time_rejected_buy = raw_buy_count - time_filtered_buy
    time_rejected_sell = raw_sell_count - time_filtered_sell
    time_rejected_total = raw_total - time_filtered_total
    
    print("="*70)
    print("⏰ STEP 2: TIME FILTER")
    if time_manager.enabled:
        print(f"  Session: {time_manager.session_start_hour:02d}:{time_manager.session_start_minute:02d} - "
              f"{time_manager.session_end_hour:02d}:{time_manager.session_end_minute:02d}")
    else:
        print("  Status: DISABLED")
    print("-"*70)
    print(f"  🟢 BUY:   {time_filtered_buy:>4}  (rejected: {time_rejected_buy})")
    print(f"  🔴 SELL:  {time_filtered_sell:>4}  (rejected: {time_rejected_sell})")
    print(f"  📊 TOTAL: {time_filtered_total:>4}  (rejected: {time_rejected_total}, "
          f"{(time_rejected_total/raw_total*100) if raw_total > 0 else 0:.1f}%)")
    print("="*70 + "\n")

    # 5. Apply RSI Filter
    from src.strategies.filters.rsi_filter import RSIFilter
    rsi_cfg = config['filters']['rsi_filter']
    rsi_logic = RSIFilter(
        enabled=True, 
        length=rsi_cfg['length'], 
        overbought=rsi_cfg['overbought'], 
        oversold=rsi_cfg['oversold']
    )
    
    not_overbought = rsi_logic.apply_filter(df, is_long=True)
    not_oversold = rsi_logic.apply_filter(df, is_long=False)
    
    final_signals = time_filtered_signals.copy()
    final_signals.loc[(time_filtered_signals == 'BUY') & ~not_overbought] = None
    final_signals.loc[(time_filtered_signals == 'SELL') & ~not_oversold] = None
    
    final_buy_count = int((final_signals == 'BUY').sum())
    final_sell_count = int((final_signals == 'SELL').sum())
    final_total = final_buy_count + final_sell_count
    
    rsi_rejected_buy = time_filtered_buy - final_buy_count
    rsi_rejected_sell = time_filtered_sell - final_sell_count
    rsi_rejected_total = time_filtered_total - final_total
    
    print("="*70)
    print("📉 STEP 3: RSI FILTER")
    print(f"  Config: length={rsi_cfg['length']}, OB={rsi_cfg['overbought']}, "
          f"OS={rsi_cfg['oversold']}")
    print("-"*70)
    print(f"  🟢 BUY:   {final_buy_count:>4}  (rejected: {rsi_rejected_buy})")
    print(f"  🔴 SELL:  {final_sell_count:>4}  (rejected: {rsi_rejected_sell})")
    print(f"  📊 TOTAL: {final_total:>4}  (rejected: {rsi_rejected_total}, "
          f"{(rsi_rejected_total/time_filtered_total*100) if time_filtered_total > 0 else 0:.1f}%)")
    print("="*70 + "\n")

    # 6. Apply Risk Management
    # IMPORTANT: RiskManager needs FULL historical data for Rolling Annual Range calculation
    # Pass df_full for accurate ATR and annual range, but only process signals from df period
    from src.strategies.trade_management.risk_manager import RiskManager
    
    risk_manager = RiskManager(time_mgr_cfg, df_full)  # Use full dataset for indicators
    
    # Prepare signals for risk management
    risk_input_signals = final_signals.dropna()
    risk_approved_count = {'buy': 0, 'sell': 0}
    risk_rejected_count = {'buy': 0, 'sell': 0}
    risk_adjusted_count = {'buy': 0, 'sell': 0}
    
    trade_details = []  # Store SL/TP info for each signal
    
    for timestamp, signal_type in risk_input_signals.items():
        is_long = (signal_type == 'BUY')
        entry_price = df.loc[timestamp, 'close']
        
        # Calculate SL/TP
        stop_loss, take_profit = risk_manager.calculate_sl_tp(
            entry_price=entry_price,
            is_long=is_long,
            timestamp=timestamp
        )
        
        if stop_loss is None or take_profit is None:
            # No valid SL/TP (ATR not available)
            risk_rejected_count['buy' if is_long else 'sell'] += 1
            final_signals.loc[timestamp] = None
            continue
        
        # Validate risk percentile
        is_valid, adjusted_sl, comment = risk_manager.validate_risk_percentile(
            entry_price=entry_price,
            stop_loss=stop_loss,
            is_long=is_long,
            timestamp=timestamp
        )
        
        if not is_valid:
            # Trade rejected by risk management
            risk_rejected_count['buy' if is_long else 'sell'] += 1
            final_signals.loc[timestamp] = None
            continue
        
        # Track if SL was adjusted
        if adjusted_sl != stop_loss:
            risk_adjusted_count['buy' if is_long else 'sell'] += 1
            stop_loss = adjusted_sl
            # Recalculate TP based on adjusted SL
            sl_distance = abs(entry_price - stop_loss)
            rr_ratio = risk_manager.sl_tp_config.get('risk_to_reward_ratio', 2.0)
            tp_distance = sl_distance * rr_ratio
            if is_long:
                take_profit = entry_price + tp_distance
            else:
                take_profit = entry_price - tp_distance
        
        # Trade approved
        risk_approved_count['buy' if is_long else 'sell'] += 1
        
        # Store trade details
        trade_details.append({
            'timestamp': str(timestamp),
            'signal': signal_type,
            'entry': round(entry_price, 2),
            'sl': round(stop_loss, 2),
            'tp': round(take_profit, 2),
            'sl_distance': round(abs(entry_price - stop_loss), 2),
            'tp_distance': round(abs(entry_price - take_profit), 2),
            'comment': comment
        })
    
    risk_approved_buy = risk_approved_count['buy']
    risk_approved_sell = risk_approved_count['sell']
    risk_approved_total = risk_approved_buy + risk_approved_sell
    
    risk_rejected_buy = risk_rejected_count['buy']
    risk_rejected_sell = risk_rejected_count['sell']
    risk_rejected_total = risk_rejected_buy + risk_rejected_sell
    
    risk_adjusted_buy = risk_adjusted_count['buy']
    risk_adjusted_sell = risk_adjusted_count['sell']
    risk_adjusted_total = risk_adjusted_buy + risk_adjusted_sell
    
    print("="*70)
    print("🛡️ STEP 4: RISK MANAGEMENT")
    sl_cfg = time_mgr_cfg.get('sl_tp', {})
    risk_cfg = time_mgr_cfg.get('risk_management', {})
    print(f"  SL: ATR({sl_cfg.get('atr_length', 14)}) × {sl_cfg.get('sl_multiplier', 1.4)}")
    print(f"  TP: R:R = 1:{sl_cfg.get('risk_to_reward_ratio', 2.0)}")
    if risk_cfg.get('enabled', False):
        print(f"  Max Risk: {risk_cfg.get('max_risk_percentile', 1.0)*100:.1f}% of annual range")
    print("-"*70)
    print(f"  🟢 BUY:   {risk_approved_buy:>4}  (rejected: {risk_rejected_buy}, adjusted: {risk_adjusted_buy})")
    print(f"  🔴 SELL:  {risk_approved_sell:>4}  (rejected: {risk_rejected_sell}, adjusted: {risk_adjusted_sell})")
    print(f"  📊 TOTAL: {risk_approved_total:>4}  (rejected: {risk_rejected_total}, "
          f"{(risk_rejected_total/final_total*100) if final_total > 0 else 0:.1f}%)")
    if risk_adjusted_total > 0:
        print(f"  ⚠️  Adjusted SL: {risk_adjusted_total} trades")
    print("="*70 + "\n")

    # 7. Calculate Performance Metrics
    if trade_details:
        sl_distances = [td['sl_distance'] for td in trade_details]
        tp_distances = [td['tp_distance'] for td in trade_details]
        
        avg_sl = np.mean(sl_distances)
        avg_tp = np.mean(tp_distances)
        max_sl = np.max(sl_distances)
        min_sl = np.min(sl_distances)
        
        win_rates = [0.30, 0.40, 0.50, 0.60]
        rr_ratio = sl_cfg.get('risk_to_reward_ratio', 2.0)
        
        theoretical_metrics = {}
        for wr in win_rates:
            expected_r = (wr * rr_ratio) - ((1 - wr) * 1)
            theoretical_metrics[f'win_rate_{int(wr*100)}pct'] = {
                'win_rate': wr,
                'expected_return_per_trade_R': round(expected_r, 3),
                'expected_return_per_trade_points': round(expected_r * avg_sl, 2),
                'total_expected_return_R': round(expected_r * len(trade_details), 2),
                'breakeven_win_rate': round(1 / (1 + rr_ratio), 3)
            }
        
        performance_metrics = {
            'total_trades': len(trade_details),
            'buy_trades': risk_approved_buy,
            'sell_trades': risk_approved_sell,
            'avg_sl_distance': round(avg_sl, 2),
            'avg_tp_distance': round(avg_tp, 2),
            'max_sl_distance': round(max_sl, 2),
            'min_sl_distance': round(min_sl, 2),
            'risk_reward_ratio': rr_ratio,
            'theoretical_scenarios': theoretical_metrics
        }
    else:
        performance_metrics = {
            'total_trades': 0,
            'message': 'No approved trades to analyze'
        }

    # 8. Prepare Output Directories and Save Reports
    out_cfg = config.get('output', {})
    report_dir = project_root / out_cfg.get('outputs_dir', 'outputs') / out_cfg.get('reports_dir', 'reports/WBWS')
    report_dir.mkdir(parents=True, exist_ok=True)
    
    signals_dir = project_root / out_cfg.get('outputs_dir', 'outputs') / out_cfg.get('signals_dir', 'signals/strategy')
    signals_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Get sample trades
    buy_samples = [td for td in trade_details if td['signal'] == 'BUY'][:10]
    sell_samples = [td for td in trade_details if td['signal'] == 'SELL'][:10]
    
    # Build JSON report
    report_data = {
        "execution_time": datetime.now().isoformat(),
        "config": {
            "data_period": {
                "start": dr['start'],
                "end": dr['end']
            },
            "indicator": config['indicator']['name'],
            "htf_period": config['indicator']['htf_period'],
            "time_filter": {
                "enabled": time_manager.enabled,
                "session": f"{time_manager.session_start_hour:02d}:{time_manager.session_start_minute:02d}-"
                          f"{time_manager.session_end_hour:02d}:{time_manager.session_end_minute:02d}"
            } if time_manager.enabled else {"enabled": False},
            "rsi_filter": rsi_cfg
        },
        "signal_flow": {
            "step1_raw_signals": {
                "buy": raw_buy_count,
                "sell": raw_sell_count,
                "total": raw_total
            },
            "step2_time_filtered": {
                "buy": time_filtered_buy,
                "sell": time_filtered_sell,
                "total": time_filtered_total,
                "rejected_buy": time_rejected_buy,
                "rejected_sell": time_rejected_sell,
                "rejected_total": time_rejected_total,
                "rejection_rate_pct": round((time_rejected_total/raw_total*100) if raw_total > 0 else 0, 2)
            },
            "step3_rsi_filtered": {
                "buy": final_buy_count,
                "sell": final_sell_count,
                "total": final_total,
                "rejected_buy": rsi_rejected_buy,
                "rejected_sell": rsi_rejected_sell,
                "rejected_total": rsi_rejected_total,
                "rejection_rate_pct": round((rsi_rejected_total/time_filtered_total*100) if time_filtered_total > 0 else 0, 2)
            },
            "step4_risk_managed": {
                "buy": risk_approved_buy,
                "sell": risk_approved_sell,
                "total": risk_approved_total,
                "rejected_buy": risk_rejected_buy,
                "rejected_sell": risk_rejected_sell,
                "rejected_total": risk_rejected_total,
                "adjusted_buy": risk_adjusted_buy,
                "adjusted_sell": risk_adjusted_sell,
                "adjusted_total": risk_adjusted_total,
                "rejection_rate_pct": round((risk_rejected_total/final_total*100) if final_total > 0 else 0, 2)
            }
        },
        "overall_rejection": {
            "total_rejected": raw_total - risk_approved_total,
            "total_rejection_rate_pct": round(((raw_total - risk_approved_total)/raw_total*100) if raw_total > 0 else 0, 2)
        },
        "performance_metrics": performance_metrics,
        "samples": {
            "buy_signals": buy_samples,
            "sell_signals": sell_samples
        },
        "risk_details": {
            "atr_length": sl_cfg.get('atr_length', 14),
            "sl_multiplier": sl_cfg.get('sl_multiplier', 1.4),
            "risk_to_reward": sl_cfg.get('risk_to_reward_ratio', 2.0),
            "max_risk_percentile": risk_cfg.get('max_risk_percentile', 1.0),
            "allow_exceed_limit": risk_cfg.get('allow_exceed_limit', False)
        }
    }
    
    # Save JSON Report
    report_path = report_dir / f"strategy_report_{timestamp_str}.json"
    with open(report_path, 'w') as f:
        json.dump(report_data, f, indent=4)
    
    # Export Trade Details to CSV with DEBUG
    print(f"\n[DEBUG] trade_details length: {len(trade_details)}")
    print(f"[DEBUG] save_signals_csv: {out_cfg.get('save_signals_csv', True)}")
    
    if out_cfg.get('save_signals_csv', True):
        if trade_details:
            trades_df = pd.DataFrame(trade_details)
            csv_path = signals_dir / f"trade_details_{timestamp_str}.csv"
            print(f"[DEBUG] Saving to: {csv_path}")
            trades_df.to_csv(csv_path, index=False)
            print(f"[DEBUG] File exists: {csv_path.exists()}")
            print(f"\n📊 CSV Export: {csv_path.relative_to(project_root)}")
            print(f"   → {len(trades_df)} trades saved")
        else:
            print("\n⚠️  No trades to export (trade_details is empty)")
    
    # 9. Display Final Summary
    print("\n" + "="*70)
    print("📊 FINAL SUMMARY - COMPLETE SIGNAL FLOW")
    print("="*70)
    print(f"  Raw Signals:        {raw_total:>4}")
    print(f"  Time Filtered:      {time_filtered_total:>4}  (↓ {time_rejected_total}, -{(time_rejected_total/raw_total*100) if raw_total > 0 else 0:.1f}%)")
    print(f"  RSI Filtered:       {final_total:>4}  (↓ {rsi_rejected_total}, -{(rsi_rejected_total/time_filtered_total*100) if time_filtered_total > 0 else 0:.1f}%)")
    print(f"  Risk Approved:      {risk_approved_total:>4}  (↓ {risk_rejected_total}, -{(risk_rejected_total/final_total*100) if final_total > 0 else 0:.1f}%)")
    print("-"*70)
    print(f"  🟢 Final BUY:       {risk_approved_buy:>4}  (from {raw_buy_count}, -{raw_buy_count-risk_approved_buy})")
    print(f"  🔴 Final SELL:      {risk_approved_sell:>4}  (from {raw_sell_count}, -{raw_sell_count-risk_approved_sell})")
    print(f"  📉 Overall Rejection: {((raw_total-risk_approved_total)/raw_total*100) if raw_total > 0 else 0:.1f}%")
    if risk_adjusted_total > 0:
        print(f"  ⚙️  SL Adjustments:  {risk_adjusted_total}")
    print("="*70)
    
    # Display Performance Metrics
    if trade_details:
        print("\n" + "="*70)
        print("📈 PERFORMANCE METRICS")
        print("="*70)
        print(f"  Total Trades:       {performance_metrics['total_trades']}")
        print(f"  Avg SL Distance:    {performance_metrics['avg_sl_distance']:.2f} points")
        print(f"  Avg TP Distance:    {performance_metrics['avg_tp_distance']:.2f} points")
        print(f"  Risk:Reward Ratio:  1:{performance_metrics['risk_reward_ratio']}")
        print(f"  Breakeven WR:       {performance_metrics['theoretical_scenarios']['win_rate_30pct']['breakeven_win_rate']*100:.1f}%")
        print("-"*70)
        print("  Theoretical Performance (assuming different win rates):")
        for scenario_key, scenario in performance_metrics['theoretical_scenarios'].items():
            wr_pct = int(scenario['win_rate'] * 100)
            exp_r = scenario['expected_return_per_trade_R']
            total_r = scenario['total_expected_return_R']
            print(f"    • {wr_pct}% WR: {exp_r:+.3f}R per trade → {total_r:+.1f}R total ({total_r * performance_metrics['avg_sl_distance']:+.1f} pts)")
        print("="*70)
    
    # Show sample trades
    if buy_samples:
        print("\n📋 Sample BUY Trades (with SL/TP):")
        for i, trade in enumerate(buy_samples[:5], 1):
            print(f"  {i}. {trade['timestamp']} | Entry: {trade['entry']} | "
                  f"SL: {trade['sl']} (-{trade['sl_distance']}) | "
                  f"TP: {trade['tp']} (+{trade['tp_distance']})")
    
    if sell_samples:
        print("\n📋 Sample SELL Trades (with SL/TP):")
        for i, trade in enumerate(sell_samples[:5], 1):
            print(f"  {i}. {trade['timestamp']} | Entry: {trade['entry']} | "
                  f"SL: {trade['sl']} (+{trade['sl_distance']}) | "
                  f"TP: {trade['tp']} (-{trade['tp_distance']})")
    
    print(f"\n📂 JSON Report: {report_path.relative_to(project_root)}")
    print("\n✅ Strategy execution completed successfully!\n")

    return df, final_signals, trade_details

if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_wbws_strategy(sys.argv[1])
    else:
        print("❌ Usage: python scripts/run_wbws_strategy.py <config_path>")
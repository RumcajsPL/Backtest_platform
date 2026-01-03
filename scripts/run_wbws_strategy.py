# Updated: scripts/run_wbws_strategy.py
"""WBWS Strategy Runner - Modular Version with Enhanced Progressive Tracking"""
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

# Get project root
project_root = Path(__file__).resolve().parent.parent

# Add project root to sys.path for imports from src/
sys.path.insert(0, str(project_root))

from strategy_modules.data_loader import DataLoader
from strategy_modules.signal_generator import SignalGenerator
from strategy_modules.filter_pipeline import FilterPipeline
from strategy_modules.trade_simulator import TradeSimulator
from strategy_modules.report_generator import ReportGenerator
from strategy_modules.metrics_calculator import calculate_performance_metrics
from strategy_modules.progressive_tracker import EnhancedProgressiveTracker

def run_wbws_strategy(config_path: str, verbose: bool = False):
    """
    Main orchestrator for WBWS strategy execution with enhanced progressive tracking
    """
    print("\n" + "="*70)
    print("🚀 WBWS STRATEGY WORKFLOW - WITH ENHANCED PROGRESSIVE TRACKING")
    print("="*70 + "\n")
    
    try:
        # 1. Load configuration and data
        print("📊 STEP 1: LOADING DATA")
        data_loader = DataLoader(config_path)
        config = data_loader.load_config()
        df_full, df_strategy = data_loader.load_data()
        
        # Initialize enhanced progressive tracker
        progressive_tracker = EnhancedProgressiveTracker(config)
        
        data_info = data_loader.get_data_info()
        print(f"  Full dataset: {data_info['full_bars']:,} bars")
        print(f"  Strategy period: {data_info['strategy_bars']:,} bars")
        print(f"  Date range: {data_info['date_range'][0]} to {data_info['date_range'][1]}")
        
        # 2. Generate signals (STAGE 0)
        print("\n📈 STEP 2: GENERATING SIGNALS")
        signal_gen = SignalGenerator(config['indicator']['htf_period'])
        raw_signals, indicator_values = signal_gen.generate_signals(df_strategy)
        
        # Get HTF signals if available
        htf_signals = None
        if hasattr(signal_gen, 'htf_signals'):
            htf_signals = signal_gen.htf_signals
        
        # Record ALL raw signals in progressive tracker with detailed info
        print("  Recording raw signals in progressive tracker...")
        signal_id_map = {}  # Map timestamp -> signal_id
        for timestamp, signal in raw_signals.dropna().items():
            mid_price = df_strategy.loc[timestamp, 'close']
            indicator_row = indicator_values.loc[timestamp] if timestamp in indicator_values.index else None
            htf_signal = htf_signals.loc[timestamp] if htf_signals is not None and timestamp in htf_signals.index else None
            
            signal_id = progressive_tracker.record_raw_signal(
                timestamp=timestamp,
                signal=signal,
                mid_price=mid_price,
                indicator_row=indicator_row,  # Pass entire row
                htf_signal=htf_signal
            )
            signal_id_map[timestamp] = signal_id
        
        signal_stats = signal_gen.get_signal_stats(raw_signals)
        print(f"  Raw BUY: {signal_stats['buy']:,}, Raw SELL: {signal_stats['sell']:,}, "
              f"Total: {signal_stats['total']:,}")
        
        # 3. Apply filters with progressive tracking (STAGES 1-2)
        print("\n🎯 STEP 3: APPLYING FILTERS WITH PROGRESSIVE TRACKING")
        filter_pipeline = FilterPipeline(config)
        filter_pipeline.initialize_risk_manager(df_full)  # Prep risk_manager for later use
        filter_pipeline.set_progressive_tracker(progressive_tracker)  # Connect tracker
        
        # Time filter with detailed tracking (STAGE 1)
        print("  Applying time filter with detailed tracking...")
        time_filtered = filter_pipeline.apply_time_filter(raw_signals, signal_id_map)
        
        time_stats = {
            'buy': int((time_filtered == 'BUY').sum()),
            'sell': int((time_filtered == 'SELL').sum()),
            'total': int((time_filtered.notna()).sum())
        }
        print(f"    → Time filtered: {time_stats['total']:,} signals "
              f"({time_stats['buy']:,} BUY, {time_stats['sell']:,} SELL)")
        
        # RSI filter with detailed tracking (STAGE 2)
        print("  Applying RSI filter with detailed tracking...")
        rsi_filtered = filter_pipeline.apply_rsi_filter(df_strategy, time_filtered, signal_id_map)
        
        rsi_stats = {
            'buy': int((rsi_filtered == 'BUY').sum()),
            'sell': int((rsi_filtered == 'SELL').sum()),
            'total': int((rsi_filtered.notna()).sum())
        }
        print(f"    → RSI filtered: {rsi_stats['total']:,} signals "
              f"({rsi_stats['buy']:,} BUY, {rsi_stats['sell']:,} SELL)")
        
        # Get filter stats (now excludes risk, as it's moved to simulation)
        filter_stats = filter_pipeline.get_filter_stats(raw_signals, time_filtered, rsi_filtered)
        
        # 4. Simulate trades (STAGES 3-5: Position mgmt, risk, execution)
        print("\n🔄 STEP 4: SIMULATING TRADES")
        trade_simulator = TradeSimulator(config)
        simulation_results = trade_simulator.simulate_trades(
            df_strategy, rsi_filtered, 
            verbose=verbose,
            progressive_tracker=progressive_tracker,
            risk_manager=filter_pipeline.filters['risk'], # Pass for Stage 4
            signal_id_map=signal_id_map  # Pass signal_id_map
        )

        print(f"  Simulated trades: {len(simulation_results['closed_trades']):,} closed, "
            f"{len(simulation_results['open_trades']):,} open, "
            f"{len(simulation_results['rejected_trades']):,} rejected")
        all_trades = simulation_results['all_trades']
        
        # Add risk stats from simulation to filter_stats
        filter_stats['risk_filtered'] = simulation_results.get('risk_stats', {})
        
        # We need to update trade simulator to provide detailed exit information
        # For now, update with basic trade information
        for trade in all_trades:
            timestamp = trade.get('entry_time')
            signal_id = None
            
            # Find signal_id for this trade's entry_time
            for ts, sid in signal_id_map.items():
                if pd.Timestamp(ts) == pd.Timestamp(timestamp):
                    signal_id = sid
                    break
            
            if signal_id:
                # Determine position action based on trade details
                position_action = 'OPEN'
                position_reason = 'New position opened'
                if 'Reversal' in str(trade.get('comment', '')):
                    position_action = 'CLOSE_AND_REVERSE'
                    position_reason = 'Close and reverse position'
                elif 'Rejected' in str(trade.get('comment', '')):
                    position_action = 'REJECT'
                    position_reason = trade.get('comment', 'Position rejected')
                
                # Update position management
                progressive_tracker.update_position_management_details(
                    signal_id=signal_id,
                    action=position_action,
                    reason=position_reason,
                    current_direction=trade.get('direction', 'NONE'),
                    open_positions_count=len(simulation_results['open_trades']) + 1,  # Approximate
                    pyramiding_enabled=config['trade_management']['position_control'].get('pyramiding_enabled', False),
                    close_on_opposite=config['trade_management']['position_control'].get('close_on_opposite', False),
                    can_open_new_position=True  # Simplified
                )
                
                # Update trade execution
                if trade.get('status') == 'CLOSED':
                    progressive_tracker.update_trade_execution_details(
                        signal_id=signal_id,
                        trade_id=trade.get('trade_id'),
                        position_id=trade.get('position_id'),
                        entry_time=trade.get('entry_time'),
                        entry_price_executed=trade.get('entry_price'),
                        sl_price_executed=trade.get('sl_price'),
                        tp_price_executed=trade.get('tp_price'),
                        exit_time=trade.get('exit_time'),
                        exit_price=trade.get('exit_price'),
                        exit_reason=trade.get('exit_reason'),
                        pnl_points=trade.get('pnl_points'),
                        pnl_percent=trade.get('pnl_percent'),
                        duration_bars=trade.get('duration_bars'),
                        duration_minutes=trade.get('duration_minutes'),
                        is_win=trade.get('pnl_points', 0) > 0,
                        is_loss=trade.get('pnl_points', 0) < 0,
                        exit_check_high=None,  # These would come from enhanced trade simulator
                        exit_check_low=None,
                        spread_adjusted_high=None,
                        spread_adjusted_low=None,
                        reason=f"Trade CLOSED - {trade.get('exit_reason')}"
                    )
                elif trade.get('status') == 'OPEN':
                    progressive_tracker.update_trade_execution_details(
                        signal_id=signal_id,
                        trade_id=trade.get('trade_id'),
                        position_id=trade.get('position_id'),
                        entry_time=trade.get('entry_time'),
                        entry_price_executed=trade.get('entry_price'),
                        sl_price_executed=trade.get('sl_price'),
                        tp_price_executed=trade.get('tp_price'),
                        reason="Trade OPEN"
                    )
                elif trade.get('status') == 'REJECTED':
                    progressive_tracker.update_trade_execution_details(
                        signal_id=signal_id,
                        reason=f"Trade REJECTED - {trade.get('reject_reason')}"
                    )
        
        # 5. Calculate metrics
        print("\n📊 STEP 5: CALCULATING METRICS")
        if simulation_results['all_trades']:
            trades_df = pd.DataFrame(simulation_results['all_trades'])
            performance_metrics = calculate_performance_metrics(trades_df, df_strategy)
            
            print(f"  Total P&L: {performance_metrics['total_pnl_points']:+,.2f} pts")
            print(f"  Win Rate: {performance_metrics['win_rate']:.1f}%")
            print(f"  Profit Factor: {performance_metrics['profit_factor']:.2f}")
        else:
            performance_metrics = {'total_trades': 0, 'message': 'No trades executed'}
            print("  No trades executed")
        
        # 6. Generate reports
        print("\n📄 STEP 6: GENERATING REPORTS")
        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_gen = ReportGenerator(config, project_root)
        
        # Generate progressive CSV with enhanced details
        print("  Generating enhanced progressive signals CSV...")
        progressive_csv_path = progressive_tracker.save_to_csv(project_root, timestamp_str)
        print(f"    → Progressive CSV saved: {progressive_csv_path.relative_to(project_root)}")
        
        # Generate regular CSV (existing functionality)
        print("  Generating trade details CSV...")
        csv_path = report_gen.generate_csv(simulation_results['all_trades'], timestamp_str)
        if csv_path:
            print(f"    → Trade CSV saved: {csv_path.relative_to(project_root)}")
        
        # Build and save JSON report
        print("  Generating JSON report...")
        
        # Get progressive statistics
        progressive_stats = progressive_tracker.get_statistics()
        
        # Add progressive data to filter_stats
        filter_stats['progressive'] = progressive_stats
        
        report_data = report_gen.build_report_data(
            config, data_info, filter_stats, simulation_results,
            performance_metrics, csv_path
        )
        
        # Add progressive tracking info to report
        if 'progressive_tracking' not in report_data:
            report_data['progressive_tracking'] = {}
        report_data['progressive_tracking'].update({
            'progressive_csv_file': str(progressive_csv_path.relative_to(project_root)),
            'signal_progression_summary': progressive_stats,
            'total_signals_tracked': progressive_stats.get('total_signals', 0),
            'tracking_columns_count': len(progressive_tracker.columns)
        })
        
        json_path = report_gen.generate_json(report_data, timestamp_str)
        print(f"    → JSON saved: {json_path.relative_to(project_root)}")
        
        # 7. Display final summary
        print("\n" + "="*70)
        print("✅ ENHANCED STRATEGY EXECUTION COMPLETED")
        print("="*70)
        
        total_raw = filter_stats['raw']['total']
        total_executed = len(simulation_results['closed_trades'])
        rejection_rate = ((total_raw - total_executed) / total_raw * 100) if total_raw > 0 else 0
        
        print(f"\n📈 PERFORMANCE SUMMARY:")
        print(f"  Raw Signals:          {total_raw:,}")
        print(f"  Executed Trades:      {total_executed:,}")
        print(f"  Overall Rejection:    {rejection_rate:.1f}%")
        
        if performance_metrics.get('total_trades', 0) > 0:
            print(f"  Total P&L:           {performance_metrics['total_pnl_points']:+,.2f} pts")
            print(f"  Win Rate:            {performance_metrics['win_rate']:.1f}%")
            print(f"  Profit Factor:       {performance_metrics['profit_factor']:.2f}")
            print(f"  Avg P&L/Trade:       {performance_metrics['avg_pnl_points']:+,.2f} pts")
        
        # Display enhanced progressive tracking summary
        if progressive_stats:
            print(f"\n📊 ENHANCED PROGRESSIVE TRACKING SUMMARY:")
            print(f"  Total Signals Tracked: {progressive_stats.get('total_signals', 0):,}")
            print(f"  Tracking Columns:      {len(progressive_tracker.columns)}")
            
            if 'by_final_status' in progressive_stats:
                print(f"  Final Status Breakdown:")
                for status, count in sorted(progressive_stats['by_final_status'].items()):
                    if count > 0:
                        percentage = (count / progressive_stats.get('total_signals', 1)) * 100
                        print(f"    - {status:25s}: {count:4d} ({percentage:.1f}%)")
            
            # Show rejection breakdown
            if 'rejection_breakdown' in progressive_stats:
                total_rejected = sum(
                    sum(stage_counts.values()) 
                    for stage_counts in progressive_stats['rejection_breakdown'].values()
                )
                if total_rejected > 0:
                    print(f"  Rejection Breakdown:")
                    for stage, reasons in progressive_stats['rejection_breakdown'].items():
                        stage_total = sum(reasons.values())
                        print(f"    - {stage}: {stage_total}")
        
        print(f"\n📁 OUTPUT FILES:")
        print(f"  Configuration:       {Path(config_path).name}")
        print(f"  JSON Report:         {json_path.relative_to(project_root)}")
        if csv_path:
            print(f"  CSV Trades:          {csv_path.relative_to(project_root)}")
        print(f"  Enhanced Progressive CSV: {progressive_csv_path.relative_to(project_root)}")
        print(f"  Total Columns in CSV: {len(progressive_tracker.columns)}")
        
        print(f"\n⏱️  Execution Time:     {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("\n" + "="*70)
        
        return df_strategy, simulation_results['all_trades'], report_data

    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    import pandas as pd
    
    if len(sys.argv) > 1:
        verbose_flag = '--verbose' in sys.argv
        config_arg = sys.argv[1] if not sys.argv[1] == '--verbose' else sys.argv[2]
        run_wbws_strategy(config_arg, verbose=verbose_flag)
    else:
        print("❌ Usage: python scripts/run_wbws_strategy.py <config_path> [--verbose]")
        print("\nExample:")
        print("  python scripts/run_wbws_strategy.py src/config/WBWS/wbws_rsi_strategy.yaml")
        print("  python scripts/run_wbws_strategy.py src/config/WBWS/wbws_rsi_strategy.yaml --verbose")
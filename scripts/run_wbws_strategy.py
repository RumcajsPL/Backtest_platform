"""
WBWS Strategy Runner - Modular Version
Main orchestrator that coordinates all modules
"""
import sys
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

def run_wbws_strategy(config_path: str, verbose: bool = False):
    """
    Main orchestrator for WBWS strategy execution
    
    Args:
        config_path: Path to YAML configuration file
        verbose: Whether to print detailed logs
    """
    print("\n" + "="*70)
    print("🚀 WBWS STRATEGY WORKFLOW - MODULAR VERSION")
    print("="*70 + "\n")
    
    try:
        # 1. Load configuration and data
        print("📊 STEP 1: LOADING DATA")
        data_loader = DataLoader(config_path)
        config = data_loader.load_config()
        df_full, df_strategy = data_loader.load_data()
        
        data_info = data_loader.get_data_info()
        print(f"  Full dataset: {data_info['full_bars']:,} bars")
        print(f"  Strategy period: {data_info['strategy_bars']:,} bars")
        print(f"  Date range: {data_info['date_range'][0]} to {data_info['date_range'][1]}")
        
        # 2. Generate signals
        print("\n📈 STEP 2: GENERATING SIGNALS")
        signal_gen = SignalGenerator(config['indicator']['htf_period'])
        raw_signals, _ = signal_gen.generate_signals(df_strategy)
        
        signal_stats = signal_gen.get_signal_stats(raw_signals)
        print(f"  Raw BUY: {signal_stats['buy']:,}, Raw SELL: {signal_stats['sell']:,}, "
              f"Total: {signal_stats['total']:,}")
        
        # 3. Apply filters
        print("\n🎯 STEP 3: APPLYING FILTERS")
        filter_pipeline = FilterPipeline(config)
        
        # Time filter
        print("  Applying time filter...")
        time_filtered = filter_pipeline.apply_time_filter(raw_signals)
        time_stats = {
            'buy': int((time_filtered == 'BUY').sum()),
            'sell': int((time_filtered == 'SELL').sum()),
            'total': int((time_filtered.notna()).sum())
        }
        print(f"    → Time filtered: {time_stats['total']:,} signals "
              f"({time_stats['buy']:,} BUY, {time_stats['sell']:,} SELL)")
        
        # RSI filter
        print("  Applying RSI filter...")
        rsi_filtered = filter_pipeline.apply_rsi_filter(df_strategy, time_filtered)
        rsi_stats = {
            'buy': int((rsi_filtered == 'BUY').sum()),
            'sell': int((rsi_filtered == 'SELL').sum()),
            'total': int((rsi_filtered.notna()).sum())
        }
        print(f"    → RSI filtered: {rsi_stats['total']:,} signals "
              f"({rsi_stats['buy']:,} BUY, {rsi_stats['sell']:,} SELL)")
        
        # Risk filter
        print("  Applying risk management...")
        filter_pipeline.initialize_risk_manager(df_full)
        potential_trades, risk_stats = filter_pipeline.apply_risk_filter(df_strategy, rsi_filtered)
        
        print(f"    → Risk approved: {risk_stats['total_approved']:,} trades "
              f"({risk_stats['approved']['buy']:,} BUY, {risk_stats['approved']['sell']:,} SELL)")
        if risk_stats['total_adjusted'] > 0:
            print(f"    → SL adjusted: {risk_stats['total_adjusted']:,} trades")
        
        # Get comprehensive filter stats
        filter_stats = filter_pipeline.get_filter_stats(raw_signals, time_filtered, 
                                                       rsi_filtered, risk_stats)
        
        # 4. Simulate trades
        print("\n🔄 STEP 4: SIMULATING TRADES")
        trade_simulator = TradeSimulator(config)
        simulation_results = trade_simulator.simulate_trades(
            df_strategy, potential_trades, verbose=verbose
        )
        
        print(f"  Simulated trades: {len(simulation_results['closed_trades']):,} closed, "
              f"{len(simulation_results['open_trades']):,} open, "
              f"{len(simulation_results['rejected_trades']):,} rejected")
        
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
        
        # Generate CSV
        print("  Generating CSV report...")
        csv_path = report_gen.generate_csv(simulation_results['all_trades'], timestamp_str)
        if csv_path:
            print(f"    → CSV saved: {csv_path.relative_to(project_root)}")
        
        # Build and save JSON report
        print("  Generating JSON report...")
        report_data = report_gen.build_report_data(
            config, data_info, filter_stats, simulation_results,
            performance_metrics, csv_path
        )
        
        json_path = report_gen.generate_json(report_data, timestamp_str)
        print(f"    → JSON saved: {json_path.relative_to(project_root)}")
        
        # 7. Display final summary
        print("\n" + "="*70)
        print("✅ STRATEGY EXECUTION COMPLETED")
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
        
        print(f"\n📁 OUTPUT FILES:")
        print(f"  Configuration:       {Path(config_path).name}")
        print(f"  JSON Report:         {json_path.relative_to(project_root)}")
        if csv_path:
            print(f"  CSV Trades:          {csv_path.relative_to(project_root)}")
        
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
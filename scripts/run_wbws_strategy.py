# Updated: scripts/run_wbws_strategy.py
"""WBWS Strategy Runner - Production Optimized"""
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler

# Force UTF-8 for console (Windows safeguard)
if sys.platform.startswith('win'):
    import os
    os.system('chcp 65001 > nul')
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# Get project root
project_root = Path(__file__).resolve().parent.parent

# Add project root to sys.path
sys.path.insert(0, str(project_root))

from strategy_modules.data_loader import DataLoader
from strategy_modules.signal_generator import SignalGenerator
from strategy_modules.filter_pipeline import FilterPipeline
from strategy_modules.trade_simulator import TradeSimulator
from strategy_modules.report_generator import ReportGenerator
from strategy_modules.metrics_calculator import calculate_performance_metrics
from strategy_modules.progressive_tracker import EnhancedProgressiveTracker
from strategy_modules.null_progressive_tracker import NullProgressiveTracker

# Logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

console = logging.StreamHandler(sys.stdout)
console.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logger.addHandler(console)

log_file = project_root / 'outputs' / 'logs' / 'wbws_strategy.log'
file_handler = RotatingFileHandler(
    log_file,
    maxBytes=10*1024*1024,
    backupCount=5,
    encoding='utf-8'
)
file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logger.addHandler(file_handler)

def run_wbws_strategy(config_path: str, verbose: bool = False):
    """Main orchestrator for WBWS strategy execution"""
    logger.info("="*70)
    logger.info("WBWS STRATEGY WORKFLOW")
    logger.info("="*70)
    
    try:
        # 1. Load configuration and data
        logger.info("STEP 1: LOADING DATA")
        data_loader = DataLoader(config_path)
        config = data_loader.load_config()
        
        # Mode detection
        execution_mode = config.get('execution', {}).get('mode', 'debug')
        is_core_mode = (execution_mode == 'core')
        
        if is_core_mode:
            logger.info("  CORE MODE: Optimized for pipeline performance")
            config.setdefault('output', {})
            config['output']['enable_progressive_tracking'] = False
            config['output']['enable_detailed_metrics'] = False
            config['output']['save_signals_csv'] = False
            config['output']['enable_cache_stats'] = False
            verbose = False
        else:
            logger.info("  DEBUG MODE: Full detailed tracking and outputs")
            config.setdefault('output', {})
            config['output'].setdefault('enable_progressive_tracking', True)
            config['output'].setdefault('enable_detailed_metrics', True)
            config['output'].setdefault('save_signals_csv', True)
            config['output'].setdefault('enable_cache_stats', True)
        
        # Load data
        df_full, df_strategy, df_htf, df_ltf = data_loader.load_data()
        
        # Mandatory LTF check
        if df_ltf is None or df_ltf.empty:
            logger.error("EXECUTION ABORTED: LTF data missing")
            logger.error("Low Timeframe (LTF) data is mandatory for realistic execution simulation.")
            logger.error("Check your config paths for LTF data file.")
            raise ValueError(
                "LTF data missing or empty. "
                "Verify config paths.ltf_ohlcv_file points to valid data."
            )
        
        # Initialize progressive tracker
        enable_tracking = config['output'].get('enable_progressive_tracking', not is_core_mode)
        if enable_tracking:
            progressive_tracker = EnhancedProgressiveTracker(config)
            if not is_core_mode:
                logger.info("  Progressive tracking: ENABLED")
        else:
            progressive_tracker = NullProgressiveTracker(config)
            logger.info("  Progressive tracking: DISABLED (null tracker)")
        
        data_info = data_loader.get_data_info()
        logger.info(f"  Full dataset: {data_info['full_bars']:,} bars")
        logger.info(f"  Strategy period: {data_info['strategy_bars']:,} bars")
        if df_htf is not None:
            logger.info(f"  HTF dataset: {data_info['htf_bars']:,} bars")
        logger.info(f"  LTF dataset: {data_info['ltf_bars']:,} bars (TF: {data_info.get('ltf_tf', 'N/A')})")
        logger.info(f"  Date range: {data_info['date_range'][0]} to {data_info['date_range'][1]}")

        # Validate data
        validation = data_loader.validate_data()
        if not validation['is_valid']:
            raise ValueError(f"Data validation failed: {validation}")
        
        # 2. Generate signals
        logger.info("STEP 2: GENERATING SIGNALS")
        signal_gen = SignalGenerator(config['indicator']['htf_period'])
        raw_signals, indicator_values = signal_gen.generate_signals(df_strategy, df_htf=df_htf)
        
        htf_signals = getattr(signal_gen, 'htf_signals', None)
        
        signal_id_map = {}
        if enable_tracking:
            logger.info("  Recording raw signals...")
            for timestamp, signal in raw_signals.dropna().items():
                mid_price = df_strategy.loc[timestamp, 'close']
                indicator_row = indicator_values.loc[timestamp] if timestamp in indicator_values.index else None
                htf_signal = htf_signals.loc[timestamp] if htf_signals is not None and timestamp in htf_signals.index else None
                
                signal_id = progressive_tracker.record_raw_signal(
                    timestamp=timestamp,
                    signal=signal,
                    mid_price=mid_price,
                    indicator_row=indicator_row,
                    htf_signal=htf_signal
                )
                signal_id_map[timestamp] = signal_id
        else:
            signal_id_map = {ts: 0 for ts in raw_signals.dropna().index}
        
        signal_stats = signal_gen.get_signal_stats(raw_signals)
        logger.info(f"  Raw BUY: {signal_stats['buy']:,}, SELL: {signal_stats['sell']:,}, Total: {signal_stats['total']:,}")
        
        # 3. Apply filters
        logger.info("STEP 3: APPLYING FILTERS")
        filter_pipeline = FilterPipeline(config)
        filter_pipeline.initialize_risk_manager(df_full)
        filter_pipeline.set_progressive_tracker(progressive_tracker)
        
        logger.info("  Applying time filter...")
        time_filtered = filter_pipeline.apply_time_filter(raw_signals, signal_id_map)
        time_stats = {
            'buy': int((time_filtered == 'BUY').sum()),
            'sell': int((time_filtered == 'SELL').sum()),
            'total': int(time_filtered.notna().sum())
        }
        logger.info(f"    Time filtered: {time_stats['total']:,} ({time_stats['buy']:,} BUY, {time_stats['sell']:,} SELL)")
        
        logger.info("  Applying RSI filter...")
        rsi_filtered = filter_pipeline.apply_rsi_filter(df_strategy, time_filtered, signal_id_map)
        rsi_stats = {
            'buy': int((rsi_filtered == 'BUY').sum()),
            'sell': int((rsi_filtered == 'SELL').sum()),
            'total': int(rsi_filtered.notna().sum())
        }
        logger.info(f"    RSI filtered: {rsi_stats['total']:,} ({rsi_stats['buy']:,} BUY, {rsi_stats['sell']:,} SELL)")
        
        filter_stats = filter_pipeline.get_filter_stats(raw_signals, time_filtered, rsi_filtered)
        
        # 4. Simulate trades
        # TradeSimulator now handles ALL progressive tracking internally
        logger.info("STEP 4: SIMULATING TRADES")
        trade_simulator = TradeSimulator(config)
        simulation_results = trade_simulator.simulate_trades(
            df_strategy, rsi_filtered, 
            verbose=verbose and not is_core_mode,
            progressive_tracker=progressive_tracker,
            risk_manager=filter_pipeline.filters['risk'],
            signal_id_map=signal_id_map,
            df_ltf=df_ltf
        )
        
        logger.info(f"  Simulated: {len(simulation_results['closed_trades']):,} closed, "
                    f"{len(simulation_results['open_trades']):,} open, "
                    f"{len(simulation_results['rejected_trades']):,} rejected")
        logger.info(f"  Execution: LTF ({data_info.get('ltf_tf', 'N/A')}) for SL/TP")
        
        filter_stats['risk_filtered'] = simulation_results.get('risk_stats', {})
        
        # 5. Metrics
        logger.info("STEP 5: CALCULATING METRICS")
        if simulation_results['all_trades']:
            trades_df = pd.DataFrame(simulation_results['all_trades'])
            detailed = config['output'].get('enable_detailed_metrics', not is_core_mode)
            performance_metrics = calculate_performance_metrics(trades_df, df_strategy, detailed=detailed)
            
            logger.info(f"  Total P&L: {performance_metrics['total_pnl_points']:+,.2f} pts")
            logger.info(f"  Win Rate: {performance_metrics['win_rate']:.1f}%")
            logger.info(f"  Profit Factor: {performance_metrics['profit_factor']:.2f}")
            if not is_core_mode:
                logger.info(f"  Max Drawdown: {performance_metrics.get('max_drawdown_points', 0):.2f} pts")
        else:
            performance_metrics = {'total_trades': 0, 'message': 'No trades executed'}
            logger.info("  No trades executed")
        
        # 6. Reports
        logger.info("STEP 6: GENERATING REPORTS")
        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_gen = ReportGenerator(config, project_root)
        
        progressive_csv_path = None
        if enable_tracking and not is_core_mode:
            logger.info("  Generating enhanced progressive CSV...")
            progressive_csv_path = progressive_tracker.save_to_csv(project_root, timestamp_str)
            logger.info(f"    Saved: {progressive_csv_path.relative_to(project_root)}")
        
        csv_path = None
        if config['output'].get('save_signals_csv', not is_core_mode):
            logger.info("  Generating trade CSV...")
            csv_path = report_gen.generate_csv(simulation_results['all_trades'], timestamp_str)
            if csv_path:
                logger.info(f"    Saved: {csv_path.relative_to(project_root)}")
        
        logger.info("  Generating JSON report...")
        progressive_stats = progressive_tracker.get_statistics()
        filter_stats['progressive'] = progressive_stats
        
        report_data = report_gen.build_report_data(
            config, data_info, filter_stats, simulation_results,
            performance_metrics, csv_path, mode=execution_mode
        )
        
        if config['output'].get('enable_cache_stats', not is_core_mode):
            data_loader_stats = data_loader.get_cache_stats()
            report_data.setdefault('validation', {})['data_loader_cache_stats'] = data_loader_stats
            report_data['data_loader_cache_stats'] = data_loader_stats
        
        if not is_core_mode and progressive_csv_path:
            report_data.setdefault('progressive_tracking', {}).update({
                'progressive_csv_file': str(progressive_csv_path.relative_to(project_root)),
                'signal_progression_summary': progressive_stats,
                'total_signals_tracked': progressive_stats.get('total_signals', 0),
            })
        
        json_path = report_gen.generate_json(report_data, timestamp_str)
        logger.info(f"    JSON saved: {json_path.relative_to(project_root)}")
        
        # Final summary
        logger.info("="*70)
        logger.info(f"EXECUTION COMPLETED ({execution_mode.upper()} MODE)")
        logger.info("="*70)
        
        total_raw = filter_stats['raw']['total']
        total_executed = len(simulation_results['closed_trades'])
        rejection_rate = ((total_raw - total_executed) / total_raw * 100) if total_raw > 0 else 0
        
        logger.info(f"PERFORMANCE SUMMARY:")
        logger.info(f"  Mode:              {execution_mode.upper()}")
        logger.info(f"  Raw Signals:       {total_raw:,}")
        logger.info(f"  Executed Trades:   {total_executed:,}")
        logger.info(f"  Rejection Rate:    {rejection_rate:.1f}%")
        
        if performance_metrics.get('total_trades', 0) > 0:
            logger.info(f"  Total P&L:         {performance_metrics['total_pnl_points']:+,.2f} pts")
            logger.info(f"  Win Rate:          {performance_metrics['win_rate']:.1f}%")
            logger.info(f"  Profit Factor:     {performance_metrics['profit_factor']:.2f}")
        
        if not is_core_mode and progressive_stats and progressive_stats.get('total_signals', 0) > 0:
            logger.info("PROGRESSIVE SUMMARY:")
            logger.info(f"  Tracked Signals:   {progressive_stats.get('total_signals', 0):,}")
            if 'by_final_status' in progressive_stats:
                logger.info("  Final Status:")
                for status, count in sorted(progressive_stats['by_final_status'].items()):
                    if count > 0:
                        pct = (count / progressive_stats.get('total_signals', 1)) * 100
                        logger.info(f"    - {status:25s}: {count:4d} ({pct:.1f}%)")
        
        logger.info("OUTPUT FILES:")
        logger.info(f"  Config:            {Path(config_path).name}")
        logger.info(f"  JSON Report:       {json_path.relative_to(project_root)}")
        if csv_path:
            logger.info(f"  Trade CSV:         {csv_path.relative_to(project_root)}")
        if not is_core_mode and progressive_csv_path:
            logger.info(f"  Progressive CSV:   {progressive_csv_path.relative_to(project_root)}")
        
        logger.info(f"Completed:         {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*70)
        
        if config['output'].get('enable_cache_stats', not is_core_mode):
            stats = data_loader.get_cache_stats()
            logger.info("="*50)
            logger.info("DATA LOADER CACHE STATISTICS")
            logger.info(f"Hits: {stats['hits']}")
            logger.info(f"Misses: {stats['misses']}")
            logger.info(f"Hit rate: {stats['hit_rate']}")
            logger.info("="*50)
        
        return df_strategy, simulation_results['all_trades'], report_data

    except Exception as e:
        logger.error(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
if __name__ == "__main__":
    if len(sys.argv) > 1:
        verbose_flag = '--verbose' in sys.argv
        config_arg = sys.argv[1] if not sys.argv[1] == '--verbose' else sys.argv[2]
        run_wbws_strategy(config_arg, verbose=verbose_flag)
    else:
        print("Usage: python scripts/run_wbws_strategy.py <config_path> [--verbose]")
        sys.exit(1)
# tests/test_ltf_debug.py
"""
Diagnostic script for LTF execution precision testing.
Run this to verify that 1-second precision is being properly applied.
"""
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime

# Get project root
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

def test_ltf_data_loading():
    """Test 1: Check LTF data loading and precision"""
    print("=" * 70)
    print("🔍 TEST 1: LTF DATA LOADING & PRECISION CHECK")
    print("=" * 70)
    
    try:
        from scripts.strategy_modules.data_loader import DataLoader
        
        config_path = "src/config/WBWS/wbws_rsi_strategy.yaml"
        print(f"Loading config from: {config_path}")
        
        data_loader = DataLoader(config_path)
        config = data_loader.load_config()
        df_full, df_strategy, df_htf, df_ltf = data_loader.load_data()
        
        print(f"\n📊 DATA LOADING RESULTS:")
        print(f"  Strategy TF (1min): {df_strategy.shape[0]:,} bars")
        print(f"  Strategy time range: {df_strategy.index[0]} to {df_strategy.index[-1]}")
        
        # Check timestamp precision
        strategy_sample = df_strategy.index[:3]
        print(f"  Strategy sample timestamps:")
        for ts in strategy_sample:
            print(f"    - {ts} (second={ts.second})")
        
        if df_ltf is not None:
            print(f"\n  LTF (1s): {df_ltf.shape[0]:,} bars")
            print(f"  LTF time range: {df_ltf.index[0]} to {df_ltf.index[-1]}")
            
            # Check LTF timestamp precision
            ltf_sample = df_ltf.index[:5]
            print(f"  LTF sample timestamps:")
            for ts in ltf_sample:
                print(f"    - {ts} (second={ts.second}, microsecond={ts.microsecond})")
            
            # Check if seconds are present
            has_seconds = any(ts.second > 0 for ts in df_ltf.index[:100])
            has_microseconds = any(ts.microsecond > 0 for ts in df_ltf.index[:100])
            
            print(f"\n  ✅ LTF timestamps have seconds: {has_seconds}")
            print(f"  ⚠️  LTF timestamps have microseconds: {has_microseconds}")
            
            # Check coverage
            strategy_start = df_strategy.index[0]
            strategy_end = df_strategy.index[-1]
            ltf_start = df_ltf.index[0]
            ltf_end = df_ltf.index[-1]
            
            print(f"\n  📅 COVERAGE CHECK:")
            print(f"    Strategy start: {strategy_start}")
            print(f"    LTF start:     {ltf_start}")
            print(f"    Strategy end:  {strategy_end}")
            print(f"    LTF end:       {ltf_end}")
            print(f"    ⬇️  LTF covers start: {ltf_start <= strategy_start}")
            print(f"    ⬆️  LTF covers end:   {ltf_end >= strategy_end}")
            
            if not (ltf_start <= strategy_start and ltf_end >= strategy_end):
                print("    ⚠️  WARNING: LTF doesn't fully cover strategy period!")
            
            # Check data frequency
            time_diffs = pd.Series(df_ltf.index).diff().dropna()
            avg_freq = time_diffs.mean().total_seconds()
            print(f"\n  ⏱️  LTF frequency check:")
            print(f"    Average interval: {avg_freq:.3f} seconds")
            print(f"    Expected: 1.000 seconds")
            print(f"    Match: {abs(avg_freq - 1.0) < 0.1}")
            
        else:
            print("\n  ❌ LTF data not loaded - check config file!")
            print(f"    Config has 'file_ltf': {'file_ltf' in config.get('data', {})}")
            
        return df_strategy, df_ltf
        
    except Exception as e:
        print(f"\n❌ ERROR in test_ltf_data_loading: {e}")
        import traceback
        traceback.print_exc()
        return None, None

def test_trade_simulation_precision():
    """Test 2: Check trade simulation with LTF precision"""
    print("\n" + "=" * 70)
    print("🔍 TEST 2: TRADE SIMULATION PRECISION CHECK")
    print("=" * 70)
    
    try:
        # Import required modules
        from scripts.strategy_modules.data_loader import DataLoader
        from scripts.strategy_modules.signal_generator import SignalGenerator
        from scripts.strategy_modules.filter_pipeline import FilterPipeline
        from scripts.strategy_modules.trade_simulator import TradeSimulator
        
        config_path = "src/config/WBWS/wbws_rsi_strategy.yaml"
        print(f"Loading config from: {config_path}")
        
        # Load data
        data_loader = DataLoader(config_path)
        config = data_loader.load_config()
        df_full, df_strategy, df_htf, df_ltf = data_loader.load_data()
        
        if df_ltf is None:
            print("❌ Cannot run simulation test - no LTF data loaded")
            return
        
        # Generate signals
        print(f"\n📈 Generating signals...")
        signal_gen = SignalGenerator(config['indicator']['htf_period'])
        raw_signals, indicator_values = signal_gen.generate_signals(df_strategy, df_htf=df_htf)
        
        # Apply filters
        print(f"🎯 Applying filters...")
        filter_pipeline = FilterPipeline(config)
        filter_pipeline.initialize_risk_manager(df_full)
        
        time_filtered = filter_pipeline.apply_time_filter(raw_signals, {})
        rsi_filtered = filter_pipeline.apply_rsi_filter(df_strategy, time_filtered, {})
        
        signal_stats = signal_gen.get_signal_stats(rsi_filtered)
        print(f"  Filtered signals: {signal_stats['total']:,} ({signal_stats['buy']:,} BUY, {signal_stats['sell']:,} SELL)")
        
        # Run simulation with enhanced debugging
        print(f"\n🔄 Running trade simulation with enhanced debugging...")
        
        # Create a debug version of TradeSimulator
        class DebugTradeSimulator(TradeSimulator):
            def __init__(self, config):
                super().__init__(config)
                self.debug_exits = []
                self.debug_entries = []
            
            def _execute_trade_exit(self, trade: dict, exit_time: pd.Timestamp, 
                                   exit_price: float, exit_reason: str, verbose: bool):
                """Override to capture debug info"""
                # Capture debug info before parent processing
                debug_info = {
                    'trade_id': trade.get('trade_id'),
                    'direction': trade.get('direction'),
                    'entry_time': trade.get('entry_time'),
                    'raw_exit_time': exit_time,
                    'exit_time_str': str(exit_time),
                    'exit_time_has_seconds': exit_time.second > 0,
                    'exit_time_second_value': exit_time.second,
                    'exit_price': exit_price,
                    'exit_reason': exit_reason,
                    'timestamp_type': type(exit_time).__name__
                }
                self.debug_exits.append(debug_info)
                
                if verbose:
                    print(f"🔍 DEBUG EXIT: Trade {trade.get('trade_id')}")
                    print(f"    Entry: {trade.get('entry_time')}")
                    print(f"    Exit:  {exit_time} (second={exit_time.second})")
                    print(f"    Price: {exit_price:.5f}")
                    print(f"    Reason: {exit_reason}")
                
                # Call parent method
                super()._execute_trade_exit(trade, exit_time, exit_price, exit_reason, verbose)
        
        # Run simulation
        trade_simulator = DebugTradeSimulator(config)
        
        # Get risk manager
        risk_manager = filter_pipeline.filters['risk']
        
        # Run simulation
        simulation_results = trade_simulator.simulate_trades(
            df_strategy=df_strategy,
            filtered_signals=rsi_filtered,
            verbose=True,  # Enable verbose for debugging
            progressive_tracker=None,
            risk_manager=risk_manager,
            signal_id_map={},
            df_ltf=df_ltf
        )
        
        # Analyze results
        print(f"\n📊 SIMULATION RESULTS:")
        print(f"  Total trades: {len(simulation_results['all_trades'])}")
        print(f"  Closed trades: {len(simulation_results['closed_trades'])}")
        print(f"  Open trades: {len(simulation_results['open_trades'])}")
        print(f"  Execution mode: {simulation_results.get('execution_mode', 'Unknown')}")
        
        # Analyze exit precision
        if trade_simulator.debug_exits:
            print(f"\n🔍 EXIT PRECISION ANALYSIS:")
            print(f"  Total exits recorded: {len(trade_simulator.debug_exits)}")
            
            exits_with_seconds = sum(1 for e in trade_simulator.debug_exits if e['exit_time_has_seconds'])
            exits_on_minute = sum(1 for e in trade_simulator.debug_exits if e['exit_time_second_value'] == 0)
            
            print(f"  Exits with second precision: {exits_with_seconds} ({exits_with_seconds/len(trade_simulator.debug_exits)*100:.1f}%)")
            print(f"  Exits on minute boundary: {exits_on_minute} ({exits_on_minute/len(trade_simulator.debug_exits)*100:.1f}%)")
            
            if trade_simulator.debug_exits:
                print(f"\n  Sample exits (first 3):")
                for i, exit_info in enumerate(trade_simulator.debug_exits[:3]):
                    print(f"    Exit {i+1}:")
                    print(f"      Trade ID: {exit_info['trade_id']}")
                    print(f"      Direction: {exit_info['direction']}")
                    print(f"      Entry: {exit_info['entry_time']}")
                    print(f"      Exit: {exit_info['raw_exit_time']}")
                    print(f"      Has seconds: {exit_info['exit_time_has_seconds']}")
                    print(f"      Second value: {exit_info['exit_time_second_value']}")
                    print(f"      Exit reason: {exit_info['exit_reason']}")
        
        # Check if trades in results have second precision
        print(f"\n🔍 FINAL TRADE DATA CHECK:")
        all_trades = simulation_results['all_trades']
        
        if all_trades:
            trades_with_second_precision = 0
            print(f"  Checking {len(all_trades)} trades in results...")
            
            for i, trade in enumerate(all_trades[:3]):  # Check first 3
                print(f"\n  Trade {i+1} (ID: {trade.get('trade_id')}):")
                
                for time_key in ['entry_time', 'exit_time', 'timestamp']:
                    if time_key in trade and trade[time_key] is not None:
                        time_val = trade[time_key]
                        if hasattr(time_val, 'second'):
                            has_seconds = time_val.second > 0
                            print(f"    {time_key}: {time_val} (second={time_val.second}, has_seconds={has_seconds})")
                            if time_key == 'exit_time' and has_seconds:
                                trades_with_second_precision += 1
                        else:
                            print(f"    {time_key}: {time_val} (not a timestamp)")
        
        # Generate comparison report
        print(f"\n📋 COMPARISON REPORT:")
        print(f"  1. LTF data loaded: {'✅' if df_ltf is not None else '❌'}")
        print(f"  2. LTF has second precision: {'✅' if df_ltf is not None and any(ts.second > 0 for ts in df_ltf.index[:100]) else '❌'}")
        print(f"  3. Simulation used LTF: {'✅' if simulation_results.get('execution_mode') == 'LTF' else '❌'}")
        if trade_simulator.debug_exits:
            percent_with_seconds = exits_with_seconds/len(trade_simulator.debug_exits)*100
            print(f"  4. Exit precision: {percent_with_seconds:.1f}% with seconds")
            print(f"  5. Expected result: >0% exits should have second precision")
        
        return simulation_results
        
    except Exception as e:
        print(f"\n❌ ERROR in test_trade_simulation_precision: {e}")
        import traceback
        traceback.print_exc()
        return None

def test_csv_output_precision():
    """Test 3: Check if CSV output preserves second precision"""
    print("\n" + "=" * 70)
    print("🔍 TEST 3: CSV OUTPUT PRECISION CHECK")
    print("=" * 70)
    
    try:
        # Simulate a trade with second precision
        test_trades = [
            {
                'trade_id': 999,
                'direction': 'BUY',
                'entry_time': pd.Timestamp('2025-12-15 10:30:00'),
                'exit_time': pd.Timestamp('2025-12-15 10:45:15.500'),  # With seconds and milliseconds
                'entry_price': 100.0,
                'exit_price': 101.5,
                'exit_reason': 'TAKE_PROFIT'
            }
        ]
        
        # Save to CSV
        from scripts.strategy_modules.report_generator import ReportGenerator
        import tempfile
        import os
        
        # Create a temp config
        temp_config = {'output': {'outputs_dir': 'outputs'}}
        
        report_gen = ReportGenerator(temp_config, project_root)
        
        # Create temp directory
        temp_dir = tempfile.mkdtemp()
        csv_path = os.path.join(temp_dir, 'test_precision.csv')
        
        # Convert to DataFrame and save
        df_trades = pd.DataFrame(test_trades)
        df_trades.to_csv(csv_path, index=False)
        
        # Reload and check
        df_loaded = pd.read_csv(csv_path, parse_dates=['entry_time', 'exit_time'])
        
        print(f"📁 CSV Precision Test:")
        print(f"  Original exit_time: {test_trades[0]['exit_time']}")
        print(f"  Loaded exit_time:   {df_loaded.iloc[0]['exit_time']}")
        print(f"  Type: {type(df_loaded.iloc[0]['exit_time'])}")
        
        if hasattr(df_loaded.iloc[0]['exit_time'], 'second'):
            print(f"  Has seconds: {df_loaded.iloc[0]['exit_time'].second > 0}")
            print(f"  Second value: {df_loaded.iloc[0]['exit_time'].second}")
            print(f"  Microsecond: {df_loaded.iloc[0]['exit_time'].microsecond}")
        
        print(f"\n✅ CSV preserves datetime precision: {df_loaded.iloc[0]['exit_time'].second == 15}")
        
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir)
        
    except Exception as e:
        print(f"❌ ERROR in test_csv_output_precision: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main diagnostic function"""
    print("\n" + "=" * 70)
    print("🔧 LTF EXECUTION PRECISION DIAGNOSTIC TOOL")
    print("=" * 70)
    print(f"Run time: {datetime.now()}")
    print(f"Project root: {project_root}")
    
    # Run all tests
    df_strategy, df_ltf = test_ltf_data_loading()
    
    if df_ltf is not None:
        simulation_results = test_trade_simulation_precision()
        test_csv_output_precision()
        
        # Final summary
        print("\n" + "=" * 70)
        print("📋 DIAGNOSTIC SUMMARY")
        print("=" * 70)
        print("1. Check LTF data has seconds in timestamps")
        print("2. Verify trade simulator receives and uses LTF data")
        print("3. Confirm exits happen with second precision")
        print("4. Ensure CSV output preserves precision")
        print("\n⚠️  If exits show only minute boundaries:")
        print("   - Check trade_simulator._check_exits_with_ltf()")
        print("   - Verify LTF data covers strategy period")
        print("   - Check if exits are happening in _check_exits_with_strategy_tf() instead")
    else:
        print("\n❌ Cannot proceed - LTF data not loaded!")
        print("   Check your config file has 'file_ltf' defined")
        print("   Example: data.file_ltf: 'data/processed/ohlcv/DEUIDXEUR_1s_20240101_20260104.csv'")

if __name__ == "__main__":
    main()
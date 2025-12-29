"""
Enhanced Standalone Performance Dashboard with Backtesting
Now includes Position Management metrics
"""
import sys
import json
import yaml
import pandas as pd
from pathlib import Path
from typing import Dict, List

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    BOLD = '\033[1m'
    END = '\033[0m'

class EnhancedDashboard:
    def __init__(self, mode: str = "FULL"):
        self.mode = mode.upper()
        self.colors = Colors
        
    def load_all_data(self, report_path: str, config_path: str = None) -> tuple:
        """Load report, config, trades, and OHLCV data with path validation"""
        report_path = Path(report_path).resolve()
        if not report_path.exists():
            raise FileNotFoundError(f"Report file not found: {report_path}")
        
        # 1. Load JSON Report
        with open(report_path, 'r', encoding='utf-8') as f:
            report_data = json.load(f)
        
        # 2. Load Config (prefer argument, fallback to internal report config)
        config = {}
        if config_path:
            cfg_p = Path(config_path)
            if cfg_p.exists():
                with open(cfg_p, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
        
        if not config and 'config' in report_data:
            config = report_data['config']
            
        # 3. Load Trade Details (INTELLIGENT LOADING)
        trades_df = None
        csv_path = None
                
        # Strategy A: Check 'outputs' key in JSON (The new robust way)
        if 'outputs' in report_data and report_data['outputs'].get('signals_csv_file'):
            rel_path = report_data['outputs']['signals_csv_file']
            # Try relative to project root (we need to find project root dynamically or assume)
            # Assumption: Scripts run from project root, so strictly relative path works
            potential_path = Path(rel_path)
            if potential_path.exists():
                csv_path = potential_path
            else:
                # Try relative to report file
                potential_path = report_path.parent / Path(rel_path).name
                if potential_path.exists():
                    csv_path = potential_path

        # Strategy B: Fallback to searching folder (The old risky way)
        if not csv_path:
            # Try same directory as report
            report_dir = report_path.parent
            trade_files = list(report_dir.glob("trade_details_*.csv"))
            if not trade_files:
                # Try standard signals directory
                signals_dir = Path("outputs/signals/strategy")
                if signals_dir.exists():
                    trade_files = list(signals_dir.glob("trade_details_*.csv"))
            
            if trade_files:
                # Warning: Taking the newest file might mismatch the report!
                csv_path = max(trade_files, key=lambda x: x.stat().st_mtime)
                print(f"{Colors.YELLOW}⚠️  Warning: Linked CSV not found in JSON. Using newest found: {csv_path.name}{Colors.END}")

        if csv_path and csv_path.exists():
            try:
                trades_df = pd.read_csv(csv_path, parse_dates=['timestamp'])
                print(f"📁 Loaded trade details: {csv_path}")
            except Exception as e:
                print(f"{Colors.RED}❌ Error loading trades CSV: {e}{Colors.END}")

        # 4. Load OHLCV Data
        ohlcv_df = None
        if config and 'data' in config and 'file' in config['data']:
            data_file = config['data']['file']
            # Try multiple path resolutions
            candidates = [
                Path(data_file), # Absolute or relative to CWD
                report_path.parent.parent.parent / data_file, # Relative to report location logic
                Path("data/processed") / Path(data_file).name # Direct guess
            ]
            
            for path in candidates:
                if path.exists():
                    try:
                        ohlcv_df = pd.read_csv(path, parse_dates=['timestamp'])
                        ohlcv_df.set_index('timestamp', inplace=True)
                        print(f"📁 Loaded OHLCV data: {path}")
                        break
                    except Exception as e:
                         print(f"{Colors.RED}❌ Error loading OHLCV: {e}{Colors.END}")
            
            if ohlcv_df is None:
                 print(f"{Colors.YELLOW}⚠️  OHLCV file not found: {data_file}{Colors.END}")
        
        return report_data, config, trades_df, ohlcv_df
    
    def simulate_backtest(self, trades_df: pd.DataFrame, ohlcv_df: pd.DataFrame, config: Dict) -> Dict:
        """Simulate backtest and calculate real metrics"""
        if trades_df is None or ohlcv_df is None:
            return {}
        
        try:
            from backtest_simulator import TradeSimulator
            # Pass config to simulator for TradeManager integration
            simulator = TradeSimulator(config=config)
            
            # OPTIMIZATION: Slice OHLCV to relevant range + buffer
            if not trades_df.empty:
                start_date = trades_df['timestamp'].min()
                end_date = trades_df['timestamp'].max() + pd.Timedelta(days=7)
                
                # Apply slice to speed up processing
                ohlcv_subset = ohlcv_df[(ohlcv_df.index >= start_date) & (ohlcv_df.index <= end_date)]
                
                if ohlcv_subset.empty:
                    print(f"{self.colors.YELLOW}⚠️  Warning: No OHLCV data found covering trade dates ({start_date} to {end_date}){self.colors.END}")
                    return {}
            else:
                ohlcv_subset = ohlcv_df

            print(f"🔄 Simulating backtest (Pessimistic: SL checked before TP) on {len(ohlcv_subset)} bars...")
            completed_trades = simulator.simulate_trades(trades_df, ohlcv_subset)
            
            if completed_trades:
                metrics = simulator.calculate_metrics(completed_trades)
                
                # Add rejected signals info
                rejected = simulator.get_rejected_signals()
                metrics['rejected_signals'] = rejected
                metrics['rejected_count'] = len(rejected)
                
                return metrics
            else:
                return {}
        except ImportError:
            print("⚠️  Backtest simulator module not found")
            return {}
        except Exception as e:
            print(f"⚠️  Simulation failed: {e}")
            import traceback; traceback.print_exc()
            return {}

    def extract_basic_metrics(self, report_data: Dict) -> Dict:
        """Extract basic metrics from report data"""
        metrics = {}
        # Signal flow metrics
        signal_flow = report_data.get('signal_flow', {})
        metrics.update({
            'raw_buy': signal_flow.get('step1_raw_signals', {}).get('buy', 0),
            'raw_sell': signal_flow.get('step1_raw_signals', {}).get('sell', 0),
            'raw_total': signal_flow.get('step1_raw_signals', {}).get('total', 0),
            'time_buy': signal_flow.get('step2_time_filtered', {}).get('buy', 0),
            'time_sell': signal_flow.get('step2_time_filtered', {}).get('sell', 0),
            'time_total': signal_flow.get('step2_time_filtered', {}).get('total', 0),
            'rsi_buy': signal_flow.get('step3_rsi_filtered', {}).get('buy', 0),
            'rsi_sell': signal_flow.get('step3_rsi_filtered', {}).get('sell', 0),
            'rsi_total': signal_flow.get('step3_rsi_filtered', {}).get('total', 0),
            'final_buy': signal_flow.get('step4_risk_managed', {}).get('buy', 0),
            'final_sell': signal_flow.get('step4_risk_managed', {}).get('sell', 0),
            'final_total': signal_flow.get('step4_risk_managed', {}).get('total', 0),
        })
        return metrics
    
    def color_text(self, text: str, color: str) -> str:
        return f"{color}{text}{self.colors.END}"
    
    def print_header(self, title: str):
        print(f"\n{self.color_text(title, self.colors.BOLD + self.colors.CYAN)}")
        print("=" * 80)
    
    def print_table(self, headers: List[str], rows: List[List], col_widths: List[int] = None):
        if col_widths is None:
            col_widths = [25] + [15] * (len(headers) - 1)
        for i, header in enumerate(headers):
            print(f"{self.color_text(header, self.colors.BOLD):<{col_widths[i]}}", end="")
        print()
        print("-" * sum(col_widths))
        for row in rows:
            for i, cell in enumerate(row):
                if i < len(col_widths):
                    print(f"{str(cell):<{col_widths[i]}}", end="")
            print()

    def display_tradingview_style_dashboard(self, basic_metrics: Dict, backtest_metrics: Dict, config: Dict):
        # 1. PERFORMANCE OVERVIEW
        self.print_header("📈 TRADINGVIEW-STYLE PERFORMANCE DASHBOARD")
        overview_rows = [
            ["Total Trades", str(backtest_metrics.get('total_trades', 0))],
            ["Winning Trades", str(backtest_metrics.get('winning_trades', 0))],
            ["Losing Trades", str(backtest_metrics.get('losing_trades', 0))],
            ["Win Rate", f"{backtest_metrics.get('win_rate', 0):.1f}%"],
            ["Total P&L (pts)", f"{backtest_metrics.get('total_pnl_points', 0):+.2f}"],
            ["Avg P&L/Trade", f"{backtest_metrics.get('avg_pnl_points', 0):+.2f}"],
            ["Profit Factor", f"{backtest_metrics.get('profit_factor', 0):.2f}"],
        ]
        print(f"{self.color_text('TOTAL PERFORMANCE', self.colors.BOLD)}")
        print("-" * 40)
        for label, value in overview_rows:
            print(f"{label:<20} {value}")
        
        # 2. POSITION MANAGEMENT (NEW SECTION)
        if 'trade_manager' in backtest_metrics:
            self.print_header("🎯 POSITION MANAGEMENT")
            tm = backtest_metrics['trade_manager']
            
            # Display settings
            position_cfg = config.get('trade_management', {}).get('position_control', {})
            close_opposite = position_cfg.get('close_on_opposite', False)
            pyramiding = position_cfg.get('pyramiding_enabled', False)
            
            print(f"{self.color_text('SETTINGS', self.colors.BOLD)}")
            print("-" * 40)
            print(f"{'Close on Opposite':<25} {'ENABLED' if close_opposite else 'DISABLED'}")
            print(f"{'Pyramiding':<25} {'ENABLED' if pyramiding else 'DISABLED'}")
            
            print(f"\n{self.color_text('SIGNAL PROCESSING', self.colors.BOLD)}")
            print("-" * 40)
            print(f"{'Total Signals Received':<25} {tm.get('total_signals_received', 0)}")
            print(f"{'Signals Accepted':<25} {tm.get('signals_accepted', 0)}")
            print(f"{'Signals Rejected':<25} {tm.get('signals_rejected', 0)} ({tm.get('signals_rejected', 0) / max(tm.get('total_signals_received', 1), 1) * 100:.1f}%)")
            
            # Rejection breakdown
            reasons = tm.get('rejected_reasons', {})
            if reasons.get('pyramiding_disabled', 0) > 0 or reasons.get('opposite_ignored', 0) > 0:
                print(f"\n{self.color_text('REJECTION REASONS', self.colors.BOLD)}")
                print("-" * 40)
                if reasons.get('pyramiding_disabled', 0) > 0:
                    print(f"{'  Pyramiding Disabled':<25} {reasons['pyramiding_disabled']}")
                if reasons.get('opposite_ignored', 0) > 0:
                    print(f"{'  Opposite Ignored':<25} {reasons['opposite_ignored']}")
            
            # Close & Reverse stats
            if close_opposite and tm.get('positions_closed_by_opposite', 0) > 0:
                print(f"\n{self.color_text('CLOSE & REVERSE', self.colors.BOLD)}")
                print("-" * 40)
                print(f"{'Positions Closed':<25} {tm.get('positions_closed_by_opposite', 0)}")
                print(f"{'Positions Reversed':<25} {tm.get('positions_reversed', 0)}")
        
        # 3. LONG/SHORT BREAKDOWN
        self.print_header("🔀 LONG/SHORT PERFORMANCE")
        breakdown_headers = ["Metric", "LONG", "SHORT", "TOTAL"]
        breakdown_rows = [
            ["Trades", str(backtest_metrics.get('long_trades', 0)), str(backtest_metrics.get('short_trades', 0)), str(backtest_metrics.get('total_trades', 0))],
            ["Win Rate", f"{backtest_metrics.get('long_win_rate', 0):.1f}%", f"{backtest_metrics.get('short_win_rate', 0):.1f}%", f"{backtest_metrics.get('win_rate', 0):.1f}%"],
            ["P&L (pts)", f"{backtest_metrics.get('long_pnl_points', 0):+.2f}", f"{backtest_metrics.get('short_pnl_points', 0):+.2f}", f"{backtest_metrics.get('total_pnl_points', 0):+.2f}"],
        ]
        self.print_table(breakdown_headers, breakdown_rows)
        
        # 4. STATS
        self.print_header("⏱️ TRADE STATISTICS")
        stats_rows = [
            ["Avg Duration (min)", f"{backtest_metrics.get('avg_duration_minutes', 0):.1f}"],
            ["Avg Bars/Trade", f"{backtest_metrics.get('avg_duration_bars', 0):.1f}"],
            ["Max Consecutive Wins", str(backtest_metrics.get('max_consecutive_wins', 0))],
            ["Max Consecutive Losses", str(backtest_metrics.get('max_consecutive_losses', 0))],
        ]
        for label, value in stats_rows:
            print(f"{label:<25} {value}")

        # 5. EXITS
        self.print_header("🚪 EXIT STATISTICS")
        total = backtest_metrics.get('total_trades', 1) or 1
        exit_rows = [
            ["Take Profit", f"{backtest_metrics.get('take_profit_exits', 0)} ({backtest_metrics.get('take_profit_exits', 0)/total*100:.1f}%)"],
            ["Stop Loss", f"{backtest_metrics.get('stop_loss_exits', 0)} ({backtest_metrics.get('stop_loss_exits', 0)/total*100:.1f}%)"],
        ]
        
        # Add opposite signal exits if feature enabled
        if backtest_metrics.get('opposite_signal_exits', 0) > 0:
            exit_rows.append(["Opposite Signal", f"{backtest_metrics.get('opposite_signal_exits', 0)} ({backtest_metrics.get('opposite_signal_exits', 0)/total*100:.1f}%)"])
        
        exit_rows.append(["End of Data", str(backtest_metrics.get('end_of_data_exits', 0))])
        
        for label, value in exit_rows:
            print(f"{label:<25} {value}")
            
        # 6. SIGNAL FLOW
        self.print_header("📡 SIGNAL FLOW")
        flow_headers = ["Stage", "BUY", "SELL", "TOTAL", "Rejected"]
        flow_rows = [
            ["Raw Signals", str(basic_metrics['raw_buy']), str(basic_metrics['raw_sell']), str(basic_metrics['raw_total']), "-"],
            ["Time Filter", str(basic_metrics['time_buy']), str(basic_metrics['time_sell']), str(basic_metrics['time_total']), str(basic_metrics['raw_total'] - basic_metrics['time_total'])],
            ["RSI Filter", str(basic_metrics['rsi_buy']), str(basic_metrics['rsi_sell']), str(basic_metrics['rsi_total']), str(basic_metrics['time_total'] - basic_metrics['rsi_total'])],
            ["Risk Managed", str(basic_metrics['final_buy']), str(basic_metrics['final_sell']), str(basic_metrics['final_total']), str(basic_metrics['rsi_total'] - basic_metrics['final_total'])],
        ]
        
        # Add position control row if relevant
        if 'trade_manager' in backtest_metrics:
            tm = backtest_metrics['trade_manager']
            executed = backtest_metrics.get('total_trades', 0)
            rejected_by_position = tm.get('signals_rejected', 0)
            flow_rows.append([
                "Position Control", 
                "-", 
                "-", 
                str(executed), 
                str(rejected_by_position)
            ])
        
        self.print_table(flow_headers, flow_rows, [15, 8, 8, 8, 10])

    def display(self, report_path: str, config_path: str = None):
        try:
            report_data, config, trades_df, ohlcv_df = self.load_all_data(report_path, config_path)
            
            print(f"\n{self.color_text('📊 ENHANCED WBWS STRATEGY DASHBOARD', self.colors.BOLD + self.colors.CYAN)}")
            print("=" * 80)
            print(f"Report: {Path(report_path).name}")
            
            basic_metrics = self.extract_basic_metrics(report_data)
            
            backtest_metrics = {}
            if trades_df is not None and ohlcv_df is not None:
                backtest_metrics = self.simulate_backtest(trades_df, ohlcv_df, config)
            
            if backtest_metrics:
                self.display_tradingview_style_dashboard(basic_metrics, backtest_metrics, config)
            else:
                print(f"\n{self.colors.YELLOW}⚠️  Partial Dashboard (No simulation data available){self.colors.END}")
                # Simple fallback display if needed or just exit
                
            print(f"\n{self.color_text('✅ Dashboard completed successfully!', self.colors.GREEN)}")
            
        except Exception as e:
            print(f"{self.colors.RED}❌ Error: {e}{self.colors.END}")
            # import traceback; traceback.print_exc() # Uncomment for debug
            sys.exit(1)

def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/dashboard_standalone.py <report_json> [config_yaml]")
        sys.exit(1)
    
    report_path = sys.argv[1]
    config_path = sys.argv[2] if len(sys.argv) > 2 else None
    
    dashboard = EnhancedDashboard()
    dashboard.display(report_path, config_path)

if __name__ == "__main__":
    main()
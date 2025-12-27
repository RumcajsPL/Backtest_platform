#!/usr/bin/env python
"""
Enhanced Standalone Performance Dashboard with Backtesting
"""
import sys
import json
import yaml
import pandas as pd
from pathlib import Path
from typing import Dict, List


class Colors:
    """ANSI color codes for terminal output"""
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    GRAY = '\033[90m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


class EnhancedDashboard:
    """Dashboard with real backtesting simulation"""
    
    def __init__(self, mode: str = "FULL"):
        self.mode = mode.upper()
        self.colors = Colors
        
    def load_all_data(self, report_path: str, config_path: str = None) -> tuple:
        """Load report, config, trades, and OHLCV data"""
        report_path = Path(report_path)
        if not report_path.exists():
            raise FileNotFoundError(f"Report file not found: {report_path}")
        
        with open(report_path, 'r', encoding='utf-8') as f:
            report_data = json.load(f)
        
        # Load config
        config = {}
        if config_path:
            config_path = Path(config_path)
            if config_path.exists():
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
        
        if not config and 'config' in report_data:
            config = report_data['config']
        
        # Try to find trade details CSV
        report_dir = report_path.parent
        trade_files = list(report_dir.glob("trade_details_*.csv"))
        if not trade_files:
            # Try signals directory
            signals_dir = report_dir.parent.parent / "signals" / "strategy"
            if signals_dir.exists():
                trade_files = list(signals_dir.glob("trade_details_*.csv"))
        
        trades_df = None
        if trade_files:
            # Use the most recent trade file
            latest_trade_file = max(trade_files, key=lambda x: x.stat().st_mtime)
            trades_df = pd.read_csv(latest_trade_file, parse_dates=['timestamp'])
            print(f"📁 Loaded trade details: {latest_trade_file.name}")
        
        # Try to load OHLCV data from config
        ohlcv_df = None
        if config and 'data' in config and 'file' in config['data']:
            data_file = config['data']['file']
            # Try different base paths
            possible_paths = [
                Path(data_file),
                report_dir.parent.parent.parent / data_file,  # project_root/data/...
                report_dir.parent.parent / data_file,
            ]
            
            for path in possible_paths:
                if path.exists():
                    ohlcv_df = pd.read_csv(path, parse_dates=['timestamp'])
                    ohlcv_df.set_index('timestamp', inplace=True)
                    print(f"📁 Loaded OHLCV data: {path.name}")
                    break
        
        return report_data, config, trades_df, ohlcv_df
    
    def simulate_backtest(self, trades_df: pd.DataFrame, ohlcv_df: pd.DataFrame) -> Dict:
        """Simulate backtest and calculate real metrics"""
        if trades_df is None or ohlcv_df is None:
            print("⚠️  Cannot simulate backtest: Missing trades or OHLCV data")
            return {}
        
        # Import the simulator
        try:
            from backtest_simulator import TradeSimulator
            simulator = TradeSimulator()
            
            print("🔄 Simulating backtest...")
            completed_trades = simulator.simulate_trades(trades_df, ohlcv_df)
            
            if completed_trades:
                metrics = simulator.calculate_metrics(completed_trades)
                print(f"✅ Backtest simulated: {len(completed_trades)} trades")
                return metrics
            else:
                print("⚠️  No completed trades from simulation")
                return {}
                
        except ImportError:
            print("⚠️  Backtest simulator not available")
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
        
        # Performance metrics from report
        perf_metrics = report_data.get('performance_metrics', {})
        metrics.update({
            'total_trades': perf_metrics.get('total_trades', 0),
            'buy_trades': perf_metrics.get('buy_trades', 0),
            'sell_trades': perf_metrics.get('sell_trades', 0),
            'avg_sl_distance': perf_metrics.get('avg_sl_distance', 0),
            'avg_tp_distance': perf_metrics.get('avg_tp_distance', 0),
            'risk_reward_ratio': perf_metrics.get('risk_reward_ratio', 0),
        })
        
        return metrics
    
    def color_text(self, text: str, color: str) -> str:
        return f"{color}{text}{self.colors.END}"
    
    def print_header(self, title: str):
        print(f"\n{self.color_text(title, self.colors.BOLD + self.colors.CYAN)}")
        print("=" * 80)
    
    def print_table(self, headers: List[str], rows: List[List], col_widths: List[int] = None):
        """Print a formatted table"""
        if col_widths is None:
            col_widths = [25] + [15] * (len(headers) - 1)
        
        # Print headers
        for i, header in enumerate(headers):
            print(f"{self.color_text(header, self.colors.BOLD):<{col_widths[i]}}", end="")
        print()
        print("-" * sum(col_widths))
        
        # Print rows
        for row in rows:
            for i, cell in enumerate(row):
                if i < len(col_widths):
                    print(f"{str(cell):<{col_widths[i]}}", end="")
            print()
    
    def display_tradingview_style_dashboard(self, basic_metrics: Dict, backtest_metrics: Dict):
        """Display TradingView-style performance dashboard"""
        
        # 1. PERFORMANCE OVERVIEW (like TradingView)
        self.print_header("📈 TRADINGVIEW-STYLE PERFORMANCE DASHBOARD")
        
        # Total Performance
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
        
        # 2. LONG/SHORT BREAKDOWN (TradingView style)
        self.print_header("🔀 LONG/SHORT PERFORMANCE")
        
        breakdown_headers = ["Metric", "LONG", "SHORT", "TOTAL"]
        breakdown_rows = [
            ["Trades", 
             str(backtest_metrics.get('long_trades', 0)), 
             str(backtest_metrics.get('short_trades', 0)), 
             str(backtest_metrics.get('total_trades', 0))],
            ["Win Rate", 
             f"{backtest_metrics.get('long_win_rate', 0):.1f}%", 
             f"{backtest_metrics.get('short_win_rate', 0):.1f}%", 
             f"{backtest_metrics.get('win_rate', 0):.1f}%"],
            ["P&L (pts)", 
             f"{backtest_metrics.get('long_pnl_points', 0):+.2f}", 
             f"{backtest_metrics.get('short_pnl_points', 0):+.2f}", 
             f"{backtest_metrics.get('total_pnl_points', 0):+.2f}"],
            ["Avg Win", 
             f"{backtest_metrics.get('avg_win_points', 0):+.2f}", 
             f"{backtest_metrics.get('avg_win_points', 0):+.2f}", 
             f"{backtest_metrics.get('avg_win_points', 0):+.2f}"],
            ["Avg Loss", 
             f"{backtest_metrics.get('avg_loss_points', 0):+.2f}", 
             f"{backtest_metrics.get('avg_loss_points', 0):+.2f}", 
             f"{backtest_metrics.get('avg_loss_points', 0):+.2f}"],
        ]
        
        self.print_table(breakdown_headers, breakdown_rows)
        
        # 3. TRADE STATISTICS (TradingView style)
        self.print_header("⏱️ TRADE STATISTICS")
        
        stats_rows = [
            ["Avg Duration (min)", f"{backtest_metrics.get('avg_duration_minutes', 0):.1f}"],
            ["Avg Bars/Trade", f"{backtest_metrics.get('avg_duration_bars', 0):.1f}"],
            ["Avg Win Duration", f"{backtest_metrics.get('avg_win_duration', 0):.1f} min"],
            ["Avg Loss Duration", f"{backtest_metrics.get('avg_loss_duration', 0):.1f} min"],
            ["Max Consecutive Wins", str(backtest_metrics.get('max_consecutive_wins', 0))],
            ["Max Consecutive Losses", str(backtest_metrics.get('max_consecutive_losses', 0))],
        ]
        
        for label, value in stats_rows:
            print(f"{label:<25} {value}")
        
        # 4. EXIT STATISTICS
        self.print_header("🚪 EXIT STATISTICS")
        
        exit_rows = [
            ["Take Profit Exits", str(backtest_metrics.get('take_profit_exits', 0))],
            ["Stop Loss Exits", str(backtest_metrics.get('stop_loss_exits', 0))],
            ["End of Data", str(backtest_metrics.get('end_of_data_exits', 0))],
            ["TP Exit Rate", f"{backtest_metrics.get('take_profit_exits', 0)/backtest_metrics.get('total_trades', 1)*100:.1f}%" if backtest_metrics.get('total_trades', 0) > 0 else "0.0%"],
            ["SL Exit Rate", f"{backtest_metrics.get('stop_loss_exits', 0)/backtest_metrics.get('total_trades', 1)*100:.1f}%" if backtest_metrics.get('total_trades', 0) > 0 else "0.0%"],
        ]
        
        for label, value in exit_rows:
            print(f"{label:<25} {value}")
        
        # 5. RISK METRICS
        self.print_header("🛡️ RISK METRICS")
        
        risk_rows = [
            ["Avg Risk/Reward", f"1:{backtest_metrics.get('avg_risk_reward', 0):.2f}"],
            ["Largest Win", f"{backtest_metrics.get('largest_win', 0):.2f} pts"],
            ["Largest Loss", f"{backtest_metrics.get('largest_loss', 0):.2f} pts"],
            ["Expectancy", f"{backtest_metrics.get('expectancy_points', 0):.2f} pts"],
            ["Sharpe Ratio", f"{backtest_metrics.get('sharpe_ratio', 0):.2f}"],
        ]
        
        for label, value in risk_rows:
            print(f"{label:<25} {value}")
        
        # 6. SIGNAL FLOW (from original report)
        self.print_header("📡 SIGNAL FLOW")
        
        flow_headers = ["Stage", "BUY", "SELL", "TOTAL", "Rejected", "% Rej"]
        flow_rows = [
            ["Raw Signals", 
             str(basic_metrics['raw_buy']), 
             str(basic_metrics['raw_sell']), 
             str(basic_metrics['raw_total']), 
             "0", "0.0%"],
            ["Time Filter", 
             str(basic_metrics['time_buy']), 
             str(basic_metrics['time_sell']), 
             str(basic_metrics['time_total']),
             str(basic_metrics['raw_total'] - basic_metrics['time_total']),
             f"{(basic_metrics['raw_total'] - basic_metrics['time_total'])/basic_metrics['raw_total']*100:.1f}%" if basic_metrics['raw_total'] > 0 else "0.0%"],
            ["RSI Filter", 
             str(basic_metrics['rsi_buy']), 
             str(basic_metrics['rsi_sell']), 
             str(basic_metrics['rsi_total']),
             str(basic_metrics['time_total'] - basic_metrics['rsi_total']),
             f"{(basic_metrics['time_total'] - basic_metrics['rsi_total'])/basic_metrics['time_total']*100:.1f}%" if basic_metrics['time_total'] > 0 else "0.0%"],
            ["Risk Management", 
             str(basic_metrics['final_buy']), 
             str(basic_metrics['final_sell']), 
             str(basic_metrics['final_total']),
             str(basic_metrics['rsi_total'] - basic_metrics['final_total']),
             f"{(basic_metrics['rsi_total'] - basic_metrics['final_total'])/basic_metrics['rsi_total']*100:.1f}%" if basic_metrics['rsi_total'] > 0 else "0.0%"],
        ]
        
        self.print_table(flow_headers, flow_rows, [20, 8, 8, 8, 10, 8])
    
    def display(self, report_path: str, config_path: str = None):
        """Main display method with backtesting"""
        try:
            # Load all data
            report_data, config, trades_df, ohlcv_df = self.load_all_data(report_path, config_path)
            
            # Print header
            print(f"\n{self.color_text('📊 ENHANCED WBWS STRATEGY DASHBOARD', self.colors.BOLD + self.colors.CYAN)}")
            print("=" * 80)
            print(f"Report: {Path(report_path).name}")
            print(f"Date: {report_data.get('execution_time', 'N/A')}")
            print(f"Mode: {self.mode}")
            print(f"Backtest Simulation: {'YES' if trades_df is not None and ohlcv_df is not None else 'NO'}")
            print("=" * 80)
            
            # Extract basic metrics
            basic_metrics = self.extract_basic_metrics(report_data)
            
            # Simulate backtest if we have data
            backtest_metrics = {}
            if trades_df is not None and ohlcv_df is not None:
                backtest_metrics = self.simulate_backtest(trades_df, ohlcv_df)
            
            # Display dashboard
            if backtest_metrics:
                self.display_tradingview_style_dashboard(basic_metrics, backtest_metrics)
            else:
                # Fall back to basic display
                self.display_basic_dashboard(basic_metrics)
            
            print(f"\n{self.color_text('✅ Dashboard completed successfully!', self.colors.GREEN)}")
            
        except Exception as e:
            print(f"{self.colors.RED}❌ Error: {e}{self.colors.END}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    def display_basic_dashboard(self, metrics: Dict):
        """Fallback basic dashboard (config parameter removed as unused)"""
        self.print_header("📊 BASIC PERFORMANCE SUMMARY")
        
        print(f"{self.color_text('Signal Flow:', self.colors.BOLD)}")
        print(f"  Raw Signals: {metrics['raw_total']}")
        print(f"  Final Trades: {metrics['final_total']}")
        print(f"  Rejection Rate: {(metrics['raw_total'] - metrics['final_total'])/metrics['raw_total']*100:.1f}%" if metrics['raw_total'] > 0 else "0.0%")
        
        if metrics['total_trades'] > 0:
            print(f"\n{self.color_text('Trade Statistics:', self.colors.BOLD)}")
            print(f"  Total Trades: {metrics['total_trades']}")
            print(f"  Long Trades: {metrics['buy_trades']}")
            print(f"  Short Trades: {metrics['sell_trades']}")
            print(f"  Avg SL: {metrics['avg_sl_distance']:.2f} pts")
            print(f"  Avg TP: {metrics['avg_tp_distance']:.2f} pts")
            print(f"  Risk/Reward: 1:{metrics['risk_reward_ratio']:.1f}")


def main():
    """Command line interface"""
    if len(sys.argv) < 2:
        print(f"{Colors.BOLD}📊 ENHANCED WBWS STRATEGY DASHBOARD{Colors.END}")
        print("=" * 60)
        print(f"{Colors.BOLD}Usage:{Colors.END}")
        print(f"  python scripts/dashboard_standalone.py <report_json> [config_yaml]")
        print(f"\n{Colors.BOLD}Arguments:{Colors.END}")
        print(f"  report_json : Path to strategy report JSON")
        print(f"  config_yaml : Optional config file (for OHLCV data)")
        print(f"\n{Colors.BOLD}Examples:{Colors.END}")
        print(f"  {Colors.CYAN}python scripts/dashboard_standalone.py outputs/reports/WBWS/strategy_report.json{Colors.END}")
        print(f"  {Colors.CYAN}python scripts/dashboard_standalone.py outputs/reports/WBWS/strategy_report.json \\")
        print(f"    src/config/WBWS/wbws_rsi_strategy.yaml{Colors.END}")
        sys.exit(1)
    
    report_path = sys.argv[1]
    config_path = sys.argv[2] if len(sys.argv) > 2 else None
    
    # Create and display dashboard
    dashboard = EnhancedDashboard(mode="FULL")
    dashboard.display(report_path, config_path)


if __name__ == "__main__":
    main()
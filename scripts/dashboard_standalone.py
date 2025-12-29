"""
Enhanced Standalone Performance Dashboard
Simplified version that uses pre-calculated data from run_wbws_strategy.py
"""
import sys
import json
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    MAGENTA = '\033[95m'
    BOLD = '\033[1m'
    END = '\033[0m'

class EnhancedDashboard:
    def __init__(self, mode: str = "FULL"):
        self.mode = mode.upper()
        self.colors = Colors
        
    def load_data(self, report_path: str) -> tuple:
        """
        Load all data from report and associated files
        """
        report_path = Path(report_path).resolve()
        if not report_path.exists():
            raise FileNotFoundError(f"Report file not found: {report_path}")
        
        print(f"\n{self.color_text('📊 LOADING DATA', self.colors.BOLD + self.colors.CYAN)}")
        print("="*80)
        
        # 1. Load JSON Report
        print(f"📁 Loading report: {report_path.name}")
        with open(report_path, 'r', encoding='utf-8') as f:
            report_data = json.load(f)
        
        # 2. Load Trades CSV
        trades_df = None
        csv_path = None
        
        if 'outputs' in report_data and report_data['outputs'].get('trades_csv_file'):
            csv_rel_path = report_data['outputs']['trades_csv_file']
            
            # Try multiple possible locations
            candidates = [
                report_path.parent / Path(csv_rel_path).name,
                Path(csv_rel_path),
                report_path.parent.parent.parent / csv_rel_path,
            ]
            
            for candidate in candidates:
                if candidate.exists():
                    csv_path = candidate
                    break
            
            if csv_path and csv_path.exists():
                try:
                    trades_df = pd.read_csv(csv_path)
                    
                    # Convert datetime strings back to datetime objects
                    for col in ['entry_time', 'exit_time']:
                        if col in trades_df.columns:
                            trades_df[col] = pd.to_datetime(trades_df[col])
                    
                    print(f"📁 Loaded trades: {len(trades_df)} records from {csv_path.name}")
                except Exception as e:
                    print(f"{self.colors.RED}❌ Error loading CSV: {e}{self.colors.END}")
            else:
                print(f"{self.colors.YELLOW}⚠️  CSV file not found: {csv_rel_path}{self.colors.END}")
        else:
            print(f"{self.colors.YELLOW}⚠️  No CSV file path in report{self.colors.END}")
        
        return report_data, trades_df, csv_path
    
    def color_text(self, text: str, color: str) -> str:
        return f"{color}{text}{self.colors.END}"
    
    def print_header(self, title: str):
        print(f"\n{self.color_text(title, self.colors.BOLD + self.colors.CYAN)}")
        print("="*80)
    
    def print_section(self, title: str):
        print(f"\n{self.color_text(title, self.colors.BOLD + self.colors.BLUE)}")
        print("-"*60)
    
    def print_metric(self, label: str, value: str, color: str = ""):
        if color:
            print(f"{label:<30} {color}{value}{self.colors.END}")
        else:
            print(f"{label:<30} {value}")
    
    def display_overview(self, report_data: Dict):
        """Display basic overview from report"""
        self.print_header("📈 STRATEGY PERFORMANCE OVERVIEW")
        
        config = report_data.get('config', {})
        sim_results = report_data.get('simulation_results', {})
        perf_metrics = sim_results.get('performance_metrics', {})
        
        # Basic info
        print(f"{'Strategy:':<25} {config.get('indicator', 'N/A')}")
        print(f"{'Timeframe:':<25} {config.get('htf_period', 'N/A')}")
        
        if 'data_period' in config:
            period = config['data_period']
            print(f"{'Backtest Period:':<25} {period.get('start', 'N/A')} to {period.get('end', 'N/A')}")
        
        print(f"{'Execution Time:':<25} {report_data.get('execution_time', 'N/A')}")
        
        self.print_section("📊 TRADE STATISTICS")
        
        if perf_metrics:
            stats = [
                ("Total Trades", f"{perf_metrics.get('total_trades', 0)}"),
                ("Winning Trades", f"{perf_metrics.get('winning_trades', 0)} ({perf_metrics.get('win_rate', 0):.1f}%)"),
                ("Losing Trades", f"{perf_metrics.get('losing_trades', 0)} ({perf_metrics.get('loss_rate', 0):.1f}%)"),
                ("Breakeven Trades", f"{perf_metrics.get('breakeven_trades', 0)}"),
                ("Total P&L (Points)", f"{perf_metrics.get('total_pnl_points', 0):+.2f}"),
                ("Avg P&L/Trade", f"{perf_metrics.get('avg_pnl_points', 0):+.2f}"),
                ("Profit Factor", f"{perf_metrics.get('profit_factor', 0):.2f}"),
                ("Expectancy", f"{perf_metrics.get('expectancy_points', 0):+.2f} pts"),
                ("Sharpe Ratio", f"{perf_metrics.get('sharpe_ratio', 0):.2f}"),
            ]
            
            for label, value in stats:
                print(f"{label:<25} {value}")
        else:
            print("No performance metrics available")
    
    def display_signal_flow(self, report_data: Dict):
        """Display signal flow statistics"""
        self.print_header("📡 SIGNAL FLOW ANALYSIS")
        
        signal_flow = report_data.get('signal_flow', {})
        
        stages = [
            ("Raw Signals", "step1_raw_signals"),
            ("Time Filtered", "step2_time_filtered"),
            ("RSI Filtered", "step3_rsi_filtered"),
            ("Risk Managed", "step4_risk_managed"),
            ("Position Managed", "step5_position_managed"),
        ]
        
        print(f"{'Stage':<20} {'BUY':>8} {'SELL':>8} {'TOTAL':>8} {'REJECTED':>10} {'% REJ':>8}")
        print("-"*62)
        
        for stage_name, stage_key in stages:
            if stage_key in signal_flow:
                stage = signal_flow[stage_key]
                buy = stage.get('buy', stage.get('buy_opens', 0))
                sell = stage.get('sell', stage.get('sell_opens', 0))
                total = stage.get('total', stage.get('total_opens', 0))
                rejected = stage.get('rejected_total', 0)
                
                # Calculate rejection rate
                prev_stage_key = None
                for prev_name, prev_key in stages:
                    if prev_key == stage_key:
                        break
                    prev_stage_key = prev_key
                
                if prev_stage_key and prev_stage_key in signal_flow:
                    prev_total = signal_flow[prev_stage_key].get('total', 
                               signal_flow[prev_stage_key].get('total_opens', total + rejected))
                    rej_rate = (rejected / prev_total * 100) if prev_total > 0 else 0
                else:
                    rej_rate = 0
                
                print(f"{stage_name:<20} {buy:>8} {sell:>8} {total:>8} {rejected:>10} {rej_rate:>7.1f}%")
        
        # Overall rejection
        overall = report_data.get('overall_rejection', {})
        total_rejected = overall.get('total_rejected', 0)
        rej_rate = overall.get('total_rejection_rate_pct', 0)
        
        print("-"*62)
        print(f"{self.color_text('Overall Rejection:', self.colors.BOLD):<20} {'':>24} {total_rejected:>10} {rej_rate:>7.1f}%")
    
    def display_advanced_metrics(self, trades_df: pd.DataFrame, report_data: Dict):
        """Display advanced trading metrics"""
        if trades_df is None or trades_df.empty:
            return
        
        closed_trades = trades_df[trades_df['status'] == 'CLOSED'].copy()
        if closed_trades.empty:
            return
        
        self.print_header("🎯 ADVANCED PERFORMANCE METRICS")
        
        # Get risk:reward from config or calculate
        rr_ratio = 3.0  # Default from your config
        if 'risk_details' in report_data:
            rr_ratio = report_data['risk_details'].get('risk_to_reward', 3.0)
        
        # 1. Breakeven win rate
        breakeven_win_rate = 1 / (1 + rr_ratio) * 100
        
        # 2. Actual win rate
        actual_win_rate = len(closed_trades[closed_trades['pnl_points'] > 0]) / len(closed_trades) * 100
        
        # 3. Expectancy in R units
        avg_sl = closed_trades['sl_distance'].mean()
        expectancy_r = closed_trades['pnl_points'].mean() / avg_sl if avg_sl > 0 else 0
        
        # 4. Trade frequency
        if len(closed_trades) > 1:
            first_trade = closed_trades['exit_time'].min()
            last_trade = closed_trades['exit_time'].max()
            days = (last_trade - first_trade).days + 1
            trades_per_day = len(closed_trades) / days
            profit_per_day = closed_trades['pnl_points'].sum() / days
        else:
            trades_per_day = 0
            profit_per_day = 0
        
        # 5. Consecutive wins/losses
        closed_trades_sorted = closed_trades.sort_values('exit_time').copy()
        closed_trades_sorted['is_win'] = closed_trades_sorted['pnl_points'] > 0
        
        max_consecutive_wins = 0
        max_consecutive_losses = 0
        current_wins = 0
        current_losses = 0
        
        for is_win in closed_trades_sorted['is_win']:
            if is_win:
                current_wins += 1
                current_losses = 0
                max_consecutive_wins = max(max_consecutive_wins, current_wins)
            else:
                current_losses += 1
                current_wins = 0
                max_consecutive_losses = max(max_consecutive_losses, current_losses)
        
        # 6. Kelly Criterion (simplified)
        win_prob = actual_win_rate / 100
        kelly = win_prob - ((1 - win_prob) / rr_ratio)
        kelly_color = self.colors.GREEN if kelly > 0 else self.colors.YELLOW
        
        # 7. Average win/loss in R units
        winning_trades = closed_trades[closed_trades['pnl_points'] > 0]
        losing_trades = closed_trades[closed_trades['pnl_points'] < 0]
        
        avg_win_r = winning_trades['pnl_points'].mean() / avg_sl if not winning_trades.empty and avg_sl > 0 else 0
        avg_loss_r = abs(losing_trades['pnl_points'].mean()) / avg_sl if not losing_trades.empty and avg_sl > 0 else 0
        
        # 8. System Quality Number (SQN)
        if len(closed_trades) >= 30:  # SQN needs at least 30 trades
            expectancy = closed_trades['pnl_points'].mean()
            std_dev = closed_trades['pnl_points'].std()
            sqn = np.sqrt(len(closed_trades)) * (expectancy / std_dev) if std_dev > 0 else 0
        else:
            sqn = "N/A (<30 trades)"
        
        # 9. Risk of Ruin (simplified)
        risk_of_ruin = ((1 - win_prob) / win_prob) ** (closed_trades['pnl_points'].sum() / avg_sl) if win_prob > 0.5 else "High"
        
        # 10. Payoff Ratio (Average Win / Average Loss)
        payoff_ratio = abs(winning_trades['pnl_points'].mean() / losing_trades['pnl_points'].mean()) if not losing_trades.empty and losing_trades['pnl_points'].mean() != 0 else float('inf')
        
        metrics = [
            ("Breakeven Win Rate", f"{breakeven_win_rate:.1f}%"),
            ("Actual Win Rate", f"{actual_win_rate:.1f}%", 
             self.colors.GREEN if actual_win_rate > breakeven_win_rate else self.colors.RED),
            ("Win Rate Premium", f"{actual_win_rate - breakeven_win_rate:+.1f}%",
             self.colors.GREEN if (actual_win_rate - breakeven_win_rate) > 0 else self.colors.RED),
            ("Risk:Reward Target", f"1:{rr_ratio:.1f}"),
            ("Actual Avg R:R", f"1:{closed_trades['risk_reward_ratio'].mean():.2f}"),
            ("Expectancy (R units)", f"{expectancy_r:+.3f}R",
             self.colors.GREEN if expectancy_r > 0 else self.colors.RED),
            ("Expectancy (Points)", f"{closed_trades['pnl_points'].mean():+.2f} pts"),
            ("Avg Win (R units)", f"{avg_win_r:+.2f}R"),
            ("Avg Loss (R units)", f"{avg_loss_r:+.2f}R"),
            ("Payoff Ratio", f"{payoff_ratio:.2f}:1"),
            ("Trades per Day", f"{trades_per_day:.1f}"),
            ("Profit per Day", f"{profit_per_day:+.2f} pts",
             self.colors.GREEN if profit_per_day > 0 else self.colors.RED),
            ("Avg Risk per Trade", f"{avg_sl:.2f} pts"),
            ("Max Consecutive Wins", f"{max_consecutive_wins}"),
            ("Max Consecutive Losses", f"{max_consecutive_losses}"),
            ("Kelly Criterion %", f"{kelly*100:.1f}%" if isinstance(kelly, (int, float)) else str(kelly),
             kelly_color),
            ("System Quality Number", f"{sqn:.2f}" if isinstance(sqn, (int, float)) else str(sqn)),
            ("Risk of Ruin", f"{risk_of_ruin:.4%}" if isinstance(risk_of_ruin, float) else str(risk_of_ruin)),
        ]
        
        for metric in metrics:
            if len(metric) == 2:
                self.print_metric(metric[0], metric[1])
            elif len(metric) == 3:
                self.print_metric(metric[0], metric[1], metric[2])
        
        # Performance by hour of day
        self.print_section("🕒 PERFORMANCE BY HOUR OF DAY")
        
        if 'entry_time' in closed_trades.columns:
            closed_trades['hour'] = closed_trades['entry_time'].dt.hour
            hourly_perf = closed_trades.groupby('hour').agg({
                'pnl_points': ['sum', 'mean', 'count'],
                'is_win': lambda x: (x == True).sum() / len(x) * 100 if len(x) > 0 else 0
            }).round(2)
            
            hourly_perf.columns = ['total_pnl', 'avg_pnl', 'trades', 'win_rate']
            
            if not hourly_perf.empty:
                print(f"{'Hour':<6} {'Trades':>8} {'Win Rate':>10} {'Total P&L':>12} {'Avg P&L':>12}")
                print("-"*58)
                
                for hour, row in hourly_perf.iterrows():
                    total_color = self.colors.GREEN if row['total_pnl'] > 0 else self.colors.RED if row['total_pnl'] < 0 else ""
                    avg_color = self.colors.GREEN if row['avg_pnl'] > 0 else self.colors.RED if row['avg_pnl'] < 0 else ""
                    
                    print(f"{hour:02d}:00 {row['trades']:>8.0f} {row['win_rate']:>9.1f}% "
                          f"{total_color}{row['total_pnl']:>+11.2f}{self.colors.END} "
                          f"{avg_color}{row['avg_pnl']:>+11.2f}{self.colors.END}")
    
    def display_trade_analysis(self, trades_df: pd.DataFrame):
        """Display detailed trade analysis from CSV data"""
        if trades_df is None or trades_df.empty:
            print(f"\n{self.colors.YELLOW}⚠️  No trade data available for analysis{self.colors.END}")
            return
        
        self.print_header("🔍 DETAILED TRADE ANALYSIS")
        
        # Filter only closed trades for analysis
        closed_trades = trades_df[trades_df['status'] == 'CLOSED'].copy()
        open_trades = trades_df[trades_df['status'] == 'OPEN']
        rejected_trades = trades_df[trades_df['status'] == 'REJECTED']
        
        print(f"Total Records: {len(trades_df)}")
        print(f"  • {self.colors.GREEN}Closed Trades: {len(closed_trades)}{self.colors.END}")
        print(f"  • {self.colors.YELLOW}Open Trades: {len(open_trades)}{self.colors.END}")
        print(f"  • {self.colors.RED}Rejected Signals: {len(rejected_trades)}{self.colors.END}")
        
        if closed_trades.empty:
            print(f"\n{self.colors.YELLOW}No closed trades to analyze{self.colors.END}")
            return
        
        self.print_section("📈 PERFORMANCE BY DIRECTION")
        
        for direction in ['BUY', 'SELL']:
            dir_trades = closed_trades[closed_trades['direction'] == direction]
            if len(dir_trades) > 0:
                winning = dir_trades[dir_trades['pnl_points'] > 0]
                losing = dir_trades[dir_trades['pnl_points'] < 0]
                
                win_rate = len(winning) / len(dir_trades) * 100 if len(dir_trades) > 0 else 0
                total_pnl = dir_trades['pnl_points'].sum()
                avg_pnl = dir_trades['pnl_points'].mean()
                avg_sl = dir_trades['sl_distance'].mean()
                
                color = self.colors.GREEN if total_pnl > 0 else self.colors.RED if total_pnl < 0 else ""
                
                print(f"{direction}: {len(dir_trades)} trades")
                print(f"  Win Rate: {win_rate:.1f}%  |  "
                      f"Total P&L: {color}{total_pnl:+.2f}{self.colors.END} pts  |  "
                      f"Avg P&L: {avg_pnl:+.2f} pts  |  "
                      f"Avg Risk: {avg_sl:.2f} pts")
        
        self.print_section("🚪 EXIT REASON ANALYSIS")
        
        if 'exit_reason' in closed_trades.columns:
            exit_counts = closed_trades['exit_reason'].value_counts()
            for reason, count in exit_counts.items():
                pct = count / len(closed_trades) * 100
                avg_pnl = closed_trades[closed_trades['exit_reason'] == reason]['pnl_points'].mean()
                win_rate = (closed_trades[closed_trades['exit_reason'] == reason]['pnl_points'] > 0).mean() * 100
                
                color = self.colors.GREEN if avg_pnl > 0 else self.colors.RED if avg_pnl < 0 else ""
                win_color = self.colors.GREEN if win_rate > 50 else self.colors.RED
                
                print(f"{reason:<20} {count:>4} ({pct:5.1f}%)  |  "
                      f"Avg P&L: {color}{avg_pnl:+.2f}{self.colors.END} pts  |  "
                      f"Win Rate: {win_color}{win_rate:.1f}%{self.colors.END}")
        
        self.print_section("⏱️  DURATION ANALYSIS")
        
        if 'duration_minutes' in closed_trades.columns:
            stats = [
                ("Avg Duration", f"{closed_trades['duration_minutes'].mean():.1f} min"),
                ("Min Duration", f"{closed_trades['duration_minutes'].min():.1f} min"),
                ("Max Duration", f"{closed_trades['duration_minutes'].max():.1f} min"),
                ("Median Duration", f"{closed_trades['duration_minutes'].median():.1f} min"),
                ("Avg Bars", f"{closed_trades['duration_bars'].mean():.1f} bars"),
            ]
            
            for label, value in stats:
                print(f"{label:<20} {value}")
            
            # Duration by win/loss
            winning = closed_trades[closed_trades['pnl_points'] > 0]
            losing = closed_trades[closed_trades['pnl_points'] < 0]
            
            if len(winning) > 0:
                print(f"{'Avg Win Duration:':<20} {winning['duration_minutes'].mean():.1f} min")
            if len(losing) > 0:
                print(f"{'Avg Loss Duration:':<20} {losing['duration_minutes'].mean():.1f} min")
            
            # Duration by exit reason
            if 'exit_reason' in closed_trades.columns:
                self.print_section("⏱️  DURATION BY EXIT REASON")
                for reason in closed_trades['exit_reason'].unique():
                    reason_trades = closed_trades[closed_trades['exit_reason'] == reason]
                    if len(reason_trades) > 0:
                        avg_dur = reason_trades['duration_minutes'].mean()
                        print(f"{reason:<20} {avg_dur:.1f} min")
        
        self.print_section("🎯 RISK MANAGEMENT")
        
        if all(col in closed_trades.columns for col in ['sl_distance', 'tp_distance', 'risk_reward_ratio']):
            stats = [
                ("Avg SL Distance", f"{closed_trades['sl_distance'].mean():.2f} pts"),
                ("Avg TP Distance", f"{closed_trades['tp_distance'].mean():.2f} pts"),
                ("Median SL Distance", f"{closed_trades['sl_distance'].median():.2f} pts"),
                ("Median TP Distance", f"{closed_trades['tp_distance'].median():.2f} pts"),
                ("Avg Risk:Reward", f"1:{closed_trades['risk_reward_ratio'].mean():.2f}"),
                ("Min R:R", f"1:{closed_trades['risk_reward_ratio'].min():.2f}"),
                ("Max R:R", f"1:{closed_trades['risk_reward_ratio'].max():.2f}"),
                ("Std Dev R:R", f"±{closed_trades['risk_reward_ratio'].std():.3f}"),
            ]
            
            for label, value in stats:
                print(f"{label:<20} {value}")
        
        # Display sample trades
        self.print_section("📋 SAMPLE TRADES (Last 5 Closed)")
        
        sample_trades = closed_trades.tail(5)
        for _, trade in sample_trades.iterrows():
            color = self.colors.GREEN if trade['pnl_points'] > 0 else self.colors.RED
            arrow = "🟢" if trade['direction'] == 'BUY' else "🔴"
            
            print(f"{arrow} {trade['entry_time'].strftime('%Y-%m-%d %H:%M')} "
                  f"{trade['direction']} @ {trade['entry_price']:.2f}")
            print(f"   Exit: {trade['exit_time'].strftime('%Y-%m-%d %H:%M')} "
                  f"@ {trade['exit_price']:.2f} ({trade['exit_reason']})")
            print(f"   P&L: {color}{trade['pnl_points']:+.2f}{self.colors.END} pts "
                  f"({trade['duration_minutes']:.0f} min) | "
                  f"Risk: {trade['sl_distance']:.2f} pts")
            print()
    
    def display_drawdown_analysis(self, trades_df: pd.DataFrame):
        """Display drawdown analysis"""
        if trades_df is None or trades_df.empty:
            return
        
        closed_trades = trades_df[trades_df['status'] == 'CLOSED'].copy()
        if closed_trades.empty:
            return
        
        self.print_header("📉 DRAWDOWN & RISK ANALYSIS")
        
        # Calculate equity curve
        closed_trades = closed_trades.sort_values('exit_time')
        closed_trades['cumulative_pnl'] = closed_trades['pnl_points'].cumsum()
        closed_trades['running_max'] = closed_trades['cumulative_pnl'].cummax()
        closed_trades['drawdown'] = closed_trades['cumulative_pnl'] - closed_trades['running_max']
        closed_trades['drawdown_pct'] = (closed_trades['drawdown'] / closed_trades['running_max'].clip(lower=1)) * 100
        
        # Find max drawdown
        max_dd_idx = closed_trades['drawdown'].idxmin()
        max_dd = closed_trades.loc[max_dd_idx, 'drawdown']
        max_dd_pct = closed_trades.loc[max_dd_idx, 'drawdown_pct']
        max_dd_time = closed_trades.loc[max_dd_idx, 'exit_time']
        
        # Calculate recovery factor
        total_profit = closed_trades['pnl_points'].sum()
        recovery_factor = abs(total_profit / max_dd) if max_dd < 0 else float('inf')
        
        # Calculate average drawdown
        avg_drawdown = closed_trades[closed_trades['drawdown'] < 0]['drawdown'].mean()
        
        # Calculate Calmar Ratio (annualized return / max drawdown)
        if len(closed_trades) > 1:
            first_trade = closed_trades['exit_time'].min()
            last_trade = closed_trades['exit_time'].max()
            days = (last_trade - first_trade).days + 1
            annual_return = (total_profit / days) * 365
            calmar_ratio = abs(annual_return / max_dd) if max_dd < 0 else float('inf')
        else:
            calmar_ratio = 0
        
        stats = [
            ("Max Drawdown", f"{max_dd:+.2f} pts ({max_dd_pct:.1f}%)"),
            ("Max DD Time", max_dd_time.strftime('%Y-%m-%d %H:%M')),
            ("Avg Drawdown", f"{avg_drawdown:+.2f} pts"),
            ("Recovery Factor", f"{recovery_factor:.2f}"),
            ("Calmar Ratio", f"{calmar_ratio:.2f}" if isinstance(calmar_ratio, (int, float)) else str(calmar_ratio)),
            ("Total Profit", f"{total_profit:+.2f} pts"),
            ("Profit to Max DD", f"{abs(total_profit / max_dd):.2f}:1" if max_dd < 0 else "N/A"),
        ]
        
        for label, value in stats:
            print(f"{label:<25} {value}")
        
        # Calculate drawdown duration
        in_drawdown = False
        dd_start = None
        max_dd_duration = 0
        dd_durations = []
        
        for _, trade in closed_trades.iterrows():
            if trade['drawdown'] < 0:
                if not in_drawdown:
                    in_drawdown = True
                    dd_start = trade['exit_time']
            else:
                if in_drawdown and dd_start:
                    dd_duration = (trade['exit_time'] - dd_start).total_seconds() / 3600  # hours
                    dd_durations.append(dd_duration)
                    max_dd_duration = max(max_dd_duration, dd_duration)
                    in_drawdown = False
        
        if in_drawdown and dd_start:
            last_time = closed_trades['exit_time'].iloc[-1]
            dd_duration = (last_time - dd_start).total_seconds() / 3600
            dd_durations.append(dd_duration)
            max_dd_duration = max(max_dd_duration, dd_duration)
        
        if dd_durations:
            avg_dd_duration = np.mean(dd_durations)
            print(f"{'Max DD Duration:':<25} {max_dd_duration:.1f} hours")
            print(f"{'Avg DD Duration:':<25} {avg_dd_duration:.1f} hours")
            print(f"{'Number of DDs:':<25} {len(dd_durations)}")
        
        # Worst losing streak
        closed_trades['is_loss'] = closed_trades['pnl_points'] < 0
        current_streak = 0
        max_loss_streak = 0
        current_streak_pnl = 0
        max_streak_pnl = 0
        
        for _, trade in closed_trades.iterrows():
            if trade['is_loss']:
                current_streak += 1
                current_streak_pnl += trade['pnl_points']
                if current_streak > max_loss_streak:
                    max_loss_streak = current_streak
                    max_streak_pnl = current_streak_pnl
            else:
                current_streak = 0
                current_streak_pnl = 0
        
        if max_loss_streak > 0:
            print(f"{'Worst Losing Streak:':<25} {max_loss_streak} trades ({max_streak_pnl:.2f} pts)")
    
    def display_position_management(self, report_data: Dict):
        """Display position management statistics"""
        self.print_header("🎯 POSITION MANAGEMENT")
        
        signal_flow = report_data.get('signal_flow', {})
        pos_mgmt = signal_flow.get('step5_position_managed', {})
        config = report_data.get('config', {})
        
        # Display settings
        pos_config = config.get('position_control', {})
        print(f"{'Close on Opposite:':<30} {'✅ ENABLED' if pos_config.get('close_on_opposite') else '❌ DISABLED'}")
        print(f"{'Pyramiding:':<30} {'✅ ENABLED' if pos_config.get('pyramiding_enabled') else '❌ DISABLED'}")
        
        if pos_mgmt:
            self.print_section("SIGNAL PROCESSING")
            
            stats = [
                ("Buy Opens", pos_mgmt.get('buy_opens', 0)),
                ("Sell Opens", pos_mgmt.get('sell_opens', 0)),
                ("Total Opens", pos_mgmt.get('total_opens', 0)),
                ("Rejected Signals", pos_mgmt.get('rejected_total', 0)),
                ("Rejection Rate", f"{pos_mgmt.get('rejected_total', 0) / (pos_mgmt.get('total_opens', 0) + pos_mgmt.get('rejected_total', 0)) * 100:.1f}%"),
            ]
            
            for label, value in stats:
                print(f"{label:<25} {value}")
            
            # Exit statistics
            exit_stats = pos_mgmt.get('exit_statistics', {})
            if exit_stats:
                self.print_section("EXIT STATISTICS")
                total_exits = sum(exit_stats.values())
                for reason, count in exit_stats.items():
                    pct = count / total_exits * 100 if total_exits > 0 else 0
                    print(f"{reason:<25} {count} ({pct:.1f}%)")
            
            # Trade manager metrics
            tm_metrics = pos_mgmt.get('trade_manager_metrics', {})
            if tm_metrics:
                self.print_section("TRADE MANAGER METRICS")
                
                tm_stats = [
                    ("Total Signals", tm_metrics.get('total_signals_received', 0)),
                    ("Signals Accepted", tm_metrics.get('signals_accepted', 0)),
                    ("Signals Rejected", tm_metrics.get('signals_rejected', 0)),
                    ("Acceptance Rate", f"{tm_metrics.get('signals_accepted', 0) / max(tm_metrics.get('total_signals_received', 1), 1) * 100:.1f}%"),
                    ("Positions Reversed", tm_metrics.get('positions_reversed', 0)),
                ]
                
                for label, value in tm_stats:
                    print(f"{label:<25} {value}")
                
                # Rejection reasons
                reasons = tm_metrics.get('rejected_reasons', {})
                if reasons:
                    self.print_section("REJECTION REASONS")
                    total_reasons = sum(reasons.values())
                    for reason, count in reasons.items():
                        pct = count / total_reasons * 100 if total_reasons > 0 else 0
                        print(f"{reason:<25} {count} ({pct:.1f}%)")
    
    def display_monthly_performance(self, trades_df: pd.DataFrame):
        """Display monthly performance breakdown"""
        if trades_df is None or trades_df.empty:
            return
        
        closed_trades = trades_df[trades_df['status'] == 'CLOSED'].copy()
        if closed_trades.empty:
            return
        
        if 'exit_time' not in closed_trades.columns:
            return
        
        self.print_header("📅 TIME-BASED PERFORMANCE")
        
        # Group by month
        closed_trades['month'] = closed_trades['exit_time'].dt.to_period('M')
        monthly = closed_trades.groupby('month').agg({
            'pnl_points': ['sum', 'mean', 'count'],
            'is_win': lambda x: (x == True).sum() / len(x) * 100 if len(x) > 0 else 0
        }).round(2)
        
        monthly.columns = ['total_pnl', 'avg_pnl', 'trades', 'win_rate']
        
        if not monthly.empty:
            print(f"{'Month':<12} {'Trades':>8} {'Win Rate':>10} {'Total P&L':>12} {'Avg P&L':>12}")
            print("-"*58)
            
            for month, row in monthly.iterrows():
                total_color = self.colors.GREEN if row['total_pnl'] > 0 else self.colors.RED if row['total_pnl'] < 0 else ""
                avg_color = self.colors.GREEN if row['avg_pnl'] > 0 else self.colors.RED if row['avg_pnl'] < 0 else ""
                win_color = self.colors.GREEN if row['win_rate'] > 50 else self.colors.RED
                
                print(f"{str(month):<12} {row['trades']:>8.0f} "
                      f"{win_color}{row['win_rate']:>9.1f}%{self.colors.END} "
                      f"{total_color}{row['total_pnl']:>+11.2f}{self.colors.END} "
                      f"{avg_color}{row['avg_pnl']:>+11.2f}{self.colors.END}")
            
            # Summary
            print("-"*58)
            total_trades = monthly['trades'].sum()
            total_pnl = monthly['total_pnl'].sum()
            avg_monthly_pnl = monthly['total_pnl'].mean()
            avg_win_rate = (monthly['win_rate'] * monthly['trades']).sum() / total_trades if total_trades > 0 else 0
            
            total_color = self.colors.GREEN if total_pnl > 0 else self.colors.RED if total_pnl < 0 else ""
            win_color = self.colors.GREEN if avg_win_rate > 50 else self.colors.RED
            
            print(f"{'TOTAL/AVG':<12} {total_trades:>8.0f} "
                  f"{win_color}{avg_win_rate:>9.1f}%{self.colors.END} "
                  f"{total_color}{total_pnl:>+11.2f}{self.colors.END} "
                  f"{avg_monthly_pnl:>+11.2f}")
            
            # Best/worst months
            best_month = monthly['total_pnl'].idxmax()
            best_pnl = monthly['total_pnl'].max()
            worst_month = monthly['total_pnl'].idxmin()
            worst_pnl = monthly['total_pnl'].min()
            
            print(f"\n{'Best Month:':<15} {best_month} ({self.colors.GREEN}{best_pnl:+.2f}{self.colors.END} pts)")
            print(f"{'Worst Month:':<15} {worst_month} ({self.colors.RED}{worst_pnl:+.2f}{self.colors.END} pts)")
            
            # Monthly consistency
            positive_months = len(monthly[monthly['total_pnl'] > 0])
            negative_months = len(monthly[monthly['total_pnl'] < 0])
            monthly_consistency = positive_months / len(monthly) * 100 if len(monthly) > 0 else 0
            
            print(f"\n{'Monthly Consistency:':<20} {monthly_consistency:.1f}% positive months")
            print(f"{'Best/Worst Ratio:':<20} {abs(best_pnl / worst_pnl):.2f}:1")
    
    def create_visualizations(self, trades_df: pd.DataFrame, output_dir: Path):
        """Create basic visualizations (optional)"""
        if trades_df is None or trades_df.empty:
            return
        
        closed_trades = trades_df[trades_df['status'] == 'CLOSED'].copy()
        if closed_trades.empty:
            return
        
        try:
            # Create output directory if it doesn't exist
            vis_dir = output_dir / "visualizations"
            vis_dir.mkdir(exist_ok=True)
            
            # 1. Equity Curve
            plt.figure(figsize=(12, 6))
            closed_trades = closed_trades.sort_values('exit_time')
            closed_trades['cumulative_pnl'] = closed_trades['pnl_points'].cumsum()
            
            plt.plot(closed_trades['exit_time'], closed_trades['cumulative_pnl'], 
                    linewidth=2, color='blue')
            plt.fill_between(closed_trades['exit_time'], 0, closed_trades['cumulative_pnl'], 
                           where=closed_trades['cumulative_pnl'] >= 0, 
                           color='green', alpha=0.3)
            plt.fill_between(closed_trades['exit_time'], 0, closed_trades['cumulative_pnl'], 
                           where=closed_trades['cumulative_pnl'] < 0, 
                           color='red', alpha=0.3)
            
            plt.title('Equity Curve', fontsize=14, fontweight='bold')
            plt.xlabel('Date', fontsize=12)
            plt.ylabel('Cumulative P&L (Points)', fontsize=12)
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(vis_dir / 'equity_curve.png', dpi=100)
            plt.close()
            
            # 2. P&L Distribution
            plt.figure(figsize=(10, 6))
            colors = ['green' if x > 0 else 'red' for x in closed_trades['pnl_points']]
            plt.bar(range(len(closed_trades)), closed_trades['pnl_points'], color=colors, alpha=0.7)
            plt.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
            plt.title('Trade P&L Distribution', fontsize=14, fontweight='bold')
            plt.xlabel('Trade Number', fontsize=12)
            plt.ylabel('P&L (Points)', fontsize=12)
            plt.grid(True, alpha=0.3, axis='y')
            plt.tight_layout()
            plt.savefig(vis_dir / 'pnl_distribution.png', dpi=100)
            plt.close()
            
            # 3. Exit Reasons Pie Chart
            if 'exit_reason' in closed_trades.columns:
                plt.figure(figsize=(8, 8))
                exit_counts = closed_trades['exit_reason'].value_counts()
                colors = plt.cm.Set3(np.linspace(0, 1, len(exit_counts)))
                plt.pie(exit_counts.values, labels=exit_counts.index, autopct='%1.1f%%',
                       colors=colors, startangle=90)
                plt.title('Exit Reasons', fontsize=14, fontweight='bold')
                plt.tight_layout()
                plt.savefig(vis_dir / 'exit_reasons.png', dpi=100)
                plt.close()
            
            print(f"\n{self.colors.GREEN}✅ Visualizations saved to: {vis_dir}{self.colors.END}")
            
        except Exception as e:
            print(f"{self.colors.YELLOW}⚠️  Could not create visualizations: {e}{self.colors.END}")
    
    def display(self, report_path: str, create_visualizations: bool = False):
        """
        Main display function
        """
        try:
            print(f"\n{self.color_text('📊 ENHANCED WBWS STRATEGY DASHBOARD', self.colors.BOLD + self.colors.CYAN)}")
            print("="*80)
            print(f"Report: {Path(report_path).name}")
            print("="*80)
            
            # Load data
            report_data, trades_df, csv_path = self.load_data(report_path)
            
            # Display all sections
            self.display_overview(report_data)
            self.display_signal_flow(report_data)
            
            if trades_df is not None:
                self.display_advanced_metrics(trades_df, report_data)
                self.display_trade_analysis(trades_df)
                self.display_drawdown_analysis(trades_df)
                self.display_monthly_performance(trades_df)
            
            self.display_position_management(report_data)
            
            # Create visualizations if requested
            if create_visualizations and csv_path:
                output_dir = csv_path.parent
                self.create_visualizations(trades_df, output_dir)
            
            # File info
            print(f"\n{self.color_text('📁 FILES', self.colors.BOLD + self.colors.MAGENTA)}")
            print("-"*80)
            print(f"JSON Report: {Path(report_path).resolve()}")
            if csv_path:
                print(f"CSV Trades: {csv_path.resolve()}")
            
            print(f"\n{self.color_text('✅ Dashboard completed successfully!', self.colors.GREEN)}")
            
        except Exception as e:
            print(f"{self.colors.RED}❌ Error: {e}{self.colors.END}")
            import traceback
            traceback.print_exc()
            sys.exit(1)


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/dashboard_standalone.py <report_json> [--visualize]")
        print("\nArguments:")
        print("  <report_json>    Path to the JSON report file")
        print("  --visualize      Create visualizations (optional)")
        sys.exit(1)
    
    report_path = sys.argv[1]
    create_visualizations = '--visualize' in sys.argv
    
    dashboard = EnhancedDashboard()
    dashboard.display(report_path, create_visualizations)


if __name__ == "__main__":
    main()
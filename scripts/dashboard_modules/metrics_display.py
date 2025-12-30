"""
Metrics Display Module
Displays performance metrics
"""
import numpy as np
import pandas as pd
from typing import Dict, Optional
from .display_engine import DisplayEngine

class MetricsDisplay:
    def __init__(self, display_engine: DisplayEngine):
        self.display = display_engine
        
    def display_overview(self, report_data: Dict):
        """Display strategy overview"""
        self.display.print_header("📈 STRATEGY PERFORMANCE OVERVIEW")
        
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
        
        self.display.print_section("📊 TRADE STATISTICS")
        
        if perf_metrics:
            self._display_performance_metrics(perf_metrics)
        else:
            print("No performance metrics available")
    
    def _display_performance_metrics(self, metrics: Dict):
        """Display performance metrics"""
        stats = [
            ("Total Trades", f"{metrics.get('total_trades', 0)}"),
            ("Winning Trades", f"{metrics.get('winning_trades', 0)} ({metrics.get('win_rate', 0):.1f}%)"),
            ("Losing Trades", f"{metrics.get('losing_trades', 0)} ({metrics.get('loss_rate', 0):.1f}%)"),
            ("Breakeven Trades", f"{metrics.get('breakeven_trades', 0)}"),
            ("Total P&L (Points)", self.display.format_pnl(metrics.get('total_pnl_points', 0))),
            ("Avg P&L/Trade", self.display.format_pnl(metrics.get('avg_pnl_points', 0))),
            ("Profit Factor", f"{metrics.get('profit_factor', 0):.2f}"),
            ("Expectancy", f"{metrics.get('expectancy_points', 0):+.2f} pts"),
            ("Sharpe Ratio", f"{metrics.get('sharpe_ratio', 0):.2f}"),
        ]
        
        for label, value in stats:
            print(f"{label:<25} {value}")
    
    def display_advanced_metrics(self, trades_df: Optional[pd.DataFrame], report_data: Dict):
        """Display advanced trading metrics"""
        if trades_df is None or trades_df.empty:
            return
        
        closed_trades = trades_df[trades_df['status'] == 'CLOSED'].copy()
        if closed_trades.empty:
            return
        
        self.display.print_header("🎯 ADVANCED PERFORMANCE METRICS")
        
        # Get risk:reward from config
        rr_ratio = 3.0
        if 'risk_details' in report_data:
            rr_ratio = report_data['risk_details'].get('risk_to_reward', 3.0)
        
        # Calculate all advanced metrics
        closed_trades_sorted = closed_trades.sort_values('exit_time').copy()
        closed_trades_sorted['is_win'] = closed_trades_sorted['pnl_points'] > 0
        
        # 1. Basic metrics
        actual_win_rate = len(closed_trades[closed_trades['pnl_points'] > 0]) / len(closed_trades) * 100
        breakeven_win_rate = 1 / (1 + rr_ratio) * 100
        avg_sl = closed_trades['sl_distance'].mean()
        expectancy_r = closed_trades['pnl_points'].mean() / avg_sl if avg_sl > 0 else 0
        
        # 2. Trade frequency
        if len(closed_trades) > 1:
            first_trade = closed_trades['exit_time'].min()
            last_trade = closed_trades['exit_time'].max()
            days = (last_trade - first_trade).days + 1
            trades_per_day = len(closed_trades) / days
            profit_per_day = closed_trades['pnl_points'].sum() / days
        else:
            trades_per_day = 0
            profit_per_day = 0
        
        # 3. Consecutive wins/losses
        max_consecutive_wins, max_consecutive_losses = self._calculate_consecutive_streaks(closed_trades_sorted)
        
        # 4. Kelly Criterion
        win_prob = actual_win_rate / 100
        kelly = win_prob - ((1 - win_prob) / rr_ratio)
        kelly_color = self.display.colors.GREEN if kelly > 0 else self.display.colors.YELLOW
        
        # 5. Average win/loss in R units
        winning_trades = closed_trades[closed_trades['pnl_points'] > 0]
        losing_trades = closed_trades[closed_trades['pnl_points'] < 0]
        
        avg_win_r = winning_trades['pnl_points'].mean() / avg_sl if not winning_trades.empty and avg_sl > 0 else 0
        avg_loss_r = abs(losing_trades['pnl_points'].mean()) / avg_sl if not losing_trades.empty and avg_sl > 0 else 0
        
        # 6. System Quality Number (SQN)
        if len(closed_trades) >= 30:
            expectancy = closed_trades['pnl_points'].mean()
            std_dev = closed_trades['pnl_points'].std()
            sqn = np.sqrt(len(closed_trades)) * (expectancy / std_dev) if std_dev > 0 else 0
            sqn_display = f"{sqn:.2f}"
        else:
            sqn_display = "N/A (<30 trades)"
        
        # 7. Risk of Ruin
        risk_of_ruin = self._calculate_risk_of_ruin(win_prob, rr_ratio, closed_trades)
        
        # 8. Payoff Ratio
        payoff_ratio = abs(winning_trades['pnl_points'].mean() / losing_trades['pnl_points'].mean()) if not losing_trades.empty and losing_trades['pnl_points'].mean() != 0 else float('inf')
        
        # 9. Actual R:R
        actual_rr = closed_trades['risk_reward_ratio'].mean() if 'risk_reward_ratio' in closed_trades.columns else 0
        
        # Display all metrics
        metrics = [
            ("Breakeven Win Rate", f"{breakeven_win_rate:.1f}%"),
            ("Actual Win Rate", f"{actual_win_rate:.1f}%",
             self.display.colors.GREEN if actual_win_rate > breakeven_win_rate else self.display.colors.RED),
            ("Win Rate Premium", self.display.format_percentage(actual_win_rate - breakeven_win_rate)),
            ("Risk:Reward Target", f"1:{rr_ratio:.1f}"),
            ("Actual Avg R:R", f"1:{actual_rr:.2f}"),
            ("Expectancy (R units)", f"{expectancy_r:+.3f}R",
             self.display.colors.GREEN if expectancy_r > 0 else self.display.colors.RED),
            ("Expectancy (Points)", f"{closed_trades['pnl_points'].mean():+.2f} pts"),
            ("Avg Win (R units)", f"{avg_win_r:+.2f}R"),
            ("Avg Loss (R units)", f"{avg_loss_r:+.2f}R"),
            ("Payoff Ratio", f"{payoff_ratio:.2f}:1"),
            ("Trades per Day", f"{trades_per_day:.1f}"),
            ("Profit per Day", f"{profit_per_day:+.2f} pts",
             self.display.colors.GREEN if profit_per_day > 0 else self.display.colors.RED),
            ("Avg Risk per Trade", f"{avg_sl:.2f} pts"),
            ("Max Consecutive Wins", f"{max_consecutive_wins}"),
            ("Max Consecutive Losses", f"{max_consecutive_losses}"),
            ("Kelly Criterion %", f"{kelly*100:.1f}%" if isinstance(kelly, (int, float)) else str(kelly),
             kelly_color),
            ("System Quality Number", sqn_display),
            ("Risk of Ruin", f"{risk_of_ruin:.4%}" if isinstance(risk_of_ruin, float) else str(risk_of_ruin)),
        ]
        
        for metric in metrics:
            if len(metric) == 2:
                self.display.print_metric(metric[0], metric[1])
            elif len(metric) == 3:
                self.display.print_metric(metric[0], metric[1], metric[2])
    
    def _calculate_consecutive_streaks(self, closed_trades: pd.DataFrame) -> tuple:
        """Calculate consecutive winning and losing streaks"""
        max_consecutive_wins = 0
        max_consecutive_losses = 0
        current_wins = 0
        current_losses = 0
        
        for _, trade in closed_trades.iterrows():
            is_win = trade['pnl_points'] > 0
            if is_win:
                current_wins += 1
                current_losses = 0
                max_consecutive_wins = max(max_consecutive_wins, current_wins)
            else:
                current_losses += 1
                current_wins = 0
                max_consecutive_losses = max(max_consecutive_losses, current_losses)
        
        return max_consecutive_wins, max_consecutive_losses
    
    def _calculate_risk_of_ruin(self, win_prob: float, rr_ratio: float, closed_trades: pd.DataFrame) -> str:
        """Calculate risk of ruin (simplified)"""
        if win_prob > 0.5 and len(closed_trades) > 0:
            try:
                avg_risk = closed_trades['sl_distance'].mean()
                total_profit = closed_trades['pnl_points'].sum()
                r_units = total_profit / avg_risk if avg_risk > 0 else 0
                risk_of_ruin = ((1 - win_prob) / win_prob) ** r_units
                return risk_of_ruin
            except:
                return "High"
        else:
            return "High"
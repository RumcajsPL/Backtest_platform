"""
Drawdown Display Module
Displays drawdown and risk analysis
"""
import numpy as np
import pandas as pd
from typing import Optional
from .display_engine import DisplayEngine

class DrawdownDisplay:
    def __init__(self, display_engine: DisplayEngine):
        self.display = display_engine
    
    def display_drawdown_analysis(self, trades_df: Optional[pd.DataFrame]):
        """Display drawdown analysis"""
        if trades_df is None or trades_df.empty:
            return
        
        closed_trades = trades_df[trades_df['status'] == 'CLOSED'].copy()
        if closed_trades.empty:
            return
        
        self.display.print_header("📉 DRAWDOWN & RISK ANALYSIS")
        
        # Calculate equity curve and drawdown
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
        
        # Calculate metrics
        total_profit = closed_trades['pnl_points'].sum()
        recovery_factor = abs(total_profit / max_dd) if max_dd < 0 else float('inf')
        avg_drawdown = closed_trades[closed_trades['drawdown'] < 0]['drawdown'].mean()
        
        # Calculate Calmar Ratio
        if len(closed_trades) > 1:
            first_trade = closed_trades['exit_time'].min()
            last_trade = closed_trades['exit_time'].max()
            days = (last_trade - first_trade).days + 1
            annual_return = (total_profit / days) * 365
            calmar_ratio = abs(annual_return / max_dd) if max_dd < 0 else float('inf')
        else:
            calmar_ratio = 0
        
        # Display metrics
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
        dd_durations = self._calculate_drawdown_durations(closed_trades)
        
        if dd_durations:
            max_dd_duration = max(dd_durations)
            avg_dd_duration = np.mean(dd_durations)
            
            print(f"{'Max DD Duration:':<25} {max_dd_duration:.1f} hours")
            print(f"{'Avg DD Duration:':<25} {avg_dd_duration:.1f} hours")
            print(f"{'Number of DDs:':<25} {len(dd_durations)}")
        
        # Calculate worst losing streak
        worst_streak = self._calculate_worst_losing_streak(closed_trades)
        if worst_streak['max_streak'] > 0:
            print(f"{'Worst Losing Streak:':<25} {worst_streak['max_streak']} trades "
                  f"({worst_streak['max_streak_pnl']:.2f} pts)")
    
    def _calculate_drawdown_durations(self, closed_trades: pd.DataFrame) -> list:
        """Calculate durations of drawdown periods"""
        in_drawdown = False
        dd_start = None
        dd_durations = []
        
        for _, trade in closed_trades.iterrows():
            if trade['drawdown'] < 0:
                if not in_drawdown:
                    in_drawdown = True
                    dd_start = trade['exit_time']
            else:
                if in_drawdown and dd_start:
                    dd_duration = (trade['exit_time'] - dd_start).total_seconds() / 3600
                    dd_durations.append(dd_duration)
                    in_drawdown = False
        
        # Handle ongoing drawdown at end
        if in_drawdown and dd_start:
            last_time = closed_trades['exit_time'].iloc[-1]
            dd_duration = (last_time - dd_start).total_seconds() / 3600
            dd_durations.append(dd_duration)
        
        return dd_durations
    
    def _calculate_worst_losing_streak(self, closed_trades: pd.DataFrame) -> dict:
        """Calculate worst losing streak"""
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
        
        return {
            'max_streak': max_loss_streak,
            'max_streak_pnl': max_streak_pnl
        }
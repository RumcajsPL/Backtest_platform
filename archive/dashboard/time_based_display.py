"""
Time-Based Display Module
Displays monthly and hourly performance
"""
import pandas as pd
import numpy as np
from typing import Optional
from .display_engine import DisplayEngine

class TimeBasedDisplay:
    def __init__(self, display_engine: DisplayEngine):
        self.display = display_engine
    
    def display_monthly_performance(self, trades_df: Optional[pd.DataFrame]):
        """Display monthly performance breakdown"""
        if trades_df is None or trades_df.empty:
            return
        
        closed_trades = trades_df[trades_df['status'] == 'CLOSED'].copy()
        if closed_trades.empty:
            return
        
        if 'exit_time' not in closed_trades.columns:
            return
        
        self.display.print_header("📅 TIME-BASED PERFORMANCE")
        
        # Group by month
        closed_trades['month'] = pd.to_datetime(closed_trades['exit_time']).dt.to_period('M')
        monthly = self._calculate_monthly_stats(closed_trades)
        
        if monthly.empty:
            print("No monthly data available")
            return
        
        # Display monthly table
        self._display_monthly_table(monthly)
        
        # Display summary and statistics
        self._display_monthly_summary(monthly)
    
    def _calculate_monthly_stats(self, closed_trades: pd.DataFrame) -> pd.DataFrame:
        """Calculate monthly statistics"""
        monthly = closed_trades.groupby('month').agg({
            'pnl_points': ['sum', 'mean', 'count'],
            'is_win': lambda x: (x == True).sum() / len(x) * 100 if len(x) > 0 else 0
        }).round(2)
        
        monthly.columns = ['total_pnl', 'avg_pnl', 'trades', 'win_rate']
        return monthly
    
    def _display_monthly_table(self, monthly: pd.DataFrame):
        """Display monthly performance table"""
        print(f"{'Month':<12} {'Trades':>8} {'Win Rate':>10} {'Total P&L':>12} {'Avg P&L':>12}")
        print("-" * 58)
        
        for month, row in monthly.iterrows():
            total_color = (self.display.colors.GREEN if row['total_pnl'] > 0 
                         else self.display.colors.RED if row['total_pnl'] < 0 
                         else "")
            avg_color = (self.display.colors.GREEN if row['avg_pnl'] > 0 
                       else self.display.colors.RED if row['avg_pnl'] < 0 
                       else "")
            win_color = (self.display.colors.GREEN if row['win_rate'] > 50 
                       else self.display.colors.RED)
            
            print(f"{str(month):<12} {row['trades']:>8.0f} "
                  f"{win_color}{row['win_rate']:>9.1f}%{self.display.colors.END} "
                  f"{total_color}{row['total_pnl']:>+11.2f}{self.display.colors.END} "
                  f"{avg_color}{row['avg_pnl']:>+11.2f}{self.display.colors.END}")
    
    def _display_monthly_summary(self, monthly: pd.DataFrame):
        """Display monthly summary statistics"""
        print("-" * 58)
        
        total_trades = monthly['trades'].sum()
        total_pnl = monthly['total_pnl'].sum()
        avg_monthly_pnl = monthly['total_pnl'].mean()
        avg_win_rate = (monthly['win_rate'] * monthly['trades']).sum() / total_trades if total_trades > 0 else 0
        
        total_color = (self.display.colors.GREEN if total_pnl > 0 
                     else self.display.colors.RED if total_pnl < 0 
                     else "")
        win_color = (self.display.colors.GREEN if avg_win_rate > 50 
                   else self.display.colors.RED)
        
        print(f"{'TOTAL/AVG':<12} {total_trades:>8.0f} "
              f"{win_color}{avg_win_rate:>9.1f}%{self.display.colors.END} "
              f"{total_color}{total_pnl:>+11.2f}{self.display.colors.END} "
              f"{avg_monthly_pnl:>+11.2f}")
        
        # Best/worst months
        best_month = monthly['total_pnl'].idxmax()
        best_pnl = monthly['total_pnl'].max()
        worst_month = monthly['total_pnl'].idxmin()
        worst_pnl = monthly['total_pnl'].min()
        
        print(f"\n{'Best Month:':<15} {best_month} "
              f"({self.display.colors.GREEN}{best_pnl:+.2f}{self.display.colors.END} pts)")
        print(f"{'Worst Month:':<15} {worst_month} "
              f"({self.display.colors.RED}{worst_pnl:+.2f}{self.display.colors.END} pts)")
        
        # Monthly consistency
        positive_months = len(monthly[monthly['total_pnl'] > 0])
        negative_months = len(monthly[monthly['total_pnl'] < 0])
        monthly_consistency = positive_months / len(monthly) * 100 if len(monthly) > 0 else 0
        
        print(f"\n{'Monthly Consistency:':<20} {monthly_consistency:.1f}% positive months")
        print(f"{'Best/Worst Ratio:':<20} {abs(best_pnl / worst_pnl):.2f}:1" if worst_pnl != 0 else f"{'Best/Worst Ratio:':<20} N/A")
    
    def display_hourly_performance(self, trades_df: Optional[pd.DataFrame]):
        """Display performance by hour of day"""
        if trades_df is None or trades_df.empty:
            return
        
        closed_trades = trades_df[trades_df['status'] == 'CLOSED'].copy()
        if closed_trades.empty:
            return
        
        if 'entry_time' not in closed_trades.columns:
            return
        
        self.display.print_section("🕒 PERFORMANCE BY HOUR OF DAY")
        
        # Group by hour
        closed_trades['hour'] = closed_trades['entry_time'].dt.hour
        hourly_perf = self._calculate_hourly_stats(closed_trades)
        
        if hourly_perf.empty:
            print("No hourly data available")
            return
        
        # Display hourly table
        print(f"{'Hour':<6} {'Trades':>8} {'Win Rate':>10} {'Total P&L':>12} {'Avg P&L':>12}")
        print("-" * 58)
        
        for hour in sorted(hourly_perf.index):
            row = hourly_perf.loc[hour]
            self._display_hourly_row(hour, row)
    
    def _calculate_hourly_stats(self, closed_trades: pd.DataFrame) -> pd.DataFrame:
        """Calculate hourly statistics"""
        hourly_perf = closed_trades.groupby('hour').agg({
            'pnl_points': ['sum', 'mean', 'count'],
            'is_win': lambda x: (x == True).sum() / len(x) * 100 if len(x) > 0 else 0
        }).round(2)
        
        hourly_perf.columns = ['total_pnl', 'avg_pnl', 'trades', 'win_rate']
        return hourly_perf
    
    def _display_hourly_row(self, hour: int, row: pd.Series):
        """Display a row of hourly performance"""
        total_color = (self.display.colors.GREEN if row['total_pnl'] > 0 
                     else self.display.colors.RED if row['total_pnl'] < 0 
                     else "")
        avg_color = (self.display.colors.GREEN if row['avg_pnl'] > 0 
                   else self.display.colors.RED if row['avg_pnl'] < 0 
                   else "")
        
        print(f"{hour:02d}:00 {row['trades']:>8.0f} {row['win_rate']:>9.1f}% "
              f"{total_color}{row['total_pnl']:>+11.2f}{self.display.colors.END} "
              f"{avg_color}{row['avg_pnl']:>+11.2f}{self.display.colors.END}")
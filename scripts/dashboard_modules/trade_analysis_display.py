"""
Trade Analysis Display Module
Displays detailed trade analysis
"""
import pandas as pd
from typing import Optional
from .display_engine import DisplayEngine

class TradeAnalysisDisplay:
    def __init__(self, display_engine: DisplayEngine):
        self.display = display_engine
    
    def display_trade_analysis(self, trades_df: Optional[pd.DataFrame]):
        """Display detailed trade analysis"""
        if trades_df is None or trades_df.empty:
            print(f"\n{self.display.colors.YELLOW}⚠️  No trade data available for analysis{self.display.colors.END}")
            return
        
        self.display.print_header("🔍 DETAILED TRADE ANALYSIS")
        
        # Filter trades by status
        closed_trades = trades_df[trades_df['status'] == 'CLOSED'].copy()
        open_trades = trades_df[trades_df['status'] == 'OPEN']
        rejected_trades = trades_df[trades_df['status'] == 'REJECTED']
        
        # Display counts
        print(f"Total Records: {len(trades_df)}")
        print(f"  • {self.display.colors.GREEN}Closed Trades: {len(closed_trades)}{self.display.colors.END}")
        print(f"  • {self.display.colors.YELLOW}Open Trades: {len(open_trades)}{self.display.colors.END}")
        print(f"  • {self.display.colors.RED}Rejected Signals: {len(rejected_trades)}{self.display.colors.END}")
        
        if closed_trades.empty:
            print(f"\n{self.display.colors.YELLOW}No closed trades to analyze{self.display.colors.END}")
            return
        
        # Display each analysis section
        self._display_performance_by_direction(closed_trades)
        self._display_exit_reason_analysis(closed_trades)
        self._display_duration_analysis(closed_trades)
        self._display_risk_management(closed_trades)
        self._display_sample_trades(closed_trades)
    
    def _display_performance_by_direction(self, closed_trades: pd.DataFrame):
        """Display performance by trade direction"""
        self.display.print_section("📈 PERFORMANCE BY DIRECTION")
        
        for direction in ['BUY', 'SELL']:
            dir_trades = closed_trades[closed_trades['direction'] == direction]
            if len(dir_trades) > 0:
                self._display_direction_stats(direction, dir_trades)
    
    def _display_direction_stats(self, direction: str, trades: pd.DataFrame):
        """Display statistics for a specific direction"""
        winning = trades[trades['pnl_points'] > 0]
        losing = trades[trades['pnl_points'] < 0]
        
        win_rate = len(winning) / len(trades) * 100 if len(trades) > 0 else 0
        total_pnl = trades['pnl_points'].sum()
        avg_pnl = trades['pnl_points'].mean()
        avg_sl = trades['sl_distance'].mean() if 'sl_distance' in trades.columns else 0
        
        color = (self.display.colors.GREEN if total_pnl > 0 
                else self.display.colors.RED if total_pnl < 0 
                else "")
        
        print(f"{direction}: {len(trades)} trades")
        print(f"  Win Rate: {win_rate:.1f}%  |  "
              f"Total P&L: {color}{total_pnl:+.2f}{self.display.colors.END} pts  |  "
              f"Avg P&L: {avg_pnl:+.2f} pts  |  "
              f"Avg Risk: {avg_sl:.2f} pts")
    
    def _display_exit_reason_analysis(self, closed_trades: pd.DataFrame):
        """Display exit reason analysis"""
        if 'exit_reason' not in closed_trades.columns:
            return
        
        self.display.print_section("🚪 EXIT REASON ANALYSIS")
        
        exit_counts = closed_trades['exit_reason'].value_counts()
        for reason, count in exit_counts.items():
            pct = count / len(closed_trades) * 100
            avg_pnl = closed_trades[closed_trades['exit_reason'] == reason]['pnl_points'].mean()
            win_rate = (closed_trades[closed_trades['exit_reason'] == reason]['pnl_points'] > 0).mean() * 100
            
            color = (self.display.colors.GREEN if avg_pnl > 0 
                    else self.display.colors.RED if avg_pnl < 0 
                    else "")
            win_color = (self.display.colors.GREEN if win_rate > 50 
                       else self.display.colors.RED)
            
            print(f"{reason:<20} {count:>4} ({pct:5.1f}%)  |  "
                  f"Avg P&L: {color}{avg_pnl:+.2f}{self.display.colors.END} pts  |  "
                  f"Win Rate: {win_color}{win_rate:.1f}%{self.display.colors.END}")
    
    def _display_duration_analysis(self, closed_trades: pd.DataFrame):
        """Display duration analysis"""
        if 'duration_minutes' not in closed_trades.columns:
            return
        
        self.display.print_section("⏱️  DURATION ANALYSIS")
        
        stats = [
            ("Avg Duration", f"{closed_trades['duration_minutes'].mean():.1f} min"),
            ("Min Duration", f"{closed_trades['duration_minutes'].min():.1f} min"),
            ("Max Duration", f"{closed_trades['duration_minutes'].max():.1f} min"),
            ("Median Duration", f"{closed_trades['duration_minutes'].median():.1f} min"),
        ]
        
        # Add average bars if available
        if 'duration_bars' in closed_trades.columns:
            stats.append(("Avg Bars", f"{closed_trades['duration_bars'].mean():.1f} bars"))
        
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
            self._display_duration_by_exit_reason(closed_trades)
    
    def _display_duration_by_exit_reason(self, closed_trades: pd.DataFrame):
        """Display duration by exit reason"""
        self.display.print_section("⏱️  DURATION BY EXIT REASON")
        
        for reason in closed_trades['exit_reason'].unique():
            reason_trades = closed_trades[closed_trades['exit_reason'] == reason]
            if len(reason_trades) > 0:
                avg_dur = reason_trades['duration_minutes'].mean()
                print(f"{reason:<20} {avg_dur:.1f} min")
    
    def _display_risk_management(self, closed_trades: pd.DataFrame):
        """Display risk management metrics"""
        required_cols = ['sl_distance', 'tp_distance', 'risk_reward_ratio']
        if not all(col in closed_trades.columns for col in required_cols):
            return
        
        self.display.print_section("🎯 RISK MANAGEMENT")
        
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
    
    def _display_sample_trades(self, closed_trades: pd.DataFrame):
        """Display sample trades"""
        self.display.print_section("📋 SAMPLE TRADES (Last 5 Closed)")
        
        sample_trades = closed_trades.tail(5)
        for _, trade in sample_trades.iterrows():
            color = self.display.colors.GREEN if trade['pnl_points'] > 0 else self.display.colors.RED
            arrow = "🟢" if trade['direction'] == 'BUY' else "🔴"
            
            print(f"{arrow} {trade['entry_time'].strftime('%Y-%m-%d %H:%M')} "
                  f"{trade['direction']} @ {trade['entry_price']:.2f}")
            print(f"   Exit: {trade['exit_time'].strftime('%Y-%m-%d %H:%M')} "
                  f"@ {trade['exit_price']:.2f} ({trade['exit_reason']})")
            print(f"   P&L: {color}{trade['pnl_points']:+.2f}{self.display.colors.END} pts "
                  f"({trade['duration_minutes']:.0f} min) | "
                  f"Risk: {trade['sl_distance']:.2f} pts")
            print()
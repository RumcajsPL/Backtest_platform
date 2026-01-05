"""
Trade Analysis Display Module
Updated for progressive data structure with actual column names
"""
import pandas as pd
import numpy as np
from typing import Optional
from .display_engine import DisplayEngine

class TradeAnalysisDisplay:
    def __init__(self, display_engine: DisplayEngine):
        self.display = display_engine
    
    def display_trade_analysis(self, trades_df: Optional[pd.DataFrame]):
        """Display detailed trade analysis - updated for progressive data"""
        if trades_df is None or trades_df.empty:
            print(f"\n{self.display.colors.YELLOW}⚠️  No trade data available for analysis{self.display.colors.END}")
            return
        
        self.display.print_header("🔍 DETAILED TRADE ANALYSIS")
        
        # Since trades_df contains executed trades (from progressive data extraction)
        # Filter for closed trades based on progressive data structure
        closed_trades = trades_df.copy()  # All extracted trades are executed/closed
        
        # Try to identify truly closed vs open trades
        if 'exit_reason' in trades_df.columns:
            closed_trades = trades_df[trades_df['exit_reason'].notna()].copy()
            open_trades = trades_df[trades_df['exit_reason'].isna()]
        else:
            closed_trades = trades_df.copy()
            open_trades = pd.DataFrame()
        
        # Display counts
        print(f"Total Records: {len(trades_df)}")
        print(f"  • {self.display.colors.GREEN}Closed Trades: {len(closed_trades)}{self.display.colors.END}")
        if not open_trades.empty:
            print(f"  • {self.display.colors.YELLOW}Open Trades: {len(open_trades)}{self.display.colors.END}")
        
        if closed_trades.empty:
            print(f"\n{self.display.colors.YELLOW}No closed trades to analyze{self.display.colors.END}")
            return
        
        # Display each analysis section
        self._display_performance_by_direction(closed_trades)
        self._display_exit_reason_analysis(closed_trades)
        self._display_duration_analysis(closed_trades)
        self._display_spread_analysis(closed_trades)  # NEW: Spread analysis
        self._display_risk_management(closed_trades)
        self._display_sample_trades(closed_trades)
    
    def _display_performance_by_direction(self, closed_trades: pd.DataFrame):
        """Display performance by trade direction"""
        self.display.print_section("📈 PERFORMANCE BY DIRECTION")
        
        # In progressive data, direction is in 'signal' column
        direction_col = 'signal' if 'signal' in closed_trades.columns else None
        
        if not direction_col:
            print("No direction data available")
            return
        
        for direction in ['BUY', 'SELL']:
            dir_trades = closed_trades[closed_trades[direction_col] == direction]
            if len(dir_trades) > 0:
                self._display_direction_stats(direction, dir_trades)
    
    def _display_direction_stats(self, direction: str, trades: pd.DataFrame):
        """Display statistics for a specific direction"""
        winning = trades[trades['pnl_points'] > 0]
        
        win_rate = len(winning) / len(trades) * 100 if len(trades) > 0 else 0
        total_pnl = trades['pnl_points'].sum()
        avg_pnl = trades['pnl_points'].mean()
        
        # Get SL distance - from progressive data it's 'sl_distance_raw'
        sl_distance = trades.get('sl_distance_raw', 0)
        if hasattr(sl_distance, 'mean'):
            sl_distance = sl_distance.mean()
        else:
            sl_distance = 0
        
        # Calculate net P&L after spread if available
        net_pnl = total_pnl
        if 'spread_cost' in trades.columns:
            total_spread = trades['spread_cost'].sum()
            net_pnl = total_pnl - total_spread
        
        color = (self.display.colors.GREEN if total_pnl > 0 
                else self.display.colors.RED if total_pnl < 0 
                else "")
        net_color = (self.display.colors.GREEN if net_pnl > 0 
                    else self.display.colors.RED if net_pnl < 0 
                    else "")
        
        print(f"{direction}: {len(trades)} trades")
        print(f"  Win Rate: {win_rate:.1f}%")
        print(f"  Gross P&L: {color}{total_pnl:+.2f}{self.display.colors.END} pts")
        
        if 'spread_cost' in trades.columns:
            total_spread = trades['spread_cost'].sum()
            print(f"  Spread Cost: {self.display.colors.YELLOW}{total_spread:+.2f}{self.display.colors.END} pts")
            print(f"  Net P&L: {net_color}{net_pnl:+.2f}{self.display.colors.END} pts")
        
        print(f"  Avg P&L: {avg_pnl:+.2f} pts")
        print(f"  Avg Risk: {sl_distance:.2f} pts")
    
    def _display_exit_reason_analysis(self, closed_trades: pd.DataFrame):
        """Display exit reason analysis with meaningful metrics"""
        if 'exit_reason' not in closed_trades.columns:
            return
        
        self.display.print_section("🚪 EXIT REASON ANALYSIS")
        
        exit_counts = closed_trades['exit_reason'].value_counts()
        for reason, count in exit_counts.items():
            pct = count / len(closed_trades) * 100
            reason_trades = closed_trades[closed_trades['exit_reason'] == reason]
            avg_pnl = reason_trades['pnl_points'].mean() if 'pnl_points' in reason_trades.columns else 0
            
            color = (self.display.colors.GREEN if avg_pnl > 0 
                    else self.display.colors.RED if avg_pnl < 0 
                    else "")
            
            # Show meaningful metrics based on exit reason
            if reason == 'STOP_LOSS':
                # For SL trades, show how much they lost vs expected risk
                if 'sl_distance_raw' in reason_trades.columns:
                    avg_risk = reason_trades['sl_distance_raw'].mean()
                    loss_vs_risk = abs(avg_pnl) / avg_risk if avg_risk > 0 else 0
                    extra_info = f" | Loss: {abs(avg_pnl):.1f}pts ({loss_vs_risk:.1f}x risk)"
                else:
                    extra_info = ""
                    
            elif reason == 'TAKE_PROFIT':
                # For TP trades, show how much they won vs expected reward
                if 'rr_ratio' in reason_trades.columns and 'sl_distance_raw' in reason_trades.columns:
                    avg_rr = reason_trades['rr_ratio'].mean()
                    avg_risk = reason_trades['sl_distance_raw'].mean()
                    expected_reward = avg_risk * avg_rr
                    win_vs_expected = avg_pnl / expected_reward if expected_reward > 0 else 0
                    extra_info = f" | Win: {avg_pnl:.1f}pts ({win_vs_expected:.1f}x expected)"
                else:
                    extra_info = ""
                    
            else:
                # For other exit reasons
                avg_duration = reason_trades['duration_minutes'].mean() if 'duration_minutes' in reason_trades.columns else 0
                extra_info = f" | Avg duration: {avg_duration:.0f} min"
            
            print(f"{reason:<20} {count:>4} ({pct:5.1f}%)  |  "
                  f"Avg P&L: {color}{avg_pnl:+.2f}{self.display.colors.END} pts{extra_info}")
                
    def _display_duration_analysis(self, closed_trades: pd.DataFrame):
        """Display duration analysis"""
        # In progressive data, duration is in 'duration_minutes'
        if 'duration_minutes' not in closed_trades.columns:
            return
        
        self.display.print_section("⏱️  DURATION ANALYSIS")
        
        duration_minutes = closed_trades['duration_minutes']
        
        stats = [
            ("Avg Duration", f"{duration_minutes.mean():.1f} min"),
            ("Min Duration", f"{duration_minutes.min():.1f} min"),
            ("Max Duration", f"{duration_minutes.max():.1f} min"),
            ("Median Duration", f"{duration_minutes.median():.1f} min"),
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
    
    def _display_spread_analysis(self, closed_trades: pd.DataFrame):
        """Display spread cost analysis"""
        if 'spread_cost' not in closed_trades.columns:
            return
        
        self.display.print_section("💰 SPREAD COST ANALYSIS")
        
        total_spread_cost = closed_trades['spread_cost'].sum()
        total_pnl = closed_trades['pnl_points'].sum()
        net_pnl = total_pnl - total_spread_cost
        
        spread_impact_pct = (abs(total_spread_cost) / abs(total_pnl) * 100) if total_pnl != 0 else 0
        
        stats = [
            ("Total Spread Cost", f"{self.display.colors.YELLOW}{total_spread_cost:+.2f}{self.display.colors.END} pts"),
            ("Gross P&L (no spread)", f"{total_pnl:+.2f} pts"),
            ("Net P&L (after spread)", f"{self.display.format_pnl(net_pnl)}"),
            ("Spread Impact", f"{spread_impact_pct:.1f}% of P&L"),
            ("Avg Spread per Trade", f"{closed_trades['spread_cost'].mean():.2f} pts"),
            ("Max Spread Cost", f"{closed_trades['spread_cost'].max():.2f} pts"),
            ("Min Spread Cost", f"{closed_trades['spread_cost'].min():.2f} pts"),
        ]
        
        # Show spread efficiency if available
        if 'spread_efficiency_percent' in closed_trades.columns:
            spread_eff = closed_trades['spread_efficiency_percent'].mean()
            stats.append(("Avg Spread Efficiency", f"{spread_eff:.3f}%"))
        
        for label, value in stats:
            print(f"{label:<30} {value}")
    
    def _display_risk_management(self, closed_trades: pd.DataFrame):
        """Display risk management metrics - updated for progressive data"""
        self.display.print_section("🎯 RISK MANAGEMENT")
        
        # From progressive data:
        # - SL distance: 'sl_distance_raw'
        # - TP distance: can be calculated from SL distance and R:R
        
        stats = []
        
        if 'sl_distance_raw' in closed_trades.columns:
            sl_data = closed_trades['sl_distance_raw']
            stats.append(("Avg SL Distance", f"{sl_data.mean():.2f} pts"))
            stats.append(("Median SL Distance", f"{sl_data.median():.2f} pts"))
            stats.append(("Min SL Distance", f"{sl_data.min():.2f} pts"))
            stats.append(("Max SL Distance", f"{sl_data.max():.2f} pts"))
        
        # Calculate ACTUAL Risk:Reward from trade execution
        actual_rr_ratios = []
        if all(col in closed_trades.columns for col in ['sl_price_final', 'tp_price_final', 'entry_price']):
            for _, trade in closed_trades.iterrows():
                try:
                    entry = trade['entry_price']
                    sl = trade['sl_price_final']
                    tp = trade['tp_price_final']
                    
                    if pd.notna(entry) and pd.notna(sl) and pd.notna(tp):
                        risk = abs(entry - sl)
                        reward = abs(tp - entry)
                        if risk > 0:
                            rr = reward / risk
                            actual_rr_ratios.append(rr)
                except (TypeError, KeyError):
                    continue
        
        if actual_rr_ratios:
            actual_rr = np.mean(actual_rr_ratios)
            stats.append(("Actual Avg R:R", f"1:{actual_rr:.2f}"))
            stats.append(("Min Actual R:R", f"1:{min(actual_rr_ratios):.2f}"))
            stats.append(("Max Actual R:R", f"1:{max(actual_rr_ratios):.2f}"))
            if len(actual_rr_ratios) > 1:
                stats.append(("Std Dev R:R", f"±{np.std(actual_rr_ratios):.3f}"))
        
        # Show target R:R if available (but label it clearly as target)
        if 'rr_ratio' in closed_trades.columns and not closed_trades['rr_ratio'].isna().all():
            target_rr = closed_trades['rr_ratio'].iloc[0]  # Assuming all same
            stats.append(("Target R:R", f"1:{target_rr:.1f}"))
        
        # Also check for ATR-based risk if available
        if 'atr_value' in closed_trades.columns and 'atr_multiplier' in closed_trades.columns:
            atr_risk = closed_trades['atr_value'] * closed_trades['atr_multiplier']
            stats.append(("Avg ATR-based Risk", f"{atr_risk.mean():.2f} pts"))
        
        # Show risk percentiles if available
        if 'risk_percentile_calculated' in closed_trades.columns:
            risk_pct = closed_trades['risk_percentile_calculated'] * 100  # Convert to percentage
            stats.append(("Avg Risk %", f"{risk_pct.mean():.3f}%"))
            stats.append(("Max Risk %", f"{risk_pct.max():.3f}%"))
        
        for label, value in stats:
            print(f"{label:<25} {value}")
    
    def _display_sample_trades(self, closed_trades: pd.DataFrame):
        """Display sample trades with spread information"""
        self.display.print_section("📋 SAMPLE TRADES (Last 5 Closed)")
        
        # Sort by exit time for sample
        if 'exit_time' in closed_trades.columns:
            sample_trades = closed_trades.sort_values('exit_time').tail(5)
        else:
            sample_trades = closed_trades.tail(5)
        
        for _, trade in sample_trades.iterrows():
            pnl = trade.get('pnl_points', 0)
            color = self.display.colors.GREEN if pnl > 0 else self.display.colors.RED
            
            # Get direction from 'signal' column
            trade_direction = trade.get('signal', 'N/A')
            arrow = "🟢" if trade_direction == 'BUY' else "🔴"
            
            # Format timestamps
            entry_time = trade.get('entry_time', 'N/A')
            if pd.notna(entry_time) and hasattr(entry_time, 'strftime'):
                entry_time_str = entry_time.strftime('%Y-%m-%d %H:%M')
            else:
                entry_time_str = str(entry_time)
            
            exit_time = trade.get('exit_time', 'N/A')
            if pd.notna(exit_time) and hasattr(exit_time, 'strftime'):
                exit_time_str = exit_time.strftime('%Y-%m-%d %H:%M')
            else:
                exit_time_str = str(exit_time)
            
            print(f"{arrow} {entry_time_str} "
                  f"{trade_direction} @ {trade.get('entry_price', 0):.2f}")
            print(f"   Exit: {exit_time_str} "
                  f"@ {trade.get('exit_price', 0):.2f} ({trade.get('exit_reason', 'N/A')})")
            
            # Get duration
            duration = trade.get('duration_minutes', 0)
            
            # Get SL distance
            sl_distance = trade.get('sl_distance_raw', 0)
            
            # Get spread cost if available
            spread_info = ""
            if 'spread_cost' in trade and pd.notna(trade['spread_cost']):
                spread = trade['spread_cost']
                gross_pnl = pnl + spread  # pnl_points is NET after spread
                spread_info = f" | Gross: {gross_pnl:+.2f}pts, Spread: {spread:.2f}pts"
            
            print(f"   P&L: {color}{pnl:+.2f}{self.display.colors.END} pts "
                  f"({duration:.0f} min){spread_info}")
            print(f"   Risk: {sl_distance:.2f} pts | R:R: 1:{trade.get('rr_ratio', 'N/A')}")
            print()
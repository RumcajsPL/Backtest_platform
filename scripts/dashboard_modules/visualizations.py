"""
Visualizations Module
Generates charts and graphs for the dashboard
"""
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from pathlib import Path
import pandas as pd
from typing import Optional
from datetime import datetime

class DashboardVisualizations:
    def __init__(self, output_dir: Path, timestamp: Optional[str] = None):
        self.output_dir = output_dir
        self.timestamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
        self.vis_dir = output_dir / "visualizations"
        self.vis_dir.mkdir(exist_ok=True)
    
    def create_all_visualizations(self, trades_df: pd.DataFrame):
        """Create all available visualizations"""
        if trades_df is None or trades_df.empty:
            print("⚠️  No trade data available for visualizations")
            return
        
        closed_trades = trades_df[trades_df['status'] == 'CLOSED'].copy()
        if closed_trades.empty:
            print("⚠️  No closed trades available for visualizations")
            return
        
        try:
            print(f"\n🖼️  Creating visualizations...")
            
            # Sort trades by exit time
            closed_trades = closed_trades.sort_values('exit_time')
            
            # Create all visualizations
            self.create_equity_curve(closed_trades)
            self.create_pnl_distribution(closed_trades)
            self.create_drawdown_chart(closed_trades)
            self.create_win_loss_pie_chart(closed_trades)
            
            if 'exit_reason' in closed_trades.columns:
                self.create_exit_reasons_chart(closed_trades)
            
            if 'direction' in closed_trades.columns:
                self.create_direction_performance_chart(closed_trades)
            
            self.create_monthly_performance_chart(closed_trades)
            self.create_hourly_performance_chart(closed_trades)
            
            print(f"✅ Visualizations saved to: {self.vis_dir}")
            print(f"   Files:")
            for file in self.vis_dir.glob(f"dashboard_*_{self.timestamp}.png"):
                print(f"   • {file.name}")
            
        except Exception as e:
            print(f"❌ Could not create visualizations: {e}")
            import traceback
            traceback.print_exc()
    
    def create_equity_curve(self, closed_trades: pd.DataFrame):
        """Create equity curve chart"""
        plt.figure(figsize=(14, 7))
        
        # Calculate equity curve
        closed_trades['cumulative_pnl'] = closed_trades['pnl_points'].cumsum()
        closed_trades['running_max'] = closed_trades['cumulative_pnl'].cummax()
        closed_trades['drawdown'] = closed_trades['cumulative_pnl'] - closed_trades['running_max']
        
        # Plot equity curve
        plt.plot(closed_trades['exit_time'], closed_trades['cumulative_pnl'], 
                linewidth=2, color='blue', label='Equity Curve')
        
        # Plot running maximum
        plt.plot(closed_trades['exit_time'], closed_trades['running_max'], 
                linewidth=1.5, color='green', linestyle='--', alpha=0.7, label='Running Max')
        
        # Fill drawdown area
        plt.fill_between(closed_trades['exit_time'], 
                        closed_trades['cumulative_pnl'], closed_trades['running_max'],
                        where=closed_trades['drawdown'] < 0,
                        color='red', alpha=0.3, label='Drawdown')
        
        # Formatting
        plt.title(f'Equity Curve\nTotal P&L: {closed_trades["cumulative_pnl"].iloc[-1]:+.2f} pts', 
                 fontsize=14, fontweight='bold')
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Cumulative P&L (Points)', fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.legend()
        
        # Format x-axis dates
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        plt.savefig(self.vis_dir / f'equity_curve_{self.timestamp}.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def create_pnl_distribution(self, closed_trades: pd.DataFrame):
        """Create P&L distribution chart"""
        plt.figure(figsize=(12, 6))
        
        # Create histogram
        n_bins = min(20, len(closed_trades) // 5)
        colors = ['green' if x > 0 else 'red' for x in closed_trades['pnl_points']]
        
        # Plot histogram
        plt.hist(closed_trades['pnl_points'], bins=n_bins, color='skyblue', 
                edgecolor='black', alpha=0.7, label='P&L Distribution')
        
        # Add vertical lines for statistics
        mean_pnl = closed_trades['pnl_points'].mean()
        median_pnl = closed_trades['pnl_points'].median()
        
        plt.axvline(x=mean_pnl, color='red', linestyle='-', linewidth=2, 
                   label=f'Mean: {mean_pnl:+.2f}')
        plt.axvline(x=median_pnl, color='blue', linestyle='--', linewidth=2, 
                   label=f'Median: {median_pnl:+.2f}')
        plt.axvline(x=0, color='black', linestyle='-', linewidth=1, alpha=0.5)
        
        # Formatting
        plt.title('P&L Distribution', fontsize=14, fontweight='bold')
        plt.xlabel('P&L (Points)', fontsize=12)
        plt.ylabel('Frequency', fontsize=12)
        plt.grid(True, alpha=0.3, axis='y')
        plt.legend()
        
        plt.tight_layout()
        plt.savefig(self.vis_dir / f'pnl_distribution_{self.timestamp}.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def create_drawdown_chart(self, closed_trades: pd.DataFrame):
        """Create drawdown chart"""
        if 'drawdown' not in closed_trades.columns:
            # Calculate drawdown if not already calculated
            closed_trades['cumulative_pnl'] = closed_trades['pnl_points'].cumsum()
            closed_trades['running_max'] = closed_trades['cumulative_pnl'].cummax()
            closed_trades['drawdown'] = closed_trades['cumulative_pnl'] - closed_trades['running_max']
            closed_trades['drawdown_pct'] = (closed_trades['drawdown'] / closed_trades['running_max'].clip(lower=1)) * 100
        
        plt.figure(figsize=(14, 6))
        
        # Plot drawdown
        plt.fill_between(closed_trades['exit_time'], 0, closed_trades['drawdown'],
                        where=closed_trades['drawdown'] < 0,
                        color='red', alpha=0.5, label='Drawdown')
        
        # Find max drawdown
        max_dd_idx = closed_trades['drawdown'].idxmin()
        max_dd = closed_trades.loc[max_dd_idx, 'drawdown']
        max_dd_time = closed_trades.loc[max_dd_idx, 'exit_time']
        
        # Mark max drawdown
        plt.scatter([max_dd_time], [max_dd], color='darkred', s=100, 
                   zorder=5, label=f'Max DD: {max_dd:.2f} pts')
        
        # Formatting
        plt.title(f'Drawdown Analysis\nMax Drawdown: {max_dd:.2f} pts', 
                 fontsize=14, fontweight='bold')
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Drawdown (Points)', fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.legend()
        
        # Format x-axis dates
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        plt.savefig(self.vis_dir / f'drawdown_{self.timestamp}.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def create_win_loss_pie_chart(self, closed_trades: pd.DataFrame):
        """Create win/loss pie chart"""
        plt.figure(figsize=(8, 8))
        
        # Calculate win/loss statistics
        winning_trades = closed_trades[closed_trades['pnl_points'] > 0]
        losing_trades = closed_trades[closed_trades['pnl_points'] < 0]
        breakeven_trades = closed_trades[closed_trades['pnl_points'] == 0]
        
        labels = ['Winning Trades', 'Losing Trades']
        sizes = [len(winning_trades), len(losing_trades)]
        colors = ['#4CAF50', '#F44336']
        
        if len(breakeven_trades) > 0:
            labels.append('Breakeven Trades')
            sizes.append(len(breakeven_trades))
            colors.append('#FFC107')
        
        # Create pie chart
        wedges, texts, autotexts = plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%',
                                          startangle=90, shadow=False, explode=[0.05] * len(sizes))
        
        # Style the text
        for text in texts:
            text.set_fontsize(11)
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
            autotext.set_fontsize(10)
        
        # Add statistics
        win_rate = len(winning_trades) / len(closed_trades) * 100 if len(closed_trades) > 0 else 0
        plt.title(f'Trade Outcomes\nWin Rate: {win_rate:.1f}%', fontsize=14, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(self.vis_dir / f'win_loss_pie_{self.timestamp}.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def create_exit_reasons_chart(self, closed_trades: pd.DataFrame):
        """Create exit reasons bar chart"""
        if 'exit_reason' not in closed_trades.columns:
            return
        
        plt.figure(figsize=(10, 6))
        
        # Group by exit reason
        exit_counts = closed_trades['exit_reason'].value_counts()
        
        # Create bar chart
        bars = plt.bar(range(len(exit_counts)), exit_counts.values, 
                      color=plt.cm.Set3(np.linspace(0, 1, len(exit_counts))))
        
        # Add value labels on top of bars
        for i, (bar, count) in enumerate(zip(bars, exit_counts.values)):
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                    f'{count}\n({count/len(closed_trades)*100:.1f}%)',
                    ha='center', va='bottom', fontsize=9)
        
        # Formatting
        plt.title('Exit Reasons Distribution', fontsize=14, fontweight='bold')
        plt.xlabel('Exit Reason', fontsize=12)
        plt.ylabel('Count', fontsize=12)
        plt.xticks(range(len(exit_counts)), exit_counts.index, rotation=45, ha='right')
        plt.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        plt.savefig(self.vis_dir / f'exit_reasons_{self.timestamp}.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def create_direction_performance_chart(self, closed_trades: pd.DataFrame):
        """Create direction performance comparison chart"""
        plt.figure(figsize=(10, 6))
        
        # Group by direction
        direction_stats = closed_trades.groupby('direction').agg({
            'pnl_points': ['sum', 'mean', 'count'],
            'is_win': lambda x: (x == True).sum() / len(x) * 100
        }).round(2)
        
        direction_stats.columns = ['total_pnl', 'avg_pnl', 'count', 'win_rate']
        
        x = np.arange(len(direction_stats))
        width = 0.35
        
        # Create subplot
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8), gridspec_kw={'height_ratios': [2, 1]})
        
        # Bar chart for total P&L
        colors = ['#4CAF50' if val > 0 else '#F44336' for val in direction_stats['total_pnl']]
        ax1.bar(x, direction_stats['total_pnl'], width, color=colors, label='Total P&L')
        ax1.set_ylabel('Total P&L (Points)', fontsize=12)
        ax1.set_title('Performance by Trade Direction', fontsize=14, fontweight='bold')
        ax1.set_xticks(x)
        ax1.set_xticklabels(direction_stats.index)
        ax1.axhline(y=0, color='black', linewidth=0.5)
        ax1.grid(True, alpha=0.3, axis='y')
        
        # Add value labels
        for i, val in enumerate(direction_stats['total_pnl']):
            ax1.text(i, val + (1 if val >= 0 else -1), f'{val:+.1f}', 
                    ha='center', va='bottom' if val >= 0 else 'top', fontsize=10, fontweight='bold')
        
        # Line chart for win rate
        ax2.plot(x, direction_stats['win_rate'], 'o-', linewidth=2, markersize=8, 
                color='blue', label='Win Rate')
        ax2.set_xlabel('Direction', fontsize=12)
        ax2.set_ylabel('Win Rate (%)', fontsize=12)
        ax2.set_xticks(x)
        ax2.set_xticklabels(direction_stats.index)
        ax2.grid(True, alpha=0.3)
        ax2.set_ylim([0, 100])
        
        # Add value labels
        for i, val in enumerate(direction_stats['win_rate']):
            ax2.text(i, val + 2, f'{val:.1f}%', 
                    ha='center', va='bottom', fontsize=9)
        
        plt.tight_layout()
        plt.savefig(self.vis_dir / f'direction_performance_{self.timestamp}.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def create_monthly_performance_chart(self, closed_trades: pd.DataFrame):
        """Create monthly performance chart"""
        if 'exit_time' not in closed_trades.columns:
            return
        
        plt.figure(figsize=(12, 6))
        
        # Group by month
        closed_trades['month'] = closed_trades['exit_time'].dt.to_period('M').astype(str)
        monthly_stats = closed_trades.groupby('month').agg({
            'pnl_points': ['sum', 'mean', 'count'],
            'is_win': lambda x: (x == True).sum() / len(x) * 100
        }).round(2)
        
        monthly_stats.columns = ['total_pnl', 'avg_pnl', 'count', 'win_rate']
        
        x = np.arange(len(monthly_stats))
        
        # Create subplot
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), gridspec_kw={'height_ratios': [2, 1]})
        
        # Bar chart for monthly P&L
        colors = ['#4CAF50' if val > 0 else '#F44336' for val in monthly_stats['total_pnl']]
        ax1.bar(x, monthly_stats['total_pnl'], color=colors, alpha=0.7)
        ax1.set_ylabel('Monthly P&L (Points)', fontsize=12)
        ax1.set_title('Monthly Performance', fontsize=14, fontweight='bold')
        ax1.set_xticks(x)
        ax1.set_xticklabels(monthly_stats.index, rotation=45)
        ax1.axhline(y=0, color='black', linewidth=0.5)
        ax1.grid(True, alpha=0.3, axis='y')
        
        # Add value labels
        for i, val in enumerate(monthly_stats['total_pnl']):
            ax1.text(i, val + (1 if val >= 0 else -1), f'{val:+.0f}', 
                    ha='center', va='bottom' if val >= 0 else 'top', fontsize=9)
        
        # Line chart for win rate
        ax2.plot(x, monthly_stats['win_rate'], 'o-', linewidth=2, markersize=6, 
                color='blue', alpha=0.7)
        ax2.set_xlabel('Month', fontsize=12)
        ax2.set_ylabel('Win Rate (%)', fontsize=12)
        ax2.set_xticks(x)
        ax2.set_xticklabels(monthly_stats.index, rotation=45)
        ax2.grid(True, alpha=0.3)
        ax2.set_ylim([0, 100])
        
        # Add value labels
        for i, val in enumerate(monthly_stats['win_rate']):
            ax2.text(i, val + 2, f'{val:.0f}%', 
                    ha='center', va='bottom', fontsize=8)
        
        plt.tight_layout()
        plt.savefig(self.vis_dir / f'monthly_performance_{self.timestamp}.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def create_hourly_performance_chart(self, closed_trades: pd.DataFrame):
        """Create hourly performance heatmap"""
        if 'entry_time' not in closed_trades.columns:
            return
        
        plt.figure(figsize=(14, 6))
        
        # Group by hour
        closed_trades['hour'] = closed_trades['entry_time'].dt.hour
        hourly_stats = closed_trades.groupby('hour').agg({
            'pnl_points': ['sum', 'mean', 'count'],
            'is_win': lambda x: (x == True).sum() / len(x) * 100
        }).round(2)
        
        hourly_stats.columns = ['total_pnl', 'avg_pnl', 'count', 'win_rate']
        hourly_stats = hourly_stats.reindex(range(24), fill_value=0)
        
        # Create heatmap data
        hours = list(range(24))
        metrics = ['total_pnl', 'avg_pnl', 'count', 'win_rate']
        
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        axes = axes.flatten()
        
        titles = ['Total P&L by Hour', 'Average P&L by Hour', 'Number of Trades by Hour', 'Win Rate by Hour']
        cmaps = ['RdYlGn', 'RdYlGn', 'Blues', 'RdYlGn']
        
        for idx, (ax, metric, title, cmap) in enumerate(zip(axes, metrics, titles, cmaps)):
            data = hourly_stats[metric].values
            
            # Create bar chart for each metric
            colors = []
            if metric in ['total_pnl', 'avg_pnl']:
                colors = ['#4CAF50' if val > 0 else '#F44336' if val < 0 else '#FFC107' for val in data]
            elif metric == 'count':
                colors = plt.cm.Blues(data / max(data) if max(data) > 0 else 0)
            else:  # win_rate
                colors = plt.cm.RdYlGn(data / 100)
            
            bars = ax.bar(hours, data, color=colors, edgecolor='black')
            ax.set_title(title, fontsize=12, fontweight='bold')
            ax.set_xlabel('Hour', fontsize=10)
            ax.set_ylabel(metric.replace('_', ' ').title(), fontsize=10)
            ax.set_xticks(range(0, 24, 2))
            ax.grid(True, alpha=0.3, axis='y')
            
            # Add value labels for significant values
            for i, val in enumerate(data):
                if val != 0 and (metric != 'count' or val > 0):
                    ax.text(i, val + (0.1 * max(abs(data)) if max(abs(data)) > 0 else 0.1), 
                           f'{val:.1f}' if metric != 'count' else f'{int(val)}',
                           ha='center', va='bottom', fontsize=8)
        
        plt.suptitle('Hourly Performance Analysis', fontsize=16, fontweight='bold', y=1.02)
        plt.tight_layout()
        plt.savefig(self.vis_dir / f'hourly_performance_{self.timestamp}.png', dpi=150, bbox_inches='tight')
        plt.close()
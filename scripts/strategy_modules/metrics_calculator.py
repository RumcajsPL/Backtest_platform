"""
Performance Metrics Calculator Module
"""
import pandas as pd
import numpy as np
from typing import Dict, Optional

def calculate_performance_metrics(trades_df: pd.DataFrame, ohlcv_df: Optional[pd.DataFrame] = None) -> Dict:
    """Calculate comprehensive performance metrics from trades including spread analysis"""
    if trades_df.empty:
        return {
            'total_trades': 0,
            'message': 'No trades to analyze',
            'spread_analysis': {
                'total_spread_cost': 0,
                'avg_spread_per_trade': 0,
                'spread_impact_on_pnl': 0
            }
        }
    
    # Filter by status
    closed_trades = trades_df[trades_df['status'] == 'CLOSED'].copy()
    open_trades = trades_df[trades_df['status'] == 'OPEN']
    rejected_trades = trades_df[trades_df['status'] == 'REJECTED']
    
    metrics = {
        # Basic counts
        'total_signals': len(trades_df),
        'total_trades': len(closed_trades),
        'open_trades': len(open_trades),
        'rejected_trades': len(rejected_trades),
        'winning_trades': len(closed_trades[closed_trades['pnl_points'] > 0]),
        'losing_trades': len(closed_trades[closed_trades['pnl_points'] < 0]),
        'breakeven_trades': len(closed_trades[closed_trades['pnl_points'] == 0]),
        
        # Win rates
        'win_rate': len(closed_trades[closed_trades['pnl_points'] > 0]) / len(closed_trades) * 100 if len(closed_trades) > 0 else 0,
        'loss_rate': len(closed_trades[closed_trades['pnl_points'] < 0]) / len(closed_trades) * 100 if len(closed_trades) > 0 else 0,
        
        # P&L metrics
        'total_pnl_points': closed_trades['pnl_points'].sum() if not closed_trades.empty else 0,
        'total_pnl_percent': closed_trades['pnl_percent'].sum() if not closed_trades.empty else 0,
        'avg_pnl_points': closed_trades['pnl_points'].mean() if not closed_trades.empty else 0,
        'avg_pnl_percent': closed_trades['pnl_percent'].mean() if not closed_trades.empty else 0,
        'avg_win_points': closed_trades[closed_trades['pnl_points'] > 0]['pnl_points'].mean() if len(closed_trades[closed_trades['pnl_points'] > 0]) > 0 else 0,
        'avg_loss_points': closed_trades[closed_trades['pnl_points'] < 0]['pnl_points'].mean() if len(closed_trades[closed_trades['pnl_points'] < 0]) > 0 else 0,
        'largest_win': closed_trades['pnl_points'].max() if not closed_trades.empty else 0,
        'largest_loss': closed_trades['pnl_points'].min() if not closed_trades.empty else 0,
        
        # Risk metrics
        'profit_factor': 0,
        'expectancy_points': 0,
        'sharpe_ratio': 0,
        
        # Duration metrics
        'avg_duration_minutes': closed_trades['duration_minutes'].mean() if not closed_trades.empty else 0,
        'avg_duration_bars': closed_trades['duration_bars'].mean() if not closed_trades.empty else 0,
        'avg_win_duration': closed_trades[closed_trades['pnl_points'] > 0]['duration_minutes'].mean() if len(closed_trades[closed_trades['pnl_points'] > 0]) > 0 else 0,
        'avg_loss_duration': closed_trades[closed_trades['pnl_points'] < 0]['duration_minutes'].mean() if len(closed_trades[closed_trades['pnl_points'] < 0]) > 0 else 0,
        
        # Exit analysis
        'exit_reasons': {},
        'long_short_breakdown': {},
        'monthly_performance': {},
        'drawdown_analysis': {},
        
        # Trade quality
        'avg_risk_reward_realized': 0,
        'avg_sl_distance': closed_trades['sl_distance'].mean() if not closed_trades.empty else 0,
        'avg_tp_distance': closed_trades['tp_distance'].mean() if not closed_trades.empty else 0,
        'avg_risk_reward_planned': closed_trades['risk_reward_ratio'].mean() if not closed_trades.empty else 0,
    }
    
    # Calculate profit factor
    if not closed_trades.empty:
        win_sum = closed_trades[closed_trades['pnl_points'] > 0]['pnl_points'].sum()
        loss_sum = abs(closed_trades[closed_trades['pnl_points'] < 0]['pnl_points'].sum())
        if loss_sum > 0:
            metrics['profit_factor'] = win_sum / loss_sum
    
    # Calculate expectancy
    if len(closed_trades) > 0:
        win_rate = metrics['win_rate'] / 100
        avg_win = metrics['avg_win_points']
        avg_loss = metrics['avg_loss_points']
        metrics['expectancy_points'] = (win_rate * avg_win) - ((1 - win_rate) * abs(avg_loss))
    
    # Exit reasons breakdown
    if not closed_trades.empty and 'exit_reason' in closed_trades.columns:
        exit_counts = closed_trades['exit_reason'].value_counts().to_dict()
        metrics['exit_reasons'] = {k: int(v) for k, v in exit_counts.items()}
    
    # Long/Short breakdown
    if not closed_trades.empty:
        long_trades = closed_trades[closed_trades['direction'] == 'BUY']
        short_trades = closed_trades[closed_trades['direction'] == 'SELL']
        
        metrics['long_short_breakdown'] = {
            'long_trades': len(long_trades),
            'short_trades': len(short_trades),
            'long_win_rate': len(long_trades[long_trades['pnl_points'] > 0]) / len(long_trades) * 100 if len(long_trades) > 0 else 0,
            'short_win_rate': len(short_trades[short_trades['pnl_points'] > 0]) / len(short_trades) * 100 if len(short_trades) > 0 else 0,
            'long_pnl_points': long_trades['pnl_points'].sum() if not long_trades.empty else 0,
            'short_pnl_points': short_trades['pnl_points'].sum() if not short_trades.empty else 0,
            'long_avg_pnl': long_trades['pnl_points'].mean() if not long_trades.empty else 0,
            'short_avg_pnl': short_trades['pnl_points'].mean() if not short_trades.empty else 0,
        }
    
    # Monthly performance
    if not closed_trades.empty and 'exit_time' in closed_trades.columns:
        try:
            closed_trades['month'] = pd.to_datetime(closed_trades['exit_time']).dt.to_period('M')
            monthly = closed_trades.groupby('month').agg({
                'pnl_points': 'sum',
                'trade_id': 'count'
            }).rename(columns={'trade_id': 'trades'})
            metrics['monthly_performance'] = {str(k): v.to_dict() for k, v in monthly.iterrows()}
        except:
            metrics['monthly_performance'] = {}
    
      # Spread impact metrics
        if 'spread_cost_points' in trades_df.columns:
            closed_with_spread = closed_trades[closed_trades['spread_cost_points'] > 0]
            metrics['spread_analysis'] = {
                'total_spread_cost_points': closed_trades['spread_cost_points'].sum() if not closed_trades.empty else 0,
                'avg_spread_per_trade_points': closed_trades['spread_cost_points'].mean() if not closed_trades.empty else 0,
                'max_spread_points': closed_trades['spread_cost_points'].max() if not closed_trades.empty else 0,
                'min_spread_points': closed_trades['spread_cost_points'].min() if not closed_trades.empty else 0,
                'trades_with_spread': len(closed_with_spread),
                'spread_penetration_rate': len(closed_with_spread) / len(closed_trades) * 100 if len(closed_trades) > 0 else 0,
                'spread_impact_on_pnl': closed_trades['spread_cost_points'].sum() / abs(closed_trades['pnl_points'].sum()) * 100 if closed_trades['pnl_points'].sum() != 0 else 0,
                'net_pnl_after_spread': closed_trades['pnl_points'].sum() - closed_trades['spread_cost_points'].sum() if not closed_trades.empty else 0,
                'spread_efficiency_analysis': {
                    'avg_spread_to_sl_ratio': (closed_trades['spread_cost_points'] / closed_trades['sl_distance']).mean() if not closed_trades.empty and closed_trades['sl_distance'].mean() > 0 else 0,
                    'avg_spread_to_tp_ratio': (closed_trades['spread_cost_points'] / closed_trades['tp_distance']).mean() if not closed_trades.empty and closed_trades['tp_distance'].mean() > 0 else 0,
                    'profitable_despite_spread': len(closed_trades[(closed_trades['pnl_points'] > 0) & (closed_trades['spread_cost_points'] > 0)]) if not closed_trades.empty else 0
                }
            }
            
            # Spread impact by direction
            if not closed_trades.empty:
                long_spread = closed_trades[closed_trades['direction'] == 'BUY']['spread_cost_points'].sum() if len(closed_trades[closed_trades['direction'] == 'BUY']) > 0 else 0
                short_spread = closed_trades[closed_trades['direction'] == 'SELL']['spread_cost_points'].sum() if len(closed_trades[closed_trades['direction'] == 'SELL']) > 0 else 0
                metrics['spread_analysis']['spread_by_direction'] = {
                    'long_total_spread': long_spread,
                    'short_total_spread': short_spread,
                    'long_avg_spread': closed_trades[closed_trades['direction'] == 'BUY']['spread_cost_points'].mean() if len(closed_trades[closed_trades['direction'] == 'BUY']) > 0 else 0,
                    'short_avg_spread': closed_trades[closed_trades['direction'] == 'SELL']['spread_cost_points'].mean() if len(closed_trades[closed_trades['direction'] == 'SELL']) > 0 else 0
                }
        else:
            metrics['spread_analysis'] = {
                'total_spread_cost': 0,
                'avg_spread_per_trade': 0,
                'spread_enabled': False,
                'message': 'No spread data available'
            }
        
        # Enhanced profit factor considering spread costs
        if not closed_trades.empty and 'spread_cost_points' in closed_trades.columns:
            win_sum_net = closed_trades[closed_trades['pnl_points'] > 0]['pnl_points'].sum() - closed_trades[closed_trades['pnl_points'] > 0]['spread_cost_points'].sum()
            loss_sum_net = abs(closed_trades[closed_trades['pnl_points'] < 0]['pnl_points'].sum()) + closed_trades[closed_trades['pnl_points'] < 0]['spread_cost_points'].sum()
            
            if loss_sum_net > 0:
                metrics['profit_factor_net_spread'] = win_sum_net / loss_sum_net
                metrics['profit_factor_gross'] = metrics['profit_factor']
                metrics['spread_impact_on_profit_factor'] = metrics['profit_factor'] - metrics['profit_factor_net_spread']
        
        # Spread-adjusted Sharpe ratio
        if len(closed_trades) > 1 and 'spread_cost_points' in closed_trades.columns:
            net_returns = (closed_trades['pnl_points'] - closed_trades['spread_cost_points']) / closed_trades['sl_distance'].clip(lower=0.1)
            if net_returns.std() > 0:
                metrics['sharpe_ratio_net_spread'] = net_returns.mean() / net_returns.std()
                metrics['spread_impact_on_sharpe'] = metrics.get('sharpe_ratio', 0) - metrics.get('sharpe_ratio_net_spread', 0)
        
        return metrics
    
    # Calculate realized risk:reward
    if not closed_trades.empty:
        winning = closed_trades[closed_trades['pnl_points'] > 0]
        if len(winning) > 0 and winning['sl_distance'].mean() > 0:
            metrics['avg_risk_reward_realized'] = winning['tp_distance'].mean() / winning['sl_distance'].mean()
    
    # Drawdown analysis
    if not closed_trades.empty:
        closed_trades_sorted = closed_trades.sort_values('exit_time').copy()
        closed_trades_sorted['cumulative_pnl'] = closed_trades_sorted['pnl_points'].cumsum()
        closed_trades_sorted['running_max'] = closed_trades_sorted['cumulative_pnl'].cummax()
        closed_trades_sorted['drawdown'] = closed_trades_sorted['cumulative_pnl'] - closed_trades_sorted['running_max']
        
        metrics['drawdown_analysis'] = {
            'max_drawdown_points': closed_trades_sorted['drawdown'].min(),
            'max_drawdown_percent': (closed_trades_sorted['drawdown'].min() / closed_trades_sorted['running_max'].max() * 100) if closed_trades_sorted['running_max'].max() > 0 else 0,
            'recovery_factor': abs(closed_trades_sorted['pnl_points'].sum() / closed_trades_sorted['drawdown'].min()) if closed_trades_sorted['drawdown'].min() < 0 else float('inf')
        }
    
    # Rejection analysis
    if len(rejected_trades) > 0 and 'reject_reason' in rejected_trades.columns:
        rejection_counts = rejected_trades['reject_reason'].value_counts().to_dict()
        metrics['rejection_analysis'] = {
            'total_rejected': len(rejected_trades),
            'rejection_rate': len(rejected_trades) / len(trades_df) * 100,
            'rejection_reasons': {k: int(v) for k, v in rejection_counts.items()}
        }
           
    return metrics
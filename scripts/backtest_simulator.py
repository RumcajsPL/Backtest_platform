#!/usr/bin/env python
"""
Backtest simulator for calculating real trading metrics
"""
import pandas as pd
from typing import Dict, List

class TradeSimulator:
    """Simulate trades to calculate real performance metrics"""
    
    def __init__(self):
        self.trades = []
    
    def simulate_trades(self, trades_df: pd.DataFrame, ohlcv_df: pd.DataFrame) -> List[Dict]:
        """
        Simulate trade execution and calculate actual P&L.
        
        NOTE: Uses 'Pessimistic' execution logic (checks SL before TP) to avoid
        look-ahead bias on bars that hit both levels.
        """
        completed_trades = []
        
        # Ensure OHLCV is sorted
        if not ohlcv_df.index.is_monotonic_increasing:
            ohlcv_df = ohlcv_df.sort_index()

        for _, trade in trades_df.iterrows():
            try:
                # Parse trade details
                entry_time = pd.to_datetime(trade['timestamp'])
                entry_price = float(trade['entry'])
                sl_price = float(trade['sl'])
                tp_price = float(trade['tp'])
                direction = trade['signal']
                
                # Find entry index in OHLCV data
                # Use 'searchsorted' for faster indexing than get_indexer
                if entry_time not in ohlcv_df.index:
                    # If exact time missing, find nearest future bar (entry at Open of next available)
                    entry_idx = ohlcv_df.index.searchsorted(entry_time)
                else:
                    entry_idx = ohlcv_df.index.get_loc(entry_time)
                
                if isinstance(entry_idx, slice): # Handle duplicates if any
                    entry_idx = entry_idx.start
                
                if entry_idx >= len(ohlcv_df) - 1:
                    continue
                
                # Simulate forward from entry
                exit_time = None
                exit_price = None
                exit_reason = "END_OF_DATA"
                pnl_points = 0
                duration_bars = 0
                
                # Iterate bars starting from the one AFTER signal (or same if Entering on Open)
                # Assuming signal is Close of bar N, Entry is Open of bar N+1. 
                # We check High/Low of bar N+1 onwards.
                for i in range(entry_idx + 1, len(ohlcv_df)):
                    bar = ohlcv_df.iloc[i]
                    current_time = ohlcv_df.index[i]
                    
                    # PESSIMISTIC EXECUTION: Check Stop Loss FIRST
                    # If a bar hits both SL and TP, assume we got stopped out.
                    
                    if direction == 'BUY':
                        # 1. Check SL (Low touches SL)
                        if bar['low'] <= sl_price:
                            exit_time = current_time
                            exit_price = sl_price
                            exit_reason = "STOP_LOSS"
                            pnl_points = sl_price - entry_price
                            break
                        # 2. Check TP (High touches TP)
                        elif bar['high'] >= tp_price:
                            exit_time = current_time
                            exit_price = tp_price
                            exit_reason = "TAKE_PROFIT"
                            pnl_points = tp_price - entry_price
                            break
                            
                    else:  # SELL
                        # 1. Check SL (High touches SL)
                        if bar['high'] >= sl_price:
                            exit_time = current_time
                            exit_price = sl_price
                            exit_reason = "STOP_LOSS"
                            pnl_points = entry_price - sl_price
                            break
                        # 2. Check TP (Low touches TP)
                        elif bar['low'] <= tp_price:
                            exit_time = current_time
                            exit_price = tp_price
                            exit_reason = "TAKE_PROFIT"
                            pnl_points = entry_price - tp_price
                            break
                
                # If no exit found, exit at end of data
                if exit_time is None:
                    exit_time = ohlcv_df.index[-1]
                    exit_price = ohlcv_df.iloc[-1]['close']
                    exit_reason = "END_OF_DATA"
                    if direction == 'BUY':
                        pnl_points = exit_price - entry_price
                    else:
                        pnl_points = entry_price - exit_price
                    duration_bars = len(ohlcv_df) - entry_idx - 1
                else:
                    duration_bars = ohlcv_df.index.get_loc(exit_time) - entry_idx

                # Calculate duration in minutes
                duration_minutes = (exit_time - entry_time).total_seconds() / 60
                
                completed_trades.append({
                    'entry_time': entry_time,
                    'exit_time': exit_time,
                    'direction': direction,
                    'entry_price': entry_price,
                    'exit_price': exit_price,
                    'sl_price': sl_price,
                    'tp_price': tp_price,
                    'exit_reason': exit_reason,
                    'pnl_points': pnl_points,
                    'duration_bars': duration_bars,
                    'duration_minutes': duration_minutes,
                    'is_win': pnl_points > 0,
                    'is_loss': pnl_points < 0,
                    'sl_distance': abs(entry_price - sl_price),
                    'tp_distance': abs(tp_price - entry_price),
                })
                
            except Exception as e:
                # print(f"⚠️  Error simulating trade at {trade.get('timestamp')}: {e}")
                continue
        
        return completed_trades
    
    def calculate_metrics(self, trades: List[Dict]) -> Dict:
        """Calculate comprehensive performance metrics"""
        if not trades:
            return {}
        
        df = pd.DataFrame(trades)
        
        long_trades = df[df['direction'] == 'BUY']
        short_trades = df[df['direction'] == 'SELL']
        
        winning_trades = df[df['pnl_points'] > 0]
        losing_trades = df[df['pnl_points'] < 0]
        
        metrics = {
            'total_trades': len(df),
            'winning_trades': len(winning_trades),
            'losing_trades': len(losing_trades),
            'win_rate': (len(winning_trades) / len(df) * 100) if len(df) > 0 else 0,
            
            # P&L
            'total_pnl_points': df['pnl_points'].sum(),
            'avg_pnl_points': df['pnl_points'].mean(),
            'avg_win_points': winning_trades['pnl_points'].mean() if len(winning_trades) > 0 else 0,
            'avg_loss_points': losing_trades['pnl_points'].mean() if len(losing_trades) > 0 else 0,
            
            # Long/Short breakdown
            'long_trades': len(long_trades),
            'short_trades': len(short_trades),
            'long_pnl_points': long_trades['pnl_points'].sum(),
            'short_pnl_points': short_trades['pnl_points'].sum(),
            'long_win_rate': (len(long_trades[long_trades['pnl_points']>0])/len(long_trades)*100) if len(long_trades)>0 else 0,
            'short_win_rate': (len(short_trades[short_trades['pnl_points']>0])/len(short_trades)*100) if len(short_trades)>0 else 0,

            # Risk Metrics
            'profit_factor': (winning_trades['pnl_points'].sum() / abs(losing_trades['pnl_points'].sum())) if len(losing_trades) > 0 else float('inf'),
            'avg_risk_reward': (df['tp_distance'] / df['sl_distance']).mean() if len(df) > 0 else 0,
            'largest_win': df['pnl_points'].max() if len(df) > 0 else 0,
            'largest_loss': df['pnl_points'].min() if len(df) > 0 else 0,
            
            # Duration
            'avg_duration_minutes': df['duration_minutes'].mean(),
            'avg_duration_bars': df['duration_bars'].mean(),
            'avg_win_duration': winning_trades['duration_minutes'].mean() if len(winning_trades) > 0 else 0,
            'avg_loss_duration': losing_trades['duration_minutes'].mean() if len(losing_trades) > 0 else 0,
            
            # Exits
            'take_profit_exits': len(df[df['exit_reason'] == 'TAKE_PROFIT']),
            'stop_loss_exits': len(df[df['exit_reason'] == 'STOP_LOSS']),
            'end_of_data_exits': len(df[df['exit_reason'] == 'END_OF_DATA']),
            
            # Streaks
            'max_consecutive_wins': self._max_consecutive(df['pnl_points'] > 0, True),
            'max_consecutive_losses': self._max_consecutive(df['pnl_points'] < 0, True),
        }
        
        # Expectancy
        win_rate = metrics['win_rate'] / 100
        metrics['expectancy_points'] = (win_rate * metrics['avg_win_points']) + ((1 - win_rate) * metrics['avg_loss_points'])
        
        # Sharpe (simplified)
        if len(df) > 1:
            returns = df['pnl_points'] / df['sl_distance']
            metrics['sharpe_ratio'] = returns.mean() / returns.std() if returns.std() > 0 else 0
        else:
            metrics['sharpe_ratio'] = 0
            
        return metrics
    
    def _max_consecutive(self, sequence, value):
        max_count = current_count = 0
        for item in sequence:
            if item == value:
                current_count += 1
                max_count = max(max_count, current_count)
            else:
                current_count = 0
        return max_count
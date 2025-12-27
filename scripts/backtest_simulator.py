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
        Simulate trade execution and calculate actual P&L
        
        Args:
            trades_df: DataFrame with trade details (entry, sl, tp)
            ohlcv_df: OHLCV data for price simulation
        
        Returns:
            List of completed trades with P&L and duration
        """
        completed_trades = []
        
        for _, trade in trades_df.iterrows():
            try:
                # Parse trade details
                entry_time = pd.to_datetime(trade['timestamp'])
                entry_price = float(trade['entry'])
                sl_price = float(trade['sl'])
                tp_price = float(trade['tp'])
                direction = trade['signal']
                
                # Find entry index in OHLCV data
                entry_idx = ohlcv_df.index.get_indexer([entry_time], method='nearest')[0]
                
                if entry_idx >= len(ohlcv_df) - 1:
                    # Entry at end of data
                    continue
                
                # Simulate forward from entry
                exit_time = None
                exit_price = None
                exit_reason = "END_OF_DATA"
                pnl_points = 0
                duration_bars = 0
                
                for i in range(entry_idx + 1, len(ohlcv_df)):
                    bar = ohlcv_df.iloc[i]
                    current_time = ohlcv_df.index[i]
                    duration_bars = i - entry_idx
                    
                    # Check exit conditions
                    if direction == 'BUY':
                        # Check TP (high touches TP)
                        if bar['high'] >= tp_price:
                            exit_time = current_time
                            exit_price = tp_price
                            exit_reason = "TAKE_PROFIT"
                            pnl_points = tp_price - entry_price
                            break
                        # Check SL (low touches SL)
                        elif bar['low'] <= sl_price:
                            exit_time = current_time
                            exit_price = sl_price
                            exit_reason = "STOP_LOSS"
                            pnl_points = sl_price - entry_price
                            break
                    else:  # SELL
                        # Check TP (low touches TP)
                        if bar['low'] <= tp_price:
                            exit_time = current_time
                            exit_price = tp_price
                            exit_reason = "TAKE_PROFIT"
                            pnl_points = entry_price - tp_price
                            break
                        # Check SL (high touches SL)
                        elif bar['high'] >= sl_price:
                            exit_time = current_time
                            exit_price = sl_price
                            exit_reason = "STOP_LOSS"
                            pnl_points = entry_price - sl_price
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
                print(f"⚠️  Error simulating trade: {e}")
                continue
        
        return completed_trades
    
    def calculate_metrics(self, trades: List[Dict]) -> Dict:
        """Calculate comprehensive performance metrics"""
        if not trades:
            return {}
        
        # Convert to DataFrame for easier calculations
        df = pd.DataFrame(trades)
        
        # Separate long and short trades
        long_trades = df[df['direction'] == 'BUY']
        short_trades = df[df['direction'] == 'SELL']
        
        # Winning/losing trades
        winning_trades = df[df['pnl_points'] > 0]
        losing_trades = df[df['pnl_points'] < 0]
        
        long_wins = long_trades[long_trades['pnl_points'] > 0]
        short_wins = short_trades[short_trades['pnl_points'] > 0]
        
        # Basic counts
        metrics = {
            'total_trades': len(df),
            'long_trades': len(long_trades),
            'short_trades': len(short_trades),
            'winning_trades': len(winning_trades),
            'losing_trades': len(losing_trades),
            'breakeven_trades': len(df[df['pnl_points'] == 0]),
        }
        
        # Win rates
        metrics['win_rate'] = (len(winning_trades) / len(df) * 100) if len(df) > 0 else 0
        metrics['long_win_rate'] = (len(long_wins) / len(long_trades) * 100) if len(long_trades) > 0 else 0
        metrics['short_win_rate'] = (len(short_wins) / len(short_trades) * 100) if len(short_trades) > 0 else 0
        
        # P&L metrics
        metrics['total_pnl_points'] = df['pnl_points'].sum()
        metrics['long_pnl_points'] = long_trades['pnl_points'].sum()
        metrics['short_pnl_points'] = short_trades['pnl_points'].sum()
        metrics['avg_pnl_points'] = df['pnl_points'].mean()
        metrics['avg_win_points'] = winning_trades['pnl_points'].mean() if len(winning_trades) > 0 else 0
        metrics['avg_loss_points'] = losing_trades['pnl_points'].mean() if len(losing_trades) > 0 else 0
        
        # Profit factor
        gross_profit = winning_trades['pnl_points'].sum()
        gross_loss = abs(losing_trades['pnl_points'].sum())
        metrics['profit_factor'] = gross_profit / gross_loss if gross_loss > 0 else float('inf')
        
        # Risk/Reward metrics
        metrics['avg_risk_reward'] = (df['tp_distance'] / df['sl_distance']).mean() if len(df) > 0 else 0
        
        # Trade duration
        metrics['avg_duration_minutes'] = df['duration_minutes'].mean()
        metrics['avg_duration_bars'] = df['duration_bars'].mean()
        metrics['avg_win_duration'] = winning_trades['duration_minutes'].mean() if len(winning_trades) > 0 else 0
        metrics['avg_loss_duration'] = losing_trades['duration_minutes'].mean() if len(losing_trades) > 0 else 0
        
        # Exit statistics
        exit_reasons = df['exit_reason'].value_counts()
        for reason in ['TAKE_PROFIT', 'STOP_LOSS', 'END_OF_DATA']:
            metrics[f'{reason.lower()}_exits'] = exit_reasons.get(reason, 0)
        
        # Consecutive wins/losses
        win_sequence = [1 if pnl > 0 else 0 for pnl in df['pnl_points']]
        metrics['max_consecutive_wins'] = self._max_consecutive(win_sequence, 1)
        metrics['max_consecutive_losses'] = self._max_consecutive(win_sequence, 0)
        
        # Sharpe-like ratio (simplified)
        if len(df) > 1:
            returns = df['pnl_points'] / df['sl_distance']  # Normalize by risk
            metrics['sharpe_ratio'] = returns.mean() / returns.std() if returns.std() > 0 else 0
        else:
            metrics['sharpe_ratio'] = 0
        
        # Expectancy
        win_rate = metrics['win_rate'] / 100
        metrics['expectancy_points'] = (win_rate * metrics['avg_win_points']) + ((1 - win_rate) * metrics['avg_loss_points'])
        
        # Largest win/loss
        metrics['largest_win'] = df['pnl_points'].max() if len(df) > 0 else 0
        metrics['largest_loss'] = df['pnl_points'].min() if len(df) > 0 else 0
        
        return metrics
    
    def _max_consecutive(self, sequence, value):
        """Calculate maximum consecutive occurrences of a value"""
        max_count = current_count = 0
        for item in sequence:
            if item == value:
                current_count += 1
                max_count = max(max_count, current_count)
            else:
                current_count = 0
        return max_count
"""
Backtest simulator for calculating real trading metrics
Enhanced with Trade Manager integration for position control
"""
import pandas as pd
import sys
from pathlib import Path
from typing import Dict, List

# Add src to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root / 'src'))

from strategies.trade_management.trade_manager import TradeManager

class TradeSimulator:
    """Simulate trades to calculate real performance metrics"""
    
    def __init__(self, config: Dict = None):
        """
        Initialize TradeSimulator with optional configuration.
        
        Args:
            config: Full strategy configuration (for TradeManager)
        """
        self.trades = []
        self.config = config or {}
        
        # Initialize TradeManager if config provided
        self.trade_manager = None
        if config:
            self.trade_manager = TradeManager(config)
    
    def simulate_trades(self, trades_df: pd.DataFrame, ohlcv_df: pd.DataFrame) -> List[Dict]:
        """
        Simulate trade execution and calculate actual P&L.
        
        Enhanced with Trade Manager for position control:
        - Enforces pyramiding limits
        - Handles close-on-opposite logic
        - Tracks rejected signals
        
        NOTE: Uses 'Pessimistic' execution logic (checks SL before TP) to avoid
        look-ahead bias on bars that hit both levels.
        """
        completed_trades = []
        rejected_signals = []
        
        # Ensure OHLCV is sorted
        if not ohlcv_df.index.is_monotonic_increasing:
            ohlcv_df = ohlcv_df.sort_index()
        
        # Reset trade manager for new simulation
        if self.trade_manager:
            self.trade_manager.reset()

        for _, trade_signal in trades_df.iterrows():
            try:
                # === TRADE MANAGER INTEGRATION ===
                if self.trade_manager:
                    action_result = self.trade_manager.handle_signal(trade_signal)
                    
                    # Handle REJECT
                    if action_result['action'] == 'REJECT':
                        rejected_signals.append({
                            'timestamp': trade_signal['timestamp'],
                            'signal': trade_signal['signal'],
                            'reason': action_result['reason'],
                        })
                        continue
                    
                    # Handle CLOSE_AND_REVERSE
                    if action_result['action'] == 'CLOSE_AND_REVERSE':
                        # Add the closed trade to results
                        if action_result['close_trade']:
                            completed_trades.append(action_result['close_trade'])
                        
                        # The new position will be opened below
                        trade_signal = pd.Series(action_result['open_trade'])
                    
                    # Handle OPEN (or open part of CLOSE_AND_REVERSE)
                    # Continue to normal trade simulation
                
                # === STANDARD TRADE SIMULATION ===
                # Parse trade details
                entry_time = pd.to_datetime(trade_signal['timestamp'])
                entry_price = float(trade_signal['entry'])
                sl_price = float(trade_signal['sl'])
                tp_price = float(trade_signal['tp'])
                direction = trade_signal['signal']
                
                # Find entry index in OHLCV data
                if entry_time not in ohlcv_df.index:
                    entry_idx = ohlcv_df.index.searchsorted(entry_time)
                else:
                    entry_idx = ohlcv_df.index.get_loc(entry_time)
                
                if isinstance(entry_idx, slice):
                    entry_idx = entry_idx.start
                
                if entry_idx >= len(ohlcv_df) - 1:
                    continue
                
                # Simulate forward from entry
                exit_time = None
                exit_price = None
                exit_reason = "END_OF_DATA"
                pnl_points = 0
                duration_bars = 0
                
                # Iterate bars starting from the one AFTER signal
                for i in range(entry_idx + 1, len(ohlcv_df)):
                    bar = ohlcv_df.iloc[i]
                    current_time = ohlcv_df.index[i]
                    
                    # PESSIMISTIC EXECUTION: Check Stop Loss FIRST
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

                # Notify trade manager of position closure
                if self.trade_manager:
                    self.trade_manager.close_position_on_exit(exit_time, exit_price, exit_reason)

                # Calculate duration in minutes
                duration_minutes = (exit_time - entry_time).total_seconds() / 60
                
                trade_record = {
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
                }
                
                # Add trade_id if available from trade manager
                if self.trade_manager and 'trade_id' in trade_signal:
                    trade_record['trade_id'] = trade_signal['trade_id']
                
                completed_trades.append(trade_record)
                
            except Exception as e:
                # print(f"⚠️  Error simulating trade at {trade_signal.get('timestamp')}: {e}")
                continue
        
        # Store rejected signals for reporting
        self.rejected_signals = rejected_signals
        
        return completed_trades
    
    def calculate_metrics(self, trades: List[Dict]) -> Dict:
        """Calculate comprehensive performance metrics"""
        if not trades:
            metrics = {'total_trades': 0}
            # Add trade manager metrics if available
            if self.trade_manager:
                metrics['trade_manager'] = self.trade_manager.get_metrics()
            return metrics
        
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
            'opposite_signal_exits': len(df[df['exit_reason'] == 'OPPOSITE_SIGNAL']),
            
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
        
        # Add trade manager metrics
        if self.trade_manager:
            metrics['trade_manager'] = self.trade_manager.get_metrics()
            
        return metrics
    
    def get_rejected_signals(self) -> List[Dict]:
        """Get list of rejected signals with reasons"""
        return getattr(self, 'rejected_signals', [])
    
    def _max_consecutive(self, sequence, value):
        max_count = current_count = 0
        for item in sequence:
            if item == value:
                current_count += 1
                max_count = max(max_count, current_count)
            else:
                current_count = 0
        return max_count
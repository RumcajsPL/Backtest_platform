"""
WBWS Strategy Runner - Enhanced with Complete Trade Simulation and Metrics
FIXED VERSION: Proper TradeManager integration with SL/TP exits
"""
import sys
import pandas as pd
import numpy as np
import yaml
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional

# Get project root
project_root = Path(__file__).resolve().parent.parent

# Add to Python path
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
if str(project_root / 'src') not in sys.path:
    sys.path.insert(0, str(project_root / 'src'))


class TradeTracker:
    """Track trades with unique IDs and complete history"""
    def __init__(self):
        self.trades = []
        self.trade_counter = 0
        self.position_counter = 0
        self.open_positions = {}
        self.trade_manager = None
        
    def set_trade_manager(self, trade_manager):
        """Set trade manager for position control"""
        self.trade_manager = trade_manager
        
    def open_position(self, timestamp, direction, entry_price, sl_price, tp_price, 
                     comment="", trade_manager_action=None, trade_manager_trade_id=None):
        """Open a new position"""
        self.trade_counter += 1
        
        # Determine position ID based on pyramiding rules
        position_id = None
        if self.trade_manager and not self.trade_manager.pyramiding_enabled:
            # If pyramiding disabled, all trades in same direction share position ID
            if direction in self.open_positions:
                position_id = self.open_positions[direction]
            else:
                self.position_counter += 1
                position_id = self.position_counter
                self.open_positions[direction] = position_id
        else:
            # Each trade gets its own position ID (unlimited pyramiding)
            self.position_counter += 1
            position_id = self.position_counter
            if direction not in self.open_positions:
                self.open_positions[direction] = []
            self.open_positions[direction].append(position_id)
        
        trade = {
            'trade_id': self.trade_counter,
            'trade_manager_trade_id': trade_manager_trade_id,
            'position_id': position_id,
            'status': 'OPEN',
            'entry_time': timestamp,
            'exit_time': None,
            'direction': direction,
            'entry_price': entry_price,
            'exit_price': None,
            'sl_price': sl_price,
            'tp_price': tp_price,
            'exit_reason': None,
            'pnl_points': 0,
            'pnl_percent': 0,
            'duration_bars': 0,
            'duration_minutes': 0,
            'sl_distance': abs(entry_price - sl_price),
            'tp_distance': abs(tp_price - entry_price),
            'risk_reward_ratio': abs(tp_price - entry_price) / abs(entry_price - sl_price) if abs(entry_price - sl_price) > 0 else 0,
            'is_win': False,
            'is_loss': False,
            'comment': comment,
            'reject_reason': None,
            'trade_manager_action': trade_manager_action
        }
        
        self.trades.append(trade)
        return trade['trade_id']
        
    def close_position(self, trade_id, exit_time, exit_price, exit_reason, ohlcv_df=None):
        """Close an open position"""
        for i, trade in enumerate(self.trades):
            if trade['trade_id'] == trade_id and trade['status'] == 'OPEN':
                trade['status'] = 'CLOSED'
                trade['exit_time'] = exit_time
                trade['exit_price'] = exit_price
                trade['exit_reason'] = exit_reason
                
                # Calculate P&L
                if trade['direction'] == 'BUY':
                    trade['pnl_points'] = exit_price - trade['entry_price']
                else:
                    trade['pnl_points'] = trade['entry_price'] - exit_price
                    
                trade['pnl_percent'] = (trade['pnl_points'] / trade['entry_price']) * 100
                trade['is_win'] = trade['pnl_points'] > 0
                trade['is_loss'] = trade['pnl_points'] < 0
                
                # Calculate duration
                trade['duration_minutes'] = (exit_time - trade['entry_time']).total_seconds() / 60
                
                # Calculate bars held
                if ohlcv_df is not None:
                    try:
                        entry_idx = ohlcv_df.index.get_loc(trade['entry_time'])
                        exit_idx = ohlcv_df.index.get_loc(exit_time)
                        trade['duration_bars'] = exit_idx - entry_idx
                    except:
                        trade['duration_bars'] = 0
                
                # Remove from open positions
                direction = trade['direction']
                position_id = trade['position_id']
                
                if direction in self.open_positions:
                    if isinstance(self.open_positions[direction], list):
                        if position_id in self.open_positions[direction]:
                            self.open_positions[direction].remove(position_id)
                            if not self.open_positions[direction]:
                                del self.open_positions[direction]
                    elif self.open_positions[direction] == position_id:
                        # Check if any other open trades in same position
                        open_in_same_pos = [t for t in self.trades 
                                          if t['direction'] == direction 
                                          and t['position_id'] == position_id
                                          and t['status'] == 'OPEN']
                        if not open_in_same_pos:
                            del self.open_positions[direction]
                
                # Notify trade manager if it has a trade ID
                if self.trade_manager and trade.get('trade_manager_trade_id'):
                    self.trade_manager.close_position_on_exit(
                        trade['trade_manager_trade_id'],
                        exit_time,
                        exit_price,
                        exit_reason
                    )
                
                return True
        return False
        
    def reject_signal(self, timestamp, direction, entry_price, sl_price, tp_price, reason, comment=""):
        """Record a rejected signal"""
        self.trade_counter += 1
        trade = {
            'trade_id': self.trade_counter,
            'trade_manager_trade_id': None,
            'position_id': None,
            'status': 'REJECTED',
            'entry_time': timestamp,
            'exit_time': None,
            'direction': direction,
            'entry_price': entry_price,
            'exit_price': None,
            'sl_price': sl_price,
            'tp_price': tp_price,
            'exit_reason': None,
            'pnl_points': 0,
            'pnl_percent': 0,
            'duration_bars': 0,
            'duration_minutes': 0,
            'sl_distance': abs(entry_price - sl_price) if entry_price and sl_price else 0,
            'tp_distance': abs(tp_price - entry_price) if entry_price and tp_price else 0,
            'risk_reward_ratio': 0,
            'is_win': False,
            'is_loss': False,
            'comment': f"{comment} (Rejected: {reason})",
            'reject_reason': reason
        }
        self.trades.append(trade)
        return trade['trade_id']
        
    def get_trades(self):
        """Get all trades"""
        return self.trades
        
    def get_open_trades(self):
        """Get currently open trades"""
        return [trade for trade in self.trades if trade['status'] == 'OPEN']
        
    def get_closed_trades(self):
        """Get completed trades"""
        return [trade for trade in self.trades if trade['status'] == 'CLOSED']
        
    def get_rejected_trades(self):
        """Get rejected trades"""
        return [trade for trade in self.trades if trade['status'] == 'REJECTED']
        
    def get_current_positions(self):
        """Get current open positions"""
        positions = {}
        for trade in self.get_open_trades():
            pos_id = trade['position_id']
            if pos_id not in positions:
                positions[pos_id] = {
                    'position_id': pos_id,
                    'direction': trade['direction'],
                    'entry_time': trade['entry_time'],
                    'entry_price': trade['entry_price'],
                    'sl_price': trade['sl_price'],
                    'tp_price': trade['tp_price'],
                    'trades': []
                }
            positions[pos_id]['trades'].append(trade)
        return list(positions.values())


def calculate_performance_metrics(trades_df: pd.DataFrame, ohlcv_df: Optional[pd.DataFrame] = None) -> Dict:
    """Calculate comprehensive performance metrics from trades"""
    if trades_df.empty:
        return {
            'total_trades': 0,
            'message': 'No trades to analyze'
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
    
    # Calculate Sharpe ratio (simplified)
    if len(closed_trades) > 1:
        returns = closed_trades['pnl_points'] / closed_trades['sl_distance'].clip(lower=0.1)
        if returns.std() > 0:
            metrics['sharpe_ratio'] = returns.mean() / returns.std()
    
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


def run_wbws_strategy(config_path: str, verbose: bool = False):
    print("\n" + "="*70)
    print("🚀 ENHANCED WBWS STRATEGY WORKFLOW WITH COMPLETE SIMULATION")
    print("="*70 + "\n")
    
    # 1. Load Configuration
    config_path_full = project_root / config_path if not Path(config_path).is_absolute() else Path(config_path)
    with open(config_path_full, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # 2. Load Data - FULL DATASET FIRST (for indicators requiring history)
    data_cfg = config.get('data', {})
    df_full = pd.read_csv(project_root / data_cfg['file'], parse_dates=['timestamp'])
    df_full.columns = df_full.columns.str.lower()
    df_full = df_full.set_index('timestamp').sort_index()
    
    print(f"📊 Full dataset: {len(df_full):,} bars ({df_full.index[0]} to {df_full.index[-1]})")
    
    # Apply Date Range for Strategy Execution
    dr = data_cfg.get('date_range', {})
    df = df_full[(df_full.index >= pd.to_datetime(dr['start'])) & (df_full.index <= pd.to_datetime(dr['end']))]
    print(f"📊 Strategy period: {len(df):,} bars")
    print(f"📅 Backtest range: {df.index[0]} to {df.index[-1]}\n")
    
    # Initialize Trade Tracker
    trade_tracker = TradeTracker()
    
    # 3. Generate Raw Trigger Signals
    from src.indicators.wbws_trigger import WBWSTrigger
    indicator = WBWSTrigger(htf_period=config['indicator']['htf_period'])
    signals_df = indicator.calculate_signals(df)
    
    # Align Index
    if not signals_df.index.equals(df.index):
        signals_df.index = df.index
        
    raw_signals = pd.Series(index=df.index, dtype=object)
    raw_signals.loc[signals_df['we_buy'] == True] = 'BUY'
    raw_signals.loc[signals_df['we_sell'] == True] = 'SELL'
    
    raw_buy_count = int((raw_signals == 'BUY').sum())
    raw_sell_count = int((raw_signals == 'SELL').sum())
    raw_total = raw_buy_count + raw_sell_count
    
    print("="*70)
    print("⚡ STEP 1: RAW TRIGGER SIGNALS")
    print("-"*70)
    print(f"  🟢 BUY:   {raw_buy_count:>4}")
    print(f"  🔴 SELL:  {raw_sell_count:>4}")
    print(f"  📊 TOTAL: {raw_total:>4}")
    print("="*70 + "\n")

    # 4. Apply Time Filter
    from src.strategies.trade_management.time_manager import TimeManager
    
    time_mgr_cfg = config.get('trade_management', {})
    time_manager = TimeManager(time_mgr_cfg)
    
    # Create signals DataFrame for time filtering
    raw_signals_df = pd.DataFrame({
        'timestamp': df.index,
        'signal': raw_signals.values
    }).dropna(subset=['signal'])
    
    # Apply time filter
    time_filtered_df = time_manager.filter_signals_by_time(raw_signals_df, timestamp_col='timestamp')
    
    # Reconstruct time-filtered signals series
    time_filtered_signals = pd.Series(index=df.index, dtype=object)
    if not time_filtered_df.empty:
        time_filtered_signals.loc[time_filtered_df['timestamp'].values] = time_filtered_df['signal'].values
    
    time_filtered_buy = int((time_filtered_signals == 'BUY').sum())
    time_filtered_sell = int((time_filtered_signals == 'SELL').sum())
    time_filtered_total = time_filtered_buy + time_filtered_sell
    
    time_rejected_buy = raw_buy_count - time_filtered_buy
    time_rejected_sell = raw_sell_count - time_filtered_sell
    time_rejected_total = raw_total - time_filtered_total
    
    print("="*70)
    print("⏰ STEP 2: TIME FILTER")
    if time_manager.enabled:
        print(f"  Session: {time_manager.session_start_hour:02d}:{time_manager.session_start_minute:02d} - "
              f"{time_manager.session_end_hour:02d}:{time_manager.session_end_minute:02d}")
    else:
        print("  Status: DISABLED")
    print("-"*70)
    print(f"  🟢 BUY:   {time_filtered_buy:>4}  (rejected: {time_rejected_buy})")
    print(f"  🔴 SELL:  {time_filtered_sell:>4}  (rejected: {time_rejected_sell})")
    print(f"  📊 TOTAL: {time_filtered_total:>4}  (rejected: {time_rejected_total}, "
          f"{(time_rejected_total/raw_total*100) if raw_total > 0 else 0:.1f}%)")
    print("="*70 + "\n")

    # 5. Apply RSI Filter
    from src.strategies.filters.rsi_filter import RSIFilter
    rsi_cfg = config['filters']['rsi_filter']
    rsi_logic = RSIFilter(
        enabled=True, 
        length=rsi_cfg['length'], 
        overbought=rsi_cfg['overbought'], 
        oversold=rsi_cfg['oversold']
    )
    
    not_overbought = rsi_logic.apply_filter(df, is_long=True)
    not_oversold = rsi_logic.apply_filter(df, is_long=False)
    
    final_signals = time_filtered_signals.copy()
    final_signals.loc[(time_filtered_signals == 'BUY') & ~not_overbought] = None
    final_signals.loc[(time_filtered_signals == 'SELL') & ~not_oversold] = None
    
    final_buy_count = int((final_signals == 'BUY').sum())
    final_sell_count = int((final_signals == 'SELL').sum())
    final_total = final_buy_count + final_sell_count
    
    rsi_rejected_buy = time_filtered_buy - final_buy_count
    rsi_rejected_sell = time_filtered_sell - final_sell_count
    rsi_rejected_total = time_filtered_total - final_total
    
    print("="*70)
    print("📉 STEP 3: RSI FILTER")
    print(f"  Config: length={rsi_cfg['length']}, OB={rsi_cfg['overbought']}, "
          f"OS={rsi_cfg['oversold']}")
    print("-"*70)
    print(f"  🟢 BUY:   {final_buy_count:>4}  (rejected: {rsi_rejected_buy})")
    print(f"  🔴 SELL:  {final_sell_count:>4}  (rejected: {rsi_rejected_sell})")
    print(f"  📊 TOTAL: {final_total:>4}  (rejected: {rsi_rejected_total}, "
          f"{(rsi_rejected_total/time_filtered_total*100) if time_filtered_total > 0 else 0:.1f}%)")
    print("="*70 + "\n")

    # 6. Apply Risk Management
    from src.strategies.trade_management.risk_manager import RiskManager
    
    risk_manager = RiskManager(time_mgr_cfg, df_full)  # Use full dataset for indicators
    
    # Prepare signals for risk management
    risk_input_signals = final_signals.dropna()
    risk_approved_count = {'buy': 0, 'sell': 0}
    risk_rejected_count = {'buy': 0, 'sell': 0}
    risk_adjusted_count = {'buy': 0, 'sell': 0}
    
    potential_trades = {}  # Store potential trades by timestamp
    
    for timestamp, signal_type in risk_input_signals.items():
        is_long = (signal_type == 'BUY')
        entry_price = df.loc[timestamp, 'close']
        
        # Calculate SL/TP
        stop_loss, take_profit = risk_manager.calculate_sl_tp(
            entry_price=entry_price,
            is_long=is_long,
            timestamp=timestamp
        )
        
        if stop_loss is None or take_profit is None:
            # No valid SL/TP (ATR not available)
            risk_rejected_count['buy' if is_long else 'sell'] += 1
            final_signals.loc[timestamp] = None
            continue
        
        # Validate risk percentile
        is_valid, adjusted_sl, comment = risk_manager.validate_risk_percentile(
            entry_price=entry_price,
            stop_loss=stop_loss,
            is_long=is_long,
            timestamp=timestamp
        )
        
        if not is_valid:
            # Trade rejected by risk management
            risk_rejected_count['buy' if is_long else 'sell'] += 1
            final_signals.loc[timestamp] = None
            continue
        
        # Track if SL was adjusted
        original_sl = stop_loss
        if adjusted_sl != stop_loss:
            risk_adjusted_count['buy' if is_long else 'sell'] += 1
            stop_loss = adjusted_sl
            # Recalculate TP based on adjusted SL
            sl_distance = abs(entry_price - stop_loss)
            rr_ratio = risk_manager.sl_tp_config.get('risk_to_reward_ratio', 2.0)
            tp_distance = sl_distance * rr_ratio
            if is_long:
                take_profit = entry_price + tp_distance
            else:
                take_profit = entry_price - tp_distance
        
        # Trade approved by risk
        risk_approved_count['buy' if is_long else 'sell'] += 1
        
        sl_distance = abs(entry_price - stop_loss)
        tp_distance = abs(entry_price - take_profit)
        
        potential_trades[timestamp] = {
            'timestamp': timestamp,
            'signal': signal_type,
            'entry': round(entry_price, 2),
            'sl': round(stop_loss, 2),
            'tp': round(take_profit, 2),
            'sl_distance': round(sl_distance, 2),
            'tp_distance': round(tp_distance, 2),
            'risk_reward_ratio': round(tp_distance / sl_distance, 2) if sl_distance > 0 else 0,
            'comment': comment,
            'sl_adjusted': (original_sl != stop_loss)
        }
    
    risk_approved_buy = risk_approved_count['buy']
    risk_approved_sell = risk_approved_count['sell']
    risk_approved_total = risk_approved_buy + risk_approved_sell
    
    risk_rejected_buy = risk_rejected_count['buy']
    risk_rejected_sell = risk_rejected_count['sell']
    risk_rejected_total = risk_rejected_buy + risk_rejected_sell
    
    risk_adjusted_buy = risk_adjusted_count['buy']
    risk_adjusted_sell = risk_adjusted_count['sell']
    risk_adjusted_total = risk_adjusted_buy + risk_adjusted_sell
    
    print("="*70)
    print("🛡️ STEP 4: RISK MANAGEMENT")
    sl_cfg = time_mgr_cfg.get('sl_tp', {})
    risk_cfg = time_mgr_cfg.get('risk_management', {})
    print(f"  SL: ATR({sl_cfg.get('atr_length', 14)}) × {sl_cfg.get('sl_multiplier', 1.4)}")
    print(f"  TP: R:R = 1:{sl_cfg.get('risk_to_reward_ratio', 2.0)}")
    if risk_cfg.get('enabled', False):
        print(f"  Max Risk: {risk_cfg.get('max_risk_percentile', 1.0)*100:.1f}% of annual range")
    print("-"*70)
    print(f"  🟢 BUY:   {risk_approved_buy:>4}  (rejected: {risk_rejected_buy}, adjusted: {risk_adjusted_buy})")
    print(f"  🔴 SELL:  {risk_approved_sell:>4}  (rejected: {risk_rejected_sell}, adjusted: {risk_adjusted_sell})")
    print(f"  📊 TOTAL: {risk_approved_total:>4}  (rejected: {risk_rejected_total}, "
          f"{(risk_rejected_total/final_total*100) if final_total > 0 else 0:.1f}%)")
    if risk_adjusted_total > 0:
        print(f"  ⚠️  Adjusted SL: {risk_adjusted_total} trades")
    print("="*70 + "\n")
    
    # 7. Apply Position Management with Enhanced Trade Tracking
    from src.strategies.trade_management.trade_manager import TradeManager
    
    trade_manager = TradeManager(config)
    trade_tracker.set_trade_manager(trade_manager)
    
    position_rejected_count = {'buy': 0, 'sell': 0}
    exit_stats = {
        'STOP_LOSS': 0,
        'TAKE_PROFIT': 0,
        'OPPOSITE_SIGNAL': 0,
        'END_OF_DATA': 0
    }
    
    print("="*70)
    print("📋 STEP 5: POSITION MANAGEMENT & TRADE SIMULATION")
    print(f"  Close on Opposite: {'ENABLED' if trade_manager.close_on_opposite else 'DISABLED'}")
    print(f"  Pyramiding: {'ENABLED' if trade_manager.pyramiding_enabled else 'DISABLED'}")
    print("="*70)
    
    # Process each bar in chronological order
    for i, (timestamp, row) in enumerate(df.iterrows()):
        # Check for exits on open positions (SL/TP) - FIRST check exits
        for open_trade in trade_tracker.get_open_trades():
            exit_price = None
            exit_reason = None
            
            if open_trade['direction'] == 'BUY':
                if row['low'] <= open_trade['sl_price']:
                    # Pessimistic: SL hit (use SL price, not low)
                    exit_price = open_trade['sl_price']
                    exit_reason = 'STOP_LOSS'
                elif row['high'] >= open_trade['tp_price']:
                    # TP hit (use TP price, not high)
                    exit_price = open_trade['tp_price']
                    exit_reason = 'TAKE_PROFIT'
            else:  # SELL
                if row['high'] >= open_trade['sl_price']:
                    exit_price = open_trade['sl_price']
                    exit_reason = 'STOP_LOSS'
                elif row['low'] <= open_trade['tp_price']:
                    exit_price = open_trade['tp_price']
                    exit_reason = 'TAKE_PROFIT'
            
            if exit_reason:
                # Close the trade in tracker AND notify trade manager
                trade_tracker.close_position(open_trade['trade_id'], timestamp, exit_price, exit_reason, df)
                exit_stats[exit_reason] += 1
                if verbose:
                    print(f"  [EXIT] {timestamp} {open_trade['direction']} {exit_reason} at {exit_price:.2f}")
        
        # Process new signal at this bar - AFTER checking exits
        if timestamp in potential_trades:
            pot_trade = potential_trades[timestamp]
            signal_row = pd.Series({
                'timestamp': timestamp,
                'signal': pot_trade['signal'],
                'entry': pot_trade['entry'],
                'sl': pot_trade['sl'],
                'tp': pot_trade['tp']
            })
            
            # Process signal through TradeManager
            result = trade_manager.handle_signal(signal_row)
            
            if result['action'] == 'OPEN':
                # Store trade manager's trade ID
                trade_manager_trade_id = result.get('open_trade', {}).get('trade_id')
                
                # Open new trade in tracker
                trade_id = trade_tracker.open_position(
                    timestamp=timestamp,
                    direction=pot_trade['signal'],
                    entry_price=pot_trade['entry'],
                    sl_price=pot_trade['sl'],
                    tp_price=pot_trade['tp'],
                    comment=pot_trade['comment'],
                    trade_manager_action='OPEN',
                    trade_manager_trade_id=trade_manager_trade_id
                )
                if verbose:
                    print(f"  [OPEN] {timestamp} {pot_trade['signal']} at {pot_trade['entry']:.2f}")
                
            elif result['action'] == 'CLOSE_AND_REVERSE':
                # Close existing trades
                for close_trade_info in result.get('close_trades', []):
                    # Find the trade in our tracker by trade manager ID
                    for track_trade in trade_tracker.get_open_trades():
                        if track_trade.get('trade_manager_trade_id') == close_trade_info.get('trade_id'):
                            trade_tracker.close_position(
                                trade_id=track_trade['trade_id'],
                                exit_time=timestamp,
                                exit_price=close_trade_info.get('exit_price', pot_trade['entry']),
                                exit_reason='OPPOSITE_SIGNAL',
                                ohlcv_df=df
                            )
                            exit_stats['OPPOSITE_SIGNAL'] += 1
                            if verbose:
                                print(f"  [CLOSE] {timestamp} {track_trade['direction']} OPPOSITE at {close_trade_info.get('exit_price', pot_trade['entry']):.2f}")
                            break
                
                # Get trade manager ID for new reversed trade
                trade_manager_trade_id = result.get('open_trade', {}).get('trade_id')
                
                # Open reverse position
                trade_id = trade_tracker.open_position(
                    timestamp=timestamp,
                    direction=pot_trade['signal'],
                    entry_price=pot_trade['entry'],
                    sl_price=pot_trade['sl'],
                    tp_price=pot_trade['tp'],
                    comment=pot_trade['comment'] + ' (Reversal)',
                    trade_manager_action='CLOSE_AND_REVERSE',
                    trade_manager_trade_id=trade_manager_trade_id
                )
                if verbose:
                    print(f"  [OPEN] {timestamp} {pot_trade['signal']} REVERSE at {pot_trade['entry']:.2f}")
                
            elif result['action'] == 'REJECT':
                # Record rejected signal
                trade_tracker.reject_signal(
                    timestamp=timestamp,
                    direction=pot_trade['signal'],
                    entry_price=pot_trade['entry'],
                    sl_price=pot_trade['sl'],
                    tp_price=pot_trade['tp'],
                    reason=result.get('reason', 'Unknown'),
                    comment=pot_trade['comment']
                )
                position_rejected_count['buy' if pot_trade['signal'] == 'BUY' else 'sell'] += 1
                if verbose:
                    print(f"  [REJECT] {timestamp} {pot_trade['signal']} - {result.get('reason', 'Unknown')}")
    
    # Close any remaining open positions at end of data
    for open_trade in trade_tracker.get_open_trades():
        exit_price = df.iloc[-1]['close']
        trade_tracker.close_position(
            trade_id=open_trade['trade_id'],
            exit_time=df.index[-1],
            exit_price=exit_price,
            exit_reason='END_OF_DATA',
            ohlcv_df=df
        )
        exit_stats['END_OF_DATA'] += 1
        if verbose:
            print(f"  [CLOSE] End of data {open_trade['direction']} at {exit_price:.2f}")
    
    # Get all trades
    all_trades = trade_tracker.get_trades()
    closed_trades = trade_tracker.get_closed_trades()
    open_trades = trade_tracker.get_open_trades()
    rejected_trades = trade_tracker.get_rejected_trades()
    
    position_approved_buy = sum(1 for t in closed_trades if t['direction'] == 'BUY')
    position_approved_sell = sum(1 for t in closed_trades if t['direction'] == 'SELL')
    position_approved_total = position_approved_buy + position_approved_sell
    
    position_rejected_buy = position_rejected_count['buy']
    position_rejected_sell = position_rejected_count['sell']
    position_rejected_total = position_rejected_buy + position_rejected_sell
    
    # Get trade manager metrics
    tm_metrics = trade_manager.get_metrics()
    
    print("-"*70)
    print(f"  🟢 BUY Opens:   {position_approved_buy:>4}  (rejected: {position_rejected_buy})")
    print(f"  🔴 SELL Opens:  {position_approved_sell:>4}  (rejected: {position_rejected_sell})")
    print(f"  📊 TOTAL Opens: {position_approved_total:>4}  (rejected: {position_rejected_total}, "
          f"{(position_rejected_total/risk_approved_total*100) if risk_approved_total > 0 else 0:.1f}%)")
    print(f"  📈 Exit Statistics:")
    for reason, count in exit_stats.items():
        if count > 0:
            print(f"    • {reason}: {count}")
    print("="*70 + "\n")
    
    # 8. Calculate Performance Metrics
    if all_trades:
        trades_df = pd.DataFrame(all_trades)
        performance_metrics = calculate_performance_metrics(trades_df, df)
    else:
        performance_metrics = {
            'total_trades': 0,
            'message': 'No trades executed'
        }
    
    # 9. Prepare Output Directories and Save Reports
    out_cfg = config.get('output', {})
    report_dir = project_root / out_cfg.get('outputs_dir', 'outputs') / out_cfg.get('reports_dir', 'reports/WBWS')
    report_dir.mkdir(parents=True, exist_ok=True)
    
    signals_dir = project_root / out_cfg.get('outputs_dir', 'outputs') / out_cfg.get('signals_dir', 'signals/strategy')
    signals_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save CSV with all trades
    save_csv = out_cfg.get('save_signals_csv', True)
    csv_filename = f"trade_details_{timestamp_str}.csv"
    csv_path = signals_dir / csv_filename
    
    if save_csv and all_trades:
        trades_df = pd.DataFrame(all_trades)
        
        # Convert datetime objects to strings for CSV
        for col in ['entry_time', 'exit_time']:
            if col in trades_df.columns:
                trades_df[col] = trades_df[col].apply(
                    lambda x: x.isoformat() if pd.notnull(x) and not isinstance(x, str) else x
                )
        
        trades_df.to_csv(csv_path, index=False)
        csv_relative_path = str(csv_path.relative_to(project_root))
        print(f"📊 Enhanced CSV Export: {csv_relative_path}")
        print(f"   → {len(trades_df)} trade records saved")
        print(f"   → {len(closed_trades)} closed trades")
        print(f"   → {len(open_trades)} open trades")
        print(f"   → {len(rejected_trades)} rejected signals")
    else:
        csv_relative_path = None
    
    # Build JSON report
    report_data = {
        "execution_time": datetime.now().isoformat(),
        "config": {
            "data_period": {
                "start": dr['start'],
                "end": dr['end']
            },
            "indicator": config['indicator']['name'],
            "htf_period": config['indicator']['htf_period'],
            "time_filter": {
                "enabled": time_manager.enabled,
                "session": f"{time_manager.session_start_hour:02d}:{time_manager.session_start_minute:02d}-"
                          f"{time_manager.session_end_hour:02d}:{time_manager.session_end_minute:02d}"
            } if time_manager.enabled else {"enabled": False},
            "rsi_filter": rsi_cfg,
            "position_control": time_mgr_cfg.get('position_control', {})
        },
        "signal_flow": {
            "step1_raw_signals": {
                "buy": raw_buy_count,
                "sell": raw_sell_count,
                "total": raw_total
            },
            "step2_time_filtered": {
                "buy": time_filtered_buy,
                "sell": time_filtered_sell,
                "total": time_filtered_total,
                "rejected_buy": time_rejected_buy,
                "rejected_sell": time_rejected_sell,
                "rejected_total": time_rejected_total,
                "rejection_rate_pct": round((time_rejected_total/raw_total*100) if raw_total > 0 else 0, 2)
            },
            "step3_rsi_filtered": {
                "buy": final_buy_count,
                "sell": final_sell_count,
                "total": final_total,
                "rejected_buy": rsi_rejected_buy,
                "rejected_sell": rsi_rejected_sell,
                "rejected_total": rsi_rejected_total,
                "rejection_rate_pct": round((rsi_rejected_total/time_filtered_total*100) if time_filtered_total > 0 else 0, 2)
            },
            "step4_risk_managed": {
                "buy": risk_approved_buy,
                "sell": risk_approved_sell,
                "total": risk_approved_total,
                "rejected_buy": risk_rejected_buy,
                "rejected_sell": risk_rejected_sell,
                "rejected_total": risk_rejected_total,
                "adjusted_buy": risk_adjusted_buy,
                "adjusted_sell": risk_adjusted_sell,
                "adjusted_total": risk_adjusted_total,
                "rejection_rate_pct": round((risk_rejected_total/final_total*100) if final_total > 0 else 0, 2)
            },
            "step5_position_managed": {
                "buy_opens": position_approved_buy,
                "sell_opens": position_approved_sell,
                "total_opens": position_approved_total,
                "rejected_buy": position_rejected_buy,
                "rejected_sell": position_rejected_sell,
                "rejected_total": position_rejected_total,
                "exit_statistics": exit_stats,
                "trade_manager_metrics": tm_metrics
            }
        },
        "simulation_results": {
            "total_trades_simulated": len(all_trades),
            "closed_trades": len(closed_trades),
            "open_trades": len(open_trades),
            "rejected_signals": len(rejected_trades),
            "performance_metrics": performance_metrics,
            "trade_summary": {
                "first_trade": all_trades[0]['entry_time'].isoformat() if all_trades else None,
                "last_trade": all_trades[-1]['entry_time'].isoformat() if all_trades else None,
                "total_duration_days": (df.index[-1] - df.index[0]).days if len(df) > 1 else 0,
                "trades_per_day": len(closed_trades) / max(((df.index[-1] - df.index[0]).days + 1), 1) if len(df) > 1 else 0
            },
            "position_management": {
                "max_concurrent_positions": len(trade_tracker.get_current_positions()),
                "pyramiding_used": any(len([t for t in all_trades if t['direction'] == d and t['status'] == 'OPEN']) > 1 
                                      for d in ['BUY', 'SELL']),
                "close_and_reverse_count": len([t for t in all_trades if 'Reversal' in str(t.get('comment', ''))])
            }
        },
        "overall_rejection": {
            "total_rejected": raw_total - position_approved_total,
            "total_rejection_rate_pct": round(((raw_total - position_approved_total)/raw_total*100) if raw_total > 0 else 0, 2)
        },
        "risk_details": {
            "atr_length": sl_cfg.get('atr_length', 14),
            "sl_multiplier": sl_cfg.get('sl_multiplier', 1.4),
            "risk_to_reward": sl_cfg.get('risk_to_reward_ratio', 2.0),
            "max_risk_percentile": risk_cfg.get('max_risk_percentile', 1.0),
            "allow_exceed_limit": risk_cfg.get('allow_exceed_limit', False)
        },
        "outputs": {
            "signals_csv_file": csv_relative_path,
            "trades_csv_file": csv_relative_path,
        }
    }
    
    # Save JSON Report
    report_path = report_dir / f"strategy_report_{timestamp_str}.json"
    with open(report_path, 'w') as f:
        json.dump(report_data, f, indent=4)
    
    # 10. Display Final Summary
    print("\n" + "="*80)
    print("📊 FINAL SUMMARY - COMPLETE SIGNAL FLOW")
    print("="*80)
    print(f"  Raw Signals:        {raw_total:>4}")
    print(f"  Time Filtered:      {time_filtered_total:>4}  (↓ {time_rejected_total}, -{(time_rejected_total/raw_total*100) if raw_total > 0 else 0:.1f}%)")
    print(f"  RSI Filtered:       {final_total:>4}  (↓ {rsi_rejected_total}, -{(rsi_rejected_total/time_filtered_total*100) if time_filtered_total > 0 else 0:.1f}%)")
    print(f"  Risk Approved:      {risk_approved_total:>4}  (↓ {risk_rejected_total}, -{(risk_rejected_total/final_total*100) if final_total > 0 else 0:.1f}%)")
    print(f"  Position Approved:  {position_approved_total:>4}  (↓ {position_rejected_total}, -{(position_rejected_total/risk_approved_total*100) if risk_approved_total > 0 else 0:.1f}%)")
    print("-"*80)
    print(f"  🟢 Final BUY Trades: {position_approved_buy:>4}  (from {raw_buy_count}, -{raw_buy_count-position_approved_buy})")
    print(f"  🔴 Final SELL Trades:{position_approved_sell:>4}  (from {raw_sell_count}, -{raw_sell_count-position_approved_sell})")
    print(f"  📉 Overall Rejection: {((raw_total-position_approved_total)/raw_total*100) if raw_total > 0 else 0:.1f}%")
    
    if risk_adjusted_total > 0:
        print(f"  ⚙️  SL Adjustments:  {risk_adjusted_total}")
    
    # Display Performance Metrics
    if performance_metrics.get('total_trades', 0) > 0:
        print("\n" + "="*80)
        print("📈 PERFORMANCE METRICS (SIMULATED)")
        print("="*80)
        print(f"  Total Trades:       {performance_metrics['total_trades']}")
        print(f"  Winning Trades:     {performance_metrics['winning_trades']} ({performance_metrics['win_rate']:.1f}%)")
        print(f"  Losing Trades:      {performance_metrics['losing_trades']} ({performance_metrics['loss_rate']:.1f}%)")
        print(f"  Total P&L Points:   {performance_metrics['total_pnl_points']:+.2f}")
        print(f"  Avg P&L/Trade:      {performance_metrics['avg_pnl_points']:+.2f}")
        print(f"  Profit Factor:      {performance_metrics['profit_factor']:.2f}")
        print(f"  Expectancy:         {performance_metrics['expectancy_points']:+.2f} points")
        print(f"  Sharpe Ratio:       {performance_metrics['sharpe_ratio']:.2f}")
        print("-"*80)
        
        if 'drawdown_analysis' in performance_metrics:
            dd = performance_metrics['drawdown_analysis']
            print(f"  Max Drawdown:       {dd['max_drawdown_points']:+.2f} points ({dd['max_drawdown_percent']:.1f}%)")
            print(f"  Recovery Factor:    {dd['recovery_factor']:.2f}")
        
        if 'exit_reasons' in performance_metrics:
            print(f"  Exit Reasons:")
            for reason, count in performance_metrics['exit_reasons'].items():
                pct = (count / performance_metrics['total_trades']) * 100
                print(f"    • {reason}: {count} ({pct:.1f}%)")
    
    print(f"\n📂 JSON Report: {report_path.relative_to(project_root)}")
    if csv_relative_path:
        print(f"📂 CSV Trades: {csv_relative_path}")
    print("\n\n✅ Strategy execution completed successfully!\n")
    
    return df, all_trades, report_data


if __name__ == "__main__":
    if len(sys.argv) > 1:
        verbose_flag = '--verbose' in sys.argv
        config_arg = sys.argv[1] if not sys.argv[1] == '--verbose' else sys.argv[2]
        run_wbws_strategy(config_arg, verbose=verbose_flag)
    else:
        print("❌ Usage: python scripts/run_wbws_strategy.py <config_path> [--verbose]")
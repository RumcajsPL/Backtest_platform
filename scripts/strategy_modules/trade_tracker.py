"""
Enhanced Trade Tracker Module with Progressive Tracking Support
Tracks trades with unique IDs and complete history, including signal_id linking
"""
import pandas as pd
from typing import List, Dict

class TradeTracker:
    """Track trades with unique IDs and complete history"""
    def __init__(self):
        self.trades = []
        self.trade_counter = 0
        self.position_counter = 0
        self.open_positions = {}
        self.trade_manager = None
        self.signal_id_counter = 0  # Counter for trade-related signal tracking
        
    def set_trade_manager(self, trade_manager):
        """Set trade manager for position control"""
        self.trade_manager = trade_manager
        
    def open_position(self, timestamp, direction, entry_price, sl_price, tp_price, 
                     comment="", trade_manager_action=None, trade_manager_trade_id=None,
                     signal_id=None):  # NEW: Add signal_id parameter
        """Open a new position with signal_id linking"""
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
        
        # Create trade record with signal_id
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
            'trade_manager_action': trade_manager_action,
            'signal_id': signal_id,  # NEW: Store signal_id for progressive tracking
            'signal_linked': signal_id is not None,  # NEW: Flag for signal linking
            'exit_details': None,  # NEW: Will store detailed exit information
            'created_at': pd.Timestamp.now()
        }
        
        self.trades.append(trade)
        return trade['trade_id']
        
    def close_position(self, trade_id, exit_time, exit_price, exit_reason, ohlcv_df=None,
                      exit_details=None):  # NEW: Add exit_details parameter
        """Close an open position with optional exit details"""
        for i, trade in enumerate(self.trades):
            if trade['trade_id'] == trade_id and trade['status'] == 'OPEN':
                trade['status'] = 'CLOSED'
                trade['exit_time'] = exit_time
                trade['exit_price'] = exit_price
                trade['exit_reason'] = exit_reason
                
                # Store exit details if provided
                if exit_details is not None:
                    trade['exit_details'] = exit_details
                
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
                
                # REMOVED: Old close_position_on_exit() call
                # TradeManager state is now updated directly by TradeSimulator
                # via trade_manager.close_positions() calls
                
                return True
        return False
        
    def reject_signal(self, timestamp, direction, entry_price, sl_price, tp_price, 
                     reason, comment="", signal_id=None):  # NEW: Add signal_id parameter
        """Record a rejected signal with signal_id linking"""
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
            'reject_reason': reason,
            'signal_id': signal_id,  # NEW: Store signal_id for progressive tracking
            'signal_linked': signal_id is not None,  # NEW: Flag for signal linking
            'created_at': pd.Timestamp.now()
        }
        self.trades.append(trade)
        return trade['trade_id']
    
    def update_trade_exit_details(self, trade_id, exit_details):
        """Update a trade with detailed exit information"""
        for trade in self.trades:
            if trade['trade_id'] == trade_id:
                trade['exit_details'] = exit_details
                return True
        return False
    
    def get_trade_by_signal_id(self, signal_id):
        """Get trade by its associated signal_id"""
        for trade in self.trades:
            if trade.get('signal_id') == signal_id:
                return trade
        return None
    
    def get_trades_by_signal_ids(self, signal_ids):
        """Get multiple trades by their signal_ids"""
        return [trade for trade in self.trades if trade.get('signal_id') in signal_ids]
    
    def get_signal_linked_trades(self):
        """Get all trades that are linked to a signal"""
        return [trade for trade in self.trades if trade.get('signal_linked', False)]
    
    def get_unlinked_trades(self):
        """Get all trades that are not linked to a signal"""
        return [trade for trade in self.trades if not trade.get('signal_linked', False)]
        
    def get_trades(self) -> List[Dict]:
        """Get all trades"""
        return self.trades
        
    def get_open_trades(self) -> List[Dict]:
        """Get currently open trades"""
        return [trade for trade in self.trades if trade['status'] == 'OPEN']
        
    def get_closed_trades(self) -> List[Dict]:
        """Get completed trades"""
        return [trade for trade in self.trades if trade['status'] == 'CLOSED']
        
    def get_rejected_trades(self) -> List[Dict]:
        """Get rejected trades"""
        return [trade for trade in self.trades if trade['status'] == 'REJECTED']
        
    def get_current_positions(self) -> List[Dict]:
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
                    'signal_ids': [],  # NEW: Track signal IDs in position
                    'trades': []
                }
            positions[pos_id]['trades'].append(trade)
            # Collect signal IDs
            if trade.get('signal_id'):
                positions[pos_id]['signal_ids'].append(trade['signal_id'])
        return list(positions.values())
    
    def get_position_by_signal_id(self, signal_id):
        """Get position that contains a trade with the given signal_id"""
        for position in self.get_current_positions():
            for trade in position['trades']:
                if trade.get('signal_id') == signal_id:
                    return position
        return None
    
    def get_statistics(self):
        """Get statistics about tracked trades including signal linking"""
        stats = {
            'total_trades': len(self.trades),
            'open_trades': len(self.get_open_trades()),
            'closed_trades': len(self.get_closed_trades()),
            'rejected_trades': len(self.get_rejected_trades()),
            'positions': len(self.get_current_positions()),
            'signal_linked_trades': len(self.get_signal_linked_trades()),
            'unlinked_trades': len(self.get_unlinked_trades()),
            'signal_linking_rate': (len(self.get_signal_linked_trades()) / len(self.trades) * 100 
                                    if len(self.trades) > 0 else 0)
        }
        
        # Win/loss stats for closed trades
        closed_trades = self.get_closed_trades()
        if closed_trades:
            winning_trades = [t for t in closed_trades if t['is_win']]
            losing_trades = [t for t in closed_trades if t['is_loss']]
            stats['winning_trades'] = len(winning_trades)
            stats['losing_trades'] = len(losing_trades)
            stats['win_rate'] = (len(winning_trades) / len(closed_trades)) * 100
            
            # P&L stats
            total_pnl = sum(t['pnl_points'] for t in closed_trades)
            avg_pnl = total_pnl / len(closed_trades) if closed_trades else 0
            stats['total_pnl_points'] = total_pnl
            stats['avg_pnl_per_trade'] = avg_pnl
        
        return stats
    
    def reset(self):
        """Reset the tracker"""
        self.trades = []
        self.trade_counter = 0
        self.position_counter = 0
        self.open_positions = {}
        self.signal_id_counter = 0
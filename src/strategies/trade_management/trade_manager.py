"""
Trade Manager - Position Control & Signal Filtering
Handles pyramiding limits and close-on-opposite logic
"""

import pandas as pd
from typing import Dict, Optional, List
from dataclasses import dataclass
from datetime import datetime

@dataclass
class Position:
    """Represents an open trading position"""
    entry_time: datetime
    direction: str  # 'BUY' or 'SELL'
    entry_price: float
    sl_price: float
    tp_price: float
    trade_id: int

class TradeManager:
    """
    Manages trade positions and enforces position rules.
    
    Features:
    - Pyramiding control (enable/disable multiple positions)
    - Close-on-opposite (reverse on opposite signal vs ignore)
    - Position tracking and state management
    """
    
    def __init__(self, config: Dict):
        """
        Initialize TradeManager with configuration.
        
        Args:
            config: Dictionary with 'position_control' settings:
                - close_on_opposite: bool (default False)
                - pyramiding_enabled: bool (default False)
        """
        position_config = config.get('trade_management', {}).get('position_control', {})
        
        self.close_on_opposite = position_config.get('close_on_opposite', False)
        self.pyramiding_enabled = position_config.get('pyramiding_enabled', False)
        
        # State tracking
        self.current_positions: List[Position] = []
        self.current_direction: Optional[str] = None
        self.trade_counter = 0
        
        # Metrics tracking
        self.metrics = {
            'total_signals_received': 0,
            'signals_accepted': 0,
            'signals_rejected': 0,
            'rejected_reasons': {
                'pyramiding_disabled': 0,
                'opposite_ignored': 0,
            },
            'positions_closed_by_opposite': 0,
            'positions_reversed': 0,
        }
        
        print(f"✅ TradeManager initialized:")
        print(f"   - Close on Opposite: {self.close_on_opposite}")
        print(f"   - Pyramiding: {'ENABLED' if self.pyramiding_enabled else 'DISABLED'}")
    
    def handle_signal(self, signal_row: pd.Series) -> Dict:
        """
        Process a trading signal and determine action.
        
        Args:
            signal_row: Series containing signal data (timestamp, signal, entry, sl, tp)
        
        Returns:
            Dictionary with:
                - action: 'OPEN', 'CLOSE_AND_REVERSE', 'REJECT'
                - reason: str (explanation)
                - close_trades: Optional[List[Dict]] (if closing positions)
                - open_trade: Optional[Dict] (if opening position)
        """
        self.metrics['total_signals_received'] += 1
        
        signal_time = signal_row['timestamp']
        signal_direction = signal_row['signal']
        
        # Case 1: No positions open - always accept and set direction
        if not self.current_positions:
            return self._create_open_action(signal_row)
        
        # Determine if same or opposite
        is_same_direction = signal_direction == self.current_direction
        
        # Case 2: Same direction as current positions
        if is_same_direction:
            if self.pyramiding_enabled:
                # Pyramiding enabled - open another position
                return self._create_open_action(signal_row)
            else:
                # Pyramiding disabled - reject if already have position
                self.metrics['signals_rejected'] += 1
                self.metrics['rejected_reasons']['pyramiding_disabled'] += 1
                return {
                    'action': 'REJECT',
                    'reason': f'Pyramiding disabled - {signal_direction} position already open',
                    'close_trades': None,
                    'open_trade': None,
                }
        
        # Case 3: Opposite direction
        else:
            if self.close_on_opposite:
                # Close all current positions and reverse
                close_trades = []
                for pos in self.current_positions:
                    close_trade = self._create_close_trade(signal_time, signal_row['entry'], 'OPPOSITE_SIGNAL', pos)
                    close_trades.append(close_trade)
                
                self.metrics['positions_closed_by_opposite'] += len(close_trades)
                self.metrics['positions_reversed'] += 1
                
                # Clear positions and direction
                self.current_positions = []
                self.current_direction = None
                
                # Open new position
                open_result = self._create_open_action(signal_row)
                
                return {
                    'action': 'CLOSE_AND_REVERSE',
                    'reason': f'Closed {len(close_trades)} {self.current_direction} positions and reversed to {signal_direction}',
                    'close_trades': close_trades,
                    'open_trade': open_result['open_trade'],
                }
            else:
                # Ignore opposite signal
                self.metrics['signals_rejected'] += 1
                self.metrics['rejected_reasons']['opposite_ignored'] += 1
                return {
                    'action': 'REJECT',
                    'reason': f'Opposite signal ignored - {self.current_direction} positions still open',
                    'close_trades': None,
                    'open_trade': None,
                }
    
    def _create_open_action(self, signal_row: pd.Series) -> Dict:
        """Create action to open a new position"""
        self.trade_counter += 1
        
        position = Position(
            entry_time=signal_row['timestamp'],
            direction=signal_row['signal'],
            entry_price=float(signal_row['entry']),
            sl_price=float(signal_row['sl']),
            tp_price=float(signal_row['tp']),
            trade_id=self.trade_counter,
        )
        
        self.current_positions.append(position)
        if self.current_direction is None:
            self.current_direction = position.direction
        self.metrics['signals_accepted'] += 1
        
        return {
            'action': 'OPEN',
            'reason': f'Opening {position.direction} position',
            'close_trades': None,
            'open_trade': {
                'timestamp': position.entry_time,
                'signal': position.direction,
                'entry': position.entry_price,
                'sl': position.sl_price,
                'tp': position.tp_price,
                'trade_id': position.trade_id,
            }
        }
    
    def _create_close_trade(self, exit_time: datetime, exit_price: float, exit_reason: str, position: Position) -> Dict:
        """Create trade closure record for a specific position"""
        if position.direction == 'BUY':
            pnl_points = exit_price - position.entry_price
        else:
            pnl_points = position.entry_price - exit_price
        
        return {
            'trade_id': position.trade_id,
            'entry_time': position.entry_time,
            'exit_time': exit_time,
            'direction': position.direction,
            'entry_price': position.entry_price,
            'exit_price': exit_price,
            'sl_price': position.sl_price,
            'tp_price': position.tp_price,
            'exit_reason': exit_reason,
            'pnl_points': pnl_points,
        }
    
    def close_position_on_exit(self, trade_id: int, exit_time: datetime, exit_price: float, exit_reason: str) -> Optional[Dict]:
        """
        Close a specific position by trade_id (called by backtest simulator when SL/TP hit).
        
        Returns:
            Trade closure record or None if position not found
        """
        for i, pos in enumerate(self.current_positions):
            if pos.trade_id == trade_id:
                close_trade = self._create_close_trade(exit_time, exit_price, exit_reason, pos)
                del self.current_positions[i]
                if not self.current_positions:
                    self.current_direction = None
                return close_trade
        return None
    
    def has_open_position(self) -> bool:
        """Check if there are any open positions"""
        return len(self.current_positions) > 0
    
    def get_current_positions(self) -> List[Position]:
        """Get list of current open positions"""
        return self.current_positions.copy()
    
    def get_metrics(self) -> Dict:
        """Get position management metrics"""
        return self.metrics.copy()
    
    def reset(self):
        """Reset trade manager state (for new backtest runs)"""
        self.current_positions = []
        self.current_direction = None
        self.trade_counter = 0
        self.metrics = {
            'total_signals_received': 0,
            'signals_accepted': 0,
            'signals_rejected': 0,
            'rejected_reasons': {
                'pyramiding_disabled': 0,
                'opposite_ignored': 0,
            },
            'positions_closed_by_opposite': 0,
            'positions_reversed': 0,
        }
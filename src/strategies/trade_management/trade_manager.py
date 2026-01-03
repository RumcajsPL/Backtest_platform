# Updated: src/strategies/trade_management/trade_manager.py
# (Refactored: No prices in Position or logic; handle_signal takes timestamp/signal_type; outputs close_trade_ids/new_trade_id; add open_position/close_positions; remove _create_close_trade/close_position_on_exit (simulator handles); current_direction as property; metrics unchanged)

from typing import Dict, Optional, List
from dataclasses import dataclass
from datetime import datetime

@dataclass
class Position:
    """Represents an open trading position (no prices)"""
    entry_time: datetime
    direction: str  # 'BUY' or 'SELL'
    trade_id: int

class TradeManager:
    def __init__(self, config: Dict):
        position_config = config.get('trade_management', {}).get('position_control', {})
        
        self.close_on_opposite = position_config.get('close_on_opposite', False)
        self.pyramiding_enabled = position_config.get('pyramiding_enabled', False)
        
        # State tracking
        self.current_positions: List[Position] = []
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
    
    @property
    def current_direction(self) -> Optional[str]:
        return self.current_positions[0].direction if self.current_positions else None
    
    def handle_signal(self, timestamp: datetime, signal_type: str) -> Dict:
        self.metrics['total_signals_received'] += 1
        
        direction = signal_type  # 'BUY' or 'SELL'
        
        # Case 1: No positions - open
        if not self.current_positions:
            return self._create_open_action(direction)
        
        is_same_direction = direction == self.current_direction
        
        # Case 2: Same direction
        if is_same_direction:
            if self.pyramiding_enabled:
                return self._create_open_action(direction)
            else:
                self.metrics['signals_rejected'] += 1
                self.metrics['rejected_reasons']['pyramiding_disabled'] += 1
                return {
                    'action': 'REJECT',
                    'reason': f'Pyramiding disabled - {direction} position already open',
                    'close_trade_ids': None,
                    'new_trade_id': None,
                }
        
        # Case 3: Opposite
        else:
            if self.close_on_opposite:
                close_trade_ids = [pos.trade_id for pos in self.current_positions]
                self.metrics['positions_closed_by_opposite'] += len(close_trade_ids)
                self.metrics['positions_reversed'] += 1
                
                return {
                    'action': 'CLOSE_AND_REVERSE',
                    'reason': f'Closing {len(close_trade_ids)} {self.current_direction} positions and reversing to {direction}',
                    'close_trade_ids': close_trade_ids,
                    'new_trade_id': self.trade_counter + 1,  # Pre-assign
                }
            else:
                self.metrics['signals_rejected'] += 1
                self.metrics['rejected_reasons']['opposite_ignored'] += 1
                return {
                    'action': 'REJECT',
                    'reason': f'Opposite signal ignored - {self.current_direction} positions still open',
                    'close_trade_ids': None,
                    'new_trade_id': None,
                }
    
    def _create_open_action(self, direction: str) -> Dict:
        new_trade_id = self.trade_counter + 1  # Pre-assign
        self.metrics['signals_accepted'] += 1
        return {
            'action': 'OPEN',
            'reason': f'Opening {direction} position',
            'close_trade_ids': None,
            'new_trade_id': new_trade_id,
        }
    
    def open_position(self, trade_id: int, timestamp: datetime, direction: str):
        """Update state for new open position"""
        position = Position(
            entry_time=timestamp,
            direction=direction,
            trade_id=trade_id,
        )
        self.current_positions.append(position)
        self.trade_counter = max(self.trade_counter, trade_id)
    
    def close_positions(self, trade_ids: List[int]):
        """Update state for closed positions"""
        self.current_positions = [pos for pos in self.current_positions if pos.trade_id not in trade_ids]
    
    def has_open_position(self) -> bool:
        return len(self.current_positions) > 0
    
    def get_current_positions(self) -> List[Position]:
        return self.current_positions.copy()
    
    def get_metrics(self) -> Dict:
        return self.metrics.copy()
    
    def reset(self):
        self.current_positions = []
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
"""Trade position management: handles position opening/closing logic and pyramiding"""
import logging
from typing import Dict, Optional, List
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)

@dataclass
class Position:
    """Represents an open trading position"""
    entry_time: datetime
    direction: str  # 'BUY' or 'SELL'
    trade_id: int

class TradeManager:
    """
    Manages trading positions with configurable pyramiding and reversal logic.
    ID System:
    - trade_id: Sequential position identifier used for tracking and closing
    """
    
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
        
        logger.info("TradeManager initialized:")
        logger.info(f"  Close on Opposite: {self.close_on_opposite}")
        logger.info(f"  Pyramiding: {'ENABLED' if self.pyramiding_enabled else 'DISABLED'}")
    
    @property
    def current_direction(self) -> Optional[str]:
        """Returns current position direction or None if no positions"""
        return self.current_positions[0].direction if self.current_positions else None
    
    def handle_signal(self, timestamp: datetime, signal_type: str) -> Dict:
        """        
        Returns:
            Dict with keys: action, reason, close_trade_ids, new_trade_id
            Actions: 'OPEN', 'CLOSE_AND_REVERSE', 'REJECT'
        """
        self.metrics['total_signals_received'] += 1
        direction = signal_type
        
        # Case 1: No positions - open new
        if not self.current_positions:
            return self._create_open_action(direction)
        
        is_same_direction = direction == self.current_direction
        
        # Case 2: Same direction signal
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
        
        # Case 3: Opposite direction signal
        else:
            if self.close_on_opposite:
                close_trade_ids = [pos.trade_id for pos in self.current_positions]
                self.metrics['positions_closed_by_opposite'] += len(close_trade_ids)
                self.metrics['positions_reversed'] += 1
                
                return {
                    'action': 'CLOSE_AND_REVERSE',
                    'reason': f'Closing {len(close_trade_ids)} {self.current_direction} positions and reversing to {direction}',
                    'close_trade_ids': close_trade_ids,
                    'new_trade_id': self.trade_counter + 1,
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
        """Create action dict for opening new position"""
        new_trade_id = self.trade_counter + 1
        self.metrics['signals_accepted'] += 1
        return {
            'action': 'OPEN',
            'reason': f'Opening {direction} position',
            'close_trade_ids': None,
            'new_trade_id': new_trade_id,
        }
    
    def open_position(self, trade_id: int, timestamp: datetime, direction: str):
        """Register new open position in state"""
        position = Position(
            entry_time=timestamp,
            direction=direction,
            trade_id=trade_id,
        )
        self.current_positions.append(position)
        self.trade_counter = max(self.trade_counter, trade_id)
    
    def close_positions(self, trade_ids: List[int]):
        """Remove closed positions from state"""
        self.current_positions = [pos for pos in self.current_positions if pos.trade_id not in trade_ids]
    
    def has_open_position(self) -> bool:
        """Check if any positions are currently open"""
        return len(self.current_positions) > 0
    
    def get_current_positions(self) -> List[Position]:
        """Get copy of current positions list"""
        return self.current_positions.copy()
    
    def get_metrics(self) -> Dict:
        """Get copy of metrics dictionary"""
        return self.metrics.copy()
    
    def reset(self):
        """Reset all state and metrics"""
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
"""Trade position management: handles position opening/closing logic and pyramiding

MIGRATION STATUS: Session 8 - Contract Integration Complete
- Uses Position contract from position_contracts.py
- Returns TradeDecision contract from trade_contracts.py
- Maintains backward compatibility via legacy methods
"""
import logging
from typing import Dict, Optional, List, Any
from datetime import datetime
import pandas as pd

from src.strategies.contracts.trade_contracts import (
    TradeDecision,
    DecisionType,
    TradeDirection
)
from src.strategies.contracts.position_contracts import Position

logger = logging.getLogger(__name__)


class TradeManager:
    """
    Manages trading positions with configurable pyramiding and reversal logic.
    
    Migration Notes (Session 8):
    - Now uses Position contract (frozen dataclass with full price/SL/TP data)
    - Returns TradeDecision contract instead of dict
    - Enhanced handle_signal() signature to accept price parameters
    - Added legacy methods for backward compatibility
    
    ID System:
    - position_id: Sequential position identifier used for tracking and closing
    - Matches previous trade_id for backward compatibility
    """
    
    def __init__(self, config: Dict):
        position_config = config.get('trade_management', {}).get('position_control', {})
        
        self.close_on_opposite = position_config.get('close_on_opposite', False)
        self.pyramiding_enabled = position_config.get('pyramiding_enabled', False)
        
        # State tracking (now uses Position contract)
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
        
        logger.info("TradeManager initialized (Session 8 - Contract Version):")
        logger.info(f"  Close on Opposite: {self.close_on_opposite}")
        logger.info(f"  Pyramiding: {'ENABLED' if self.pyramiding_enabled else 'DISABLED'}")
    
    @property
    def current_direction(self) -> Optional[TradeDirection]:
        """Returns current position direction or None if no positions"""
        return self.current_positions[0].direction if self.current_positions else None
    
    def handle_signal(
        self,
        timestamp: pd.Timestamp,
        signal_type: str,
        entry_price: float,
        stop_loss: float,
        take_profit: float,
        position_size: float = 1.0,
        meta: Optional[Dict[str, Any]] = None
    ) -> TradeDecision:
        """
        Handle incoming signal and return trading decision.
        
        Args:
            timestamp: Signal timestamp (pandas Timestamp)
            signal_type: 'BUY' or 'SELL'
            entry_price: Execution price from RiskManager
            stop_loss: SL price from RiskManager
            take_profit: TP price from RiskManager
            position_size: Position size (default 1.0)
            meta: Optional metadata dict for Position contract
        
        Returns:
            TradeDecision contract with decision type, reason, and action details
            
        Decision Logic:
            - No positions → OPEN
            - Same direction + pyramiding enabled → OPEN
            - Same direction + pyramiding disabled → REJECT
            - Opposite direction + close_on_opposite → CLOSE_AND_REVERSE
            - Opposite direction + no close_on_opposite → REJECT
        """
        self.metrics['total_signals_received'] += 1
        
        # Convert signal_type string to TradeDirection enum
        direction = TradeDirection.from_string(signal_type)
        
        # Case 1: No positions - open new
        if not self.current_positions:
            return self._create_open_decision(
                direction=direction,
                timestamp=timestamp,
                entry_price=entry_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                position_size=position_size,
                meta=meta
            )
        
        is_same_direction = direction == self.current_direction
        
        # Case 2: Same direction signal
        if is_same_direction:
            if self.pyramiding_enabled:
                return self._create_open_decision(
                    direction=direction,
                    timestamp=timestamp,
                    entry_price=entry_price,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    position_size=position_size,
                    meta=meta
                )
            else:
                self.metrics['signals_rejected'] += 1
                self.metrics['rejected_reasons']['pyramiding_disabled'] += 1
                return TradeDecision(
                    decision_type=DecisionType.REJECT,
                    reason=f'Pyramiding disabled - {direction.to_string()} position already open',
                    close_trade_ids=None,
                    new_trade_id=None,
                )
        
        # Case 3: Opposite direction signal
        else:
            if self.close_on_opposite:
                close_trade_ids = [pos.position_id for pos in self.current_positions]
                self.metrics['positions_closed_by_opposite'] += len(close_trade_ids)
                self.metrics['positions_reversed'] += 1
                self.metrics['signals_accepted'] += 1
                
                return TradeDecision(
                    decision_type=DecisionType.CLOSE_AND_REVERSE,
                    reason=f'Closing {len(close_trade_ids)} {self.current_direction.to_string()} positions and reversing to {direction.to_string()}',
                    close_trade_ids=close_trade_ids,
                    new_trade_id=self.trade_counter + 1,
                )
            else:
                self.metrics['signals_rejected'] += 1
                self.metrics['rejected_reasons']['opposite_ignored'] += 1
                return TradeDecision(
                    decision_type=DecisionType.REJECT,
                    reason=f'Opposite signal ignored - {self.current_direction.to_string()} positions still open',
                    close_trade_ids=None,
                    new_trade_id=None,
                )
    
    def _create_open_decision(
        self,
        direction: TradeDirection,
        timestamp: pd.Timestamp,
        entry_price: float,
        stop_loss: float,
        take_profit: float,
        position_size: float,
        meta: Optional[Dict[str, Any]]
    ) -> TradeDecision:
        """
        Create decision for opening new position.
        
        Internal helper that increments trade counter and metrics.
        """
        new_trade_id = self.trade_counter + 1
        self.metrics['signals_accepted'] += 1
        
        return TradeDecision(
            decision_type=DecisionType.OPEN,
            reason=f'Opening {direction.to_string()} position',
            close_trade_ids=None,
            new_trade_id=new_trade_id,
        )
    
    def open_position(
        self,
        trade_id: int,
        timestamp: pd.Timestamp,
        direction: TradeDirection,
        entry_price: float,
        stop_loss: float,
        take_profit: float,
        position_size: float = 1.0,
        meta: Optional[Dict[str, Any]] = None
    ):
        """
        Register new open position in state.
        
        Creates Position contract and adds to current_positions list.
        
        Args:
            trade_id: Position ID (sequential)
            timestamp: Entry timestamp
            direction: TradeDirection enum (LONG/SHORT)
            entry_price: Execution price
            stop_loss: SL price
            take_profit: TP price
            position_size: Position size (default 1.0)
            meta: Optional metadata dict
        """
        position = Position(
            position_id=trade_id,
            direction=direction,
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            size=position_size,
            open_time=timestamp,
            meta=meta or {},
        )
        self.current_positions.append(position)
        self.trade_counter = max(self.trade_counter, trade_id)
        
        logger.debug(f"Position opened: ID={trade_id}, Direction={direction.to_string()}, "
                    f"Entry={entry_price:.2f}, SL={stop_loss:.2f}, TP={take_profit:.2f}")
    
    def close_positions(self, trade_ids: List[int]):
        """
        Remove closed positions from state.
        
        Args:
            trade_ids: List of position IDs to close
        """
        # Convert to set for O(1) lookup performance
        trade_ids_set = set(trade_ids)
        
        # Filter out closed positions
        self.current_positions = [
            pos for pos in self.current_positions 
            if pos.position_id not in trade_ids_set
        ]
        
        logger.debug(f"Positions closed: IDs={trade_ids}, Remaining={len(self.current_positions)}")
    
    def has_open_position(self) -> bool:
        """Check if any positions are currently open"""
        return len(self.current_positions) > 0
    
    def get_current_positions(self) -> List[Position]:
        """
        Get copy of current positions list.
        
        Returns:
            List of Position contracts (immutable)
        """
        return self.current_positions.copy()
    
    def get_metrics(self) -> Dict:
        """
        Get copy of metrics dictionary.
        
        Returns:
            Dict with signal counts and rejection reasons
        """
        return self.metrics.copy()
    
    def reset(self):
        """Reset all state and metrics to initial values"""
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
        logger.debug("TradeManager state reset")
    
    # ========================================================================
    # BACKWARD COMPATIBILITY METHODS
    # ========================================================================
    
    def handle_signal_legacy(
        self, 
        timestamp: datetime, 
        signal_type: str
    ) -> Dict:
        """
        Legacy method - returns dict instead of TradeDecision.
        
        DEPRECATED: Use handle_signal() which returns TradeDecision contract.
        This method exists for backward compatibility during migration.
        
        WARNING: This method uses placeholder values (0.0) for price parameters
        since they are not available in the legacy signature. The Position
        contracts created will have invalid price data.
        
        Use only temporarily during migration. Update call sites to use
        handle_signal() with full parameters.
        
        Args:
            timestamp: Signal timestamp (datetime)
            signal_type: 'BUY' or 'SELL'
        
        Returns:
            Dict with keys: action, reason, close_trade_ids, new_trade_id
            (Legacy format - matches pre-migration TradeManager)
        """
        logger.warning("Using deprecated handle_signal_legacy() - update to handle_signal() with price parameters")
        
        # Convert to new signature with placeholder prices
        decision = self.handle_signal(
            timestamp=pd.Timestamp(timestamp),
            signal_type=signal_type,
            entry_price=0.0,  # Placeholder - not available in legacy call
            stop_loss=0.0,    # Placeholder
            take_profit=0.0,  # Placeholder
            position_size=1.0,
            meta={'legacy_call': True}
        )
        
        # Convert TradeDecision → legacy dict
        return decision.to_dict()
    
    def open_position_legacy(
        self, 
        trade_id: int, 
        timestamp: datetime, 
        direction: str
    ):
        """
        Legacy method for backward compatibility.
        
        DEPRECATED: Use open_position() with full price parameters.
        
        WARNING: Creates Position contract with placeholder prices (0.0).
        This will create invalid Position objects.
        
        Args:
            trade_id: Position ID
            timestamp: Entry timestamp (datetime)
            direction: 'BUY' or 'SELL' string
        """
        logger.warning("Using deprecated open_position_legacy() - update to open_position() with price parameters")
        
        self.open_position(
            trade_id=trade_id,
            timestamp=pd.Timestamp(timestamp),
            direction=TradeDirection.from_string(direction),
            entry_price=0.0,  # Placeholder - not available
            stop_loss=0.0,    # Placeholder
            take_profit=0.0,  # Placeholder
            position_size=1.0,
            meta={'legacy_call': True}
        )
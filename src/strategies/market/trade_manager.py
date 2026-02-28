"""Trade position management: handles position opening/closing logic and pyramiding
Version: 2.1.0
"""
import logging
from typing import Dict, Optional, List, Any
import pandas as pd

from src.strategies.contracts.trade_contracts import (
    TradeDecision,
    DecisionType,
    TradeDirection
)
from src.strategies.contracts.position_contracts import Position
from src.strategies.config.config_schema import StrategyConfig

logger = logging.getLogger(__name__)


class TradeManager:
    """
    Manages trading positions with configurable pyramiding and reversal logic.

    ID System:
    - position_id: Sequential position identifier used for tracking and closing
    """

    def __init__(self, config: StrategyConfig):
        """
        Initialize TradeManager with StrategyConfig.

        Args:
            config: StrategyConfig instance (typed)
        """
        position_config = config.trade_management.position_control

        self.close_on_opposite = position_config.close_on_opposite
        self.pyramiding_enabled = position_config.pyramiding_enabled
        self.max_positions = position_config.max_positions

        self.current_positions: List[Position] = []
        self.trade_counter = 0

        self.metrics = {
            'total_signals_received': 0,
            'signals_accepted': 0,
            'signals_rejected': 0,
            'rejected_reasons': {
                'pyramiding_disabled': 0,
                'opposite_ignored': 0,
                'max_positions_reached': 0,
            },
            'positions_closed_by_opposite': 0,
            'positions_reversed': 0,
        }

        logger.info("TradeManager initialized:")
        logger.info(f"  Close on Opposite: {self.close_on_opposite}")
        logger.info(f"  Pyramiding: {'ENABLED' if self.pyramiding_enabled else 'DISABLED'}")
        logger.info(f"  Max Positions: {self.max_positions}")

    @property
    def current_direction(self) -> Optional[TradeDirection]:
        """Returns current position direction or None if no positions."""
        return self.current_positions[0].direction if self.current_positions else None

    def handle_signal_position_only(
        self,
        timestamp: pd.Timestamp,
        signal_type: str,
    ) -> TradeDecision:
        """
        Handle incoming signal based ONLY on position rules (Legacy order).
        
        This is called FIRST in the sequence to decide if we can even consider
        opening a position, before calculating risk parameters.

        Args:
            timestamp: Signal timestamp (pandas Timestamp)
            signal_type: 'BUY' or 'SELL'

        Returns:
            TradeDecision contract with decision type, reason, and action details
        """
        self.metrics['total_signals_received'] += 1
        direction = TradeDirection.from_string(signal_type)

        # Case 1: No positions — can open new
        if not self.current_positions:
            self.metrics['signals_accepted'] += 1
            return TradeDecision(
                decision_type=DecisionType.OPEN,
                reason=f'Opening {direction.to_string()} position (no positions open)',
                close_trade_ids=None,
                new_trade_id=self.trade_counter + 1,
            )

        is_same_direction = direction == self.current_direction

        # Case 2: Same direction signal
        if is_same_direction:
            # Check max positions limit
            if len(self.current_positions) >= self.max_positions:
                self.metrics['signals_rejected'] += 1
                self.metrics['rejected_reasons']['max_positions_reached'] += 1
                return TradeDecision(
                    decision_type=DecisionType.REJECT,
                    reason=f'Max positions ({self.max_positions}) reached',
                    close_trade_ids=None,
                    new_trade_id=None,
                )

            if self.pyramiding_enabled:
                self.metrics['signals_accepted'] += 1
                return TradeDecision(
                    decision_type=DecisionType.OPEN,
                    reason=f'Pyramiding: adding to {direction.to_string()} position',
                    close_trade_ids=None,
                    new_trade_id=self.trade_counter + 1,
                )
            else:
                self.metrics['signals_rejected'] += 1
                self.metrics['rejected_reasons']['pyramiding_disabled'] += 1
                return TradeDecision(
                    decision_type=DecisionType.REJECT,
                    reason=f'Pyramiding disabled — {direction.to_string()} position already open',
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
                    reason=(
                        f'Closing {len(close_trade_ids)} {self.current_direction.to_string()} '
                        f'positions and reversing to {direction.to_string()}'
                    ),
                    close_trade_ids=close_trade_ids,
                    new_trade_id=self.trade_counter + 1,
                )
            else:
                self.metrics['signals_rejected'] += 1
                self.metrics['rejected_reasons']['opposite_ignored'] += 1
                return TradeDecision(
                    decision_type=DecisionType.REJECT,
                    reason=f'Opposite signal ignored — {self.current_direction.to_string()} positions still open',
                    close_trade_ids=None,
                    new_trade_id=None,
                )

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
        Handle incoming signal with full trade parameters (for when we already know we're opening).
        
        This is called AFTER risk parameters are calculated, when we know we're opening a position.

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
        """
        self.metrics['total_signals_received'] += 1
        direction = TradeDirection.from_string(signal_type)

        # By the time this is called, we've already passed position checks
        # So we should always be able to open
        self.metrics['signals_accepted'] += 1
        return TradeDecision(
            decision_type=DecisionType.OPEN,
            reason=f'Opening {direction.to_string()} position with risk parameters',
            close_trade_ids=None,
            new_trade_id=self.trade_counter + 1,
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

        logger.debug(
            f"Position opened: ID={trade_id}, Direction={direction.to_string()}, "
            f"Entry={entry_price:.2f}, SL={stop_loss:.2f}, TP={take_profit:.2f}"
        )

    def close_positions(self, trade_ids: List[int]):
        """
        Remove closed positions from state.

        Args:
            trade_ids: List of position IDs to close
        """
        trade_ids_set = set(trade_ids)
        self.current_positions = [
            pos for pos in self.current_positions
            if pos.position_id not in trade_ids_set
        ]
        logger.debug(f"Positions closed: IDs={trade_ids}, Remaining={len(self.current_positions)}")

    def has_open_position(self) -> bool:
        """Check if any positions are currently open."""
        return len(self.current_positions) > 0

    def get_current_positions(self) -> List[Position]:
        """Get copy of current positions list."""
        return self.current_positions.copy()

    def get_metrics(self) -> Dict:
        """Get copy of metrics dictionary."""
        return self.metrics.copy()

    def reset(self):
        """Reset all state and metrics to initial values."""
        self.current_positions = []
        self.trade_counter = 0
        self.metrics = {
            'total_signals_received': 0,
            'signals_accepted': 0,
            'signals_rejected': 0,
            'rejected_reasons': {
                'pyramiding_disabled': 0,
                'opposite_ignored': 0,
                'max_positions_reached': 0,
            },
            'positions_closed_by_opposite': 0,
            'positions_reversed': 0,
        }
        logger.debug("TradeManager state reset")
"""
Position Contracts - Position Tracking
Version: 1.0.0
Position tracking for TradeManager.
Represents an open position in the system.
"""
from dataclasses import dataclass, field
from typing import Dict, Any
import pandas as pd

from .trade_contracts import TradeDirection

__all__ = ['Position']

@dataclass(frozen=True)
class Position:
    """
    Represents an open trading position.
    
    Used by TradeManager to track currently open positions.
    Lighter weight than TradeEntry (no risk metrics, etc).
    """
    # Identity
    position_id: int                            # Unique position ID
    
    # Position details
    direction: TradeDirection                   # LONG or SHORT
    entry_price: float                          # Entry price
    stop_loss: float                            # Stop loss level
    take_profit: float                          # Take profit level
    size: float                                 # Position size (contracts/shares)
    
    # Timing
    open_time: pd.Timestamp                     # When position was opened
    
    # Metadata
    meta: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate position data"""
        if self.entry_price <= 0:
            raise ValueError("Entry price must be positive")
        if self.size <= 0:
            raise ValueError("Position size must be positive")
        
        # Validate SL/TP positioning
        if self.direction == TradeDirection.LONG:
            if not (self.stop_loss < self.entry_price < self.take_profit):
                raise ValueError(
                    f"Invalid LONG position: SL({self.stop_loss}) < "
                    f"Entry({self.entry_price}) < TP({self.take_profit})"
                )
        else:  # SHORT
            if not (self.take_profit < self.entry_price < self.stop_loss):
                raise ValueError(
                    f"Invalid SHORT position: TP({self.take_profit}) < "
                    f"Entry({self.entry_price}) < SL({self.stop_loss})"
                )
    
    @property
    def is_long(self) -> bool:
        """Is this a LONG position?"""
        return self.direction == TradeDirection.LONG
    
    @property
    def is_short(self) -> bool:
        """Is this a SHORT position?"""
        return self.direction == TradeDirection.SHORT
    
    @property
    def sl_distance(self) -> float:
        """Get stop loss distance in points"""
        return abs(self.entry_price - self.stop_loss)
    
    @property
    def tp_distance(self) -> float:
        """Get take profit distance in points"""
        return abs(self.take_profit - self.entry_price)
    
    @property
    def risk_reward_ratio(self) -> float:
        """Get risk:reward ratio"""
        sl_dist = self.sl_distance
        return self.tp_distance / sl_dist if sl_dist > 0 else 0.0
    
    def get_unrealized_pnl(self, current_price: float) -> float:
        """
        Calculate unrealized P&L in points.
        
        Args:
            current_price: Current market price
            
        Returns:
            Unrealized P&L in points (positive = profit, negative = loss)
        """
        if self.is_long:
            return current_price - self.entry_price
        else:
            return self.entry_price - current_price
    
    def get_unrealized_pnl_percent(self, current_price: float) -> float:
        """
        Calculate unrealized P&L as percentage of entry.
        
        Args:
            current_price: Current market price
            
        Returns:
            Unrealized P&L as percentage
        """
        pnl_points = self.get_unrealized_pnl(current_price)
        return (pnl_points / self.entry_price) * 100 if self.entry_price else 0.0
    
    def is_sl_hit(self, current_price: float, tolerance: float = 0.0) -> bool:
        """
        Check if stop loss is hit at current price.
        
        Args:
            current_price: Current market price
            tolerance: Price tolerance (in points) for SL trigger
            
        Returns:
            True if SL is hit
        """
        if self.is_long:
            return current_price <= (self.stop_loss + tolerance)
        else:
            return current_price >= (self.stop_loss - tolerance)
    
    def is_tp_hit(self, current_price: float, tolerance: float = 0.0) -> bool:
        """
        Check if take profit is hit at current price.
        
        Args:
            current_price: Current market price
            tolerance: Price tolerance (in points) for TP trigger
            
        Returns:
            True if TP is hit
        """
        if self.is_long:
            return current_price >= (self.take_profit - tolerance)
        else:
            return current_price <= (self.take_profit + tolerance)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict"""
        return {
            'position_id': self.position_id,
            'direction': self.direction.to_string(),
            'entry_price': self.entry_price,
            'stop_loss': self.stop_loss,
            'take_profit': self.take_profit,
            'size': self.size,
            'open_time': self.open_time,
            'sl_distance': self.sl_distance,
            'tp_distance': self.tp_distance,
            'risk_reward_ratio': self.risk_reward_ratio,
        }
    
    def __str__(self) -> str:
        """String representation"""
        return (
            f"Position({self.position_id}, {self.direction.to_string()}, "
            f"Entry: {self.entry_price:.2f}, SL: {self.stop_loss:.2f}, "
            f"TP: {self.take_profit:.2f})"
        )
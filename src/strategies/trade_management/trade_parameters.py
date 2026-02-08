from dataclasses import dataclass, field
from typing import Optional, Dict, Any

from .trade_direction import TradeDirection


@dataclass(frozen=True)
class TradeParameters:
    direction: TradeDirection
    entry: float
    stop_loss: float
    take_profit: float
    size: float

    risk_pct: Optional[float] = None
    tag: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)

    def validate(self):
        if self.entry <= 0:
            raise ValueError("Entry price must be positive")
        if self.size <= 0:
            raise ValueError("Trade size must be positive")

        if self.direction == TradeDirection.LONG:
            if not (self.stop_loss < self.entry < self.take_profit):
                raise ValueError("Invalid SL/TP for LONG trade")

        if self.direction == TradeDirection.SHORT:
            if not (self.take_profit < self.entry < self.stop_loss):
                raise ValueError("Invalid SL/TP for SHORT trade")
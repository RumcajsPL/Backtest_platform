from dataclasses import dataclass, field
from typing import Dict, Any
import pandas as pd

from .trade_direction import TradeDirection


@dataclass(frozen=True)
class Position:
    direction: TradeDirection
    entry: float
    stop_loss: float
    take_profit: float
    size: float
    open_time: pd.Timestamp
    meta: Dict[str, Any] = field(default_factory=dict)

    def is_long(self) -> bool:
        return self.direction == TradeDirection.LONG

    def is_short(self) -> bool:
        return self.direction == TradeDirection.SHORT
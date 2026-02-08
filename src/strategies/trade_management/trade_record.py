from dataclasses import dataclass, field
from typing import Dict, Any
import pandas as pd

from .trade_direction import TradeDirection

@dataclass(frozen=True)
class TradeRecord:
    direction: TradeDirection
    entry: float
    exit: float
    size: float

    open_time: pd.Timestamp
    close_time: pd.Timestamp

    pnl: float
    pnl_pct: float

    reason_open: str
    reason_close: str

    meta: Dict[str, Any] = field(default_factory=dict)
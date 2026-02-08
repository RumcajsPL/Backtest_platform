from dataclasses import dataclass, field
from typing import Optional, Dict, Any
import pandas as pd

@dataclass(frozen=True)
class SignalFrame:
    timestamp: pd.Timestamp
    open: float
    high: float
    low: float
    close: float
    volume: float

    htf: Optional[pd.Series] = None
    ltf: Optional[pd.DataFrame] = None

    indicators: Dict[str, Any] = field(default_factory=dict)
    state: Dict[str, Any] = field(default_factory=dict)

    def get_indicator(self, name: str, default=None):
        return self.indicators.get(name, default)

    def get_state(self, key: str, default=None):
        return self.state.get(key, default)
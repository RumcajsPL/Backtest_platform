from dataclasses import dataclass, field
from typing import Optional, Dict, Any

from .decision_type import DecisionType
from .trade_parameters import TradeParameters


@dataclass(frozen=True)
class TradeDecision:
    decision_type: DecisionType
    trade_params: Optional[TradeParameters] = None

    confidence: Optional[float] = None
    reason: Optional[str] = None
    tags: Dict[str, Any] = field(default_factory=dict)

    def validate(self):
        if self.decision_type in (DecisionType.OPEN, DecisionType.REVERSE):
            if self.trade_params is None:
                raise ValueError(f"{self.decision_type.name} requires trade_params")
            self.trade_params.validate()
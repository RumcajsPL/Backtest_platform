"""
Trade Management Package for Backtesting Platform.
"""



__all__ = ['RiskManager', 'TradeManager']

"""
Trade Management Package for Backtesting Platform.
Provides core trading contracts and execution structures.
"""
from .risk_manager import RiskManager
from .trade_manager import TradeManager
from .spread_manager import SpreadManager
from .trade_direction import TradeDirection
from .decision_type import DecisionType
from .trade_parameters import TradeParameters
from .trade_decision import TradeDecision
from .position import Position
from .trade_record import TradeRecord
from .signal_frame import SignalFrame

__all__ = [
    "RiskManager",
    "TradeManager",
    "SpreadManager",
    "TradeDirection",
    "DecisionType",
    "TradeParameters",
    "TradeDecision",
    "Position",
    "TradeRecord",
    "SignalFrame",
]
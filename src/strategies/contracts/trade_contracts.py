"""
Trade Contracts - Phase 4 Migration
Version: 1.4.0
Date: 2026-02-27

Changes from v1.3.0:
- [C3] TradeExit.create(): added duration_bars parameter (int, default 0).
  Previously duration_bars was declared on the dataclass with default 0 but was
  never passed by any call site — every trade silently reported 0 bars duration,
  causing trade_analytics._analyze_duration_patterns() to classify 100% of trades
  as fast exits (< 3 bars). The value is now computed in TradeSimulator (where
  df_strategy is available) and passed explicitly into this factory method.
  duration_minutes is unchanged — it continues to be computed here from wall-clock
  timestamps and serves a complementary purpose (absolute time, not bar count).
"""
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, Any, Optional, List
import pandas as pd

__all__ = [
    'TradeDirection',
    'ExitReason',
    'TradeParameters',
    'TradeEntry',
    'TradeExit',
    'Trade',
    'RejectedSignal',
    'TradeResult',
    'DecisionType',
    'TradeDecision',
]


# ============================================================================
# ENUMS
# ============================================================================

class TradeDirection(Enum):
    """Trade direction with integer values for compatibility"""
    LONG = 1
    SHORT = -1

    @classmethod
    def from_string(cls, direction: str) -> 'TradeDirection':
        """Convert string to TradeDirection"""
        direction_upper = direction.upper()
        if direction_upper in ('BUY', 'LONG'):
            return cls.LONG
        elif direction_upper in ('SELL', 'SHORT'):
            return cls.SHORT
        else:
            raise ValueError(f"Invalid direction: {direction}")

    def to_string(self) -> str:
        """Convert to legacy string format"""
        return "BUY" if self == TradeDirection.LONG else "SELL"

    @property
    def is_long(self) -> bool:
        return self == TradeDirection.LONG

    @property
    def is_short(self) -> bool:
        return self == TradeDirection.SHORT


class ExitReason(Enum):
    """Reason for trade exit"""
    STOP_LOSS = auto()
    TAKE_PROFIT = auto()
    OPPOSITE_SIGNAL = auto()
    END_OF_DATA = auto()
    MANUAL = auto()              # Reserved for future
    TIME_EXIT = auto()           # Reserved for future

    @classmethod
    def from_string(cls, reason: str) -> 'ExitReason':
        """Convert string to ExitReason"""
        reason_upper = reason.upper()
        try:
            return cls[reason_upper]
        except KeyError:
            raise ValueError(f"Invalid exit reason: {reason}")

    def to_string(self) -> str:
        """Convert to legacy string format"""
        return self.name


class DecisionType(Enum):
    """Trade manager decision types"""
    NONE = auto()
    OPEN = auto()
    CLOSE = auto()
    REVERSE = auto()
    MODIFY = auto()
    REJECT = auto()
    CLOSE_AND_REVERSE = auto()

    @classmethod
    def from_string(cls, decision: str) -> 'DecisionType':
        """Convert string to DecisionType"""
        decision_upper = decision.upper()
        try:
            return cls[decision_upper]
        except KeyError:
            raise ValueError(f"Invalid decision type: {decision}")


# ============================================================================
# TRADE PARAMETERS
# ============================================================================

@dataclass(frozen=True)
class TradeParameters:
    """
    Trade parameters from risk management calculations.

    Maps to RiskManager.compute_trade_parameters() output.
    Contains all information needed to open a position and manage its exit.

    TP trigger fields (DEC-037, DEC-038)
    --------------------------------------
    take_profit is the raw TP price (mid/bid level).
    take_profit_trigger is the actual price at which the exit fires:
      - LONG:  equals take_profit (TP hit at bid, spread already paid at open)
      - SHORT: equals take_profit + spread (exit at Ask = TP_bid + spread)
    tp_mode records which calculation branch produced the TP distance.
    spread_at_tp_exit records the spread cost embedded in take_profit_trigger
    for SHORT trades; None for LONG (analytics use only).
    """
    # Core execution prices
    entry_price_mid: float
    entry_price_executed: float
    stop_loss_raw: float
    stop_loss_trigger: float
    take_profit: float

    # TP trigger and mode (DEC-037, DEC-038) — [C2] fields previously missing
    take_profit_trigger: Optional[float] = None   # Actual exit trigger price (spread-adjusted for SHORT)
    tp_mode: Optional[str] = None                 # 'rr_ratio' | 'atr_multiplier'

    # Position sizing
    position_size: float = 1.0

    # Risk metrics
    atr_value: Optional[float] = None
    atr_length: Optional[int] = None
    atr_multiplier: Optional[float] = None
    sl_distance: Optional[float] = None
    tp_distance: Optional[float] = None
    risk_reward_ratio: Optional[float] = None

    # Annual range validation
    annual_range_value: Optional[float] = None
    risk_percentile_calculated: Optional[float] = None
    max_risk_percentile: Optional[float] = None
    risk_percentile_passed: bool = True

    # Spread details
    spread_enabled: bool = False
    spread_applied: bool = False
    spread_type: Optional[str] = None
    spread_value: Optional[float] = None
    spread_points: Optional[float] = None
    spread_cost: Optional[float] = None
    spread_efficiency_percent: Optional[float] = None
    spread_at_tp_exit: Optional[float] = None     # SHORT only: spread cost at TP exit [C2]

    # Adjustments
    sl_adjusted: bool = False
    sl_distance_raw: Optional[float] = None
    sl_price_raw: Optional[float] = None

    # Metadata
    comment: Optional[str] = None
    tag: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate trade parameters"""
        if self.entry_price_executed <= 0:
            raise ValueError("Entry price must be positive")
        if self.position_size <= 0:
            raise ValueError("Position size must be positive")

    @classmethod
    def from_risk_manager_output(
        cls,
        risk_output: Dict[str, Any],
        position_size: float = 1.0,
        **kwargs
    ) -> 'TradeParameters':
        """Create TradeParameters from RiskManager.compute_trade_parameters() output."""
        executed_entry = risk_output['executed_entry']
        trigger_sl = risk_output['trigger_sl']
        tp = risk_output['tp']

        sl_distance = abs(executed_entry - trigger_sl)
        tp_distance = abs(tp - executed_entry)
        rr_ratio = tp_distance / sl_distance if sl_distance > 0 else 0

        return cls(
            entry_price_mid=risk_output.get('entry_price_mid', executed_entry),
            entry_price_executed=executed_entry,
            stop_loss_raw=risk_output['raw_sl'],
            stop_loss_trigger=trigger_sl,
            take_profit=tp,
            # [C2] New fields — read from output dict if present, default to None
            take_profit_trigger=risk_output.get('take_profit_trigger'),
            tp_mode=risk_output.get('tp_mode'),
            position_size=position_size,
            sl_distance=sl_distance,
            tp_distance=tp_distance,
            risk_reward_ratio=rr_ratio,
            spread_enabled=risk_output.get('spread_enabled', False),
            spread_applied=risk_output.get('spread_applied', False),
            spread_value=risk_output.get('spread_value', 0.0),
            spread_points=risk_output.get('spread_points', 0.0),
            spread_cost=risk_output.get('spread_cost', 0.0),
            spread_at_tp_exit=risk_output.get('spread_at_tp_exit'),  # [C2]
            sl_adjusted=risk_output.get('sl_adjusted', False),
            comment=risk_output.get('comment', ''),
            **kwargs
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict."""
        return {
            'entry_price_mid': self.entry_price_mid,
            'executed_entry': self.entry_price_executed,
            'raw_sl': self.stop_loss_raw,
            'trigger_sl': self.stop_loss_trigger,
            'tp': self.take_profit,
            # [C2] New fields included in serialised output
            'take_profit_trigger': self.take_profit_trigger,
            'tp_mode': self.tp_mode,
            'position_size': self.position_size,
            'sl_distance': self.sl_distance,
            'tp_distance': self.tp_distance,
            'risk_reward_ratio': self.risk_reward_ratio,
            'spread_enabled': self.spread_enabled,
            'spread_applied': self.spread_applied,
            'spread_value': self.spread_value,
            'spread_points': self.spread_points,
            'spread_cost': self.spread_cost,
            'spread_at_tp_exit': self.spread_at_tp_exit,  # [C2]
            'sl_adjusted': self.sl_adjusted,
            'comment': self.comment,
        }


# ============================================================================
# TRADE ENTRY
# ============================================================================

@dataclass(frozen=True)
class TradeEntry:
    """
    Immutable trade entry state.

    Represents a position that has been opened.
    """
    # Identity
    entry_id: str
    trade_manager_id: Optional[int] = None
    position_id: Optional[int] = None
    signal_id: Optional[int] = None

    # Timing
    entry_time: pd.Timestamp = field(default_factory=pd.Timestamp.now)

    # Trade details
    direction: TradeDirection = TradeDirection.LONG
    entry_price: float = 0.0
    stop_loss: float = 0.0
    take_profit: float = 0.0
    position_size: float = 1.0

    # Risk metrics (at entry)
    sl_distance: float = 0.0
    tp_distance: float = 0.0
    risk_reward_ratio: float = 0.0
    atr_value: Optional[float] = None
    risk_percentile: Optional[float] = None

    # Execution details
    spread_enabled: bool = False
    spread_points: Optional[float] = None
    spread_cost: Optional[float] = None
    sl_adjusted: bool = False

    # Metadata
    comment: Optional[str] = None
    tag: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate entry data"""
        if self.entry_price <= 0:
            raise ValueError("Entry price must be positive")
        if self.position_size <= 0:
            raise ValueError("Position size must be positive")

    @property
    def is_long(self) -> bool:
        return self.direction == TradeDirection.LONG

    @property
    def is_short(self) -> bool:
        return self.direction == TradeDirection.SHORT

    @classmethod
    def from_trade_parameters(
        cls,
        entry_id: str,
        timestamp: pd.Timestamp,
        direction: TradeDirection,
        params: TradeParameters,
        trade_manager_id: Optional[int] = None,
        signal_id: Optional[int] = None,
        **kwargs
    ) -> 'TradeEntry':
        """Create TradeEntry from TradeParameters"""
        return cls(
            entry_id=entry_id,
            trade_manager_id=trade_manager_id,
            position_id=trade_manager_id,
            signal_id=signal_id,
            entry_time=timestamp,
            direction=direction,
            entry_price=params.entry_price_executed,
            stop_loss=params.stop_loss_trigger,
            take_profit=params.take_profit,
            position_size=params.position_size,
            sl_distance=params.sl_distance or 0.0,
            tp_distance=params.tp_distance or 0.0,
            risk_reward_ratio=params.risk_reward_ratio or 0.0,
            atr_value=params.atr_value,
            risk_percentile=params.risk_percentile_calculated,
            spread_enabled=params.spread_enabled,
            spread_points=params.spread_points,
            spread_cost=params.spread_cost,
            sl_adjusted=params.sl_adjusted,
            comment=params.comment,
            **kwargs
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict."""
        return {
            'trade_id': self.entry_id,
            'trade_manager_trade_id': self.trade_manager_id,
            'position_id': self.position_id,
            'signal_id': self.signal_id,
            'entry_time': self.entry_time,
            'direction': self.direction.to_string(),
            'entry_price': self.entry_price,
            'sl_price': self.stop_loss,
            'tp_price': self.take_profit,
            'sl_distance': self.sl_distance,
            'tp_distance': self.tp_distance,
            'risk_reward_ratio': self.risk_reward_ratio,
            'comment': self.comment or '',
            'status': 'OPEN',
        }


# ============================================================================
# TRADE EXIT
# ============================================================================

@dataclass(frozen=True)
class TradeExit:
    """
    Immutable trade exit state.
    """
    # Identity
    exit_id: str
    entry_id: str

    # Timing
    exit_time: pd.Timestamp = field(default_factory=pd.Timestamp.now)
    duration_bars: int = 0
    duration_minutes: float = 0.0

    # Exit details
    exit_price: float = 0.0
    exit_reason: ExitReason = ExitReason.END_OF_DATA

    # P&L
    pnl_points: float = 0.0
    pnl_percent: float = 0.0
    is_win: bool = False
    is_loss: bool = False

    # LTF execution details
    exit_bar_high: Optional[float] = None
    exit_bar_low: Optional[float] = None
    ltf_execution: bool = False
    ltf_execution_mode: Optional[str] = None

    # Metadata
    comment: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        entry: TradeEntry,
        exit_time: pd.Timestamp,
        exit_price: float,
        exit_reason: ExitReason,
        duration_bars: int = 0,          # [C3] computed by TradeSimulator, passed explicitly
        exit_bar_high: Optional[float] = None,
        exit_bar_low: Optional[float] = None,
        ltf_execution: bool = False,
        ltf_execution_mode: Optional[str] = None,
        **kwargs
    ) -> 'TradeExit':
        """
        Create TradeExit from TradeEntry and exit details.

        duration_bars must be supplied by the caller (TradeSimulator) as a
        count of strategy-TF bars elapsed between entry and exit.  It cannot
        be computed here because this factory has no access to df_strategy.
        duration_minutes is still derived from wall-clock timestamps and
        remains a complementary field (absolute elapsed time).
        """
        if entry.is_long:
            pnl_points = exit_price - entry.entry_price
        else:
            pnl_points = entry.entry_price - exit_price

        pnl_percent = (pnl_points / entry.entry_price) * 100 if entry.entry_price else 0
        duration_minutes = (exit_time - entry.entry_time).total_seconds() / 60
        exit_id = f"{entry.entry_id}_EXIT"

        return cls(
            exit_id=exit_id,
            entry_id=entry.entry_id,
            exit_time=exit_time,
            exit_price=exit_price,
            exit_reason=exit_reason,
            pnl_points=pnl_points,
            pnl_percent=pnl_percent,
            is_win=pnl_points > 0,
            is_loss=pnl_points < 0,
            duration_bars=duration_bars,         # [C3] passed through from TradeSimulator
            duration_minutes=duration_minutes,
            exit_bar_high=exit_bar_high,
            exit_bar_low=exit_bar_low,
            ltf_execution=ltf_execution,
            ltf_execution_mode=ltf_execution_mode,
            **kwargs
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict."""
        return {
            'exit_time': self.exit_time,
            'exit_price': self.exit_price,
            'exit_reason': self.exit_reason.to_string(),
            'pnl_points': self.pnl_points,
            'pnl_percent': self.pnl_percent,
            'duration_minutes': self.duration_minutes,
            'duration_bars': self.duration_bars,
            'is_win': self.is_win,
            'is_loss': self.is_loss,
            'exit_bar_high': self.exit_bar_high,
            'exit_bar_low': self.exit_bar_low,
        }


# ============================================================================
# TRADE (ENTRY + EXIT)
# ============================================================================

@dataclass(frozen=True)
class Trade:
    """Complete trade: entry + optional exit."""
    entry: TradeEntry
    exit: Optional[TradeExit] = None

    @property
    def is_open(self) -> bool:
        return self.exit is None

    @property
    def is_closed(self) -> bool:
        return self.exit is not None

    @property
    def trade_id(self) -> str:
        return self.entry.entry_id

    @property
    def status(self) -> str:
        return "CLOSED" if self.is_closed else "OPEN"

    @property
    def direction(self) -> TradeDirection:
        return self.entry.direction

    @property
    def entry_time(self) -> pd.Timestamp:
        return self.entry.entry_time

    @property
    def exit_time(self) -> Optional[pd.Timestamp]:
        return self.exit.exit_time if self.exit else None

    @property
    def duration_bars(self) -> Optional[int]:
        return self.exit.duration_bars if self.exit else None

    @property
    def duration_minutes(self) -> Optional[float]:
        return self.exit.duration_minutes if self.exit else None

    @property
    def pnl_points(self) -> Optional[float]:
        return self.exit.pnl_points if self.exit else None

    @property
    def pnl_percent(self) -> Optional[float]:
        return self.exit.pnl_percent if self.exit else None

    @property
    def is_win(self) -> bool:
        return self.exit.is_win if self.exit else False

    @property
    def is_loss(self) -> bool:
        return self.exit.is_loss if self.exit else False

    @property
    def exit_reason(self) -> Optional[ExitReason]:
        return self.exit.exit_reason if self.exit else None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict."""
        return {
            "trade_id": self.entry.entry_id,
            "trade_manager_trade_id": self.entry.trade_manager_id,
            "position_id": self.entry.position_id,
            "signal_id": self.entry.signal_id,
            "status": self.status,
            "entry_time": self.entry.entry_time,
            "direction": self.entry.direction.to_string(),
            "entry_price": self.entry.entry_price,
            "sl_price": self.entry.stop_loss,
            "tp_price": self.entry.take_profit,
            "sl_distance": self.entry.sl_distance,
            "tp_distance": self.entry.tp_distance,
            "risk_reward_ratio": self.entry.risk_reward_ratio,
            "comment": self.entry.comment or '',
            "exit_time": self.exit.exit_time if self.exit else None,
            "exit_price": self.exit.exit_price if self.exit else None,
            "exit_reason": self.exit.exit_reason.to_string() if self.exit else None,
            "pnl_points": self.exit.pnl_points if self.exit else 0,
            "pnl_percent": self.exit.pnl_percent if self.exit else 0,
            "duration_bars": self.exit.duration_bars if self.exit else 0,
            "duration_minutes": self.exit.duration_minutes if self.exit else 0,
            "is_win": self.exit.is_win if self.exit else False,
            "is_loss": self.exit.is_loss if self.exit else False,
            "reject_reason": None,
        }

    def __str__(self) -> str:
        status = "OPEN" if self.is_open else f"CLOSED ({self.exit_reason.name})"
        pnl_str = f"{self.pnl_points:+.2f} pts" if self.is_closed else "N/A"
        return (
            f"Trade({self.trade_id}, {self.direction.to_string()}, "
            f"{status}, P&L: {pnl_str})"
        )


# ============================================================================
# REJECTED SIGNAL (NOT A TRADE)
# ============================================================================

@dataclass(frozen=True)
class RejectedSignal:
    """
    Represents a signal that was rejected before becoming a trade.

    Separate from Trade because rejected signals never had valid entry prices,
    stop loss / take profit levels, position sizing, or risk calculations.
    This is NOT a trade — it is a signal that failed to open a position.
    """
    # Identity
    rejection_id: str
    signal_id: Optional[int] = None

    # Timing
    rejection_time: pd.Timestamp = field(default_factory=pd.Timestamp.now)

    # Signal details
    direction: str = "BUY"  # String, not enum — it never became a trade

    # Rejection details
    rejection_stage: str = "UNKNOWN"
    rejection_reason: str = ""

    # Context (optional)
    current_price: Optional[float] = None
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dict.

        Does NOT try to match Trade dict format — rejected signals are
        fundamentally different from trades (DEC-031).
        """
        return {
            "rejection_id": self.rejection_id,
            "signal_id": self.signal_id,
            "rejection_time": self.rejection_time,
            "direction": self.direction,
            "rejection_stage": self.rejection_stage,
            "rejection_reason": self.rejection_reason,
            "current_price": self.current_price,
            "status": "REJECTED",
        }

    def __str__(self) -> str:
        return f"RejectedSignal({self.rejection_id}, {self.direction}, {self.rejection_reason})"


# ============================================================================
# TRADE RESULT (PIPELINE OUTPUT)
# ============================================================================

@dataclass(frozen=True)
class TradeResult:
    """
    Complete trade simulation result.

    Aggregates all trades and provides statistics.
    Maps to the output of trade_simulator.simulate_trades().
    """
    # Trades
    trades: List[Trade]
    rejected_signals: List[RejectedSignal]

    # Counts
    total_entries: int
    total_opened: int
    total_closed: int
    total_rejected: int
    currently_open: int

    # Exit breakdown
    exits_by_reason: Dict[str, int]

    # Risk statistics
    risk_approved: int = 0
    risk_rejected: int = 0
    risk_adjusted: int = 0

    # Position control statistics
    position_rejected: Dict[str, int] = field(default_factory=dict)
    trade_manager_metrics: Dict[str, Any] = field(default_factory=dict)

    # Performance metrics
    win_count: int = 0
    loss_count: int = 0
    win_rate: float = 0.0
    total_pnl_points: float = 0.0
    average_pnl_points: float = 0.0

    # Execution details
    execution_mode: str = "UNKNOWN"
    execution_time_ms: Optional[float] = None

    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def open_trades(self) -> List[Trade]:
        return [t for t in self.trades if t.is_open]

    @property
    def closed_trades(self) -> List[Trade]:
        return [t for t in self.trades if t.is_closed]

    @classmethod
    def from_trades(
        cls,
        trades: List[Trade],
        rejected_signals: List[RejectedSignal],
        exit_stats: Dict[str, int],
        risk_stats: Dict[str, Any],
        position_rejected: Dict[str, int],
        trade_manager_metrics: Dict[str, Any],
        execution_mode: str,
        execution_time_ms: Optional[float] = None,
    ) -> 'TradeResult':
        """Create TradeResult directly from simulation components."""
        closed_trades = [t for t in trades if t.is_closed]
        open_trades = [t for t in trades if t.is_open]

        win_count = sum(1 for t in closed_trades if t.is_win)
        loss_count = sum(1 for t in closed_trades if t.is_loss)
        win_rate = (win_count / len(closed_trades) * 100) if closed_trades else 0.0
        total_pnl = sum(t.pnl_points for t in closed_trades)
        avg_pnl = total_pnl / len(closed_trades) if closed_trades else 0.0
        total_entries = len(trades) + len(rejected_signals)

        return cls(
            trades=trades,
            rejected_signals=rejected_signals,
            total_entries=total_entries,
            total_opened=len(trades),
            total_closed=len(closed_trades),
            total_rejected=len(rejected_signals),
            currently_open=len(open_trades),
            exits_by_reason=exit_stats,
            risk_approved=risk_stats.get('total_approved', 0),
            risk_rejected=risk_stats.get('total_rejected', 0),
            risk_adjusted=risk_stats.get('total_adjusted', 0),
            position_rejected=position_rejected,
            trade_manager_metrics=trade_manager_metrics,
            win_count=win_count,
            loss_count=loss_count,
            win_rate=win_rate,
            total_pnl_points=total_pnl,
            average_pnl_points=avg_pnl,
            execution_mode=execution_mode,
            execution_time_ms=execution_time_ms,
        )

    def to_dataframe(self) -> pd.DataFrame:
        """Convert trades to DataFrame for analysis."""
        if not self.trades:
            return pd.DataFrame()
        return pd.DataFrame([t.to_dict() for t in self.trades])

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict. Uses RejectedSignal.to_dict() directly (no legacy format)."""
        return {
            'all_trades': [t.to_dict() for t in self.trades],
            'closed_trades': [t.to_dict() for t in self.closed_trades],
            'open_trades': [t.to_dict() for t in self.open_trades],
            'rejected_signals': [r.to_dict() for r in self.rejected_signals],
            'exit_stats': self.exits_by_reason,
            'risk_stats': {
                'total_approved': self.risk_approved,
                'total_rejected': self.risk_rejected,
                'total_adjusted': self.risk_adjusted,
            },
            'position_rejected_count': self.position_rejected,
            'trade_manager_metrics': self.trade_manager_metrics,
            'execution_mode': self.execution_mode,
        }

    def to_json(self, indent: Optional[int] = None) -> str:
        """Serialize TradeResult to JSON string."""
        import json
        result_dict = self.to_dict()

        def default_handler(obj):
            if isinstance(obj, pd.Timestamp):
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

        return json.dumps(result_dict, indent=indent, default=default_handler)

    def get_summary(self) -> str:
        """Get human-readable summary."""
        return (
            f"TradeResult Summary:\n"
            f"  Total Entries: {self.total_entries}\n"
            f"  Opened: {self.total_opened}\n"
            f"  Closed: {self.total_closed}\n"
            f"  Rejected: {self.total_rejected}\n"
            f"  Currently Open: {self.currently_open}\n"
            f"  Win Rate: {self.win_rate:.1f}%\n"
            f"  Total P&L: {self.total_pnl_points:+.2f} points\n"
            f"  Avg P&L: {self.average_pnl_points:+.2f} points/trade\n"
            f"  Execution: {self.execution_mode}"
        )

    def __str__(self) -> str:
        return self.get_summary()


# ============================================================================
# TRADE DECISION (TRADE MANAGER OUTPUT)
# ============================================================================

@dataclass(frozen=True)
class TradeDecision:
    """
    Trade manager decision.

    Maps to TradeManager.handle_signal() output.
    """
    decision_type: DecisionType
    reason: str
    close_trade_ids: Optional[List[int]] = None
    new_trade_id: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_trade_manager_result(cls, result: Dict[str, Any]) -> 'TradeDecision':
        """Create TradeDecision from TradeManager.handle_signal() output."""
        decision_type = DecisionType.from_string(result['action'])
        return cls(
            decision_type=decision_type,
            reason=result['reason'],
            close_trade_ids=result.get('close_trade_ids'),
            new_trade_id=result.get('new_trade_id'),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict."""
        return {
            'action': self.decision_type.name,
            'reason': self.reason,
            'close_trade_ids': self.close_trade_ids,
            'new_trade_id': self.new_trade_id,
        }

    @property
    def is_open(self) -> bool:
        return self.decision_type in (DecisionType.OPEN, DecisionType.CLOSE_AND_REVERSE)

    @property
    def is_close(self) -> bool:
        return self.decision_type in (DecisionType.CLOSE, DecisionType.CLOSE_AND_REVERSE)

    @property
    def is_reject(self) -> bool:
        return self.decision_type == DecisionType.REJECT
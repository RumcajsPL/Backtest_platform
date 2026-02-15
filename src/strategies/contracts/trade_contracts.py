"""
Trade Contracts - Phase 4 Migration
Version: 1.1.0 (Session 11)
Date: 2025-02-15

SESSION 11 CHANGES:
- TradeResult now uses List[RejectedSignal] instead of List[Dict]
- Added TradeResult.from_trades() classmethod for direct construction
- TradeResult.to_dict() handles rejected_signals conversion
- Complete contract-based architecture (no dicts in core flow)

Typed contracts for trade management, replacing dict-based trade structures.
Designed for zero-regression migration from legacy trade_simulator.py.

Architecture:
- Immutable dataclasses (frozen=True)
- Strong typing for all fields
- Conversion methods to/from legacy dict format
- Support for LTF execution and progressive tracking
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
    Contains all information needed to open a position.
    """
    # Core execution prices
    entry_price_mid: float                      # Mid/bid price (before spread)
    entry_price_executed: float                 # Actual execution price (after spread)
    stop_loss_raw: float                        # SL before spread trigger adjustment
    stop_loss_trigger: float                    # Chart SL that triggers exit
    take_profit: float                          # Take profit level
    
    # Position sizing
    position_size: float = 1.0                  # Number of contracts/shares
    
    # Risk metrics
    atr_value: Optional[float] = None
    atr_length: Optional[int] = None
    atr_multiplier: Optional[float] = None
    sl_distance: Optional[float] = None         # SL distance in points
    tp_distance: Optional[float] = None         # TP distance in points
    risk_reward_ratio: Optional[float] = None
    
    # Annual range validation (risk management)
    annual_range_value: Optional[float] = None
    risk_percentile_calculated: Optional[float] = None
    max_risk_percentile: Optional[float] = None
    risk_percentile_passed: bool = True
    
    # Spread details
    spread_enabled: bool = False
    spread_applied: bool = False
    spread_type: Optional[str] = None           # 'percentage', 'points', 'pips'
    spread_value: Optional[float] = None        # Raw spread value from config
    spread_points: Optional[float] = None       # Spread in price points
    spread_cost: Optional[float] = None         # Total spread cost
    spread_efficiency_percent: Optional[float] = None
    
    # Adjustments
    sl_adjusted: bool = False                   # Was SL adjusted for risk limits?
    sl_distance_raw: Optional[float] = None     # SL distance before adjustment
    sl_price_raw: Optional[float] = None        # SL price before adjustment
    
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
        """
        Create TradeParameters from RiskManager.compute_trade_parameters() output.
        
        Args:
            risk_output: Dict from risk_manager.compute_trade_parameters()
            position_size: Position size (contracts/shares)
            **kwargs: Additional fields to override/add
        """
        # Calculate distances
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
            position_size=position_size,
            sl_distance=sl_distance,
            tp_distance=tp_distance,
            risk_reward_ratio=rr_ratio,
            spread_enabled=risk_output.get('spread_enabled', False),
            spread_applied=risk_output.get('spread_applied', False),
            spread_value=risk_output.get('spread_value', 0.0),
            spread_points=risk_output.get('spread_points', 0.0),
            spread_cost=risk_output.get('spread_cost', 0.0),
            sl_adjusted=risk_output.get('sl_adjusted', False),
            comment=risk_output.get('comment', ''),
            **kwargs
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for legacy compatibility"""
        return {
            'entry_price_mid': self.entry_price_mid,
            'executed_entry': self.entry_price_executed,
            'raw_sl': self.stop_loss_raw,
            'trigger_sl': self.stop_loss_trigger,
            'tp': self.take_profit,
            'position_size': self.position_size,
            'sl_distance': self.sl_distance,
            'tp_distance': self.tp_distance,
            'risk_reward_ratio': self.risk_reward_ratio,
            'spread_enabled': self.spread_enabled,
            'spread_applied': self.spread_applied,
            'spread_value': self.spread_value,
            'spread_points': self.spread_points,
            'spread_cost': self.spread_cost,
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
    Maps to the 'OPEN' status in legacy trade dict.
    """
    # Identity
    entry_id: str                               # Unique ID (e.g., "T_20250213_143052_001")
    trade_manager_id: Optional[int] = None      # TradeManager's position ID
    position_id: Optional[int] = None           # Position tracking ID
    signal_id: Optional[int] = None             # Link to source signal
    
    # Timing
    entry_time: pd.Timestamp = field(default_factory=pd.Timestamp.now)
    
    # Trade details
    direction: TradeDirection = TradeDirection.LONG
    entry_price: float = 0.0                    # Executed entry price
    stop_loss: float = 0.0                      # SL trigger price
    take_profit: float = 0.0                    # TP price
    position_size: float = 1.0                  # Position size
    
    # Risk metrics (at entry)
    sl_distance: float = 0.0                    # SL distance in points
    tp_distance: float = 0.0                    # TP distance in points
    risk_reward_ratio: float = 0.0              # TP/SL ratio
    atr_value: Optional[float] = None           # ATR at entry time
    risk_percentile: Optional[float] = None     # Risk % of annual range
    
    # Execution details
    spread_enabled: bool = False
    spread_points: Optional[float] = None
    spread_cost: Optional[float] = None
    sl_adjusted: bool = False                   # Was SL adjusted for risk?
    
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
        """Convert to dict for legacy compatibility"""
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
    
    Represents the closing of a position.
    Contains P&L and exit details.
    """
    # Identity
    exit_id: str                                # Unique ID
    entry_id: str                               # Link to TradeEntry
    
    # Timing
    exit_time: pd.Timestamp = field(default_factory=pd.Timestamp.now)
    duration_bars: int = 0                      # Bars held
    duration_minutes: float = 0.0               # Minutes held
    
    # Exit details
    exit_price: float = 0.0                     # Actual exit price
    exit_reason: ExitReason = ExitReason.END_OF_DATA
    
    # P&L
    pnl_points: float = 0.0                     # P&L in price points
    pnl_percent: float = 0.0                    # P&L as % of entry
    is_win: bool = False                        # True if profitable
    is_loss: bool = False                       # True if loss
    
    # LTF execution details (if available)
    exit_bar_high: Optional[float] = None       # High of exit bar
    exit_bar_low: Optional[float] = None        # Low of exit bar
    ltf_execution: bool = False                 # Was LTF used?
    ltf_execution_mode: Optional[str] = None    # LTF mode (e.g., "NUMBA")
    
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
        exit_bar_high: Optional[float] = None,
        exit_bar_low: Optional[float] = None,
        ltf_execution: bool = False,
        ltf_execution_mode: Optional[str] = None,
        **kwargs
    ) -> 'TradeExit':
        """
        Create TradeExit from TradeEntry and exit details.
        Automatically calculates P&L and duration.
        """
        # Calculate P&L
        if entry.is_long:
            pnl_points = exit_price - entry.entry_price
        else:
            pnl_points = entry.entry_price - exit_price
        
        pnl_percent = (pnl_points / entry.entry_price) * 100 if entry.entry_price else 0
        
        # Calculate duration
        duration_minutes = (exit_time - entry.entry_time).total_seconds() / 60
        
        # Generate exit ID
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
            duration_minutes=duration_minutes,
            exit_bar_high=exit_bar_high,
            exit_bar_low=exit_bar_low,
            ltf_execution=ltf_execution,
            ltf_execution_mode=ltf_execution_mode,
            **kwargs
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for legacy compatibility"""
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
    """
    Complete trade: entry + optional exit.
    
    Represents a trade from entry to exit (if closed).
    Provides unified interface for both open and closed trades.
    """
    entry: TradeEntry
    exit: Optional[TradeExit] = None
    
    @property
    def is_open(self) -> bool:
        """Is this trade still open?"""
        return self.exit is None
    
    @property
    def is_closed(self) -> bool:
        """Is this trade closed?"""
        return self.exit is not None
    
    @property
    def trade_id(self) -> str:
        """Get trade ID"""
        return self.entry.entry_id
    
    @property
    def status(self) -> str:
        """Get trade status string"""
        return "CLOSED" if self.is_closed else "OPEN"
    
    @property
    def direction(self) -> TradeDirection:
        """Get trade direction"""
        return self.entry.direction
    
    @property
    def entry_time(self) -> pd.Timestamp:
        """Get entry time"""
        return self.entry.entry_time
    
    @property
    def exit_time(self) -> Optional[pd.Timestamp]:
        """Get exit time (None if open)"""
        return self.exit.exit_time if self.exit else None
    
    @property
    def duration_bars(self) -> Optional[int]:
        """Get duration in bars (None if open)"""
        return self.exit.duration_bars if self.exit else None
    
    @property
    def duration_minutes(self) -> Optional[float]:
        """Get duration in minutes (None if open)"""
        return self.exit.duration_minutes if self.exit else None
    
    @property
    def pnl_points(self) -> Optional[float]:
        """Get P&L in points (None if open)"""
        return self.exit.pnl_points if self.exit else None
    
    @property
    def pnl_percent(self) -> Optional[float]:
        """Get P&L as percentage (None if open)"""
        return self.exit.pnl_percent if self.exit else None
    
    @property
    def is_win(self) -> bool:
        """Is this a winning trade?"""
        return self.exit.is_win if self.exit else False
    
    @property
    def is_loss(self) -> bool:
        """Is this a losing trade?"""
        return self.exit.is_loss if self.exit else False
    
    @property
    def exit_reason(self) -> Optional[ExitReason]:
        """Get exit reason (None if open)"""
        return self.exit.exit_reason if self.exit else None
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dict for legacy compatibility.
        Matches the structure of trade_simulator.py trade dicts.
        """
        result = {
            # Identity
            "trade_id": self.entry.entry_id,
            "trade_manager_trade_id": self.entry.trade_manager_id,
            "position_id": self.entry.position_id,
            "signal_id": self.entry.signal_id,
            
            # Status
            "status": self.status,
            
            # Entry details
            "entry_time": self.entry.entry_time,
            "direction": self.entry.direction.to_string(),
            "entry_price": self.entry.entry_price,
            "sl_price": self.entry.stop_loss,
            "tp_price": self.entry.take_profit,
            "sl_distance": self.entry.sl_distance,
            "tp_distance": self.entry.tp_distance,
            "risk_reward_ratio": self.entry.risk_reward_ratio,
            "comment": self.entry.comment or '',
            
            # Exit details (if closed)
            "exit_time": self.exit.exit_time if self.exit else None,
            "exit_price": self.exit.exit_price if self.exit else None,
            "exit_reason": self.exit.exit_reason.to_string() if self.exit else None,
            "pnl_points": self.exit.pnl_points if self.exit else 0,
            "pnl_percent": self.exit.pnl_percent if self.exit else 0,
            "duration_bars": self.exit.duration_bars if self.exit else 0,
            "duration_minutes": self.exit.duration_minutes if self.exit else 0,
            "is_win": self.exit.is_win if self.exit else False,
            "is_loss": self.exit.is_loss if self.exit else False,
            
            # Rejection fields (for compatibility)
            "reject_reason": None,
        }
        return result
    
    def __str__(self) -> str:
        """String representation"""
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
    
    Separate from Trade because rejected signals never had:
    - Valid entry prices
    - Stop loss / take profit levels
    - Position sizing
    - Risk calculations
    
    This is NOT a trade - it's a signal that failed filters.
    """
    # Identity
    rejection_id: str                           # Unique ID (e.g., "R1", "R2")
    signal_id: Optional[int] = None             # Link to source signal
    
    # Timing
    rejection_time: pd.Timestamp = field(default_factory=pd.Timestamp.now)
    
    # Signal details
    direction: str = "BUY"                      # "BUY" or "SELL" (not enum - it never became a trade)
    
    # Rejection details
    rejection_stage: str = "UNKNOWN"            # "RISK", "POSITION", "FILTER", etc.
    rejection_reason: str = ""                  # Detailed reason
    
    # Context (optional)
    current_price: Optional[float] = None       # Price when rejected
    meta: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dict.
        
        Note: This does NOT try to match Trade dict format.
        Rejected signals are fundamentally different from trades.
        """
        return {
            "rejection_id": self.rejection_id,
            "signal_id": self.signal_id,
            "rejection_time": self.rejection_time,
            "direction": self.direction,
            "rejection_stage": self.rejection_stage,
            "rejection_reason": self.rejection_reason,
            "current_price": self.current_price,
            "status": "REJECTED",  # For compatibility with legacy output
        }
    
    def to_legacy_trade_dict(self) -> Dict[str, Any]:
        """
        Convert to legacy trade dict format for backward compatibility.
        
        Only use this during migration period for test comparisons.
        Once TradeResult is used, this method can be removed.
        """
        return {
            # Identity (use rejection_id as trade_id for legacy tests)
            "trade_id": int(self.rejection_id.replace("R", "")),
            "trade_manager_trade_id": None,
            "position_id": None,
            "signal_id": self.signal_id,
            
            # Status
            "status": "REJECTED",
            
            # Timing
            "entry_time": self.rejection_time,
            
            # Direction
            "direction": self.direction,
            
            # Rejection details
            "reject_reason": self.rejection_reason,
            "comment": f"Rejected: {self.rejection_reason}",
            
            # Placeholder values (required by legacy format)
            "entry_price": None,
            "sl_price": None,
            "tp_price": None,
            "exit_time": None,
            "exit_price": None,
            "exit_reason": None,
            "pnl_points": 0,
            "pnl_percent": 0,
            "duration_bars": 0,
            "duration_minutes": 0,
            "sl_distance": 0,
            "tp_distance": 0,
            "risk_reward_ratio": 0,
            "is_win": False,
            "is_loss": False,
        }
    
    def __str__(self) -> str:
        return f"RejectedSignal({self.rejection_id}, {self.direction}, {self.rejection_reason})"
    
# ============================================================================
# TRADE RESULT (PIPELINE OUTPUT) - SESSION 11 UPDATE
# ============================================================================

@dataclass(frozen=True)
class TradeResult:
    """
    Complete trade simulation result.
    
    Aggregates all trades and provides statistics.
    Maps to the output of trade_simulator.simulate_trades().
    
    SESSION 11 CHANGES:
    - rejected_signals: List[RejectedSignal] (was rejected_entries: List[Dict])
    - Added from_trades() classmethod for direct construction
    - to_dict() handles rejected_signals → rejected_trades conversion
    """
    # Trades
    trades: List[Trade]                         # All trades (open + closed)
    rejected_signals: List[RejectedSignal]      # Rejected entry signals (Session 11)
    
    # Counts
    total_entries: int                          # Total entry signals received
    total_opened: int                           # Positions opened
    total_closed: int                           # Positions closed
    total_rejected: int                         # Entries rejected
    currently_open: int                         # Positions still open
    
    # Exit breakdown
    exits_by_reason: Dict[str, int]             # Exit reason counts
    
    # Risk statistics
    risk_approved: int = 0                      # Entries passing risk check
    risk_rejected: int = 0                      # Entries failing risk check
    risk_adjusted: int = 0                      # Entries with adjusted SL
    
    # Position control statistics
    position_rejected: Dict[str, int] = field(default_factory=dict)
    trade_manager_metrics: Dict[str, Any] = field(default_factory=dict)
    
    # Performance metrics (quick access)
    win_count: int = 0
    loss_count: int = 0
    win_rate: float = 0.0                       # Wins / (Wins + Losses)
    total_pnl_points: float = 0.0               # Sum of all PnL
    average_pnl_points: float = 0.0             # Mean PnL per trade
    
    # Execution details
    execution_mode: str = "UNKNOWN"
    execution_time_ms: Optional[float] = None
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def open_trades(self) -> List[Trade]:
        """Get all open trades"""
        return [t for t in self.trades if t.is_open]
    
    @property
    def closed_trades(self) -> List[Trade]:
        """Get all closed trades"""
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
        """
        Create TradeResult directly from simulation components.
        
        SESSION 11: Primary construction method for TradeSimulator
        
        Args:
            trades: List of Trade contracts (open + closed)
            rejected_signals: List of RejectedSignal contracts
            exit_stats: Dict mapping exit reason to count
            risk_stats: Dict with risk approval/rejection counts
            position_rejected: Dict with position rejection counts
            trade_manager_metrics: Dict from TradeManager.get_metrics()
            execution_mode: String identifying execution mode
            execution_time_ms: Optional execution time in milliseconds
        
        Returns:
            TradeResult contract with calculated statistics
        """
        # Calculate statistics from Trade objects
        closed_trades = [t for t in trades if t.is_closed]
        open_trades = [t for t in trades if t.is_open]
        
        win_count = sum(1 for t in closed_trades if t.is_win)
        loss_count = sum(1 for t in closed_trades if t.is_loss)
        win_rate = (win_count / len(closed_trades) * 100) if closed_trades else 0.0
        
        total_pnl = sum(t.pnl_points for t in closed_trades)
        avg_pnl = total_pnl / len(closed_trades) if closed_trades else 0.0
        
        # Total entries = trades + rejections
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
        """Convert trades to DataFrame for analysis"""
        if not self.trades:
            return pd.DataFrame()
        rows = [t.to_dict() for t in self.trades]
        return pd.DataFrame(rows)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to legacy dict format for backward compatibility.
        
        SESSION 11: Handles rejected_signals → rejected_trades conversion
        """
        return {
            'all_trades': [t.to_dict() for t in self.trades],
            'closed_trades': [t.to_dict() for t in self.closed_trades],
            'open_trades': [t.to_dict() for t in self.open_trades],
            'rejected_trades': [r.to_legacy_trade_dict() for r in self.rejected_signals],
            'exit_stats': self.exits_by_reason,
            'risk_stats': {
                'total_approved': self.risk_approved,
                'total_rejected': self.risk_rejected,
                'total_adjusted': self.risk_adjusted,
                'approved': {},  # Legacy format - could be populated if needed
                'rejected': {},  # Legacy format - could be populated if needed
                'adjusted': {},  # Legacy format - could be populated if needed
            },
            'position_rejected_count': self.position_rejected,
            'trade_manager_metrics': self.trade_manager_metrics,
            'execution_mode': self.execution_mode,
        }
    
    def get_summary(self) -> str:
        """Get human-readable summary"""
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
    
    @classmethod
    def from_simulator_output(
        cls,
        simulator_result: Dict[str, Any]
    ) -> 'TradeResult':
        """
        Create TradeResult from trade_simulator.simulate_trades() output.
        
        Handles conversion from legacy dict-based format.
        Used for backward compatibility.
        """
        # Convert trade dicts to Trade objects
        all_trades_dicts = simulator_result.get('all_trades', [])
        trades = []
        
        for trade_dict in all_trades_dicts:
            if trade_dict.get('status') == 'REJECTED':
                # Skip rejected trades (they're in rejected_entries)
                continue
            
            # Create TradeEntry
            direction = TradeDirection.from_string(trade_dict['direction'])
            entry = TradeEntry(
                entry_id=str(trade_dict['trade_id']),
                trade_manager_id=trade_dict.get('trade_manager_trade_id'),
                position_id=trade_dict.get('position_id'),
                signal_id=trade_dict.get('signal_id'),
                entry_time=trade_dict['entry_time'],
                direction=direction,
                entry_price=trade_dict['entry_price'],
                stop_loss=trade_dict['sl_price'],
                take_profit=trade_dict['tp_price'],
                sl_distance=trade_dict.get('sl_distance', 0.0),
                tp_distance=trade_dict.get('tp_distance', 0.0),
                risk_reward_ratio=trade_dict.get('risk_reward_ratio', 0.0),
                comment=trade_dict.get('comment'),
            )
            
            # Create TradeExit if closed
            exit_obj = None
            if trade_dict.get('status') == 'CLOSED':
                exit_reason = ExitReason.from_string(trade_dict['exit_reason'])
                exit_obj = TradeExit(
                    exit_id=f"{entry.entry_id}_EXIT",
                    entry_id=entry.entry_id,
                    exit_time=trade_dict['exit_time'],
                    exit_price=trade_dict['exit_price'],
                    exit_reason=exit_reason,
                    pnl_points=trade_dict['pnl_points'],
                    pnl_percent=trade_dict['pnl_percent'],
                    duration_bars=trade_dict.get('duration_bars', 0),
                    duration_minutes=trade_dict.get('duration_minutes', 0.0),
                    is_win=trade_dict['is_win'],
                    is_loss=trade_dict['is_loss'],
                )
            
            trades.append(Trade(entry=entry, exit=exit_obj))
        
        # Get rejected entries (convert to RejectedSignal if needed)
        rejected_entries = [
            t for t in all_trades_dicts if t.get('status') == 'REJECTED'
        ]
        
        # Calculate statistics
        closed_trades = [t for t in trades if t.is_closed]
        win_count = sum(1 for t in closed_trades if t.is_win)
        loss_count = sum(1 for t in closed_trades if t.is_loss)
        win_rate = (win_count / len(closed_trades) * 100) if closed_trades else 0.0
        total_pnl = sum(t.pnl_points for t in closed_trades)
        avg_pnl = total_pnl / len(closed_trades) if closed_trades else 0.0
        
        # Get exit statistics
        exit_stats = simulator_result.get('exit_stats', {})
        
        # Get risk statistics
        risk_stats = simulator_result.get('risk_stats', {})
        
        # Convert to RejectedSignal (temporary for backward compatibility)
        rejected_signals_list = []
        # For now, keep as empty list since we're returning contracts
        
        return cls(
            trades=trades,
            rejected_signals=rejected_signals_list,  # Empty for legacy compatibility
            total_entries=len(all_trades_dicts),
            total_opened=len(trades),
            total_closed=len(closed_trades),
            total_rejected=len(rejected_entries),
            currently_open=len([t for t in trades if t.is_open]),
            exits_by_reason=exit_stats,
            risk_approved=risk_stats.get('total_approved', 0),
            risk_rejected=risk_stats.get('total_rejected', 0),
            risk_adjusted=risk_stats.get('total_adjusted', 0),
            position_rejected=simulator_result.get('position_rejected_count', {}),
            trade_manager_metrics=simulator_result.get('trade_manager_metrics', {}),
            win_count=win_count,
            loss_count=loss_count,
            win_rate=win_rate,
            total_pnl_points=total_pnl,
            average_pnl_points=avg_pnl,
            execution_mode=simulator_result.get('execution_mode', 'UNKNOWN'),
        )


# ============================================================================
# TRADE DECISION (TRADE MANAGER OUTPUT)
# ============================================================================

@dataclass(frozen=True)
class TradeDecision:
    """
    Trade manager decision.
    
    Maps to TradeManager.handle_signal() output.
    Represents what action to take on a signal.
    """
    decision_type: DecisionType
    reason: str
    close_trade_ids: Optional[List[int]] = None
    new_trade_id: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def from_trade_manager_result(cls, result: Dict[str, Any]) -> 'TradeDecision':
        """Create TradeDecision from TradeManager.handle_signal() output"""
        decision_type = DecisionType.from_string(result['action'])
        return cls(
            decision_type=decision_type,
            reason=result['reason'],
            close_trade_ids=result.get('close_trade_ids'),
            new_trade_id=result.get('new_trade_id'),
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for legacy compatibility"""
        return {
            'action': self.decision_type.name,
            'reason': self.reason,
            'close_trade_ids': self.close_trade_ids,
            'new_trade_id': self.new_trade_id,
        }
    
    @property
    def is_open(self) -> bool:
        """Should we open a new position?"""
        return self.decision_type in (DecisionType.OPEN, DecisionType.CLOSE_AND_REVERSE)
    
    @property
    def is_close(self) -> bool:
        """Should we close existing positions?"""
        return self.decision_type in (DecisionType.CLOSE, DecisionType.CLOSE_AND_REVERSE)
    
    @property
    def is_reject(self) -> bool:
        """Is this signal rejected?"""
        return self.decision_type == DecisionType.REJECT
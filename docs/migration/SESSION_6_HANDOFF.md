# SESSION 6 HANDOFF - Phase 4 Trade Management (Contracts Foundation)
## Status: Phase 3 Complete ✅ | Phase 4 Ready to Start ⏳

### Session 5 Completion Summary ✅
**Date**: 2025-02-13  
**Duration**: ~2 hours  
**Achievement**: Filter Layer & FilterPipeline migration complete with **4.36x speedup** and perfect parity
---
## Phase 4 Roadmap: Trade Management
### Overview
**Goal**: Migrate trade entry/exit logic from dict-based to typed contracts  
**Estimated Duration**: 4-5 sessions (Sessions 6-10)  
**Complexity**: VERY HIGH (complex business logic, LTF execution, multiple integrations)

### Current Architecture Analysis
#### Existing Trade Contracts (Partially Defined)
**Location**: `src/strategies/trade_management/`

Already defined but need review/migration:
- ✅ `TradeDirection` enum (LONG=1, SHORT=-1)
- ✅ `TradeParameters` dataclass (direction, entry, SL, TP, size, risk_pct)
- ✅ `Position` dataclass (frozen, immutable position state)
- ✅ `TradeRecord` dataclass (closed trade record with PnL)
- ✅ `DecisionType` enum (NONE, OPEN, CLOSE, REVERSE, MODIFY)
- ✅ `TradeDecision` dataclass (decision_type + trade_params)
- ⚠️ `SignalFrame` dataclass (naming conflict with Phase 2!)

**Critical Issue**: Name collision!
```python
# OLD (trade_management/signal_frame.py)
class SignalFrame:  # ❌ Conflicts with Phase 2 SignalFrame
    timestamp: pd.Timestamp
    open: float
    high: float
    # ... OHLC data, not signals!

# NEW (Phase 2 - contracts/signal_contracts.py)
class SignalFrame:  # ✅ Contains BUY/SELL signal codes
    signals: pd.Series  # int8: 1=BUY, 2=SELL, 0=none
Resolution: Rename old SignalFrame → MarketFrame or PriceFrame
Trade Simulator Complexity
Location: src/strategies/core/trade_simulator.py (1000+ lines!)
Features:

LTF OHLC Execution: 1-second bar precision for SL/TP triggers, pre-computed LTF windows, vectorized exit detection with numpy, optional Numba JIT (5-10x faster).
Integrated Managers: RiskManager, SpreadManager, TradeManager, ProgressiveTracker (debug mode).
Exit Detection: Multiple reasons (STOP_LOSS, TAKE_PROFIT, OPPOSITE_SIGNAL, END_OF_DATA), priority handling.
Performance Optimizations: Numpy float32, Numba hot loops, vectorized checks.

Complexity Score: 9/10 (most complex module so far)
Files to Migrate (Priority Order)
textPhase 4 Migration Order:
├── Session 6: Trade Contracts Foundation
│   ├── Resolve SignalFrame name conflict → MarketFrame
│   ├── Review/validate existing contracts (TradeDirection, etc.)
│   ├── Design new contracts (TradeEntry, TradeExit, Trade)
│   └── Create ExitReason enum
│
├── Session 7: Manager Migrations (Part 1)
│   ├── RiskManager (ATR calculation, risk validation)
│   ├── SpreadManager (bid/ask spread handling)
│   └── Integration test (managers in isolation)
│
├── Session 8: Manager Migrations (Part 2)
│   ├── TradeManager (position control, pyramiding)
│   ├── Entry/Exit logic extraction
│   └── Integration test (managers together)
│
├── Session 9: Trade Simulator (Part 1 - Core)
│   ├── Main simulation loop
│   ├── Entry execution
│   ├── Simple exit detection (no LTF)
│   └── Parity test (basic trades)
│
└── Session 10: Trade Simulator (Part 2 - LTF)
    ├── LTF window precomputation
    ├── Vectorized exit detection
    ├── Numba acceleration (optional)
    ├── Full parity test
    └── Performance benchmark
Key Components Deep Dive
1. Risk Management (RiskManager)
Responsibilities: Calculate ATR for SL distance, validate risk percentile, adjust SL if exceeds max risk, integrate spread costs.
Key Methods:
Pythondef compute_trade_parameters(
    timestamp: pd.Timestamp,
    entry_price: float,
    is_long: bool
) -> Optional[Dict]:
    # Returns dict with entry, SL, TP, ATR, risk details
Migration Challenge: Complex calculations, must maintain exact parity.
2. Spread Management (SpreadManager)
Responsibilities: Load spread config, calculate spread in points, apply to entry price.
Key Methods:
Pythondef get_spread_in_points(price: float) -> float
def apply_spread_to_entry(price: float, is_long: bool) -> float
Migration Challenge: Simple but integrates with risk calculations.
3. Trade Management (TradeManager)
Responsibilities: Position control (pyramiding, close_on_opposite), track open positions, handle opposite signals.
Key Methods:
Pythondef handle_signal(
    timestamp: pd.Timestamp,
    signal_type: str  # "BUY" or "SELL"
) -> Dict:
    # Returns action, reason, new_trade_id, close_trade_ids
Migration Challenge: Stateful, complex decision logic.
4. Trade Simulator (TradeSimulator)
Responsibilities: Main simulation loop, entry execution, exit detection, statistics aggregation.
Key Methods:
Pythondef simulate_trades(
    df_strategy: pd.DataFrame,
    filtered_signals: pd.Series,
    df_ltf: pd.DataFrame,  # 1-second bars
    verbose: bool,
    progressive_tracker: Optional,
    signal_id_map: Dict
) -> Dict:
    # Returns all_trades, closed_trades, open_trades, etc.
Migration Challenge: Highest complexity, multiple integrations, LTF execution.
Migration Strategy: Thin Slice
Why Thin Slice: High complexity (SL/TP math, state tracking), many edge cases, small incremental steps reduce risk.
Approach:

Session 6: Trade Contracts + Entry Logic Foundation
Sessions 7-10: Managers and Simulator (high-level; details in FROM_SESSION_7.md)


Session 6 Plan: Trade Contracts Foundation
Objectives

Resolve naming conflicts (SignalFrame → MarketFrame)
Audit existing contracts (validate, enhance, document)
Design missing contracts (TradeEntry, TradeExit, Trade, TradeResult)
Create migration roadmap for managers and simulator

Step 6.1: Contract Audit & Name Resolution (45 mins)
Task 1: Resolve SignalFrame Conflict
Solution: Rename trade management's SignalFrame → MarketFrame
Python# NEW (trade_management/market_frame.py or contracts/market_contracts.py)
@dataclass(frozen=True)
class MarketFrame:  # ✅ Clear: market price data for a single bar
    timestamp: pd.Timestamp
    open: float
    high: float
    low: float
    close: float
    volume: float
    
    htf: Optional[pd.Series] = None      # Higher timeframe data
    ltf: Optional[pd.DataFrame] = None   # Lower timeframe data
    
    indicators: Dict[str, Any] = field(default_factory=dict)
    state: Dict[str, Any] = field(default_factory=dict)
Action Items:

 Rename file: signal_frame.py → market_frame.py
 Update class name throughout codebase
 Update imports in managers (RiskManager, TradeManager, etc.)
 Add to contracts reference documentation

Task 2: Audit Existing Contracts

trade_direction.py ✅ Keep as-isPythonclass TradeDirection(Enum):
    LONG = 1
    SHORT = -1
trade_parameters.py ⚠️ EnhanceEnhanced Version:Python@dataclass(frozen=True)
class TradeParameters:
    # Core parameters
    direction: TradeDirection
    entry_price: float              # Mid price (before spread)
    entry_price_adjusted: float     # After spread (actual execution)
    stop_loss: float                # Final SL price
    take_profit: float              # Final TP price
    size: float                     # Position size (contracts/shares)
    
    # Risk metrics
    atr_value: Optional[float] = None
    atr_length: Optional[int] = None
    sl_distance: Optional[float] = None  # SL distance in points
    tp_distance: Optional[float] = None  # TP distance in points
    risk_reward_ratio: Optional[float] = None
    risk_percentile: Optional[float] = None
    
    # Spread details
    spread_enabled: bool = False
    spread_points: Optional[float] = None
    spread_cost: Optional[float] = None
    
    # Validation flags
    sl_adjusted: bool = False       # Was SL adjusted for risk?
    risk_approved: bool = True      # Passed risk validation?
    
    # Metadata
    comment: Optional[str] = None
    tag: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)
    
    def validate(self):
        """Validate trade parameters (SL/TP positioning)."""
        if self.entry_price <= 0:
            raise ValueError("Entry price must be positive")
        if self.size <= 0:
            raise ValueError("Trade size must be positive")
        
        if self.direction == TradeDirection.LONG:
            if not (self.stop_loss < self.entry_price < self.take_profit):
                raise ValueError("Invalid SL/TP for LONG: SL < Entry < TP")
        
        if self.direction == TradeDirection.SHORT:
            if not (self.take_profit < self.entry_price < self.stop_loss):
                raise ValueError("Invalid SL/TP for SHORT: TP < Entry < SL")
position.py ✅ Keep as-is (mostly)
Add position_id: int

trade_record.py ⚠️ Rename to TradeExit
decision_type.py ✅ Keep as-is
trade_decision.py ✅ Keep as-is (mostly)

Step 6.2: Design New Trade Contracts (60 mins)
TradeEntry Contract
Python@dataclass(frozen=True)
class TradeEntry:
    entry_id: str                       # Unique ID (e.g., "T_20250213_143052_001")
    trade_manager_id: Optional[int]     # TradeManager's position ID
    signal_id: Optional[int]            # Link to source signal
    
    # Timing
    entry_time: pd.Timestamp            # When position opened
    
    # Trade details
    direction: TradeDirection           # LONG or SHORT
    entry_price: float                  # Mid price (before spread)
    entry_price_executed: float         # Actual execution price (after spread)
    stop_loss: float                    # SL price
    take_profit: float                  # TP price
    size: float                         # Position size
    
    # Risk metrics
    sl_distance: float                  # SL distance in points
    tp_distance: float                  # TP distance in points
    risk_reward_ratio: float            # TP distance / SL distance
    atr_value: Optional[float] = None   # ATR at entry time
    risk_percentile: Optional[float] = None  # Risk % of annual range
    
    # Execution details
    spread_enabled: bool = False
    spread_points: Optional[float] = None
    sl_adjusted: bool = False           # Was SL adjusted for risk?
    
    # Metadata
    comment: Optional[str] = None
    tag: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_long(self) -> bool:
        return self.direction == TradeDirection.LONG
    
    @property
    def is_short(self) -> bool:
        return self.direction == TradeDirection.SHORT
ExitReason Enum and TradeExit Contract
Pythonclass ExitReason(Enum):
    """Reason for trade exit."""
    STOP_LOSS = auto()
    TAKE_PROFIT = auto()
    OPPOSITE_SIGNAL = auto()
    END_OF_DATA = auto()
    MANUAL = auto()              # Reserved for future
    TIME_EXIT = auto()           # Reserved for future

@dataclass(frozen=True)
class TradeExit:
    exit_id: str                        # Unique ID
    entry_id: str                       # Link to TradeEntry
    
    # Timing
    exit_time: pd.Timestamp             # When position closed
    duration_bars: int                  # Bars held
    duration_minutes: float             # Minutes held
    
    # Exit details
    exit_price: float                   # Actual exit price
    exit_reason: ExitReason             # Why closed
    
    # P&L
    pnl_points: float                   # P&L in price points
    pnl_percent: float                  # P&L as % of entry
    is_win: bool                        # True if profitable
    is_loss: bool                       # True if loss
    
    # LTF execution details (if available)
    exit_bar_high: Optional[float] = None   # High of exit bar
    exit_bar_low: Optional[float] = None    # Low of exit bar
    ltf_execution: bool = False             # Was LTF used?
    
    # Metadata
    comment: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)
Trade Contract (Entry + Exit)
Python@dataclass(frozen=True)
class Trade:
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
    def duration_bars(self) -> Optional[int]:
        return self.exit.duration_bars if self.exit else None
    
    @property
    def pnl_points(self) -> Optional[float]:
        return self.exit.pnl_points if self.exit else None
    
    @property
    def pnl_percent(self) -> Optional[float]:
        return self.exit.pnl_percent if self.exit else None
    
    def to_dict(self) -> Dict[str, Any]:
        result = {
            "trade_id": self.entry.entry_id,
            "entry_time": self.entry.entry_time,
            "direction": self.entry.direction.name,
            "entry_price": self.entry.entry_price,
            "entry_price_executed": self.entry.entry_price_executed,
            "stop_loss": self.entry.stop_loss,
            "take_profit": self.entry.take_profit,
            "size": self.entry.size,
            "risk_reward_ratio": self.entry.risk_reward_ratio,
            "sl_adjusted": self.entry.sl_adjusted,
            "comment_entry": self.entry.comment,
            
            "status": "CLOSED" if self.is_closed else "OPEN",
            "exit_time": self.exit.exit_time if self.exit else None,
            "exit_price": self.exit.exit_price if self.exit else None,
            "exit_reason": self.exit.exit_reason.name if self.exit else None,
            "duration_bars": self.exit.duration_bars if self.exit else None,
            "duration_minutes": self.exit.duration_minutes if self.exit else None,
            "pnl_points": self.exit.pnl_points if self.exit else 0,
            "pnl_percent": self.exit.pnl_percent if self.exit else 0,
            "is_win": self.exit.is_win if self.exit else False,
            "is_loss": self.exit.is_loss if self.exit else False,
        }
        return result
TradeResult Contract (Pipeline Output)
Python@dataclass(frozen=True)
class TradeResult:
    trades: List[Trade]                 # All trades (open + closed)
    rejected_entries: List[Dict]        # Rejected entry signals
    
    # Counts
    total_entries: int                  # Total entry signals
    total_opened: int                   # Positions opened
    total_closed: int                   # Positions closed
    total_rejected: int                 # Entries rejected
    currently_open: int                 # Positions still open
    
    # Exit breakdown
    exits_by_reason: Dict[ExitReason, int]  # Exit reason counts
    
    # Risk statistics
    risk_approved: int                  # Entries passing risk check
    risk_rejected: int                  # Entries failing risk check
    risk_adjusted: int                  # Entries with adjusted SL
    
    # Position control statistics
    position_rejected: Dict[str, int]   # Rejected by position rules
    
    # Performance metrics (quick access)
    win_count: int
    loss_count: int
    win_rate: float                     # Wins / (Wins + Losses)
    total_pnl_points: float             # Sum of all PnL
    average_pnl_points: float           # Mean PnL per trade
    
    # Execution details
    execution_mode: str                 # "LTF_OHLC_VECTORIZED_V4_3_NUMBA"
    execution_time_ms: Optional[float] = None  # Debug mode only
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def open_trades(self) -> List[Trade]:
        return [t for t in self.trades if t.is_open]
    
    @property
    def closed_trades(self) -> List[Trade]:
        return [t for t in self.trades if t.is_closed]
    
    def to_dataframe(self) -> pd.DataFrame:
        if not self.trades:
            return pd.DataFrame()
        rows = [t.to_dict() for t in self.trades]
        return pd.DataFrame(rows)
    
    def get_summary(self) -> str:
        return (
            f"TradeResult Summary:\n"
            f"  Total Entries: {self.total_entries}\n"
            f"  Opened: {self.total_opened}\n"
            f"  Closed: {self.total_closed}\n"
            f"  Rejected: {self.total_rejected}\n"
            f"  Currently Open: {self.currently_open}\n"
            f"  Win Rate: {self.win_rate:.1f}%\n"
            f"  Total P&L: {self.total_pnl_points:+.2f} points\n"
            f"  Avg P&L: {self.average_pnl_points:+.2f} points/trade"
        )
Step 6.3: Contract Organization (30 mins)
Recommended: Move to src/strategies/contracts/ (consistent with Phases 1-3)
New Structure:
textsrc/strategies/contracts/
├── data_contracts.py           # Phase 1
├── signal_contracts.py         # Phase 2
├── filter_contracts.py         # Phase 3
├── trade_contracts.py          # Phase 4 (NEW - all trade contracts)
│   ├── TradeDirection
│   ├── TradeParameters
│   ├── TradeEntry
│   ├── TradeExit
│   ├── ExitReason
│   ├── Trade
│   ├── TradeResult
│   ├── DecisionType
│   └── TradeDecision
├── market_contracts.py         # Phase 4 (NEW - renamed from signal_frame.py)
│   └── MarketFrame
├── position_contracts.py       # Phase 4 (NEW - position tracking)
│   └── Position
└── cache.py                    # Phase 3
Migration Steps:

Create trade_contracts.py with all new contracts
Create market_contracts.py with renamed MarketFrame
Create position_contracts.py with Position
Update imports in existing code
Deprecate old trade_management/*.py files (mark for cleanup)

Step 6.4: Documentation Update (30 mins)
Files to Update:

CONTRACTS_REFERENCE.md - Add trade contracts section
DECISION_LOG.md - Document contract design decisions (e.g., why split TradeEntry + TradeExit)
SESSION_LOG.md - Record Session 6 activities

Step 6.5: Create Contract Stubs (15 mins)
Deliverable: Create skeleton files with contracts
File 1: trade_contracts.py
Python"""
Trade Contracts - Phase 4 Migration
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
    'TradeResult',
    'DecisionType',
    'TradeDecision',
]

# ... contract implementations ...
File 2: market_contracts.py
Python"""
Market Contracts - Price Data Frames
"""
from dataclasses import dataclass, field
from typing import Dict, Any, Optional
import pandas as pd

__all__ = ['MarketFrame']

@dataclass(frozen=True)
class MarketFrame:
    # ... implementation ...
File 3: position_contracts.py
Python"""
Position Contracts - Position Tracking
"""
from dataclasses import dataclass, field
from typing import Dict, Any
import pandas as pd

from .trade_contracts import TradeDirection

__all__ = ['Position']

@dataclass(frozen=True)
class Position:
    position_id: int
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

Session 6 Success Criteria
Must Have ✅

SignalFrame renamed to MarketFrame (no conflicts)
 All existing contracts audited and enhanced
 New contracts designed (TradeEntry, TradeExit, Trade, TradeResult)
 Contract files created in src/strategies/contracts/
 Documentation updated (contracts reference, decision log)

Nice to Have 🎯

 Migration guide for managers (what to change in imports)
 Example usage snippets for each contract
 Type validation tests (ensure frozen works, etc.)


Critical Files for Session 6
Required (Already Provided in Documents):

✅ trade_simulator.py (Document 9)
✅ trade_direction.py (Document 14)
✅ trade_parameters.py (Document 15)
✅ position.py (Document 11)
✅ trade_record.py (Document 16)
✅ decision_type.py (Document 10)
✅ trade_decision.py (Document 13)
✅ signal_frame.py (Document 12)

Still Needed (For Session 7):

⏳ src/strategies/trade_management/risk_manager.py
⏳ src/strategies/trade_management/spread_manager.py
⏳ src/strategies/trade_management/trade_manager.py


Phase 4 Timeline (High-Level)
Session 6: Trade Contracts Foundation ✅ (This Session)

Resolve naming conflicts, audit contracts, design new ones, documentation.
Estimated Duration: 2-3 hours
Complexity: Medium
Risk: Low

Sessions 7-10: Managers and Simulator (High-Level)

Session 7: RiskManager + SpreadManager (3-4 hours, high complexity, medium risk)
Session 8: TradeManager (3-4 hours, very high complexity, high risk)
Session 9: Trade Simulator Core (4-5 hours, very high complexity, high risk)
Session 10: Trade Simulator LTF (3-4 hours, very high complexity, medium risk)
Details: See FROM_SESSION_7.md for full breakdown.


Key Reminders for Session 6
Architecture Principles

Immutability: Use frozen dataclasses.
Separation of Concerns: Entry logic vs exit logic.
Type Safety: Strong typing.
Clear Naming: No ambiguous names.

Testing Strategy

Contract Validation: Test frozen works, validation triggers.
Type Checking: Run mypy on all contracts.
Documentation: Each contract has docstring + examples.

Performance Considerations

Memory/Speed: Frozen dataclasses efficient, no impact yet.
Compatibility: Easy to convert to/from dicts for legacy code.
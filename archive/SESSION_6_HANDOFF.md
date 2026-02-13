# SESSION 6 HANDOFF - Phase 4 Trade Management (Planning)
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
```
**Resolution**: Rename old `SignalFrame` → `MarketFrame` or `PriceFrame`
#### Trade Simulator Complexity (Document 9)
**Location**: `src/strategies/core/trade_simulator.py` (1000+ lines!)
**Features**:
1. **LTF OHLC Execution**:
   - 1-second bar precision for SL/TP triggers
   - Pre-computed LTF windows for each strategy bar
   - Vectorized exit detection with numpy
   - Optional Numba JIT acceleration (5-10x faster)

2. **Integrated Managers**:
   - `RiskManager` - ATR-based SL, risk percentile validation
   - `SpreadManager` - Bid/ask spread modeling
   - `TradeManager` - Position control, pyramiding, opposite signal handling
   - `ProgressiveTracker` - Debug mode tracking (optional)

3. **Exit Detection**:
   - Multiple exit reasons: STOP_LOSS, TAKE_PROFIT, OPPOSITE_SIGNAL, END_OF_DATA
   - Exact exit bar detection using LTF data
   - Priority handling (which exit fires first?)

4. **Performance Optimizations**:
   - Pre-computation of LTF windows
   - Numpy float32 optimization
   - Numba-accelerated hot loops
   - Vectorized exit checks (batch processing)

**Complexity Score**: 9/10 (most complex module so far)

#### Files to Migrate (Priority Order)
```
Phase 4 Migration Order:
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
```

### Key Components Deep Dive

#### 1. Risk Management (RiskManager)
**Responsibilities**:
- Calculate ATR (Average True Range) for SL distance
- Validate risk percentile (position size vs annual range)
- Adjust SL if exceeds max risk
- Integrate spread costs into calculations

**Key Methods**:
```python
def compute_trade_parameters(
    timestamp: pd.Timestamp,
    entry_price: float,
    is_long: bool
) -> Optional[Dict]:
    # Returns: {
    #   'entry_price': float,
    #   'executed_entry': float,  # After spread
    #   'sl_price': float,
    #   'trigger_sl': float,      # Final SL
    #   'tp': float,
    #   'atr_value': float,
    #   'risk_percentile_calculated': float,
    #   'sl_adjusted': bool,
    #   'comment': str
    # }
```

**Migration Challenge**: Complex calculations, must maintain exact parity

#### 2. Spread Management (SpreadManager)
**Responsibilities**:
- Load spread config (fixed points or percentage)
- Calculate spread in points for given price
- Apply to entry price (long=ask, short=bid)

**Key Methods**:
```python
def get_spread_in_points(price: float) -> float
def apply_spread_to_entry(price: float, is_long: bool) -> float
```

**Migration Challenge**: Simple logic, but must integrate with risk calculations

#### 3. Trade Management (TradeManager)
**Responsibilities**:
- Position control (pyramiding, close_on_opposite)
- Track open positions
- Generate trade IDs
- Handle opposite signals (close, reverse, reject)

**Key Methods**:
```python
def handle_signal(
    timestamp: pd.Timestamp,
    signal_type: str  # "BUY" or "SELL"
) -> Dict:
    # Returns: {
    #   'action': str,  # OPEN, REJECT, CLOSE_AND_REVERSE
    #   'reason': str,
    #   'new_trade_id': int,
    #   'close_trade_ids': List[int]
    # }
```

**Migration Challenge**: Stateful (tracks positions), complex decision logic

#### 4. Trade Simulator (TradeSimulator)
**Responsibilities**:
- Main simulation loop (bar-by-bar iteration)
- Entry execution (price, SL/TP calculation via RiskManager)
- Exit detection (LTF OHLC checks every bar)
- Progressive tracking integration (debug mode)
- Statistics aggregation

**Key Methods**:
```python
def simulate_trades(
    df_strategy: pd.DataFrame,
    filtered_signals: pd.Series,
    df_ltf: pd.DataFrame,  # 1-second bars
    verbose: bool,
    progressive_tracker: Optional,
    signal_id_map: Dict
) -> Dict:
    # Returns: {
    #   'all_trades': List[Dict],
    #   'closed_trades': List[Dict],
    #   'open_trades': List[Dict],
    #   'rejected_trades': List[Dict],
    #   'exit_stats': Dict,
    #   'risk_stats': Dict,
    #   'execution_mode': str
    # }
```

**Migration Challenge**: 
- Highest complexity (1000+ lines)
- Multiple integrations (3 managers + tracker)
- LTF execution (vectorized, Numba-optimized)
- State management (open positions across bars)

### Migration Strategy: Thin Slice

**Why Thin Slice**:
- ❌ High complexity (SL/TP math, state tracking)
- ❌ Business logic critical (must be exact)
- ❌ Many edge cases (multiple exits, pyramiding)
- ✅ Small incremental steps reduce risk
- ✅ Validate each component before next

**Approach**:
```
Session 6: Trade Contracts + Entry Logic
Session 7: Exit Logic (SL/TP)
Session 8: Position Management
Session 9: Trade Simulator + Integration
```
---

## Session 6 Plan: Trade Contracts Foundation

### Objectives
1. **Resolve naming conflicts** (SignalFrame → MarketFrame)
2. **Audit existing contracts** (validate, enhance, document)
3. **Design missing contracts** (TradeEntry, TradeExit, Trade, TradeResult)
4. **Create migration roadmap** for managers and simulator

### Step 6.1: Contract Audit & Name Resolution (45 mins)

#### Task 1: Resolve SignalFrame Conflict
**Problem**: Two classes named `SignalFrame` exist:
- Phase 2: `contracts/signal_contracts.py` → Contains BUY/SELL signal codes ✅
- Trade Mgmt: `trade_management/signal_frame.py` → Contains OHLCV price data ❌

**Solution**: Rename trade management's `SignalFrame` → `MarketFrame`
```python
# OLD (trade_management/signal_frame.py)
@dataclass(frozen=True)
class SignalFrame:  # ❌ Confusing name
    timestamp: pd.Timestamp
    open: float
    high: float
    low: float
    close: float
    volume: float
    # ...

# NEW (trade_management/market_frame.py or contracts/market_contracts.py)
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
```

**Action Items**:
- [ ] Rename file: `signal_frame.py` → `market_frame.py`
- [ ] Update class name throughout codebase
- [ ] Update imports in managers (RiskManager, TradeManager, etc.)
- [ ] Add to contracts reference documentation

#### Task 2: Audit Existing Contracts
**Files to Review**:
1. `trade_direction.py` ✅ **Keep as-is**
   ```python
   class TradeDirection(Enum):
       LONG = 1
       SHORT = -1
   ```
   - Simple, clear, well-defined
   - No changes needed

2. `trade_parameters.py` ⚠️ **Enhance**
   ```python
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
   ```
   
   **Issues**:
   - Missing: spread-adjusted entry price
   - Missing: risk metrics (ATR, percentile)
   - Missing: calculation metadata
   
   **Enhanced Version**:
   ```python
   @dataclass(frozen=True)
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
   ```

3. `position.py` ✅ **Keep as-is** (mostly)
   ```python
   @dataclass(frozen=True)
   class Position:
       direction: TradeDirection
       entry: float
       stop_loss: float
       take_profit: float
       size: float
       open_time: pd.Timestamp
       meta: Dict[str, Any] = field(default_factory=dict)
   ```
   - Used by TradeManager for position tracking
   - Simple, immutable, well-defined
   - Minor enhancement: add `position_id: int`

4. `trade_record.py` ⚠️ **Rename to TradeExit**
   ```python
   # Current name is confusing (is it open or closed?)
   @dataclass(frozen=True)
   class TradeRecord:  # ❌ Unclear scope
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
       meta: Dict[str, Any]
   ```
   
   **Better**: Separate into `TradeEntry` and `TradeExit`

5. `decision_type.py` ✅ **Keep as-is**
   ```python
   class DecisionType(Enum):
       NONE = auto()
       OPEN = auto()
       CLOSE = auto()
       REVERSE = auto()
       MODIFY = auto()  # Reserved for future
   ```
   - Clean enum for trade manager decisions
   - Well-defined, no changes needed

6. `trade_decision.py` ✅ **Keep as-is** (mostly)
   ```python
   @dataclass(frozen=True)
   class TradeDecision:
       decision_type: DecisionType
       trade_params: Optional[TradeParameters] = None
       confidence: Optional[float] = None
       reason: Optional[str] = None
       tags: Dict[str, Any] = field(default_factory=dict)
   ```
   - Used by TradeManager to communicate decisions
   - Clean interface, minimal changes needed

### Step 6.2: Design New Trade Contracts (60 mins)

#### TradeEntry Contract
```python
@dataclass(frozen=True)
class TradeEntry:
    """
    Immutable record of a trade entry.
    
    Created when a position is opened. Contains all entry details
    including price, SL/TP levels, and execution metadata.
    """
    # Identity
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
```

#### TradeExit Contract
```python
class ExitReason(Enum):
    """Reason for trade exit."""
    STOP_LOSS = auto()
    TAKE_PROFIT = auto()
    OPPOSITE_SIGNAL = auto()
    END_OF_DATA = auto()
    MANUAL = auto()              # Reserved for future
    TIME_EXIT = auto()           # Reserved for future

@dataclass(frozen=True)
class TradeExit:
    """
    Immutable record of a trade exit.
    
    Created when a position is closed. Contains exit details,
    PnL calculation, and closure metadata.
    """
    # Identity
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
```

#### Trade Contract (Entry + Exit)
```python
@dataclass(frozen=True)
class Trade:
    """
    Complete trade record (entry + exit).
    
    Combines TradeEntry and optional TradeExit. Use is_open
    to check if trade is still active.
    """
    entry: TradeEntry
    exit: Optional[TradeExit] = None
    
    @property
    def is_open(self) -> bool:
        """True if trade has no exit (still active)."""
        return self.exit is None
    
    @property
    def is_closed(self) -> bool:
        """True if trade has exit (completed)."""
        return self.exit is not None
    
    @property
    def trade_id(self) -> str:
        """Unique trade identifier."""
        return self.entry.entry_id
    
    @property
    def duration_bars(self) -> Optional[int]:
        """Bars held (None if still open)."""
        return self.exit.duration_bars if self.exit else None
    
    @property
    def pnl_points(self) -> Optional[float]:
        """P&L in points (None if still open)."""
        return self.exit.pnl_points if self.exit else None
    
    @property
    def pnl_percent(self) -> Optional[float]:
        """P&L as percentage (None if still open)."""
        return self.exit.pnl_percent if self.exit else None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to flat dictionary for DataFrame compatibility."""
        result = {
            # Entry fields
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
            
            # Exit fields (None if open)
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
```

#### TradeResult Contract (Pipeline Output)
```python
@dataclass(frozen=True)
class TradeResult:
    """
    Result of trade simulation pipeline.
    
    Contains all trades (open, closed, rejected) plus
    execution statistics and performance metrics.
    """
    # Trade lists
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
        """Get all open trades."""
        return [t for t in self.trades if t.is_open]
    
    @property
    def closed_trades(self) -> List[Trade]:
        """Get all closed trades."""
        return [t for t in self.trades if t.is_closed]
    
    def to_dataframe(self) -> pd.DataFrame:
        """Convert trades to DataFrame for analysis."""
        if not self.trades:
            return pd.DataFrame()
        
        rows = [t.to_dict() for t in self.trades]
        return pd.DataFrame(rows)
    
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
            f"  Avg P&L: {self.average_pnl_points:+.2f} points/trade"
        )
```

### Step 6.3: Contract Organization (30 mins)

### Step 6.3: Contract Organization (30 mins)

**Decision**: Where should trade contracts live?

**Option 1**: Keep in `src/strategies/trade_management/` (current location)
- ✅ Already there
- ❌ Not aligned with migration structure
- ❌ Mixes old and new code

**Option 2**: Move to `src/strategies/contracts/` (with other contracts)
- ✅ Consistent with Phase 1-3 pattern
- ✅ Clear separation (contracts vs implementation)
- ✅ Easier imports
- ❌ Need to move existing files

**Recommended**: Option 2 - Move to contracts

**New Structure**:
```
src/strategies/contracts/
├── data_contracts.py           # Phase 1 (DataBundle, DataInfo, etc.)
├── signal_contracts.py         # Phase 2 (SignalFrame, SignalType)
├── filter_contracts.py         # Phase 3 (FilterResult, FilterMetadata, etc.)
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
└── cache.py                    # Phase 3 (FilterPipelineCache)
```

**Migration Steps**:
1. Create `trade_contracts.py` with all new contracts
2. Create `market_contracts.py` with renamed `MarketFrame`
3. Create `position_contracts.py` with `Position`
4. Update imports in existing code
5. Deprecate old `trade_management/*.py` files (mark for cleanup)

### Step 6.4: Documentation Update (30 mins)

**Files to Update**:
1. `CONTRACTS_REFERENCE.md` - Add trade contracts section
2. `DECISION_LOG.md` - Document contract design decisions
3. `SESSION_LOG.md` - Record Session 6 activities

**Key Decision to Document**:
- Why TradeEntry + TradeExit instead of single TradeRecord?
  - **Reason**: Separate concerns (entry logic vs exit logic)
  - **Benefit**: Immutable entry (can't change after opening)
  - **Benefit**: Can track open trades (entry without exit)
  - **Benefit**: Clear separation for testing

### Step 6.5: Create Contract Stubs (15 mins)

**Deliverable**: Create skeleton files with contracts

**File 1**: `trade_contracts.py`
```python
"""
Trade Contracts - Phase 4 Migration

Typed contracts for trade management system.
Replaces dict-based trade records with immutable dataclasses.

Author: Migration Project
Version: 1.0.0
Date: 2025-02-14
Session: 6
"""

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, Any, Optional, List
import pandas as pd

# Export all contracts
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

# ... contract implementations from Step 6.2 ...
```

**File 2**: `market_contracts.py`
```python
"""
Market Contracts - Price Data Frames

Renamed from signal_frame.py to avoid confusion with SignalFrame.
Contains OHLCV data for a single bar with optional HTF/LTF context.

Author: Migration Project
Version: 1.0.0
Date: 2025-02-14
Session: 6
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional
import pandas as pd

__all__ = ['MarketFrame']

@dataclass(frozen=True)
class MarketFrame:
    """
    Market price data for a single bar.
    
    Contains OHLCV data plus optional higher/lower timeframe context.
    Used by trade simulator for entry/exit execution.
    """
    # ... implementation from Step 6.1 ...
```

**File 3**: `position_contracts.py`
```python
"""
Position Contracts - Position Tracking

Contracts for position management and tracking.

Author: Migration Project
Version: 1.0.0
Date: 2025-02-14
Session: 6
"""

from dataclasses import dataclass, field
from typing import Dict, Any
import pandas as pd

from .trade_contracts import TradeDirection

__all__ = ['Position']

@dataclass(frozen=True)
class Position:
    """
    Immutable position state for TradeManager.
    
    Represents an open position being tracked by the system.
    """
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
```

---

## Session 6 Success Criteria
### Must Have ✅
- [ ] `SignalFrame` renamed to `MarketFrame` (no conflicts)
- [ ] All existing contracts audited and enhanced
- [ ] New contracts designed (TradeEntry, TradeExit, Trade, TradeResult)
- [ ] Contract files created in `src/strategies/contracts/`
- [ ] Documentation updated (contracts reference, decision log)
### Nice to Have 🎯
- [ ] Migration guide for managers (what to change in imports)
- [ ] Example usage snippets for each contract
- [ ] Type validation tests (ensure frozen works, etc.)
---
## Critical Files for Session 6
### Required (Already Provided in Documents):
- ✅ `trade_simulator.py` (Document 9) - For understanding full flow
- ✅ `trade_direction.py` (Document 14) - Existing contract
- ✅ `trade_parameters.py` (Document 15) - Existing contract
- ✅ `position.py` (Document 11) - Existing contract
- ✅ `trade_record.py` (Document 16) - To be refactored
- ✅ `decision_type.py` (Document 10) - Existing enum
- ✅ `trade_decision.py` (Document 13) - Existing contract
- ✅ `signal_frame.py` (Document 12) - To be renamed
### Still Needed (For Session 7):
- ⏳ `src/strategies/trade_management/risk_manager.py`
- ⏳ `src/strategies/trade_management/spread_manager.py`
- ⏳ `src/strategies/trade_management/trade_manager.py`
---
## Phase 4 Timeline (Revised)
### Session 6: Trade Contracts Foundation ✅ (This Session)
- Resolve naming conflicts
- Audit existing contracts
- Design new contracts (Entry, Exit, Trade, Result)
- Create contract files
- Documentation
**Estimated Duration**: 2-3 hours  
**Complexity**: Medium (design work, no code migration yet)  
**Risk**: Low (just contracts, no logic)
### Session 7: RiskManager + SpreadManager Migration
- Migrate RiskManager (ATR calculation, risk validation)
- Migrate SpreadManager (spread handling)
- Use new TradeParameters contract
- Parity test (risk calculations match exactly)
**Estimated Duration**: 3-4 hours  
**Complexity**: High (complex math, must maintain parity)  
**Risk**: Medium (critical calculations)
### Session 8: TradeManager Migration
- Migrate TradeManager (position control, pyramiding)
- Use new Position contract
- Use DecisionType/TradeDecision
- Parity test (decisions match exactly)
**Estimated Duration**: 3-4 hours  
**Complexity**: Very High (stateful logic, complex decisions)  
**Risk**: High (business logic critical)
### Session 9: Trade Simulator - Core Migration
- Migrate main simulation loop
- Entry execution (using RiskManager)
- Simple exit detection (no LTF yet)
- Use new Trade/TradeEntry/TradeExit contracts
- Parity test (basic trades match)
**Estimated Duration**: 4-5 hours  
**Complexity**: Very High (1000+ lines, multiple integrations)  
**Risk**: High (complex state management)
### Session 10: Trade Simulator - LTF Execution
- Migrate LTF window precomputation
- Vectorized exit detection
- Numba acceleration (optional)
- Full parity test (exact exit prices, bars)
- Performance benchmark
**Estimated Duration**: 3-4 hours  
**Complexity**: Very High (vectorization, Numba)  
**Risk**: Medium (optimization can be iterative)
---
## Key Reminders for Session 6
### Architecture Principles
- **Immutability**: Use frozen dataclasses (no mutation after creation)
- **Separation of Concerns**: Entry ≠ Exit ≠ Trade management
- **Type Safety**: Strong typing throughout
- **Clear Naming**: No ambiguous names (MarketFrame not SignalFrame)
### Testing Strategy
- **Contract Validation**: Test frozen works, validation triggers
- **Type Checking**: Run mypy on all contracts
- **Documentation**: Each contract has docstring + examples
### Performance Considerations
- **Memory**: Frozen dataclasses are memory-efficient
- **Speed**: No performance impact (just data structures)
- **Compatibility**: Easy to convert to/from dicts (for legacy code)
---
**Session 5 Complete**: FilterPipeline ✅  
**Session 6 Focus**: Trade Contracts Design 🎯  
**Next Session**: Manager Migrations ⏳

**Overall Progress**: 75% → 78% (Phase 4 contracts designed)

1. **TradeEntry**:
   ```python
   @dataclass(frozen=True)
   class TradeEntry:
       entry_id: str                    # Unique ID (timestamp-based)
       timestamp: pd.Timestamp          # Entry time
       signal_type: SignalType          # BUY or SELL
       entry_price: float               # Actual entry price
       size: float                      # Position size (contracts/shares)
       stop_loss: float                 # SL price
       take_profit: float               # TP price
       metadata: Dict[str, Any]         # Extra info (ATR, spread, etc.)
   ```

2. **TradeExit**:
   ```python
   @dataclass(frozen=True)
   class TradeExit:
       exit_id: str                     # Unique ID
       entry_id: str                    # Link to entry
       timestamp: pd.Timestamp          # Exit time
       exit_price: float                # Actual exit price
       exit_reason: ExitReason          # SL, TP, OPPOSITE, etc.
       pnl: float                       # Profit/loss
       pnl_percent: float               # % return
       bars_held: int                   # Duration
       metadata: Dict[str, Any]         # Exit details
   ```

3. **ExitReason Enum**:
   ```python
   class ExitReason(Enum):
       STOP_LOSS = auto()
       TAKE_PROFIT = auto()
       OPPOSITE_SIGNAL = auto()
       END_OF_DATA = auto()
   ```

4. **Trade** (Entry + Exit):
   ```python
   @dataclass(frozen=True)
   class Trade:
       entry: TradeEntry
       exit: Optional[TradeExit]        # None if still open
       
       @property
       def is_open(self) -> bool: ...
       
       @property
       def is_closed(self) -> bool: ...
      
       @property
       def duration_bars(self) -> int: ...
   ```
5. **TradeResult** (Pipeline output):
   ```python
   @dataclass(frozen=True)
   class TradeResult:
       trades: List[Trade]              # All closed trades
       open_positions: List[TradeEntry] # Currently open
       entry_signals_count: int         # Total entries attempted
       exits_by_reason: Dict[ExitReason, int]
       metadata: Dict[str, Any]         # Performance metrics
   ```
### Step 6.2: Review Legacy Entry Logic (30 mins)
**Examine**:
- `src/strategies/filters/trade_entry.py`
- Understand:
  - How signals become entries
  - Position control rules (pyramiding, close_on_opposite)
  - Entry price determination
  - Size calculation
**Document**:
- Business rules
- Edge cases
- Config parameters used
### Step 6.3: Implement Entry Module (60 mins)
**File**: `src/strategies/specific/modules/trade_entry.py`
**Interface**:
```python
class TradeEntryManager:
    def __init__(self, config: Dict):
        self.position_control = config['position_control']
        self.opposite_signal = config['opposite_signal']
        # ...
    
    def process_signals(
        self,
        signal_frame: SignalFrame,
        df: pd.DataFrame,
        mode: str = "core"
    ) -> List[TradeEntry]:
        # Convert signals to trade entries
        # Apply position control
        # Calculate entry prices
        # Return list of TradeEntry objects
```
**Key Logic**:
1. Iterate through filtered signals
2. Check if entry allowed (position control)
3. Create TradeEntry with:
   - Entry price (open of next bar)
   - Stop loss (calculated but not yet implemented)
   - Take profit (calculated but not yet implemented)
4. Track open positions
### Step 6.4: Parity Test (45 mins)
**File**: `tests/migration/test_trade_entry_parity.py`
**Tests**:
1. Entry count matches (old vs new)
2. Entry timestamps match
3. Entry prices match
4. Entry signals match (BUY/SELL)
5. Position control behavior matches
6. Performance benchmark
**Test Data**:
- Use same 3-month dataset
- Compare entry-by-entry
- Validate edge cases (pyramiding, opposite signals)
---
## Critical Files to Upload for Session 6
### Required for Contract Design:
1. `src/strategies/filters/trade_entry.py` (legacy entry logic)
2. `src/strategies/filters/trade_exit.py` (legacy exit logic - for reference)
3. `src/strategies/filters/trade_execution.py` (position tracking - for reference)
### Required for Testing:
4. `src/strategies/filters/trade_simulator.py` (legacy simulator - for reference)
### Configuration:
5. Relevant sections of `configs/strategies/wbws/wbws_strategy.yaml` (already have)
---
## Key Reminders for Session 6
### Performance Targets
- **No regression**: New ≤ 110% of old execution time
- **Parity required**: Entry count, timestamps, prices must match exactly
- **Business logic**: SL/TP calculations must be identical
### Architecture Principles
- **Single Responsibility**: Entry manager only handles entries
- **Immutable Contracts**: Use frozen dataclasses
- **Dual-Mode**: Core (fast) vs Debug (tracking)
- **Type Safety**: Strong typing throughout
### Testing Strategy
- **Entry-by-entry comparison**: Validate each trade entry
- **Edge case coverage**: Pyramiding, opposite signals, etc.
- **Performance benchmark**: Measure execution time
- **Both modes**: Test core and debug
---
## Dependencies Resolved
### Phase 3 Complete ✅
- ✅ SignalFrame contract (input to trade management)
- ✅ FilterPipelineResult contract (filtered signals)
- ✅ Dual-mode execution pattern established
- ✅ Testing framework proven (parity + performance)
### Ready for Phase 4 ✅
- ✅ All filters migrated and tested
- ✅ Pipeline orchestration working
- ✅ Pattern library established
- ✅ Documentation up to date
---
## Success Criteria for Session 6
### Must Have ✅
- [ ] Trade contracts defined (TradeEntry, TradeExit, Trade, TradeResult)
- [ ] Entry logic migrated to new module
- [ ] Parity test passing (entry count, timestamps, prices)
- [ ] Performance within target (≤110% of legacy)
- [ ] Documentation updated (contracts reference, decision log)
### Nice to Have 🎯
- [ ] SL/TP placeholder calculations (full implementation in Session 7)
- [ ] Position tracking foundation (full implementation in Session 8)
- [ ] Metadata collection (debug mode)
---
## Potential Challenges in Phase 4
### Known Complexities:
1. **SL/TP Calculation**:
   - ATR-based stop loss
   - Risk-to-reward ratio for take profit
   - Dynamic level updates (trailing stops?)
2. **Position Tracking**:
   - Multiple open positions (pyramiding)
   - Close on opposite signal
   - Bar-by-bar exit checking
3. **Exit Conditions**:
   - Multiple exit reasons (SL, TP, opposite, EOD)
   - Precedence rules (which exit fires first?)
   - Partial exits (not in current system but consider)
4. **State Management**:
   - Open positions across bars
   - SL/TP level tracking
   - Entry signal history
### Mitigation Strategies:
- **Thin slice**: One component at a time
- **Frequent validation**: Test after each step
- **Reference legacy**: Keep old code as ground truth
- **Clear contracts**: Define data structures first
---
## Next Session Tasks
### Immediate Actions (Session 6 Start):
1. **Upload files**:
   - `trade_entry.py`, `trade_exit.py`, `trade_execution.py` (legacy)
   - This handoff document
2. **Review legacy logic** (30 mins):
   - Understand entry processing
   - Document business rules
   - Identify edge cases
3. **Design contracts** (45 mins):
   - TradeEntry, TradeExit, Trade, TradeResult
   - ExitReason enum
   - Review with user for feedback
4. **Implement entry module** (60 mins):
   - TradeEntryManager class
   - Signal to entry conversion
   - Position control logic
5. **Test parity** (45 mins):
   - Entry count, timestamps, prices
   - Both modes (core/debug)
   - Performance benchmark
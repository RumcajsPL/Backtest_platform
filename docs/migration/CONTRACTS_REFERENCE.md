# CONTRACTS QUICK REFERENCE
**Session 11 | Version 4.1 | 2025-02-14**

## DATA LAYER (Phase 1 ✅)
### DataBundle
```python
@dataclass
class DataBundle:
    full: pd.DataFrame           # Complete dataset
    strategy: pd.DataFrame       # Date-sliced data
    htf: Optional[pd.DataFrame]  # Higher timeframe (e.g., 1H)
    ltf: Optional[pd.DataFrame]  # Lower timeframe (e.g., 1s)
    artf: Optional[pd.DataFrame] # Monthly bars
    info: DataInfo               # Metadata (bar counts, date range)
    validation: DataValidationResult
    config: Optional[DataConfig]
```
**Key Methods**: `has_htf`, `has_ltf`, `has_artf`  
**Validation**: All DataFrames must have DatetimeIndex + OHLC columns

---

## SIGNAL LAYER (Phase 2 ✅)
### SignalFrame - OPTIMIZED v2.2
```python
@dataclass
class SignalFrame:
    signals: pd.Series           # int8: 1=BUY, 2=SELL, 0=none
    indicator_data: Optional[pd.DataFrame]  # Lazy (debug mode only)
    signal_metadata: Dict[str, Any]
```
**Key Methods**:
- `count_by_type()` → `{"buy": int, "sell": int, "total": int}`
- `iter_raw()` → Fast iterator: `(timestamp, code)`
- `buy_signals`, `sell_signals` properties

**Performance**: int8 storage (not Enum objects) for 5-10% speedup

### SignalType Enum
```python
class SignalType(Enum):
    BUY = auto()   # Code: 1
    SELL = auto()  # Code: 2
```
**Conversion**: `SignalType.from_code(1)` → `SignalType.BUY`

---

## FILTER LAYER (Phase 3 ✅)
### FilterStatus Enum
```python
class FilterStatus(Enum):
    PASSED = auto()     # Signals passed filter criteria
    REJECTED = auto()   # Signals failed filter criteria
    SKIPPED = auto()    # Filter was disabled or not applicable
    ERROR = auto()      # Filter execution encountered an error
```

### FilterResult
```python
@dataclass(frozen=True)
class FilterResult:
    passed: bool                 # Did signals pass this filter?
    signal_frame: SignalFrame    # Filtered signals (subset)
    metadata: FilterMetadata     # Execution details
```

### FilterPipelineResult
```python
@dataclass(frozen=True)
class FilterPipelineResult:
    final_signals: SignalFrame
    raw_count: int
    time_filtered_count: int
    technical_filtered_count: int
    final_count: int
    filter_results: list[FilterMetadata]
    rejection_reasons: Dict[str, int]
    execution_time_ms: Optional[float]
```
**Key Properties**: `pass_rate`, `total_rejection_count`, `get_stats_summary()`

---

## TRADE LAYER (Phase 4 ✅) - UPDATED SESSION 10.1!
### TradeDirection Enum
```python
class TradeDirection(Enum):
    LONG = 1    # Buy position
    SHORT = -1  # Sell position
```
**Key Methods**:
- `from_string("BUY")` → `TradeDirection.LONG`
- `to_string()` → "BUY" or "SELL"
- Properties: `is_long`, `is_short`

### ExitReason Enum
```python
class ExitReason(Enum):
    STOP_LOSS = auto()
    TAKE_PROFIT = auto()
    OPPOSITE_SIGNAL = auto()
    END_OF_DATA = auto()
    MANUAL = auto()              # Reserved for future
    TIME_EXIT = auto()           # Reserved for future
```

### TradeParameters
```python
@dataclass(frozen=True)
class TradeParameters:
    # Core execution prices
    entry_price_mid: float                      # Mid/bid price (before spread)
    entry_price_executed: float                 # Actual execution (after spread)
    stop_loss_raw: float                        # SL before spread adjustment
    stop_loss_trigger: float                    # Chart SL (triggers exit)
    take_profit: float                          # TP level
    position_size: float = 1.0
   
    # Risk metrics
    atr_value: Optional[float]
    atr_length: Optional[int]
    sl_distance: Optional[float]                # SL distance in points
    tp_distance: Optional[float]                # TP distance in points
    risk_reward_ratio: Optional[float]
    
    # Annual range validation
    annual_range_value: Optional[float]
    risk_percentile_calculated: Optional[float]
    max_risk_percentile: Optional[float]
    risk_percentile_passed: bool = True
    
    # Spread details
    spread_enabled: bool = False
    spread_applied: bool = False
    spread_points: Optional[float]
    
    # Adjustments
    sl_adjusted: bool = False                   # Was SL adjusted for risk?
```
**Key Methods**:
- `from_risk_manager_output(risk_dict)` → Creates from RiskManager output
- `to_dict()` → Converts to legacy dict format

### TradeEntry
```python
@dataclass(frozen=True)
class TradeEntry:
    # Identity
    entry_id: str                               # Unique ID
    trade_manager_id: Optional[int]             # TradeManager position ID
    signal_id: Optional[int]                    # Source signal link
    
    # Timing
    entry_time: pd.Timestamp
    
    # Trade details
    direction: TradeDirection
    entry_price: float                          # Executed entry
    stop_loss: float                            # SL trigger price
    take_profit: float                          # TP price
    position_size: float = 1.0
    
    # Risk metrics (at entry)
    sl_distance: float
    tp_distance: float
    risk_reward_ratio: float
    atr_value: Optional[float]
    
    # Execution details
    spread_enabled: bool = False
    spread_points: Optional[float]
    sl_adjusted: bool = False
    
    # Metadata
    comment: Optional[str]
```
**Key Methods**:
- `from_trade_parameters(id, timestamp, direction, params)` → Create from TradeParameters
- `to_dict()` → Convert to legacy dict
- Properties: `is_long`, `is_short`

**Validation**: `entry_price` must be > 0 (enforced in `__post_init__`)

### TradeExit
```python
@dataclass(frozen=True)
class TradeExit:
    # Identity
    exit_id: str
    entry_id: str                               # Link to TradeEntry
    
    # Timing
    exit_time: pd.Timestamp
    duration_bars: int
    duration_minutes: float
    
    # Exit details
    exit_price: float
    exit_reason: ExitReason
    
    # P&L
    pnl_points: float
    pnl_percent: float
    is_win: bool
    is_loss: bool
    
    # LTF execution details (optional)
    exit_bar_high: Optional[float]
    exit_bar_low: Optional[float]
    ltf_execution: bool = False
    ltf_execution_mode: Optional[str]           # "NUMBA" etc.
```
**Key Methods**:
- `create(entry, exit_time, exit_price, exit_reason)` → Auto-calculates P&L
- `to_dict()` → Convert to legacy dict

### Trade (Entry + Exit)
```python
@dataclass(frozen=True)
class Trade:
    entry: TradeEntry
    exit: Optional[TradeExit] = None
```
**Key Properties**:
- `is_open`, `is_closed` → Trade status
- `trade_id`, `status` → Identity & status string
- `direction`, `entry_time`, `exit_time` → Quick access
- `pnl_points`, `pnl_percent` → P&L (None if open)
- `is_win`, `is_loss` → Win/loss status
- `exit_reason` → Why closed (None if open)

**Key Methods**:
- `to_dict()` → Full dict (matches legacy trade_simulator format)
- `__str__()` → Human-readable summary

---

## REJECTED SIGNALS (Phase 4 ✅) - NEW SESSION 10.1!

### RejectedSignal
```python
@dataclass(frozen=True)
class RejectedSignal:
    """
    Signal that was rejected before becoming a trade.
    
    NOT a trade - it's a signal that failed filters.
    Separate from Trade because rejected signals never had:
    - Valid entry prices
    - Stop loss / take profit levels
    - Position sizing
    - Risk calculations
    """
    # Identity
    rejection_id: str                           # Unique ID (e.g., "R1", "R2")
    signal_id: Optional[int] = None             # Link to source signal
    
    # Timing
    rejection_time: pd.Timestamp = field(default_factory=pd.Timestamp.now)
    
    # Signal details
    direction: str = "BUY"                      # "BUY" or "SELL" (string, not enum)
    
    # Rejection details
    rejection_stage: str = "UNKNOWN"            # "RISK", "POSITION", "FILTER", etc.
    rejection_reason: str = ""                  # Detailed reason
    
    # Context (optional)
    current_price: Optional[float] = None       # Price when rejected
    meta: Dict[str, Any] = field(default_factory=dict)
```

**Key Methods**:
- `to_dict()` → Clean rejection format
- `to_legacy_trade_dict()` → For test compatibility (temporary)
- `__str__()` → Human-readable summary

**Design Philosophy**:
```
Trade          = A signal that was executed (has valid prices)
RejectedSignal = A signal that was filtered out (no execution)
```

**Why Separate from Trade?**
1. **Conceptual Clarity**: Rejected signals never became trades
2. **Type Safety**: No need to hack around entry_price validation
3. **Clean Code**: Clear separation of concerns
4. **Future Flexibility**: Can track rejection details without polluting Trade

---

## TRADE RESULT (Phase 4 ✅) - TO BE ENHANCED SESSION 11

### TradeResult (Pipeline Output)
```python
@dataclass(frozen=True)
class TradeResult:
    # Trades
    trades: List[Trade]                         # All trades (open + closed)
    rejected_signals: List[RejectedSignal]      # NEW SESSION 10.1!
    
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
```

**Key Properties**:
- `open_trades` → List of open trades
- `closed_trades` → List of closed trades

**Key Methods**:
- `from_simulator_output(simulator_dict)` → Create from legacy simulator
- `from_trades(trades, rejected_signals, ...)` → NEW SESSION 11 (to be added)
- `to_dataframe()` → Convert trades to DataFrame
- `get_summary()` → Human-readable statistics
- `__str__()` → Quick summary

**Session 11 Update**: Add `from_trades()` classmethod for direct construction

---

## TRADE DECISION (Trade Manager Output)

### DecisionType Enum
```python
class DecisionType(Enum):
    NONE = auto()
    OPEN = auto()
    CLOSE = auto()
    REVERSE = auto()
    MODIFY = auto()
    REJECT = auto()
    CLOSE_AND_REVERSE = auto()
```

### TradeDecision
```python
@dataclass(frozen=True)
class TradeDecision:
    decision_type: DecisionType
    reason: str
    close_trade_ids: Optional[List[int]]
    new_trade_id: Optional[int]
```

**Key Methods**:
- `from_trade_manager_result(result_dict)` → Create from TradeManager
- `to_dict()` → Convert to legacy dict
- Properties: `is_open`, `is_close`, `is_reject`

---

## MARKET CONTRACTS (Phase 4 ✅)

### MarketFrame
```python
@dataclass(frozen=True)
class MarketFrame:
    # Core OHLCV
    timestamp: pd.Timestamp
    open: float
    high: float
    low: float
    close: float
    volume: float
    
    # Multi-timeframe (optional)
    htf: Optional[pd.Series]                    # Higher timeframe
    ltf: Optional[pd.DataFrame]                 # Lower timeframe
    
    # Computed indicators
    indicators: Dict[str, Any]
    
    # State/metadata
    state: Dict[str, Any]
```

**Key Properties**:
- `price_range`, `body_size` → Bar metrics
- `is_bullish`, `is_bearish`, `is_doji` → Candle patterns
- `upper_wick`, `lower_wick` → Wick sizes
- `has_htf`, `has_ltf` → Timeframe availability

**Key Methods**:
- `from_series(series)` → Create from pandas Series
- `from_dataframe_row(df, timestamp)` → Extract from DataFrame
- `to_dict()` → Convert to dict

---

## POSITION CONTRACTS (Phase 4 ✅)

### Position
```python
@dataclass(frozen=True)
class Position:
    # Identity
    position_id: int
    
    # Position details
    direction: TradeDirection
    entry_price: float
    stop_loss: float
    take_profit: float
    size: float
    
    # Timing
    open_time: pd.Timestamp
    
    # Metadata
    meta: Dict[str, Any]
```

**Key Properties**:
- `is_long`, `is_short` → Direction checks
- `sl_distance`, `tp_distance` → Distance metrics
- `risk_reward_ratio` → R:R ratio

**Key Methods**:
- `get_unrealized_pnl(current_price)` → Unrealized P&L in points
- `get_unrealized_pnl_percent(current_price)` → Unrealized P&L %
- `is_sl_hit(current_price)` → Check if SL hit
- `is_tp_hit(current_price)` → Check if TP hit
- `to_dict()` → Convert to dict

---

## CACHING (Phase 3 ✅)

### FilterPipelineCache
```python
class FilterPipelineCache:
    def compute_cache_id(df) -> str  # Hash of OHLCV
    def has(cache_id: str) -> bool
    def get(cache_id: str) -> Dict
    def store(cache_id, indicators, indicators_np)
    def clear() -> None
    def size() -> int
    def get_stats() -> Dict[str, Any]
```

**Location**: `src/strategies/contracts/cache.py`

---

## CONTRACT ORGANIZATION

```
src/strategies/contracts/
├── data_contracts.py           # Phase 1: DataBundle, DataInfo, etc.
├── signal_contracts.py         # Phase 2: SignalFrame, SignalType
├── filter_contracts.py         # Phase 3: FilterResult, FilterPipelineResult
├── trade_contracts.py          # Phase 4: Trade*, RejectedSignal, TradeDecision
├── market_contracts.py         # Phase 4: MarketFrame
├── position_contracts.py       # Phase 4: Position
└── cache.py                    # Phase 3: FilterPipelineCache
```

---

## KEY DESIGN PATTERNS

### 1. Immutability
All Phase 4 contracts use `frozen=True`:
```python
@dataclass(frozen=True)
class Trade:
    entry: TradeEntry
    exit: Optional[TradeExit] = None
```

### 2. Legacy Compatibility (Temporary)
Contracts provide `to_dict()` for migration period:
```python
# Convert to legacy format (temporary during migration)
trade_dict = trade.to_dict()

# Session 11+: Use contracts directly
result: TradeResult = simulator.simulate_trades(...)
```

### 3. Type Safety
Strong typing throughout:
```python
direction: TradeDirection  # Not str
exit_reason: ExitReason    # Not str
timestamp: pd.Timestamp    # Not str/datetime
```

### 4. Validation
Contracts validate on creation:
```python
def __post_init__(self):
    if self.entry_price <= 0:
        raise ValueError("Entry price must be positive")
```

### 5. Property Methods
Rich property accessors:
```python
@property
def is_long(self) -> bool:
    return self.direction == TradeDirection.LONG

@property
def pnl_points(self) -> Optional[float]:
    return self.exit.pnl_points if self.exit else None
```

### 6. Clear Separation of Concerns (NEW SESSION 10.1!)
```python
# Trades vs Rejected Signals
Trade          = Executed (has prices, P&L)
RejectedSignal = Filtered (has reason, no prices)

# Stored separately
simulator.all_trades: List[Trade]
simulator.rejected_signals: List[RejectedSignal]
```

---

## ARCHITECTURE PRINCIPLES (SESSION 10)

### Design for Clarity, Not Legacy Compatibility

**Core Principle**:
> "We migrate based on legacy but create a completely new parallel tool. Parity is for validation only, not runtime compatibility."

**What This Means**:
1. **Design contracts for clarity** - Not legacy artifacts
2. **Parity = Validation tool** - Not compatibility requirement
3. **Clean architecture** - Over backward compatibility
4. **Legacy tools can convert** - If needed via `.to_dict()`

**Example**:
```python
# GOOD: Clean design
class RejectedSignal:
    rejection_reason: str
    # No need for entry_price, sl_price, etc.

# BAD: Forcing into Trade
class Trade:
    entry_price: float = 0.0  # Hack for rejected signals
```

---

## CONTRACT FLOW (SESSION 10.1)

### Signal to Trade Pipeline
```
Signal (from SignalGenerator)
    ↓
Filter (pass/fail)
    ↓
    ├─ PASS → RiskManager → TradeParameters
    │            ↓
    │         TradeManager → TradeDecision
    │            ↓
    │         ├─ OPEN → Trade (entry)
    │         │    ↓
    │         │  Trade (entry + exit when closed)
    │         │
    │         └─ REJECT → RejectedSignal
    │
    └─ FAIL → RejectedSignal
```

### Storage Separation
```python
# In TradeSimulator
self.all_trades: List[Trade]                    # Only actual trades
self.rejected_signals: List[RejectedSignal]     # Only rejections

# In TradeResult (Session 11)
TradeResult(
    trades=all_trades,
    rejected_signals=rejected_signals,
)
```

---

## SESSION 11 MIGRATION NOTES

### Current State (v4.5.1)
- **Internal**: Uses Trade and RejectedSignal contracts
- **Output**: Converts to dict for backward compatibility
- **Performance**: 4.5% faster than legacy! ✅

### Target State (v4.6)
- **Internal**: Same (Trade and RejectedSignal)
- **Output**: TradeResult contract
- **Migration**: Remove dict conversion layer

### TradeResult Enhancement Needed
```python
@classmethod
def from_trades(
    cls,
    trades: List[Trade],
    rejected_signals: List[RejectedSignal],
    exit_stats: Dict[str, int],
    risk_stats: Dict,
    position_rejected: Dict[str, int],
    trade_manager_metrics: Dict,
    execution_mode: str,
) -> 'TradeResult':
    """Create TradeResult directly from simulation components"""
    # Calculate statistics from trades
    # Return TradeResult contract
```

---

## MIGRATION STATUS

**Phase 1 (Data)**: ✅ Complete - DataBundle  
**Phase 2 (Signals)**: ✅ Complete - SignalFrame  
**Phase 3 (Filters)**: ✅ Complete - FilterResult  
**Phase 4 (Trades)**: ✅ 95% Complete - Trade, RejectedSignal  
**Phase 5 (Results)**: ⏳ Session 11 - TradeResult output

---

**Last Updated**: 2025-02-14 Session 10.1  
**File Location**: `docs/migration/CONTRACTS_REFERENCE.md`  
**Phase**: 4 - Trade Management Contracts + RejectedSignal ✅
**Next**: Session 11 - TradeResult Output Migration
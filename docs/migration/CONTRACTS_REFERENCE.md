# CONTRACTS QUICK REFERENCE
**Session 6 | Version 4.0 | 2025-02-13**
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
## TRADE LAYER (Phase 4 ✅) - NEW!
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
### TradeResult (Pipeline Output)
```python
@dataclass(frozen=True)
class TradeResult:
    # Trades
    trades: List[Trade]                         # All trades
    rejected_entries: List[Dict]                # Rejected signals
    
    # Counts
    total_entries: int
    total_opened: int
    total_closed: int
    total_rejected: int
    currently_open: int
    
    # Exit breakdown
    exits_by_reason: Dict[str, int]
    
    # Risk statistics
    risk_approved: int
    risk_rejected: int
    risk_adjusted: int
    
    # Position control
    position_rejected: Dict[str, int]
    trade_manager_metrics: Dict[str, Any]
    
    # Performance metrics
    win_count: int
    loss_count: int
    win_rate: float
    total_pnl_points: float
    average_pnl_points: float
    
    # Execution
    execution_mode: str
    execution_time_ms: Optional[float]
```
**Key Properties**:
- `open_trades` → List of open trades
- `closed_trades` → List of closed trades
**Key Methods**:
- `from_simulator_output(simulator_dict)` → Create from legacy simulator
- `to_dataframe()` → Convert trades to DataFrame
- `get_summary()` → Human-readable statistics
- `__str__()` → Quick summary
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
## MARKET CONTRACTS (Phase 4 ✅) - NEW!
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
**Note**: Replaces old `SignalFrame` from trade_management (naming conflict resolved)
---
## POSITION CONTRACTS (Phase 4 ✅) - NEW!
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
├── trade_contracts.py          # Phase 4: Trade*, TradeDecision
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
### 2. Legacy Compatibility
All contracts provide `to_dict()` and `from_*()` methods:
```python
# Convert to legacy format
trade_dict = trade.to_dict()
# Create from legacy format
trade_result = TradeResult.from_simulator_output(simulator_dict)
```
### 3. Type Safety
Strong typing throughout:
```python
direction: TradeDirection  # Not str
exit_reason: ExitReason    # Not str
timestamp: pd.Timestamp    # Not str/datetime
```
### 4. Validation
All contracts validate on creation:
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
---
## MIGRATION NOTES (Phase 4 → Phase 5)
### Session 7-10 Plan:
- **Session 7**: Migrate RiskManager & SpreadManager
- **Session 8**: Migrate TradeManager
- **Session 9**: Migrate TradeSimulator (core logic)
- **Session 10**: Migrate TradeSimulator (LTF execution)
### Key Integration Points:
1. **RiskManager** → Returns `TradeParameters`
2. **TradeManager** → Returns `TradeDecision`
3. **TradeSimulator** → Returns `TradeResult`
### Contract Flow:
```
Signal → FilterPipeline → TradeManager → RiskManager → TradeSimulator
    ↓           ↓              ↓              ↓              ↓
SignalFrame → FilterPipeline → TradeDecision → TradeParameters → TradeResult
                Result
```
---
**Last Updated**: 2025-02-13 Session 6  
**File Location**: `docs/migration/CONTRACTS_REFERENCE.md`  
**Phase**: 4 - Trade Management Contracts Complete ✅
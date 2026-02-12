# CONTRACTS QUICK REFERENCE
**Session 4+ | Version 2.2 | 2025-02-11**
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
### Key Properties:
signals_count: int → Number of signals that passed
is_empty: bool → True if no signals passed
__str__() → String representation based on metadata
### FilterMetadata
```python
@dataclass(frozen=True)
class FilterMetadata:
    filter_name: str
    status: FilterStatus
    signals_in: int
    signals_out: int
    signals_rejected: int = 0
    reason: Optional[str] = None
    indicator_values: Optional[Dict[str, float]] = None
    execution_time_ms: Optional[float] = None
```
### Key Methods/Properties:
__post_init__(): Validates and sets rejection count
rejection_rate: float → Percentage of signals rejected
to_dict() → Dict[str, Any] for JSON serialization
__str__() → Human-readable string with status icon
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
    rejection_reasons: Dict[str, int] = field(default_factory=dict)
    execution_time_ms: Optional[float] = None
```
### Key Properties:
time_rejection_count: int → Signals rejected by time filter
technical_rejection_count: int → Signals rejected by technical filters
total_rejection_count: int → Total signals rejected
pass_rate: float → Percentage of signals that passed all filters
Key Methods:
to_dict() → Dict[str, Any] for JSON serialization
get_stats_summary() → str → Human-readable statistics summary
__str__() → Summary string with counts and pass rate
### FilterProtocol
```python
class FilterProtocol(Protocol):
    name: str
    enabled: bool
    
    def compute_indicators(
        self,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray]
    ) -> None: ...
    
    def apply_filter(
        self,
        signal_frame: SignalFrame,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray],
        mode: str = "core"
    ) -> FilterResult: ...
```
---
## CACHING
### FilterPipelineCache
```python
class FilterPipelineCache:
    def compute_cache_id(df) -> str  # Hash of OHLCV
    def has(cache_id: str) -> bool
    def get(cache_id: str) -> Dict  # {"indicators": ..., "indicators_np": ...}
    def store(cache_id, indicators, indicators_np)
```
**Note**: Cache uses SHA1 hash of first/last timestamps + close prices (head/tail 50)
---
## KEY PATTERNS
### Dual-Mode Execution
- **Core Mode**: Fast, no metadata (`include_metadata=False`)
- **Debug Mode**: Full metadata, progressive tracking (`include_metadata=True`)
### Type Conversions
```python
# String → SignalType
SignalType.from_string("BUY") → SignalType.BUY
# Code → SignalType  
SignalType.from_code(1) → SignalType.BUY
# SignalFrame → Old format (for compatibility)
signals_series = signal_frame.signals  # int8 Series
```
### Performance Optimizations
1. **int8 storage** for signals (not Enum objects)
2. **Lazy metadata** loading (skip in core mode)
3. **Numpy-optimized** boolean masks
4. **Vectorized** operations (no row iteration)
5. **Indicator caching** (compute once per dataset)
---
**Last Updated**: 2025-02-12 Session 4  
**File Location**: `docs/migration/CONTRACTS_REFERENCE.md`
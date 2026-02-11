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
## FILTER LAYER (Phase 3 ⏳)
### FilterResult
```python
@dataclass(frozen=True)
class FilterResult:
    passed: bool                 # Did signals pass this filter?
    signal_frame: SignalFrame    # Filtered signals (subset)
    metadata: FilterMetadata     # Execution details
```
### FilterMetadata
```python
@dataclass(frozen=True)
class FilterMetadata:
    filter_name: str
    status: FilterStatus         # PASSED/REJECTED/SKIPPED/ERROR
    reason: Optional[str]        # Why rejected
    indicator_values: Optional[Dict[str, float]]  # Debug mode
    execution_time_ms: Optional[float]
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
```
### FilterProtocol
```python
class FilterProtocol(Protocol):
    name: str
    enabled: bool
    
    def compute_indicators(
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray]
    ) -> None: ...
    
    def apply_filter(
        signal_frame: SignalFrame,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray]
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
## MIGRATION STATUS

| Phase | Contract | Status | Notes |
|-------|----------|--------|-------|
| 1 - Data | DataBundle | ✅ v2.1 | ARTF support, dual-mode |
| 2 - Signal | SignalFrame | ✅ v2.2 | int8 optimized |
| 3 - Filter | FilterResult | ⏳ v3.0 | In progress |
| 4 - Trade | TradeRecord | ⏳ Pending | |
| 5 - Execution | ExecutionResult | ⏳ Pending | |
---
**Last Updated**: 2025-02-11 Session 4  
**File Location**: `docs/migration/CONTRACTS_REFERENCE.md`
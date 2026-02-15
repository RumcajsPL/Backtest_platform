# Phase 2: Signal Layer Migration - Deep Analysis

**Date**: 2025-02-10
**Session**: 3 (Starting)
**Status**: 🔍 Analysis Phase

---

## 1. CURRENT ARCHITECTURE ANALYSIS

### 1.1 Component Overview

```
Current Flow:
DataLoader → SignalGenerator → WBWSTrigger → Filters → TradeSimulator
             (dict-based)      (we_buy/we_sell)  (BUY/SELL strings)
```

**Components**:
1. **WBWSTrigger** (`src/indicators/wbws_trigger.py`)
   - Pure indicator calculation
   - Generates `we_buy` and `we_sell` boolean columns
   - Returns DataFrame with signals
   - Already well-optimized (vectorized, minimal copies)

2. **SignalGenerator** (`src/strategies/core/signal_generator.py`)
   - Wrapper around WBWSTrigger
   - Converts boolean signals → "BUY"/"SELL" strings
   - Returns: (raw_signals: pd.Series, signals_df: pd.DataFrame)
   - Provides signal statistics

3. **Signal Contracts** (`src/strategies/contracts/signal_contracts.py`)
   - Already designed (Session 1)
   - SignalType enum (BUY/SELL)
   - Signal dataclass (single signal)
   - SignalFrame (collection of signals)
   - SignalStats (statistics)

---

## 2. CURRENT IMPLEMENTATION REVIEW

### 2.1 WBWSTrigger Analysis

**Strengths** ✅:
- Fully vectorized (NumPy operations)
- Minimal DataFrame copies
- Anti-lookahead HTF alignment
- Efficient candle classification
- Clean separation of concerns
- Already production-ready

**Performance**:
- Very fast (~10-20ms for 3-month dataset)
- No obvious bottlenecks
- Memory efficient (only keeps needed columns)

**Issues** ⚠️:
- Returns boolean columns (`we_buy`, `we_sell`) instead of typed signals
- No integration with DataBundle
- DataFrame-centric (not contract-based)

**Recommendation**: 
✅ **Keep WBWSTrigger as-is** - It's a pure indicator, doesn't need migration
⚠️ **Create wrapper** in SignalGenerator to convert to typed contracts

---

### 2.2 SignalGenerator Analysis

**Current Interface**:
```python
def generate_signals(df: pd.DataFrame, df_htf: pd.DataFrame) 
    -> Tuple[pd.Series, pd.DataFrame]:
    # Returns (raw_signals, signals_df)
    # raw_signals: Series with "BUY"/"SELL" strings
    # signals_df: DataFrame with we_buy, we_sell columns
```

**Issues** ⚠️:
1. **String-based signals**: "BUY"/"SELL" instead of SignalType enum
2. **No DataBundle integration**: Takes raw DataFrames instead of DataBundle
3. **Tuple return**: Returns 2 separate objects instead of typed contract
4. **No dual-mode support**: Always generates full signals_df
5. **No signal metadata**: Doesn't capture HTF state, candle types, etc.

**Recommendation**: 
✅ **Migrate to SignalFrame contract**
✅ **Integrate with DataBundle**
✅ **Add dual-mode support**

---

### 2.3 Signal Contracts Analysis

**Already Designed** ✅:
- `SignalType` enum (BUY/SELL)
- `Signal` dataclass (single signal with metadata)
- `SignalFrame` (collection with iteration support)
- `SignalStats` (statistics)

**Gaps Identified** ⚠️:
1. **No WBWSTrigger integration**: SignalFrame expects generic signals
2. **Missing candle type metadata**: Should capture candle_type, htf_bull/bear
3. **Performance**: Iteration over SignalFrame might be slow for large datasets
4. **Dual-mode support**: No concept of "slim" vs "full" signal frames

---

## 3. MIGRATION STRATEGY

### 3.1 Goals

1. ✅ Replace string-based signals with `SignalType` enum
2. ✅ Return `SignalFrame` instead of tuple
3. ✅ Integrate with `DataBundle` (input)
4. ✅ Preserve WBWSTrigger (no changes needed)
5. ✅ Add dual-mode support (core/debug)
6. ✅ Maintain or improve performance
7. ✅ Capture signal metadata (candle types, HTF state)

---

### 3.2 Proposed New Architecture

```
New Flow:
DataBundle → SignalGenerator_v2 → SignalFrame → Filters → TradeSimulator
            (typed)              (typed)       (typed)
```

**Component Responsibilities**:

1. **WBWSTrigger** (unchanged)
   - Input: df, df_htf (from DataBundle)
   - Output: signals_df with we_buy, we_sell columns
   - Pure indicator logic

2. **SignalGenerator_v2** (new, in `specific/modules/`)
   - Input: DataBundle
   - Output: SignalFrame
   - Converts WBWSTrigger output to typed contracts
   - Dual-mode support:
     - **Core mode**: Minimal SignalFrame (just signal types)
     - **Debug mode**: Full SignalFrame (with metadata, indicator data)

3. **SignalFrame** (enhanced)
   - Add `from_wbws_trigger()` factory method
   - Optimize iteration for performance
   - Support slim vs full mode

---

### 3.3 Implementation Plan

#### Step 1: Enhance SignalFrame Contract
**File**: `src/strategies/contracts/signal_contracts.py`

**Changes**:
```python
@dataclass
class SignalFrame:
    signals: pd.Series  # SignalType enum values
    indicator_data: Optional[pd.DataFrame] = None
    signal_metadata: Dict[str, Any] = field(default_factory=dict)
    
    # NEW: Factory method for WBWSTrigger
    @classmethod
    def from_wbws_trigger(
        cls,
        signals_df: pd.DataFrame,
        strategy_df: pd.DataFrame,
        include_metadata: bool = True
    ) -> "SignalFrame":
        """
        Create SignalFrame from WBWSTrigger output.
        
        Args:
            signals_df: DataFrame with we_buy, we_sell columns
            strategy_df: Original OHLCV data
            include_metadata: If True, include full indicator data (debug mode)
        
        Returns:
            SignalFrame with typed signals
        """
        # Convert we_buy/we_sell to SignalType
        buy_mask = signals_df["we_buy"].to_numpy()
        sell_mask = signals_df["we_sell"].to_numpy()
        
        signal_values = np.where(
            buy_mask, SignalType.BUY,
            np.where(sell_mask, SignalType.SELL, None)
        )
        
        signals = pd.Series(signal_values, index=signals_df.index, dtype="object")
        
        # Include metadata only in debug mode
        indicator_data = None
        if include_metadata:
            # Merge OHLCV with we_buy/we_sell for metadata
            indicator_data = strategy_df.join(signals_df, how="left")
        
        return cls(
            signals=signals,
            indicator_data=indicator_data,
            signal_metadata={"source": "wbws_trigger"}
        )
```

---

#### Step 2: Create SignalGenerator_v2
**File**: `src/strategies/specific/modules/signal_generator.py`

**Interface**:
```python
class SignalGenerator:
    """
    Signal Generator v2 - Typed Contracts & Dual-Mode
    
    Features:
    - Accepts DataBundle (not raw DataFrames)
    - Returns SignalFrame (not tuple)
    - Dual-mode support (core/debug)
    - Preserves WBWSTrigger performance
    """
    
    def __init__(self, htf_period: str, mode: str = "debug"):
        self.htf_period = htf_period
        self.mode = mode
        self.trigger = WBWSTrigger(htf_period=self.htf_period)
    
    def generate_signals(self, data_bundle: DataBundle) -> SignalFrame:
        """
        Generate signals from DataBundle.
        
        Args:
            data_bundle: Loaded data (from DataLoader_v2)
            
        Returns:
            SignalFrame with typed signals
        """
        # Call WBWSTrigger (unchanged)
        signals_df = self.trigger.calculate_signals(
            data_bundle.strategy,
            data_bundle.htf
        )
        
        # Convert to SignalFrame
        include_metadata = (self.mode == "debug")
        
        signal_frame = SignalFrame.from_wbws_trigger(
            signals_df=signals_df,
            strategy_df=data_bundle.strategy,
            include_metadata=include_metadata
        )
        
        return signal_frame
    
    def get_signal_stats(self, signal_frame: SignalFrame) -> SignalStats:
        """Get signal statistics."""
        return SignalStats.from_signal_frame(signal_frame)
```

---

#### Step 3: Update SignalStats
**Enhancement**: Make it dual-mode aware

```python
@classmethod
def from_signal_frame(cls, signal_frame: SignalFrame, verbose: bool = True) -> "SignalStats":
    """
    Create SignalStats from a SignalFrame.
    
    Args:
        signal_frame: SignalFrame to analyze
        verbose: If True, include detailed metadata (debug mode)
    """
    counts = signal_frame.count_by_type()
    # ... existing logic ...
    
    metadata = {}
    if verbose:
        metadata = signal_frame.signal_metadata.copy()
    
    return cls(...)
```

---

## 4. PERFORMANCE CONSIDERATIONS

### 4.1 Potential Bottlenecks

**1. SignalFrame Iteration** ⚠️
```python
# Current implementation (in signal_contracts.py)
def __iter__(self):
    for ts, sig_type in self.signals.dropna().items():
        # Creates Signal object for each signal
        # Might be slow for 1000+ signals
```

**Impact**: Could be 10-50ms overhead for large signal sets
**Solution**: Make iteration lazy or add batch processing method

**2. Metadata Joining** ⚠️
```python
# In from_wbws_trigger()
indicator_data = strategy_df.join(signals_df, how="left")
```

**Impact**: ~5-10ms for large DataFrames
**Solution**: Skip in core mode (already planned)

**3. Signal Conversion** ✅
```python
# NumPy vectorized - already fast
signal_values = np.where(buy_mask, SignalType.BUY, ...)
```

**Impact**: Negligible (<1ms)
**Solution**: None needed

---

### 4.2 Performance Targets

| Metric | Current | Target (v2) | Notes |
|--------|---------|-------------|-------|
| Signal generation | ~10-20ms | ≤25ms | +25% acceptable |
| Core mode overhead | N/A | <5ms | Minimal difference |
| Memory (debug mode) | ~5MB | ~5MB | No regression |
| Memory (core mode) | ~5MB | ~3MB | Save metadata |

---

### 4.3 Optimization Opportunities

**1. Skip Metadata Join in Core Mode** ✅
```python
# Core mode: No indicator_data
if self.mode == "core":
    indicator_data = None
# Debug mode: Full join
else:
    indicator_data = strategy_df.join(signals_df)
```
**Expected Speedup**: 5-10ms in core mode

**2. Lazy Signal Iteration** ✅
```python
# Only iterate when needed (filters, progressive tracking)
# Most code paths just need the Series, not individual Signal objects
```
**Expected Speedup**: Avoid unnecessary object creation

**3. Batch Signal Processing** (Optional)
```python
# Instead of iterating, provide batch methods
signal_frame.get_signals_between(start, end)
signal_frame.filter_by_type(SignalType.BUY)
```
**Expected Speedup**: 50-100x for batch operations

---

## 5. DUAL-MODE SUPPORT

### 5.1 Core Mode (Production)

**Behavior**:
- Minimal SignalFrame (no indicator_data)
- Fast signal generation
- No metadata overhead
- Skip statistics unless requested

**Example**:
```python
generator = SignalGenerator(htf_period="1H", mode="core")
signal_frame = generator.generate_signals(data_bundle)

# signal_frame.signals → pd.Series of SignalType
# signal_frame.indicator_data → None
# signal_frame.signal_metadata → {"source": "wbws_trigger"}
```

---

### 5.2 Debug Mode (Development)

**Behavior**:
- Full SignalFrame with metadata
- Includes we_buy, we_sell, htf_bull, htf_bear
- Can iterate over individual Signal objects
- Full statistics

**Example**:
```python
generator = SignalGenerator(htf_period="1H", mode="debug")
signal_frame = generator.generate_signals(data_bundle)

# signal_frame.signals → pd.Series of SignalType
# signal_frame.indicator_data → Full OHLCV + we_buy/we_sell + HTF state
# Can iterate: for signal in signal_frame: ...
```

---

## 6. BACKWARD COMPATIBILITY

### 6.1 Adapter Pattern (Optional)

If old code needs string-based signals:

```python
class SignalGeneratorAdapter:
    """Adapter to make new SignalGenerator compatible with old interface."""
    
    def __init__(self, htf_period: str):
        self.new_generator = SignalGenerator(htf_period, mode="debug")
    
    def generate_signals(self, df: pd.DataFrame, df_htf: pd.DataFrame):
        # Convert to DataBundle (temporary)
        from src.strategies.contracts.data_contracts import DataBundle, DataInfo
        bundle = DataBundle(
            full=df,
            strategy=df,
            htf=df_htf,
            info=DataInfo(len(df), len(df))
        )
        
        # Generate typed signals
        signal_frame = self.new_generator.generate_signals(bundle)
        
        # Convert back to strings for old code
        raw_signals = signal_frame.signals.apply(
            lambda x: x.name if isinstance(x, SignalType) else None
        )
        
        # Reconstruct signals_df (we_buy, we_sell)
        signals_df = pd.DataFrame({
            "we_buy": (signal_frame.signals == SignalType.BUY),
            "we_sell": (signal_frame.signals == SignalType.SELL)
        }, index=signal_frame.signals.index)
        
        return raw_signals, signals_df
```

**Recommendation**: ⚠️ Only create if absolutely necessary during transition

---

## 7. MIGRATION CHECKLIST

### Phase 2.1: Enhance Contracts ✅
- [ ] Add `from_wbws_trigger()` to SignalFrame
- [ ] Add dual-mode support to SignalStats
- [ ] Add performance optimizations (lazy iteration)
- [ ] Test SignalFrame creation from mock data

### Phase 2.2: Create SignalGenerator_v2 ✅
- [ ] Create `src/strategies/specific/modules/signal_generator.py`
- [ ] Implement DataBundle input
- [ ] Implement SignalFrame output
- [ ] Add dual-mode support
- [ ] Preserve WBWSTrigger integration

### Phase 2.3: Testing ✅
- [ ] Unit test: SignalFrame creation
- [ ] Unit test: SignalType conversion
- [ ] Integration test: Compare old vs new signal generator
- [ ] Performance test: Ensure <25ms signal generation
- [ ] Test dual-mode behavior

### Phase 2.4: Documentation ✅
- [ ] Update module docstrings
- [ ] Document SignalFrame usage
- [ ] Add examples for filters

---

## 8. RISKS & MITIGATION

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Performance regression | Low | Medium | Benchmark at each step |
| Filter incompatibility | Medium | High | Create adapter if needed |
| Metadata overhead | Low | Low | Skip in core mode |
| Iteration slowness | Medium | Low | Lazy iteration, batch methods |

---

## 9. SUCCESS CRITERIA

### Must Have ✅
1. SignalFrame replaces tuple return
2. SignalType enum replaces strings
3. Performance: ≤25ms signal generation
4. Dual-mode support (core/debug)
5. 100% signal parity with old generator

### Nice to Have 💡
1. Batch signal processing methods
2. Lazy iteration optimization
3. Rich metadata in debug mode
4. Backward compatibility adapter

---

## 10. ESTIMATED TIMELINE

**Total Time**: ~2-3 hours

| Task | Duration | Complexity |
|------|----------|------------|
| Enhance SignalFrame | 30 min | Medium |
| Create SignalGenerator_v2 | 45 min | Medium |
| Testing & validation | 45 min | High |
| Documentation | 30 min | Low |

---

## 11. NEXT STEPS

1. **Enhance SignalFrame contract** with `from_wbws_trigger()` factory
2. **Create SignalGenerator_v2** in `specific/modules/`
3. **Create test suite** comparing old vs new
4. **Benchmark performance** (target: ≤25ms)
5. **Update session log** with progress

---

**Status**: ✅ Analysis complete, ready to implement
**Next Action**: Enhance SignalFrame contract
**Expected Completion**: Session 3

---

**End of Analysis**
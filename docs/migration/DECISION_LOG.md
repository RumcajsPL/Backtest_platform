# Architecture Decision Log

## Format
Each decision follows this structure:
- **Decision**: What was decided
- **Context**: Why it mattered
- **Options Considered**: Alternatives evaluated
- **Rationale**: Why this option was chosen
- **Implications**: What this means for the project
- **Date**: When decided

---

## Decision 001: Parallel Architecture (Old vs New)
**Date**: [SESSION 1]

**Decision**: Keep old system in `src/strategies/core/`, build new in `src/strategies/specific/`

**Context**: Need to maintain working system while migrating

**Options Considered**:
1. In-place migration (modify existing files)
2. Parallel architecture (separate folders)
3. Branch-based isolation (Git branches only)

**Rationale**:
- Option 2 provides maximum safety
- Old system remains executable for validation
- No risk of breaking production
- Easy rollback if needed

**Implications**:
- Duplicate code during migration
- Need adapter layer if modules interact
- Final cleanup phase to remove old code

---

## Decision 002: Hybrid Migration Strategy
**Date**: [SESSION 1]

**Decision**: Use Big Bang for simple modules (DataLoader, SignalGenerator), Thin Slice for complex modules (FilterPipeline, TradeSimulator)

**Context**: Different modules have different complexity levels

**Options Considered**:
1. Pure Big Bang (migrate entire module at once)
2. Pure Thin Slice (incremental across all modules)
3. Hybrid approach

**Rationale**:
- DataLoader is self-contained → Big Bang is safe
- TradeSimulator has many dependencies → Thin Slice reduces risk
- Hybrid optimizes for speed + safety

**Implications**:
- Need to identify which modules qualify for Big Bang
- Different validation strategies per approach

---

## Decision 003: Checkpoint Protocol
**Date**: [SESSION 1]

**Decision**: Provide checkpoint every 3-5 substantial exchanges with resume command

**Context**: Chat window limits risk losing progress

**Options Considered**:
1. Checkpoint at end of session only
2. Checkpoint every exchange
3. Checkpoint every 3-5 exchanges

**Rationale**:
- Option 3 balances continuity with verbosity
- Provides multiple recovery points
- Doesn't overwhelm with checkpoints

**Implications**:
- Need to maintain checkpoint discipline
- User must save checkpoints locally

---

## Decision 004: [Next Decision]
[To be filled as we progress]

## Decision 004: DataBundle Validation Strategy
**Date**: Session 1 (2025-02-09)

**Decision**: Validate DataFrame structure in `DataBundle.__post_init__()`

**Context**: Need to ensure DataFrames have correct structure before use

**Options Considered**:
1. Validate in __post_init__() (eager validation)
2. Validate on first access (lazy validation)
3. Make validation optional (controlled by config)
4. Validate only strategy DataFrame, skip optional ones

**Rationale**:
- Chose Option 1 for fail-fast behavior
- Better to catch errors early than during execution

**Implications**:
- ✅ Catches structural issues immediately
- ✅ Clear error messages at load time
- ❌ **Performance regression: +31%**

**Revision Needed**: Yes - Session 2 will optimize this

**Proposed Fix**:
- Use Option 3: Make validation optional (default=False)
- Only validate when in "debug" mode
- Or use Option 4: Only validate strategy DataFrame

---
```

---

## 📋 HANDOFF INSTRUCTIONS FOR TOMORROW

### **Step 1: Start New Chat with This Context**

**Paste this at the start of your next session:**
```
I'm continuing the WBWSStrategy migration project from Session 1.

CONTEXT:
- Project: Migrating WBWSStrategy from dict-based to typed contracts
- Phase: Phase 1 - Data Layer Migration
- Status: DataLoader_v2 implemented but has performance regression

COMPLETED IN SESSION 1:
- ✅ All contracts defined (DataConfig, DataBundle, SignalFrame, etc.)
- ✅ DataLoader_v2 implemented and tested
- ✅ DataFrame parity: 100% match with old DataLoader
- ✅ Metadata parity: 100% match with old DataLoader

CURRENT ISSUE:
Performance regression in DataLoader_v2:
- Old DataLoader: 746.8 ms
- New DataLoader: 977.9 ms  
- Regression: +30.9% (threshold is ≤110%, we're at 131%)
- Failing test: tests/migration/test_dataloader_parity.py

HYPOTHESIS:
The performance issue is caused by DataFrame validation in DataBundle.__post_init__().
The validation checks 4 DataFrames (full, strategy, htf, ltf) on every load.

TASK FOR THIS SESSION:
1. Profile DataLoader_v2 to confirm bottleneck
2. Optimize validation (make it optional or lazy)
3. Re-run test and achieve ≤821ms (110% of 747ms baseline)
4. Once performance test passes, proceed to Phase 2 (SignalGenerator)

FILES INVOLVED:
- src/strategies/specific/modules/data_loader.py (to optimize)
- src/strategies/contracts/data_contracts.py (DataBundle class)
- tests/migration/test_dataloader_parity.py (validation test)

Please help me profile and fix the performance regression.

# DECISION LOG - Migration Project

## Purpose
Documents key architectural decisions, rationale, and tradeoffs made during migration.

---

## Session 5: FilterPipeline Architecture

### Decision 5.1: Auto-Instantiation via Class Mapping
**Date**: 2025-02-13  
**Context**: Need to load filters from config dynamically  
**Options**:
1. Manual instantiation (caller creates filters)
2. Dynamic imports with `importlib`
3. Class mapping dict (`FILTER_CLASSES`)

**Decision**: Class mapping dict (#3)

**Rationale**:
- ✅ Explicit and maintainable (all filters visible in one place)
- ✅ No import overhead per filter
- ✅ Easy to add new filters (add to mapping)
- ✅ Type-safe (class references checked at import time)
- ✅ Fast (no string parsing or dynamic imports)
- ❌ Must update mapping when adding filters (acceptable tradeoff)

**Implementation**:
```python
FILTER_CLASSES = {
    'rsi_filter': RSIFilter,
    'cci_filter': CCIFilter,
    # ... 10 total
}

for name in filter_sequence:
    cls = FILTER_CLASSES[name]
    filter = cls(name=name, **config[name])
```

**Impact**: Cleaner code, faster instantiation, easier maintenance

---

### Decision 5.2: Time Filter Always First
**Date**: 2025-02-13  
**Context**: Time filter should always run before technical filters  
**Options**:
1. Include in `filter_sequence` (user controls order)
2. Hardcode to always run first

**Decision**: Hardcode (#2)

**Rationale**:
- ✅ Time filtering is fundamentally different (no indicators needed)
- ✅ Always makes sense to run first (reduces data for technical filters)
- ✅ Prevents user misconfiguration (putting time filter later)
- ✅ Clear separation of concerns (time vs technical)
- ❌ Less flexible (acceptable - time filter should always be first)

**Implementation**:
```python
def _load_filters(self):
    self._load_time_filter()  # Always first
    self._load_technical_filters()  # From sequence
```

**Impact**: Simpler config, faster execution, prevents misuse

---

### Decision 5.3: Indicator Dict Pattern (Not Refactored)
**Date**: 2025-02-13  
**Context**: Old code uses mutable dicts for indicator sharing  
**Options**:
1. Refactor to immutable indicator store class
2. Keep mutable dict pattern

**Decision**: Keep mutable dict pattern (#2)

**Rationale**:
- ✅ Proven performance (no regression risk)
- ✅ Simple and fast (direct dict access)
- ✅ Filters already use this pattern (consistency)
- ✅ Parity guaranteed (same memory layout)
- ❌ Less elegant (mutable state shared)
- ❌ Not "best practice" (acceptable for migration phase)

**Implementation**:
```python
self.indicators: Dict[str, pd.Series] = {}
self.ind_np: Dict[str, np.ndarray] = {}

# Passed to all filters
filter.compute_indicators(df, self.indicators, self.ind_np)
filter.apply_filter(..., self.indicators, self.ind_np)
```

**Impact**: Zero performance regression, guaranteed parity

**Post-Migration Note**: Consider refactoring to `IndicatorStore` class in Phase 5 for better encapsulation.

---

### Decision 5.4: Early Exit on Empty Signals
**Date**: 2025-02-13  
**Context**: No point processing filters if no signals remain  
**Options**:
1. Always run all filters (complete pipeline)
2. Early exit when signal count reaches zero

**Decision**: Early exit (#2)

**Rationale**:
- ✅ Performance optimization (skip unnecessary work)
- ✅ Clear logging (shows where signals were eliminated)
- ✅ Matches user expectation (pipeline stops when done)
- ✅ No risk (remaining filters wouldn't change empty set)
- ❌ Less metadata collected (acceptable - we know why it stopped)

**Implementation**:
```python
if signal_count == 0:
    logger.info(f"Pipeline early exit: no signals after {filter.name}")
    break
```

**Impact**: ~5-15% performance improvement when filters reject all signals

---

### Decision 5.5: Error Handling - Pass Through
**Date**: 2025-02-13  
**Context**: What to do when a filter fails?  
**Options**:
1. Raise exception (stop pipeline)
2. Reject all signals (conservative)
3. Pass through unchanged (optimistic)

**Decision**: Pass through unchanged (#3)

**Rationale**:
- ✅ Pipeline resilience (one bad filter doesn't break everything)
- ✅ Debugging friendly (see which filter failed, rest continue)
- ✅ Matches legacy behavior (filters were independent)
- ❌ Could pass bad signals (acceptable - we log the error)
- ❌ Less strict (acceptable for non-critical filters)

**Implementation**:
```python
try:
    result = filter.apply_filter(...)
except Exception as e:
    logger.error(f"Filter {filter.name} failed: {e}")
    result = FilterResult(
        passed=True,  # Pass through
        signal_frame=current_signals,  # Unchanged
        metadata=FilterMetadata(..., status=FilterStatus.ERROR)
    )
```

**Impact**: More robust pipeline, easier debugging

---

### Decision 5.6: Cache Location
**Date**: 2025-02-13  
**Context**: Where should `FilterPipelineCache` live?  
**Options**:
1. Keep in `src/backtesting/tools/`
2. Move to `src/strategies/contracts/`
3. Move to `src/strategies/specific/utils/`

**Decision**: Move to contracts (#2)

**Rationale**:
- ✅ Better organization (cache is a contract component)
- ✅ Closer to other contracts (signal, filter, data)
- ✅ Avoids backtesting dependency (strategies are independent)
- ✅ Reusable across strategies (not WBWS-specific)
- ❌ Slightly different from "data contracts" (acceptable - it's infrastructure)

**Implementation**:
```python
# New location
from src.strategies.contracts.cache import FilterPipelineCache
```

**Impact**: Better project structure, clearer dependencies

---

### Decision 5.7: Dual-Mode Metadata Collection
**Date**: 2025-02-13  
**Context**: When to collect detailed metadata?  
**Options**:
1. Always collect (consistent behavior)
2. Never collect (fastest)
3. Mode-dependent (core=skip, debug=collect)

**Decision**: Mode-dependent (#3)

**Rationale**:
- ✅ Performance in production (core mode skips overhead)
- ✅ Rich debugging when needed (debug mode collects everything)
- ✅ User control (config specifies mode)
- ✅ Matches project charter (dual-mode requirement)
- ❌ Two code paths to maintain (acceptable - well-tested)

**Implementation**:
```python
def apply_filters(self, signal_frame, df, mode="core"):
    execution_time = perf_counter() if mode == "debug" else None
    # ...
    metadata = FilterMetadata(
        execution_time_ms=execution_time
    )
```

**Impact**: 4.36x faster in core mode, full tracking in debug mode

---

## Session 4: Individual Filter Migration

### Decision 4.1: FilterProtocol Interface
**Date**: 2025-02-12  
**Decision**: Use Protocol (structural typing) not ABC (inheritance)

**Rationale**:
- ✅ Duck typing (filters don't need to inherit)
- ✅ Flexibility (easy to add filters)
- ✅ Type checking (mypy validates interface)
- ❌ No runtime enforcement (acceptable - we control all filters)

---

### Decision 4.2: int8 Signal Storage
**Date**: 2025-02-12  
**Decision**: Store signals as int8 codes, not Enum objects

**Rationale**:
- ✅ 5-10% performance improvement
- ✅ Vectorization-friendly (numpy operations)
- ✅ Memory efficient (1 byte per signal)
- ❌ Less readable (use helper methods to convert)

---

## Session 3: Signal Layer

### Decision 3.1: SignalFrame Dataclass
**Date**: 2025-02-11  
**Decision**: Use dataclass for SignalFrame (not custom class)

**Rationale**:
- ✅ Clean syntax (auto-generated __init__, __repr__)
- ✅ Type hints (better IDE support)
- ✅ Frozen option available (immutability when needed)
- ✅ Less boilerplate (standard library)

---

## Session 2: Data Layer

### Decision 2.1: Parallel Architecture
**Date**: 2025-02-10  
**Decision**: Keep old system untouched, build new in parallel

**Rationale**:
- ✅ No risk to existing system
- ✅ Rollback always possible
- ✅ Easy comparison (old vs new)
- ✅ Gradual migration (module by module)
- ❌ Duplicate code temporarily (acceptable - deleted at end)

---

### Decision 2.2: DataBundle Structure
**Date**: 2025-02-10  
**Decision**: Single DataBundle class with optional timeframes

**Rationale**:
- ✅ Flexible (not all strategies need all timeframes)
- ✅ Type-safe (Optional[pd.DataFrame] for missing data)
- ✅ Efficient (load only what's needed)
- ❌ Nullable fields (acceptable - has_htf/has_ltf helpers)

---

## Session 1: Project Strategy

### Decision 1.1: Hybrid Migration Approach
**Date**: 2025-02-09  
**Decision**: Use "Hybrid Big Bang + Thin Slice"

**Rationale**:
- ✅ Fast for simple modules (DataLoader, SignalGenerator)
- ✅ Safe for complex modules (FilterPipeline, TradeSimulator)
- ✅ Incremental validation (test each module)
- ✅ Flexible (adapt approach per module)

---

### Decision 1.2: Typed Contracts Over Dicts
**Date**: 2025-02-09  
**Decision**: Use dataclasses/typed contracts, not dicts

**Rationale**:
- ✅ Type safety (catch errors at development time)
- ✅ IDE support (autocomplete, refactoring)
- ✅ Self-documenting (structure visible in code)
- ✅ Performance (no string key lookup overhead)
- ❌ More verbose (acceptable - clarity over brevity)

---

## Decision Categories

### Architecture Decisions
- Parallel architecture (keep old system)
- Typed contracts (dataclasses over dicts)
- Dual-mode execution (core/debug)
- Indicator caching (SHA1-based)

### Performance Decisions
- int8 signal storage (not Enum objects)
- Vectorized operations (numpy)
- Early exit (stop on empty signals)
- Lazy metadata loading (skip in core mode)
- Auto-instantiation (avoid dynamic imports)

### Design Decisions
- Protocol interface (not ABC inheritance)
- Mutable indicator dicts (proven pattern)
- Time filter priority (always first)
- Error pass-through (resilience)
- Cache in contracts folder (organization)

---

**Last Updated**: 2025-02-13 (Session 5)  
**Total Decisions Documented**: 16
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
# MIGRATION DECISION LOG
**Project**: Trading System Migration to Typed Contracts  
**Last Updated**: 2025-02-13 (Session 9)

---

## Purpose
This document records key architectural decisions made during the migration from legacy dict-based code to typed contract-based architecture.

---

## DECISION 1: Incremental Migration Strategy
**Session**: 1-4 (Planning)  
**Date**: 2025-02-10  
**Status**: ✅ APPROVED

### Context
Need to migrate large codebase (~5000+ lines) from legacy dict/string-based code to typed contracts.

### Decision
Adopt incremental phase-based migration:
1. Phase 1: Data Layer (DataBundle)
2. Phase 2: Signal Layer (SignalFrame)
3. Phase 3: Filter Layer (FilterResult, FilterPipeline)
4. Phase 4: Trade Layer (Trade, TradeEntry, TradeExit)
5. Phase 5: Execution Layer (TradeSimulator)

### Rationale
- **Lower Risk**: Each phase independently testable
- **Maintain Parity**: Can verify results at each step
- **Team Velocity**: Can deploy incrementally
- **Rollback Safety**: Can revert individual phases

### Outcome
✅ Successfully completed Phases 1-4 with 100% parity

---

## DECISION 2: Frozen Dataclasses for Contracts
**Session**: 6 (Trade Contracts Design)  
**Date**: 2025-02-12  
**Status**: ✅ APPROVED

### Context
Need to choose between mutable vs immutable contracts.

### Decision
Use `@dataclass(frozen=True)` for all Phase 4+ contracts.

### Rationale
- **Immutability**: Prevents accidental modifications
- **Thread Safety**: Safe for concurrent access
- **Debugging**: Easier to track data flow
- **Performance**: Hashable, can be dict keys

### Example
```python
@dataclass(frozen=True)
class Trade:
    entry: TradeEntry
    exit: Optional[TradeExit] = None
```

### Outcome
✅ All Phase 4 contracts frozen, no issues reported

---

## DECISION 3: RiskManager Call Ordering (Critical!)
**Session**: 9 (TradeSimulator Integration)  
**Date**: 2025-02-13  
**Status**: ✅ APPROVED

### Context
**Problem**: TradeManager now requires price parameters (entry_price, stop_loss, take_profit) but these come from RiskManager.

**Legacy Flow** (v4.3):
```python
# 1. TradeManager decision (didn't need prices)
result = tm.handle_signal(timestamp, signal_type)  # Dict-based

# 2. Get risk params AFTER decision
if result["action"] == "OPEN":
    params = risk_mgr.compute_trade_parameters(...)
```

**Problem with Migrated TradeManager**:
```python
# TradeManager NOW needs prices upfront
result = tm.handle_signal(
    timestamp, signal_type,
    entry_price=???,  # Don't have this yet!
    stop_loss=???,
    take_profit=???
)
```

### Options Considered

**Option A: Always Call RiskManager First**
```python
# 1. Get risk params FIRST
params = risk_mgr.compute_trade_parameters(timestamp, bid_price, is_long)
if params is None:
    reject_signal()  # Early exit
    continue

# 2. TradeManager decision with real prices
result = tm.handle_signal(
    timestamp, signal_type,
    entry_price=params.entry_price_executed,
    stop_loss=params.stop_loss_trigger,
    take_profit=params.take_profit
)
```

**Pros**:
- ✅ TradeManager always has real prices
- ✅ Simpler code flow
- ✅ Early exit if risk fails
- ✅ Matches natural decision flow (risk → position → execute)

**Cons**:
- ⚠️ Wastes RiskManager computation on signals TradeManager will reject (pyramiding, opposite)
- ⚠️ ~50-100µs overhead per rejected signal

**Option B: Two-Phase Decision**
```python
# Phase 1: Quick decision (no prices needed)
quick_decision = tm.can_open(timestamp, signal_type)  # Hypothetical

if quick_decision.can_proceed:
    # Phase 2: Get prices and full decision
    params = risk_mgr.compute_trade_parameters(...)
    result = tm.handle_signal_with_params(...)
```

**Pros**:
- ✅ Optimal performance (no wasted RiskManager calls)

**Cons**:
- ❌ Requires TradeManager refactor (new methods)
- ❌ More complex flow
- ❌ Splits decision logic
- ❌ Not backward compatible

**Option C: Use Placeholder Prices**
```python
# Use placeholder prices for TradeManager
temp_entry = bid_price
temp_sl = bid_price * 0.99  # Fake
temp_tp = bid_price * 1.02  # Fake

result = tm.handle_signal(timestamp, signal_type, temp_entry, temp_sl, temp_tp)

if result.is_open:
    # Get real prices from RiskManager
    params = risk_mgr.compute_trade_parameters(...)
```

**Pros**:
- ✅ No RiskManager waste

**Cons**:
- ❌ TradeManager has incorrect prices
- ❌ Position contracts created with fake data
- ❌ Confusing for debugging
- ❌ Violates contract integrity

### Decision: Option A (Always Call RiskManager First)

**Rationale**:
1. **Correctness > Performance**: TradeManager should always have real prices
2. **Contract Integrity**: Position contracts must have accurate data
3. **Performance Acceptable**: 
   - RiskManager: ~50-100µs per call
   - Typical strategy: 10-20% rejection rate
   - Waste: ~5-10µs per signal (negligible)
   - For 10,000 signals: ~50-100ms total overhead
4. **Simplicity**: Clear, linear flow
5. **Future-Proof**: If we add price-dependent position logic to TradeManager, we're ready

**Implementation**:
```python
# NEW FLOW (Session 9)
for timestamp in strategy_index:
    # 1. Get risk parameters FIRST
    params = risk_mgr.compute_trade_parameters(timestamp, bid_price, is_long)
    
    if params is None:
        # Risk rejected - early exit
        handle_risk_rejection()
        continue
    
    # 2. TradeManager decision (with real prices)
    result = tm.handle_signal(
        timestamp=timestamp,
        signal_type=signal_type,
        entry_price=params.entry_price_executed,
        stop_loss=params.stop_loss_trigger,
        take_profit=params.take_profit,
        position_size=params.position_size
    )
    
    # 3. Execute decision
    if result.is_open:
        open_position(params, result.new_trade_id)
```

### Performance Impact Measured

**Benchmark** (10,000 signals):
- Legacy (v4.3): 2.50s total
- Migrated (v4.4): 2.52s total
- Overhead: 0.02s (0.8%)

**Conclusion**: ✅ Negligible performance impact

### Alternative Considered for Future
**Option B** (Two-Phase Decision) could be implemented in Session 10 as optimization if profiling shows RiskManager is bottleneck. Would require:
- New TradeManager method: `can_accept_signal(signal_type) -> bool`
- Refactor to check pyramiding/opposite BEFORE risk calculation
- Estimated dev time: 2-3 hours
- Estimated performance gain: 5-10% on high-rejection strategies

**Recommendation**: Defer to Session 10 optimization phase. Current solution is correct and performant enough.

---

## DECISION 4: ProgressiveTracker Compatibility
**Session**: 9 (TradeSimulator Integration)  
**Date**: 2025-02-13  
**Status**: ✅ APPROVED

### Context
ProgressiveTracker expects string `action` field, but TradeManager now returns `TradeDecision` contract with `DecisionType` enum.

### Options Considered

**Option A: Convert at Boundary**
```python
tracker.update_position_management_details(
    action=result.to_dict()['action'],  # Convert to string
    # ...
)
```

**Option B: Migrate ProgressiveTracker**
```python
# Update ProgressiveTracker to accept DecisionType
tracker.update_position_management_details(
    decision_type=result.decision_type,  # Use enum
    # ...
)
```

### Decision: Option A (Convert at Boundary)

**Rationale**:
- ProgressiveTracker is debug/analysis tool (not core simulation)
- Defer ProgressiveTracker migration to Session 10
- Keep Session 9 focused on TradeManager integration
- `to_dict()` provides clean conversion

### Implementation
```python
# Convert TradeDecision → string for tracker
action_str = result.to_dict()['action']  # "OPEN", "REJECT", etc.
tracker.update_position_management_details(action=action_str, ...)
```

---

## DECISION 5: Trade Dict Structure Preservation
**Session**: 9 (TradeSimulator Integration)  
**Date**: 2025-02-13  
**Status**: ✅ APPROVED

### Context
TradeSimulator maintains list of trade dicts for backward compatibility with reporting/analysis tools.

### Decision
Keep trade dict structure unchanged in Session 9.

**Rationale**:
- Reporting tools expect specific dict structure
- Analysis scripts depend on dict format
- Defer migration to `TradeResult` contract to Session 10
- Focus Session 9 on TradeManager integration only

**Future**: Session 10 will migrate to:
```python
# Session 10: Return TradeResult contract
result = simulator.simulate_trades(...)
# result.trades → List[Trade] (contracts)
# result.to_dataframe() → pandas DataFrame
```

---

## DECISION SUMMARY TABLE

| # | Decision | Session | Status | Impact |
|---|----------|---------|--------|--------|
| 1 | Incremental Migration | 1-4 | ✅ Approved | Foundation |
| 2 | Frozen Dataclasses | 6 | ✅ Approved | All contracts |
| 3 | **RiskManager First** | **9** | **✅ Approved** | **Critical** |
| 4 | Tracker Compatibility | 9 | ✅ Approved | Boundary conversion |
| 5 | Trade Dict Preservation | 9 | ✅ Approved | Defer to Session 10 |

---

## Future Decisions (Session 10+)

### Pending Review
- **P1**: TradeResult contract migration (Session 10)
- **P2**: ProgressiveTracker contract integration (Session 10)
- **P3**: Two-phase TradeManager decision (optimization - if needed)
- **P4**: LTF execution contract enhancement (TBD)

---

**Document Owner**: Migration Team  
**Review Frequency**: After each session  
**Next Review**: Session 10

# DECISION LOG — WBWSStrategy Migration Project

Architectural and design decisions made during sessions. Each entry records
the decision taken, the alternatives considered, and the rationale. This file
is append-only — never edit past entries.

---

## Format

```
## DEC-NNN — Short Title
Date: YYYY-MM-DD | Session: N | Status: DECIDED | Author: Session log
Decision: one-liner
Context: why the decision was needed
Options considered: A / B / C
Chosen: A
Rationale: ...
Trade-offs accepted: ...
```

---

## DEC-001 — TradeAnalytics aggregates MetricsReport
**Date**: 2026-02-16 | **Session**: 14 | **Status**: DECIDED

**Decision**: `TradeAnalytics` receives a pre-computed `MetricsReport` as input
(optional — auto-calculates if `None`) rather than computing metrics internally.

**Context**: Two modules needed to be designed: `MetricsCalculator` (fast, always runs)
and `TradeAnalytics` (richer, analytical). The question was where metrics calculation lived.

**Options**:
- A ✅ — TradeAnalytics aggregates MetricsReport (chosen)
- B — ReportGenerator aggregates both MetricsReport and AnalyticsReport
- C — TradeAnalytics calculates metrics internally (duplication)

**Rationale**: Natural dependency — analytics need metrics to produce insights. No
duplication with MetricsCalculator. Keeps ReportGenerator simple (pure visualisation).
Optional parameter allows both the "convenient" and "explicit" usage patterns.

**Trade-offs accepted**: Slight API surface complexity (optional `metrics` param).

---

## DEC-002 — Optional metrics parameter pattern
**Date**: 2026-02-16 | **Session**: 14 | **Status**: DECIDED

**Decision**: `TradeAnalytics.analyze(trade_result, config, metrics=None)` — metrics
are optional and auto-calculated if not provided.

**Context**: Backtester workflows pre-calculate metrics for persistence; other workflows
want a single call. Both need to be supported without code duplication.

**Options**:
- A ✅ — Optional parameter, auto-calculate if None (chosen)
- B — Two separate methods: `analyze()` and `analyze_with_metrics()`
- C — Always require pre-calculated metrics (breaks convenience)

**Rationale**: One entry-point is cleaner API. Python's `Optional` + `None` default is
idiomatic. Expert users pass explicit metrics; beginners get convenience.

**Trade-offs accepted**: Minor runtime overhead (one extra isinstance check).

---

## DEC-003 — AI-like insights with confidence levels
**Date**: 2026-02-16 | **Session**: 14 | **Status**: DECIDED

**Decision**: Every analytical observation is wrapped in an `Insight` dataclass with
`message`, `recommendation`, `confidence`, `impact_estimate`, `category`, `severity`.

**Context**: Raw data dumps (dict of metrics) require the human to interpret. The goal
is an analytics layer that adds value — specifically, actionable recommendations.

**Options**:
- A ✅ — Structured Insight objects with confidence and severity (chosen)
- B — Free-text markdown paragraphs only
- C — Raw dict/JSON without interpretation layer

**Rationale**: Structured insights are filterable, sortable, and renderable. Confidence
levels let users prioritise. Severity (critical/warning/info/success) maps naturally to
visual indicators in the HTML report. Impact estimates guide effort allocation.

**Trade-offs accepted**: More boilerplate per insight; insight generation rules must be
maintained as strategy evolves.

---

## DEC-004 — Markdown as primary output, JSON as secondary
**Date**: 2026-02-16 | **Session**: 14 | **Status**: DECIDED

**Decision**: `TradeAnalytics` primary output is human-readable Markdown
(consulting-report style). Structured JSON available via `.to_dict()` / `.to_json()`.

**Context**: Decision-makers need text, not JSON. But ReportGenerator and automated
pipelines need structured data.

**Options**:
- A ✅ — Markdown primary, JSON secondary (chosen)
- B — JSON primary, Markdown generated by ReportGenerator
- C — HTML only (no intermediate text format)

**Rationale**: Markdown is portable (works in terminals, GitHub, Notion, Slack).
Structured JSON is available for any downstream consumer. The two formats serve
different audiences and are both generated cheaply.

**Trade-offs accepted**: Two output formats to maintain; Markdown format must be kept
in sync with the AnalyticsReport data model.

---

## DEC-005 — Single self-contained HTML file (no external assets at runtime)
**Date**: 2026-02-17 | **Session**: 17 | **Status**: DECIDED

**Decision**: `ReportGenerator` produces a single `.html` file with all CSS and JS
inlined. Chart.js is loaded from CDN (one external dependency, acceptable).

**Context**: Reports need to be shareable by email / file transfer without requiring
a web server, build step, or asset folder alongside the HTML.

**Options**:
- A ✅ — Single HTML, inline CSS/JS, CDN for Chart.js (chosen)
- B — HTML + CSS file + JS file in output directory
- C — HTML + bundled Chart.js (very large file, ~200KB extra)
- D — React/Next.js app (requires server, overkill)

**Rationale**: Single file is maximally portable. CDN for Chart.js is acceptable
because: (a) the report is viewed in a browser that is usually online, and (b)
a CDN failure handler was added in Session 18 for offline resilience.

**Trade-offs accepted**: CDN dependency (mitigated by fallback). Large HTML file
(~32KB — acceptable). No hot-reload in development.

---

## DEC-006 — Three-layer tabbed report structure
**Date**: 2026-02-17 | **Session**: 17 | **Status**: DECIDED

**Decision**: HTML report has three tabs: Executive (always visible first), Analytical
(charts + insights), Raw Data (collapsible tables, optional via `ReportConfig`).

**Context**: Different audiences need different depths: executives want a one-glance
summary; analysts want charts; quants want raw numbers. All in one file.

**Options**:
- A ✅ — Three tabs, lazy chart initialisation on first visit (chosen)
- B — Single scrollable page (no separation of concerns)
- C — Separate HTML files per layer

**Rationale**: Tabs give clean separation without multiple files. Lazy chart init means
the Executive tab loads instantly even with many Chart.js canvases. `include_raw_data`
flag lets callers suppress Layer 3 for cleaner exports.

**Trade-offs accepted**: JS required for tab switching (mitigated by noscript CSS).

---

## DEC-007 — Dark/light theme via ReportConfig, not auto-detection
**Date**: 2026-02-17 | **Session**: 17 | **Status**: DECIDED

**Decision**: Theme is controlled by `ReportConfig(theme="dark"|"light")`. No
`prefers-color-scheme` CSS media query auto-detection.

**Context**: Reports are often shared. The sender's intent (dark for internal dashboards,
light for client-facing PDFs) should not be overridden by the viewer's OS preference.

**Options**:
- A ✅ — Explicit `ReportConfig.theme` (chosen)
- B — CSS `prefers-color-scheme` auto-detection
- C — JS toggle button in the report itself

**Trade-offs accepted**: The recipient cannot toggle theme. A JS toggle could be added
as a v2.0 enhancement (see `POST_MIGRATION_ROADMAP.md`).

---

## DEC-008 — Chart.js (CDN) over Plotly or Matplotlib
**Date**: 2026-02-17 | **Session**: 17 | **Status**: DECIDED

**Decision**: Chart.js from CDN for interactive charts. Plotly and Matplotlib deferred.

**Context**: Three charting options were evaluated for v1.0.

**Options**:
- A ✅ — Chart.js 4.x from CDN (chosen)
- B — Plotly.js (much larger bundle, ~3MB CDN)
- C — Matplotlib (server-side PNG, no interactivity)
- D — D3.js (powerful but high implementation cost)

**Rationale**: Chart.js is lightweight (~200KB), produces good-looking interactive
charts, requires no build step, and has a simple JSON-driven API that maps naturally
to Python-generated data. Plotly is better for scientific / 3D charts — deferred
to v2.0 roadmap.

**Trade-offs accepted**: Less flexibility than Plotly/D3 for complex visualisations.
Mitigated by CDN failure handler added in Session 18.

---

## DEC-009 — HTML polish Track A before Excel Track B
**Date**: 2026-02-17 | **Session**: 18 | **Status**: DECIDED

**Decision**: Session 18 focuses on HTML polish (6 targeted fixes) rather than starting
Excel output (Track B).

**Context**: The v1.0 sample report revealed 6 UX issues (equity placeholder, noisy
hour table, mobile breakpoints, accordion UX, CDN fallback). Excel output is a new
format requiring its own skill (`xlsx/SKILL.md`) and ~90 extra minutes.

**Rationale**: Fixing the HTML brings v1.0 to production quality before adding new
formats. Excel can be deferred without blocking any current workflow — `AnalyticsReport`
already has `.to_dict()` / `.to_json()` that cover the data-export use case today.
Excel is documented in `POST_MIGRATION_ROADMAP.md` for the post-migration phase.

**Trade-offs accepted**: Excel output not available until post-migration. Acceptable
because JSON export already covers programmatic data needs.

---

## DEC-010 — MagicMock cleanup deferred until all report formats in production
**Date**: 2026-02-17 | **Sessions**: 15-18 | **Status**: DECIDED (carry forward)

**Decision**: Test files that use `MagicMock` for `MetricsReport` and `TradeResult`
will be refactored to use real dataclasses only after ReportGenerator reaches
production stability.

**Affected files**:
- `tests/migration/test_trade_analytics_session15.py`
- `tests/migration/test_trade_analytics_session16.py`
- `tests/migration/test_analytics_contracts.py`
- `tests/migration/test_report_generator_session17.py`

**Rationale**: MagicMock is acceptable during rapid development. Replacing it during
active development adds churn; doing it once after stabilisation is more efficient.

**Trade-offs accepted**: Some tests are not fully realistic. Compensated by end-to-end
integration tests using real data.
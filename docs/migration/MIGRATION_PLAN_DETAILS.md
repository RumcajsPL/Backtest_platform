# WBWSStrategy Migration Plan - UPDATED

## Overview
This document tracks the detailed migration roadmap from dict-based to typed contract architecture.

**Last Updated**: 2025-02-14 Session 10.1  
**Status**: Phase 4 ✅ COMPLETE, Phase 5 Ready to Start

---

## Phase 0: Foundation ✅ COMPLETE
**Goal**: Establish migration framework and define core contracts

### Step 0.1: Documentation Setup ✅
- [x] Create PROJECT_CHARTER.md
- [x] Create MIGRATION_PLAN.md
- [x] Create SESSION_LOG.md
- [x] Create DECISION_LOG.md
- [x] Create DEPENDENCY_MAP.md

### Step 0.2: Define Core Contracts ✅
- [x] Create `src/strategies/contracts/data_contracts.py`
- [x] Create `src/strategies/contracts/signal_contracts.py`
- [x] Create `src/strategies/contracts/trade_contracts.py`
- [x] Create `src/strategies/contracts/__init__.py`

### Step 0.3: DataLoader Audit ✅
- [x] Complete deep audit (inputs, outputs, assumptions)
- [x] Document implicit contracts
- [x] Identify breaking changes
- [x] Create DATALOADER_AUDIT.md

### Step 0.4: Validation Framework ✅
- [x] Create test harness template
- [x] Set up performance benchmarking
- [x] Define acceptance criteria

---

## Phase 1: Data Layer ✅ COMPLETE

### Step 1.1: DataLoader Design ✅ COMPLETE
- [x] Design DataBundle contract
- [x] Design DataConfig contract
- [x] Design DataInfo contract
- [x] Design DataValidationResult contract
- [x] Plan backward compatibility

### Step 1.2: DataLoader Implementation ✅ COMPLETE
- [x] Create `data_loader.py` in `specific/modules/`
- [x] Implement DataBundle output
- [x] Add validation logic
- [x] Add ARTF (monthly) data support
- [x] Add dual-mode support (core/debug)
- [x] Optimization #1: Optional content hash (5-10% speedup)
- [x] Optimization #2: Fast sanitization (3-5% speedup)
- [x] Parquet performance fixes (80% faster)

### Step 1.3: Integration Test ✅ COMPLETE
- [x] Compare outputs with old DataLoader - **PASS**
- [x] Validate cache behavior - **PASS**
- [x] Benchmark performance - **PASS** (20-40% improvement)
- [x] Test Parquet vs CSV - **PASS** (Parquet now faster)
- [x] Test dual-mode execution - **PASS**
- [x] Update documentation

**PHASE 1 COMPLETE**: DataLoader v2.1 FINAL is production-ready ✅
- Performance: 80% faster Parquet, 8-15% overall improvement
- Features: Typed contracts, ARTF data, dual-mode execution
- Status: Ready for deployment

---

## Phase 2: Signal Layer ✅ COMPLETE
**Goal**: Migrate SignalGenerator to typed contracts

### Step 2.1: Signal Contracts ✅ COMPLETE
- [x] Review existing signal_frame.py, trade_direction.py
- [x] Design Signal contract
- [x] Design SignalType enum
- [x] Design SignalMetadata contract
- [x] Integration plan with DataBundle

### Step 2.2: SignalGenerator Implementation ✅ COMPLETE
- [x] Create new SignalGenerator
- [x] Integrate with DataBundle
- [x] Output typed Signals
- [x] Maintain WBWSTrigger compatibility
- [x] Performance test

### Step 2.3: Integration Test ✅ COMPLETE
- [x] Compare signal outputs (old vs new)
- [x] Validate WBWSTrigger integration
- [x] Validate filter pipeline compatibility
- [x] Benchmark performance

**PHASE 2 COMPLETE**: SignalGenerator v2.2 FINAL is production-ready ✅
- Performance: Perfect parity, 5% faster in core mode, acceptable for debug mode
- Features: Dual-mode execution
- Status: Ready for deployment

---

## Phase 3: Filter Layer ✅ COMPLETE
**Goal**: Standardize filter interfaces

### Step 3.1: Filter Contracts ✅ COMPLETE
- [x] Design FilterResult contract
- [x] Standardize filter interface protocol
- [x] Plan filter pipeline architecture

### Step 3.2: Filter Migration (Thin Slice) ✅ COMPLETE
- [x] Migrate time filter
- [x] Migrate technical filters (11 filters)
- [x] Create new FilterPipeline
- [x] Performance test

**PHASE 3 COMPLETE**: FilterPipeline v3.x FINAL is production-ready ✅

---

## Phase 4: Trade Management ✅ COMPLETE (SESSION 10.1)
**Goal**: Replace string-based trade logic with typed contracts

### Step 4.1: Trade Contracts ✅ COMPLETE
- [x] Review existing trade_*.py files
- [x] Finalize TradeDirection enum
- [x] Finalize TradeParameters contract
- [x] Design DecisionType
- [x] Design RejectedSignal contract (Session 10.1) ⭐ NEW!

### Step 4.2: Trade Management Migration ✅ COMPLETE
- [x] Refactor RiskManager (returns TradeParameters)
- [x] Refactor SpreadManager
- [x] Refactor TradeManager (returns TradeDecision)
- [x] Integration test

### Step 4.3: TradeSimulator Migration ✅ COMPLETE
- [x] Internal Trade contract usage (Session 10)
- [x] RejectedSignal for rejected signals (Session 10.1) ⭐ NEW!
- [x] Entry logic using contracts
- [x] Exit logic using contracts
- [x] LTF execution with contracts
- [x] Progressive tracking integration
- [x] Performance test - **PASS** (4.5% FASTER than legacy!) 🚀
- [x] Parity test - **PASS** (100% match)
- [x] All 12 tests pass ✅

**PHASE 4 COMPLETE**: TradeSimulator v4.5.1 FINAL is production-ready ✅
- Performance: **4.5% FASTER than legacy** (0.95x ratio) 🚀
- Features: Trade contracts, RejectedSignal, dual-mode execution
- Architecture: Risk before TradeManager (validated)
- Status: Ready for Session 11 (TradeResult output)

**Key Achievement (Session 10.1)**:
- RejectedSignal contract separates rejected signals from trades
- Clean design: trades vs rejections
- No legacy compatibility hacks
- Type-safe validation

---

## Phase 5: TradeResult Output ⏳ SESSION 11
**Goal**: Remove dict output layer, return TradeResult contract

### Step 5.1: TradeResult Output Migration ⏳ SESSION 11
- [ ] Update simulate_trades() return type to TradeResult
- [ ] Add TradeResult.from_trades() classmethod
- [ ] Remove dict conversion layer
- [ ] Update test suite for contracts
- [ ] Performance validation
- [ ] Backward compatibility via to_dict() (if needed)

**Expected Duration**: 2-3 hours  
**Expected Outcome**: Pure contract architecture end-to-end

---

## Phase 6: Reporting Layer ⏳ DESIGN PHASE
**Goal**: Define requirements and architecture for reporting modules

### Critical Design Questions (SESSION 11+)

#### ProgressiveTracker
**Requirements to Define**:
1. What data should be tracked at each stage?
   - Signal generation stage?
   - Filter stage?
   - Risk management stage?
   - Position management stage?
   - Trade execution stage?

2. How should it integrate?
   - Observer pattern (passive tracking)?
   - Direct calls from each module?
   - Event-based system?

3. What to keep from legacy?
   - Progressive CSV export?
   - Stage-by-stage snapshots?
   - Or completely new design?

#### MetricsCalculator
**Requirements to Define**:
1. What metrics are essential?
   - Basic: Win rate, P&L, Sharpe ratio
   - Advanced: MAE, MFE, consecutive wins/losses
   - Custom: Strategy-specific metrics

2. Input contracts?
   - Consume `TradeResult` directly?
   - Or separate `Trade` analysis?

3. Output format?
   - Typed `MetricsReport` contract?
   - Or flexible dict/DataFrame?

#### ReportGenerator
**Requirements to Define**:
1. What reports are needed?
   - Performance summary?
   - Trade journal?
   - Risk analysis?
   - Progressive tracking visualization?

2. Output formats?
   - CSV export?
   - HTML reports?
   - JSON for APIs?
   - Excel spreadsheets?

3. Integration with orchestrator?
   - Standalone tool?
   - Or part of backtest pipeline?

### Step 6.1: Requirements Definition ⏳ SESSION 11+
- [ ] ProgressiveTracker requirements documented
- [ ] MetricsCalculator requirements documented
- [ ] ReportGenerator requirements documented
- [ ] Integration strategy with contracts defined
- [ ] Output formats and use cases specified

### Step 6.2: ProgressiveTracker Design ⏳ PENDING
- [ ] Define what to track at each stage
- [ ] Design integration pattern (observer/event/direct)
- [ ] Contract interfaces
- [ ] Performance considerations

### Step 6.3: MetricsCalculator Design ⏳ PENDING
- [ ] Define essential metrics
- [ ] Design MetricsReport contract
- [ ] Input: TradeResult contract
- [ ] Output: Typed metrics

### Step 6.4: ReportGenerator Design ⏳ PENDING
- [ ] Define report types needed
- [ ] Output formats (CSV, HTML, JSON, Excel)
- [ ] Integration with orchestrator
- [ ] Contract-based inputs

---

## Phase 7: Integration ⏳ PENDING
**Goal**: Wire all new modules together

### Step 7.1: New Runner
- [ ] Create run_wbws_strategy_v2.py
- [ ] Parallel execution test (old vs new)
- [ ] Performance comparison
- [ ] Final validation

---

## Phase 8: Cleanup ⏳ PENDING
**Goal**: Finalize migration

### Step 8.1: Documentation
- [ ] Update all module docstrings
- [ ] Create migration summary
- [ ] Archive decision logs

### Step 8.2: Handoff
- [ ] Code review
- [ ] Performance report
- [ ] Lessons learned document

---

## Progress Tracking

| Phase | Status | Completion | Notes |
|-------|--------|------------|-------|
| 0 - Foundation | ✅ Complete | 100% | Contracts defined |
| 1 - Data Layer | ✅ Complete | 100% | v2.1 FINAL ready |
| 2 - Signal Layer | ✅ Complete | 100% | v2.2 FINAL ready |
| 3 - Filter Layer | ✅ Complete | 100% | v3.x FINAL ready |
| 4 - Trade Mgmt | ✅ Complete | 100% | v4.5.1 FINAL ready, 4.5% faster! 🚀 |
| 5 - TradeResult | ⏳ Ready | 0% | Session 11 - Contract output |
| 6 - Reporting | ⏳ Design | 0% | Requirements definition needed |
| 7 - Integration | ⏳ Pending | 0% | After reporting modules |
| 8 - Cleanup | ⏳ Pending | 0% | Final phase |

---

## Session History

### Session 1 (2025-02-09)
- Foundation setup
- Initial DataLoader implementation
- Identified performance issue (+31% regression)
- Status: Incomplete due to performance concerns

### Session 2 (2025-02-10) ✅
- Fixed Parquet performance (80% faster)
- Added ARTF (monthly) data support
- Implemented dual-mode execution
- Applied optimizations #1 and #2 (8-15% additional speedup)
- **Result**: Phase 1 COMPLETE, DataLoader production-ready

### Sessions 3-9 ✅
- Phase 2: SignalGenerator migration
- Phase 3: FilterPipeline migration
- Phase 4: RiskManager, TradeManager migration
- Phase 4: TradeSimulator migration (Sessions 7-9)

### Session 10 (2025-02-14) ✅
- TradeSimulator internal contract usage
- Trade contracts: Entry, Exit, Trade
- Dict output for backward compatibility
- Smoke test: 9/9 pass ✅

### Session 10.1 (2025-02-14) ✅
- **Issue**: TradeEntry validation rejected entry_price=0.0
- **Solution**: RejectedSignal contract (separate from Trade)
- **Design Principle**: No legacy compatibility required
- **Result**: Phase 4 COMPLETE
- **Performance**: 4.5% FASTER than legacy! 🚀
- **Tests**: 12/12 PASS ✅

### Session 11 (Upcoming)
- TradeResult contract output
- Remove dict conversion layer
- Test suite migration
- Design discussions for reporting modules

---

## Design Principles (Established Session 10)

### No Legacy Compatibility Required
> "We migrate based on legacy but create a completely new parallel tool. Parity is for validation only, not runtime compatibility."

**Implications**:
1. ✅ Design for clarity, not legacy artifacts
2. ✅ Parity validates correctness, not compatibility
3. ✅ Clean contracts over backward compatibility
4. ✅ Legacy tools can convert if needed (via .to_dict())

### Separation of Concerns
```
Trade          = Executed (has prices, P&L)
RejectedSignal = Filtered (has reason, no prices)
```

**No overlap, clean boundaries**

### Type Safety Throughout
```python
direction: TradeDirection  # Not str
exit_reason: ExitReason    # Not str
timestamp: pd.Timestamp    # Not datetime
```

### Performance First
- New architecture: 4.5% FASTER than legacy ✅
- Contract overhead: Negligible
- Frozen dataclasses: Efficient

---

**Last Updated**: 2025-02-14 Session 10.1  
**Current Phase**: Phase 5 - TradeResult Output (Ready to Start)  
**Overall Progress**: 4/8 phases complete (50%)  
**Performance**: 🚀 NEW ARCHITECTURE IS FASTER THAN LEGACY!
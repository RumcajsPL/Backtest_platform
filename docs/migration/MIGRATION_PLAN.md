# WBWSStrategy Migration Plan - UPDATED

## Overview
This document tracks the detailed migration roadmap from dict-based to typed contract architecture.

**Last Updated**: 2025-02-10 Session 2
**Status**: Phase 1 ✅ COMPLETE, Phase 2 Ready to Start

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

## Phase 2: Signal Layer ⏳ READY TO START
**Goal**: Migrate SignalGenerator to typed contracts

### Step 2.1: Signal Contracts
- [x] Review existing signal_frame.py, trade_direction.py
- [x] Design Signal contract
- [x] Design SignalType enum
- [x] Design SignalMetadata contract
- [x] Integration plan with DataBundle

### Step 2.2: SignalGenerator Implementation
- [x] Create new SignalGenerator
- [x] Integrate with DataBundle
- [x] Output typed Signals
- [x] Maintain WBWSTrigger compatibility
- [x] Performance test

### Step 2.3: Integration Test
- [x] Compare signal outputs (old vs new)
- [x] Validate WBWSTrigger integration
- [x] Validate filter pipeline compatibility
- [x] Benchmark performance

**PHASE 2 COMPLETE**: SignalDenerator v2.2 FINAL is production-ready ✅
- Performance: perfect parity, 5% faster in core mode, acceptable for debug mode
- Features: dual-mode execution
- Status: Ready for deployment
---

## Phase 3: Filter Layer ⏳ PENDING
**Goal**: Standardize filter interfaces

### Step 3.1: Filter Contracts
- [ ] Design FilterResult contract
- [ ] Standardize filter interface protocol
- [ ] Plan filter pipeline architecture

### Step 3.2: Filter Migration (Thin Slice)
- [ ] Migrate time filter
- [ ] Migrate technical filters (11 filters)
- [ ] Create new FilterPipeline
- [ ] Performance test

---

## Phase 4: Trade Management ⏳ PENDING
**Goal**: Replace string-based trade logic with typed contracts

### Step 4.1: Trade Contracts
- [ ] Review existing trade_*.py files
- [ ] Finalize TradeDirection enum
- [ ] Finalize TradeParameters contract
- [ ] Design DecisionType

### Step 4.2: Trade Management Migration
- [ ] Refactor RiskManager
- [ ] Refactor SpreadManager
- [ ] Refactor TradeManager
- [ ] Integration test

---

## Phase 5: Execution Layer ⏳ PENDING
**Goal**: Migrate TradeSimulator to TradeRecord-based execution

### Step 5.1: Execution Contracts
- [ ] Review existing trade_record.py
- [ ] Design TradeRecord contract
- [ ] Design execution state machine

### Step 5.2: Simulator Migration (Thin Slice)
- [ ] Entry logic migration
- [ ] Exit logic migration
- [ ] LTF execution migration
- [ ] Progressive tracking integration
- [ ] Performance test

---

## Phase 6: Reporting Layer ⏳ PENDING
**Goal**: Adapt metrics and reporting to typed trades

### Step 6.1: Metrics Migration
- [ ] Update MetricsCalculator for TradeRecord
- [ ] Create typed metrics objects

### Step 6.2: Reporting Migration
- [ ] Update ReportGenerator
- [ ] Validate output formats

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
| 2 - Signal Layer | ⏳ Ready | 0% | Starting Session 3 |
| 3 - Filter Layer | ⏳ Pending | 0% | |
| 4 - Trade Mgmt | ⏳ Pending | 0% | |
| 5 - Execution | ⏳ Pending | 0% | |
| 6 - Reporting | ⏳ Pending | 0% | |
| 7 - Integration | ⏳ Pending | 0% | |
| 8 - Cleanup | ⏳ Pending | 0% | |

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
- **Next**: Phase 2 - Signal Layer migration

---

**Last Updated**: 2025-02-10
**Current Phase**: Phase 2 - Signal Layer (Ready to Start)
**Overall Progress**: 2/9 phases complete (22%)
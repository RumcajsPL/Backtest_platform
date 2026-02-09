# WBWSStrategy Migration Plan

## Overview
This document tracks the detailed migration roadmap from dict-based to typed contract architecture.

---

## Phase 0: Foundation ✅ IN PROGRESS
**Goal**: Establish migration framework and define core contracts

### Step 0.1: Documentation Setup ✅
- [ ] Create PROJECT_CHARTER.md
- [ ] Create MIGRATION_PLAN.md
- [ ] Create SESSION_LOG.md
- [ ] Create DECISION_LOG.md
- [ ] Create DEPENDENCY_MAP.md

### Step 0.2: Define Core Contracts 🔄
- [ ] Create `src/strategies/contracts/data_contracts.py`
- [ ] Create `src/strategies/contracts/signal_contracts.py`
- [ ] Create `src/strategies/contracts/trade_contracts.py`
- [ ] Create `src/strategies/contracts/__init__.py`

### Step 0.3: DataLoader Audit 🔄
- [ ] Complete deep audit (inputs, outputs, assumptions)
- [ ] Document implicit contracts
- [ ] Identify breaking changes
- [ ] Create DATALOADER_AUDIT.md

### Step 0.4: Validation Framework
- [ ] Create test harness template
- [ ] Set up performance benchmarking
- [ ] Define acceptance criteria

---
## Phase 1: Data Layer 🔄 IN PROGRESS

### Step 1.1: DataLoader Design ✅ COMPLETE
- [x] Design DataBundle contract
- [x] Design DataConfig contract
- [x] Plan backward compatibility

### Step 1.2: DataLoader Implementation ✅ COMPLETE (with issue)
- [x] Create `data_loader.py` in `specific/modules/`
- [x] Implement DataBundle output
- [x] Add validation logic
- [⚠️] Performance test - **FAILED** (+31% regression)

### Step 1.3: Integration Test ⏳ BLOCKED
- [x] Compare outputs with old DataLoader - **PASS**
- [x] Validate cache behavior - **PASS**
- [⚠️] Benchmark performance - **FAIL** (need optimization)
- [ ] Update documentation

**BLOCKER**: Performance regression must be fixed before proceeding to Phase 2
---

## Phase 2: Signal Layer ⏳ PENDING
**Goal**: Migrate SignalGenerator to typed contracts

### Step 2.1: Signal Contracts
- [ ] Review existing signal_frame.py, trade_direction.py
- [ ] Design Signal contract
- [ ] Design SignalType enum
- [ ] Integration plan

### Step 2.2: SignalGenerator Implementation
- [ ] Create new SignalGenerator
- [ ] Integrate with DataBundle
- [ ] Output typed Signals
- [ ] Performance test

### Step 2.3: Integration Test
- [ ] Compare signal outputs
- [ ] Validate WBWSTrigger integration
- [ ] Benchmark performance

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
| 0 - Foundation | 🔄 In Progress | 30% | Contracts being defined |
| 1 - Data Layer | ⏳ Pending | 0% | |
| 2 - Signal Layer | ⏳ Pending | 0% | |
| 3 - Filter Layer | ⏳ Pending | 0% | |
| 4 - Trade Mgmt | ⏳ Pending | 0% | |
| 5 - Execution | ⏳ Pending | 0% | |
| 6 - Reporting | ⏳ Pending | 0% | |
| 7 - Integration | ⏳ Pending | 0% | |
| 8 - Cleanup | ⏳ Pending | 0% | |

**Last Updated**: [SESSION DATE]
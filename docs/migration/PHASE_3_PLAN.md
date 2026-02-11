# PHASE_3_PLAN.md - Filter Layer Migration

## Overview
**Goal**: Standardize filter interfaces and migrate FilterPipeline to use typed contracts, ensuring seamless integration with SignalGenerator (Phase 2) outputs. Maintain performance parity and enable dual-mode execution (core/debug).

**Session**: 4 (2025-02-11) - Initiate and complete Phase 3  
**Dependencies**: Phase 2 complete (Signal Layer)  
**Estimated Duration**: 1 session (4-6 hours)  
**Approach**: Thin Slice - Migrate time filter first, then batch technical filters.  
**Success Criteria**: 
- All filters use typed FilterResult contracts.
- New FilterPipeline outputs match old system (signal parity test).
- Performance ≤110% of baseline (benchmark on 3-day dataset).

**Last Updated**: 2025-02-11  
**Status**: Ready to Start

---

## High-Level Objectives
1. **Contract Design**: Define standardized FilterResult and interface protocols for consistency across all filters.
2. **Migration Execution**: Refactor filters to consume typed Signals and produce typed outputs.
3. **Pipeline Integration**: Build new FilterPipeline that chains filters efficiently.
4. **Validation & Testing**: Ensure functional parity, performance, and debug mode support.

---

## Detailed Planning

### Step 3.1: Filter Contracts Design (1-2 hours)
- Review existing filters in `src/strategies/core/filters/` (time_manager.py + 11 technical: ADX, Bollinger, CCI, Choppiness, DPO, MA, MACD, Pivot, RSI, Supertrend).
- Design `FilterResult` dataclass: Include fields like `passed: bool`, `signal: Signal`, `metadata: dict` (for debug info).
- Standardize `FilterInterface` protocol: Methods like `apply(signal: Signal) -> FilterResult`.
- Plan FilterPipeline architecture: Chainable list of filters, with early-exit optimization for failed signals.
- Update `src/strategies/contracts/filter_contracts.py` with new types.
- Document assumptions (e.g., input from SignalGenerator, handling of null signals).

### Step 3.2: Filter Migration & Implementation (2-3 hours)
- Create `src/strategies/specific/filters/` directory.
- Migrate Time Filter (`TimeManager`): Refactor to use typed inputs/outputs, add dual-mode (core: fast pass/fail; debug: full logging).
- Batch Migrate Technical Filters: Group similar ones (e.g., oscillator-based: CCI/RSI/MACD), refactor each to FilterInterface.
- Implement New `FilterPipeline`: In `src/strategies/specific/core/filter_pipeline.py`, support sequential application with configurable order from YAML.
- Integrate with Phase 2 outputs: Test end-to-end from SignalGenerator -> FilterPipeline.
- Add optimizations: Vectorized checks where possible, cache filter params.

### Step 3.3: Integration Testing & Validation (1 hour)
- Run parallel tests: Compare old `core/filter_pipeline.py` vs new `specific/filter_pipeline.py` outputs on 3-day dataset.
- Performance benchmark: Use `timeit` or `psutil` to measure execution time (target: <110% baseline).
- Metrics parity: Validate filtered signal counts and contents match.
- Dual-mode test: Core mode (fast), debug mode (with progressive tracking signals).
- Handle edge cases: Empty signals, invalid timestamps, filter failures.
- Update MIGRATION_PLAN.md with completion status.

---

## Analysis Scripts & Tools
- **Audit Script**: `scripts/validation/audit_filters.py` - Analyze old filters for input/output patterns (e.g., count dict keys, identify assumptions). Usage: `python scripts/validation/audit_filters.py --filter-dir src/strategies/core/filters`.
- **Benchmark Script**: `scripts/validation/benchmark_filters.py` - Compare old/new pipeline performance. Usage: `python scripts/validation/benchmark_filters.py --dataset test_3days.parquet --mode core`.
- **Parity Test Script**: `scripts/validation/test_filter_parity.py` - Diff old/new filtered signals. Usage: `python scripts/validation/test_filter_parity.py --input-signals signals.json --expected old_filtered.json`.
- **Tools**: Use Jupyter notebook in `notebooks/` for quick prototyping (e.g., `filter_migration.ipynb` for contract testing).

---

## Risks & Mitigations
- **Risk**: Filter order dependencies break parity. **Mitigation**: Configurable YAML ordering, thin-slice validation.
- **Risk**: Performance drop in debug mode. **Mitigation**: Optional logging flags.
- **Rollback**: If issues, revert to old pipeline; mark phase incomplete in SESSION_LOG.md.

**Next Phase**: Phase 4 - Trade Management (Session 5)
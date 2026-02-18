# PHASE 8 CODE SCAN REPORT — Index

**Created**: Session 19  
**Structure**: This index + 7 chapter files (one per pipeline stage)  
**Status**: ⏳ Template — to be completed in Session 19

---

## Scan Summary

> _Fill in after completing all 7 chapters._

| Metric | Value |
|--------|-------|
| Files scanned | — |
| Issues found — P0 (correctness) | — |
| Issues found — P1 (quality) | — |
| Issues found — P2 (observability) | — |
| Items added to POST_MIGRATION_ROADMAP | — |
| Session date | — |

---

## Chapter Files

| Chapter | File | Pipeline Stage | Status |
|---------|------|---------------|--------|
| 1 | `SCAN_CH1_bootstrap.md` | Strategy bootstrap & config | ⏳ |
| 2 | `SCAN_CH2_data_layer.md` | DataLoader + DataBundle | ⏳ |
| 3 | `SCAN_CH3_signals.md` | SignalGenerator + SignalFrame | ⏳ |
| 4 | `SCAN_CH4_filters.md` | FilterPipeline + all filters | ⏳ |
| 5 | `SCAN_CH5_simulation.md` | TradeSimulator + Risk + TradeManager | ⏳ |
| 6 | `SCAN_CH6_analytics.md` | MetricsCalculator + TradeAnalytics | ⏳ |
| 7 | `SCAN_CH7_reporting.md` | ReportGenerator + contracts | ⏳ |

---

## Consolidated Findings

> _Populated after all chapters complete._

### P0 — Fix Immediately (correctness, must fix before Session 20)

| # | Issue | File | Chapter | Source |
|---|-------|------|---------|--------|
| P0-1 | Core mode trade simulation 11% **slower** than Debug | `trade_simulator.py` | 5 | E2E test |
| P0-2 | Cache hit rate 50% on hot runs (target: 100%) | `filter_pipeline.py` / cache | 4 | E2E test |
| — | _additional items from Session 19 scan_ | | | |

### P1 — Fix in Session 20 (quality, edge cases)

| # | Issue | File | Line | Proposed Fix |
|---|-------|------|------|-------------|
| — | | | | |

### P2 — Fix in Session 21 (observability, logging)

| # | Issue | File | Proposed Fix |
|---|-------|------|-------------|
| — | | | |

### P3 — Add to POST_MIGRATION_ROADMAP

| # | Item | Chapter | Suggested Section |
|---|------|---------|------------------|
| — | | | |

---

## Action Plan (feeds SESSION_20_HANDOFF)

> _Written after all chapters complete._

**Session 20 target**: Fix all P0 + P1 items, expand test coverage for affected modules.  
**Session 21 target**: Add structured logging (P2), observability layer.  
**Session 22 target**: MagicMock cleanup + end-to-end integration test.

---

## POST_MIGRATION_ROADMAP Updates

> _New items discovered during scan, added to appropriate chapter._

| Item | Chapter | Section | Effort |
|------|---------|---------|--------|
| _from scan_ | | | |
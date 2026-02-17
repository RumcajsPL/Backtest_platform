# SESSION 19 HANDOFF — Phase 8 Entry: Code Scan & Observability

**Date**: 2026-02-17  
**Phase**: 8.1 — Infrastructure Completion: Full Code Scan  
**Prerequisites**: Phase 7 ✅ COMPLETE (ReportGenerator v1.1, Session 18)

---

## ✅ COMPLETED PHASES SNAPSHOT

| Phase | Deliverable | Sessions | Tests | Status |
|-------|-------------|----------|-------|--------|
| 1 | DataLoader + DataBundle | 1-4 | — | ✅ |
| 2 | SignalGenerator + SignalFrame | 5-7 | — | ✅ |
| 3 | FilterPipeline + FilterResult | 8-10 | — | ✅ |
| 4 | TradeSimulator + TradeResult | 11-13 | — | ✅ |
| 5 | MetricsCalculator + MetricsReport | 13 | — | ✅ |
| 6 | TradeAnalytics + AnalyticsReport | 14-16 | 141 | ✅ |
| 7 | ReportGenerator v1.1 HTML | 17-18 | 131 | ✅ |
| **Phase 5-7 Total** | | | **374** | ✅ |

**Phase 8 starts now.** Goal: production hardening through full code scan, prioritised improvements, observability additions, and test coverage expansion.

---

## 📋 FILES TO OPEN AT SESSION 19 START

**Must read first (orient yourself):**
```
docs/migration/SESSION_19_HANDOFF.md          ← THIS FILE
docs/migration/POST_MIGRATION_ROADMAP.md      ← deferred items list
docs/migration/DECISION_LOG.md                ← DEC-001 to DEC-010
docs/architecture/ARCHITECTURE.md             ← v2.1
docs/migration/CONTRACTS_REFERENCE.md         ← v6.0
```

**Scan targets (read each before evaluating it):**
```
src/strategies/contracts/data_contracts.py
src/strategies/contracts/signal_contracts.py
src/strategies/contracts/filter_contracts.py
src/strategies/contracts/trade_contracts.py
src/strategies/contracts/metrics_contracts.py
src/strategies/contracts/analytics_contracts.py
src/strategies/contracts/report_contracts.py
src/strategies/specific/modules/data_loader.py
src/strategies/specific/modules/signal_generator.py
src/strategies/specific/modules/filter_pipeline.py
src/strategies/specific/modules/trade_simulator.py
src/strategies/specific/modules/risk_manager.py
src/strategies/specific/modules/trade_manager.py
src/strategies/specific/modules/metrics_calculator.py
src/strategies/specific/modules/trade_analytics.py
src/strategies/specific/modules/report_generator.py
src/strategies/specific/filters/base.py
src/strategies/specific/filters/time_filters/        ← whole directory
src/strategies/specific/filters/technical_filters/   ← whole directory
src/strategies/utils/paths.py
src/strategies/utils/validation.py
```

**Output at session end:**
```
docs/migration/PHASE8_SCAN_REPORT.md          ← new, created this session
docs/migration/POST_MIGRATION_ROADMAP.md      ← updated with scan findings
docs/migration/SESSION_20_HANDOFF.md          ← next session prep
```

---

## 🎯 SESSION 19 GOAL

**Deliverable**: `PHASE8_SCAN_REPORT.md` — a structured audit of the entire codebase,
producing a prioritised list of improvements that feeds directly into Sessions 20-22.

This is **read-mostly work**: open each file, evaluate it against the checklist in each
chapter below, and record findings. No refactoring this session — understanding first.

**Time budget** (single session, ~90 min):
- 10 min — orient, read this file, confirm project state
- 60 min — code scan (7 chapters, ~8 min each)
- 15 min — write PHASE8_SCAN_REPORT.md + update POST_MIGRATION_ROADMAP.md
- 5 min — write SESSION_20_HANDOFF.md

---

## 📖 SCAN CHAPTERS

Work through chapters in order. Each chapter covers a logical pipeline stage.
For each file: read it, run the checklist, record findings in PHASE8_SCAN_REPORT.md.

---

### Chapter 1 — Strategy Bootstrap & Configuration

**Files**:
```
src/strategies/specific/wbws_strategy.py   (or equivalent main entry point)
src/strategies/specific/config.py          (StrategyConfig)
src/strategies/utils/paths.py
src/strategies/utils/validation.py
```

**What to look for**:
- [ ] Is `StrategyConfig` a frozen dataclass? Are all fields typed?
- [ ] Are there hardcoded paths, debug flags, or `print()` statements?
- [ ] Is timezone handling explicit? Does config enforce UTC?
- [ ] Is there a clean `__main__` entry point or CLI hook?
- [ ] Does `paths.py` use `pathlib.Path` throughout (not `os.path`)?
- [ ] Does `validation.py` contain reusable validators or is it ad hoc?
- [ ] Are there TODO/FIXME comments that indicate known debt?
- [ ] Does the strategy initialise logging correctly (module-level `logger`)?

**Key question**: Is the strategy startup sequence explicit, deterministic, and testable?

---

### Chapter 2 — Data Layer

**Files**:
```
src/strategies/contracts/data_contracts.py
src/strategies/specific/modules/data_loader.py
```

**What to look for**:
- [ ] `DataBundle` frozen? All fields typed? `__post_init__` validation present?
- [ ] `DataInfo` — does it capture bar count, date range, resolution?
- [ ] `DataValidationResult` — does it capture warnings (gaps, duplicates) not just pass/fail?
- [ ] `DataLoader.load_data()` — does it enforce UTC on all timestamps?
- [ ] Does it raise typed exceptions (not generic `Exception`)?
- [ ] Is LTF/ARTF loading lazy (only when requested) or always eager?
- [ ] Are there dtype optimisations? `float32` for OHLCV, `int8` for signals?
- [ ] Is there protection against forward-look bias (data slicing by date)?
- [ ] Is there a test for the data loader? Check `tests/` for coverage.
- [ ] Are large file reads logged with duration?

**Key question**: Would a malformed or timezone-naive CSV silently corrupt downstream results?

---

### Chapter 3 — Signal Generation

**Files**:
```
src/strategies/contracts/signal_contracts.py
src/strategies/specific/modules/signal_generator.py
```

**What to look for**:
- [ ] `SignalFrame` frozen? `signals` typed as `pd.Series` with `int8` dtype confirmed?
- [ ] `SignalType` enum — is `from_code()` safe against unknown codes?
- [ ] Does `SignalGenerator` validate that input `DataBundle` has required columns?
- [ ] Are indicators computed vectorised (no row-by-row loops)?
- [ ] Is indicator data stored lazily (`debug_mode` only)?
- [ ] Swing high/low detection — is the lookback period validated (not zero, not > data length)?
- [ ] Is there a guard against signals being generated on the last N bars (no execution window)?
- [ ] Are there any global/class-level mutable variables that could cause state bleed between runs?

**Key question**: Can the signal generator be called twice on the same data and return identical results (determinism)?

---

### Chapter 4 — Filter Pipeline

**Files**:
```
src/strategies/contracts/filter_contracts.py
src/strategies/specific/filters/base.py
src/strategies/specific/filters/time_filters/      ← enumerate all files
src/strategies/specific/filters/technical_filters/ ← enumerate all files
src/strategies/specific/modules/filter_pipeline.py
```

**What to look for**:

**Contracts**:
- [ ] `FilterResult` frozen? Does `FilterMetadata` capture rejection count and reasons?
- [ ] `FilterPipelineResult` — does `pass_rate` property guard against zero division?
- [ ] `FilterStatus` enum covers ERROR case — is it actually used when filters throw?

**Base filter**:
- [ ] Abstract base class defined? Does it enforce `apply()` signature?
- [ ] Is there a shared `_validate_signal_frame()` helper or is validation duplicated?

**Time filters** (check each file found):
- [ ] Session filter — are session boundaries in UTC? Documented?
- [ ] Day-of-week filter — does it handle market holidays?
- [ ] Is there a combined time filter or are they applied sequentially?

**Technical filters** (check each file found):
- [ ] Trend filter — is ADX/slope lookback configurable or hardcoded?
- [ ] Volatility filter — is ATR period configurable?
- [ ] Are filter thresholds validated against data availability (e.g. ATR needs N bars)?
- [ ] Do filters short-circuit correctly (early return on SKIPPED status)?

**Pipeline**:
- [ ] Is `FilterPipelineCache` used? Does it invalidate correctly?
- [ ] Are filters applied in a documented, deterministic order?
- [ ] Is there an integration test covering the full filter chain?

**Key question**: Can a single misconfigured filter silently pass all signals or block all signals with no warning?

---

### Chapter 5 — Trade Simulation

**Files**:
```
src/strategies/contracts/trade_contracts.py
src/strategies/specific/modules/trade_simulator.py
src/strategies/specific/modules/risk_manager.py
src/strategies/specific/modules/trade_manager.py
```

**What to look for**:

**Contracts**:
- [ ] `Trade`, `TradeEntry`, `TradeExit`, `TradeParameters` — all frozen?
- [ ] `TradeDirection.from_string()` — does it handle case-insensitive input?
- [ ] `ExitReason` enum — are all 6 reasons actually used in the simulator?
- [ ] `RejectedSignal` — is `meta: Dict` consistent (same keys every time) or freeform?
- [ ] `TradeResult` — does it expose `__len__` or `__iter__` for convenience?

**RiskManager**:
- [ ] Is spread application optional and documented (`spread_enabled` flag)?
- [ ] Is `risk_percentile_passed` validated against actual annual range data, or skipped when ARTF is absent?
- [ ] Are SL/TP distances validated to be positive and non-zero?
- [ ] Is there a maximum SL distance guard (prevents unreasonably wide stops)?

**TradeManager**:
- [ ] Pyramiding logic — is max open position count enforced?
- [ ] Opposite signal handling — is the close + reopen sequence atomic?
- [ ] Is there protection against duplicate signal IDs?

**TradeSimulator**:
- [ ] Is Numba JIT compilation handled gracefully if Numba is not installed?
- [ ] Are LTF windows precomputed once (not per-trade)?
- [ ] Is there a fallback execution mode (strategy-bar OHLC) when LTF is absent?
- [ ] Is `execution_time_ms` recorded correctly (wall-clock, not CPU time)?
- [ ] End-of-data handling — are open positions closed at last bar with `END_OF_DATA` reason?

**Key question**: Does the simulator produce the same result when run twice on identical data (determinism check)?

---

### Chapter 6 — Analytics Layer

**Files**:
```
src/strategies/contracts/metrics_contracts.py
src/strategies/contracts/analytics_contracts.py
src/strategies/specific/modules/metrics_calculator.py
src/strategies/specific/modules/trade_analytics.py
```

**What to look for**:

**MetricsCalculator**:
- [ ] `profit_factor` — guarded against zero gross loss (division by zero)?
- [ ] `max_drawdown` — returns negative value as documented?
- [ ] `trades_per_week` / `trades_per_day` — guarded against zero date range?
- [ ] `expectancy_points` formula — is it `(win_rate × avg_win) - (loss_rate × avg_loss)`?
- [ ] `winning_streak` / `losing_streak` — correct handling of single-trade case?
- [ ] Does `to_flat_dict()` produce consistent keys (for DB storage)?

**Analytics contracts**:
- [ ] `Insight.__post_init__` — validates all 4 constrained fields (`confidence`, `severity`, `category`)?
- [ ] `SessionMetrics` — `win_rate` validated as 0-100 range?
- [ ] `DurationAnalysis.fast_exits_pct` — consistent with count fields?
- [ ] `ComparativeContext` — `vs_baseline` and `percentile_rank` are `None` (v1.0) — documented as deferred?
- [ ] `AnalyticsReport.get_all_insights()` — does it include insights from all 3 domains (time + quality + risk)?

**TradeAnalytics**:
- [ ] Empty `trade_result` (zero trades) — does it raise or return a graceful empty report?
- [ ] `_classify_session()` — handles trades at exact boundary times (00:00, 08:00, 16:00)?
- [ ] Does `_analyze_time_performance()` handle a dataset with only one session?
- [ ] Insight thresholds (e.g. "critical if Asia PnL < -X") — are they configurable or hardcoded?
- [ ] `format_markdown_report()` — does it handle empty `critical_insights` list?
- [ ] `_save_report()` — does it create output directory if absent?

**Key question**: Does the analytics layer handle the zero-trades edge case without raising an unhandled exception?

---

### Chapter 7 — Reporting Layer

**Files**:
```
src/strategies/contracts/report_contracts.py
src/strategies/specific/modules/report_generator.py
tests/migration/test_report_generator_session17.py
```

**What to look for** (this is recently written — lighter scan):
- [ ] `ReportConfig.__post_init__` — validates both `theme` and `chart_height_px`?
- [ ] `GeneratedReport` — `html_path` is absolute (not relative)?
- [ ] `_save_html()` — creates `output_dir` recursively (`mkdir(parents=True)`)?
- [ ] Hour table fix (v1.1) — confirms `if sm.trades > 0` filter is in place?
- [ ] CDN failure handler — `onerror` attribute on `<script>` tag present?
- [ ] Light theme — are all `LIGHT_THEME` keys used in CSS (no orphan keys)?
- [ ] Test coverage — do tests cover the `theme="light"` path?
- [ ] Test coverage — do tests cover `include_raw_data=False`?
- [ ] MagicMock usage — is it isolated to `MetricsReport` only, or leaking into other contracts?

**Key question**: Can the report generator be called in a tight loop (e.g. batch reporting for 100 backtest runs) without memory leaks from Chart.js canvas accumulation?

---

## 📝 PHASE8_SCAN_REPORT.md — OUTPUT FORMAT

Create this file at `docs/migration/PHASE8_SCAN_REPORT.md`. Structure:

```markdown
# PHASE 8 CODE SCAN REPORT
Date: YYYY-MM-DD | Session: 19 | Author: Claude

## Executive Summary
N files scanned, M issues found: X critical, Y warnings, Z info

## Chapter 1 — Strategy Bootstrap & Configuration
### ✅ Confirmed Good
- ...
### ⚠️ Issues Found
- [CRITICAL] description — file:line — proposed fix
- [WARNING]  description — file:line — proposed fix
- [INFO]     description — worth noting

## Chapter 2 — Data Layer
...

## Prioritised Action Plan
### P0 — Fix Immediately (blocks correctness)
1. ...

### P1 — Fix in Session 20 (quality / edge cases)
1. ...

### P2 — Fix in Session 21 (polish / observability)
1. ...

### P3 — Deferred to POST_MIGRATION_ROADMAP
1. ...

## POST_MIGRATION_ROADMAP updates
List any new items to add to the roadmap based on scan findings.
```

---

## 🗺️ SESSIONS 20-22 PLAN (derived from scan findings)

The exact content of Sessions 20-22 depends on scan findings, but the expected shape:

### Session 20 — P0 + P1 Fixes
- Fix all correctness issues (P0) — likely: division-by-zero guards, edge cases
- Fix quality issues (P1) — likely: validation gaps, error handling
- Target: all P0 + P1 items resolved, test coverage expanded

### Session 21 — Observability & Logging
- Add structured logging to all 7 modules (module-level `logger`)
- `execution_time_ms` recording at each pipeline stage
- Memory usage tracking for large datasets (LTF 2M+ bars)
- Audit trail: log each stage input counts and output counts
- Write `tests/migration/test_observability.py`

### Session 22 — Quality Enhancements & MagicMock Cleanup
- Contract validation enhancements (found in scan)
- Timezone handling verification (enforce UTC end-to-end)
- MagicMock → real dataclass replacement in 4 test files:
  - `test_analytics_contracts.py`
  - `test_trade_analytics_session15.py`
  - `test_trade_analytics_session16.py`
  - `test_report_generator_session17.py`
- Spread manager edge case validation
- Final integration test: full pipeline CSV → HTML report end-to-end

---

## 📊 PROJECT STATUS ON ENTRY TO SESSION 19

### Production (locked — do not modify without tests)
| Module | File | Phase | Tests |
|--------|------|-------|-------|
| DataLoader | `data_loader.py` | 1-4 | — |
| SignalGenerator | `signal_generator.py` | 5-7 | — |
| FilterPipeline | `filter_pipeline.py` | 8-10 | — |
| TradeSimulator | `trade_simulator.py` | 11-13 | — |
| RiskManager | `risk_manager.py` | 11-13 | — |
| TradeManager | `trade_manager.py` | 11-13 | — |
| MetricsCalculator | `metrics_calculator.py` | 13 | — |
| TradeAnalytics | `trade_analytics.py` | 14-16 | 141 ✅ |
| ReportGenerator | `report_generator.py` | 17-18 | 131 ✅ |

**Total Phase 5-7 tests**: 374  
**Known test gap**: Phases 1-4 modules have no dedicated unit tests in `tests/migration/` — scan to confirm and document.

### Open carry-forward items
1. **MagicMock cleanup** — 4 test files (DEC-010) — target Session 22
2. **Excel output** — deferred post-migration (DEC-009, POST_MIGRATION_ROADMAP §1.1)
3. **ComparativeContext v2** — `vs_baseline` and `percentile_rank` are None (POST_MIGRATION_ROADMAP §3.1)
4. **Hour heatmap, drawdown chart, theme toggle** — (POST_MIGRATION_ROADMAP §2)

---

## 🚀 SESSION 19 EXECUTION ORDER (step by step)

```
1.  Read this file fully                                          [ 5 min ]
2.  Confirm project state: run all existing tests → should be 374 pass
3.  Chapter 1 scan: bootstrap + config                            [ 8 min ]
4.  Chapter 2 scan: data layer                                    [ 8 min ]
5.  Chapter 3 scan: signal generation                             [ 8 min ]
6.  Chapter 4 scan: filter pipeline                               [ 12 min ]
7.  Chapter 5 scan: trade simulation                              [ 12 min ]
8.  Chapter 6 scan: analytics layer                               [ 8 min ]
9.  Chapter 7 scan: reporting layer                               [ 6 min ]
10. Write PHASE8_SCAN_REPORT.md with all findings                 [ 10 min ]
11. Update POST_MIGRATION_ROADMAP.md with new items               [ 5 min ]
12. Write SESSION_20_HANDOFF.md using P0/P1 findings              [ 8 min ]
```

**Do not start fixing in Session 19.** The value of this session is a complete, honest picture of the codebase. Fixing during scan leads to incomplete coverage and missed inter-module issues.

---

## 📊 TEST COUNT TRACKER

| Session | Module | Tests | Cumulative |
|---------|--------|-------|------------|
| 14-16 | TradeAnalytics | 141 | 141 |
| 17-18 | ReportGenerator | 131 | 272 |
| 19 | Code scan — no new tests | 0 | 272 |
| 20 | P0/P1 fix tests | ~30 target | ~302 |
| 21 | Observability tests | ~20 target | ~322 |
| 22 | Integration + cleanup | ~25 target | ~347 |

---

## 🔖 DECISION LOG ENTRIES TO CONSIDER ADDING IN SESSION 19

After scan, add entries to `DECISION_LOG.md` for any significant choices made:
- DEC-011: Logging strategy chosen (structured vs unstructured)
- DEC-012: How to handle Numba optional dependency (if applicable)
- DEC-013: UTC enforcement mechanism chosen
- DEC-014: Whether to backfill unit tests for Phases 1-4 modules

---

**ReportGenerator v1.1**: ✅ COMPLETE  
**Phase 8.1 (Code Scan)**: ⏳ Starts Session 19  
**Phase 8.2 (Quality)**: ⏳ Sessions 20-22  
**Post-migration**: Excel output, PDF, theme toggle — `POST_MIGRATION_ROADMAP.md`
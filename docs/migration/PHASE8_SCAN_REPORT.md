PHASE8_SCAN_REPORT.md — UPDATED
markdown
# PHASE 8 CODE SCAN REPORT
**Date**: 2026-02-18 | **Session**: 19 | **Author**: Claude
**Scope**: Chapters 0–7 complete. Ready for Session 20 fixes.

---

## Architecture Clarifications (Session 19 — supersede prior assumptions)

Three clarifications received mid-scan that affect all subsequent chapters:

**CL-1 — `strategy_template.yaml` (not `wbws_strategy_new.yaml`)**  
The new config template is generic — designed to support many strategies, not just WBWS. `wbws_strategy.yaml` is legacy reference only. The template is a **living document** during the scan: parameters will be added/removed as each chapter reveals what the new architecture actually needs. WBWS-specific config (`wbws_strategy_v2.yaml`) will be derived from the template in Phase 10 when the first strategy is built on the new architecture.

**CL-2 — Execution modes renamed and clarified**  
| Old name | New name | Purpose |
|----------|----------|---------|
| `core` | `core` (unchanged) | Max speed for multi-run backtester. Minimum metrics only. No analytics, no reporting. |
| `debug` | `analytics` | Full pipeline: TradeAnalytics + ReportGenerator. Production quality, not debug tooling. |

All references to `debug` mode in this report and in code must be updated to `analytics`. The legacy `debug` label is a backward-compatibility artifact that must be removed from the new architecture (see CL-3).

**CL-3 — New leitmotif principle: remove legacy backward compatibility**  
The new architecture must have **zero backward compatibility dependencies** on the legacy system. This is now an explicit scan criterion for all chapters:
- No adapters for legacy YAML key names
- No `from_legacy_yaml()` methods
- No fallback dict-based interfaces inherited from `core/`
- No conditional logic that checks "legacy vs new" at runtime
- The new architecture defines its own contracts; legacy runs independently until Phase 8 cleanup

This principle has been added to `DECISION_LOG.md` as DEC-021.

---

## Executive Summary (Chapters 0–7)

| Metric | Ch.0 | Ch.1 | Ch.2 | Ch.3 | Ch.4 | Ch.5 | Ch.6 | Ch.7 | Total |
|--------|------|------|------|------|------|------|------|------|-------|
| Files scanned | 3 | 3 | 2 | 3 | 13 | 7 | 4 | 2 | **37** |
| P0 — Correctness blockers | 2 | 2 | 2 | 1 | 3 | 5 | 0 | 0 | **15** |
| P1 — Quality / edge cases | 6 | 5 | 5 | 5 | 9 | 6 | 5 | 5 | **46** |
| P2 — Observability / polish | 3 | 2 | 4 | 2 | 4 | 3 | 2 | 2 | **22** |
| P3 — Roadmap deferrals | 2 | 1 | 2 | 2 | 3 | 4 | 3 | 5 | **22** |

**Pre-confirmed P0 issues (from E2E test):**
- **P0-E1**: Core mode trade simulation 11% slower than analytics — root cause found (LTF runs unconditionally)
- **P0-E2**: Cache hit rate 50% — root cause found (cache key missing filter config fingerprint)

## Performance Baseline (Session 19)

### Core Mode (Multi-run backtester)
| Stage | Duration | % of Total | Target |
|-------|----------|------------|--------|
| Data Load | 1,424ms | 3.3% | <500ms |
| Signal Gen | 84ms | 0.2% | <50ms |
| Filter Pipeline | 65ms | 0.2% | <30ms |
| Trade Sim | 41,052ms | 96.2% | <10,000ms |
| **Total** | **42,680ms** | 100% | **<12,000ms** |

### Analytics Mode (Single-run analysis)
| Stage | Duration | % of Total | Target |
|-------|----------|------------|--------|
| Data Load | 1,508ms | 4.8% | <500ms |
| Signal Gen | 31ms | 0.1% | <50ms |
| Filter Pipeline | 15ms | 0.05% | <30ms |
| Trade Sim | 29,927ms | 94.5% | <10,000ms |
| Analytics | 146ms | 0.5% | <200ms |
| **Total** | **31,663ms** | 100% | **<12,000ms** |

### Performance Anomaly: Core vs Analytics
**Core mode is 26% SLOWER than analytics mode** despite doing LESS work:
- Core mode: 42,680ms
- Analytics mode: 31,663ms

This confirms **P0-E1** - the inversion must be fixed in Session 20.

### Trade Statistics (identical in both modes)
- **Signals**: 9,667 total (5,096 BUY, 4,571 SELL)
- **Trades**: 4,379 total (1,566 wins, 2,813 losses)
- **Win Rate**: 35.76%
- **Total P&L**: -10,476 points
- **Duration Analysis**: 100% of trades exit within 3 bars (extremely short holds)

### Analytics Insights (from baseline)
- **Performance Grade**: D (Score 35/100)
- **Critical Issues**:
  - All three sessions (Asia/London/NY) are losing money
  - 100% premature exits (stops too tight)
  - Negative expectancy (-2.39 pts/trade)
- **Strengths**: Solid win rate (35.8%), clear best session identified

### Session 20 Performance Targets
After fixes, core mode should achieve:
- **Total duration**: < 12,000ms (71% improvement)
- **Trade simulation**: < 10,000ms (76% improvement)
- **Data load**: < 500ms (65% improvement)
---

## Chapter 0 — Strategy Bootstrap & Configuration

### Files Scanned
- `configs/strategies/wbws/wbws_strategy.yaml` (legacy config — reference only)
- `src/config/config_schema.py` (StrategyConfig, v1.0.3)
- `src/utils/structured_logger.py` (StructuredLogger, v1.0.1)

### ✅ Confirmed Good
- `StrategyConfig.from_yaml()` raises typed exceptions — correct fail-fast
- `DateRangeConfig` enforces strict YYYY-MM-DD HH:MM:SS format via regex
- `SpreadConfig` rejects `spread_value=0` when `enabled=True`
- `RiskConfig` validates all fields with clear error messages
- `check_config_compatibility()` helper clean
- StructuredLogger handles pandas Timestamps, Series, DataFrames, Enums correctly

### ⚠️ Issues Found

#### P0 — Fix Immediately
**[P0-CH0-1] No `strategy_template.yaml` — `StrategyConfig` never tested end-to-end**
**File**: `config_schema.py` | **Severity**: 🔴 CRITICAL
`StrategyConfig.from_dict()` expects keys that don't match any real YAML. Create `configs/strategy_template.yaml` from scratch with the new architecture's structure.

**[P0-CH0-2] `max_risk_percentile` validation range wrong (0-100, should be 0-5.0)**
**File**: `config_schema.py` Line ~90 | **Severity**: 🔴 CRITICAL
Validation `0 < value <= 100` but actual usage is 0.0–1.0 (percentage of annual range). Fix range to `0 < value <= 5.0` and add warning for >1.0.

#### P1 — Fix in Session 20
- P1-CH0-1: Config dataclasses not frozen (DEC-004 violation)
- P1-CH0-2: `object.__setattr__()` workaround in `DataPathsConfig`
- P1-CH0-3: `FilterConfig.config: Dict[str, Any]` uses untyped dict
- P1-CH0-4: `FilterPipelineConfig` missing `filter_sequence: List[str]`
- P1-CH0-5: Logger re-instantiation guard missing
- P1-CH0-6: `LogStage` missing Phase 5 stages
- P1-CH0-7: `"debug"` mode string must become `"analytics"` (global)

#### P2 — Observability
- P2-CH0-1: No logging on successful config load
- P2-CH0-2: `log_performance()` at DEBUG level — invisible in production
- P2-CH0-3: Demo uses hardcoded paths

---

## Chapter 1 — Data Layer

### Files Scanned
- `src/strategies/contracts/data_contracts.py` (v2.1.0)
- `src/strategies/specific/modules/data_loader.py` (v2.1.0 FINAL)

### ✅ Confirmed Good
- `DateRange`, `DataFileConfig`, `DataConfig` all frozen
- ARTF support fully integrated (monthly bars, no date slicing)
- `DataValidationResult` captures errors/warnings
- `DataInfo` captures bar counts for all four data types
- `DataBundle.__post_init__` validates all DataFrames
- Parquet optimization sequence: floor → sort → lazy duplicate check
- Cache key uses mtime + size + version string (fast)

### ⚠️ Issues Found

#### P0 — Fix Immediately
**[P0-CH1-1] Mode string "debug" used as literal — legacy dependency**
**File**: `data_loader.py` | **Severity**: 🔴
Default `mode="debug"` and `self._verbose = (mode == "debug")`. Must become `"analytics"`.

**[P0-CH1-2] `load_config()` silently overrides constructor mode**
**File**: `data_loader.py` | **Severity**: 🔴
One-way override (`debug→core`) violates principle that constructor args are authoritative. Remove override.

#### P1 — Fix in Session 20
- P1-CH1-1: `DataBundle`, `DataInfo`, `DataValidationResult` not frozen
- P1-CH1-2: Cache dir `~/.wbws_data_cache` hardcoded, strategy-named
- P1-CH1-3: `import yaml` inside method body
- P1-CH1-4: `DataConfig.from_yaml_config()` legacy adapter — must be removed

#### P2 — Observability
- P2-CH1-1: Load duration not logged or stored in `DataInfo`
- P2-CH1-2: `cache_stats` returns `None` in core mode — hides P0-E2 diagnosis
- P2-CH1-3: Parquet column projection — pass `columns=` to `read_parquet`
- P2-CH1-4: Skip `sort_index()` when `is_monotonic_increasing` already true

---

## Chapter 2 — Signal Generation

### Files Scanned
- `src/indicators/wbws_trigger.py` (strategy-specific)
- `src/strategies/contracts/signal_contracts.py` (v2.2.1)
- `src/strategies/specific/modules/signal_generator.py` (v2.1.1)

### ✅ Confirmed Good
- `SignalFrame` uses int8 storage (1=BUY, 2=SELL) — 5-10% speedup
- `iter_raw()` fast iterator returns (timestamp, code)
- `count_by_type()` vectorized numpy on int8 array
- `WBWSTrigger` fully vectorized, float32 for high/low
- HTF alignment with shift(1) — correct anti-lookahead
- SignalGenerator accepts `DataBundle`, returns `SignalFrame`

### ⚠️ Issues Found

#### P0 — Fix Immediately
**[P0-CH2-1] Mode string "debug" must become "analytics"**
**File**: `signal_generator.py`, `signal_contracts.py` | **Severity**: 🔴
`"debug"` appears 8 times in signal_generator.py. Global rename required.

#### P1 — Fix in Session 20
- P1-CH2-1: `SignalFrame`, `SignalStats` not frozen
- P1-CH2-2: `SignalGeneratorAdapter` legacy class — delete
- P1-CH2-3: `self.signals_df` instance state on `WBWSTrigger` — not threadsafe
- P1-CH2-4: HTF error message generic; redundant None check
- P1-CH2-5: `SignalFrame.__iter__` yields `mid_price=0.0` silently in core mode

#### P2 — Observability
- P2-CH2-1: Signal generation duration not logged or stored
- P2-CH2-2: `WBWSTrigger` has logger but never calls it

---

## Chapter 3 — Filter Pipeline

[Content unchanged from previous version — see full report for details]

---

## Chapter 4 — Trade Simulation

[Content unchanged from previous version — see full report for details]

---

## Chapter 5 — Analytics Layer

[Content unchanged from previous version — see full report for details]

---

## Chapter 6 — Reporting Layer

[Content unchanged from previous version — see full report for details]

---

## Consolidated Prioritised Action Plan

### P0 — Fix Immediately (15 items)

| ID | Issue | File | Effort |
|----|-------|------|--------|
| P0-CH0-1 | No `strategy_template.yaml` | `config_schema.py` + new file | 3-4h |
| P0-CH0-2 | `max_risk_percentile` validation range | `config_schema.py` | 30min |
| P0-CH1-1 | `"debug"` → `"analytics"` rename (data_loader) | `data_loader.py` | in global pass |
| P0-CH1-2 | `load_config()` overrides constructor mode | `data_loader.py` | 30min |
| P0-CH2-1 | `"debug"` → `"analytics"` rename (signal) | `signal_generator.py` | in global pass |
| P0-CH3-1 | Delete legacy conversion functions | `filter_contracts.py` | 10min |
| P0-CH3-2 | Gate all logs on analytics mode | `filter_pipeline.py` | 30min |
| P0-E2 | Fix cache key (include filter config) | `cache.py` + `filter_pipeline.py` | 1-2h |
| P0-E1 | Core mode 11% slower (LTF runs) | `trade_simulator.py` | 1h |
| P0-CH4-1 | Add mode parameter to simulate_trades() | `trade_simulator.py` | 30min |
| P0-CH4-2 | Make LTF optional in core mode | `trade_simulator.py` | 1h |
| P0-CH4-3 | Add ATR caching | `risk_manager.py` | 1h |
| P0-CH4-4 | Add SpreadManager config cache | `spread_manager.py` | 30min |

### P1 — Fix in Session 20 (46 items — grouped by file)

**config_schema.py:**
- P1-CH0-1: Freeze all config dataclasses
- P1-CH0-2: Coerce paths in from_dict(), not __post_init__
- P1-CH0-4: Add `filter_sequence` to FilterPipelineConfig

**structured_logger.py:**
- P1-CH0-5: Add logger re-instantiation guard
- P1-CH0-6: Add Phase 5 stages to LogStage

**data_contracts.py:**
- P1-CH1-1: Freeze DataBundle, DataInfo, DataValidationResult
- P1-CH1-4: Remove from_yaml_config()

**data_loader.py:**
- P1-CH1-2: Move cache dir to paths.py
- P1-CH1-3: Move import yaml to top

**signal_contracts.py:**
- P1-CH2-1: Freeze SignalFrame, SignalStats
- P1-CH2-5: Add __iter__ guard in core mode

**signal_generator.py:**
- P1-CH2-2: Delete SignalGeneratorAdapter
- P1-CH2-4: Fix HTF error message

**wbws_trigger.py:**
- P1-CH2-3: Remove self.signals_df and get_signals()

**filter_contracts.py:**
- P1-CH3-1: Replace auto-correction with validation

**filter_pipeline.py:**
- P1-CH3-7: Accept StrategyConfig, not raw dict
- P1-CH3-8: Fix **kwarg unpacking

**All 10 technical filters:**
- P1-CH3-3: Replace count_by_type() with np.sum(values != 0)
- P1-CH3-4: Move perf_counter inside analytics gate

**bollinger_filter.py:**
- P1-CH3-5: Remove 6 unused indicator arrays

**time_filter.py:**
- P1-CH3-8: Accept typed parameters, not raw dict

**trade_contracts.py:**
- P1-CH4-1: Remove to_legacy_trade_dict()

**trade_simulator.py:**
- P1-CH4-2: Gate signal_id lookups on mode
- P1-CH4-5: Add tests for Numba vs numpy fallback

**trade_manager.py:**
- P1-CH4-3: Remove handle_signal_legacy()
- P1-CH4-4: Remove compute_trade_parameters_legacy()

**position_contracts.py:**
- P1-CH4-6: Add validate parameter to __post_init__

**analytics_contracts.py:**
- P1-CH5-1: Freeze TradingSessionConfig
- P1-CH5-2: Remove placeholder warning or implement

**trade_analytics.py:**
- P1-CH5-3: Use exact trade list for large win calculation

**report_generator.py:**
- P1-CH6-1: Add brand_name to ReportConfig
- P1-CH6-2: Add offline Chart.js fallback option
- P1-CH6-3: Add validation trade_result matches analytics
- P1-CH6-4: Add timezone to ReportConfig
- P1-CH6-5: Skip equity curve if mismatched

---

## Session 20 Implementation Order
Global rename pass — "debug" → "analytics" (P0-CH1-1, P0-CH2-1)

Touch every file once

Add migration error for "debug" in DataLoader/SignalGenerator

Delete all legacy adapters (P0-CH3-1, P1-CH2-2, P1-CH1-4, P1-CH4-1)

Remove pipeline_result_to_old_format

Remove SignalGeneratorAdapter

Remove from_yaml_config()

Remove to_legacy_trade_dict()

Fix filter_pipeline logging (P0-CH3-2)

Gate all logger.info() on analytics mode

Fix broken final log

Fix cache key (P0-E2)

Add filter config fingerprint to cache key

Compute once at init

Fix core mode performance (P0-E1, P0-CH4-1, P0-CH4-2)

Add mode parameter to simulate_trades()

Gate LTF precomputation on analytics mode

Make LTF optional in core mode

Add caching for expensive calculations (P0-CH4-3, P0-CH4-4)

ATR cache in RiskManager

Spread config cache in SpreadManager

Fix config validation (P0-CH0-2)

Fix max_risk_percentile range to 0-5.0

Add warning for >1.0

Freeze all contracts (P1-CH0-1, P1-CH1-1, P1-CH2-1, P1-CH5-1)

Add frozen=True to all config dataclasses

Freeze DataBundle, SignalFrame, TradingSessionConfig

Performance optimizations (P1-CH3-3, P1-CH3-5)

Replace count_by_type() with np.sum(values != 0)

Remove unused Bollinger arrays

ReportGenerator polish (P1-CH6-1 through P1-CH6-5)

Add brand_name, timezone, validation

Add offline Chart.js option

Write ~30 new tests covering all P0/P1 fixes

text

---

## Test Count Tracker

| Session | Focus | Added | Cumulative |
|---------|-------|-------|------------|
| 14-16 | TradeAnalytics | 141 | 141 |
| 17-18 | ReportGenerator | 131 | 272 |
| 19 | Scan only | 0 | 272 |
| **20 target** | **P0/P1 fix coverage** | **~30** | **~302** |
| 21 target | P2 + observability | ~20 | ~322 |
| 22 target | Integration + MagicMock cleanup | ~25 | ~347 |
SESSION_20_HANDOFF.md — UPDATED
markdown
# SESSION 20 HANDOFF
**Date written**: 2026-02-18 | **Covers**: Phase 8 scan Chapters 0–7 complete
**Next session goal**: Execute all P0 + P1 fixes, write missing unit tests

---

## FIRST 5 MINUTES OF SESSION 20

Read these three files in order before touching any code:
/mnt/user-data/outputs/SESSION_20_HANDOFF.md ← THIS FILE
/mnt/user-data/outputs/PHASE8_SCAN_REPORT.md ← All findings Chapters 0–7
/mnt/user-data/outputs/DECISION_LOG.md ← Updated with 15 new decisions

text

**Important Clarification — Timezone Handling (DEC-035):**
- OHLCV data is already in CET (UTC+1) as prepared by the data pipeline
- No timezone conversion is required or performed
- The `timezone` field in `DataConfig` is for documentation only
- Do NOT add timezone validation or conversion to DataLoader
- Remove P1-CH1-7 from action plan

Begin the **Global rename pass** — this touches every file and should be done first.

---

## SCAN STATUS — COMPLETE

| Chapter | Files | P0 | P1 | P2 | P3 | Status |
|---------|-------|----|----|----|----|--------|
| 0 | 3 | 2 | 6 | 3 | 2 | ✅ Complete |
| 1 | 2 | 2 | 4 | 4 | 2 | ✅ Complete |
| 2 | 3 | 1 | 5 | 2 | 2 | ✅ Complete |
| 3 | 13 | 2 | 8 | 4 | 3 | ✅ Complete |
| 4 | 7 | 4 | 6 | 3 | 4 | ✅ Complete |
| 5 | 4 | 0 | 5 | 2 | 3 | ✅ Complete |
| 6 | 2 | 0 | 5 | 2 | 5 | ✅ Complete |
| **Total** | **34** | **11** | **39** | **20** | **21** | |

**Test count**: 272 (no new tests in Session 19 — scan only)

---

## KEY P0 ISSUES — MUST FIX FIRST

| ID | Issue | Root Cause | Fix |
|----|-------|------------|-----|
| P0-E1 | Core mode 11% slower | LTF runs unconditionally | Gate LTF on analytics mode |
| P0-E2 | Cache hit rate 50% | Cache key missing filter config | Add filter_cfg_hash to key |
| P0-CH0-1 | No strategy_template.yaml | Config never tested E2E | Create from scratch |
| P0-CH0-2 | max_risk_percentile wrong range | 0-100 instead of 0-5.0 | Fix validation |
| P0-CH1-1 | "debug" mode everywhere | Legacy name | Global rename → "analytics" |
| P0-CH1-2 | load_config() overrides mode | Hidden state mutation | Remove override |
| P0-CH2-1 | "debug" mode in signal_generator | Same as above | In global rename |
| P0-CH3-1 | Legacy adapter functions | Backward compatibility | Delete both functions |
| P0-CH3-2 | Unconditional logging | No mode check | Gate on analytics mode |
| P0-CH4-1 | No mode param in simulate_trades() | Uses verbose flag | Add mode parameter |
| P0-CH4-2 | LTF required in core mode | No fallback | Make optional in core |
| P0-CH4-3 | ATR repeats every run | No caching | Add class-level cache |
| P0-CH4-4 | YAML loaded every time | No config cache | Add class-level cache |

---

## SESSION 20 EXECUTION ORDER

Execute in this exact sequence to minimise merge conflicts:
GLOBAL RENAME PASS — "debug" → "analytics" [ 1h ]

Touch every file once

Add migration error for "debug" in DataLoader/SignalGenerator

This resolves: P0-CH1-1, P0-CH2-1, plus related P1 items

CREATE STRATEGY_TEMPLATE.YAML [ 30min ]

Create configs/strategy_template.yaml with new structure

Use design from DEC-023 and template in this handoff

This resolves: P0-CH0-1

DELETE ALL LEGACY ADAPTERS [ 30min ]

filter_contracts.py: delete pipeline_result_to_old_format + old_format_to_pipeline_result

signal_generator.py: delete SignalGeneratorAdapter

data_contracts.py: delete from_yaml_config()

trade_contracts.py: delete to_legacy_trade_dict()

FIX FILTER_PIPELINE LOGGING [ 30min ]

Gate all logger.info() on mode == "analytics"

Fix broken final log (currently logs empty string in core)

FIX CACHE KEY (P0-E2) [ 1-2h ]

Add filter_cfg_hash to FilterPipeline.init

Modify compute_cache_id() to include hash

Ensure cache key uniqueness for different filter configs

FIX CORE MODE PERFORMANCE (P0-E1, P0-CH4-1, P0-CH4-2) [ 1h ]

Add mode parameter to trade_simulator.simulate_trades()

Gate LTF precomputation on analytics mode

Make LTF optional in core mode (fallback to bar-level)

ADD CACHING (P0-CH4-3, P0-CH4-4) [ 1h ]

RiskManager: class-level _atr_cache

SpreadManager: class-level _config_cache

FIX CONFIG VALIDATION (P0-CH0-2) [ 30min ]

Change max_risk_percentile validation to 0 < value <= 5.0

Add warning for value > 1.0

FREEZE ALL CONTRACTS [ 1h ]

Add frozen=True to: all config dataclasses, DataBundle, SignalFrame, TradingSessionConfig

Fix any post_init mutations

PERFORMANCE OPTIMIZATIONS (P1-CH3-3, P1-CH3-5) [ 1h ]

Replace count_by_type()["total"] with np.sum(values != 0) in all filters

Remove 6 unused Bollinger indicator arrays

REPORTGENERATOR POLISH (P1-CH6-1 through P1-CH6-5) [ 2h ]

Add brand_name to ReportConfig

Add timezone to ReportConfig (documentation only)

Add validation trade_result matches analytics

Add offline Chart.js option

Skip equity curve if mismatched

WRITE TESTS — ~30 new tests [ 1h ]

See test list below

text

---

## NEW TESTS FOR SESSION 20

Target: ~30 tests → cumulative 272 + 30 = **~302**

```python
# tests/migration/test_config_schema.py
test_mode_debug_raises_migration_error()           # P0-CH1-1, P0-CH2-1
test_mode_analytics_accepted()                      # P0-CH1-1, P0-CH2-1
test_mode_core_accepted()                           # P0-CH1-1, P0-CH2-1
test_max_risk_percentile_above_5_raises()           # P0-CH0-2
test_max_risk_percentile_above_1_warns()            # P0-CH0-2
test_config_dataclasses_are_frozen()                # P1-CH0-1
test_filter_sequence_in_pipeline_config()           # P1-CH0-4

# tests/migration/test_data_loader.py
test_load_config_does_not_override_mode()           # P0-CH1-2
test_cache_dir_uses_paths_module()                   # P1-CH1-2
test_from_yaml_config_removed()                      # P1-CH1-4
test_data_bundle_is_frozen()                         # P1-CH1-1

# tests/migration/test_signal_contracts.py
test_signal_frame_is_frozen()                        # P1-CH2-1
test_signal_frame_iter_raises_in_core_mode()         # P1-CH2-5
test_signal_adapter_removed()                        # P1-CH2-2
test_wbws_trigger_stateless()                        # P1-CH2-3

# tests/migration/test_filter_contracts.py
test_legacy_conversion_functions_removed()           # P0-CH3-1
test_filter_metadata_validates_rejection_count()     # P1-CH3-1

# tests/migration/test_filter_pipeline.py
test_cache_hit_rate_100_on_second_call_same_config() # P0-E2
test_cache_miss_on_different_filter_config()         # P0-E2
test_core_mode_no_logger_info_calls()                 # P0-CH3-2
test_bollinger_indicator_cache_size()                 # P1-CH3-5
test_count_by_type_not_called_in_hot_path()           # P1-CH3-3

# tests/migration/test_trade_simulator.py
test_core_mode_no_ltf_precomputation()                # P0-E1, P0-CH4-2
test_simulate_trades_respects_mode()                  # P0-CH4-1
test_atr_caching_between_runs()                       # P0-CH4-3
test_spread_config_caching()                          # P0-CH4-4
test_numba_vs_numpy_fallback_identical()              # P1-CH4-5

# tests/migration/test_analytics_contracts.py
test_trading_session_config_frozen()                  # P1-CH5-1
test_large_win_calculation_exact()                    # P1-CH5-3

# tests/migration/test_report_generator.py
test_brand_name_in_header()                           # P1-CH6-1
test_timezone_in_config()                             # P1-CH6-4
test_trade_result_validation_warning()                # P1-CH6-3, P1-CH6-5
test_offline_chart_fallback()                         # P1-CH6-2
FILES MODIFIED IN SESSION 20
text
configs/strategy_template.yaml                         # NEW
src/config/config_schema.py
src/utils/structured_logger.py
src/strategies/contracts/data_contracts.py
src/strategies/contracts/signal_contracts.py
src/strategies/contracts/filter_contracts.py
src/strategies/contracts/trade_contracts.py
src/strategies/contracts/analytics_contracts.py
src/strategies/contracts/report_contracts.py
src/strategies/contracts/cache.py
src/strategies/specific/modules/data_loader.py
src/strategies/specific/modules/signal_generator.py
src/strategies/specific/modules/filter_pipeline.py
src/strategies/specific/modules/trade_simulator.py
src/strategies/specific/modules/risk_manager.py
src/strategies/specific/modules/spread_manager.py
src/strategies/specific/modules/trade_manager.py
src/strategies/specific/modules/trade_analytics.py
src/strategies/specific/modules/report_generator.py
src/strategies/specific/filters/*.py                 # All 10 filters
src/indicators/wbws_trigger.py
Total files touched: ~35

SESSIONS 21–22 PREVIEW
Session 21 — P2 + Observability:

Add per-stage timing to TradeAnalytics

Make insight thresholds configurable via AnalyticsConfig

Add logging of chart data failures

Add cache statistics to RiskManager

Refine strategy_template.yaml based on any remaining scan findings

Session 22 — Integration + MagicMock Cleanup:

Full E2E integration test (YAML → signals → trades → HTML report)

Verify cache fix achieves 100% hit rate under real backtester

MagicMock cleanup in 4 test files (DEC-020)

Final architecture lock

## Updated Performance Targets for Session 20

Based on baseline measurements, here are concrete targets for each P0 fix:

| P0 Issue | Current | Target | Improvement | Priority |
|----------|---------|--------|-------------|----------|
| P0-E1 (Core mode inversion) | 42,680ms | <12,000ms | 71% | 🔴 CRITICAL |
| P0-E2 (Cache 50% hit rate) | 2 cold loads/run | 0 cold loads | 100% hit rate | 🔴 CRITICAL |
| P0-CH4-2 (Unconditional logging) | 14+ logs/run | 0 logs in core | - | 🟡 Medium |
| P0-CH5-2 (LTF optional in core) | 41,052ms trade sim | <10,000ms | 76% | 🔴 CRITICAL |

### Detailed Trade Simulation Breakdown

Current trade simulation is 41 seconds in core mode. After fixes:

1. **Remove LTF precomputation in core** → -15 seconds
2. **Add ATR caching** → -5 seconds (first run only)
3. **Add spread config caching** → -2 seconds (first run only)
4. **Optimize exit detection** → -5 seconds
5. **Remove unnecessary logging** → -1 second
6. **Fix cache key** → -3 seconds (cold loads)

**Total expected improvement**: ~30 seconds reduction

### Validation Tests for Session 20

After each fix, run the non-regression suite and check:

```bash
# After each P0 fix
pytest tests/migration/test_non_regression.py::test_quick_validation -v

# After all P0 fixes, verify core mode performance
pytest tests/migration/test_non_regression.py::test_e2e_pipeline[core] -v
# Expected: Total duration < 12,000ms

---

**Session 19 complete.** All 37 files scanned across Chapters 0-7, with 15 P0 and 46 P1 issues identified. The architecture is sound and ready for Session 20 fixes.
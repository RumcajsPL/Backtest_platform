# PHASE8_SCAN_REPORT — SESSION 20 STATUS TRACKER
# This file tracks resolution of P0/P1 items during Session 20 implementation.
# Update as each block completes. Merge back into PHASE8_SCAN_REPORT.md at end of session.

---

## P0 Resolution Status

| ID | Issue | Block | Status |
|----|-------|-------|--------|
| P0-CH0-1 | No strategy_template.yaml | C | ⬜ Pending |
| P0-CH0-2 | max_risk_percentile wrong range | G | ⬜ Pending |
| P0-CH1-1 | "debug" mode in data_loader | A | ⬜ Pending |
| P0-CH1-2 | load_config() overrides constructor mode | A | ⬜ Pending |
| P0-CH2-1 | "debug" mode in signal_generator | A | ⬜ Pending |
| P0-CH3-1 | Legacy conversion functions in filter_contracts | B | ⬜ Pending |
| P0-CH3-2 | Unconditional logging in filter_pipeline | D | ⬜ Pending |
| P0-E1 | Core mode 26% slower than analytics | E | ⬜ Pending |
| P0-E2 | Cache hit rate 50% | D | ⬜ Pending |
| P0-CH4-1 | No mode param in simulate_trades() | E | ⬜ Pending |
| P0-CH4-2 | LTF runs in core mode | E | ⬜ Pending |
| P0-CH4-3 | ATR repeats every run | F | ⬜ Pending |
| P0-CH4-4 | YAML loaded every time | F | ⬜ Pending |

## P1 Resolution Status (Session 20 scope)

| ID | Issue | Block | Status |
|----|-------|-------|--------|
| P1-CH0-1 | Config dataclasses not frozen | G | ⬜ Pending |
| P1-CH0-4 | FilterPipelineConfig missing filter_sequence | C | ⬜ Pending |
| P1-CH0-6 | LogStage missing Phase 5 stages | A | ⬜ Pending |
| P1-CH0-7 | "debug" mode string (global) | A | ⬜ Pending |
| P1-CH1-1 | DataBundle/DataInfo/DataValidationResult not frozen | G | ⬜ Pending |
| P1-CH1-2 | Cache dir hardcoded, strategy-named | A | ⬜ Pending |
| P1-CH1-4 | DataConfig.from_yaml_config() legacy adapter | B | ⬜ Pending |
| P1-CH2-1 | SignalFrame, SignalStats not frozen | G | ⬜ Pending |
| P1-CH2-2 | SignalGeneratorAdapter legacy class | B | ⬜ Pending |
| P1-CH2-3 | WBWSTrigger self.signals_df instance state | B | ⬜ Pending |
| P1-CH2-5 | SignalFrame.__iter__ guard for core mode | G | ⬜ Pending |
| P1-CH3-3 | count_by_type() in filter hot paths | H | ⬜ Pending |
| P1-CH3-5 | 6 unused Bollinger arrays | H | ⬜ Pending |
| P1-CH4-1 | RejectedSignal.to_legacy_trade_dict() | B | ⬜ Pending |
| P1-CH4-3 | TradeManager handle_signal_legacy() | B | ⬜ Pending |
| P1-CH4-4 | TradeManager compute_trade_parameters_legacy() | B | ⬜ Pending |
| P1-CH5-1 | TradingSessionConfig not frozen | G | ⬜ Pending |
| P1-CH6-1 | ReportGenerator brand_name hardcoded | I | ⬜ Pending |
| P1-CH6-2 | No offline Chart.js fallback option | I | ⬜ Pending |
| P1-CH6-3 | No validation: trade_result vs analytics mismatch | I | ⬜ Pending |
| P1-CH6-4 | No timezone in ReportConfig | I | ⬜ Pending |
| P1-CH6-5 | Skip equity curve if mismatched | I | ⬜ Pending |

## Performance Tracker

| Metric | Baseline | After E | After F | Final | Target |
|--------|----------|---------|---------|-------|--------|
| Core total | 42,680ms | ? | ? | ? | <12,000ms |
| Analytics total | 31,663ms | ? | ? | ? | <12,000ms |
| TradeSimulator core | 41,052ms | ? | ? | ? | <10,000ms |
| Filter pipeline | 65ms | — | — | ? | <30ms |
| Cache hit rate | 50% | ? | — | ? | 100% |
| Test count | 272 | — | — | ? | ~302 |

## Block Completion Log

| Block | Description | Status | Tests Added | Notes |
|-------|-------------|--------|-------------|-------|
| A | Global rename "debug"→"analytics" | ⬜ | 0 | |
| B | Delete legacy adapters | ⬜ | 0 | |
| C | Create strategy_template.yaml | ⬜ | 0 | |
| D | Filter pipeline: logging + cache key | ⬜ | 0 | |
| E | Core mode performance | ⬜ | 0 | |
| F | ATR + YAML caching | ⬜ | 0 | |
| G | Config validation + freeze contracts | ⬜ | 0 | |
| H | Filter optimizations | ⬜ | 0 | |
| I | ReportGenerator polish | ⬜ | 0 | |
| J | Write ~30 tests | ⬜ | 0 | |
| K | Update docs | ⬜ | 0 | |

## Deferred to Session 21 (P2 items)

- P2-CH0-1: No logging on successful config load
- P2-CH1-1: Load duration not logged in DataInfo
- P2-CH1-2: cache_stats returns None in core mode
- P2-CH2-1: Signal generation duration not logged
- P2-CH3-x: Per-filter timing collection
- All remaining P2 items across Chapters 4–6

## Permanently Deferred (Session 22)

- DEC-020: MagicMock cleanup in 4 test files
- P1-CH4-5: Numba vs numpy fallback tests
- Full E2E integration test
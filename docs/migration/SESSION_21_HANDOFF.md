# SESSION 21 — HANDOFF & OPENING BRIEF
**Created**: 2026-02-20 (end of Session 20)  
**Phase**: 9 — Integration & Orchestration  
**Session focus**: Wire the orchestrator skeleton to real data and achieve a passing E2E run

---

## What to Read Before Starting

Three documents only — in this order:

1. **This file** — complete context for Session 21
2. **`docs/architecture/ARCHITECTURE.md`** (v2.2.0) — module signatures, contract types, import paths
3. **`src/strategies/orchestrator.py`** — the skeleton built at end of Session 20; read the `# ASSUMPTION:` comments before touching anything else

No other documents are required to start.

---

## State of the System

Phase 8 is closed. Every pipeline module is production-hardened.

| Module | File | State |
|--------|------|-------|
| DataLoader | `src/strategies/specific/modules/data_loader.py` | ✅ Frozen contracts, mode-clean |
| SignalGenerator | `src/strategies/specific/modules/signal_generator.py` | ✅ Stateless, `"debug"` guard live |
| FilterPipeline | `src/strategies/specific/modules/filter_pipeline.py` | ✅ Config-fingerprint cache key |
| TradeSimulator | `src/strategies/specific/modules/trade_simulator.py` | ✅ Mode-gated LTF and tracking |
| RiskManager | `src/strategies/specific/modules/risk_manager.py` | ✅ `clear_cache()` available |
| SpreadManager | `src/strategies/specific/modules/spread_manager.py` | ✅ Class-level config cache |
| MetricsCalculator | `src/strategies/specific/modules/metrics_calculator.py` | ✅ `calculate_metrics(trade_result)` |
| TradeAnalytics | `src/strategies/specific/modules/trade_analytics.py` | ✅ Not wired in orchestrator v1 |
| ReportGenerator | `src/strategies/specific/modules/report_generator.py` | ✅ Not wired in orchestrator v1 |
| Orchestrator | `src/strategies/orchestrator.py` | ⏳ Skeleton built — assumptions unverified |
| Runner script | `scripts/runners/run_strategy.py` | ⏳ Skeleton built — needs real config path |
| `strategy_template.yaml` | `configs/strategies/strategy_template.yaml` | ✅ Exists |
| `wbws_strategy_v2.yaml` | `configs/strategies/wbws/wbws_strategy_v2.yaml` | ❌ Not yet created |
| Test count | — | ~302 |

**Architecture is locked**: any contract or interface change requires a DEC entry in `DECISION_LOG.md` before implementation.

---

## What Was Built at End of Session 20

### `src/strategies/orchestrator.py`
Full pipeline skeleton from config load through `MetricsCalculator`. Key design decisions already made:

- `StrategyOrchestrator(config)` — takes a `StrategyConfig`, reads mode from `config.execution.mode`
- `StrategyOrchestrator.from_yaml(path)` — convenience constructor for scripts
- `run(clear_cache=True)` — executes all five stages, returns `OrchestratorResult`
- `RiskManager.clear_cache()` called automatically at the start of every `run()` (overridable via `clear_cache=False`)
- `OrchestratorResult` — frozen dataclass holding all five stage outputs + per-stage timing dict
- Phase 9.2 extension point left as commented block at bottom of `run()` — TradeAnalytics and ReportGenerator slot in there when ready

### `scripts/runners/run_strategy.py`
Thin CLI entry point. Accepts `--config`, `--mode` (override), `--log-level`. Prints a formatted result summary to stdout. Returns exit code 0 on success, 1 on failure.

---

## Session 21 Primary Goal — First Green E2E Run

The orchestrator skeleton was written with five documented assumptions about module interfaces. **Verify each assumption before running anything.** This is the first and most important task of the session — a wrong assumption produces a confusing traceback, not a helpful error.

### The Five Assumptions to Verify

Open `src/strategies/orchestrator.py` and locate each `# ASSUMPTION:` comment. For each one, open the referenced module and confirm or correct.

---

**Assumption 1 — Config dot-path to execution mode**

Current code in `StrategyOrchestrator.__init__`:
```python
self._mode: str = config.execution.mode
```
Open `src/config/config_schema.py`. Check: does `StrategyConfig` have an `execution` sub-config with a `mode` field? Or is mode accessed as `config.mode` directly?

---

**Assumption 2 — DataLoader constructor and load method**

Current code in `_load_data()`:
```python
loader = DataLoader(self._config)
bundle = loader.load()
```
Open `src/strategies/specific/modules/data_loader.py`. Check:
- Does `DataLoader.__init__` accept `(config: StrategyConfig)`? Or does it also require `mode`?
- Is the load method `.load()`, `.load_data()`, or `.run()`?
- What field on `DataInfo` gives the strategy bar count? Currently logged as `bundle.info.strategy_bar_count` — correct the field name if different.

---

**Assumption 3 — SignalGenerator constructor and generate method**

Current code in `_generate_signals()`:
```python
generator = SignalGenerator(self._config)
frame = generator.generate(data_bundle)
```
Open `src/strategies/specific/modules/signal_generator.py`. Check constructor signature and method name.

---

**Assumption 4 — FilterPipeline constructor and run method**

Current code in `_run_filters()`:
```python
pipeline = FilterPipeline(self._config)
result = pipeline.run(signal_frame)
```
Open `src/strategies/specific/modules/filter_pipeline.py`. Check:
- Is the method `.run()`, `.apply()`, or `.execute()`?
- Does `FilterPipelineResult.pass_rate` return 0–1 (multiplied by 100 in current logging) or 0–100 already?
- Is `FilterPipelineResult.final_signals` the correct field name for the filtered `SignalFrame`?

---

**Assumption 5 — TradeSimulator constructor and simulate_trades signature**

Current code in `_simulate_trades()`:
```python
simulator = TradeSimulator(self._config)
result = simulator.simulate_trades(
    signal_frame=filter_result.final_signals,
    data_bundle=data_bundle,
    mode=self._mode,
)
```
Open `src/strategies/specific/modules/trade_simulator.py`. Check:
- Does `simulate_trades()` accept `signal_frame`, `data_bundle`, `mode` as keyword args in that form?
- Is `TradeResult.total_trades` a direct field or must it be derived from `len(result.trades)`? The orchestrator currently uses `hasattr` defensively.

---

### After Verifying Assumptions — Steps to First Green Run

**Step 1**: Create `configs/strategies/wbws/wbws_strategy_v2.yaml`.  
Copy `strategy_template.yaml`. Fill in WBWS-specific parameters by translating from the legacy `configs/strategies/wbws/wbws_strategy.yaml` — new key names only, no legacy keys.  
Verify: `StrategyConfig.from_yaml(Path("configs/strategies/wbws/wbws_strategy_v2.yaml"))` must succeed without `ValueError`.

**Step 2**: Run core mode first:
```bash
python scripts/runners/run_strategy.py \
    --config configs/strategies/wbws/wbws_strategy_v2.yaml \
    --mode core \
    --log-level INFO
```

**Step 3**: Expected output shape (numbers will differ — use Session 19 baseline as a sanity check):
```
============================================================
RESULT SUMMARY
============================================================
  Mode          : core
  Total trades  : ~4379
  Win rate      : ~35.8%
  Total PnL     : ~-10476.0 pts
  Expectancy    : ~-2.39 pts/trade
  Profit factor : ~0.87
  Max drawdown  : -XXX.X pts

  Stage timing:
    data          XXXX.X ms   ← target <500ms
    signals         XX.X ms   ← target <50ms
    filters         XX.X ms   ← target <30ms
    trades        XXXX.X ms   ← target <10,000ms
    metrics          X.X ms   ← target <5ms
    TOTAL         XXXX.X ms   ← target <12,000ms
============================================================
```

**Step 4**: Once core mode passes, run analytics mode. Confirm trade counts are identical between modes on the same data.

**Step 5**: Lock the timings as the Session 21 performance baseline. Create `docs/migration/PERFORMANCE_BASELINE_S21.md`:
```
Date: [Session 21 date]
Dataset: [instrument] | [bar count] bars | [start] to [end]

Core mode:
  data_load:      Xms
  signal_gen:     Xms
  filter:         Xms
  trade_sim:      Xms
  metrics:        Xms
  TOTAL:          Xms

Analytics mode:
  data_load:      Xms
  signal_gen:     Xms
  filter:         Xms
  trade_sim:      Xms
  metrics:        Xms
  TOTAL:          Xms

Non-regression rule: >5% degradation on any stage = P0 blocker.
```

---

## Carry-Forward Items (do after green E2E run)

Do not start these until the E2E run is green and the baseline is locked.

**CF-1 — AnalyticsConfig contract (DEC-032)**  
Add `AnalyticsConfig` frozen dataclass to `analytics_contracts.py`. Update `TradeAnalytics.analyze()` to accept an optional `analytics_config` parameter. Replace 4 hardcoded threshold constants in `trade_analytics.py` with configurable fields. Default values must match current hardcodes — behaviour unchanged.

**CF-2 — TimeFilter typed parameters (P1-CH3-8)**  
`TimeFilter.__init__` currently accepts `config: Dict`. Replace with a typed `TimeFilterConfig` frozen dataclass. Coordinated two-file change: `time_filter.py` + `filter_pipeline.py`. Do both atomically or not at all.

**CF-3 — New tests**  
After CF-1 and CF-2:
- `test_analytics_config_defaults_match_legacy_constants()`
- `test_analytics_config_custom_thresholds_used_in_insights()`
- `test_wbws_v2_yaml_loads()`
- `test_time_filter_rejects_raw_dict()`
- `test_orchestrator_core_mode_returns_metrics_report()`
- `test_orchestrator_clears_risk_manager_cache_between_runs()`

Target test count after Session 21: ~310

---

## Constraints

- **Architecture locked**: no contract or interface change without a DEC entry first.
- **Verify before running**: correct all five assumptions before the first `python` invocation.
- **STATUS: PARTIAL rule**: if the session ends before the green E2E run, write `STATUS: PARTIAL — stopped at [Assumption N / Step N]` at the very top of this file so the next session knows exactly where to resume.
- **No MagicMock in any new test**: real dataclasses only.
- **Performance non-regression**: once the Session 21 baseline is locked, any stage regression >5% is a P0 blocker.

---

## Key Contract Quick Reference

The minimum contract knowledge needed to verify the five assumptions and interpret orchestrator output — without reading the full ARCHITECTURE.md.

**StrategyConfig** (`src/config/config_schema.py`)  
Top-level config. Sub-configs: `.data`, `.execution`, `.trade_management`, `.filters`, `.output`.  
Mode expected at: `config.execution.mode` → `"core"` or `"analytics"` (assumption to verify).

**DataBundle** (`src/strategies/contracts/data_contracts.py`)  
Output of DataLoader. Key: `.strategy` (date-sliced DataFrame), `.info` (DataInfo with bar counts), `.has_ltf()`, `.has_htf()`.

**SignalFrame** (`src/strategies/contracts/signal_contracts.py`)  
Output of SignalGenerator. Key: `.signals` (int8 Series: 1=BUY, 2=SELL, 0=none). Methods: `.count_by_type()` → `{"buy": int, "sell": int, "total": int}`, `.iter_raw()`. Do not call `__iter__` in core mode — use `iter_raw()`.

**FilterPipelineResult** (`src/strategies/contracts/filter_contracts.py`)  
Output of FilterPipeline. Key: `.final_signals` (SignalFrame), `.raw_count`, `.final_count`, `.pass_rate`.

**TradeResult** (`src/strategies/contracts/trade_contracts.py`)  
Output of TradeSimulator. Key: `.trades` (List[Trade]), `.win_count`, `.loss_count`, `.total_pnl_points`, `.execution_mode`, `.execution_time_ms`.

**MetricsReport** (`src/strategies/contracts/metrics_contracts.py`)  
Output of `calculate_metrics(trade_result)`. Key: `.total_trades`, `.win_rate`, `.total_pnl_points`, `.expectancy_points`, `.profit_factor`, `.max_drawdown`.

**OrchestratorResult** (`src/strategies/orchestrator.py`)  
Output of `orchestrator.run()`. Holds all five stage outputs plus `.stage_durations_ms` (dict) and `.total_duration_ms`. Convenience properties: `.total_trades`, `.win_rate`, `.total_pnl_points`, `.summary()`.
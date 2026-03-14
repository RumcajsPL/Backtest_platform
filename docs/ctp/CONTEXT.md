# CONTEXT.md — CTP Session State
# Updated: 2026-03-14 (Weekend exploratory session — end of day)
---
## Where We Are
```
BACKTESTING ENGINE:    V1 PRODUCTION — frozen.
                       Weekend exploratory runs in progress (V1_03 → V1_06).
BROKER INTEGRATION:    Phase 2 pipeline confirmed live 2026-03-13.
                       Stage 1 dry-run ✅. Stage 2 place-order path ✅.
                       Awaiting first live signal Monday (DAX hours 09:00–16:00 UTC).
```
---
## Phase 2 Deliverables (all in place — use phase2_deliverables_v3.zip)
```
configs/broker_support/broker_support_config.yaml
src/broker_support/config/broker_support_config.py
src/broker_support/utils/time_utils.py
src/broker_support/live/__init__.py
src/broker_support/live/live_data_fetcher.py
src/broker_support/live/live_config_patcher.py
src/broker_support/live/live_data_bundle.py
src/broker_support/live/order_signal.py
src/broker_support/live/signal_bridge.py
scripts/broker_support/run_signal.py
scripts/broker_support/run_signal_loop.py
tests/broker_support/test_time_utils.py
tests/broker_support/test_signal_pipeline_integration.py
```
---
## Test Status
```
90/90  unit tests passing
63/63  integration tests passing
```
---
## Trade Constraint Status (all four closed)
| Constraint | Value | Enforced where |
|---|---|---|
| max_risk_percentile | 0.45 | RiskManager — uses full ARTF parquet |
| pyramiding_enabled | false | _check_pyramiding() in run_signal.py Stage 2 |
| max_positions | 1 | Same guard — source: strategy YAML |
| close_on_opposite | false | Emergent from pyramiding guard |
---
## Paper Trade Candidates (run b651ec5c — production reference)
| Priority | Candidate | WFO | Ruin | Status |
|----------|-----------|-----|------|--------|
| 1st | c424a0e04327 | 0.8108 | 0.000 | PRIMARY — active in run_signal.py |
| 2nd | 20745ca991be | 0.7201 | 0.054 | SECONDARY — after PRIMARY stable |
| Watch | c42f8b009283 | 0.6473 | 0.000 | MONITOR |
| Watch | c209820886c8 | 0.5699 | 0.000 | SECONDARY MONITOR — do NOT promote |
---
## Weekend Exploratory Runs
| YAML | STF | HTF | Filters | Status | Notes |
|------|-----|-----|---------|--------|-------|
| V1_03 | 1min | 1min (no HTF) | DPO+Chop+CCI+ADX | ✅ Complete (547c3161) | 2 auto_go: 0984ffd0af4c WFO=0.734, 462dd658ffeb WFO=0.635 |
| V1_04 | 5min | 15min | DPO+CCI+MACD/+BB | 🔲 Pending | Launch after V1_06 confirmed clean |
| V1_05 | 10min | 4H | DPO+MA/+BB | 🔲 Pending | Launch after V1_06 confirmed clean |
| V1_06 | 15min | 1D | DPO+MACD/+CCI | 🔄 Fix applied — relaunch pending | Crash fixed in macd_filter.py |

---
## CRITICAL — V1_06 Crash Investigation (FIX APPLIED, VERIFICATION PENDING)

### Summary
Deterministic crash at candidate 102/200 (also 155/300 in earlier run — same
proportional position, same LHS seed). Process killed silently by Windows.

### Root cause — CONFIRMED via crash dump + reproduce_crash.py
```
pandas_ta_classic MACD bug on short WFO window slices:
  pta.macd(series_n30) returns None for signalma on short input
  → histogram = macd - None → TypeError
  → exception propagates into C extension error handling
  → VCRUNTIME140.dll access violation (0xc0000005) → process killed

Evidence:
  - Crash dump: RAX=0xaaaaaaaaaaaaaaaa (MSVC freed-heap fill)
  - Faulting offset 0x113eb VCRUNTIME140.dll identical both crashes
  - _ta_lib.cp313-win_amd64.pyd confirmed in call stack
  - reproduce_crash.py Phase 1 Test 6: pta.macd(n=30, talib=False)
    → TypeError: unsupported operand type(s) for -: 'float' and 'NoneType'
    histogram = macd - signalma  ← signalma is None on n=30
```

### Crashing candidate parameters (exploration zone, seed=42, index=1)
```
atr_length=11, atr_multiplier=1.9, rr_target=4.6, risk_percentile=1.16
dpo_length=11, dpo_smooth=7, dpo_threshold=0.2
macd_fast=8, macd_slow=23, macd_signal=14
cci_length=20, cci_overbought=64, cci_oversold=-95
```

### Fix applied to src/strategies/filters/macd_filter.py
```python
# _calculate_macd — tightened length guard:
min_required = self.slow_length + self.signal_length + 1
if len(series) < min_required:
    return pd.Series(np.nan, index=series.index, dtype="float32")
# + None guard on hist after macd_df returned

# compute_indicators — matching guard:
min_length = self.slow_length + self.signal_length + 1
if len(df) < min_length:
    # return NaN series early
```

### Eliminated approaches (do not retry)
- gc.disable() around pta.macd() — wrong path, crash still occurred
- talib=False parameter — same bug exists in pure Python path
- TA-Lib version downgrade — 0.6.8 is latest, no older stable version available
- OOM / multiprocessing race — eliminated by event log + max_workers=1 test

### Pending cleanup
- Remove gc.disable from cci_filter.py (was wrong fix, not needed)
  gc.disable is in compute_indicators wrapping pta.cci() call

### Diagnostic tools
```
scripts/diagnostics/diagnose_crash_candidate.py
  — reproduces LHS sampling to identify crashing candidate parameters
  — usage: edit TARGET_ZONE, TARGET_IDX, N_SAMPLES, SEED at top of file

scripts/diagnostics/reproduce_crash.py
  — Phase 1: tests each indicator in isolation on real data
  — Phase 2: runs full pipeline via StrategyConfig + StrategyOrchestrator
  — usage: python scripts/diagnostics/reproduce_crash.py
```

---
## V2 Backlog (from this session)
```
V2-RISK-PERC-TF   risk_percentile is TF-dependent trade filter (not position sizer).
                  Unit: % of account equity (0.45 = 0.45%, NOT 45%).
                  Must re-calibrate empirically per TF before setting zone ranges.
                  1min=0.45%, 15min sweet spot=0.93%, range 0.80-1.20%.

V2-PARAM-VALID    Parameter constraint validator at candidate construction.
                  Reject MACD fast>=slow before any indicator library called.
                  Extend to other filters with similar constraints.

V2-SIGMOID-CFG    Make _SIGMOID_SCALE a per-run config parameter.
                  Currently hardcoded 310.0 in consistency_scorer.py.
                  Higher TF runs have different P&L distribution stdev.

V2-WORKER-CRASH   Isolated worker crash must not kill parent process.
                  Currently: silent parent kill at max_workers=2.
                  Fix: catch subprocess exit, log candidate, continue pipeline.

V2-WINDOW-TF      WFO window width should adapt to trade frequency.
                  Rule: window must average ≥30 trades.
                  15min needs 6-month windows; 1min fine with 3-month.

V2-PTA-MACD       pandas_ta_classic MACD returns None for signalma on short series.
                  Workaround: length guard in macd_filter.py (applied).
                  Permanent fix: replace pta.macd with pure pandas EMA implementation
                  or wait for pandas_ta_classic fix.
                  Pure pandas alternative:
                    ema_fast = series.ewm(span=fast, adjust=False).mean()
                    ema_slow = series.ewm(span=slow, adjust=False).mean()
                    macd_line = ema_fast - ema_slow
                    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
                    histogram = macd_line - signal_line
```
---
## Open Issues
| ID | Description | Priority |
|----|-------------|----------|
| PHASE-2-STAGE2 | First live demo order: open → track → close | P0 — Monday |
| RESOLVER-FIELDS | InstrumentResolver missing 'fields' param + exact-match | P1 |
| WINZIP-32 | WinError 32 on GA temp YAMLs | Cosmetic |
| V1_06-CRASH | Fix applied — verify by relaunching V1_06 | P1 — next run |
| CCI-GC-CLEANUP | Remove gc.disable from cci_filter.py | P2 — cosmetic |
---
## Backtest YAML Status
```
configs/backtesting/backtest_V1_03.yaml  ✅ Ready
configs/backtesting/backtest_V1_04.yaml  ✅ Ready — 5min/15min DPO+CCI+MACD/+BB
configs/backtesting/backtest_V1_05.yaml  ✅ Ready — 10min/4H DPO+MA/+BB
configs/backtesting/backtest_V1_06.yaml  ✅ Ready v1.3 — 15min/1D DPO+MACD/+CCI
                                           max_workers=1 (crash isolation, restore to 2 after fix confirmed)
                                           6-month WFO windows
                                           risk_percentile 0.80–1.20
```
---
## Useful Commands
```powershell
pytest tests/broker_support/ -v
python scripts/broker_support/run_signal_loop.py --verbose
python scripts/broker_support/run_signal.py --verbose
python scripts/broker_support/run_signal.py --place-order --verbose
python scripts/broker_support/inspect_portfolio.py
python scripts/broker_support/run_tracker_loop.py --once --no-hours-guard
python scripts/diagnostics/diagnose_crash_candidate.py
python scripts/diagnostics/reproduce_crash.py
python scripts/diagnostics/query_run.py
```
---
## Key Paths
```
Strategy YAML:    outputs/backtesting/trading_yamls/b651ec5c_c424a0e04327_strategy.yaml
ARTF parquet:     data/processed/ohlcv/DEUIDXEUR_1ME_20210101_20260301.parquet
BS config:        configs/broker_support/broker_support_config.yaml
Instrument map:   configs/broker_support/instrument_map.yaml  (symbol key: GER40)
Credentials:      configs/broker_support/broker_settings.env
Backtest configs: configs/backtesting/backtest_V1_0[3-6].yaml
MACD filter:      src/strategies/filters/macd_filter.py  ← crash fix applied here
CCI filter:       src/strategies/filters/cci_filter.py   ← gc.disable to remove
Filter pipeline:  src/strategies/core/filter_pipeline.py
Strategy runner:  src/backtesting/strategy_runner.py     ← _PARAM_KEY_MAP reference
```
---
## Next Session Start
1. Check if first demo order was placed: python scripts/broker_support/inspect_portfolio.py
2. If yes → confirm journal entry, review tracker loop output
3. If no → run_signal_loop.py during DAX hours (09:00–16:00 UTC, skip 17–18)
4. Relaunch V1_06 → confirm passes candidate 102 without crash
5. If V1_06 clean → restore max_workers=2, launch V1_04 and V1_05
6. Remove gc.disable from cci_filter.py
7. After first order confirmed → plan Stage 3 automation loop
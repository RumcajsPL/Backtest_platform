# CONTEXT.md — CTP Session State
# Updated: 2026-03-16 (Full day session — V1_04 analysis + V1_05 queued, V1_06 v3.0 analysis + v4.0 queued, first live signal observed)
---
## Where We Are
```
BACKTESTING ENGINE:    V1 PRODUCTION — frozen.
                       1min series:  V1_04 complete. V1_05 queued overnight.
                       15min series: V1_06 v3.0 complete. V1_06 v4.0 queued daytime.
BROKER INTEGRATION:    Phase 2 pipeline LIVE as of 2026-03-13.
                       Signal loop running. First signal observed 2026-03-16 14:35 UTC.
                       Rejected by RiskManager — expected behaviour, not a bug.
                       Plan: run full week, review RiskManager calibration if pattern repeats.
                       Command: python scripts/broker_support/run_signal_loop.py
                                --config configs/broker_support/broker_support_config.yaml
```
---
## CRITICAL: Two Independent Backtest Series
```
DO NOT cross-compare results between series. Different TF, different filters,
different parameter ranges, different window structures. All analysis is per-series.
1min series:  configs/backtesting/backtest_V1_0X.yaml    (overnight, ~8–15 hrs)
15min series: configs/backtesting/backtest_V1_06_vX.yaml (daytime, ~3–6 hrs)
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
## Live Signal Loop — Status
```
First day:  2026-03-16, 09:00–16:00 UTC
Signal:     Poll #324, 14:35 UTC — BUY @ 23605.05 (bid)
            49 raw → 1 filter survivor (2% pass rate — consistent with backtest)
            REJECTED by RiskManager (threshold_pct=0.45)
            Likely cause: elevated ATR near US open 14:30 UTC
Backtest baseline (c424a0e04327, 38 months):
  ~4.6 filter signals/day, ~1.8 trades/day approved (~39% RiskManager pass rate)
Next steps:
  - Run full week before any conclusions on RiskManager calibration
  - If rejections cluster near 14:30 UTC → check whether 0.45% threshold needs
    recalibration for 2026 DAX volatility vs 2023–2024 backtest period
  - If random rejections across session → investigate ATR distribution
  - No code changes until pattern confirmed across 3–5 trading days
```
---
## 1min Exploratory Runs
| YAML | Run ID | Status | auto_go | Best WFO | Notes |
|------|--------|--------|---------|----------|-------|
| V1_02 (generic) | 547c3161 | ✅ Complete | 2 | 0.734 | safe zone dead, exploration only productive |
| V1_03 (focused) | 6fcf82b9 | ✅ Complete | 3 | 0.766 | confirmed focused zone correct |
| V1_04 (refined) | 63f3cc3d | ✅ Complete | 9 | 0.810 | series high — risk_perc sweet spot 0.21–0.29 |
| V1_05 (convergence) | — | 🔄 Queued overnight | — | — | sigmoid=128, risk_perc 0.20–0.35 |
### V1_05 Key Settings (overnight run)
```
_SIGMOID_SCALE = 128  (stdev=257 in 63f3cc3d → recommended=128)
Zone: focused (single zone)
rr_target:       2.6–3.2   (upper trimmed from 3.6 — nothing above 3.1 in V1_04 top-5)
atr_multiplier:  1.9–2.7   (both tails trimmed — 1.8 and 2.8–2.9 dead)
atr_length:      10–24     (floor raised from 5 — 5–9 confirmed underperforming)
risk_percentile: 0.20–0.35 (KEY CHANGE — V1_04 top-5 ALL in 0.21–0.29)
GA: population=80, generations=40, stagnation=12 (unchanged from V1_04)
Samples: 250
Key question: does 0.20–0.35 ceiling maintain quality without trade starvation?
Warning trigger: if trades_per_week failures spike beyond ~6/run → widen to 0.20–0.42
```
### 1min Parameter Findings (confirmed across 547c3161 + 6fcf82b9 + 63f3cc3d)
```
rr_target:        2.6–3.2 sweet spot. >3.2 dead. 2.6 floor confirmed.
atr_multiplier:   1.9–2.7 productive. 1.8 dead. 2.8–2.9 dead.
atr_length:       10–24 productive. 5–9 confirmed underperforming (bottom-5 twice).
risk_percentile:  0.20–0.35 CONFIRMED sweet spot. V1_04 top-5 all 0.21–0.29.
                  Zero top-5 above 0.35 across any 1min run.
dpo_threshold:    Mixed signals across candidates. 0.10–0.25 confirmed productive.
choppiness_threshold: Near-insensitive (≤0.0011). 54–65 correct.
adx_threshold:    <22 dead. 22–30 confirmed.
W10 survivor:     9dc5db154fe1 only candidate to survive W10 (+90.8 net_pnl). Key diagnostic.
Sigmoid trend:    stdev declining: 620→361→326→257. Check before each run.
```
---
## 15min Exploratory Runs
| YAML | Run ID | Status | Honest auto_go | Best honest WFO | Notes |
|------|--------|--------|----------------|-----------------|-------|
| V1_06 v1.3 | 6b137540 | ✅ Complete | — | 0.747 (5 win) | phantom problem, min_trades=20 |
| V1_06 v2.0 | 2d50b27e | ✅ Complete | 5 genuine | 0.882 (3 win) | 2 phantom, min_trades=15 |
| V1_06 v3.0 | 1fd58c85 | ✅ Complete | 6 genuine | 0.960 (4 win) | 2 phantom (2-window), W03 unlocked |
| V1_06 v4.0 | — | 🔄 Queued daytime | — | — | 4×9-month windows, win_rate floor 0.15 |
### V1_06 v4.0 Key Settings (next daytime run)
```
Windows:         4 × ~9-month (MAJOR CHANGE from 6 × 6-month)
  W01: 2023-01-02 → 2023-09-29  (DAX recovery)
  W02: 2023-10-02 → 2024-06-28  (ECB rate cycle — primary stress)
  W03: 2024-07-01 → 2025-03-31  (H2 productive absorbs dead H1 2025)
  W04: 2025-04-01 → 2026-02-28  (most recent regime)
min_win_rate:    0.15  (reduced from 0.18 — 190/192 Stage 1 failures were win_rate)
atr_multiplier:  safe 1.5–1.9 / exploration 1.2–2.0  (ceilings reduced)
risk_percentile: exploration 0.83–1.10  (minor trim)
go_wfo_floor:    0.70  (raised from 0.65 — partial phantom mitigation)
samples/zone:    175  (increased from 150)
_SIGMOID_SCALE = 310 (stdev~598, recommended~299 — essentially correct, no change)
max_workers: 4
Key questions:
  1. Does 4-window structure raise avg windows_evaluated for top-10?
     Success: avg >= 3.0, zero auto_go with windows_evaluated < 3
  2. Does win_rate floor 0.15 recover Stage 1 pass rate (was 36% in v3.0)?
     Success: >= 55% pass rate, top-5 win_rate still >= 0.20
  3. Does absorbing W05 into W03 help or just add noise?
```
### 15min Parameter Findings (confirmed across 6b137540 + 2d50b27e + 1fd58c85)
```
rr_target:        6.0–9.5 productive. Sub-6 dead. V3.0 top-5: 8.8, 8.2, 6.9, 8.3, 6.2.
atr_multiplier:   1.5–1.9 confirmed sweet spot. >2.0 never in top-5 across 3 runs.
risk_percentile:  0.83–1.10 confirmed. V3.0 top-5: 0.90, 1.01, 0.85, 0.87, 1.07.
win_rate:         0.18 floor was over-filtering (63% Stage 1 cut). Lowered to 0.15 in v4.0.
Sigmoid:          310 correct for 15min series throughout (~1.04× inflation in v3.0).
W05 (2025 H1):    Structurally dead for DPO+MACD. Absorbed into W03 in 4-window design.
Phantom verdicts: Still present at 2-window level in v3.0. V2-VERDICT-GATE is the fix.
```
---
## Open Issues
| ID | Description | Priority |
|----|-------------|----------|
| LIVE-RISKMANAGER | Review 0.45% threshold after 1 week of live signals | P0 — next Monday |
| V2-VERDICT-GATE | windows_evaluated >= 3 minimum before any verdict | P1 — highest V2 priority |
| MACD-SIGNAL-GUARD | macd_filter.py: raise ValueError if signal_length < 2 | P1 |
| RESOLVER-FIELDS | InstrumentResolver missing 'fields' param + exact-match | P1 |
| CCI-GC-CLEANUP | Remove gc.disable from cci_filter.py | P2 — cosmetic |
| WINZIP-32 | WinError 32 on GA temp YAMLs | Cosmetic |
---
## V2 Backlog
```
V2-VERDICT-GATE   windows_evaluated >= 3 gate in verdict engine.
                  INSUFFICIENT_COVERAGE verdict if < 3 windows scored.
                  Highest-priority V2 item — phantom auto_go confirmed across 4 runs.
                  go_wfo_floor raised to 0.70 in v4.0 — NOT a fix, just mitigation.
V2-PARAM-VALID    Parameter validator at candidate construction.
                  Reject macd_signal_length < 2. Reject MACD fast >= slow.
V2-SIGMOID-CFG    Make _SIGMOID_SCALE a per-run config parameter.
                  1min: 128 (V1_05) — declining stdev trend.
                  15min: 310 correct throughout.
V2-RISK-PERC-TF   risk_percentile TF-dependent. 1min: 0.20–0.35. 15min: 0.83–1.10.
V2-WORKER-CRASH   Isolated worker crash must not kill parent.
                  15min stable at 4. 1min stable at 2.
V2-WINDOW-TF      WFO window width adapt to trade frequency. Partially solved by
                  4×9-month design. Quantify LTF end-of-window coverage gap.
V2-PTA-MACD       Replace pta.macd with pure pandas EMA. Workaround sufficient for V1.
```
---
## Backtest YAML Status
```
configs/backtesting/backtest_V1_04.yaml    ✅ Complete (63f3cc3d)
configs/backtesting/backtest_V1_05.yaml    🔄 Overnight — sigmoid=128, risk_perc 0.20–0.35
configs/backtesting/backtest_V1_06_v4.yaml 🔄 Queued daytime — 4×9-month windows, win_rate 0.15
```
---
## Useful Commands
```powershell
# Live trading
python scripts/broker_support/run_signal_loop.py --config configs/broker_support/broker_support_config.yaml
python scripts/broker_support/run_signal.py --verbose
python scripts/broker_support/run_signal.py --place-order --verbose
python scripts/broker_support/inspect_portfolio.py
python scripts/broker_support/run_tracker_loop.py --once --no-hours-guard
# Diagnostics
python scripts/diagnostics/query_run.py
python scripts/diagnostics/diagnose_crash_candidate.py
python scripts/diagnostics/reproduce_crash.py
# Tests
pytest tests/broker_support/ -v
```
---
## Key Paths
```
Strategy YAML:    outputs/backtesting/trading_yamls/b651ec5c_c424a0e04327_strategy.yaml
ARTF parquet:     data/processed/ohlcv/DEUIDXEUR_1ME_20210101_20260301.parquet
BS config:        configs/broker_support/broker_support_config.yaml
Instrument map:   configs/broker_support/instrument_map.yaml  (symbol key: GER40)
Credentials:      configs/broker_support/broker_settings.env
1min configs:     configs/backtesting/backtest_V1_04.yaml (complete)
                  configs/backtesting/backtest_V1_05.yaml (overnight)
15min configs:    configs/backtesting/backtest_V1_06_v4.yaml (queued)
MACD filter:      src/strategies/filters/macd_filter.py  ← crash fix applied
CCI filter:       src/strategies/filters/cci_filter.py   ← gc.disable to remove (P2)
```
---
## Next Session Start
1. Check live signal loop: python scripts/broker_support/inspect_portfolio.py
2. If trade placed → confirm journal entry, review tracker loop output
3. If no trade → note time-of-day pattern on any RiskManager rejections
4. Check V1_05 overnight results → key question: did risk_percentile 0.20–0.35
   maintain quality? Did trades_per_week failures spike?
5. Check V1_06 v4.0 daytime results → key question: did 4-window structure
   improve avg windows_evaluated? Did win_rate floor 0.15 fix pass rate?
6. Remove gc.disable from cci_filter.py (P2 cosmetic — carry-forward)
7. After 1 week of live signals → review RiskManager 0.45% calibration
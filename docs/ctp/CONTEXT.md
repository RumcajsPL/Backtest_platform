# CONTEXT.md — CTP Session State
# Updated: 2026-03-15 (Full day session — 1min recalibration + 15min TF exploration)
---
## Where We Are
```
BACKTESTING ENGINE:    V1 PRODUCTION — frozen.
                       1min exploratory series: V1_03 complete, V1_04 queued overnight.
                       15min exploratory series: V1_06 v1/v2/v3 in progress.
BROKER INTEGRATION:    Phase 2 pipeline confirmed live 2026-03-13.
                       Stage 1 dry-run ✅. Stage 2 place-order path ✅.
                       First live signal loop: tomorrow morning (DAX 09:00–16:00 UTC).
                       Command: python scripts/broker_support/run_signal_loop.py
                                --config configs/broker_support/broker_support_config.yaml
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
## 1min Exploratory Runs
| YAML | Run ID | Status | auto_go | Best WFO | Notes |
|------|--------|--------|---------|----------|-------|
| V1_03 (generic, v1.02) | 547c3161 | ✅ Complete | 2 | 0.734 | safe zone dead, exploration only productive |
| V1_03 (focused, v1.03) | 6fcf82b9 | ✅ Complete | 3 | 0.7655 | confirmed focused zone correct approach |
| V1_04 (refined, v1.04) | — | 🔄 Queued overnight | — | — | sigmoid=163, GA 80×40 |

### V1_04 Key Settings (overnight run)
```
_SIGMOID_SCALE = 163  (manually set — stdev=325.93 in 6fcf82b9, recommended=163)
Zone: focused (single zone, evidence-based bounds)
rr_target:       2.6–3.6   (confirmed sweet spot 2.8–3.3)
atr_multiplier:  1.8–2.9   (floor raised: 1.2–1.7 dead)
risk_percentile: 0.20–0.60 (ceiling: >0.55 underperforms)
choppiness_threshold: 54.0–65.0 (trimmed dead lower tail)
GA: population=80, generations=40, stagnation=12
Samples: 250 (tighter range → ~68% pass rate estimated)
```
### 1min Parameter Findings (confirmed across 547c3161 + 6fcf82b9)
```
rr_target:        2.8–3.3 sweet spot. >3.7 dead. <2.6 underperforms.
atr_multiplier:   1.5 confirmed dead (bottom-5). 2.1–2.7 productive.
risk_percentile:  0.25 and 0.51 both produced auto_go — wide range valid.
                  >0.55 tends to underperform. <0.20 not tested.
dpo_threshold:    sensitivity signal points lower (0.10–0.15). GA should explore.
choppiness_threshold: near-insensitive parameter (deltas ≤0.0007).
adx_threshold:    <22 dead (V1_02 confirmed). 22–30 productive.
Sigmoid:          stdev varies ~325–620 across runs. _SIGMOID_SCALE=163 for V1_04.
                  Within-run relative ranking preserved regardless of scale value.
```
---
## 15min Exploratory Runs
| YAML | Run ID | Status | auto_go | Best WFO | Notes |
|------|--------|--------|---------|----------|-------|
| V1_06 v1.3 | 6b137540 | ✅ Complete | 6* | 0.9731* | *phantom — 1-window scores |
| V1_06 v2.0 | 2d50b27e | ✅ Complete | 7* | 0.9507* | *2 phantom, 5 genuine |
| V1_06 v3.0 | — | 🔄 Queued | — | — | min_trades=12, mult ceil lowered |
### V1_06 v3.0 Key Settings (next run)
```
_SIGMOID_SCALE = 310  (stdev=566 in 2d50b27e → recommended=283, but 310≈close enough)
min_significant_trades: 12  (reduced from 15 — target W03 unlock)
atr_multiplier: safe 1.5–2.0 / exploration 1.2–2.3  (ceiling reduced)
rr_target: safe 6.0–9.5 / exploration 5.5–10.0  (floor raised)
risk_percentile: exploration ceiling 1.20→1.12
go_wfo_floor: 0.65 (raised from 0.55 — partial phantom mitigation)
borderline_wfo_floor: 0.45
max_workers: 4  (confirmed stable at 15min TF)
```
### 15min Structural Findings (confirmed across 6b137540 + 2d50b27e)
```
WINDOW STARVATION: Core problem. W03/W05 structurally under-traded.
  W03 (2024 H1 ECB cycle): 13–14 trades avg — just below threshold.
        → Target: min_significant_trades=12 should unlock this window.
  W05 (2025 H1 range-bound): ~6–10 trades. Structurally dead for DPO+MACD.
        → Accept as dead window for this filter family. Not fixable by params.
PHANTOM VERDICTS: WFO scorer assigns near-perfect scores to 1-window candidates
  (variance=0, frac_pos=1.0 trivially). go_wfo_floor does not prevent this.
  V2 FIX REQUIRED: windows_evaluated >= 3 gate in verdict engine before
  any verdict can be issued. Highest-priority V2 backtesting item.
HONEST BENCHMARKS (windows_evaluated >= 3, frac_pos >= 0.80):
  fadaf986a898: 5 windows, WFO=0.747, frac_pos=1.0  ← best structurally sound
  a25c382aa687: 5 windows, WFO=0.573, frac_pos=0.80
  fa0be02aa749: 3 windows, WFO=0.882, auto_go  ← most trustworthy auto_go
  fce4168e8c42: 4 windows, WFO=0.861, borderline
  995d8190bff7: 4 windows, WFO=0.852, borderline
rr_target:        6–9 sweet spot. Sub-6 consistently bottom-half both runs.
atr_multiplier:   Universal degradation at higher values. 1.5–2.0 productive.
                  da44ec91c996 sensitivity: +0.072 at mult-0.1 (strongest signal).
risk_percentile:  0.85–1.10 confirmed. >1.12 consistently underperforms.
max_workers=4:    Confirmed stable. LTF slices smaller at 15min → lower mem/worker.
Sigmoid 310:      stdev=566–628 across 15min runs → recommended ~283–314. 310 close.
macd_signal=1:    CONFIRMED CRASH TRIGGER. min=2 enforced in all zone definitions.
                  Also add structural guard in macd_filter.py (V2-PARAM-VALID).
LTF coverage:     98% at 15min. Trades lasting until window end close at
                  end-of-data price. More impactful at 15min than 1min.
                  Quantify % of trades affected in V2 (V2-WINDOW-TF).
```
---
## Weekend Exploratory Plan Status
| YAML | STF | HTF | Filters | Status |
|------|-----|-----|---------|--------|
| V1_03 | 1min | 1min | DPO+Chop+CCI+ADX | ✅ Complete (6fcf82b9) |
| V1_04 | 1min | 1min | DPO+Chop+CCI+ADX | 🔄 Overnight — sigmoid=163 |
| V1_05 | 10min | 4H | DPO+MA/+BB | 🔲 Pending |
| V1_06 v3 | 15min | 1D | DPO+MACD/+CCI | 🔄 Queued |
---
## Open Issues
| ID | Description | Priority |
|----|-------------|----------|
| PHASE-2-STAGE2 | First live signal loop tomorrow morning | P0 — Monday 09:00 UTC |
| V2-VERDICT-GATE | windows_evaluated >= 3 minimum before any verdict | P1 — highest V2 priority |
| MACD-SIGNAL-GUARD | macd_filter.py: raise ValueError if signal_length < 2 at construction | P1 |
| RESOLVER-FIELDS | InstrumentResolver missing 'fields' param + exact-match | P1 |
| CCI-GC-CLEANUP | Remove gc.disable from cci_filter.py | P2 — cosmetic |
| WINZIP-32 | WinError 32 on GA temp YAMLs | Cosmetic |
---
## V2 Backlog
```
V2-VERDICT-GATE   windows_evaluated >= 3 minimum gate in verdict engine.
                  If windows_evaluated < 3 → verdict = INSUFFICIENT_COVERAGE.
                  Highest-priority V2 backtesting item — confirmed needed across
                  two 15min runs. Phantom auto_go verdicts on 1-window candidates.
V2-PARAM-VALID    Parameter constraint validator at candidate construction.
                  - Reject MACD signal_length < 2 before indicator called.
                  - Reject MACD fast >= slow before any indicator library called.
                  Extend to other filters with similar constraints.
V2-SIGMOID-CFG    Make _SIGMOID_SCALE a per-run config parameter (currently hardcoded).
                  Observed values: 1min=310 (b651ec5c), 1min=163 (V1_04),
                  15min=283–314 (V1_06 series). Not comparable across TFs.
V2-RISK-PERC-TF   risk_percentile is TF-dependent trade filter (not position sizer).
                  1min: 0.45% production. 15min: 0.85–1.10% confirmed range.
                  Re-calibrate empirically per TF. Never transfer 1min values up.
V2-WORKER-CRASH   Isolated worker crash must not kill parent process.
                  Currently: silent parent kill at max_workers=2 (1min).
                  15min: confirmed stable at max_workers=4.
                  Fix: catch subprocess exit, log candidate, continue pipeline.
V2-WINDOW-TF      WFO window width should adapt to trade frequency.
                  1min: 3-month fine. 15min: 6-month required.
                  Also: quantify LTF coverage impact on 15min results —
                  trades closing at end-of-data price (98% coverage = 2% affected).
V2-PTA-MACD       pandas_ta_classic pta.macd() returns None for signalma on short series.
                  Workaround: length guard in macd_filter.py (applied).
                  Permanent fix: replace with pure pandas EMA:
                    ema_fast = series.ewm(span=fast, adjust=False).mean()
                    ema_slow = series.ewm(span=slow, adjust=False).mean()
                    macd_line = ema_fast - ema_slow
                    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
                    histogram = macd_line - signal_line
```
---
## Backtest YAML Status
```
configs/backtesting/backtest_V1_03.yaml  ✅ Complete (6fcf82b9)
configs/backtesting/backtest_V1_04.yaml  🔄 Overnight — sigmoid=163, GA 80×40
configs/backtesting/backtest_V1_05.yaml  🔲 Pending — 10min/4H DPO+MA/+BB
configs/backtesting/backtest_V1_06.yaml  🔄 v3.0 queued — min_trades=12, mult↓, rr↑
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
1min configs:     configs/backtesting/backtest_V1_03.yaml (complete)
                  configs/backtesting/backtest_V1_04.yaml (overnight)
15min configs:    configs/backtesting/backtest_V1_06.yaml (v3.0 queued)
MACD filter:      src/strategies/filters/macd_filter.py  ← crash fix applied
CCI filter:       src/strategies/filters/cci_filter.py   ← gc.disable to remove
```
---
## Next Session Start
1. Check first live signal: python scripts/broker_support/inspect_portfolio.py
2. If order placed → confirm journal entry, review tracker loop output
3. If no order → run_signal_loop.py during DAX hours (09:00–16:00 UTC)
4. Check V1_04 overnight results → analyse vs 6fcf82b9, note if WFO improves
5. Check V1_06 v3.0 results → key question: did W03 unlock (rejections < 20)?
6. Remove gc.disable from cci_filter.py (P2 cosmetic)
7. After first order confirmed → plan Stage 3 automation loop
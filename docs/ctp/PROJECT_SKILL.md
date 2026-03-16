---
name: backtester-project
description: >
  Use this skill whenever working on the Backtesting & Optimization Framework project
  OR the broker_support / eToro API integration project. Triggers: any mention of
  backtester, backtest pipeline, CandidateStore, GA engine, WFO evaluator, Monte Carlo
  engine, fitness evaluator, scenario profile, backtest_template.yaml, sensitivity
  evaluator, verdict engine, report generator, any module from src/backtesting/,
  broker_support, EToroClient, PositionTracker, CSVJournal, paper trading automation,
  eToro API, signal bridge, CTP roadmap, WBWS+, time filter, hour filter,
  LiveDataFetcher, SignalBridge, OrderSignal, LiveConfigPatcher, BrokerSupportConfig,
  pyramiding, max_positions, _check_pyramiding, macd_filter, cci_filter, filter_pipeline.
  Read this SKILL.md before writing any code, creating any file, or making any design
  decision for this project.
---
# CTP Project Skill — Backtesting + Broker Integration
## Project Status (2026-03-16, end of day)
```
BACKTESTING ENGINE:    V1 PRODUCTION — PHASE 1 GATE FULLY CLOSED. Engine frozen.
                       1min series:  V1_04 complete (63f3cc3d). V1_05 queued overnight.
                       15min series: V1_06 v3.0 complete (1fd58c85). V1_06 v4.0 queued daytime.

BROKER INTEGRATION:    Phase 2 pipeline LIVE. Signal loop running.
                       First live signal observed 2026-03-16 14:35 UTC — BUY @ 23605.05.
                       Rejected by RiskManager (threshold_pct=0.45). Expected behaviour.
                       No executed trades yet. Monitoring continues tomorrow.
```
---
## TWO INDEPENDENT BACKTEST SERIES — DO NOT CROSS-COMPARE
```
1min series  (overnight runs, ~8–15 hours):
  Config family: backtest_V1_0X.yaml
  Strategy TF:   1min
  HTF:           1min
  WFO windows:   13 × 3-month (2023-01 → 2026-02)
  Filters:       DPO + Choppiness + CCI + ADX
  Zone:          Single focused zone
  Production:    c424a0e04327 (run b651ec5c) — active in run_signal.py
15min series (daytime runs, ~3–6 hours):
  Config family: backtest_V1_06_vX.yaml
  Strategy TF:   15min
  HTF:           1D
  WFO windows:   4 × ~9-month from v4.0 (previously 6 × 6-month)
  Filters:       DPO + MACD (safe zone) / DPO + MACD + CCI (exploration)
  Zone:          Two zones — safe + exploration
  Production:    Not yet — still in exploratory series
Rule: NEVER compare fitness scores, WFO scores, risk_percentile values,
      rr_target values, or parameter ranges across these two series.
      They are different instruments (same DAX), different regimes, different
      filter families. All calibration is per-series only.
```
---
## CRITICAL — macd_filter.py crash fix (2026-03-14)
```
BUG: pandas_ta_classic pta.macd() returns None for signalma on short series
     (n < slow_length + signal_length + 1). Causes C extension crash.
FIX APPLIED in src/strategies/filters/macd_filter.py:
  _calculate_macd:    min_required = self.slow_length + self.signal_length + 1
  compute_indicators: min_length   = self.slow_length + self.signal_length + 1
  Both return NaN series early if len < min_required.
ALSO CONFIRMED: macd_signal=1 is a crash/degenerate trigger.
  Zone definitions must enforce macd_signal min=2.
  V2-PARAM-VALID: add ValueError at construction if signal_length < 2.
WRONG APPROACHES (do not retry):
  - gc.disable() around pta.macd() — wrong code path
  - talib=False parameter — same bug in pure Python path
PENDING CLEANUP:
  - Remove gc.disable from cci_filter.py compute_indicators (was wrong fix)
V2 permanent fix: replace pta.macd with pure pandas EMA implementation.
```
---
## risk_percentile — CRITICAL UNIT AND BEHAVIOUR
```
Unit: percentage of account equity. 0.45 = 0.45% NOT 45%.
Behaviour: TRADE FILTER, not position sizer.
  RiskManager computes ATR-based risk in points, converts to % of equity.
  Signal REJECTED if that % > max_risk_percentile.
  Effect is TF-dependent: larger ATR at higher TFs → more signals rejected.
Empirical calibration (DAX, 38 months):
  1min:  0.20–0.35 confirmed sweet spot (V1_04 top-5 all 0.21–0.29)
         0.45% production value (c424a0e04327)
         >0.35 consistently underperforms across three runs
  15min: 0.83–1.10 productive zone (V1_06 v3.0 confirmed)
         <0.83 = trade starvation. >1.10 = consistently underperforms.
Rule: re-calibrate empirically per TF before setting zone ranges.
Do NOT transfer 1min values to higher TF runs.
```
---
## WFO window sizing — CRITICAL
```
Rule: WFO window must average >= min_significant_trades trades.
  1min:  3-month windows fine (~150+ trades/window). 13 windows confirmed.
  15min: 9-month windows from v4.0 (previously 6-month — starvation issues)
15min 4-window structure (v4.0 onwards):
  W01  2023-01-02 → 2023-09-29  DAX recovery — directional, scores well
  W02  2023-10-02 → 2024-06-28  ECB rate cycle — choppy, primary stress test
  W03  2024-07-01 → 2025-03-31  H2 productive absorbs H1 2025 dead months
  W04  2025-04-01 → 2026-02-28  Most recent regime — partial 2026 Q1
15min 6-window history (v1.3 → v3.0, for reference):
  W01 (2023 H1): ~18–20 trades — borderline scorer
  W02 (2023 H2): ~24–28 trades — most reliable
  W03 (2024 H1): ~13–14 trades — unlocked at min_trades=12 in v3.0
  W04 (2024 H2): ~16–21 trades — scores well
  W05 (2025 H1): ~6–10 trades — STRUCTURALLY DEAD for DPO+MACD family
                 Absorbed into W03 in 4-window design from v4.0
  W06 (2025 H2+2026 Q1): ~23–32 trades — most recent, reliable
min_significant_trades history:
  v1.3: 20 → widespread starvation, phantom 1-window verdicts
  v2.0: 15 → improved, W03 still mostly rejected
  v3.0: 12 → W03 partially unlocked (30 rejections vs 86 for W05)
  v4.0: 12 → kept at floor, longer windows solve starvation structurally
  ABSOLUTE FLOOR: do NOT go below 12.
```
---
## Phantom WFO verdicts — KNOWN STRUCTURAL BUG
```
Problem: WFO scorer assigns near-perfect scores to low-window candidates.
  variance=0 and frac_pos=1.0 are trivially true for 1–2 data points.
  go_wfo_floor does NOT prevent this — the issue is upstream in the scorer.
Observed across V1_06 series:
  v1.3: phantom candidates on 1 window
  v2.0: 2 phantom auto_go (1-window scores)
  v3.0: 2 phantom auto_go (2-window scores: 09e1609b4a56 WFO=0.9606,
         c151411ec5ee WFO=0.9043 — both windows_evaluated=2)
  go_wfo_floor progression: 0.40 → 0.55 → 0.65 → 0.70 (v4.0)
  None of these mitigations solve the root cause.
V2 FIX REQUIRED (highest priority V2 backtesting item):
  Minimum windows_evaluated >= 3 gate in verdict engine.
  If windows_evaluated < 3 → verdict = INSUFFICIENT_COVERAGE regardless of score.
When reading results: always check windows_evaluated column first.
  Discard any candidate with windows_evaluated < 3 regardless of WFO score.
  Honest benchmark: windows_evaluated >= 3, frac_pos >= 0.80.
  With 4-window structure: minimum honest bar is windows_evaluated >= 3 of 4.
```
---
## Sigmoid scale calibration
```
_SIGMOID_SCALE = 310.0 hardcoded in consistency_scorer.py.
Formula: recommended = stdev_of_net_pnl_values × 0.5
Observed values:
  1min b651ec5c:  stdev=620, recommended=310  ✅ exact match (production)
  1min 547c3161:  stdev=361, recommended=181  (ran at 310 — slight inflation)
  1min 6fcf82b9:  stdev=326, recommended=163  (ran at 310 — inflation ~1.9×)
  1min 63f3cc3d:  stdev=257, recommended=128  (ran at 163 — inflation ~1.27×)
  1min V1_05:     sigmoid manually set to 128 before run
  15min 6b137540: stdev=628, recommended=314  (ran at 310 — essentially exact)
  15min 2d50b27e: stdev=566, recommended=283  (ran at 310 — inflation ~1.1×)
  15min 1fd58c85: stdev=598, recommended=299  (ran at 310 — inflation ~1.04×) ✅ negligible
  15min v4.0:     310 unchanged — stdev ~597, essentially correct
Rules:
  - Scale affects fitness scores uniformly — relative ranking preserved.
  - Fitness scores NOT comparable across TFs (different P&L distributions).
  - Fitness scores NOT comparable across runs with different scale values.
  - Within-run relative ranking always valid regardless of scale value.
  - V2 action: make _SIGMOID_SCALE a per-run config parameter.
  - For now: check sigmoid diagnostic in query_run.py, set manually if >> 310.
  - 15min series: 310 is effectively correct — no manual override needed.
  - 1min series:  stdev has been declining. Check diagnostic before each run.
```
---
## 1min parameter findings (confirmed across 547c3161 + 6fcf82b9 + 63f3cc3d)
```
rr_target:        2.6–3.2 confirmed sweet spot. >3.2 produced nothing in V1_04 top-5.
                  2.6 floor confirmed (9dc5db154fe1 WFO=0.810). >3.7 dead.
atr_multiplier:   1.9–2.7 confirmed. 1.8 produced nothing. 2.8–2.9 produced nothing.
                  V1_04 top-5: 2.6, 1.9, 2.5, 2.2, 2.0
atr_length:       10–24 productive. 5–9 confirmed underperforming (bottom-5 twice).
                  12 appeared in V1_04 #2 candidate — do not cut below 10.
risk_percentile:  0.20–0.35 CONFIRMED sweet spot across three runs.
                  V1_04 top-5 ALL in 0.21–0.29. Zero top-5 above 0.35 across any run.
                  Ceiling tightened to 0.35 in V1_05. Critical trade-off:
                  tighter ceiling = fewer trades passing ATR filter. Monitor
                  trades_per_week failures — if they spike, widen to 0.20–0.42.
dpo_threshold:    Mixed signals — lower better for some, higher for others.
                  Range 0.10–0.25 confirmed productive. Do not narrow until resolved.
choppiness_threshold: Near-insensitive (deltas ≤0.0011 across all V1_04 candidates).
                  54.0–65.0 confirmed. No action.
adx_threshold:    <22 dead (V1_02). 22–30 confirmed active range.
W10 (2025 Q2):    9dc5db154fe1 is ONLY 1min candidate to post positive W10 net_pnl
                  (+90.8) across all runs. Structural stress window. Key diagnostic.
Sigmoid:          1min stdev declining run-over-run (~620→361→326→257).
                  Set _SIGMOID_SCALE = 128 for V1_05.
Safe zone:        Dead. Single focused zone confirmed correct since V1_03.
```
---
## 15min parameter findings (confirmed across 6b137540 + 2d50b27e + 1fd58c85)
```
rr_target:        6.0–9.5 productive. Sub-6 consistently bottom-half all three runs.
                  V1_06 v3.0 top-5 genuine: 8.8, 8.2, 6.9, 8.3, 6.2
                  Ceiling 9.5 still in play — do not narrow upper.
atr_multiplier:   1.5–1.9 confirmed sweet spot (v3.0 genuine top-5: 1.7, 1.6, 1.9, 1.7, 1.8)
                  Universal sensitivity degradation at higher values across all runs.
                  >2.0 never produced a top-5 candidate across three 15min runs.
                  Ceiling: safe 1.9 / exploration 2.0 in v4.0.
risk_percentile:  0.83–1.10 confirmed. V3.0 top-5: 0.90, 1.01, 0.85, 0.87, 1.07.
                  >1.10 consistently dead. <0.83 not tested — floor may have room.
win_rate:         0.18 floor was over-filtering at 15min — killed 190/192 Stage 1
                  candidates in v3.0 (avg win_rate=0.1686 across all 300 candidates).
                  Lowered to 0.15 in v4.0. Monitor: if top-5 drop below 0.18 → revert.
macd_signal:      min=2 enforced. signal=1 crash/degenerate — do not allow.
MACD structural:  fast_max < slow_min must be maintained in all zone defs.
                  safe:        fast max=12, slow min=14
                  exploration: fast max=15, slow min=16
max_workers:      4 confirmed stable at 15min. Revert to 2 if any OOM observed.
Sigmoid 310:      stdev ~597–628 across 15min runs → recommended ~298–314. 310 correct.
                  No manual override needed for 15min series.
```
---
## 15min series progression (honest candidates — windows_evaluated >= 3 only)
```
V1_06 v1.3  6b137540  best: fadaf986a898  5 windows  WFO=0.747
V1_06 v2.0  2d50b27e  best: fa0be02aa749  3 windows  WFO=0.882
V1_06 v3.0  1fd58c85  best: 89703d22bf44  4 windows  WFO=0.960  ← series high
V1_06 v4.0  queued    key question: do 4×9-month windows improve avg coverage?
             success criterion: avg windows_evaluated for top-10 >= 3.0
             success criterion: zero auto_go with windows_evaluated < 3
```
---
## 1min series progression
```
V1_02  547c3161  2 auto_go   best WFO=0.734  generic zones, safe zone dead
V1_03  6fcf82b9  3 auto_go   best WFO=0.766  focused zone confirmed
V1_04  63f3cc3d  9 auto_go   best WFO=0.810  series high — 9dc5db154fe1
                              key finding: risk_percentile sweet spot 0.21–0.29
                              key finding: only candidate to survive W10 (+90.8)
V1_05  queued    key question: does risk_percentile 0.20–0.35 maintain quality
                               without causing trade starvation?
```
---
## Live signal loop — observations (2026-03-16)
```
First day running: 09:00–16:00 UTC, DAX session.
Signal observed:   Poll #324, 14:35 UTC. BUY @ 23605.05.
Filter pipeline:   49 raw → 1 surviving (2% pass rate — consistent with backtest)
RiskManager:       REJECTED. threshold_pct=0.45. Expected behaviour.
                   Likely cause: elevated ATR near US open (14:30 UTC). Not a bug.
Backtest baseline for context:
  c424a0e04327: 3805 filter survivors / 820 trading days ≈ 4.6 signals/day
                1498 trades approved / 820 days ≈ 1.8 trades/day
                RiskManager approval rate ≈ 39% of filter survivors
Assessment:       1 rejection on day 1 is statistically meaningless (1 vs 820-day
                  baseline). Run full week before drawing conclusions.
Plan:             If RiskManager rejects every signal near 14:30 UTC → investigate
                  whether 0.45% threshold needs recalibration for 2026 DAX volatility
                  vs 2023–2024 backtest period. Not a code issue — a calibration question.
```
---
## Live pipeline flow
```
broker_support_config.yaml -> BrokerSupportConfig
    |
LiveConfigPatcher.load_and_patch() -> patched StrategyConfig
    |
LiveDataFetcher.fetch(symbol) -> (df_strategy, df_htf)
    |
build_live_data_bundle(df_strategy, df_htf, artf_path) -> DataBundle
    |
SignalGenerator -> FilterPipeline [strategy time_filter 08:30-20:30 CET]
    |
Last-bar signal check
    |
RiskManager [max_risk_percentile enforced using ARTF]
    |
is_valid_trading_window() -> WBWS+ gate [non-blocking]
    |
OrderSignal(direction, sl, tp, max_positions=1, ...)
    |
run_signal.py --place-order:
    _check_pyramiding() -> portfolio fetch -> abort if >= max_positions
    OrderRouter.open_position()
```
### Key design decisions (locked)
1. DataLoader bypassed — no parquet reads in live context except artf
2. TradeSimulator NOT called — only last-bar signal + RiskManager
3. Strategy time_filter kept unchanged — backtested params must not be altered
4. WBWS+ is non-blocking — signals shown even outside window
5. artf path explicit in broker_support_config.yaml
6. max_positions from strategy YAML (backtested), not safety section
---
## Empirically confirmed API facts
```
KEY TYPE:    ETORO_USER_KEY = Demo Write key. Real key → 403 on /demo/ endpoints.
Portfolio:   GET /api/v1/trading/info/demo/portfolio
             'credit' (/portfolio) vs 'credits' (/pnl) — do NOT mix
Two-step open:
             POST market-open-orders/by-amount → orderForOpen.orderID
             GET demo/orders/{orderID} poll until statusID==1 → positionID
             positionID NOT in open-order response — must poll
Execution:   PascalCase + capital ID: InstrumentID, IsBuy, Amount, Leverage
Trade history: GET /api/v1/trading/info/trade/history?minDate=YYYY-MM-DD
Candles:     max 1000 bars. direction: always fetch 'desc', reverse to asc.
             volume always 0 for DAX — keep for schema compat.
OHLC fields: can be None (not missing key — value is None).
             Use bar.get("field") or 0.0, NOT bar.get("field", 0.0)
```
---
## Trade Constraint Enforcement
| Constraint | Value | Enforced where |
|---|---|---|
| max_risk_percentile | 0.45% | RiskManager — full ARTF parquet |
| pyramiding_enabled | false | _check_pyramiding() in run_signal.py Stage 2 |
| max_positions | 1 | Same — source: strategy YAML (backtested) |
| close_on_opposite | false | Emergent from pyramiding guard |
---
## V2 Backlog (priority order)
```
V2-VERDICT-GATE   [P1 — HIGHEST] windows_evaluated >= 3 minimum gate in verdict engine.
                  If windows_evaluated < 3 → verdict = INSUFFICIENT_COVERAGE.
                  Confirmed needed across all four 15min runs (v1.3→v3.0).
                  Phantom auto_go verdicts observed at 2-window level in v3.0 despite
                  go_wfo_floor=0.65. go_wfo_floor raised to 0.70 in v4.0 — not a fix.
V2-PARAM-VALID    [P1] Parameter constraint validator at candidate construction.
                  - Reject macd_signal_length < 2 before indicator called.
                  - Reject MACD fast >= slow before any indicator called.
                  Extend to other filters with similar constraints.
V2-SIGMOID-CFG    [P2] Make _SIGMOID_SCALE a per-run config parameter.
                  Currently hardcoded 310.0 in consistency_scorer.py.
                  Values by TF: 1min≈128–310, 15min≈299–314. Not comparable across TFs.
V2-RISK-PERC-TF   [P2] risk_percentile is TF-dependent. Re-calibrate per TF.
                  1min: 0.20–0.35 confirmed. 15min: 0.83–1.10 confirmed.
                  Never transfer across TFs.
V2-WORKER-CRASH   [P2] Isolated worker crash must not kill parent process.
                  15min: max_workers=4 confirmed stable.
                  1min: confirmed OOM at 6, stable at 2. Do not raise until
                  V2 shared-memory architecture.
V2-WINDOW-TF      [P2] WFO window width should adapt to trade frequency.
                  Partially addressed by 4×9-month design in V1_06 v4.0.
                  Also: quantify LTF end-of-window coverage gap at 15min.
V2-PTA-MACD       [P3] Replace pta.macd with pure pandas EMA implementation.
                  Workaround in macd_filter.py sufficient for V1.
```
---
## Frozen Constants
```python
_SIGMOID_SCALE         = 310.0   # Default — override manually per run if stdev >> 620
                                  # 1min V1_05: manually set to 128 (stdev=257 in 63f3cc3d)
                                  # 15min V1_06 v4.0: 310 correct (stdev~598, rec~299)
_MAX_EXPECTED_DRAWDOWN = 2_500.0
max_workers (1min)     = 2       # OOM at 6 — mandatory until V2 shared memory
max_workers (15min)    = 4       # Confirmed stable — revert to 2 if any OOM
```
---
## Architecture Rules (non-negotiable)
```python
# Contracts: Pydantic models / frozen dataclasses — never raw dicts across boundaries
# Fail fast: invalid config raises at construction, no silent fallbacks
# Datetime: datetime.now(timezone.utc) — NEVER datetime.utcnow()
# Paths: pathlib.Path — never hardcoded separators
# Logging: logger.info/debug only — never print()
# Broker: _make_request() is the HTTP engine — never implement HTTP in public methods
# Live: DataLoader bypassed — use LiveDataFetcher + build_live_data_bundle
# Pyramiding: _check_pyramiding() in run_signal.py — portfolio fetch before OrderRouter
# Constraints: strategy YAML position_control values authoritative, not safety section
# WBWS+: is_valid_trading_window() — non-blocking, sets flag only
# Time filter: strategy time_filter params never patched in LiveConfigPatcher
```
---
## What NOT to do
```
# Broker API
- Do NOT call /demo/ endpoints with Real key → 403
- Do NOT omit 'fields' param on market-data/search → empty results
- Do NOT use 'from'/'fromDate' for trade history → use minDate=YYYY-MM-DD
- Do NOT use Read-only key for trade/history → 403
- Do NOT assume positionID is in open-order response → poll order info
- Do NOT send 'InstrumentId' (lowercase d) in close body
- Do NOT confuse 'credit' (/portfolio) with 'credits' (/pnl)
- Do NOT use bar.get("field", 0.0) for OHLC → value can be None even when key exists
# Architecture
- Do NOT refactor _make_request() — solid, do not touch
- Do NOT set LIVE_APPROVED in code — operator-only
- Do NOT call DataLoader in live context
- Do NOT call TradeSimulator in live context
- Do NOT modify strategy time_filter in LiveConfigPatcher
- Do NOT modify position_control in LiveConfigPatcher
- Do NOT use broker_support_config.yaml safety.max_open_positions as pyramiding limit
- Do NOT use datetime.utcnow() — use datetime.now(timezone.utc)
- Do NOT use ltf_timeframe=None in DataInfo — use "1s"
# Backtesting — both series
- Do NOT cross-compare 1min and 15min results — different series, different everything
- Do NOT transfer risk_percentile, rr_target or any parameter values across TFs
- Do NOT assume sigmoid scale transfers across TFs or runs
- Do NOT set MACD fast >= slow in zone parameter ranges
- Do NOT retry gc.disable() as fix for pandas_ta_classic MACD crash — wrong path
- Do NOT retry talib=False as fix for MACD crash — same bug in pure Python path
# Backtesting — 1min specific
- Do NOT use 3-month WFO windows at 15min TF — use 9-month (v4.0+)
- Do NOT set risk_percentile ceiling above 0.35 for 1min — 0.35–0.60 confirmed dead
# Backtesting — 15min specific
- Do NOT set macd_signal min < 2 — signal=1 confirmed crash/degenerate trigger
- Do NOT accept WFO auto_go verdict if windows_evaluated < 3 — phantom score
- Do NOT set min_significant_trades < 12 — statistical floor
- Do NOT raise max_workers above 4 at 15min until confirmed stable
- Do NOT lower wfo_collapse_drawdown_threshold below 600 at 15min TF
- Do NOT use 6-month WFO windows for 15min from v4.0 onwards — 9-month design adopted
- Do NOT set win_rate floor above 0.15 for 15min — 0.18 was killing 63% of Stage 1
```
---
## Platform
```
OS:          Windows 10, Python 3.13.12
Timezone:    OHLCV/signals CET/CEST; pipeline timestamps UTC
Project:     E:\Trading\Backtest_platform
API base:    https://public-api.etoro.com/api/v1
Credentials: configs/broker_support/broker_settings.env (Demo Write key)
TA-Lib:      0.6.8 (latest — no downgrade path available)
```
---
## Session Deliverables (end of every session)
- Updated docs/ctp/CONTEXT.md
- Updated SKILL.md if architecture/findings changed
- docs/ctp/BROKER_INTEGRATION.md if API findings changed
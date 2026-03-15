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
## Project Status (2026-03-15, end of day)
```
BACKTESTING ENGINE:    V1 PRODUCTION — PHASE 1 GATE FULLY CLOSED. Engine frozen.
                       1min series: V1_03 complete, V1_04 overnight.
                       15min series: V1_06 v1/v2/v3 in progress.

BROKER INTEGRATION:    Steps 1-5 COMPLETE. 90/90 tests passing.
                       Phase 2 pipeline confirmed live 2026-03-13.
                       First live signal loop: Monday morning (DAX 09:00–16:00 UTC).
                       Command: python scripts/broker_support/run_signal_loop.py
                                --config configs/broker_support/broker_support_config.yaml
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
  1min:  0.20–0.60 productive zone (V1_03/V1_04 series)
         0.45% production value (c424a0e04327)
  15min: 0.80–1.12 productive zone (V1_06 series, confirmed across 2 runs)
         <0.80 = trade starvation. >1.12 = consistently underperforms.
Rule: re-calibrate empirically per TF before setting zone ranges.
Do NOT transfer 1min values to higher TF runs.
```
---
## WFO window sizing and starvation — CRITICAL FOR 15min+
```
Rule: WFO window must average >= min_significant_trades trades.
  1min:  3-month windows fine (~150+ trades/window)
  15min: 6-month windows required (~15–30 trades/window)
         3-month = widespread REJECTED_INSUFFICIENT_TRADES
15min structural window behaviour (confirmed V1_06 series):
  W01 (2023 H1): ~18–20 trades — borderline, often scores
  W02 (2023 H2): ~24–28 trades — most reliable scoring window
  W03 (2024 H1): ~13–14 trades — just below threshold at 15, target with 12
  W04 (2024 H2): ~16–21 trades — scores for some candidates
  W05 (2025 H1): ~6–10 trades — STRUCTURALLY DEAD for DPO+MACD family
                 Not fixable by parameter tuning. Accept and move on.
  W06 (2025 H2+2026 Q1): ~23–32 trades — reliable, most recent regime
min_significant_trades history:
  v1.3: 20 → widespread starvation, phantom 1-window verdicts
  v2.0: 15 → improved, but W03 still mostly rejected (13-14 trades)
  v3.0: 12 → target W03 unlock. Do NOT go below 12.
```
---
## Phantom WFO verdicts — KNOWN STRUCTURAL BUG
```
Problem: WFO scorer assigns near-perfect scores to 1-window candidates.
  variance=0 and frac_pos=1.0 are trivially true for a single data point.
  go_wfo_floor does NOT prevent this — the issue is upstream in the scorer.
Observed: 4 phantom auto_go candidates across V1_06 v1.3 and v2.0.
  Example: 0d921ad4b8a9 WFO=0.9507, windows_evaluated=1. Meaningless score.
Partial mitigation (applied in YAML):
  go_wfo_floor raised progressively: 0.40 → 0.55 → 0.65
  Does not solve the problem but reduces false positive rate.
V2 FIX REQUIRED (highest priority V2 backtesting item):
  Minimum windows_evaluated >= 3 gate in verdict engine.
  If windows_evaluated < 3 → verdict = INSUFFICIENT_COVERAGE regardless of score.
When reading results: always check windows_evaluated column first.
  Discard any candidate with windows_evaluated < 3 regardless of WFO score.
  Honest benchmark: windows_evaluated >= 3, frac_pos >= 0.80.
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
  1min V1_04:     sigmoid manually set to 163 before run ← first corrected run
  15min 6b137540: stdev=628, recommended=314  (ran at 310 — essentially exact)
  15min 2d50b27e: stdev=566, recommended=283  (ran at 310 — inflation ~1.1×)

Rules:
  - Scale affects fitness scores uniformly — relative ranking preserved.
  - Fitness scores NOT comparable across TFs (different P&L distributions).
  - Fitness scores NOT comparable across runs with different scale values.
  - Within-run relative ranking always valid regardless of scale value.
  - V2 action: make _SIGMOID_SCALE a per-run config parameter.
  - For now: check sigmoid diagnostic in query_run.py, set manually if >> 310.
```
---
## 1min parameter findings (confirmed across 547c3161 + 6fcf82b9)
```
rr_target:            2.8–3.3 sweet spot. >3.7 dead. <2.6 underperforms.
atr_multiplier:       1.5 confirmed dead (bottom-5). 2.1–2.7 productive.
                      1.8 floor: 1.2–1.7 dead space confirmed.
risk_percentile:      0.25 and 0.51 both auto_go — wide range productive.
                      >0.55 tends to underperform. Ceiling 0.60 for V1_04.
dpo_threshold:        Sensitivity signal: lower is better (0.10–0.15 direction).
                      GA should find floor — do not narrow range prematurely.
choppiness_threshold: Near-insensitive (deltas ≤0.0007). 54–65 range confirmed.
adx_threshold:        <22 dead (V1_02). 22–30 confirmed active range.
atr_length:           No structural bias. Full 5–24 range productive.
Safe zone:            Dead — produced zero top candidates in 547c3161.
                      Retired. Single focused zone approach confirmed correct.
```
---
## 15min parameter findings (confirmed across 6b137540 + 2d50b27e)
```
rr_target:        6–9 sweet spot. Sub-6 consistently bottom-half both runs.
                  9.3 produced genuine 2-window candidate (a68c06066c4d).
                  Floor raised to 5.5–6.0 in v3.0.
atr_multiplier:   Universal sensitivity degradation at higher values.
                  Strongest signal: da44ec91c996 delta +0.072 at mult -0.1.
                  1.5–2.0 productive. >2.3 underperforms. Ceiling 2.3 in v3.0.
risk_percentile:  0.85–1.10 confirmed sweet spot. >1.12 consistently dead.
                  Ceiling lowered to 1.12 (exploration) in v3.0.
macd_signal:      min=2 enforced. signal=1 crash/degenerate — do not allow.
MACD structural:  fast_max < slow_min must be maintained in all zone defs.
                  safe: fast max=12, slow min=14.
                  exploration: fast max=15, slow min=16.
max_workers:      4 confirmed stable at 15min. Memory per worker lower than 1min.
                  Revert to 2 immediately if any OOM or silent crash observed.
```
---
## pandas_ta_classic TA-Lib delegation
```
Delegate to TA-Lib by default when installed:
  pta.macd() → BUG on short series + signal=1 (fix in macd_filter.py)
  pta.cci()  → gc.disable added (not needed, remove in cleanup)
  pta.atr(), pta.bbands(), pta.rsi(), pta.ema(), pta.sma() — all delegate

Safe (no TA-Lib path): pta.dpo() — pure Python/pandas
To force pure Python: talib=False. Note: does NOT fix short-series None bug.
```
---
## MACD zone parameter ranges — structural constraint
```
ALWAYS ensure macd_fast_max < macd_slow_min in zone definitions.
ALWAYS ensure macd_signal_min >= 2 in zone definitions.

Correct zone structure (safe):
  macd_fast:   {min: 6,  max: 12, step: 1}   # max strictly < slow min
  macd_slow:   {min: 14, max: 30, step: 1}   # min strictly > fast max
  macd_signal: {min: 2,  max: 12, step: 1}   # min >= 2, never 1

Correct zone structure (exploration):
  macd_fast:   {min: 3,  max: 15, step: 1}
  macd_slow:   {min: 16, max: 38, step: 1}
  macd_signal: {min: 2,  max: 16, step: 1}
```
---
## ACTIVE TRACK — broker_support / Phase 2
### Live pipeline flow
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
                  Confirmed needed across two 15min runs. Phantom auto_go verdicts.
V2-PARAM-VALID    [P1] Parameter constraint validator at candidate construction.
                  - Reject macd_signal_length < 2 before indicator called.
                  - Reject MACD fast >= slow before any indicator called.
                  Extend to other filters with similar constraints.
V2-SIGMOID-CFG    [P2] Make _SIGMOID_SCALE a per-run config parameter.
                  Currently hardcoded 310.0 in consistency_scorer.py.
                  Values by TF: 1min≈163–310, 15min≈283–314. Not comparable across TFs.
V2-RISK-PERC-TF   [P2] risk_percentile is TF-dependent. Re-calibrate per TF.
                  1min: 0.20–0.60. 15min: 0.80–1.12. Never transfer across TFs.
V2-WORKER-CRASH   [P2] Isolated worker crash must not kill parent process.
                  15min: max_workers=4 confirmed stable.
                  1min: confirmed OOM at 6, stable at 2. Do not raise until
                  V2 shared-memory architecture (B9O-009).
V2-WINDOW-TF      [P2] WFO window width should adapt to trade frequency.
                  Also: quantify LTF end-of-window coverage gap at 15min.
                  (98% coverage = ~2% of trades close at end-of-data price)
V2-PTA-MACD       [P3] Replace pta.macd with pure pandas EMA implementation.
                  Workaround in macd_filter.py sufficient for V1.
```
---
## Frozen Constants
```python
_SIGMOID_SCALE         = 310.0   # Default — override manually per run if stdev >> 620
                                  # V1_04: manually set to 163 (stdev=326 in 6fcf82b9)
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
# Backtesting — 1min
- Do NOT use 1min risk_percentile values for higher TF runs — re-calibrate empirically
- Do NOT set MACD fast >= slow in zone parameter ranges
- Do NOT use 3-month WFO windows at 15min TF — use 6-month
- Do NOT assume sigmoid scale transfers across TFs or runs
- Do NOT retry gc.disable() as fix for pandas_ta_classic MACD crash — wrong path
- Do NOT retry talib=False as fix for MACD crash — same bug in pure Python path
# Backtesting — 15min
- Do NOT set macd_signal min < 2 — signal=1 confirmed crash/degenerate trigger
- Do NOT accept WFO auto_go verdict if windows_evaluated < 3 — phantom score
- Do NOT expect W05 (2025 H1) to score for DPO+MACD — structurally dead window
- Do NOT set min_significant_trades < 12 — statistical floor
- Do NOT raise max_workers above 4 at 15min until confirmed stable
- Do NOT lower wfo_collapse_drawdown_threshold below 600 at 15min TF
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
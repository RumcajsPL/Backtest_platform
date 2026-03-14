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
## Project Status (2026-03-14, weekend exploratory session end)
```
BACKTESTING ENGINE:    V1 PRODUCTION — PHASE 1 GATE FULLY CLOSED. Engine frozen.
                       Weekend exploratory runs V1_03–V1_06 in progress.
                       V1_06 crash fixed (see below). V1_04/V1_05 pending.

BROKER INTEGRATION:    Steps 1-5 COMPLETE. 90/90 tests passing.
                       Phase 2 pipeline confirmed live 2026-03-13.
                       Stage 1 dry-run ✅. Stage 2 place-order path ✅.
                       Awaiting first live signal Monday.
```
---
## CRITICAL — macd_filter.py crash fix (2026-03-14)
```
BUG: pandas_ta_classic pta.macd() returns None for signalma on short series
     (n < slow_length + signal_length + 1). This causes:
     histogram = macd - None → TypeError → C extension crash →
     VCRUNTIME140.dll access violation → process killed silently.

FIX APPLIED in src/strategies/filters/macd_filter.py:
  _calculate_macd:    min_required = self.slow_length + self.signal_length + 1
  compute_indicators: min_length   = self.slow_length + self.signal_length + 1
  Both return NaN series early if len < min_required.

WRONG APPROACHES (do not retry):
  - gc.disable() around pta.macd() — wrong code path
  - talib=False parameter — same bug in pure Python path
  - TA-Lib version downgrade — no older stable version available

PENDING CLEANUP:
  - Remove gc.disable from cci_filter.py compute_indicators (was wrong fix)

V2 permanent fix: replace pta.macd with pure pandas EMA implementation:
  ema_fast = series.ewm(span=fast, adjust=False).mean()
  ema_slow = series.ewm(span=slow, adjust=False).mean()
  macd_line = ema_fast - ema_slow
  signal_line = macd_line.ewm(span=signal, adjust=False).mean()
  histogram = macd_line - signal_line
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
  1min:  0.45% production value (c424a0e04327)
  15min: 0.40% →  12 trades (dead zone)
         0.85% → 203 trades (practical lower bound)
         0.93% → 224 trades (sweet spot: PF=1.13, exp=+3.82)
         1.20% → 244 trades (near ceiling: PF=1.15)
         ~1.35% → 253 trades (plateau)

Rule: re-calibrate empirically per TF before setting zone ranges.
Do NOT transfer 1min values to higher TF runs.
```
---
## pandas_ta_classic TA-Lib delegation
```
These pta.* functions delegate to TA-Lib by default when installed:
  pta.macd() → talib.MACD   ← BUG on short series (fix in macd_filter.py)
  pta.cci()  → talib.CCI    ← gc.disable added (not needed, remove in cleanup)
  pta.atr()  → talib.ATR
  pta.cci(), pta.macd(), pta.atr(), pta.bbands(), pta.rsi(), pta.ema(),
  pta.sma(), pta.tema(), and many others all have TA-Lib paths.

Safe functions (no TA-Lib path):
  pta.dpo()  — pure Python/pandas, no TA-Lib delegation

To force pure Python path: pass talib=False to any pta.* call.
Note: talib=False does NOT fix the short-series None bug in pta.macd().
```
---
## WFO window sizing by TF
```
Rule: WFO window must average ≥30 trades.
  1min:  3-month windows fine (~150+ trades/window)
  5min:  3-month windows OK (~50-150 trades/window)
  10min: 3-month windows borderline — monitor REJECTED_INSUFFICIENT_TRADES
  15min: 6-month windows required (~33 trades/window at 0.93% risk_percentile)
         3-month windows give ~15 trades → widespread REJECTED_INSUFFICIENT_TRADES

For 15min runs: use 6 windows covering 2023-01 to 2026-02:
  W01 2023-01-02 to 2023-06-30
  W02 2023-07-03 to 2023-12-29
  W03 2024-01-02 to 2024-06-28
  W04 2024-07-01 to 2024-12-31
  W05 2025-01-02 to 2025-06-30
  W06 2025-07-01 to 2026-02-28
```
---
## Sigmoid scale by TF
```
_SIGMOID_SCALE = 310.0 hardcoded in consistency_scorer.py.
Calibrated on 1min data (stdev ~620, scale = stdev × 0.5).
Higher TF runs have larger P&L per trade → different stdev → fitness inflated.
Fitness scores NOT comparable across TFs.
Comparable within same TF runs only.

Observed sigmoid diagnostic values:
  1min (b651ec5c):  stdev=620, recommended=310, used=310 ✅ exact match
  1min (547c3161):  stdev=361, recommended=180, used=310 (slight inflation)
  15min: TBD after first clean V1_06 run

V2 action: make _SIGMOID_SCALE a per-run config parameter.
```
---
## MACD zone parameter ranges — structural constraint
```
ALWAYS ensure macd_fast_max < macd_slow_min in zone definitions.
fast >= slow produces Python-level ValueError (handled gracefully)
but wastes compute on degenerate candidates.

Correct zone structure:
  macd_fast: {min: 3,  max: 12, step: 1}   # max strictly < slow min
  macd_slow: {min: 14, max: 38, step: 1}   # min strictly > fast max

V2 action: parameter constraint validator at candidate construction.
```
---
## ACTIVE TRACK — broker_support / Phase 2
### Project path
E:\Trading\Backtest_platform — package src/broker_support/ (editable install)
### Test suite
```
90/90 passing (pytest tests/broker_support/ -v)
```
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
             Positions: PascalCase + capital ID. instrumentID (capital).
Two-step open:
             POST market-open-orders/by-amount → orderForOpen.orderID
             GET demo/orders/{orderID} poll until statusID==1 → positionID
             positionID NOT in open-order response — must poll
Execution:   PascalCase + capital ID: InstrumentID, IsBuy, Amount, Leverage
Trade history: GET /api/v1/trading/info/trade/history?minDate=YYYY-MM-DD
               Requires Demo Write key. Demo key → 403.
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
## broker_support_config.yaml structure
```yaml
strategy:
  yaml_path: outputs/backtesting/trading_yamls/b651ec5c_c424a0e04327_strategy.yaml
live_data:
  artf_ohlcv_path: data/processed/ohlcv/DEUIDXEUR_1ME_20210101_20260301.parquet
  strategy_bars_to_fetch: 500
  htf_bars_to_fetch: 120
  strategy_interval: OneMinute
  htf_interval: OneHour
  candle_direction: desc
trading_window:
  enabled: true
  allowed_hours_utc: [9, 10, 11, 12, 13, 14, 15, 16]
  skip_hours_utc: [17, 18]
  monday_size_factor: 1.0
execution:
  instrument_map_path: configs/broker_support/instrument_map.yaml
  symbol: GER40
  amount_usd: 60.0
  leverage: 20
safety:                  # Stage 4 only — Stage 2 ignores
  max_open_positions: 3
  min_available_cash_usd: 200.0
  max_consecutive_losses: 5
  kill_switch_file: STOP
```
---
## LiveConfigPatcher — what is patched vs kept
```
PATCHED:     data.paths.strategy_ohlcv/htf_ohlcv → sentinel (artf path)
             data.paths.artf_ohlcv → real path from bs_config
             data.paths.ltf_ohlcv → None
             data.date_range → None
             execution.mode → 'core'
             output.reports.enabled → False
NOT PATCHED: filters.* / trade_management.* / asset.* / position_control.*
```
---
## Stage sequence (Phase 2)
```
Stage 1: run_signal.py (no flags)     — dry-run ✅ CONFIRMED
Stage 2: run_signal_loop.py           — polls 60s, 1 order, stops 🔄 ACTIVE
Stage 3: 3-trade automation batch     — after Stage 2 confirmed
Stage 4: full loop with abort conditions
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
## Frozen Constants
```python
_SIGMOID_SCALE         = 310.0   # NOT 359.4, NOT 221.1, NOT 180.8
_MAX_EXPECTED_DRAWDOWN = 2_500.0
max_workers            = 2       # OOM at 6 — mandatory (currently 1 for V1_06 debug)
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

# Backtesting
- Do NOT use 1min risk_percentile values for higher TF runs — re-calibrate empirically
- Do NOT set MACD fast >= slow in zone parameter ranges
- Do NOT use 3-month WFO windows at 15min TF — use 6-month
- Do NOT assume sigmoid scale transfers across TFs
- Do NOT retry gc.disable() as fix for pandas_ta_classic MACD crash — wrong path
- Do NOT retry talib=False as fix for MACD crash — same bug in pure Python path
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
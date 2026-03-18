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
## Project Status (2026-03-17, end of day) - see CONTEXT.md
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
```
---
## Platform
```
OS:          Windows 10, Python 3.13.12
Timezone:    OHLCV/signals CET/CEST; pipeline timestamps UTC
Project:     E:\Trading\Backtest_platform
API base:    https://public-api.etoro.com/api/v1
Credentials: configs/broker_support/broker_settings.env (Demo Write key)
```
---
## Session Deliverables (end of every session)
- Updated docs/ctp/CONTEXT.md
- Updated SKILL.md if architecture/findings changed
- docs/ctp/BROKER_INTEGRATION.md if API findings changed or V2 subject identified
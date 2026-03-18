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
  pyramiding, max_positions, _check_pyramiding, macd_filter, cci_filter, filter_pipeline,
  PaperTradingGuard, HaltLoopError, PauseUntilTomorrowError, circuit breaker,
  consecutive_losses, daily_drawdown, kill_switch, off-hours gate.
  Read this SKILL.md before writing any code, creating any file, or making any design
  decision for this project.
---
# CTP Project Skill — Backtesting + Broker Integration
## Project Status (2026-03-18, end of session) — Phase 2 COMPLETE
---
## Live signal loop — observations (2026-03-16 / 2026-03-17)
```
2026-03-16: Poll #324, 14:35 UTC. BUY @ 23605.05.
            Filter: 49 raw → 1 surviving (2% pass rate — consistent with backtest)
            RiskManager: REJECTED. threshold_pct=0.45. Expected. Not a bug.
            Likely cause: elevated ATR near US open (14:30 UTC).
2026-03-17: Poll #235, 13:06 UTC. BUY @ 23694.81.
            Filter: 48 raw → 2 surviving (4.2% pass rate)
            PLACED: orderID=336588020 → positionID=3466009287
            openRate=23705.89, SL=23676.47, TP=23891.07, R:R=8.8x
            Fast-fill bug caused false ORDER FAILED log — fixed this session.
            Trade was profitable.
Backtest baseline (c424a0e04327, 38 months):
  ~4.6 filter signals/day, ~1.8 trades/day, ~39% RiskManager approval rate.
RiskManager calibration: insufficient data (2 events). Run full week first.
```
---
## Fast-fill bug (fixed 2026-03-18) — order_router.py
```
Root cause: POST response already had orderForOpen.statusID=1 (fast-fill).
  order_router.py ignored it and polled /demo/orders/{id} which returned:
    attempt 1: 404 (transient — endpoint not yet indexed)
    attempt 2: statusID=3 REJECTED (stale state)
  Result: false ORDER FAILED log, but position WAS open.
Fix (order_router.py):
  1. Fast-fill detection: check orderForOpen.statusID in POST response.
     If 1 → skip polling, call _find_position_in_portfolio(orderID) instead.
  2. 404-tolerant polling: catch HTTPError 404 in poll loop, treat as
     transient for first 3 attempts (_ORDER_POLL_404_GRACE=3), then
     fall back to portfolio scan.
  3. REJECTED resilience: on statusID=3, try portfolio scan before raising
     OrderExecutionError — may be stale state on a fast-fill.
  4. _find_position_in_portfolio(): polls GET /demo/portfolio matching
     pos["orderID"] == order_id. Portfolio positions confirmed to carry
     orderID field (int). Max 10 attempts × 2s = 20s.
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
run_signal_loop.py (persistent):
    PaperTradingGuard checks (kill switch, off-hours, drawdown, cash, losses)
    _check_pyramiding() -> portfolio fetch -> idle if >= max_positions
    OrderRouter.open_position() -> fast-fill path or poll path
```
### Key design decisions (locked)
1. DataLoader bypassed — no parquet reads in live context except artf
2. TradeSimulator NOT called — only last-bar signal + RiskManager
3. Strategy time_filter kept unchanged — backtested params must not be altered
4. WBWS+ is non-blocking — signals shown even outside window
5. artf path explicit in broker_support_config.yaml
6. max_positions from strategy YAML (backtested), not safety section
7. PaperTradingGuard raises exceptions, never sys.exit() — loop decides
8. Off-hours sleep uses allowed_hours_utc from YAML — no new config fields
9. Consecutive loss streak reconstructed from journal at daily reset — not
   real-time (tracker loop integration deferred to V2)
---
## Empirically confirmed API facts
```
KEY TYPE:    ETORO_USER_KEY = Demo Write key. Real key → 403 on /demo/ endpoints.
Portfolio:   GET /api/v1/trading/info/demo/portfolio
             'credit' (/portfolio) vs 'credits' (/pnl) — do NOT mix
Two-step open (normal path):
             POST market-open-orders/by-amount → orderForOpen.orderID
             GET demo/orders/{orderID} poll until statusID==1 → positionID
             positionID NOT in open-order response — must poll
Fast-fill path (statusID==1 in POST response):
             /demo/orders/{id} returns 404 then statusID=3 — both stale/wrong
             Resolve positionID via portfolio scan: pos["orderID"] == orderID
             Portfolio positions carry orderID field (int) — confirmed 2026-03-17
404 on poll: Transient for first 3 attempts even on non-fast-fill orders
statusID=3:  May be stale on fast-fill — always check portfolio before raising
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
| pyramiding_enabled | false | _check_pyramiding() in run_signal_loop.py |
| max_positions | 1 | Same — source: strategy YAML (backtested) |
| close_on_opposite | false | Emergent from pyramiding guard |
| max_consecutive_losses | 3 | PaperTradingGuard.check_consecutive_losses() |
| consecutive_loss_action | hard_stop | SafetyConfig — first live week |
| max_daily_drawdown_pct | 5.0% | PaperTradingGuard.check_daily_drawdown() |
| min_available_cash_usd | 200.0 | PaperTradingGuard.check_min_cash() |
| max_pipeline_errors | 5 | PaperTradingGuard.record_pipeline_error() |
| kill_switch_file | STOP | PaperTradingGuard.check_kill_switch() |
| off-hours gate | allowed_hours_utc | Loop — _seconds_until_next_allowed_hour() |
---
## PaperTradingGuard — key rules
```python
# Never call sys.exit() inside PaperTradingGuard — raise HaltLoopError or
# PauseUntilTomorrowError. The loop calls sys.exit(0).
# Session-open credit captured once at startup and at each daily reset.
# Consecutive loss streak: reconstructed from journal at daily reset via
# _load_todays_pnl() + _count_tail_losses(). NOT updated in real-time
# during session (tracker loop integration is V2).
# pause_until_next_day resumes at: first hour in allowed_hours_utc
# (excluding skip_hours_utc) on the next UTC calendar day.
# Off-hours sleep: _sleep_interruptible() checks kill switch every 5 min.
# Session-open banner logged on wake before first poll.
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
# Pyramiding: _check_pyramiding() in run_signal_loop.py — portfolio fetch before OrderRouter
# Constraints: strategy YAML position_control values authoritative, not safety section
# WBWS+: is_valid_trading_window() — non-blocking, sets flag only
# Time filter: strategy time_filter params never patched in LiveConfigPatcher
# Guard: PaperTradingGuard raises — never exits. Loop calls sys.exit(0).
# Fast-fill: always check orderForOpen.statusID before polling /demo/orders/{id}
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
- Do NOT treat /demo/orders/{id} 404 as failure → transient, use grace period
- Do NOT treat /demo/orders/{id} statusID=3 as definitive → check portfolio first
- Do NOT skip orderForOpen.statusID check in POST response → fast-fill goes direct
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
- Do NOT call sys.exit() in PaperTradingGuard — raise HaltLoopError/PauseUntilTomorrowError
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
## V2 Backlog
```
- Tests: PaperTradingGuard, order_router fast-fill path
- run_tracker_loop.py → call guard.record_trade_result() on close
  (requires shared guard state or inter-process journal polling)
- daily_order_cap safeguard
- SafetyConfig / circuit-breaker evolution
- Promote secondary candidate 20745ca991be after PRIMARY stable 1 week
```
---
## Session Deliverables (end of every session)
- Updated docs/ctp/CONTEXT.md
- Updated SKILL.md if architecture/findings changed
- docs/ctp/BROKER_INTEGRATION.md if API findings changed or V2 subject identified
---
name: ctp-broker-integration
description: >
  Use for any CTP broker_support and integration work. Triggers: broker_support,
  EToroClient, PositionTracker, CSVJournal, PaperTradingGuard, HaltLoopError,
  PauseUntilTomorrowError, OrderRouter, SignalBridge, LiveDataFetcher,
  LiveConfigPatcher, OrderSignal, BrokerSupportConfig, InstrumentResolver,
  TradeEnricher, pyramiding, _check_pyramiding, open_positions.json,
  pending_order_ids, run_signal_loop, run_tracker_loop, kill_switch.
---
# -----------------------------------------------------------------------------------------
# IMPORTANT CHANGE: Claude.ai can read directly (no permission required) 
#     from BACTEST_PLATFORM and write (under Owner approval)
# Secondly Agents AI are running in the platform as "eyes and hands" of Claude.ai
#     Claude.ai can issue instruction to an agent AI (Owner would relay) to read/analyze and 
#           also write/create text, scripts etc. Claude.ai can preserve its tokens for    
#           important analitical and management tasks.  
# -----------------------------------------------------------------------------------------  
# CTP Broker Integration — Session Skill
## Session Start Protocol
```
1. Read docs/broker/CONTEXT.md       ← open issues, next actions, session state
2. If required read docs/broker/ARCHITECTURE.md  ← single source of truth for all code facts
3. Read only the source file being modified — never guess file content
4. Never write code before reading ARCHITECTURE.md
```
## Architecture Rules (non-negotiable)
```python
# HTTP:      _make_request() is the only HTTP engine — never implement HTTP elsewhere
# Config:    fail fast at construction — no silent fallbacks
# Datetime:  datetime.now(timezone.utc) — NEVER datetime.utcnow()
# Paths:     pathlib.Path — never hardcoded separators
# Logging:   logger.info/debug — never print()
# Guards:    PaperTradingGuard raises — never calls sys.exit()
# Live data: DataLoader bypassed — use LiveDataFetcher + build_live_data_bundle
# Strategy:  time_filter and position_control never patched in LiveConfigPatcher
# Pyramiding: _check_pyramiding() uses ctp_open_position_ids only (CTP-scoped)
# Pipeline errors: guard.record_pipeline_error() from SignalBridge block ONLY
# open_positions.json: written by run_signal_loop.py only
# TradeEnricher: _HISTORY_LOOKBACK_DAYS must use settings.default_days_back (30)
```
## What NOT to Do
```
# API
- Do NOT implement HTTP outside _make_request()
- Do NOT omit fields param on market-data/search → empty results
- Do NOT use from/fromDate for trade history → use minDate=YYYY-MM-DD
- Do NOT use bar.get("field", 0.0) for OHLC → value can be None even when key present
- Do NOT treat /demo/orders/{id} 404 as failure → transient, use grace period
- Do NOT trust /demo/orders/{id} statusID=3 on fast-fill → check portfolio first
- Do NOT skip orderForOpen.statusID check → fast-fill must bypass polling
- Do NOT request >30 days in fetch_closed_trades → 403
- Do NOT confuse credit (/portfolio) with credits (/pnl)
# Architecture
- Do NOT refactor _make_request()
- Do NOT call DataLoader in live context
- Do NOT call TradeSimulator in live context
- Do NOT modify strategy time_filter or position_control in LiveConfigPatcher
- Do NOT use SafetyConfig.max_open_positions as pyramiding limit
- Do NOT call sys.exit() in PaperTradingGuard
- Do NOT count portfolio fetch errors against pipeline error budget
- Do NOT write open_positions.json from run_tracker_loop.py
- Do NOT hardcode _HISTORY_LOOKBACK_DAYS — use settings.default_days_back
```
## Trade Constraints
| Constraint | Value | Enforced in |
|---|---|---|
| max_risk_percentile | 0.45 | RiskManager — ARTF parquet |
| pyramiding / max_positions | 1 | _check_pyramiding() — CTP positionIDs only |
| max_consecutive_losses | 3 | PaperTradingGuard — hard_stop |
| max_daily_drawdown_pct | 5.0% | PaperTradingGuard — CTP journal-scoped |
| min_available_cash_usd | 200.0 | PaperTradingGuard |
| max_pipeline_errors | 5 | PaperTradingGuard — SignalBridge only |
| kill_switch_file | STOP + STOP_<id> | master + per-instance |
| off-hours gate | allowed_hours_utc | Loop — _seconds_until_next_allowed_hour |
## Key API Facts (quick reference)
```
Keys:        ETORO_API_KEY = app-level. ETORO_USER_KEY = demo read/write.
Portfolio:   /demo/portfolio → credit. /demo/pnl → credits (different name).
Fast-fill:   POST statusID=1 → skip polling → portfolio scan on orderID.
Order poll:  404 transient ≤ 3 attempts. statusID=3 may be stale.
Candles:     Always fetch desc, reverse to asc. max 1000 bars.
OHLC:        bar.get("f") or 0.0 — value can be None.
History:     minDate=YYYY-MM-DD. 30-day max. Returns array directly.
isNoSL/TP:   true = DISABLED (inverted flags).
casing:      Portfolio PascalCase+capital ID. History camelCase+lowercase id.
```
## Platform
```
OS:       Windows 10, Python 3.13
Project:  E:\Trading\Backtest_platform
API base: https://public-api.etoro.com/api/v1
Config:   configs/broker_support/broker_settings.env
```
## Session Deliverables (end of every session)
```
docs/broker/ARCHITECTURE.md   ← update if any new confirmed facts or fact validated updates
docs/broker/BROKER_INTEGRATION.md ← update if API facts or endpoints changed
docs/broker/CONTEXT.md        ← update with continued over sessions and next actions/task, open points. Closed change history goes to SESSION_LOG.md
docs/broker/SKILL.md          ← update only if architecture rules changed
docs/broker/SESSION_LOG.md ← append after each session for session closed changes to uncharge other session documents. Available on request if deeper historical changes analysis required.
```
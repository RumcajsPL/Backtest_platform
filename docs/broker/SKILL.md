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
# GOVERNANCE (see docs/broker/GOV.md for full detail)
#
# Claude.ai authorities:
#   - Direct read on all project files (no Owner relay required)
#   - Write authority on .md and text files (no approval required)
#   - Does NOT write .py / .yaml / .env / .csv / .json — agents do this
#   - Does NOT execute code or run tests — agents do this
#
# Agent roster:
#   Agent A — Claude Code    : complex multi-file dev, tests, src/ changes
#   Agent B — Codex          : scoped tasks, config, utilities, boilerplate
#   Agent C — Qwen Code 3.6  : QA, search, impact analysis, health monitoring
#   Agent D — OpenCode/Gemma4: dev (overflow/replacement for B), loop liveness monitor
#
# Agent D autonomous restart: strictly bounded — see GOV.md Section 6.2
#   NEVER auto-restart after HaltLoopError or PauseUntilTomorrowError
#   NEVER auto-restart if kill switch present
#   Limit: 2 restarts per instance per 24h
#
# All instructions to agents use the template in GOV.md Section 9
# -----------------------------------------------------------------------------------------

# CTP Broker Integration — Session Skill

## Session Start Protocol
```
1. Read docs/broker/CONTEXT.md       ← open issues, next actions, session state
2. If required read docs/broker/ARCHITECTURE.md  ← single source of truth for all code facts
3. If required read docs/broker/GOV.md           ← governance, roles, monitoring protocol
4. Read only the source file being modified — never guess file content
5. Never write code before reading ARCHITECTURE.md
6. Surface health/trading advisory observations before coding work begins
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
# open_positions.json: written by run_demo_trading.py (run_signal_loop) only
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
# Monitoring
- Do NOT auto-restart loop after HaltLoopError or PauseUntilTomorrowError
- Do NOT auto-restart loop if kill switch file is present
- Do NOT exceed 2 Agent D restarts per instance per 24h
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
docs/broker/CONTEXT.md        ← next actions, open points, continued items
                                 closed changes go to SESSION_LOG.md
docs/broker/SESSION_LOG.md    ← append closed changes for this session
docs/broker/ARCHITECTURE.md   ← update only if new confirmed API/code facts
docs/broker/BROKER_INTEGRATION.md ← update only if API endpoints/facts changed
docs/broker/SKILL.md          ← update only if architecture or governance rules changed
docs/broker/GOV.md            ← update only if governance structure changed
```

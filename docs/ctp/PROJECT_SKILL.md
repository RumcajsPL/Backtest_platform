---
name: backtester-project
description: >
  Use this skill whenever working on the Backtesting & Optimization Framework project
  OR the broker_support / eToro API integration project. Triggers: any mention of
  backtester, backtest pipeline, CandidateStore, GA engine, WFO evaluator, Monte Carlo
  engine, fitness evaluator, scenario profile, backtest_template.yaml, sensitivity
  evaluator, verdict engine, report generator, any module from src/backtesting/,
  broker_support, EToroClient, PositionTracker, CSVJournal, paper trading automation,
  eToro API, signal bridge, CTP roadmap, WBWS+, time filter, hour filter.
  Read this SKILL.md before writing any code, creating any file, or making any design
  decision for this project.
---
# CTP Project Skill — Backtesting + Broker Integration
## Project Status (2026-03-12, Block 9P+3)
```
BACKTESTING ENGINE:    V1 PRODUCTION — PHASE 1 GATE FULLY CLOSED. Engine frozen.
BROKER INTEGRATION:    Steps 1–5 COMPLETE. 71/71 tests passing.
                       Full signal bridge built and tested.
                       Documentation complete (API_REFERENCE.md + BROKER_INTEGRATION.md).
CTP ROADMAP:           Phase 0 DONE.
                       Phase 2 = graduated automation + WBWS+ filter layer.
                       Stage 1 (signal validation) → Stage 2 (1 trade) → Stage 3 (3 trades) → Stage 4 (loop).
```
---
## ACTIVE TRACK — broker_support / Phase 2
### Project path
`E:\Trading\Backtest_platform` — package `src/broker_support/` (editable install)
### Test suite (71/71 passing as of 2026-03-12)
```
test_models.py            8 tests
test_csv_journal.py       7 tests
test_position_tracker.py  11 tests
test_time_utils.py        11 tests
test_order_router.py      34 tests
```
Run: `pytest tests/broker_support/ -v`
### Empirically confirmed API facts (frozen — overrule any docs that contradict)
```
KEY TYPE:               ETORO_USER_KEY = Demo Write key.
                        Real key → 403 on ALL /demo/ endpoints.
                        Key type determines account (demo vs real), not URL prefix.
Portfolio:              GET /api/v1/trading/info/demo/portfolio  ← REQUIRES Demo Write key
                        Returns { clientPortfolio: { credit, positions, orders, ordersForOpen, mirrors } }
                        Field: 'credit' here. /demo/pnl uses 'credits' — do NOT mix.
                        Position aliases: PascalCase + capital ID (positionID, instrumentID).
Live position data:     positionID: 3464232739, instrumentID: 32 (DAX), isBuy: false
                        openConversionRate: 1.15137, settlementTypeID: 0 (CFD)
                        isNoTakeProfit/isNoStopLoss: false = ENABLED (inverted semantics)
Order info:             GET /api/v1/trading/info/demo/orders/{orderId}
                        statusID: 0=Pending, 1=Executed, 2=Cancelled, 3=Rejected, 4=Partial
                        positions[0].positionID = use this for all close calls
Two-step open flow:     POST market-open-orders/by-amount → orderForOpen.orderID
                        GET demo/orders/{orderID} poll until statusID==1 → positions[0].positionID
                        positionID is NOT in the open-order response
Execution body:         PascalCase + capital ID: InstrumentID, IsBuy, Amount, Leverage
                        Close body key: InstrumentID (capital) — NOT InstrumentId (lowercase d)
                        UnitsToDeduct: null = full close
Trade history:          GET /api/v1/trading/info/trade/history?minDate=YYYY-MM-DD
                        Requires Read+Write key. Demo trades appear here (RESULT A).
                        Returns array directly. Field casing: camelCase + lowercase id.
Instrument search:      GET /api/v1/market-data/search
                        'fields' param REQUIRED (omit → empty results)
                        Use searchText; exact-match on internalSymbolFull in results
                        Response: { items: [...] }
DAX:                    instrumentId=32, symbolFull="GER40"
```
### Package structure (Steps 1–5 complete)
```
src/broker_support/
  client/client.py              ← EToroClient — do not refactor _make_request()
                                   get_portfolio → /demo/portfolio (Demo Write key)
                                   get_order_info(order_id) — two-step open Step 2
                                   close_position body → InstrumentID (capital)
  models/trade.py               ← Trade (camelCase + lowercase id — trade history)
  models/portfolio.py           ← OpenPosition: positionID/instrumentID (PascalCase)
                                   OrderForOpen, PendingOrder, available_cash()
  tracking/csv_journal.py       ← CSVJournal (dedup, header-on-empty)
  tracking/position_tracker.py  ← PositionTracker (snapshot diff → enrich → journal)
  enrichment/instrument_resolver.py ← YAML primary (DAX=32), API fallback
  enrichment/trade_enricher.py      ← RESULT A: history by positionId, 10 pages max
  execution/order_router.py     ← OrderRouter: two-step open (poll positionID), close
  execution/__init__.py         ← exports OrderRouter, OutsideTradingHoursError
  utils/time_utils.py           ← is_trading_hours(), seconds_until_open()
                                   PHASE 2: add is_valid_trading_window(dt) for WBWS+ filter
configs/broker_support/
  instrument_map.yaml           ← DAX: {instrument_id: 32, symbol_full: GER40}
  broker_settings.env           ← ETORO_API_KEY, ETORO_USER_KEY (Demo Write required)
```
### OpenPosition aliases (FIXED 2026-03-12 — confirmed live)
```python
positionID   → position_id    # PascalCase + capital ID
instrumentID → instrument_id  # PascalCase + capital ID
isBuy        → is_buy
openDateTime → open_date_time
# isNoStopLoss: true = DISABLED; isNoTakeProfit: true = DISABLED (inverted)
```
### What NOT to do
- Do NOT use `/trading/info/portfolio` (no /demo/) — wrong for Demo key → 403
- Do NOT use 'InstrumentId' (lowercase d) in close body — must be 'InstrumentID'
- Do NOT call search without 'fields' param — returns empty
- Do NOT assume positionID is in the open-order response — poll order info
- Do NOT use 'from'/'fromDate' for trade history — correct param is 'minDate'
- Do NOT use Read-only key for fetch_closed_trades — 403
- Do NOT refactor _make_request() — solid, do not touch
- Do NOT confuse 'credit' (/demo/portfolio) with 'credits' (/demo/pnl)
- Do NOT set LIVE_APPROVED in code — operator-only
---
## Phase 2 — Graduated Automation Plan
### Stage sequence
```
Stage 1: Signal validation — print only, no trades
Stage 2: 1 trade manually via run_signal.py → confirm journal → full review
Stage 3: 3-trade automation batch → analysis
Stage 4: Full loop with abort conditions
```
### Stage 4 abort conditions
```python
max_open_positions = 3          # halt if exceeded
min_available_cash = 200.0      # USD — halt if below
max_consecutive_losses = 5      # halt streak
hours_guard = True              # 08:00-22:00 CET (time_utils)
trading_window_filter = True    # WBWS+ hour filter
kill_switch_file = 'STOP'       # create this file to halt loop
```
### First script to build
`scripts/broker_support/run_signal.py`:
- Instantiate EToroClient, InstrumentResolver, OrderRouter
- Call `router.open_position('DAX', 'SELL', amount=60.0, leverage=20)`
- Print returned positionID
- Confirm in inspect_portfolio.py output
---
## WBWS+ Strategy Filter
### Analysis results (2026-03-12, full history 1,498 trades)
**Consistent across both full history AND last 3 months (reliable filters):**
- London session (08–16 UTC): 74–80% of all profit — protect this window
- Hour 17 UTC: consistently negative both periods → skip
- Hour 11, 14, 16 UTC: positive in both periods → prioritise
**Regime-dependent (do NOT hard-filter):**
- Monday: -493 pts full history BUT +287 pts last 3 months → reduce size, not skip
- Wednesday: +179 pts full history BUT -213 pts recent → monitor
- Hour 10, 13 UTC: opposite sign across periods → no filter
**Proposed WBWS+ filter config:**
```yaml
# strategy YAML or broker_settings.env
allowed_hours_utc: [9, 10, 11, 12, 13, 14, 15, 16]   # skip 17–18 UTC
session_preference: london                              # primary focus
monday_size_reduction: 0.5                             # half size on Monday (optional)
```
**Expected impact (conservative estimate from full history):**
- Skipping hours 17–18: removes ~320 trades, saves ~+730 pts of losses
- Profit factor improves from 1.08 → estimated 1.3–1.5 range
- Drawdown reduction: meaningful but not modelled yet
**Implementation location:** `utils/time_utils.py` → `is_valid_trading_window(signal_dt)`
---
## CLOSED TRACK — Backtesting (reference only)
### Paper trade candidates (run b651ec5c)
| Rank | Candidate | Status | WFO | Ruin |
|------|-----------|--------|-----|------|
| 1 | c424a0e04327 | PRIMARY | 0.8108 | 0.000 |
| 2 | 20745ca991be | SECONDARY | 0.7201 | 0.054 |
| 3 | c42f8b009283 | MONITOR | 0.6473 | 0.000 |
| 4 | c209820886c8 | SECONDARY MONITOR | 0.5699 | 0.000 |
Do NOT promote c209820886c8 above c42f8b009283 — hard cliff on atr_multiplier (+1 step).
Trading YAMLs: `outputs/backtesting/trading_yamls/b651ec5c_<id>_strategy.yaml`
**Frozen constants**
```python
_SIGMOID_SCALE = 310.0        # NOT 359.4, NOT 221.1
_MAX_EXPECTED_DRAWDOWN = 2_500.0
max_workers = 2               # OOM at 6 — mandatory
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
# Enrichment: TradeEnricher searches up to 10 pages (1000 trades, 90-day window)
# Trading hours: gate execution via is_trading_hours() — 08:00-22:00 CET/CEST
```
## Platform
- OS: Windows 10, Python 3.13.12
- Timezone: OHLCV/signals CET/CEST; pipeline timestamps UTC
- Project: `E:\Trading\Backtest_platform`
- API base: `https://public-api.etoro.com/api/v1`
- Credentials: `configs/broker_support/broker_settings.env`
- CTP API reference: `docs/ctp/BROKER_INTEGRATION.md`
- Full API reference: `docs/ctp/API_REFERENCE.md`
## Session Deliverables (end of every session)
- Updated `docs/backtesting/CONTEXT.md`
- Updated `SKILL.md` in outputs/ (replace user skill)
- `docs/ctp/BROKER_INTEGRATION.md` if API findings changed
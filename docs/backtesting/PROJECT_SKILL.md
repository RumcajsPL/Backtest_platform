---
name: backtester-project
description: >
  Use this skill whenever working on the Backtesting & Optimization Framework project
  OR the broker_support / eToro API integration project. Triggers: any mention of
  backtester, backtest pipeline, CandidateStore, GA engine, WFO evaluator, Monte Carlo
  engine, fitness evaluator, scenario profile, backtest_template.yaml, sensitivity
  evaluator, verdict engine, report generator, any module from src/backtesting/,
  broker_support, EToroClient, PositionTracker, CSVJournal, paper trading automation,
  eToro API, signal bridge, or CTP roadmap.
  Read this SKILL.md before writing any code, creating any file, or making any design
  decision for this project.
---
# CTP Project Skill — Backtesting + Broker Integration

## Project Status (2026-03-12, Block 9P+1 end)
```
BACKTESTING ENGINE:    V1 PRODUCTION — PHASE 1 GATE FULLY CLOSED (2026-03-12)
                       Confirmation run b651ec5c complete. All 10 MC candidates
                       have verdicts. Paper trade candidate list finalised.
                       No further backtesting runs planned. Engine frozen.

BROKER INTEGRATION:    Phase 0 COMPLETE (Steps 1–4 done, 37 tests passing).
                       Step 5 (signal bridge / order router) = next active work.
                       Tracker loop running and confirmed end-to-end on live API.

CTP ROADMAP:           Phase 0 (broker fixes + tracker) = DONE.
                       Phase 2 = automated paper trading (Step 5 unblocks this).
                       V2 architecture blueprint complete (deferred to Phase 3).
```

---
## ACTIVE TRACK — broker_support (Phase 0 → Step 5)

### Project path
`E:\Trading\Backtest_platform`
Package: `src/broker_support/` (installed editable in venv)

### Confirmed working (37/37 tests passing as of 2026-03-12)
- `EToroClient._make_request()` — do NOT refactor
- `EToroClient.get_portfolio()` — endpoint `/trading/info/portfolio` (empirically confirmed)
- `EToroClient.fetch_closed_trades()` — requires Read+Write key; `minDate=YYYY-MM-DD`
- `EToroClient.search_instrument()` — uses `searchText` param (NOT `internalSymbolFull`)
- `InstrumentResolver` — YAML primary (DAX=32), API fallback
- `TradeEnricher` — RESULT A path: searches history by positionId, up to 10 pages/1000 trades
- `PositionTracker.track()` — snapshot diff → enrich → journal write
- `CSVJournal` — dedup on trade_id, correct header-on-empty logic
- `run_tracker_loop.py` — polling loop with trading hours guard (08:00–22:00 CET)
- `time_utils.py` — `is_trading_hours()`, `seconds_until_open()`

### Empirically confirmed API facts (overrule any docs that contradict)
```
Portfolio endpoint:     GET /api/v1/trading/info/portfolio  (NO /demo/ prefix)
                        Returns { clientPortfolio: { credit, positions, orders, mirrors } }
                        /demo/pnl → 403, /demo/portfolio → 403 for all key types
                        Key environment (Virtual/Real) determines which account — not URL prefix

Trade history:          GET /api/v1/trading/info/trade/history?minDate=YYYY-MM-DD
                        Requires Read+Write key (Read-only → 403 confirmed)
                        Demo trades appear here (RESULT A confirmed)
                        Returns array of trade objects directly (not wrapped)

Instrument search:      GET /api/v1/market-data/search?searchText=GER40&fields=...
                        Use 'searchText' for fuzzy search (NOT 'internalSymbolFull')
                        internalSymbolFull as query param → always empty result

DAX instrument:         instrumentId=32, symbolFull="GER40"
                        Confirmed via /market-data/instruments?instrumentIds=32

Execution endpoints     (Step 5 — not yet implemented, stubs raise NotImplementedError):
  Open order:  POST /api/v1/trading/execution/demo/market-open-orders/by-amount
               Body PascalCase: { InstrumentID, IsBuy, Leverage, Amount }
               Optional: StopLossRate, TakeProfitRate (absolute price levels, not distances)
  Close order: POST /api/v1/trading/execution/demo/market-close-orders/positions/{positionId}
               Body: { "InstrumentId": ..., "UnitsToDeduct": null }  ← null = full close
```

### Key type requirement
`fetch_closed_trades()` requires a **Read+Write** key. The Read-only key returns 403.
Set ETORO_USER_KEY in `configs/broker_support/broker_settings.env` to the Write key.

### Package structure (complete as of Step 4)
```
src/broker_support/
  __init__.py
  cli.py
  client/client.py            ← EToroClient — do not refactor _make_request()
  config/settings.py          ← Pydantic settings, loads from broker_settings.env
  models/trade.py             ← Trade model (positionId alias, direction derived from isBuy)
  models/portfolio.py         ← OpenPosition, PortfolioSummary Pydantic models
  tracking/csv_journal.py     ← CSVJournal (dedup, header-on-empty)
  tracking/position_tracker.py ← PositionTracker (snapshot diff + enrich + journal)
  enrichment/instrument_resolver.py ← YAML primary + API fallback
  enrichment/trade_enricher.py      ← RESULT A: history search by positionId
  execution/                  ← placeholder — Step 5
  utils/time_utils.py         ← is_trading_hours(), seconds_until_open()

configs/broker_support/
  instrument_map.yaml         ← DAX: {instrument_id: 32, symbol_full: GER40}
  broker_settings.env         ← ETORO_API_KEY, ETORO_USER_KEY (Write key required)

scripts/broker_support/
  run_tracker.py              ← single-cycle manual run
  run_tracker_loop.py         ← Step 4: polling loop (5 min interval, hours guard)
  run_demo_history_test.py    ← empirical test (COMPLETE — RESULT A)
  inspect_portfolio.py        ← diagnostic (run when positions > 0)
  inspect_instruments.py      ← diagnostic (uses searchText)
  probe_demo_endpoints.py     ← diagnostic (COMPLETE)

tests/broker_support/
  conftest.py                 ← patches env vars, no real API calls
  test_models.py              ← 8 tests
  test_csv_journal.py         ← 7 tests
  test_position_tracker.py    ← 11 tests
  test_time_utils.py          ← 11 tests

outputs/broker_support/
  journal/trades.csv          ← closed trades journal
  snapshots/last_positions.csv ← position snapshot
  logs/tracker_YYYY-MM-DD.log ← daily rotating log (30-day retention)
```

### Trade model confirmed schema (from live API)
```python
# API field        → model field         type/notes
positionId         → trade_id:           str  (coerced from int via field_validator)
instrumentId       → instrument_id:      int
isBuy              → direction:          str  ('BUY'/'SELL', derived in model_validator)
openTimestamp      → open_time:          datetime
closeTimestamp     → close_time:         datetime
openRate           → entry_price:        float
closeRate          → exit_price:         float
investment         → volume:             float
units              → units:              float
netProfit          → profit_loss:        float
fees               → fees:              float  (default 0.0)
leverage           → leverage:           int
stopLossRate       → sl_rate:           Optional[float]
takeProfitRate     → tp_rate:           Optional[float]
trailingStopLoss   → trailing_stop_loss: bool
```

### Step 5 — Signal Bridge (next session)
Implement `execution/order_router.py`:
- `OrderRouter.open_position(signal: TradingSignal) -> str` (returns positionId)
- `OrderRouter.close_position(position_id: str, instrument_id: int) -> bool`
- Read signal from strategy YAML (or from a live signal file/queue — TBD)
- Wire to `EToroClient.place_market_order()` and `EToroClient.close_position()`
- Implement those two client stubs (currently raise NotImplementedError)
- Gate: only execute during `is_trading_hours()`
- Before implementing: open a demo trade on eToro, run `inspect_portfolio.py` to
  confirm OpenPosition field names match Pydantic model aliases

### What NOT to do (broker_support)
- Do NOT call GET /demo/pnl or /demo/portfolio — both 403 for all key types
- Do NOT use 'internalSymbolFull' as query param for instrument search — use 'searchText'
- Do NOT use 'from'/'fromDate' for trade history — correct param is 'minDate'
- Do NOT use Read-only key for fetch_closed_trades — returns 403
- Do NOT refactor _make_request() — it works, retry logic is solid
- Do NOT implement place_market_order / close_position until Step 5
- Do NOT guess portfolio field names — run inspect_portfolio.py with an open position first
- Do NOT set LIVE_APPROVED in code — operator-only manual action

---
## CLOSED TRACK — Backtesting (reference only)

### Final paper trade candidate list (run b651ec5c, confirmed 2026-03-12)

| Rank | Candidate | Status | WFO | Ruin | Notes |
|------|-----------|--------|-----|------|-------|
| 1 | c424a0e04327 | PRIMARY | 0.8108 | 0.000 | Spike on atr_multiplier (upward asymmetric) |
| 2 | 20745ca991be | SECONDARY | 0.7201 | 0.054 | Regime-dependent, high variance |
| 3 | c42f8b009283 | MONITOR | 0.6473 | 0.000 | Parameter fragility |
| 4 | c209820886c8 | SECONDARY MONITOR | 0.5699 | 0.000 | avg_equity=9370 (best MC), atr_multiplier cliff at +1 step |
| — | c4f0aea11a3e | DISCARD | 0.6233 | 0.000 | frac_pos=0.167 |
| — | 5d89157ad626 | NO_GO | — | 0.593 | |
| — | 2cd6f1886371 | NO_GO | — | 0.959 | |

Trading YAMLs: `outputs/backtesting/trading_yamls/b651ec5c_<candidate_id>_strategy.yaml`

**Key constants (frozen — do not modify)**
```python
_SIGMOID_SCALE: float = 310.0      # full-history track — CONFIRMED (not 359.4, not 221.1)
_MAX_EXPECTED_DRAWDOWN: float = 2_500.0
max_workers: int = 2                # MANDATORY — OOM at 6 workers confirmed
```

**Critical lessons (abbreviated — see CHANGE_LOG.md for full list L-01 through L-58)**
- Confirmation run b651ec5c: Stage 1 234/400 (58%) = identical to 63b85270 ✅
- VERDICT-BUG is CLOSED — verdict.py, orchestrator.py, candidate_store.py all correct
- RSI confirmed dead (zero sensitivity delta across 7+ runs) — remove in V2
- mc_prefilter: DISABLE for full-history (38-month) — false ruin confirmed
- Do NOT raise monte_carlo.deep.input_count for more verdicts — raise sensitivity.input_count
- Do NOT use _SIGMOID_SCALE=359.4 from run 63b85270 (includes GA samples)
- Do NOT set max_drawdown constraint for Stage 1 date ranges > 3 months

---
## Architecture Rules (non-negotiable — both tracks)
```python
# Contracts: always frozen dataclasses / Pydantic models — never raw dicts crossing boundaries
# Fail fast: invalid config raises at construction, no silent fallbacks
# Datetime: datetime.now(timezone.utc) — NEVER datetime.utcnow()
# Paths: pathlib.Path — never hardcoded separators
# Logging: logger.info/debug only — never print()
# Broker: _make_request() is the HTTP engine — never implement HTTP in public methods
# Enrichment: TradeEnricher searches history up to 10 pages (1000 trades, 90-day window)
# Trading hours: always gate execution via is_trading_hours() — 08:00-22:00 CET/CEST
```

---
## Platform
- **OS**: Windows 10, Python 3.13.12
- **Timezone**: OHLCV/signals CET/CEST; pipeline timestamps UTC
- **DB**: `outputs/backtesting/backtester.db` (read-only going forward)
- **Broker project path**: `E:\Trading\Backtest_platform`
- **API base**: `https://public-api.etoro.com/api/v1`
- **Credentials**: `configs/broker_support/broker_settings.env`

---
## Session Deliverables (end of every session)
- Updated `outputs/CONTEXT_<block>.md`
- Updated `SKILL.md` in outputs/ (replace user skill)
- `docs/ctp/BROKER_INTEGRATION.md` if API findings or architecture changed
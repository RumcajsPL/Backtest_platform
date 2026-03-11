# Broker Integration Project — broker_support
**Date**: 2026-03-08 (updated 2026-03-11)
**Project path**: `E:\Trading\` (integrated into main CTP project)
**Package**: `broker-support` v0.1.0
**Broker**: eToro (demo + real accounts via public API v1)
---
## Folder Structure
Full layout for `broker_support` integrated into the existing CTP project tree.
`[EXISTS]` = file/folder already created or confirmed working.
`[BUG]` = exists but requires a fix before use.
`[PLACEHOLDER]` = not yet written — reserved for a future development step.
```
E:\Trading\
│
├── configs\
│   ├── backtesting\                        [EXISTS] — backtester YAMLs
│   └── broker\
│       ├── broker_settings.env             [EXISTS] — API keys, account type flag
│       └── instrument_map.yaml             [PLACEHOLDER] Step 2 — DAX InstrumentID + others
│
├── docs\
│   └── ctp\
│       ├── BROKER_INTEGRATION.md           [EXISTS] — this file
│       └── CTP_ROADMAP.md                  [EXISTS] — 6-phase plan
│
├── outputs\
│   ├── backtesting\                        [EXISTS] — backtester DB, trading YAMLs, reports
│   └── broker\
│       ├── journal\
│       │   └── trades.csv                  [PLACEHOLDER] Step 1 — CSVJournal output
│       ├── snapshots\
│       │   └── positions_<timestamp>.json  [PLACEHOLDER] Step 1 — PositionTracker snapshots
│       └── logs\
│           └── tracker.log                 [PLACEHOLDER] Step 4 — polling loop log file
│
├── scripts\
│   ├── runners\                            [EXISTS] — backtester runner scripts
│   └── broker\
│       ├── run_tracker.py                  [EXISTS/BUG] Step 1 — one-shot tracker (fix bugs first)
│       ├── run_tracker_loop.py             [PLACEHOLDER] Step 4 — polling loop (--once flag)
│       ├── run_instrument_lookup.py        [PLACEHOLDER] Step 2 — find DAX InstrumentID
│       └── run_signal_bridge.py            [PLACEHOLDER] Step 5 — execute paper trade YAMLs
│
├── src\
│   ├── backtesting\                        [EXISTS] — full backtesting engine
│   └── broker_support\
│       │
│       ├── __init__.py                     [EXISTS]
│       ├── settings.py                     [EXISTS] — pydantic-settings, env-based config
│       │
│       ├── client\
│       │   ├── __init__.py                 [EXISTS]
│       │   ├── client.py                   [EXISTS/BUG] — EToroClient, _make_request, bugs 1-3
│       │   └── exceptions.py               [PLACEHOLDER] — EToroAPIError, RateLimitError, etc.
│       │
│       ├── models\
│       │   ├── __init__.py                 [EXISTS]
│       │   ├── trade.py                    [EXISTS/BUG] — Trade Pydantic model (bug 4 alias)
│       │   ├── portfolio.py                [EXISTS] — clientPortfolio / open position model
│       │   └── order.py                    [PLACEHOLDER] Step 5 — MarketOrder request/response
│       │
│       ├── tracking\
│       │   ├── __init__.py                 [EXISTS]
│       │   ├── position_tracker.py         [EXISTS] — snapshot-comparison logic (correct)
│       │   └── csv_journal.py              [EXISTS] — CSVJournal with deduplication (correct)
│       │
│       ├── enrichment\
│       │   ├── __init__.py                 [PLACEHOLDER]
│       │   ├── trade_enricher.py           [PLACEHOLDER] Step 3 — close price + PnL fill
│       │   └── instrument_resolver.py      [PLACEHOLDER] Step 2 — InstrumentID → symbol map
│       │
│       ├── execution\
│       │   ├── __init__.py                 [PLACEHOLDER]
│       │   ├── order_router.py             [PLACEHOLDER] Step 5 — place/close demo orders
│       │   └── signal_mapper.py            [PLACEHOLDER] Step 5 — YAML params → API fields
│       │
│       └── utils\
│           ├── __init__.py                 [PLACEHOLDER]
│           ├── time_utils.py               [PLACEHOLDER] — trading hours guard (08:00-22:00 CET)
│           └── rate_limiter.py             [PLACEHOLDER] — request throttle wrapper
│
└── tests\
    ├── backtesting\                        [EXISTS] — backtester test suite
    └── broker_support\
        ├── __init__.py                     [EXISTS]
        ├── test_client.py                  [EXISTS] — connection + endpoint tests
        ├── test_models.py                  [PLACEHOLDER] — Trade/Portfolio model validation
        ├── test_position_tracker.py        [PLACEHOLDER] — snapshot diff logic
        ├── test_csv_journal.py             [PLACEHOLDER] — deduplication, write/read round trip
        ├── test_trade_enricher.py          [PLACEHOLDER] Step 3 — enrichment path (mock API)
        └── test_signal_bridge.py           [PLACEHOLDER] Step 5 — order placement (mock API)
```
---
## Migration — Existing Files
Files currently held in the temporary folder. Move to the locations above before starting Phase 0.
| Current file | Move to |
|---|---|
| `client.py` | `src/broker_support/client/client.py` |
| `trade.py` | `src/broker_support/models/trade.py` |
| `portfolio.py` (if exists) | `src/broker_support/models/portfolio.py` |
| `position_tracker.py` | `src/broker_support/tracking/position_tracker.py` |
| `csv_journal.py` | `src/broker_support/tracking/csv_journal.py` |
| `settings.py` | `src/broker_support/settings.py` |
| `run_tracker.py` | `scripts/broker/run_tracker.py` |
| `broker_settings.env` | `configs/broker/broker_settings.env` |
Do **not** move files while bugs are unfixed — move and fix in the same commit so the
post-migration state is clean from the start.
---
## Development Sequence (5 Steps)
### Step 1 — Fix and validate (Phase 0)
Apply all four bug fixes below. Move files to final locations. Run one complete tracker
cycle manually. Verify portfolio endpoint returns correctly. Run empirical demo history test.
### Step 2 — Instrument ID map (Phase 0 tail)
Query `GET /api/v1/market/instruments?searchTerm=DAX` to find the DAX InstrumentID.
Build `configs/broker/instrument_map.yaml` and `src/broker_support/enrichment/instrument_resolver.py`.
### Step 3 — Close price enrichment
Depends on Step 1 empirical test result. Either:
- **History endpoint works for demo**: add `get_trade_details(position_id)` to `EToroClient`,
  call it in `trade_enricher.py` to fill `exit_price` and `profit_loss`
- **History is real-only**: add `get_current_price(instrument_id)` using
  `GET /api/v1/market/rates`, call at closure detection time as approximation
Implement in `src/broker_support/enrichment/trade_enricher.py`.
### Step 4 — Reliable tracker loop
Convert `run_tracker.py` to `run_tracker_loop.py` — proper polling loop, every 5 minutes
during DAX trading hours (08:00–22:00 CET), graceful reconnection, `--once` flag to
preserve manual run behaviour. Trading hours guard lives in `utils/time_utils.py`.
### Step 5 — Signal bridge
Add `place_order()` to `EToroClient` using:
```
POST /api/v1/trading/execution/demo/market-orders-by-amount
```
`src/broker_support/execution/signal_mapper.py` maps candidate YAML parameters to API fields:
```
atr_multiplier + current_atr → stopLossRate   (absolute price, not distance)
rr_target × SL_distance      → takeProfitRate  (absolute price, not distance)
risk_percentile × balance     → Amount          (USD investment)
```
`src/broker_support/execution/order_router.py` handles place/close lifecycle.
`scripts/broker/run_signal_bridge.py` reads paper trade YAMLs and executes.
---
## Four Bugs to Fix Before Any New Development
### Bug 1 — Wrong portfolio endpoint
`get_portfolio()` calls `/api/v1/trading/info/demo/portfolio`.
Correct endpoint from official OpenAPI spec is `/api/v1/trading/info/demo/pnl`.
```python
# client.py — change:
result = self._make_request('GET', 'api/v1/trading/info/demo/pnl')
```
### Bug 2 — Orphaned function in client.py
Second `fetch_closed_trades` definition is a free function (not indented inside `EToroClient`).
The class holds the first stub version; the second proper version is unreachable dead code.
Fix: delete first stub version inside the class, properly indent second version as class method.
### Bug 3 — Wrong date parameter name
`fetch_closed_trades` trial-loops `from`/`fromDate`/`from_date`. Confirmed param is `minDate`.
```python
# client.py — change:
params = {'minDate': from_date.strftime('%Y-%m-%d')}
result = self._make_request('GET', 'api/v1/trading/info/trade/history', params=params)
```
### Bug 4 — Trade model field alias mismatch + missing fields
`Trade` declares `Field(..., alias='id')` but eToro returns `positionId`. Missing fields.
```python
# src/broker_support/models/trade.py — replace Trade model:
class Trade(BaseModel):
    trade_id: str                    = Field(..., alias='positionId')
    instrument_id: int               = Field(..., alias='instrumentId')
    instrument: Optional[str]        = None                              # populated post-lookup
    direction: str                                                        # derived from isBuy
    open_time: datetime              = Field(..., alias='openTimestamp')
    close_time: datetime             = Field(..., alias='closeTimestamp')
    entry_price: float               = Field(..., alias='openRate')
    exit_price: float                = Field(..., alias='closeRate')
    volume: float                    = Field(..., alias='investment')
    profit_loss: float               = Field(..., alias='netProfit')
    fees: float                      = Field(default=0.0, alias='fees')
    leverage: int                    = Field(default=1, alias='leverage')
    sl_rate: Optional[float]         = Field(default=None, alias='stopLossRate')
    tp_rate: Optional[float]         = Field(default=None, alias='takeProfitRate')
```
---
## Critical Open Question — Demo Trade History
The eToro API documentation does **not** include a demo-account equivalent of the real-account
history endpoint (`/api/v1/trading/info/trade/history`). The demo API only exposes current
portfolio state (`/demo/pnl`).
**Test empirically after Bug 1–3 fixes:**
```python
result = client._make_request(
    'GET', 'api/v1/trading/info/trade/history',
    params={'minDate': '2026-01-01'}
)
```
**If demo trades appear**: close-price enrichment is straightforward — query by `positionId`
after snapshot detects a closure. `trade_enricher.py` uses `get_trade_details()`.
**If they do not**: `PositionTracker` snapshot approach is permanent for demo accounts.
Close price approximated from `GET /api/v1/market/rates?instrumentIds={id}` at detection time.
Do **not** architect close-price enrichment before this test result is known.
---
## What Exists and Works
- `EToroClient._make_request()` — core HTTP engine with session reuse, UUID per request,
  masked key logging, tenacity retry. Do not refactor.
- `EToroClient.test_connection()` via `GET /api/v1/watchlists` — 200 confirmed, keys valid.
- Portfolio fetch returning `clientPortfolio` structure with open positions.
- `PositionTracker` snapshot-comparison logic — architecturally correct.
- `CSVJournal` with deduplication — clean and reusable as-is.
- `Trade` Pydantic model — correct pattern, needs Bug 4 field fixes only.
- `settings.py` with `pydantic-settings` — perfect, do not change.
**Key architectural finding**: demo and real account endpoints are structurally identical —
same schemas, same auth, different path prefix (`/demo/` vs `/`). Every line written for
demo paper trading is production code. Transition to live = one config flag, not a code change.
---
## API Endpoints — Confirmed Reference
| Operation | Method | Path | Key params |
|-----------|--------|------|------------|
| Demo portfolio + PnL | GET | `/api/v1/trading/info/demo/pnl` | headers only |
| Real trade history | GET | `/api/v1/trading/info/trade/history` | `minDate` (required, YYYY-MM-DD) |
| Open demo order (by amount) | POST | `/api/v1/trading/execution/demo/market-orders-by-amount` | InstrumentID, IsBuy, Amount, Leverage, StopLossRate, TakeProfitRate |
| Close demo position | POST | `/api/v1/trading/execution/demo/close-position` | positionId |
| Market rates (current price) | GET | `/api/v1/market/rates` | `instrumentIds` |
| Instrument search | GET | `/api/v1/market/instruments` | `searchTerm` |
| Test connection | GET | `/api/v1/watchlists` | headers only |
Authentication headers (all requests):
```
x-api-key:    <public API key>
x-user-key:   <user key from Settings > Trading>
x-request-id: <UUID, unique per request>
```
---
## Architecture Principles
**Keep unchanged** — solid and reusable:
- `settings.py` with `pydantic-settings`
- `EToroClient._make_request()` core
- `CSVJournal`
- `PositionTracker` snapshot comparison logic
**Not started yet** (Phase 3+ scope, do not anticipate):
- WebSocket price feed (intra-bar SL hit detection)
- Live vs backtest performance comparison dashboard
- Circuit breaker / drawdown gate
- Multi-instrument support beyond DAX
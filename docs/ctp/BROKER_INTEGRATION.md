# BROKER_INTEGRATION.md
# eToro Broker Integration — CTP Phase 0→2 Development Plan
# Last updated: 2026-03-12 (Block 9P+1 — Phase 0 complete)
---
## Status

| Step | Description | Status |
|------|-------------|--------|
| Step 1 | Client fixes + empirical demo history test | ✅ COMPLETE |
| Step 2 | Instrument resolver (YAML + API fallback) | ✅ COMPLETE |
| Step 3 | Trade enricher (exit price + P&L) | ✅ COMPLETE |
| Step 4 | Tracker loop (polling daemon + hours guard) | ✅ COMPLETE |
| Step 5 | Signal bridge / order router | 🔲 NEXT |
---
## Empirically Confirmed API Facts
> These override any documentation that contradicts them.
> All findings from live testing on 2026-03-12.
### Authentication
- Headers required on every request: `x-api-key`, `x-user-key`, `x-request-id` (UUID)
- Key environment (Virtual/Real) determines which account's data is returned
- The `/demo/` URL prefix is ONLY used for **execution** endpoints, not info endpoints
- `fetch_closed_trades()` requires a **Read+Write** key — Read-only returns 403
### Confirmed working endpoints

| Endpoint | Method | Notes |
|----------|--------|-------|
| `/api/v1/watchlists` | GET | Connection test |
| `/api/v1/trading/info/portfolio` | GET | Portfolio + open positions (no /demo/ prefix) |
| `/api/v1/trading/info/real/pnl` | GET | Also works, same response structure |
| `/api/v1/trading/info/trade/history` | GET | Requires Write key. `minDate=YYYY-MM-DD`. Demo trades appear here. |
| `/api/v1/market-data/search` | GET | Use `searchText` param for fuzzy search |
| `/api/v1/market-data/instruments` | GET | Filter by `instrumentIds` (comma-separated) |
| `/api/v1/market-data/instruments/rates` | GET | Current prices by `instrumentIds` |
### Confirmed broken endpoints (all 403)
| Endpoint | Why broken |
|----------|------------|
| `/api/v1/trading/info/demo/pnl` | Does not exist for any key type |
| `/api/v1/trading/info/demo/portfolio` | Does not exist for any key type |
### Portfolio response structure
```json
{
  "clientPortfolio": {
    "credit": 72.24,
    "positions": [],
    "orders": [],
    "mirrors": []
  }
}
```
### Trade history response structure
Returns array directly (not wrapped):
```json
[
  {
    "positionId": 123456789,
    "instrumentId": 32,
    "isBuy": true,
    "openTimestamp": "2026-03-01T10:00:00",
    "closeTimestamp": "2026-03-01T14:00:00",
    "openRate": 22150.0,
    "closeRate": 22280.0,
    "investment": 500.0,
    "units": 0.023,
    "netProfit": 130.0,
    "fees": 2.5,
    "leverage": 5,
    "stopLossRate": 22000.0,
    "takeProfitRate": 22500.0,
    "trailingStopLoss": false
  }
]
```
### Instrument search
```
# WRONG — always returns empty:
GET /market-data/search?internalSymbolFull=GER40&fields=instrumentId,...
# CORRECT — use searchText:
GET /market-data/search?searchText=GER40&fields=instrumentId,internalSymbolFull,displayname
```
### DAX instrument
- `instrumentId`: 32
- `symbolFull`: "GER40"
- Confirmed via `GET /market-data/instruments?instrumentIds=32`
---
## Execution Endpoints (Step 5 — not yet implemented)
### Open demo market order (by amount)
```
POST /api/v1/trading/execution/demo/market-open-orders/by-amount
Content-Type: application/json
{
  "InstrumentID": 32,
  "IsBuy": true,
  "Leverage": 5,
  "Amount": 500,
  "StopLossRate": 22000.0,    // optional — absolute price level
  "TakeProfitRate": 22500.0   // optional — absolute price level
}
```
### Close demo position (full close)
```
POST /api/v1/trading/execution/demo/market-close-orders/positions/{positionId}
Content-Type: application/json

{
  "InstrumentId": 32,
  "UnitsToDeduct": null    // null = full close; number = partial close
}
```
---
## Package Structure
```
src/broker_support/
  __init__.py
  cli.py
  client/
    __init__.py
    client.py              ← EToroClient — _make_request() is core, do not refactor
  config/
    __init__.py
    settings.py            ← Pydantic settings, loads broker_settings.env
  models/
    __init__.py
    trade.py               ← Trade (positionId alias, direction from isBuy)
    portfolio.py           ← OpenPosition, PortfolioSummary
  tracking/
    __init__.py
    csv_journal.py         ← CSVJournal (dedup, header-on-empty)
    position_tracker.py    ← PositionTracker (snapshot diff + enrich + journal)
  enrichment/
    __init__.py
    instrument_resolver.py ← YAML primary + API fallback
    trade_enricher.py      ← RESULT A: history search by positionId
  execution/               ← Step 5
    __init__.py
    order_router.py        ← OrderRouter (to be implemented)
  utils/
    __init__.py
    time_utils.py          ← is_trading_hours(), seconds_until_open()
configs/broker_support/
  instrument_map.yaml      ← DAX: {instrument_id: 32, symbol_full: GER40}
  broker_settings.env      ← ETORO_API_KEY, ETORO_USER_KEY (Write key required)
scripts/broker_support/
  run_tracker.py           ← single-cycle manual run
  run_tracker_loop.py      ← Step 4: polling loop (5 min, hours guard)
  run_demo_history_test.py ← COMPLETE — do not re-run unless key changes
  inspect_portfolio.py     ← diagnostic — run with an open demo trade
  inspect_instruments.py   ← diagnostic — uses searchText
tests/broker_support/
  conftest.py
  test_models.py           ← 8 tests
  test_csv_journal.py      ← 7 tests
  test_position_tracker.py ← 11 tests
  test_time_utils.py       ← 11 tests
outputs/broker_support/
  journal/trades.csv
  snapshots/last_positions.csv
  logs/tracker_YYYY-MM-DD.log
```
---
## Trade Model — Field Mapping
```python
# API field (from trade history response) → Python model field
positionId:       str    alias='positionId'  (int coerced to str via field_validator)
instrumentId:     int    alias='instrumentId'
isBuy:            bool   → direction: str  ('BUY'/'SELL', derived in model_validator)
openTimestamp:    str    alias='openTimestamp'  → open_time: datetime
closeTimestamp:   str    alias='closeTimestamp' → close_time: datetime
openRate:         float  alias='openRate'   → entry_price
closeRate:        float  alias='closeRate'  → exit_price
investment:       float  alias='investment' → volume
units:            float  alias='units'
netProfit:        float  alias='netProfit'  → profit_loss
fees:             float  alias='fees'       (default 0.0)
leverage:         int    alias='leverage'
stopLossRate:     float  alias='stopLossRate' → sl_rate (Optional)
takeProfitRate:   float  alias='takeProfitRate' → tp_rate (Optional)
trailingStopLoss: bool   alias='trailingStopLoss'
```
---
## Key Architecture Decisions
### RESULT A — Trade enrichment path (confirmed)
Demo trades appear in `GET /trading/info/trade/history` with a Write key.
`TradeEnricher` searches history by `positionId`, up to 10 pages (1000 trades),
looking back 90 days. This is the permanent enrichment path — no approximation needed.
### PositionTracker — snapshot diff
Snapshot-diff approach is retained as primary even though RESULT A enables direct query.
Reason: snapshot diff detects position closure even if enrichment fails (graceful degradation).
Flow: fetch portfolio → diff vs last snapshot → detect closed positions → enrich via history → journal write.
### Trading hours guard
`is_trading_hours()` checks 08:00 ≤ time < 22:00 Europe/Berlin (CET/CEST auto via zoneinfo).
Applies to: polling loop sleep logic. Step 5 order execution must also gate on this.
### Key environment vs endpoint
There is no demo-specific info endpoint. The Virtual key accesses the same endpoints
as the Real key. The key's environment setting determines which account data is returned.
Only execution endpoints use the `/demo/` path prefix.
---
## Step 5 — Signal Bridge Implementation Plan
### Goal
Given a signal (direction, instrument, amount, leverage), open or close a demo trade.
### OrderRouter contract
```python
class OrderRouter:
    def __init__(self, client: EToroClient, resolver: InstrumentResolver): ...

    def open_position(
        self,
        symbol: str,           # e.g. 'DAX'
        direction: str,        # 'BUY' or 'SELL'
        amount: float,         # EUR amount
        leverage: int = 1,
        stop_loss_rate: Optional[float] = None,   # absolute price
        take_profit_rate: Optional[float] = None, # absolute price
    ) -> str:  # returns positionId as str
        ...
    def close_position(
        self,
        position_id: str,
        instrument_id: int,
        units_to_deduct: Optional[float] = None,  # None = full close
    ) -> bool:
        ...
```
### Implementation checklist
- [ ] Implement `EToroClient.place_market_order()` (remove NotImplementedError stub)
- [ ] Implement `EToroClient.close_position()` (remove NotImplementedError stub)
- [ ] Create `execution/order_router.py` with `OrderRouter`
- [ ] Gate execution on `is_trading_hours()` — raise or return False outside hours
- [ ] Add `tests/broker_support/test_order_router.py` (mock client, no live calls)
- [ ] Run `inspect_portfolio.py` with an open demo trade to verify OpenPosition fields
### Prerequisite
Open a demo trade on eToro manually, then run:
```powershell
python scripts/broker_support/inspect_portfolio.py
```
Confirm that the `positions[0]` field names match `OpenPosition` model aliases before
writing any OrderRouter code that depends on position field names.
---
## What NOT to do
- Do NOT call GET `/demo/pnl` or `/demo/portfolio` — both 403 for all key types
- Do NOT use `internalSymbolFull` as a query param — use `searchText` for instrument search
- Do NOT use `from` or `fromDate` as trade history params — use `minDate=YYYY-MM-DD`
- Do NOT use a Read-only key for `fetch_closed_trades` — returns 403
- Do NOT refactor `_make_request()` — it works, retry logic is solid
- Do NOT implement execution stubs until Step 5 and OpenPosition fields verified
- Do NOT hardcode `/demo/` prefix in info endpoints — Virtual key serves same endpoints as Real
- Do NOT set `deployment_status = LIVE_APPROVED` in code — operator-only action
- Do NOT use `print()` — use `logger.info`
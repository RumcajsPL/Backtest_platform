# Broker Integration Project — broker_support
**Date**: 2026-03-08
**Project path**: `E:\Trading\Broker_support`
**Package**: `broker-support` v0.1.0
**Broker**: eToro (demo + real accounts via public API v1)

---

## What Exists and Works

The project is a Python package with proper structure, dependency management (`pyproject.toml`), and environment-based configuration (`pydantic-settings`). A working connection to the eToro API has been confirmed.

**Confirmed working today**:
- `EToroClient._make_request()` — core HTTP engine with session reuse, UUID per request, masked key logging, tenacity retry
- `EToroClient.test_connection()` via `GET /api/v1/watchlists` — returns 200, API keys confirmed valid
- Portfolio fetch returning `clientPortfolio` structure with open positions
- `PositionTracker` snapshot-comparison logic — architecturally correct
- `CSVJournal` with deduplication — clean and reusable
- `Trade` Pydantic model — correct pattern, needs minor field fixes

**Key finding from eToro official OpenAPI spec**:
The demo and real account trading endpoints are structurally identical — same schemas, same auth, different path prefix (`/demo/` vs `/`). Every line of code written for demo paper trading is production code for live trading. Transition to live requires only a configuration flag, not a code change.

---

## Three Bugs to Fix Before Any New Development

### Bug 1 — Wrong portfolio endpoint
`get_portfolio()` calls `/api/v1/trading/info/demo/portfolio`. The correct endpoint from the official OpenAPI spec is `/api/v1/trading/info/demo/pnl`.

Fix in `client.py`:
```python
result = self._make_request('GET', 'api/v1/trading/info/demo/pnl')
```

### Bug 2 — Orphaned function in client.py
The second `fetch_closed_trades` definition (lines ~80 onward in client.py) is accidentally a free function, not a class method — the `def` is not indented inside `EToroClient`. This causes the class to have the first (stub) version, and the second (proper) version to be unreachable dead code.

Fix: delete the first stub version inside the class, properly indent the second version as a class method.

### Bug 3 — Wrong date parameter name for trade history
`fetch_closed_trades` uses `from`, `fromDate`, `from_date` in a trial loop. The confirmed parameter name from the official spec is `minDate` (query param, required, date format `YYYY-MM-DD`).

Fix:
```python
params = {'minDate': from_date.strftime('%Y-%m-%d')}
result = self._make_request('GET', 'api/v1/trading/info/trade/history', params=params)
```

### Bug 4 — Trade model field alias mismatch
`Trade` model declares `trade_id: str = Field(..., alias='id')` but eToro returns `positionId`, not `id`. Also missing several fields the API provides.

Fix — update `trade.py`:
```python
class Trade(BaseModel):
    trade_id: str = Field(..., alias='positionId')
    instrument_id: int = Field(..., alias='instrumentId')   # raw; map to symbol separately
    instrument: Optional[str] = None                        # populated after instrument lookup
    direction: str                                          # derived from isBuy
    open_time: datetime = Field(..., alias='openTimestamp')
    close_time: datetime = Field(..., alias='closeTimestamp')
    entry_price: float = Field(..., alias='openRate')
    exit_price: float = Field(..., alias='closeRate')
    volume: float = Field(..., alias='investment')
    profit_loss: float = Field(..., alias='netProfit')
    fees: float = Field(default=0.0, alias='fees')
    leverage: int = Field(default=1, alias='leverage')
    sl_rate: Optional[float] = Field(default=None, alias='stopLossRate')
    tp_rate: Optional[float] = Field(default=None, alias='takeProfitRate')
```

---

## Critical Open Question — Demo Trade History

The eToro API documentation does **not** include a demo-account equivalent of the real-account history endpoint (`/api/v1/trading/info/trade/history`). The demo API only exposes current portfolio state (`/demo/pnl`).

**This must be tested empirically before further architecture decisions**:
```python
result = client._make_request(
    'GET', 'api/v1/trading/info/trade/history',
    params={'minDate': '2026-01-01'}
)
```

**If demo trades appear in the real-account history endpoint**: close-price enrichment is straightforward — query by `positionId` after snapshot detects a closure.

**If they do not**: the `PositionTracker` snapshot approach is permanent for demo accounts. Close price must be approximated from the market rates endpoint at the moment of detection (`GET /api/v1/market/rates?instrumentIds={id}`).

---

## Development Sequence (5 Steps)

### Step 1 — Fix and validate (1–2 days)
Apply all four bug fixes. Run one complete tracker cycle manually with `python scripts/run_tracker.py --verbose`. Verify at least the portfolio endpoint returns correctly. Run the empirical demo history test.

### Step 2 — Instrument ID map (1 day)
Call `GET /api/v1/market/instruments/search` or metadata endpoint to find the DAX InstrumentID. Build a small static mapping file or a cached lookup in `EToroClient`. This makes journal entries human-readable (instrument name instead of raw integer ID).

DAX on eToro is likely in the `indices` or `CFD` asset class. Use:
```python
GET /api/v1/market/instruments?searchTerm=DAX
```
Confirm the InstrumentID and add it to a constants file.

### Step 3 — Close price enrichment (1–2 days)
Depends on Step 1 empirical test result. Either:
- **If history endpoint works for demo**: add `get_trade_details(position_id)` to `EToroClient`, call it in `convert_to_trade()` to fill `exit_price` and `profit_loss`
- **If history is real-only**: add `get_current_price(instrument_id)` using `GET /api/v1/market/rates`, call it at closure detection time as an approximation

### Step 4 — Reliable tracker loop (1 day)
Convert `run_tracker.py` from a one-shot script to a proper polling loop. Run every 5 minutes during DAX trading hours (08:00–22:00 CET). Handle reconnection gracefully. Add `--once` flag to preserve existing manual run behaviour.

```python
# Simple loop structure
while True:
    tracker.track()
    time.sleep(300)  # 5 minutes
```

Keep it simple — a `while True` loop in a PowerShell window is sufficient for paper trading. Productionise later.

### Step 5 — Signal bridge stub (2–3 days)
Add `place_order()` to `EToroClient` using the demo execution endpoint:
```
POST /api/v1/trading/execution/demo/market-orders-by-amount
```

Map candidate YAML parameters to API order fields:
```
atr_multiplier + current_atr → stopLossRate (price, not distance)
rr_target × SL_distance     → takeProfitRate (price, not distance)
risk_percentile × account_balance → Amount (USD investment)
```

This is the step that connects the backtester's paper trade YAMLs to live order execution. Once working, `c4f0aea11a3e` and `da38ecc0ddc6` can be traded automatically.

---

## Architecture Principles

**Keep these unchanged** — they are solid and reusable:
- `settings.py` with `pydantic-settings` — perfect, will integrate cleanly with CTP
- `EToroClient._make_request()` core — working, do not refactor
- `CSVJournal` — clean, reusable as-is
- `PositionTracker` snapshot comparison logic — correct approach

**Not started yet** (Phase 3 scope):
- WebSocket price feed (needed for intra-bar SL hit detection)
- Live vs backtest performance comparison
- Circuit breaker / drawdown gate

---

## API Endpoints — Confirmed Reference

| Operation | Method | Path | Key Params |
|-----------|--------|------|------------|
| Demo portfolio + PnL | GET | `/api/v1/trading/info/demo/pnl` | headers only |
| Real trade history | GET | `/api/v1/trading/info/trade/history` | `minDate` (required) |
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
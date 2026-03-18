# BROKER_INTEGRATION.md — eToro Broker Integration Reference
# Empirical facts + flows used by CTP. Full API catalogue: docs/ctp/API_REFERENCE.md
# Updated: 2026-03-13
---
## Integration Status
| Component | Status |
|-----------|--------|
| Steps 1–5 (client, resolver, enricher, tracker, router) | ✅ COMPLETE |
| Phase 2 live pipeline | ✅ CONFIRMED 2026-03-16 |
| First live demo order | 🔲 Awaiting signal | - See CONTEXT.md for 1st order placed
---
## Authentication
| Header | Value |
|--------|-------|
| `x-api-key` | `ETORO_API_KEY` from `broker_settings.env` |
| `x-user-key` | `ETORO_USER_KEY` — **must be Demo Write key** |
| `x-request-id` | Fresh UUID per request |
Real key → 403 on ALL `/demo/` endpoints. Key type, not URL prefix, determines account.
---
## Endpoints Used by CTP
| Purpose | Method | Endpoint |
|---------|--------|----------|
| Portfolio snapshot | GET | `/trading/info/demo/portfolio` |
| Portfolio + PnL | GET | `/trading/info/demo/pnl` |
| Order status | GET | `/trading/info/demo/orders/{orderId}` |
| Open position | POST | `/trading/execution/demo/market-open-orders/by-amount` |
| Close position | POST | `/trading/execution/demo/market-close-orders/positions/{positionId}` |
| Trade history | GET | `/trading/info/trade/history` (Real Write key) |
| Instrument search | GET | `/market-data/search` |
| Historical candles | GET | `/market-data/instruments/{id}/history/candles/{dir}/{interval}/{count}` |
---
## Critical Field Casing
| Context | Style |
|---------|-------|
| Open/close request body | PascalCase + capital ID: `InstrumentID`, `IsBuy`, `Amount` |
| Portfolio positions | PascalCase + capital ID: `positionID`, `instrumentID` |
| `/portfolio` wrapper | camelCase: `credit` |
| `/pnl` wrapper | camelCase: `credits` ← different name |
| Trade history | camelCase + lowercase id: `positionId`, `instrumentId` |
| Candles outer wrapper | camelCase lowercase id: `instrumentId` |
| Candles inner objects | camelCase capital ID: `instrumentID` |
---
## Key Flows
### Two-Step Open Position
```
POST /trading/execution/demo/market-open-orders/by-amount
  Body: { InstrumentID, IsBuy, Leverage, Amount, [StopLossRate, TakeProfitRate] }
  → response.orderForOpen.orderID

GET /trading/info/demo/orders/{orderID}
  Poll until statusID == 1 (Executed)
  → response.positions[0].positionID   ← use for all close calls
```
statusID: 0=Pending, 1=Executed, 2=Cancelled, 3=Rejected, 4=Partial
⚠️ positionID is NOT in the open-order response — must poll.
### Close Position
```
POST /trading/execution/demo/market-close-orders/positions/{positionId}
Body: { "InstrumentID": <id>, "UnitsToDeduct": null }   ← null = full close
```
### Trade History
```
GET /trading/info/trade/history?minDate=YYYY-MM-DD
Key: Real Write (Demo key → 403)
Returns: array directly (not wrapped). Demo trades appear here.
```
### Instrument Resolution
```
GET /market-data/search?internalSymbolFull=GER40&fields=instrumentId,internalSymbolFull,displayname
→ item where item.internalSymbolFull == 'GER40' → item.instrumentId
```
`fields` param REQUIRED — omit → empty results.
Instrument IDs are immutable — cache in instrument_map.yaml.
---
## Empirical API Facts (confirmed on live API — override any docs)
| Fact | Detail |
|------|--------|
| OHLC fields can be None | Key present, value None — bars during market closure. Use `bar.get("field") or 0.0` not `bar.get("field", 0.0)` |
| Candles max per request | 1000 bars hard limit. Current config (500+120) within limit. Pagination not implemented. |
| DAX symbol key | GER40 in instrument_map.yaml. `execution.symbol` must match key exactly. |
| DAX instrument_id | 32 — immutable, confirmed. |
| volume field (DAX) | Always 0 — kept for schema compatibility. |
| Candle direction | Always fetch 'desc', reverse to asc — guarantees most recent N bars. |
| Demo trades in history | Real Write key required. Demo key → 403 on trade/history. |
---
## Portfolio Schema
```
GET /demo/portfolio
clientPortfolio.credit           ← available cash (NOT 'credits')
clientPortfolio.positions[]      ← open positions (PascalCase + capital ID)
clientPortfolio.ordersForOpen[]  ← pending open orders
```
### OpenPosition Key Fields
| Field | Notes |
|-------|-------|
| `positionID` | capital ID |
| `instrumentID` | capital ID |
| `isBuy` | true=long, false=short |
| `openDateTime` | ISO 8601 UTC |
| `openRate` | Entry price |
| `stopLossRate` | 0.0001 = not set |
| `takeProfitRate` | 0 = not set |
| `isNoStopLoss` | ⚠️ true = SL **DISABLED** (inverted) |
| `isNoTakeProfit` | ⚠️ true = TP **DISABLED** (inverted) |
---
## Trade History Key Fields (camelCase + lowercase id)
| API field | Model field |
|-----------|-------------|
| `positionId` | `trade_id` (str) |
| `instrumentId` | `instrument_id` |
| `isBuy` | `direction` (BUY/SELL) |
| `openTimestamp` | `open_time` |
| `closeTimestamp` | `close_time` |
| `openRate` / `closeRate` | `entry_price` / `exit_price` |
| `netProfit` | `profit_loss` |
| `investment` | `volume` |
---
## Package Structure
```
src/broker_support/
  client/client.py                    ← EToroClient — do not refactor _make_request()
  config/broker_support_config.py     ← BrokerSupportConfig typed schema
  models/trade.py + portfolio.py      ← typed contracts
  tracking/csv_journal.py             ← CSVJournal
  tracking/position_tracker.py        ← PositionTracker
  enrichment/instrument_resolver.py   ← YAML primary + API fallback
  enrichment/trade_enricher.py        ← exit price + P&L (10 pages, 90-day window)
  execution/order_router.py           ← two-step open, close
  utils/time_utils.py                 ← is_trading_hours(), is_valid_trading_window()
  live/
    live_data_fetcher.py              ← candles → DataFrame
    live_config_patcher.py            ← patches strategy YAML for live context
    live_data_bundle.py               ← DataBundle from live DataFrames + artf
    order_signal.py                   ← OrderSignal contract
    signal_bridge.py                  ← full pipeline → OrderSignal
scripts/broker_support/
  run_signal_loop.py                  ← polls 60s, places 1 order, stops
  run_signal.py                       ← single run dry-run / --place-order
  run_tracker_loop.py                 ← tracks open positions, journals closes
  inspect_portfolio.py                ← diagnostic
```
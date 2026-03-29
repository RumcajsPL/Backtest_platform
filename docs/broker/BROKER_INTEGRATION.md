# BROKER_INTEGRATION.md — eToro Broker Integration Project Charter
# High-level reference: scope, API endpoints, field contracts, key flows.
# Detail lives in docs/ctp/ARCHITECTURE.md.
# Updated: 2026-03-28
---
## Integration Scope
CTP broker integration operates exclusively on the eToro demo account via the
eToro public API v1. Base URL: `https://public-api.etoro.com/api/v1`.
Full API specification: `docs/ctp/API_REFERENCE.md`.
Instrument traded: DAX (eToro symbol `GER40`, instrumentId `32`).
---
## Endpoints Used by CTP

| Purpose | Method | Endpoint | Notes |
|---------|--------|----------|-------|
| Portfolio snapshot | GET | `/trading/info/demo/portfolio` | Primary for position tracking |
| Portfolio + PnL | GET | `/trading/info/demo/pnl` | `credits` (not `credit`) for cash |
| Order status | GET | `/trading/info/demo/orders/{orderId}` | Step 2 of open flow |
| Open position | POST | `/trading/execution/demo/market-open-orders/by-amount` | PascalCase body |
| Close position | POST | `/trading/execution/demo/market-close-orders/positions/{positionId}` | Full close: `UnitsToDeduct=null` |
| Trade history | GET | `/trading/info/trade/history?minDate=YYYY-MM-DD` | 30-day working window |
| Instrument search | GET | `/market-data/search?fields=...` | `fields` param required |
| Historical candles | GET | `/market-data/instruments/{id}/history/candles/{dir}/{interval}/{count}` | max per reques: 1000 bars -> for more history more request needs be placed |
---
## Critical Field Casing
| Context | Style | Examples |
|---------|-------|---------|
| Execution request body | PascalCase + capital ID | `InstrumentID`, `IsBuy`, `Amount` |
| Portfolio positions | PascalCase + capital ID | `positionID`, `instrumentID`, `orderID` |
| Portfolio wrapper `/portfolio` | camelCase | `credit` |
| Portfolio wrapper `/pnl` | camelCase | `credits` ← different field name |
| Trade history | camelCase + lowercase id | `positionId`, `instrumentId` |
| Candles outer | camelCase lowercase id | `instrumentId` |
| Candles inner | camelCase capital ID | `instrumentID` |
---
## Key Flows
### Open Position (two-step)
```
POST /trading/execution/demo/market-open-orders/by-amount
  Body: { InstrumentID, IsBuy, Leverage, Amount, [StopLossRate, TakeProfitRate] }
  → response.orderForOpen.orderID
  → response.orderForOpen.statusID
statusID == 1 (fast-fill):
  Skip /demo/orders/{id} polling.
  GET /demo/portfolio → match pos.orderID → positionID
  ⚠️ /demo/orders/{id} returns 404 then statusID=3 for ~4–8s — both stale.
statusID == 0 (normal):
  GET /trading/info/demo/orders/{orderID} — poll until statusID == 1
  → positions[0].positionID
  ⚠️ 404 is transient for first 3 attempts.
  ⚠️ statusID=3 may be stale — check portfolio before raising.
```
statusID: 0=Pending, 1=Executed, 2=Cancelled, 3=Rejected, 4=Partial
### Close Position
```
POST /trading/execution/demo/market-close-orders/positions/{positionId}
Body: { "InstrumentID": <id>, "UnitsToDeduct": null }   ← null = full close
```
### Trade History
```
GET /trading/info/trade/history?minDate=YYYY-MM-DD
Working lookback: 30 days (DEFAULT_DAYS_BACK=30).
Returns: array directly (not wrapped). Demo trades appear here.
```
### Instrument Resolution
```
GET /market-data/search?internalSymbolFull=GER40&fields=instrumentId,internalSymbolFull,displayname
→ exact match on item.internalSymbolFull == "GER40" → item.instrumentId
fields param required — omit → empty results.
```
---
## Portfolio Schema
```
GET /demo/portfolio → clientPortfolio:
  credit           float    available cash (NOT 'credits' — that is /pnl only)
  positions[]      OpenPosition[]
  ordersForOpen[]  pending open orders
  orders[]         MIT/limit orders
  mirrors[]        copy trading configs
```
### OpenPosition Fields
| Field | Type | Notes |
|-------|------|-------|
| `positionID` | int | capital ID |
| `instrumentID` | int | capital ID |
| `orderID` | int | matches POST open response orderID |
| `isBuy` | bool | true=long, false=short |
| `openDateTime` | datetime | ISO 8601 UTC |
| `openRate` | float | entry price |
| `amount` | float | USD invested |
| `stopLossRate` | float | 0.0001 = not set |
| `takeProfitRate` | float | 0 = not set |
| `isNoStopLoss` | bool | ⚠️ true = SL DISABLED (inverted) |
| `isNoTakeProfit` | bool | ⚠️ true = TP DISABLED (inverted) |
| `leverage` | int | |
| `mirrorID` | int | 0 = manual trade |
---
## Trade History Fields
| API field | Trade model field | Notes |
|-----------|------------------|-------|
| `positionId` | `trade_id` | str, coerced |
| `instrumentId` | `instrument_id` | lowercase id |
| `isBuy` | `direction` | BUY/SELL via validator |
| `openTimestamp` | `open_time` | NOT openDateTime |
| `closeTimestamp` | `close_time` | |
| `openRate` | `entry_price` | |
| `closeRate` | `exit_price` | |
| `investment` | `volume` | USD invested |
| `netProfit` | `profit_loss` | |
| `fees` | `fees` | |
| `leverage` | `leverage` | |
| `stopLossRate` | `sl_rate` | |
| `takeProfitRate` | `tp_rate` | |
---
## Empirical API Facts
| Fact | Detail |
|------|--------|
| OHLC None values | Key present, value None during market closure. Use `bar.get("f") or 0.0`. |
| Candles max | 1000 bars hard limit per request. |
| DAX instrument_id | 32 — immutable. |
| DAX symbol | GER40 — must match instrument_map.yaml exactly. |
| DAX volume | Always 0. |
| Candle direction | Fetch `desc`, reverse to `asc`. |
| trade/history lookback | 30 days. Requests >30 days → 403. |
| Portfolio orderID | Each open position has `orderID` matching the POST response value. |
| Portfolio write lag | With 3+ external positions, new orderID may not appear for >20s. |
| Fast-fill 404/statusID=3 | Both stale after fast-fill — always resolve via portfolio scan. |
| `fields` on search | Required. Omit → empty results. |
| credit vs credits | `/portfolio` → `credit`. `/pnl` → `credits`. |
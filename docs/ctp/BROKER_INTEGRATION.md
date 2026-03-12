# BROKER_INTEGRATION.md
# eToro Broker Integration — CTP Paper Trading Reference
# Last updated: 2026-03-12 (Block 9P+2 — Steps 1–5 COMPLETE)
# Full API reference (all endpoints, schemas, WebSocket, social features): docs/ctp/API_REFERENCE.md
---

## CTP Integration Status

| Step | Description | Status |
|------|-------------|--------|
| Step 1 | Client fixes + empirical demo history test | ✅ COMPLETE |
| Step 2 | Instrument resolver (YAML + API fallback) | ✅ COMPLETE |
| Step 3 | Trade enricher (exit price + P&L) | ✅ COMPLETE |
| Step 4 | Tracker loop (polling daemon + hours guard) | ✅ COMPLETE |
| Step 5 | Signal bridge / order router | ✅ COMPLETE |
| Phase 2 | Wire strategy YAML → OrderRouter → live demo trades | 🔲 NEXT |
---
## Authentication
Three headers required on **every** request:
| Header | Value |
|--------|-------|
| `x-api-key` | `ETORO_API_KEY` from `broker_settings.env` |
| `x-user-key` | `ETORO_USER_KEY` from `broker_settings.env` — **must be Demo Write key** |
| `x-request-id` | Fresh UUID per request |
**Base URL:** `https://public-api.etoro.com/api/v1`
**Our setup:** Demo Write key for `ETORO_USER_KEY`. Demo Write = read + write on demo account.
Real key → 403 on ALL `/demo/` endpoints. Key type, not URL prefix, determines the account.
---
## Endpoints Used by CTP

| Purpose | Method | Endpoint | Key |
|---------|--------|----------|-----|
| Portfolio snapshot | GET | `/trading/info/demo/portfolio` | Demo Write |
| Portfolio + PnL | GET | `/trading/info/demo/pnl` | Demo Write |
| Order status (two-step) | GET | `/trading/info/demo/orders/{orderId}` | Demo Write |
| Open position | POST | `/trading/execution/demo/market-open-orders/by-amount` | Demo Write |
| Close position | POST | `/trading/execution/demo/market-close-orders/positions/{positionId}` | Demo Write |
| Cancel open order | DELETE | `/trading/execution/demo/market-open-orders/{orderId}` | Demo Write |
| Cancel close order | DELETE | `/trading/execution/demo/market-close-orders/{orderId}` | Demo Write |
| Trade history | GET | `/trading/info/trade/history` | Real Write |
| Instrument search | GET | `/market-data/search` | Any |
| Instrument metadata | GET | `/market-data/instruments` | Any |
| Current rates | GET | `/market-data/instruments/rates` | Any |
| Historical candles | GET | `/market-data/instruments/{id}/history/candles/{dir}/{interval}/{count}` | Any |
---
## Critical Field Casing Rules
| Context | Style | Key Examples |
|---------|-------|-------------|
| Open order request body | PascalCase + capital ID | `InstrumentID`, `IsBuy`, `Amount`, `Leverage` |
| Close order request body | PascalCase + capital ID | `InstrumentID` (NOT `InstrumentId`) |
| Portfolio positions | PascalCase + capital ID | `positionID`, `instrumentID`, `CID` |
| Portfolio wrapper (`/portfolio`) | camelCase | `credit` (NOT `credits`) |
| Portfolio wrapper (`/pnl`) | camelCase | `credits` (NOT `credit`) |
| Trade history fields | camelCase + lowercase id | `positionId`, `instrumentId`, `openTimestamp` |
---
## Key API Flows
### Two-Step Open Position
```
Step 1: POST /trading/execution/demo/market-open-orders/by-amount
        Body: { InstrumentID, IsBuy, Leverage, Amount }
        → response.orderForOpen.orderID
Step 2: GET /trading/info/demo/orders/{orderID}
        Poll until statusID == 1 (Executed)
        → response.positions[0].positionID  ← use for close calls
```
statusID values: 0=Pending, 1=Executed, 2=Cancelled, 3=Rejected, 4=Partial
⚠️ `positionID` is NOT in the open-order response. Must poll order info endpoint.
### Close Position
```
POST /trading/execution/demo/market-close-orders/positions/{positionId}
Body: { "InstrumentID": <id>, "UnitsToDeduct": null }   ← null = full close
```
### Fetch Closed Trades (RESULT A)
```
GET /trading/info/trade/history?minDate=YYYY-MM-DD
Headers: Demo Write key
Returns: array directly (not wrapped). Demo trades appear here.
```
### Instrument Resolution
```
GET /market-data/search?internalSymbolFull=GER40&fields=instrumentId,internalSymbolFull,displayname
→ find item where item.internalSymbolFull == 'GER40'
→ use item.instrumentId
```
`fields` param is REQUIRED — omit → empty results. Instrument IDs are immutable; cache in YAML.
---
## Portfolio Schema (used by PositionTracker)
### `/demo/portfolio` Response
```
clientPortfolio.credit          ← available cash (NOT 'credits')
clientPortfolio.positions[]     ← open positions
clientPortfolio.ordersForOpen[] ← pending open orders
```
### OpenPosition Fields (PascalCase + capital ID)
| Field | Python alias | Notes |
|-------|-------------|-------|
| `positionID` | `position_id` | ← capital ID |
| `instrumentID` | `instrument_id` | ← capital ID |
| `isBuy` | `is_buy` | true=long, false=short |
| `openDateTime` | `open_date_time` | ISO 8601 UTC |
| `openRate` | `open_rate` | Entry price |
| `amount` | `amount` | USD invested |
| `units` | `units` | Number of units |
| `stopLossRate` | `stop_loss_rate` | 0.0001 = not set |
| `takeProfitRate` | `take_profit_rate` | 0 = not set |
| `leverage` | `leverage` | |
| `isNoStopLoss` | — | ⚠️ true = SL **DISABLED** (inverted) |
| `isNoTakeProfit` | — | ⚠️ true = TP **DISABLED** (inverted) |
| `mirrorID` | — | 0 = manual trade |
| `settlementTypeID` | — | 0=CFD, 1=Real Asset, 2=SWAP, 3=Crypto, 4=Future |
### Available Cash Formula (requires `/demo/pnl`)
```
available_cash = credits
               - sum(ordersForOpen[i].amount for i where mirrorID == 0)
               - sum(orders[i].amount for all i)
```
---
## Trade History Schema (camelCase + lowercase id)
| API field | Model field | Notes |
|-----------|-------------|-------|
| `positionId` | `trade_id` | lowercase id, coerced to str |
| `instrumentId` | `instrument_id` | lowercase id |
| `isBuy` | `direction` | true→'BUY', false→'SELL' |
| `openTimestamp` | `open_time` | NOT openDateTime |
| `closeTimestamp` | `close_time` | |
| `openRate` | `entry_price` | |
| `closeRate` | `exit_price` | |
| `investment` | `volume` | |
| `netProfit` | `profit_loss` | |
| `fees` | `fees` | default 0.0 |
| `leverage` | `leverage` | |
---
## Instrument Resolution
**YAML primary** → `configs/broker_support/instrument_map.yaml`
```yaml
DAX:
  instrument_id: 32
  symbol_full: GER40
```
**API fallback:**
```
GET /market-data/search?internalSymbolFull=GER40&fields=instrumentId,internalSymbolFull,displayname
```
Instrument IDs are immutable. Cache once, trust forever.
---
## Package Structure
```
src/broker_support/
  client/client.py              ← EToroClient — _make_request() is core, do not refactor
  config/settings.py            ← Pydantic settings, loads broker_settings.env
  models/trade.py               ← Trade model (camelCase aliases — trade history)
  models/portfolio.py           ← OpenPosition (PascalCase aliases), OrderForOpen, available_cash()
  tracking/csv_journal.py       ← CSVJournal (dedup, header-on-empty)
  tracking/position_tracker.py  ← PositionTracker (snapshot diff + enrich + journal)
  enrichment/instrument_resolver.py ← YAML primary + API fallback
  enrichment/trade_enricher.py      ← RESULT A: history search by positionId, 10 pages max
  execution/order_router.py     ← OrderRouter: two-step open (poll positionID), close
  execution/__init__.py         ← exports OrderRouter, OutsideTradingHoursError
  utils/time_utils.py           ← is_trading_hours(), seconds_until_open()
configs/broker_support/
  instrument_map.yaml           ← DAX: {instrument_id: 32, symbol_full: GER40}
  broker_settings.env           ← ETORO_API_KEY + ETORO_USER_KEY (must be Demo Write key)
scripts/broker_support/
  run_tracker_loop.py           ← polling loop (5 min, hours guard 08:00-22:00 CET)
  inspect_portfolio.py          ← diagnostic
  inspect_instruments.py        ← diagnostic
```
---
## What NOT To Do
- Do NOT call `/demo/` endpoints with a Real key → 403
- Do NOT call `/trading/info/portfolio` (missing `/demo/`) with Demo key → wrong account
- Do NOT omit `fields` param on market-data/search → empty results
- Do NOT use `from` or `fromDate` for trade history → use `minDate=YYYY-MM-DD`
- Do NOT use Read-only key for `trade/history` → 403
- Do NOT assume `positionID` is in the open-order response → must poll order info
- Do NOT send `InstrumentId` (lowercase d) in close body → must be `InstrumentID`
- Do NOT confuse `credit` (/portfolio) with `credits` (/pnl) — different field names
- Do NOT refactor `_make_request()` — retry logic is solid, do not touch
- Do NOT set `deployment_status = LIVE_APPROVED` in code — operator-only
- Do NOT use `print()` — use `logger.info/debug`
- Do NOT skip pre-trade rate check — validate SL/TP against current ask/bid first
---
## Phase 2 — Next Steps
Wire strategy YAML → OrderRouter → live demo trades:
1. Parse signal from `b651ec5c_c424a0e04327_strategy.yaml` (PRIMARY candidate)
2. Run `OrderRouter.open_position()` manually, confirm positionID journaled
3. Let tracker loop detect close, confirm journal entry
4. Only then consider automation loop
Full API reference for real account trading, WebSocket, social features, user info:
→ `docs/ctp/API_REFERENCE.md`
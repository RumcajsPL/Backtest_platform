# BROKER_INTEGRATION.md
# eToro Broker Integration — Complete API Reference + CTP Development Plan
# Last updated: 2026-03-12 (Block 9P+1 — Step 5 in progress)
# Source: https://api-portal.etoro.com (OpenAPI v1.138.0)
---
## Quick-Reference: Endpoint Map
| Group | Method | Endpoint | Key required | Confirmed |
|-------|--------|----------|--------------|-----------|
| Demo Portfolio + PnL | GET | `/api/v1/trading/info/demo/pnl` | Demo Write | ✅ docs |
| Demo Portfolio (no PnL) | GET | `/api/v1/trading/info/demo/portfolio` | Demo Write | ✅ docs + empirical |
| Demo Order Info | GET | `/api/v1/trading/info/demo/orders/{orderId}` | Demo Write | ✅ docs |
| Demo Open by Amount | POST | `/api/v1/trading/execution/demo/market-open-orders/by-amount` | Demo Write | ✅ docs |
| Demo Open by Units | POST | `/api/v1/trading/execution/demo/market-open-orders/by-units` | Demo Write | ✅ docs |
| Demo Close Position | POST | `/api/v1/trading/execution/demo/market-close-orders/positions/{positionId}` | Demo Write | ✅ docs |
| Demo Cancel Open Order | DELETE | `/api/v1/trading/execution/demo/market-open-orders/{orderId}` | Demo Write | ✅ docs |
| Demo Cancel Close Order | DELETE | `/api/v1/trading/execution/demo/market-close-orders/{orderId}` | Demo Write | ✅ docs |
| Demo Limit (MIT) Order | POST | `/api/v1/trading/execution/demo/limit-orders` | Demo Write | ✅ docs |
| Demo Cancel Limit Order | DELETE | `/api/v1/trading/execution/demo/limit-orders/{orderId}` | Demo Write | ✅ docs |
| Real Portfolio + PnL | GET | `/api/v1/trading/info/real/pnl` | Real Write | ✅ empirical |
| Real Portfolio (no PnL) | GET | `/api/v1/trading/info/real/portfolio` | Real Write | ⚠️ not yet tested |
| Real Open by Amount | POST | `/api/v1/trading/execution/market-open-orders/by-amount` | Real Write | ⚠️ docs only |
| Real Close Position | POST | `/api/v1/trading/execution/market-close-orders/positions/{positionId}` | Real Write | ⚠️ docs only |
| Trade History | GET | `/api/v1/trading/info/trade/history` | Real Write | ✅ empirical |
| Instrument Search | GET | `/api/v1/market-data/search` | Any | ✅ docs (fields param required) |
| Instrument Metadata | GET | `/api/v1/market-data/instruments` | Any | ✅ docs + empirical |
| Instrument Rates | GET | `/api/v1/market-data/instruments/rates` | Any | ✅ docs + empirical |
| Historical Candles | GET | `/api/v1/market-data/instruments/{id}/history/candles/{dir}/{interval}/{count}` | Any | ✅ docs |
| Instrument Types | GET | `/api/v1/market-data/instrument-types` | Any | ✅ docs |
| Stocks Industries | GET | `/api/v1/market-data/stocks-industries` | Any | ✅ docs |
| Watchlists (conn test) | GET | `/api/v1/watchlists` | Any Read | ✅ empirical |
---
## Authentication
Three headers required on **every** request:
| Header | Description |
|--------|-------------|
| `x-api-key` | Public API Key — identifies the application. App-level, shared. |
| `x-user-key` | User Key — identifies the account. Determines demo vs real data. |
| `x-request-id` | UUID — unique per request. Generate fresh every call. |
### Key types and permissions
Keys created at: https://www.etoro.com/settings/trade → Settings → Trading → API Key Management.
| Key type | Permissions | Account |
|----------|-------------|---------|
| Demo Read | Read only | Demo account |
| Demo Write | Read + Write | Demo account + demo execution |
| Real Read | Read only | Real account |
| Real Write | Read + Write | Real account + real execution |
**Critical architecture fact:** The key type (Demo/Real) determines which account's data is
returned — not the URL prefix. Info endpoints are identical regardless of demo/real.
Only execution endpoints use `/demo/` prefix for demo trades.
**Our setup (corrected 2026-03-12):**
- `ETORO_API_KEY` = Public API Key (global, app-level)
- `ETORO_USER_KEY` = Demo Write key (read + write, demo account)
**Previous failure root cause:** Real key was set in `broker_settings.env` → 403 on all
`/demo/` info endpoints. Demo key is required to access demo account data.
---
## CTP Integration Status
| Step | Description | Status |
|------|-------------|--------|
| Step 1 | Client fixes + empirical demo history test | ✅ COMPLETE |
| Step 2 | Instrument resolver (YAML + API fallback) | ✅ COMPLETE |
| Step 3 | Trade enricher (exit price + P&L) | ✅ COMPLETE |
| Step 4 | Tracker loop (polling daemon + hours guard) | ✅ COMPLETE |
| Step 5 | Signal bridge / order router | 🔲 IN PROGRESS |
---
## Demo Account — Portfolio Endpoints
### GET /api/v1/trading/info/demo/portfolio
**Source:** OpenAPI v1.138.0 — confirmed schema with live example
**Use:** Full portfolio snapshot without PnL calculations. Preferred for position tracking/diffing.
### GET /api/v1/trading/info/demo/pnl
**Use:** Full portfolio with unrealized PnL per position. Preferred for monitoring P&L and
calculating available cash (uses `credits` field — see difference below).
Both return: `{ clientPortfolio: { ... } }`
**`clientPortfolio` top-level fields:**
| Field | Type | Endpoint | Description |
|-------|------|----------|-------------|
| `credit` | float | `/portfolio` | Available cash balance (USD) |
| `credits` | float | `/pnl` | ⚠️ Same concept, different field name in PnL endpoint |
| `bonusCredit` | float | both | Bonus credit (USD) |
| `unrealizedPnL` | float | `/pnl` only | Total unrealized P&L |
| `positions` | Position[] | both | Open positions |
| `mirrors` | Mirror[] | both | Copy trading configs |
| `orders` | Order[] | both | Pending MIT/limit orders |
| `ordersForOpen` | OrderForOpen[] | both | Active orders to open positions |
| `ordersForClose` | OrderForClose[] | both | Active orders to close positions |
| `ordersForCloseMultiple` | OrderForCloseMultiple[] | both | Active orders to close multiple |
| `stockOrders` | [] | both | Obsolete |
| `entryOrders` | [] | both | Obsolete |
| `exitOrders` | [] | both | Obsolete |
⚠️ **`credit` vs `credits` field name trap:**
- `/demo/portfolio` → `clientPortfolio.credit`
- `/demo/pnl` → `clientPortfolio.credits` (used in available cash formula)
- Our client uses `/demo/portfolio` for tracking → use `credit`. If we switch to `/pnl` for
  available cash calculation → use `credits`. Do not mix them.
### Available Cash Formula
**Source:** https://api-portal.etoro.com/guides/calculate-available-cash.md
Requires `/pnl` endpoint (uses `credits` field):
```
available_cash = credits
                 - sum(ordersForOpen[i].amount for i where mirrorID == 0)
                 - sum(orders[i].amount for all i)
```
- Only manual `ordersForOpen` (where `mirrorID == 0`) are deducted
- All `orders` (MIT/limit) are deducted regardless of mirrorID
- Mirrored `ordersForOpen` (`mirrorID != 0`) are excluded
**Example:** credits=1000, ordersForOpen=[200 (manual), 200 (manual), 100 (mirrored)], orders=[150]
→ available = 1000 - (200+200) - 150 = **450** (the 100 mirrored order is excluded)
---
## Open Position Schema
**Source:** `/demo/portfolio` OpenAPI schema (confirmed with live example)
⚠️ **Field names use PascalCase with capital-ID suffix.**
This is different from trade history (camelCase + lowercase id). Do not mix them.
| API field | Type | Description |
|-----------|------|-------------|
| `positionID` | int | Position unique identifier ← **capital ID** |
| `CID` | int | Customer ID ← **all caps** |
| `openDateTime` | datetime | ISO 8601 UTC |
| `openRate` | float | Entry price |
| `instrumentID` | int | Instrument ← **capital ID** |
| `isBuy` | bool | true=long, false=short |
| `takeProfitRate` | float | TP trigger price (0 = not set) |
| `stopLossRate` | float | SL trigger price (0.0001 = not set) |
| `mirrorID` | int | 0 if manual trade ← **capital ID** |
| `parentPositionID` | int | 0 if manual trade ← **capital ID** |
| `amount` | float | USD invested (includes margin collateral) |
| `leverage` | int | Leverage multiplier |
| `orderID` | int | Order that opened this position ← **capital ID** |
| `orderType` | int | Order type of opening order (match with orderID) |
| `units` | float | Number of units |
| `totalFees` | float | Overnight fees + dividends (negative = refund) |
| `initialAmountInDollars` | float | Original investment (unchanged after partial close) |
| `isTslEnabled` | bool | Trailing stop loss active |
| `stopLossVersion` | int | Increments on each manual SL edit |
| `isSettled` | bool | Obsolete |
| `redeemStatusID` | int | Redeem process status ← **capital ID** |
| `initialUnits` | float | Original units (unchanged after partial close) |
| `isPartiallyAltered` | bool | True if partially closed |
| `unitsBaseValueDollars` | float | USD value of current units (= initialAmountInDollars if not partially altered) |
| `isDiscounted` | bool | Obsolete |
| `openPositionActionType` | int | Reason position was opened |
| `settlementTypeID` | int | 0=CFD, 1=Real Asset, 2=SWAP, 3=Crypto Margin, 4=Future ← **capital ID** |
| `isDetached` | bool | True if detached from mirror |
| `openConversionRate` | float | FX rate asset→USD at open |
| `pnlVersion` | int | P&L formula version |
| `totalExternalFees` | float | Ticket fees (excludes overnight) |
| `totalExternalTaxes` | float | SDRT etc |
| `isNoTakeProfit` | bool | ⚠️ **true = TP DISABLED** (inverted semantics) |
| `isNoStopLoss` | bool | ⚠️ **true = SL DISABLED** (inverted semantics) |
| `lotCount` | float | Lot count (futures only, irrelevant for CFDs) |
| `unrealizedPnL` | object\|null | **Only present in `/pnl` endpoint** (see below) |
**`unrealizedPnL` sub-object** (only in `/demo/pnl`):
| Field | Type | Description |
|-------|------|-------------|
| `pnL` | float | Unrealized P&L in USD |
| `pnlAssetCurrency` | float | Unrealized P&L in asset currency |
| `exposureInAccountCurrency` | float | Current exposure USD |
| `exposureInAssetCurrency` | float | Current exposure asset currency |
| `marginInAccountCurrency` | float | Margin USD |
| `marginInAssetCurrency` | float | Margin asset currency |
| `marginCurrencyId` | int | Currency ID for margin |
| `assetCurrencyId` | int | Currency ID for the asset |
| `closeRate` | float | Current close rate |
| `closeConversionRate` | float | Current FX rate |
| `timestamp` | datetime | PnL calculation timestamp |
### OpenPosition Pydantic model — required alias fixes
**File:** `src/broker_support/models/portfolio.py`
Current aliases are WRONG (camelCase). Correct aliases (PascalCase from confirmed schema):
| Python field | WRONG (current) | CORRECT |
|---|---|---|
| `position_id` | `positionId` | `positionID` |
| `instrument_id` | `instrumentId` | `instrumentID` |
| `is_buy` | (check) | `isBuy` ✅ |
| `open_rate` | (check) | `openRate` ✅ |
| `open_date_time` | (check) | `openDateTime` ✅ |
| `amount` | (check) | `amount` ✅ |
| `units` | (check) | `units` ✅ |
| `stop_loss_rate` | (check) | `stopLossRate` ✅ |
| `take_profit_rate` | (check) | `takeProfitRate` ✅ |
| `leverage` | (check) | `leverage` ✅ |
---
## Demo Account — Execution Endpoints
### POST /api/v1/trading/execution/demo/market-open-orders/by-amount
**Request body (PascalCase + capital ID):**
| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `InstrumentID` | int32 | ✅ | ← capital ID |
| `IsBuy` | bool | ✅ | true=long, false=short |
| `Leverage` | int32 | ✅ | |
| `Amount` | double | ✅ | USD amount |
| `StopLossRate` | double | ❌ | Absolute price. Must be worse than current. |
| `TakeProfitRate` | double | ❌ | Absolute price. Must be better than current. |
| `IsTslEnabled` | bool | ❌ | Trailing stop loss |
| `IsNoStopLoss` | bool | ❌ | true = disable SL |
| `IsNoTakeProfit` | bool | ❌ | true = disable TP |
**Response:** `{ orderForOpen: { orderID, instrumentID, amount, isBuy, leverage, statusID, CID, openDateTime, ... }, token: uuid }`
⚠️ **positionID is NOT in the response.** Must call order info endpoint to get it (see two-step flow).
**Example response:**
```json
{
  "orderForOpen": {
    "orderID": 13902598,
    "instrumentID": 100000,
    "amount": 150,
    "isBuy": true,
    "leverage": 1,
    "statusID": 1,
    "CID": 7765437,
    "openDateTime": "2025-04-02T15:47:15.937Z"
  },
  "token": "066faaee-e1e9-49d2-a568-c6e1cc336ad8"
}
```
### POST /api/v1/trading/execution/demo/market-open-orders/by-units
Same as by-amount but replace `Amount` with `AmountInUnits` (double, required).
Not currently used in CTP (we use by-amount).
### POST /api/v1/trading/execution/demo/market-close-orders/positions/{positionId}
**Path param:** `positionId` (int64)
**Request body:**
| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `InstrumentID` | int32 | ✅ | ⚠️ capital ID — NOT `InstrumentId` |
| `UnitsToDeduct` | double | ❌ | null or omit = full close |
**Response:** `{ orderForClose: { positionID, instrumentID, unitsToDeduct, orderID, statusID, CID, openDateTime, lastUpdate }, token: uuid }`
**Example response:**
```json
{
  "orderForClose": {
    "positionID": 2150941015,
    "instrumentID": 1111,
    "unitsToDeduct": null,
    "orderID": 13904638,
    "statusID": 1,
    "CID": 7765437,
    "openDateTime": "2025-04-02T16:07:54.088Z"
  },
  "token": "5fe065bc-f6f9-4897-a2ce-c4fccef73ff8"
}
```
### GET /api/v1/trading/info/demo/orders/{orderId}
**Use:** Retrieve positionID(s) created by an open order (Step 2 of two-step open flow).
**Path param:** `orderId` (int64)
**Response top-level fields:**
| Field | Type | Description |
|-------|------|-------------|
| `orderID` | int64 | |
| `CID` | int64 | |
| `statusID` | int | 0=Pending, 1=Executed, 2=Cancelled, 3=Rejected, 4=Partial |
| `orderType` | int | |
| `instrumentID` | int | |
| `amount` | decimal | USD invested |
| `units` | decimal | Units traded |
| `requestOccurred` | datetime | Submission timestamp |
| `errorCode` | int\|null | null on success |
| `errorMessage` | str\|null | null on success |
| `positions` | PositionInfo[] | Positions opened by this order |
| `token` | uuid | Tracking token |
**`positions[n]` fields:**
| Field | Type | Description |
|-------|------|-------------|
| `positionID` | int64 | ✅ **Use this for close calls** |
| `orderType` | int | |
| `occurred` | datetime | Position open timestamp |
| `rate` | decimal | Execution price |
| `units` | decimal | Units |
| `conversionRate` | decimal | FX rate at open |
| `amount` | decimal | USD invested |
| `isOpen` | bool | True if currently open |
---
## Two-Step Open Position Flow
Opening a position returns an `orderID`, not a `positionID`. Two API calls required:
```
Step 1: POST /trading/execution/demo/market-open-orders/by-amount
        Body: { InstrumentID, IsBuy, Leverage, Amount, [StopLossRate, TakeProfitRate] }
        → response.orderForOpen.orderID  (e.g. 13902598)

Step 2: GET /trading/info/demo/orders/{orderID}
        Poll until response.statusID == 1 (Executed)
        → response.positions[0].positionID  (e.g. 9876543210)
```
`positionID` from Step 2 is used in all subsequent close calls and for journaling.
**statusID values:**
- 0 = Pending
- 1 = Executed ← target state
- 2 = Cancelled
- 3 = Rejected
- 4 = Partially Executed
---
## Demo Account — Cancellation + Limit Order Endpoints
### DELETE /api/v1/trading/execution/demo/market-open-orders/{orderId}
Cancel a pending market-open order before execution. Response: `{ token: uuid }`
### DELETE /api/v1/trading/execution/demo/market-close-orders/{orderId}
Cancel a pending market-close order before execution. Response: `{ token: uuid }`
### POST /api/v1/trading/execution/demo/limit-orders
Market-if-touched (MIT) order. Opens position when `Rate` trigger price is reached.
Additional required field vs market order: `Rate` (trigger price).
- Long positions: `Rate` must be lower than current price.
- Short positions: `Rate` must be higher than current price.
Response: `{ token: uuid }`
### DELETE /api/v1/trading/execution/demo/limit-orders/{orderId}
Cancel a pending MIT order before it triggers. Response: `{ token: uuid }`
---
## Trade History
**Endpoint:** `GET /api/v1/trading/info/trade/history`
**Key:** Real Write (empirically confirmed — Demo key returns 403)
**Query params:**
| Param | Type | Required | Notes |
|-------|------|----------|-------|
| `minDate` | string | ✅ | YYYY-MM-DD format. NOT `from`, NOT `fromDate`. |
| `page` | int | ❌ | 1-based |
| `pageSize` | int | ❌ | Default 100, max 100 |
**Response:** Array of trade objects directly (not wrapped in a key).
**Field mapping (camelCase — different from portfolio PascalCase):**
| API field | Model field | Type | Notes |
|-----------|-------------|------|-------|
| `positionId` | `trade_id` | str | Coerced from int — lowercase id |
| `instrumentId` | `instrument_id` | int | lowercase id |
| `isBuy` | `direction` | str | true→'BUY', false→'SELL' |
| `openTimestamp` | `open_time` | datetime | NOT openDateTime |
| `closeTimestamp` | `close_time` | datetime | |
| `openRate` | `entry_price` | float | |
| `closeRate` | `exit_price` | float | |
| `investment` | `volume` | float | |
| `units` | `units` | float | |
| `netProfit` | `profit_loss` | float | |
| `fees` | `fees` | float | default 0.0 |
| `leverage` | `leverage` | int | |
| `stopLossRate` | `sl_rate` | float\|None | |
| `takeProfitRate` | `tp_rate` | float\|None | |
| `trailingStopLoss` | `trailing_stop_loss` | bool | |
---
## Market Data Endpoints
### GET /api/v1/market-data/search
**Source:** OpenAPI v1.138.0 — confirmed full schema
⚠️ **`fields` param is REQUIRED** — requests without it will fail or return empty results.
This was likely why our earlier tests with `internalSymbolFull` returned empty — `fields` was not set.
**Query params:**
| Param | Required | Notes |
|-------|----------|-------|
| `fields` | ✅ | Comma-separated fields to return. e.g. `instrumentId,internalSymbolFull,displayname` |
| `searchText` | ❌ | Fuzzy text search within instrument names |
| `internalSymbolFull` | ❌ | Exact symbol filter. Use for precise resolution. |
| `pageSize` | ❌ | Results per page |
| `pageNumber` | ❌ | Page number |
| `sort` | ❌ | e.g. `popularityUniques7Day desc` |
**Response:** `{ page, pageSize, totalItems, items: [ Instrument, ... ] }`
**Full `Instrument` field list (only returned if included in `fields` param):**
| Field | Type | Description |
|-------|------|-------------|
| `instrumentId` | int | ✅ The ID to use in all other calls — lowercase id |
| `internalSymbolFull` | str | e.g. `GER40`, `AAPL` |
| `displayname` | str | Human-readable name |
| `instrumentTypeID` | int | Asset class |
| `instrumentType` | str | Asset class name |
| `exchangeID` | int | Exchange |
| `symbol` | str | Trading symbol |
| `isOpen` | bool | Exchange currently open |
| `isCurrentlyTradable` | bool | False = cannot trade now |
| `isInternalInstrument` | bool | True = restricted from public access |
| `isHiddenFromClient` | bool | Hidden from client display |
| `isBuyEnabled` | bool | Buying currently enabled |
| `currentRate` | float | Current price |
| `isDelisted` | bool | Delisted instrument |
| `isActiveInPlatform` | bool | Active in platform |
| `dailyPriceChange` | float | Daily price change |
| `popularityUniques7Day` | int | Unique viewers last 7 days |
| `cvtBid` / `cvtAsk` | float | Converted bid/ask prices |
| `holdingPct` | float | % of users holding this instrument |
| `buyHoldingPct` / `sellHoldingPct` | float | Long/short split |
| `logo35x35` / `logo50x50` / `logo150x150` | str | Logo URLs |
**Recommended usage for InstrumentResolver:**
```
GET /market-data/search?internalSymbolFull=GER40&fields=instrumentId,internalSymbolFull,displayname
→ Find item where item.internalSymbolFull == 'GER40' (exact match validation)
→ Use item.instrumentId
```
⚠️ **Action required:** Our `InstrumentResolver` currently uses `searchText` and likely omits
the required `fields` param. Fix: add `fields=instrumentId,internalSymbolFull,displayname`
to all search calls. Consider adding `internalSymbolFull` exact-match as primary strategy
with `searchText` as fallback.
### GET /api/v1/market-data/instruments
**Source:** OpenAPI v1.138.0 — confirmed full schema
**Query params (all optional, comma-separated lists):**
| Param | Description |
|-------|-------------|
| `instrumentIds` | Filter by specific instrument IDs |
| `exchangeIds` | Filter by exchange |
| `stocksIndustryIds` | Filter by industry |
| `instrumentTypeIds` | Filter by asset class |
**Response:** `{ instrumentDisplayDatas: [ { ... } ] }`
**Full field list:**
| Field | Type | Description |
|-------|------|-------------|
| `instrumentID` | int | ← capital ID |
| `instrumentDisplayName` | str | e.g. `DAX (GER40 Index)` |
| `symbolFull` | str | e.g. `GER40` |
| `instrumentTypeID` | int | Asset class ID ← capital ID |
| `exchangeID` | int | Exchange ID ← capital ID |
| `stocksIndustryId` | int | Industry ID ← lowercase id (inconsistent with others) |
| `priceSource` | str | Data provider e.g. `Nasdaq`, `LSE`, `CME` |
| `hasExpirationDate` | bool | True for futures |
| `isInternalInstrument` | bool | True = restricted from public access |
| `images` | Image[] | Array of `{ instrumentID, width, height, uri, backgroundColor, textColor }` |
**Confirmed empirical:** DAX = `instrumentID=32`, `symbolFull="GER40"`.
### GET /api/v1/market-data/instruments/rates
**Source:** OpenAPI v1.138.0 — confirmed full schema
**Query params:**
| Param | Required | Notes |
|-------|----------|-------|
| `instrumentIds` | ✅ | Comma-separated. Max 100. |
**Response:** `{ rates: [ { ... } ] }`
**Full field list:**
| Field | Type | Description |
|-------|------|-------------|
| `instrumentID` | int | ← capital ID |
| `ask` | float | Current ask (buy) price |
| `bid` | float | Current bid (sell) price |
| `lastExecution` | float | Last trade price |
| `conversionRateAsk` | float | FX rate asset→USD (ask) |
| `conversionRateBid` | float | FX rate asset→USD (bid) |
| `date` | datetime | Price timestamp |
| `priceRateID` | int | Internal rate ID |
| `unitMargin` / `unitMarginAsk` / `unitMarginBid` | float | Obsolete |
| `bidDiscounted` / `askDiscounted` | float | Obsolete |
| `unitMarginBidDiscounted` / `unitMarginAskDiscounted` | float | Obsolete |
**Key usage:**
- `ask` = price to buy (long entry / short close)
- `bid` = price to sell (short entry / long close)
- Pre-trade validation: check SL/TP levels against current ask/bid before placing order
### GET /api/v1/market-data/instruments/{instrumentId}/history/candles/{direction}/{interval}/{candlesCount}
**Source:** OpenAPI v1.138.0 — confirmed full schema
**Path params:**
| Param | Values | Description |
|-------|--------|-------------|
| `instrumentId` | int | Instrument ID |
| `direction` | `asc` \| `desc` | asc=oldest first, desc=newest first |
| `interval` | `OneMinute` `FiveMinutes` `TenMinutes` `FifteenMinutes` `ThirtyMinutes` `OneHour` `FourHours` `OneDay` `OneWeek` | Candle timeframe |
| `candlesCount` | int (max 1000) | Number of candles |
**Response structure:**
```json
{
  "interval": "OneMinute",
  "candles": [
    {
      "instrumentId": 12,
      "candles": [
        {
          "instrumentID": 12,
          "fromDate": "2025-03-05T10:34:00Z",
          "open": 1.70227,
          "high": 1.70277,
          "low": 1.70221,
          "close": 1.70253,
          "volume": 0
        }
      ],
      "rangeOpen": 1.70227,
      "rangeClose": 1.70276,
      "rangeHigh": 1.70277,
      "rangeLow": 1.70221,
      "volume": 0
    }
  ]
}
```
⚠️ **Casing quirk in candles response:**
- Outer wrapper uses `instrumentId` (lowercase id)
- Inner candle objects use `instrumentID` (capital ID)
**Notes:**
- `volume` is 0 for most instruments on eToro — not provided
- `fromDate` = candle open time. No `toDate` — calculate from interval
- For longer history: use larger interval or make multiple requests (max 1000 candles per call)
### GET /api/v1/market-data/instrument-types
Optional param: `instrumentTypeIds` (comma-separated list to filter).
Response: `{ instrumentTypes: [ { instrumentTypeID, instrumentTypeDescription } ] }`
Use: resolve `instrumentTypeID` integers to human-readable names (e.g. 5 = "Currencies").
### GET /api/v1/market-data/stocks-industries
Optional param: `stocksIndustryIds` (comma-separated).
Response: `{ stocksIndustries: [ { industryID, industryName } ] }`
Use: resolve `stocksIndustryId` integers from instrument metadata.
---
## Field Casing — Critical Reference
| Context | Casing style | Examples |
|---------|-------------|---------|
| Execution request body | PascalCase + capital ID | `InstrumentID`, `IsBuy`, `Amount`, `StopLossRate` |
| Execution response (open) | camelCase + capital ID | `orderID`, `instrumentID`, `statusID`, `CID` |
| Execution response (close) | camelCase + capital ID | `positionID`, `orderID`, `instrumentID` |
| Portfolio positions | PascalCase + capital ID | `positionID`, `instrumentID`, `CID`, `orderID`, `redeemStatusID` |
| Portfolio wrapper `/portfolio` | camelCase | `credit`, `positions`, `ordersForOpen` |
| Portfolio wrapper `/pnl` | camelCase | `credits` ← different name! |
| Trade history | camelCase + lowercase id | `positionId`, `instrumentId`, `openTimestamp` |
| Market data rates | camelCase + capital ID | `instrumentID`, `conversionRateAsk` |
| Market data metadata | PascalCase + capital ID (mostly) | `instrumentID`, `exchangeID` — except `stocksIndustryId` (lowercase) |
| Market data search response | camelCase + lowercase id | `instrumentId`, `displayname`, `exchangeID` (mixed!) |
| Candles outer | camelCase + lowercase id | `instrumentId` |
| Candles inner | camelCase + capital ID | `instrumentID` |
---
## Instrument Resolution — Best Practice
**Source:** https://api-portal.etoro.com/guides/get-instrument-id.md
Official recommendation: use `internalSymbolFull` filter param for exact match, always verify
`item.internalSymbolFull === symbol` in results to guard against partial matches.
**Correct call:**
```
GET /market-data/search?internalSymbolFull=GER40&fields=instrumentId,internalSymbolFull,displayname
```
**Instrument IDs are immutable** — they never change even if ticker/name changes.
Caching in `instrument_map.yaml` is correct. Fetch once, trust forever.
**Our `InstrumentResolver` issue:** Uses `searchText` (fuzzy) and likely missing the required
`fields` param. This was almost certainly the cause of empty results seen in earlier tests.
Fix both: add `fields` param, add `internalSymbolFull` exact-match strategy.
---
## Confirmed Working Endpoints (empirical, 2026-03-12)
| Endpoint | Key | Result | Notes |
|----------|-----|--------|-------|
| GET `/api/v1/watchlists` | Real Read | ✅ 200 | Connection test |
| GET `/api/v1/trading/info/portfolio` | Real Write | ✅ 200 | Real account data |
| GET `/api/v1/trading/info/real/pnl` | Real Write | ✅ 200 | Real account data |
| GET `/api/v1/trading/info/trade/history` | Real Write | ✅ 200 | Demo trades appear here |
| GET `/api/v1/market-data/search?searchText=GER40` | Any | ✅ 200 | (missing fields param — may be incomplete) |
| GET `/api/v1/market-data/instruments?instrumentIds=32` | Any | ✅ 200 | DAX confirmed |
| GET `/api/v1/trading/info/demo/pnl` | Real Write | ❌ 403 | Wrong key type |
| GET `/api/v1/trading/info/demo/portfolio` | Real Write | ❌ 403 | Wrong key type |
---
## What NOT To Do
- Do NOT call `/demo/pnl` or `/demo/portfolio` with a Real key → 403
- Do NOT call `/trading/info/market-data/search` without the required `fields` param → empty/error
- Do NOT use `from` or `fromDate` for trade history → use `minDate=YYYY-MM-DD`
- Do NOT use Read-only key for `fetch_closed_trades` → 403
- Do NOT refactor `_make_request()` — retry logic is solid, do not touch
- Do NOT assume `positionID` is in the open-order response — it is NOT, use order info endpoint
- Do NOT send `InstrumentId` (lowercase d) in close body — must be `InstrumentID`
- Do NOT confuse `credit` (portfolio endpoint) with `credits` (pnl endpoint) — different names
- Do NOT set `deployment_status = LIVE_APPROVED` in code — operator-only action
- Do NOT use `print()` — use `logger.info/debug`
- Do NOT skip pre-trade rate check — validate SL/TP levels against current ask/bid first
- Do NOT assume `stocksIndustryId` follows the capital-ID rule — it uses lowercase id (confirmed quirk)
---
## Open Issues
| ID | Description | Priority | File |
|----|-------------|----------|------|
| PORT-FIELDS | `OpenPosition` aliases wrong — `positionId` → `positionID`, `instrumentId` → `instrumentID` | P0 | `models/portfolio.py` |
| CLIENT-PORTFOLIO | `get_portfolio()` uses wrong endpoint for demo key | P0 | `client/client.py` |
| CLIENT-CASING | `close_position()` body sends `InstrumentId` — must be `InstrumentID` | P0 | `client/client.py` |
| CLIENT-ORDER-INFO | Missing `get_order_info(order_id)` method for positionID resolution | P0 | `client/client.py` |
| ROUTER-TWO-STEP | `open_position()` must call order info after placing order to get positionID | P0 | `execution/order_router.py` |
| RESOLVER-FIELDS | `InstrumentResolver` missing required `fields` param on search calls | P1 | `enrichment/instrument_resolver.py` |
| RESOLVER-EXACT | `InstrumentResolver` uses fuzzy `searchText` — add `internalSymbolFull` exact-match | P1 | `enrichment/instrument_resolver.py` |
| B9O-009 | V2 shared memory for backtester | Deferred | — |
| WINZIP-32 | WinError 32 on GA temp YAMLs | Cosmetic | — |
---
## Package Structure
```
src/broker_support/
  client/client.py              ← EToroClient — _make_request() is core, do not refactor
  config/settings.py            ← Pydantic settings, loads broker_settings.env
  models/trade.py               ← Trade model (camelCase aliases — trade history)
  models/portfolio.py           ← OpenPosition (PascalCase aliases — NEEDS FIX per PORT-FIELDS)
  tracking/csv_journal.py       ← CSVJournal (dedup, header-on-empty)
  tracking/position_tracker.py  ← PositionTracker (snapshot diff + enrich + journal)
  enrichment/instrument_resolver.py ← YAML primary + API fallback (NEEDS fields + exact-match fix)
  enrichment/trade_enricher.py      ← RESULT A: history search by positionId
  execution/order_router.py     ← Step 5 (written, needs two-step open + client fixes)
  utils/time_utils.py           ← is_trading_hours(), seconds_until_open()

configs/broker_support/
  instrument_map.yaml           ← DAX: {instrument_id: 32, symbol_full: GER40}
  broker_settings.env           ← ETORO_API_KEY + ETORO_USER_KEY (must be Demo Write key)

scripts/broker_support/
  run_tracker_loop.py           ← polling loop (5 min, hours guard 08:00-22:00 CET)
  inspect_portfolio.py          ← diagnostic — re-run with Demo key to verify field names
  inspect_instruments.py        ← diagnostic (uses searchText — needs fields param fix)
```
---
## Pending Documentation
- Group 2: Real account trading endpoints — deferred until Phase 2 live trading
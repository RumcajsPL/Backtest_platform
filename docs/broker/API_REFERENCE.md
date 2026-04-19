# API_REFERENCE.md
# eToro Public API — Complete Reference
# Source: BROKER_INTEGRATION.md (empirical) + ETORO_API_DUMP.md (OpenAPI v1.156.0)
# Last updated: 2026-03-12
# Base URL: https://public-api.etoro.com/api/v1
---
## Authentication
Three headers required on **every** request:
| Header | Description |
|--------|-------------|
| `x-api-key` | Public API Key — app-level, shared |
| `x-user-key` | User Key — determines demo vs real account |
| `x-request-id` | UUID — unique per request, generate fresh every call |
### Key Types
| Key type | Permissions | Account |
|----------|-------------|---------|
| Demo Read | Read only | Demo account |
| Demo Write | Read + Write | Demo account + demo execution |
| Real Read | Read only | Real account |
| Real Write | Read + Write | Real account + real execution |
**Critical:** Key type determines which account's data is returned — not the URL prefix.
Demo key required for all `/demo/` endpoints. Real key → 403 on `/demo/` endpoints.
**CTP setup:** `ETORO_API_KEY` = Public Key, `ETORO_USER_KEY` = Demo Write key.
---
## Field Casing Reference
| Context | Style | Examples |
|---------|-------|---------|
| Execution request body | PascalCase + capital ID | `InstrumentID`, `IsBuy`, `Amount`, `StopLossRate` |
| Execution response (open) | camelCase + capital ID | `orderID`, `instrumentID`, `statusID`, `CID` |
| Execution response (close) | camelCase + capital ID | `positionID`, `orderID`, `instrumentID` |
| Portfolio positions (`/portfolio`) | PascalCase + capital ID | `positionID`, `instrumentID`, `CID`, `orderID` |
| Portfolio wrapper (`/portfolio`) | camelCase | `credit`, `positions`, `ordersForOpen` |
| Portfolio wrapper (`/pnl`) | camelCase | `credits` ← different name! |
| Trade history | camelCase + lowercase id | `positionId`, `instrumentId`, `openTimestamp` |
| Market data rates | camelCase + capital ID | `instrumentID`, `conversionRateAsk` |
| Market data metadata | PascalCase + capital ID (mostly) | `instrumentID`, `exchangeID`; except `stocksIndustryId` |
| Market data search response | mixed | `instrumentId` (lowercase), `exchangeID` (capital) |
| Candles outer wrapper | camelCase + lowercase id | `instrumentId` |
| Candles inner objects | camelCase + capital ID | `instrumentID` |
---
## Endpoint Map
| Group | Method | Endpoint | Key required |
|-------|--------|----------|--------------|
| **Demo — Portfolio** | GET | `/trading/info/demo/portfolio` | Demo Write |
| **Demo — Portfolio + PnL** | GET | `/trading/info/demo/pnl` | Demo Write |
| **Demo — Order Info** | GET | `/trading/info/demo/orders/{orderId}` | Demo Write |
| **Demo — Open by Amount** | POST | `/trading/execution/demo/market-open-orders/by-amount` | Demo Write |
| **Demo — Open by Units** | POST | `/trading/execution/demo/market-open-orders/by-units` | Demo Write |
| **Demo — Close Position** | POST | `/trading/execution/demo/market-close-orders/positions/{positionId}` | Demo Write |
| **Demo — Cancel Open Order** | DELETE | `/trading/execution/demo/market-open-orders/{orderId}` | Demo Write |
| **Demo — Cancel Close Order** | DELETE | `/trading/execution/demo/market-close-orders/{orderId}` | Demo Write |
| **Demo — MIT Order** | POST | `/trading/execution/demo/limit-orders` | Demo Write |
| **Demo — Cancel MIT Order** | DELETE | `/trading/execution/demo/limit-orders/{orderId}` | Demo Write |
| **Real — Portfolio** | GET | `/trading/info/portfolio` | Real Write |
| **Real — Portfolio + PnL** | GET | `/trading/info/real/pnl` | Real Write |
| **Real — Open by Amount** | POST | `/trading/execution/market-open-orders/by-amount` | Real Write |
| **Real — Open by Units** | POST | `/trading/execution/market-open-orders/by-units` | Real Write |
| **Real — Close Position** | POST | `/trading/execution/market-close-orders/positions/{positionId}` | Real Write |
| **Real — Cancel Open Order** | DELETE | `/trading/execution/market-open-orders/{orderId}` | Real Write |
| **Real — MIT Order** | POST | `/trading/execution/limit-orders` | Real Write |
| **Real — Cancel MIT Order** | DELETE | `/trading/execution/limit-orders/{orderId}` | Real Write |
| **Trade History** | GET | `/trading/info/trade/history` | Real Write |
| **Identity** | GET | `/me` | Any |
| **Instrument Search** | GET | `/market-data/search` | Any |
| **Instrument Metadata** | GET | `/market-data/instruments` | Any |
| **Instrument Rates** | GET | `/market-data/instruments/rates` | Any |
| **Historical Candles** | GET | `/market-data/instruments/{id}/history/candles/{dir}/{interval}/{count}` | Any |
| **Instrument Types** | GET | `/market-data/instrument-types` | Any |
| **Stocks Industries** | GET | `/market-data/stocks-industries` | Any |
| **Watchlists** | GET/POST/PUT/DELETE | `/watchlists` | Any Read |
| **Instrument Feed** | GET | `/feeds/instrument/{marketId}` | Any |
| **User Feed** | GET | `/feeds/user/{userId}` | Any |
| **User Profile** | GET | `/user-info/people` | Any |
| **User Gain History** | GET | `/user-info/people/{username}/gain` | Any |
| **User Live Portfolio** | GET | `/user-info/people/{username}/portfolio/live` | Any |
| **User Trade Info** | GET | `/user-info/people/{username}/tradeinfo` | Any |
| **Copiers Info** | GET | `/pi-data/copiers` | Any |
---
## SECTION 1 — Demo Trading Endpoints
### GET /trading/info/demo/portfolio
Returns full portfolio snapshot without PnL. Primary endpoint for position tracking/diffing.
**Response:** `{ clientPortfolio: { credit, positions[], orders[], ordersForOpen[], ordersForClose[], ordersForCloseMultiple[], mirrors[], bonusCredit } }`
**Top-level fields:**
| Field | Type | Notes |
|-------|------|-------|
| `credit` | float | Available cash (USD). ⚠️ Different name from `/pnl` which uses `credits` |
| `bonusCredit` | float | Bonus credit (USD) |
| `positions` | Position[] | Open positions |
| `mirrors` | Mirror[] | Copy trading configs |
| `orders` | Order[] | Pending MIT/limit orders |
| `ordersForOpen` | OrderForOpen[] | Active orders to open |
| `ordersForClose` | OrderForClose[] | Active orders to close |
| `ordersForCloseMultiple` | OrderForCloseMultiple[] | Active orders to close multiple |
### GET /trading/info/demo/pnl
Returns portfolio with unrealized PnL per position.
**Differences from `/portfolio`:**
- `credits` (not `credit`) for available cash
- Each position has `unrealizedPnL` sub-object
**Available Cash Formula:**
```
available_cash = credits
               - sum(ordersForOpen[i].amount for i where mirrorID == 0)
               - sum(orders[i].amount for all i)
```
**Total Invested Formula:**
```
total_invested = Σ(positions[i].amount)
               + Σ(mirrors[i].positions[j].amount)
               + Σ(mirrors[i].availableAmount - mirrors[i].closedPositionsNetProfit)
               + Σ(ordersForOpen[i].amount where mirrorID == 0)
               + Σ(orders[i].amount)
               + Σ(ordersForOpen[i].totalExternalCosts where mirrorID == 0)
```
**Profit/Loss Formula:**
```
profit_loss = Σ(positions[i].unrealizedPnL.pnL)
            + Σ(mirrors[i].positions[j].unrealizedPnL.pnL)
            + Σ(mirrors[i].closedPositionsNetProfit)
```
**Equity:**
```
equity = available_cash + total_invested + unrealized_pnl
```
---
### OpenPosition Schema (PascalCase + capital ID)
⚠️ Field names use PascalCase. Different from trade history (camelCase).
| API field | Type | Description |
|-----------|------|-------------|
| `positionID` | int | ← capital ID |
| `CID` | int | Customer ID |
| `openDateTime` | datetime | ISO 8601 UTC |
| `openRate` | float | Entry price |
| `instrumentID` | int | ← capital ID |
| `isBuy` | bool | true=long, false=short |
| `takeProfitRate` | float | TP trigger (0 = not set) |
| `stopLossRate` | float | SL trigger (0.0001 = not set) |
| `mirrorID` | int | 0 = manual trade |
| `parentPositionID` | int | 0 = manual trade |
| `amount` | float | USD invested (includes margin collateral) |
| `leverage` | int | Leverage multiplier |
| `orderID` | int | Order that opened this position |
| `orderType` | int | Order type |
| `units` | float | Number of units |
| `totalFees` | float | Overnight fees + dividends |
| `initialAmountInDollars` | float | Original investment |
| `isTslEnabled` | bool | Trailing stop loss active |
| `stopLossVersion` | int | Increments on each SL edit |
| `redeemStatusID` | int | Redeem process status |
| `initialUnits` | float | Original units |
| `isPartiallyAltered` | bool | True if partially closed |
| `unitsBaseValueDollars` | float | USD value of current units |
| `openPositionActionType` | int | Reason position was opened |
| `settlementTypeID` | int | 0=CFD, 1=Real Asset, 2=SWAP, 3=Crypto Margin, 4=Future |
| `isDetached` | bool | True if detached from mirror |
| `openConversionRate` | float | FX rate asset→USD at open |
| `pnlVersion` | int | P&L formula version |
| `totalExternalFees` | float | Ticket fees (excludes overnight) |
| `totalExternalTaxes` | float | SDRT etc |
| `isNoTakeProfit` | bool | ⚠️ **true = TP DISABLED** (inverted) |
| `isNoStopLoss` | bool | ⚠️ **true = SL DISABLED** (inverted) |
| `lotCount` | float | Lot count (futures only) |
| `unrealizedPnL` | object\|null | **Only in `/pnl` endpoint** |
**`unrealizedPnL` sub-object** (only from `/demo/pnl`):
| Field | Type | Description |
|-------|------|-------------|
| `pnL` | float | Unrealized P&L in USD |
| `pnlAssetCurrency` | float | Unrealized P&L in asset currency |
| `exposureInAccountCurrency` | float | Current exposure USD |
| `exposureInAssetCurrency` | float | Current exposure asset currency |
| `marginInAccountCurrency` | float | Margin USD |
| `marginInAssetCurrency` | float | Margin asset currency |
| `marginCurrencyId` | int | Currency ID for margin |
| `assetCurrencyId` | int | Currency ID for asset |
| `closeRate` | float | Current close rate |
| `closeConversionRate` | float | Current FX rate |
| `timestamp` | datetime | PnL calculation timestamp |
---
### GET /trading/info/demo/orders/{orderId}
Retrieve positionID(s) created by an open order. Step 2 of two-step open flow.
| Response field | Type | Description |
|----------------|------|-------------|
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
| `positionID` | int64 | ✅ Use this for all close calls |
| `orderType` | int | |
| `occurred` | datetime | Position open timestamp |
| `rate` | decimal | Execution price |
| `units` | decimal | Units |
| `conversionRate` | decimal | FX rate at open |
| `amount` | decimal | USD invested |
| `isOpen` | bool | True if currently open |
---
### Two-Step Open Position Flow
```
Step 1: POST /trading/execution/demo/market-open-orders/by-amount
        Body: { InstrumentID, IsBuy, Leverage, Amount, [StopLossRate, TakeProfitRate] }
        → response.orderForOpen.orderID  (e.g. 13902598)

Step 2: GET /trading/info/demo/orders/{orderID}
        Poll until response.statusID == 1 (Executed)
        → response.positions[0].positionID  (e.g. 9876543210)
```
`positionID` from Step 2 is used in all subsequent close calls and journaling.
⚠️ `positionID` is NOT in the open-order response — must poll order info.
---
### POST /trading/execution/demo/market-open-orders/by-amount
**Request body (PascalCase + capital ID):**
| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `InstrumentID` | int32 | ✅ | ← capital ID |
| `IsBuy` | bool | ✅ | true=long, false=short |
| `Leverage` | int32 | ✅ | |
| `Amount` | double | ✅ | USD amount |
| `StopLossRate` | double | ❌ | Must be worse than current price |
| `TakeProfitRate` | double | ❌ | Must be better than current price |
| `IsTslEnabled` | bool | ❌ | Trailing stop loss |
| `IsNoStopLoss` | bool | ❌ | true = disable SL |
| `IsNoTakeProfit` | bool | ❌ | true = disable TP |
**Response:**
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
### POST /trading/execution/demo/market-open-orders/by-units
Same as by-amount but replace `Amount` with `AmountInUnits` (double, required).
---
### POST /trading/execution/demo/market-close-orders/positions/{positionId}
**Path param:** `positionId` (int64)
**Request body:**

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `InstrumentID` | int32 | ✅ | ⚠️ capital ID — NOT `InstrumentId` |
| `UnitsToDeduct` | double | ❌ | null or omit = full close |
**Response:**
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
---
### DELETE /trading/execution/demo/market-open-orders/{orderId}
Cancel pending market-open order before execution. Response: `{ token: uuid }`
### DELETE /trading/execution/demo/market-close-orders/{orderId}
Cancel pending market-close order before execution. Response: `{ token: uuid }`
### POST /trading/execution/demo/limit-orders
Market-if-touched (MIT) order. Opens position when `Rate` trigger price is reached.
- Long: `Rate` must be lower than current price
- Short: `Rate` must be higher than current price
Required fields same as by-amount plus `Rate` (trigger price). Response: `{ token: uuid }`
### DELETE /trading/execution/demo/limit-orders/{orderId}
Cancel pending MIT order. Response: `{ token: uuid }`
---
## SECTION 2 — Real Account Trading Endpoints
All real endpoints mirror the demo structure but without `/demo/` prefix.
**Key:** Real Write required.
### GET /trading/info/portfolio
Real portfolio without PnL. Same schema as `/demo/portfolio`.
`clientPortfolio.credit` = available cash.
### GET /trading/info/real/pnl
Real portfolio with PnL. Same schema as `/demo/pnl`.
`clientPortfolio.credits` = available cash (different name from `/portfolio`).
### POST /trading/execution/market-open-orders/by-amount
Same schema as demo version. Body: `{ InstrumentID, IsBuy, Leverage, Amount, ... }`
### POST /trading/execution/market-open-orders/by-units
Same as demo. Body replaces `Amount` with `AmountInUnits`.
### POST /trading/execution/market-close-orders/positions/{positionId}
Same schema as demo. Body: `{ InstrumentId, UnitsToDeduct }`
⚠️ Real API OpenAPI shows `InstrumentId` (lowercase d). Demo confirmed requires `InstrumentID` (capital). Use capital for safety.
### DELETE /trading/execution/market-open-orders/{orderId}
Cancel pending real open order. Response: `{ token: uuid }`
### POST /trading/execution/limit-orders
Real MIT order. Same schema as demo limit-orders.
### DELETE /trading/execution/limit-orders/{orderId}
Cancel pending real MIT order. Response: `{ token: uuid }`
---
## SECTION 3 — Trade History
### GET /trading/info/trade/history
**Key:** Real Write (empirically confirmed — Demo key returns 403)
Demo trades appear here.
**Query params:**
| Param | Type | Required | Notes |
|-------|------|----------|-------|
| `minDate` | string | ✅ | YYYY-MM-DD format. NOT `from`, NOT `fromDate` |
| `page` | int | ❌ | 1-based |
| `pageSize` | int | ❌ | Default 100, max 100 |
**Response:** Array of trade objects directly (not wrapped in a key).
**Field mapping (camelCase + lowercase id):**
| API field | Python model field | Type | Notes |
|-----------|-------------------|------|-------|
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
## SECTION 4 — Market Data Endpoints
### GET /market-data/search
⚠️ **`fields` param is REQUIRED** — omit → empty/broken results.
**Query params:**
| Param | Required | Notes |
|-------|----------|-------|
| `fields` | ✅ | Comma-separated. e.g. `instrumentId,internalSymbolFull,displayname` |
| `searchText` | ❌ | Fuzzy text search |
| `internalSymbolFull` | ❌ | Exact symbol filter. Preferred for precision. |
| `pageSize` | ❌ | Results per page |
| `pageNumber` | ❌ | Page number |
| `sort` | ❌ | e.g. `popularityUniques7Day desc` |
**Response:** `{ page, pageSize, totalItems, items: [ Instrument, ... ] }`
**Instrument fields** (only returned if in `fields` param):
| Field | Type | Description |
|-------|------|-------------|
| `instrumentId` | int | ✅ Use in all other calls — lowercase id |
| `internalSymbolFull` | str | e.g. `GER40`, `AAPL` |
| `displayname` | str | Human-readable name |
| `instrumentTypeID` | int | Asset class |
| `instrumentType` | str | Asset class name |
| `exchangeID` | int | Exchange |
| `symbol` | str | Trading symbol |
| `isOpen` | bool | Exchange currently open |
| `isCurrentlyTradable` | bool | False = cannot trade now |
| `currentRate` | float | Current price |
| `dailyPriceChange` | float | Daily price change |
| `popularityUniques7Day` | int | Unique viewers last 7 days |
**Recommended usage:**
```
GET /market-data/search?internalSymbolFull=GER40&fields=instrumentId,internalSymbolFull,displayname
→ Find item where item.internalSymbolFull == 'GER40'
→ Use item.instrumentId
```
**Instrument IDs are immutable** — cache in `instrument_map.yaml`, trust forever.
---
### GET /market-data/instruments
**Query params (all optional, comma-separated):**
| Param | Description |
|-------|-------------|
| `instrumentIds` | Filter by instrument IDs |
| `exchangeIds` | Filter by exchange |
| `stocksIndustryIds` | Filter by industry |
| `instrumentTypeIds` | Filter by asset class |
**Response:** `{ instrumentDisplayDatas: [ { ... } ] }`
**Fields:**
| Field | Type | Notes |
|-------|------|-------|
| `instrumentID` | int | ← capital ID |
| `instrumentDisplayName` | str | e.g. `DAX (GER40 Index)` |
| `symbolFull` | str | e.g. `GER40` |
| `instrumentTypeID` | int | ← capital ID |
| `exchangeID` | int | ← capital ID |
| `stocksIndustryId` | int | ← lowercase id (inconsistent quirk) |
| `priceSource` | str | e.g. `Nasdaq`, `LSE`, `CME` |
| `hasExpirationDate` | bool | True for futures |
| `images` | Image[] | `{ instrumentID, width, height, uri, backgroundColor, textColor }` |
Confirmed: DAX = `instrumentID=32`, `symbolFull="GER40"`
---
### GET /market-data/instruments/rates
**Query params:**
| Param | Required | Notes |
|-------|----------|-------|
| `instrumentIds` | ✅ | Comma-separated. Max 100. |
**Response:** `{ rates: [ { ... } ] }`
**Fields:**
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
**Key usage:**
- `ask` = price to buy (long entry / short close)
- `bid` = price to sell (short entry / long close)
- Validate SL/TP against current ask/bid before placing order
---
### GET /market-data/instruments/{instrumentId}/history/candles/{direction}/{interval}/{candlesCount}
**Path params:**
| Param | Values | Description |
|-------|--------|-------------|
| `instrumentId` | int | Instrument ID |
| `direction` | `asc` \| `desc` | asc=oldest first, desc=newest first |
| `interval` | `OneMinute` `FiveMinutes` `TenMinutes` `FifteenMinutes` `ThirtyMinutes` `OneHour` `FourHours` `OneDay` `OneWeek` | Candle timeframe |
| `candlesCount` | int (max 1000) | Number of candles per request |

**Candle count limit and pagination:**
Hard maximum is **1000 bars per single request** — enforced server-side.
This limit applies equally across all timeframes (1-min, 10-min, 4H, etc.).
To fetch a window longer than 1000 bars, issue multiple requests with
date offsets (fromDate/toDate or equivalent). No single-call workaround exists.
CTP current config: 500 strategy bars + 120 HTF bars — well within limit.

**Supported timeframes (confirmed):**
Highest available timeframe is `OneWeek`. `OneMonth` is NOT available via the
broker candle endpoint. Daily and weekly bars are available. For applications
requiring monthly-equivalent data (e.g. ARTF risk normalisation), use
locally-stored monthly parquet — do not attempt to reconstruct from broker weekly.

**Historical OHLCV data availability — 1-hour lag:**
When fetching broker candles (or updating local historical parquet files),
data is available with approximately 1 hour of delay.
Example: data update run at 15:00 UTC on 2026-04-20 will return candles
covering up to 14:00 UTC on 2026-04-20. The most recent completed bar
may not yet appear if the update runs close to the bar boundary.
Impact on validation: when comparing broker candles to local historical
parquet for the same period, ensure the comparison window ends at least
1 hour before the data update time to avoid spurious boundary mismatches.
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
      ]
    }
  ]
}
```
⚠️ Casing quirk: outer uses `instrumentId` (lowercase), inner candles use `instrumentID` (capital).
`volume` is 0 for most instruments. `fromDate` = candle open time (no `toDate`).
---
### GET /market-data/instrument-types
Optional: `instrumentTypeIds` (comma-separated).
Response: `{ instrumentTypes: [ { instrumentTypeID, instrumentTypeDescription } ] }`
### GET /market-data/stocks-industries
Optional: `stocksIndustryIds` (comma-separated).
Response: `{ stocksIndustries: [ { industryID, industryName } ] }`
---
## SECTION 5 — Identity & User Info
### GET /me
Returns authenticated user's IDs.
**Response:**
```json
{
  "gcid": 123456,
  "realCid": 789012,
  "demoCid": 345678
}
```
| Field | Description |
|-------|-------------|
| `gcid` | Global Customer ID — unique across all systems |
| `realCid` | Real account Customer ID |
| `demoCid` | Demo account Customer ID |
---
### GET /user-info/people
**Query params:**
| Param | Type | Required |
|-------|------|----------|
| `usernames` | string[] | ❌ |
| `cidList` | int[] | ❌ |
Returns array of user profiles with: `gcid`, `realCID`, `demoCID`, `username`, `language`, `country`, `isVerified`, `verificationLevel`, `accountStatus`, `avatars[]`, `userBio`, etc.
---
### GET /user-info/people/{username}/gain
Returns monthly and yearly performance data.
Response: `{ monthly: [ { timestamp, gain } ], yearly: [ { timestamp, gain } ] }`
---
### GET /user-info/people/{username}/portfolio/live
Returns live public portfolio. Response includes `positions[]` and `socialTrades[]`.
Position fields: `positionId`, `openTimestamp`, `openRate`, `instrumentId`, `isBuy`, `leverage`, `takeProfitRate`, `stopLossRate`, `netProfit`, etc.
---
### GET /user-info/people/{username}/tradeinfo
**Required query param:** `period` — one of: `CurrMonth`, `CurrQuarter`, `CurrYear`, `LastYear`, `LastTwoYears`, `OneMonthAgo`, `TwoMonthsAgo`, `ThreeMonthsAgo`, `SixMonthsAgo`, `OneYearAgo`
Returns: `gain`, `riskScore`, `copiers`, `winRatio`, `trades`, `dailyDd`, `weeklyDd`, `peakToValley`, `profitableMonthsPct`, etc.
---
### GET /pi-data/copiers
Returns list of copiers with demographic and financial info.
Fields per copier: `Gender`, `Club`, `Country`, `CopyStartedAtCategory`, `AmountCategory`, `AgeCategory`, `CopyRealizedEquity_pnl`, `AvailableCopyBalance`.
---
## SECTION 6 — Watchlists
### GET /api/v1/watchlists
Returns list of watchlists. Optional: `ensureBuiltinWatchlists` param.
### POST /api/v1/watchlists?name={name}
Create a new watchlist with given name.
### POST /api/v1/watchlists/{watchlistId}/items
Add instruments. Body: `[instrumentId1, instrumentId2, ...]`
### PUT /api/v1/watchlists/setUserSelectedUserDefault/{watchlistId}
Set default watchlist.
### DELETE /api/v1/watchlists/{watchlistId}
Delete watchlist and all items.
---
## SECTION 7 — Feeds
### GET /feeds/instrument/{marketId}
**Query params:** `take` (1-100, default 20), `offset` (default 0), `requesterUserId`, `reactionsPageSize` (1-50)
Response: `{ discussions: [ Discussion ], paging: { ... } }`
### GET /feeds/user/{userId}
Same params and schema as instrument feed.
---
## SECTION 8 — WebSocket API
**WebSocket URL:** `wss://ws.etoro.com/ws`
### Authentication
```json
{
  "id": "<uuid>",
  "operation": "Authenticate",
  "data": { "userKey": "<key>", "apiKey": "<key>" }
}
```
### Subscribe to Instrument Rates
```json
{
  "id": "<uuid>",
  "operation": "Subscribe",
  "data": { "topics": ["instrument:<instrumentId>"], "snapshot": false }
}
```
Rate message content fields: `Ask`, `Bid`, `LastExecution`, `Date`, `PriceRateID`
### Subscribe to Private (Transaction Updates)
```json
{
  "id": "<uuid>",
  "operation": "Subscribe",
  "data": { "topics": ["private"], "snapshot": false }
}
```
### Unsubscribe
```json
{ "id": "<uuid>", "operation": "Unsubscribe", "data": { "topics": ["instrument:100000"] } }
```
---
## SECTION 9 — Empirically Confirmed Facts (Override Docs)
| Endpoint | Key | Result | Notes |
|----------|-----|--------|-------|
| GET `/watchlists` | Real Read | ✅ 200 | Connection test |
| GET `/trading/info/portfolio` | Real Write | ✅ 200 | Real account |
| GET `/trading/info/real/pnl` | Real Write | ✅ 200 | Real account |
| GET `/trading/info/trade/history` | Real Write | ✅ 200 | Demo trades appear here |
| GET `/market-data/search?searchText=GER40` | Any | ✅ 200 | Missing fields → incomplete |
| GET `/market-data/instruments?instrumentIds=32` | Any | ✅ 200 | DAX confirmed |
| GET `/trading/info/demo/pnl` | Real Write | ❌ 403 | Wrong key type |
| GET `/trading/info/demo/portfolio` | Real Write | ❌ 403 | Wrong key type |
| GET candles (live, 2026-03-13) | Any | ✅ 200 | 500 1-min + 120 1H bars confirmed |
Candle OHLC fields can be None (confirmed 2026-03-13):
Key is present in the response but value is None — occurs for bars during market closure
or instrument suspension. bar.get("field", 0.0) does NOT handle this (default only fires
when key is absent). Use bar.get("field") or 0.0 instead.
Candles per request — hard maximum 1000 bars (confirmed 2026-03-13):
candlesCount path param is capped at 1000. Requests above this limit are rejected.
For windows > 1000 bars, multiple paginated requests with date offsets are required.
CTP current config: 500 strategy + 120 HTF — well within limit.
**Live position observed (2026-03-12):**
```
positionID: 3464232739, instrumentID: 32 (DAX/GER40)
isBuy: false, openConversionRate: 1.15137, settlementTypeID: 0 (CFD)
isNoTakeProfit: false = TP ENABLED; isNoStopLoss: false = SL ENABLED
strategy=500 bars [2026-03-13 06:38 → 14:57 UTC], htf=120 bars [2026-03-06 → 14:00 UTC]
Raw signals: buy=42, sell=12 → after filters: 1 (pass_rate=1.9%) — pipeline healthy
```
---
## SECTION 10 — What NOT To Do
- Do NOT call `/demo/pnl` or `/demo/portfolio` with a Real key → 403
- Do NOT omit `fields` param on market-data/search → empty/error
- Do NOT use `from` or `fromDate` for trade history → use `minDate=YYYY-MM-DD`
- Do NOT use Read-only key for `trade/history` → 403
- Do NOT assume `positionID` is in the open-order response → poll order info
- Do NOT send `InstrumentId` (lowercase d) in close body → use `InstrumentID`
- Do NOT confuse `credit` (portfolio) with `credits` (pnl) — different field names
- Do NOT set `deployment_status = LIVE_APPROVED` in code — operator-only
- Do NOT use `print()` → use `logger.info/debug`
- Do NOT skip pre-trade rate check → validate SL/TP against current ask/bid
- Do NOT assume `stocksIndustryId` follows the capital-ID rule → lowercase id quirk
- Do NOT use `fuzzy searchText` alone → add `internalSymbolFull` exact-match + `fields` param
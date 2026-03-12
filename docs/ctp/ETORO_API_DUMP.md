> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Open and close market orders

> Learn how to programmatically execute trades to enter and exit positions using the eToro API.

Once you have resolved your target asset's `instrumentId`, you can begin executing trades. The eToro API separates trading logic into two distinct phases: opening a new position and closing an existing one.

## Opening a Position

There are two ways to open a market position: by specifying the cash amount you wish to invest, or by specifying the number of units you wish to buy.

### Method 1: By Amount (Cash)

This is the most common method for dollar-cost averaging or fixed-budget strategies. You specify the cash value (e.g., \$1,000) and the API calculates the units based on the current market price.

**Endpoint:** `POST /api/v1/trading/execution/market-open-orders/by-amount`

> **Note:** You can apply additional settings such as leverage, stop-loss, and take-profit during this request.

### Method 2: By Units (Volume)

Use this method when you need to control the exact volume of the asset (e.g., buying exactly 1.5 Bitcoin or 10 shares of Apple).

**Endpoint:** `POST /api/v1/trading/execution/market-open-orders/by-units`

### Example Request (Open by Amount)

The following examples demonstrate the full flow: **Search** for 'BTC' to get its ID, then **Buy** \$1,000 worth of it.

<CodeGroup>
  ```bash cURL theme={null}
  # 1. Search for the Instrument ID (Symbol: BTC)
  # Returns a JSON object containing the "InstrumentID" (e.g., 100000)
  curl -X GET "https://public-api.etoro.com/api/v1/market-data/search?internalSymbolFull=BTC" \
    -H "x-api-key: <YOUR_PUBLIC_KEY>" \
    -H "x-user-key: <YOUR_USER_KEY>" \
    -H "x-request-id: <UUID>"

  # 2. Use the ID from step 1 (e.g., 100000) to place the order
  curl -X POST "https://public-api.etoro.com/api/v1/trading/execution/market-open-orders/by-amount" \
    -H "x-api-key: <YOUR_PUBLIC_KEY>" \
    -H "x-user-key: <YOUR_USER_KEY>" \
    -H "x-request-id: <UUID>" \
    -H "Content-Type: application/json" \
    -d '{
          "InstrumentId": 100000,
          "Amount": 1000,
          "Leverage": 1,
          "IsBuy": true
        }'
  ```

  ```javascript JavaScript theme={null}
  const crypto = require('crypto');

  const placeOrder = async () => {
    const symbol = 'BTC';
    const headers = {
      'x-api-key': '<YOUR_PUBLIC_KEY>',
      'x-user-key': '<YOUR_USER_KEY>',
      'x-request-id': crypto.randomUUID(), // or 'your-unique-uuid'
      'Content-Type': 'application/json'
    };

    try {
      // 1. Get Instrument ID
      const searchUrl = `https://public-api.etoro.com/api/v1/market-data/search?internalSymbolFull=${symbol}`;
      const searchRes = await fetch(searchUrl, { headers });
      const searchData = await searchRes.json();
      
      // Find exact match
      const instrument = searchData.items.find(i => i.internalSymbolFull === symbol);
      if (!instrument) throw new Error(`Instrument ${symbol} not found`);
      
      const instrumentId = instrument.instrumentId;
      console.log(`Resolved ${symbol} to ID: ${instrumentId}`);

      // 2. Place Order
      const orderUrl = 'https://public-api.etoro.com/api/v1/trading/execution/market-open-orders/by-amount';
      const orderBody = {
        InstrumentId: instrumentId,
        Amount: 1000,
        Leverage: 1,
        IsBuy: true
      };

      const orderRes = await fetch(orderUrl, {
        method: 'POST',
        headers,
        body: JSON.stringify(orderBody)
      });
      
      console.log("Order Response:", await orderRes.json());

    } catch (err) {
      console.error(err);
    }
  };

  placeOrder();
  ```

  ```python Python theme={null}
  import requests
  import uuid

  symbol = "BTC"
  headers = {
      "x-api-key": "<YOUR_PUBLIC_KEY>",
      "x-user-key": "<YOUR_USER_KEY>",
      "x-request-id": str(uuid.uuid4()),
      "Content-Type": "application/json"
  }

  # 1. Get Instrument ID
  search_url = "https://public-api.etoro.com/api/v1/market-data/search"
  search_res = requests.get(search_url, headers=headers, params={"internalSymbolFull": symbol})
  search_data = search_res.json()

  # Find exact match
  instrument = next((i for i in search_data['items'] if i['internalSymbolFull'] == symbol), None)

  if instrument:
      instrument_id = instrument['instrumentId']
      print(f"Resolved {symbol} to ID: {instrument_id}")

      # 2. Place Order
      order_url = "https://public-api.etoro.com/api/v1/trading/execution/market-open-orders/by-amount"
      payload = {
          "InstrumentId": instrument_id,
          "Amount": 1000,
          "Leverage": 1,
          "IsBuy": True
      }

      order_res = requests.post(order_url, json=payload, headers=headers)
      print("Order Response:", order_res.json())

  else:
      print(f"Instrument {symbol} not found")
  ```
</CodeGroup>

## Closing a Position

To close a trade, you must reference the specific `positionId` of the open position. You cannot simply "sell" the instrument; you must close the specific line item in your portfolio.

**Endpoint:** `POST /api/v1/trading/execution/market-close-orders/positions/{positionId}`

### Full vs. Partial Close

You can choose to close the entire position or just a portion of it.

* **Full Close:** Omit the `UnitsToDeduct` parameter or set it to `null`. This liquidates the entire position.
* **Partial Close:** Provide a specific value for `UnitsToDeduct`. Only that portion of the position will be closed, leaving the remainder active.

### Example Request (Close Position)

<CodeGroup>
  ```bash cURL theme={null}
  # Closing position ID 12345678
  curl -X POST "https://public-api.etoro.com/api/v1/trading/execution/market-close-orders/positions/12345678" \
    -H "x-api-key: <YOUR_PUBLIC_KEY>" \
    -H "x-user-key: <YOUR_USER_KEY>" \
    -H "x-request-id: <UUID>" \
    -H "Content-Type: application/json" \
    -d '{
          "UnitsToDeduct": null
        }'
  ```

  ```javascript JavaScript theme={null}
  const positionId = '12345678';
  const url = `https://public-api.etoro.com/api/v1/trading/execution/market-close-orders/positions/${positionId}`;

  fetch(url, {
    method: 'POST',
    headers: {
      'x-api-key': '<YOUR_PUBLIC_KEY>',
      'x-user-key': '<YOUR_USER_KEY>',
      'x-request-id': 'your-uuid-here',
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ UnitsToDeduct: null })
  })
  .then(res => res.json())
  .then(console.log)
  .catch(console.error);
  ```

  ```python Python theme={null}
  import requests
  import uuid

  position_id = "12345678"
  url = f"https://public-api.etoro.com/api/v1/trading/execution/market-close-orders/positions/{position_id}"
  headers = {
      "x-api-key": "<YOUR_PUBLIC_KEY>",
      "x-user-key": "<YOUR_USER_KEY>",
      "x-request-id": str(uuid.uuid4()),
      "Content-Type": "application/json"
  }

  payload = {
      "UnitsToDeduct": None
  }

  response = requests.post(url, json=payload, headers=headers)
  print(response.json())
  ```
</CodeGroup>

## Important Considerations

1. **Instrument IDs:** You must know the numeric `instrumentId` before placing an order. Use the Search endpoint to resolve tickers (e.g., AAPL) to IDs.
2. **Demo Environment:** When testing, ensure you use the demo endpoints (e.g., `/api/v1/trading/execution/demo/...`) to avoid risking real capital.
3. **Market Rates:** It is recommended to check current rates using `GET /instruments/rates` before executing orders to ensure price accuracy.


Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Calculate Total Invested

> Learn how to calculate your total invested amount in demo or real accounts.

Total invested represents the total amount of capital you have allocated across all positions and pending orders. It is calculated by summing all position amounts, mirror position amounts, mirror available amounts (adjusted for closed positions), and pending order amounts.

<Note>
  Total invested only refers to your USD balance.
</Note>

To retrieve the data needed for this calculation, use the P\&L endpoint for either demo or real accounts.

## The Calculation Process

Calculating total invested involves fetching your account data and summing all amounts allocated to positions and orders.

### 1. Endpoints

Use the **P\&L** endpoint to retrieve your account information.

**Demo Account:** `GET https://public-api.etoro.com/api/v1/trading/info/demo/pnl`

**Real Account:** `GET https://public-api.etoro.com/api/v1/trading/info/real/pnl`

### 2. Header Requirements

Remember to include your authentication headers with every request.

* `x-api-key`
* `x-user-key`
* `x-request-id`

### 3. Calculation Formula

```
Total Invested = Σ(positions[i].amount)
               + Σ(mirrors[i].positions[j].amount)
               + Σ(mirrors[i].availableAmount - mirrors[i].closedPositionsNetProfit)
               + Σ(ordersForOpen[i].amount where mirrorID = 0)
               + Σ(orders[i].amount)
               + Σ(ordersForOpen[i].totalExternalCosts where mirrorID = 0)
```

Where:

* `positions` is an array of your open positions
* `mirrors` is an array of your copy trading portfolios
* `mirrors[i].positions` is an array of positions within each mirror portfolio
* `mirrors[i].availableAmount` is the available amount in each mirror portfolio
* `mirrors[i].closedPositionsNetProfit` is the net profit from closed positions in each mirror portfolio
* `ordersForOpen` is an array of pending market orders (filtered to only include manual positions where `mirrorID = 0`)
* `orders` is an array of pending Market-if-touched orders
* `totalExternalCosts` is the total external costs for each order
* `amount` is the allocated amount for each position or order

## Examples

<CodeGroup>
  ```bash cURL theme={null}
  curl -X GET "https://public-api.etoro.com/api/v1/trading/info/demo/pnl" \
    -H "x-api-key: <YOUR_PUBLIC_KEY>" \
    -H "x-user-key: <YOUR_USER_KEY>" \
    -H "x-request-id: <UUID>"
  ```

  ```javascript JavaScript theme={null}
  const url = 'https://public-api.etoro.com/api/v1/trading/info/demo/pnl';

  fetch(url, options)
      .then(res => res.json())
      .then(data => {
          // Sum all position amounts
          const positionsAmount = data.positions.reduce((sum, pos) => sum + pos.amount, 0);
          
          // Sum all mirror position amounts and adjusted available amounts
          let mirrorsPositionsAmount = 0;
          let mirrorsAdjustedAmount = 0;
          data.mirrors.forEach(mirror => {
              mirrorsPositionsAmount += mirror.positions.reduce((sum, pos) => sum + pos.amount, 0);
              mirrorsAdjustedAmount += (mirror.availableAmount - mirror.closedPositionsNetProfit);
          });
          
          // Sum manual pending orders (mirrorID = 0)
          const ordersForOpenAmount = data.ordersForOpen
              .filter(order => order.mirrorID === 0)
              .reduce((sum, order) => sum + order.amount, 0);
          
          // Sum all orders
          const ordersAmount = data.orders.reduce((sum, order) => sum + order.amount, 0);
          
          // Sum external costs for manual pending orders
          const externalCosts = data.ordersForOpen
              .filter(order => order.mirrorID === 0)
              .reduce((sum, order) => sum + order.totalExternalCosts, 0);
          
          const totalInvested = positionsAmount + mirrorsPositionsAmount + mirrorsAdjustedAmount 
                              + ordersForOpenAmount + ordersAmount + externalCosts;
          
          console.log("Total Invested:", totalInvested);
      })
      .catch(err => console.error(err));
  ```

  ```python Python theme={null}
  import requests
  import uuid

  url = "https://public-api.etoro.com/api/v1/trading/info/demo/pnl"

  headers = {
      "x-api-key": "<YOUR_PUBLIC_KEY>",
      "x-user-key": "<YOUR_USER_KEY>",
      "x-request-id": str(uuid.uuid4())
  }

  response = requests.get(url, headers=headers)

  if response.status_code == 200:
      data = response.json()
      
      # Sum all position amounts
      positions_amount = sum(pos['amount'] for pos in data['positions'])
      
      # Sum all mirror position amounts and adjusted available amounts
      mirrors_positions_amount = sum(
          pos['amount'] 
          for mirror in data['mirrors'] 
          for pos in mirror['positions']
      )
      mirrors_adjusted_amount = sum(
          mirror['availableAmount'] - mirror['closedPositionsNetProfit']
          for mirror in data['mirrors']
      )
      
      # Sum manual pending orders (mirrorID = 0)
      orders_for_open_amount = sum(
          order['amount'] 
          for order in data['ordersForOpen'] 
          if order['mirrorID'] == 0
      )
      
      # Sum all orders
      orders_amount = sum(order['amount'] for order in data['orders'])
      
      # Sum external costs for manual pending orders
      external_costs = sum(
          order['totalExternalCosts'] 
          for order in data['ordersForOpen'] 
          if order['mirrorID'] == 0
      )
      
      total_invested = (positions_amount + mirrors_positions_amount + mirrors_adjusted_amount 
                       + orders_for_open_amount + orders_amount + external_costs)
      
      print(f"Total Invested: {total_invested}")
  else:
      print(f"Error: {response.status_code}")
  ```
</CodeGroup>

### Example Calculation

If your account has:

* `positions`: Two positions with `amount` values of 500 and 300
* `mirrors`: One mirror portfolio with:
  * Two positions with `amount` values of 200 and 150
  * `availableAmount`: 100
  * `closedPositionsNetProfit`: 50
* `ordersForOpen`: One manual pending order with `amount` of 200 and `totalExternalCosts` of 10 (where `mirrorID = 0`)
* `orders`: One existing order with `amount` value of 150

Then your total invested would be:

```
(500 + 300) + (200 + 150) + (100 - 50) + 200 + 150 + 10 = 1560
```

### Best Practices

1. **Monitor Your Investment:** Regularly check your total invested to understand your capital allocation across all positions and orders.
2. **Account for Mirror Portfolios:** Remember that mirror portfolios contribute to your total invested through both their positions and adjusted available amounts.
3. **Include External Costs:** Don't forget to include external costs from manual pending orders in your calculation.
4. **Use the Correct Environment:** Make sure to use the demo endpoint when testing and the real endpoint for live trading.


Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Calculate Profit/Loss

> Learn how to calculate your profit/loss (unrealized PnL) in demo or real accounts.

Profit/Loss represents your unrealized profit and loss across all open positions. It is calculated by summing the unrealized PnL from all your positions, mirror positions, and the net profit from closed positions in your mirror portfolios.

<Note>
  Profit/Loss appears as "Profit/Loss" in the app and represents your unrealized PnL.
</Note>

To retrieve the data needed for this calculation, use the P\&L endpoint for either demo or real accounts.

## The Calculation Process

Calculating profit/loss involves fetching your account data and summing all unrealized PnL values from positions and closed position profits.

### 1. Endpoints

Use the **P\&L** endpoint to retrieve your account information.

**Demo Account:** `GET https://public-api.etoro.com/api/v1/trading/info/demo/pnl`

**Real Account:** `GET https://public-api.etoro.com/api/v1/trading/info/real/pnl`

### 2. Header Requirements

Remember to include your authentication headers with every request.

* `x-api-key`
* `x-user-key`
* `x-request-id`

### 3. Calculation Formula

```
Profit/Loss = Σ(positions[i].unrealizedPnL.pnL)
            + Σ(mirrors[i].positions[j].unrealizedPnL.pnL)
            + Σ(mirrors[i].closedPositionsNetProfit)
```

Where:

* `positions` is an array of your open positions
* `positions[i].unrealizedPnL.pnL` is the unrealized profit/loss for each position
* `mirrors` is an array of your copy trading portfolios
* `mirrors[i].positions` is an array of positions within each mirror portfolio
* `mirrors[i].positions[j].unrealizedPnL.pnL` is the unrealized profit/loss for each mirror position
* `mirrors[i].closedPositionsNetProfit` is the net profit from closed positions in each mirror portfolio

## Examples

<CodeGroup>
  ```bash cURL theme={null}
  curl -X GET "https://public-api.etoro.com/api/v1/trading/info/demo/pnl" \
    -H "x-api-key: <YOUR_PUBLIC_KEY>" \
    -H "x-user-key: <YOUR_USER_KEY>" \
    -H "x-request-id: <UUID>"
  ```

  ```javascript JavaScript theme={null}
  const url = 'https://public-api.etoro.com/api/v1/trading/info/demo/pnl';

  fetch(url, options)
      .then(res => res.json())
      .then(data => {
          // Sum unrealized PnL from all positions
          const positionsPnL = data.positions.reduce((sum, pos) => sum + pos.unrealizedPnL.pnL, 0);
          
          // Sum unrealized PnL from mirror positions and closed positions net profit
          let mirrorsPnL = 0;
          let closedPositionsProfit = 0;
          data.mirrors.forEach(mirror => {
              mirrorsPnL += mirror.positions.reduce((sum, pos) => sum + pos.unrealizedPnL.pnL, 0);
              closedPositionsProfit += mirror.closedPositionsNetProfit;
          });
          
          const profitLoss = positionsPnL + mirrorsPnL + closedPositionsProfit;
          
          console.log("Profit/Loss:", profitLoss);
      })
      .catch(err => console.error(err));
  ```

  ```python Python theme={null}
  import requests
  import uuid

  url = "https://public-api.etoro.com/api/v1/trading/info/demo/pnl"

  headers = {
      "x-api-key": "<YOUR_PUBLIC_KEY>",
      "x-user-key": "<YOUR_USER_KEY>",
      "x-request-id": str(uuid.uuid4())
  }

  response = requests.get(url, headers=headers)

  if response.status_code == 200:
      data = response.json()
      
      # Sum unrealized PnL from all positions
      positions_pnl = sum(pos['unrealizedPnL']['pnL'] for pos in data['positions'])
      
      # Sum unrealized PnL from mirror positions
      mirrors_pnl = sum(
          pos['unrealizedPnL']['pnL']
          for mirror in data['mirrors']
          for pos in mirror['positions']
      )
      
      # Sum closed positions net profit
      closed_positions_profit = sum(
          mirror['closedPositionsNetProfit']
          for mirror in data['mirrors']
      )
      
      profit_loss = positions_pnl + mirrors_pnl + closed_positions_profit
      
      print(f"Profit/Loss: {profit_loss}")
  else:
      print(f"Error: {response.status_code}")
  ```
</CodeGroup>

### Example Calculation

If your account has:

* `positions`: Two positions with `unrealizedPnL.pnL` values of 50 and -20
* `mirrors`: One mirror portfolio with:
  * Two positions with `unrealizedPnL.pnL` values of 30 and 15
  * `closedPositionsNetProfit`: 100

Then your profit/loss would be:

```
(50 + (-20)) + (30 + 15) + 100 = 175
```

### Best Practices

1. **Monitor Regularly:** Check your profit/loss frequently to track the performance of your open positions.
2. **Understand Components:** Remember that profit/loss includes both unrealized PnL from open positions and realized profits from closed mirror positions.
3. **Consider Market Volatility:** Unrealized PnL can fluctuate with market conditions, so monitor it alongside your total invested amount.
4. **Use the Correct Environment:** Make sure to use the demo endpoint when testing and the real endpoint for live trading.


Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Calculate Equity

> Learn how to calculate your equity in demo or real accounts.

Equity represents the total value of your account, including your available cash, invested capital, and unrealized profit/loss. It is calculated by summing your available cash, total invested amount, and unrealized PnL.

<Note>
  Equity only refers to your USD balance.
</Note>

To retrieve the data needed for this calculation, use the P\&L endpoint for either demo or real accounts.

## The Calculation Process

Calculating equity involves fetching your account data and combining three key components: available cash, total invested, and unrealized PnL.

### 1. Endpoints

Use the **P\&L** endpoint to retrieve your account information.

**Demo Account:** `GET https://public-api.etoro.com/api/v1/trading/info/demo/pnl`

**Real Account:** `GET https://public-api.etoro.com/api/v1/trading/info/real/pnl`

### 2. Header Requirements

Remember to include your authentication headers with every request.

* `x-api-key`
* `x-user-key`
* `x-request-id`

### 3. Calculation Formula

```
Equity = Available Cash + Total Invested + Unrealized PnL
```

Where:

* **Available Cash** = `credits - (Σ(ordersForOpen[i].amount where mirrorID = 0) + Σ(orders[i].amount))`
* **Total Invested** = `Σ(positions[i].amount) + Σ(mirrors[i].positions[j].amount) + Σ(mirrors[i].availableAmount - mirrors[i].closedPositionsNetProfit) + Σ(ordersForOpen[i].amount where mirrorID = 0) + Σ(orders[i].amount) + Σ(ordersForOpen[i].totalExternalCosts where mirrorID = 0)`
* **Unrealized PnL** = `Σ(positions[i].unrealizedPnL.pnL) + Σ(mirrors[i].positions[j].unrealizedPnL.pnL) + Σ(mirrors[i].closedPositionsNetProfit)`

<Info>
  For detailed information on calculating each component, see:

  * [Calculate Available Cash](/guides/calculate-available-cash)
  * [Calculate Total Invested](/guides/calculate-total-invested)
  * [Calculate Profit/Loss](/guides/calculate-profit-loss)
</Info>

## Examples

<CodeGroup>
  ```bash cURL theme={null}
  curl -X GET "https://public-api.etoro.com/api/v1/trading/info/demo/pnl" \
    -H "x-api-key: <YOUR_PUBLIC_KEY>" \
    -H "x-user-key: <YOUR_USER_KEY>" \
    -H "x-request-id: <UUID>"
  ```

  ```javascript JavaScript theme={null}
  const url = 'https://public-api.etoro.com/api/v1/trading/info/demo/pnl';

  fetch(url, options)
      .then(res => res.json())
      .then(data => {
          // Calculate Available Cash
          const credits = data.credits;
          const ordersForOpenAmount = data.ordersForOpen
              .filter(order => order.mirrorID === 0)
              .reduce((sum, order) => sum + order.amount, 0);
          const ordersAmount = data.orders.reduce((sum, order) => sum + order.amount, 0);
          const availableCash = credits - (ordersForOpenAmount + ordersAmount);
          
          // Calculate Total Invested
          const positionsAmount = data.positions.reduce((sum, pos) => sum + pos.amount, 0);
          let mirrorsPositionsAmount = 0;
          let mirrorsAdjustedAmount = 0;
          data.mirrors.forEach(mirror => {
              mirrorsPositionsAmount += mirror.positions.reduce((sum, pos) => sum + pos.amount, 0);
              mirrorsAdjustedAmount += (mirror.availableAmount - mirror.closedPositionsNetProfit);
          });
          const externalCosts = data.ordersForOpen
              .filter(order => order.mirrorID === 0)
              .reduce((sum, order) => sum + order.totalExternalCosts, 0);
          const totalInvested = positionsAmount + mirrorsPositionsAmount + mirrorsAdjustedAmount 
                              + ordersForOpenAmount + ordersAmount + externalCosts;
          
          // Calculate Unrealized PnL
          const positionsPnL = data.positions.reduce((sum, pos) => sum + pos.unrealizedPnL.pnL, 0);
          let mirrorsPnL = 0;
          let closedPositionsProfit = 0;
          data.mirrors.forEach(mirror => {
              mirrorsPnL += mirror.positions.reduce((sum, pos) => sum + pos.unrealizedPnL.pnL, 0);
              closedPositionsProfit += mirror.closedPositionsNetProfit;
          });
          const unrealizedPnL = positionsPnL + mirrorsPnL + closedPositionsProfit;
          
          // Calculate Equity
          const equity = availableCash + totalInvested + unrealizedPnL;
          
          console.log("Equity:", equity);
      })
      .catch(err => console.error(err));
  ```

  ```python Python theme={null}
  import requests
  import uuid

  url = "https://public-api.etoro.com/api/v1/trading/info/demo/pnl"

  headers = {
      "x-api-key": "<YOUR_PUBLIC_KEY>",
      "x-user-key": "<YOUR_USER_KEY>",
      "x-request-id": str(uuid.uuid4())
  }

  response = requests.get(url, headers=headers)

  if response.status_code == 200:
      data = response.json()
      
      # Calculate Available Cash
      credits = data['credits']
      orders_for_open_amount = sum(order['amount'] for order in data['ordersForOpen'] if order['mirrorID'] == 0)
      orders_amount = sum(order['amount'] for order in data['orders'])
      available_cash = credits - (orders_for_open_amount + orders_amount)
      
      # Calculate Total Invested
      positions_amount = sum(pos['amount'] for pos in data['positions'])
      mirrors_positions_amount = sum(
          pos['amount'] 
          for mirror in data['mirrors'] 
          for pos in mirror['positions']
      )
      mirrors_adjusted_amount = sum(
          mirror['availableAmount'] - mirror['closedPositionsNetProfit']
          for mirror in data['mirrors']
      )
      external_costs = sum(
          order['totalExternalCosts'] 
          for order in data['ordersForOpen'] 
          if order['mirrorID'] == 0
      )
      total_invested = (positions_amount + mirrors_positions_amount + mirrors_adjusted_amount 
                       + orders_for_open_amount + orders_amount + external_costs)
      
      # Calculate Unrealized PnL
      positions_pnl = sum(pos['unrealizedPnL']['pnL'] for pos in data['positions'])
      mirrors_pnl = sum(
          pos['unrealizedPnL']['pnL']
          for mirror in data['mirrors']
          for pos in mirror['positions']
      )
      closed_positions_profit = sum(
          mirror['closedPositionsNetProfit']
          for mirror in data['mirrors']
      )
      unrealized_pnl = positions_pnl + mirrors_pnl + closed_positions_profit
      
      # Calculate Equity
      equity = available_cash + total_invested + unrealized_pnl
      
      print(f"Equity: {equity}")
  else:
      print(f"Error: {response.status_code}")
  ```
</CodeGroup>

### Example Calculation

If your account has:

* **Available Cash**: 450
* **Total Invested**: 1560
* **Unrealized PnL**: 175

Then your equity would be:

```
450 + 1560 + 175 = 2185
```

### Best Practices

1. **Monitor Total Account Value:** Equity gives you a complete picture of your account's total value, including both liquid and invested capital.
2. **Track Performance:** Compare your equity over time to understand your overall trading performance.
3. **Risk Management:** Use equity to calculate position sizes and manage risk appropriately.
4. **Use the Correct Environment:** Make sure to use the demo endpoint when testing and the real endpoint for live trading.


Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Manage your Watchlists

> Learn how to create custom watchlists, populate them with assets, and sync your preferences across the eToro ecosystem.

Watchlists are the primary way to organize and track groups of assets on eToro. By managing watchlists via the API, you can programmatically sync your external favorites, create custom sectors (e.g., "My AI Picks"), and control what appears on your main trading dashboard.

## 1. Get Your Watchlists

First, retrieve the list of watchlists currently associated with your account. This will provide you with the `watchlistId` needed for further management actions.

**Endpoint:** `GET /api/v1/watchlists`

<CodeGroup>
  ```bash cURL theme={null}
  curl -X GET "https://public-api.etoro.com/api/v1/watchlists" \
    -H "x-api-key: <YOUR_PUBLIC_KEY>" \
    -H "x-user-key: <YOUR_USER_KEY>" \
    -H "x-request-id: <UUID>"
  ```

  ```javascript JavaScript theme={null}
  fetch('https://public-api.etoro.com/api/v1/watchlists', {
      headers: {
          'x-api-key': '<YOUR_PUBLIC_KEY>',
          'x-user-key': '<YOUR_USER_KEY>',
          'x-request-id': 'your-uuid-here'
      }
  })
  .then(res => res.json())
  .then(console.log)
  .catch(console.error);
  ```

  ```python Python theme={null}
  import requests
  import uuid

  url = "https://public-api.etoro.com/api/v1/watchlists"
  headers = {
      "x-api-key": "<YOUR_PUBLIC_KEY>",
      "x-user-key": "<YOUR_USER_KEY>",
      "x-request-id": str(uuid.uuid4())
  }

  response = requests.get(url, headers=headers)
  print(response.json())
  ```
</CodeGroup>

> **Note:** You can use the `ensureBuiltinWatchlists` parameter to make sure system default lists are included in the response.

## 2. Create a New Watchlist

You can create a custom watchlist by specifying a name.

**Endpoint:** `POST /api/v1/watchlists`

| Parameter | Type  | Description                                                    |
| --------- | ----- | -------------------------------------------------------------- |
| `name`    | Query | The display name for your new watchlist (e.g., "Tech Stocks"). |

<CodeGroup>
  ```bash cURL theme={null}
  curl -X POST "https://public-api.etoro.com/api/v1/watchlists?name=Tech%20Stocks" \
    -H "x-api-key: <YOUR_PUBLIC_KEY>" \
    -H "x-user-key: <YOUR_USER_KEY>" \
    -H "x-request-id: <UUID>"
  ```

  ```javascript JavaScript theme={null}
  const name = 'Tech Stocks';
  const url = `https://public-api.etoro.com/api/v1/watchlists?name=${encodeURIComponent(name)}`;

  fetch(url, {
      method: 'POST',
      headers: {
          'x-api-key': '<YOUR_PUBLIC_KEY>',
          'x-user-key': '<YOUR_USER_KEY>',
          'x-request-id': 'your-uuid-here'
      }
  })
  .then(res => res.json())
  .then(console.log)
  .catch(console.error);
  ```

  ```python Python theme={null}
  import requests
  import uuid

  url = "https://public-api.etoro.com/api/v1/watchlists"
  params = {"name": "Tech Stocks"}
  headers = {
      "x-api-key": "<YOUR_PUBLIC_KEY>",
      "x-user-key": "<YOUR_USER_KEY>",
      "x-request-id": str(uuid.uuid4())
  }

  response = requests.post(url, params=params, headers=headers)
  print(response.json())
  ```
</CodeGroup>

## 3. Add Instruments to a Watchlist

Once a watchlist is created, you can add instruments to it using its `watchlistId`.

**Endpoint:** `POST /api/v1/watchlists/{watchlistId}/items`

<CodeGroup>
  ```bash cURL theme={null}
  curl -X POST "https://public-api.etoro.com/api/v1/watchlists/{watchlistId}/items" \
    -H "x-api-key: <YOUR_PUBLIC_KEY>" \
    -H "x-user-key: <YOUR_USER_KEY>" \
    -H "x-request-id: <UUID>" \
    -H "Content-Type: application/json" \
    -d '[1001, 1002]'
  ```

  ```javascript JavaScript theme={null}
  const watchlistId = '12345';
  const url = `https://public-api.etoro.com/api/v1/watchlists/${watchlistId}/items`;

  fetch(url, {
      method: 'POST',
      headers: {
          'x-api-key': '<YOUR_PUBLIC_KEY>',
          'x-user-key': '<YOUR_USER_KEY>',
          'x-request-id': 'your-uuid-here',
          'Content-Type': 'application/json'
      },
      body: JSON.stringify([1001, 1002])
  })
  .then(res => res.json())
  .then(console.log)
  .catch(console.error);
  ```

  ```python Python theme={null}
  import requests
  import uuid

  watchlist_id = "12345"
  url = f"https://public-api.etoro.com/api/v1/watchlists/{watchlist_id}/items"
  headers = {
      "x-api-key": "<YOUR_PUBLIC_KEY>",
      "x-user-key": "<YOUR_USER_KEY>",
      "x-request-id": str(uuid.uuid4()),
      "Content-Type": "application/json"
  }

  payload = [1001, 1002]

  # payload is a list of instrument IDs
  response = requests.post(url, json=payload, headers=headers)
  print(response.json())
  ```
</CodeGroup>

## 4. Set a Default Watchlist

The "Default" watchlist is the one that appears immediately when you log in.

**Endpoint:** `PUT /api/v1/watchlists/setUserSelectedUserDefault/{watchlistId}`

<CodeGroup>
  ```bash cURL theme={null}
  curl -X PUT "https://public-api.etoro.com/api/v1/watchlists/setUserSelectedUserDefault/12345" \
    -H "x-api-key: <YOUR_PUBLIC_KEY>" \
    -H "x-user-key: <YOUR_USER_KEY>" \
    -H "x-request-id: <UUID>"
  ```

  ```javascript JavaScript theme={null}
  const watchlistId = '12345';
  const url = `https://public-api.etoro.com/api/v1/watchlists/setUserSelectedUserDefault/${watchlistId}`;

  fetch(url, {
      method: 'PUT',
      headers: {
          'x-api-key': '<YOUR_PUBLIC_KEY>',
          'x-user-key': '<YOUR_USER_KEY>',
          'x-request-id': 'your-uuid-here'
      }
  })
  .then(res => res.json())
  .then(console.log)
  .catch(console.error);
  ```

  ```python Python theme={null}
  import requests
  import uuid

  watchlist_id = "12345"
  url = f"https://public-api.etoro.com/api/v1/watchlists/setUserSelectedUserDefault/{watchlist_id}"
  headers = {
      "x-api-key": "<YOUR_PUBLIC_KEY>",
      "x-user-key": "<YOUR_USER_KEY>",
      "x-request-id": str(uuid.uuid4())
  }

  response = requests.put(url, headers=headers)
  print(response.json())
  ```
</CodeGroup>

## 5. Delete a Watchlist

Remove a watchlist and all its contained items permanently.

**Endpoint:** `DELETE /api/v1/watchlists/{watchlistId}`

<CodeGroup>
  ```bash cURL theme={null}
  curl -X DELETE "https://public-api.etoro.com/api/v1/watchlists/12345" \
    -H "x-api-key: <YOUR_PUBLIC_KEY>" \
    -H "x-user-key: <YOUR_USER_KEY>" \
    -H "x-request-id: <UUID>"
  ```

  ```javascript JavaScript theme={null}
  const watchlistId = '12345';
  const url = `https://public-api.etoro.com/api/v1/watchlists/${watchlistId}`;

  fetch(url, {
      method: 'DELETE',
      headers: {
          'x-api-key': '<YOUR_PUBLIC_KEY>',
          'x-user-key': '<YOUR_USER_KEY>',
          'x-request-id': 'your-uuid-here'
      }
  })
  .then(res => res.json())
  .then(console.log)
  .catch(console.error);
  ```

  ```python Python theme={null}
  import requests
  import uuid

  watchlist_id = "12345"
  url = f"https://public-api.etoro.com/api/v1/watchlists/{watchlist_id}"
  headers = {
      "x-api-key": "<YOUR_PUBLIC_KEY>",
      "x-user-key": "<YOUR_USER_KEY>",
      "x-request-id": str(uuid.uuid4())
  }

  response = requests.delete(url, headers=headers)
  print(response.json())
  ```
</CodeGroup>


Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Get instrument feed posts

> Retrieves feed posts associated with a specific financial instrument. The feed includes discussions, analyses, and other content related to the instrument.



## OpenAPI

````yaml api-reference/openapi.json get /api/v1/feeds/instrument/{marketId}
openapi: 3.0.1
info:
  title: eToro Api
  version: v1.156.0
  description: >-
    eToro’s public API provides access to real-time financial data, trading
    insights, and account management features, allowing developers to integrate
    eToro’s services into their applications. With access to market prices,
    historical data, and social trading information, the API empowers users to
    enhance their trading strategies. Designed for security and scalability, the
    eToro API ensures smooth and reliable integration for a variety of financial
    applications.


    For more details on integrating with eToro's public WebSocket service,
    please refer to the dedicated [WebSocket
    documentation](./websocket/websocket-doc.html).
servers:
  - url: https://public-api.etoro.com
    description: eToro Public API
security: []
paths:
  /api/v1/feeds/instrument/{marketId}:
    get:
      tags:
        - Feeds
      summary: Get instrument feed posts
      description: >-
        Retrieves feed posts associated with a specific financial instrument.
        The feed includes discussions, analyses, and other content related to
        the instrument.
      parameters:
        - name: x-request-id
          in: header
          required: true
          schema:
            type: string
            format: uuid
            example: 755f6cd5-90ce-4c07-98bd-90c9d897af7a
          description: A unique request identifier.
        - name: x-api-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: lhgfaslk21490FAScVPkdsb53F9dNkfHG4faZSG5vfjndfcfgdssdgsdHF4663
          description: API key for authentication.
        - name: x-user-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: >-
              eyJlYW4iOiJVbnJlZ2lzdGVyZWRBcHBsaWNhdGlvbiIsImVrIjoiOE5sZ2cwcW5EUVdROUFNWGpXT2lmOWktZnpidG5KcUlqWGJ3WHJZZkpZcldrbG90ZEhvLVBjSWhQaU8xU1ZtMW84aU1WZGZqN2xWNzFjLXFxLmcybXE1dnh4Q1hUT25xaWRUaTFlcEhmVk1fIn0_
          description: User-specific authentication key.
        - name: marketId
          in: path
          description: >-
            Unique identifier of the financial instrument/market to retrieve
            feed posts for
          required: true
          schema:
            type: string
          example: '123456'
        - name: requesterUserId
          in: query
          description: >-
            ID of the user making the request. Used for personalization and
            permission checks.
          schema:
            type: string
          example: '7890'
        - name: take
          in: query
          description: Number of feed posts to retrieve. Used for pagination.
          schema:
            type: integer
            format: int32
            default: 20
            minimum: 1
            maximum: 100
          example: 20
        - name: badgesExperimentIsEnabled
          in: query
          description: >-
            Flag indicating whether to include user badges in the response. Part
            of badges feature experiment.
          schema:
            type: boolean
            default: false
        - name: offset
          in: query
          description: >-
            Number of feed posts to skip. Used for pagination in combination
            with take parameter.
          schema:
            type: integer
            format: int32
            default: 0
            minimum: 0
        - name: reactionsPageSize
          in: query
          description: >-
            Number of reactions to include per post. Controls the pagination of
            post reactions.
          schema:
            type: integer
            format: int32
            default: 10
            minimum: 1
            maximum: 50
      responses:
        '200':
          description: Successfully retrieved instrument feed posts
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/DiscussionsResponse'
components:
  schemas:
    DiscussionsResponse:
      type: object
      properties:
        discussions:
          type: array
          items:
            $ref: '#/components/schemas/Discussion'
        paging:
          type: object
          properties:
            next:
              type: string
            offSet:
              type: integer
            take:
              type: integer
            version:
              type: string
        metadata:
          type: object
          properties:
            experimentName:
              type: string
            streamType:
              type: string
            designatedStreamType:
              type: string
    Discussion:
      type: object
      properties:
        id:
          type: string
        post:
          $ref: '#/components/schemas/DiscussionsPost'
        commentsData:
          type: object
          properties:
            reactionPaging:
              type: object
              properties:
                totalCount:
                  type: integer
            comments:
              type: array
              items:
                $ref: '#/components/schemas/Comment'
        emotionsData:
          type: object
          properties:
            like:
              type: object
              properties:
                paging:
                  type: object
                  properties:
                    totalCount:
                      type: integer
                emotions:
                  type: array
                  items:
                    $ref: '#/components/schemas/Emotion'
        requesterContext:
          type: object
          properties:
            isFlaggingAsSpam:
              type: boolean
            isSubscribed:
              type: boolean
            isLiking:
              type: boolean
            isSaved:
              type: boolean
            isPinned:
              type: boolean
            isRequesterBlocking:
              type: boolean
            isInteractionRestricted:
              type: boolean
            isFollowing:
              type: boolean
        summary:
          type: object
          properties:
            totalCommentsAndReplies:
              type: integer
            sharedCount:
              type: integer
        reason:
          nullable: true
          description: >-
            Reason for the discussion being shown - can be a string or an object
            with sourceId, owner, and type fields
    DiscussionsPost:
      type: object
      properties:
        id:
          type: string
        owner:
          $ref: '#/components/schemas/User'
        obsoleteId:
          type: string
          description: Legacy identifier for backward compatibility
        message:
          type: object
          properties:
            text:
              type: string
            languageCode:
              type: string
        created:
          type: string
          format: date-time
        updated:
          type: string
          format: date-time
        type:
          type: string
        isDeleted:
          type: boolean
          description: Whether the post has been deleted
        isSpam:
          type: boolean
          description: Whether the post is flagged as spam
        editStatus:
          type: string
          description: Edit status of the post
          enum:
            - None
            - Edited
            - Moderated
        attachments:
          type: array
          items:
            $ref: '#/components/schemas/Attachment'
        tags:
          type: array
          items:
            type: object
            properties:
              market:
                type: object
                properties:
                  id:
                    type: string
                  symbolName:
                    type: string
                  displayName:
                    type: string
                  updated:
                    type: string
                  assetType:
                    type: string
                  internalId:
                    type: integer
                  avatar:
                    type: object
                    properties:
                      small:
                        type: string
                      medium:
                        type: string
                      large:
                        type: string
                      svg:
                        type: object
                        nullable: true
                        properties:
                          url:
                            type: string
                          backgroundColor:
                            type: string
                          textColor:
                            type: string
                  application:
                    type: string
                  metadata:
                    type: string
                  assetTypeId:
                    type: integer
                  assetTypeSubCategoryId:
                    type: integer
        mentions:
          type: array
          items:
            type: object
            properties:
              user:
                $ref: '#/components/schemas/User'
              isDirect:
                type: boolean
        metadata:
          type: object
          description: Additional metadata about the post
          properties:
            share:
              type: object
              properties:
                sharedPost:
                  type: string
                sharedOriginPost:
                  type: string
            poll:
              type: object
              properties:
                id:
                  type: integer
                title:
                  type: string
                gcid:
                  type: integer
                options:
                  type: array
                  items:
                    type: object
                    properties:
                      id:
                        type: integer
                      index:
                        type: integer
                      text:
                        type: string
                      isUserVoted:
                        type: boolean
                      votesCount:
                        type: integer
    Comment:
      type: object
      properties:
        entity:
          type: object
          properties:
            message:
              type: object
              properties:
                text:
                  type: string
                languageCode:
                  type: string
            id:
              type: string
            owner:
              $ref: '#/components/schemas/User'
            created:
              type: string
              format: date-time
            obsoleteId:
              type: string
            attachments:
              type: array
              items:
                $ref: '#/components/schemas/Attachment'
            isSpam:
              type: boolean
            editStatus:
              type: string
              enum:
                - None
                - Edited
                - Moderated
            parent:
              type: object
              description: Parent post reference
              properties:
                id:
                  type: string
                obsoleteId:
                  type: string
                type:
                  type: string
        repliesCount:
          type: integer
        replies:
          type: array
          items:
            type: object
        emotionsData:
          type: object
          properties:
            like:
              type: object
              properties:
                paging:
                  type: object
                  properties:
                    totalCount:
                      type: integer
                emotions:
                  type: array
                  items:
                    $ref: '#/components/schemas/Emotion'
        requesterContext:
          type: object
          properties:
            isFlaggingAsSpam:
              type: boolean
            isSubscribed:
              type: boolean
            isLiking:
              type: boolean
            isSaved:
              type: boolean
            isPinned:
              type: boolean
            isRequesterBlocking:
              type: boolean
            isInteractionRestricted:
              type: boolean
            isFollowing:
              type: boolean
    Emotion:
      type: object
      properties:
        type:
          type: string
        id:
          type: string
        owner:
          $ref: '#/components/schemas/User'
        obsoleteId:
          type: string
        parent:
          type: object
          properties:
            id:
              type: string
            obsoleteId:
              type: string
            type:
              type: string
        created:
          type: string
          format: date-time
    User:
      type: object
      properties:
        id:
          type: string
        username:
          type: string
        firstName:
          type: string
        lastName:
          type: string
        avatar:
          type: object
          properties:
            small:
              type: string
              format: uri
            medium:
              type: string
              format: uri
            large:
              type: string
              format: uri
        roles:
          type: array
          items:
            type: string
        isBlocked:
          type: boolean
        isPrivate:
          type: boolean
        countryCode:
          type: integer
        piLevel:
          type: integer
    Attachment:
      type: object
      description: Represents a media attachment in a post
      properties:
        type:
          type: string
          enum:
            - image
            - video
            - link
          description: Type of the attachment
        url:
          type: string
          description: URL where the attachment content can be accessed
        thumbnailUrl:
          type: string
          description: URL of a thumbnail image for video attachments
        host:
          type: string
          description: Host domain of the attachment URL
        mediaType:
          type: string
          description: Type of media
          enum:
            - None
            - Image
            - Video
        media:
          type: object
          description: Media content details
        metadata:
          type: object
          description: Additional metadata about the attachment
          properties:
            width:
              type: integer
              description: Width of the media in pixels
            height:
              type: integer
              description: Height of the media in pixels
            duration:
              type: integer
              description: Duration in seconds for video attachments

````

Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Get user feed posts

> Retrieves feed posts associated with a specific user. The feed includes the user's discussions, analyses, and other content they have posted.



## OpenAPI

````yaml api-reference/openapi.json get /api/v1/feeds/user/{userId}
openapi: 3.0.1
info:
  title: eToro Api
  version: v1.156.0
  description: >-
    eToro’s public API provides access to real-time financial data, trading
    insights, and account management features, allowing developers to integrate
    eToro’s services into their applications. With access to market prices,
    historical data, and social trading information, the API empowers users to
    enhance their trading strategies. Designed for security and scalability, the
    eToro API ensures smooth and reliable integration for a variety of financial
    applications.


    For more details on integrating with eToro's public WebSocket service,
    please refer to the dedicated [WebSocket
    documentation](./websocket/websocket-doc.html).
servers:
  - url: https://public-api.etoro.com
    description: eToro Public API
security: []
paths:
  /api/v1/feeds/user/{userId}:
    get:
      tags:
        - Feeds
      summary: Get user feed posts
      description: >-
        Retrieves feed posts associated with a specific user. The feed includes
        the user's discussions, analyses, and other content they have posted.
      parameters:
        - name: x-request-id
          in: header
          required: true
          schema:
            type: string
            format: uuid
            example: 65b2e446-c20b-4e1e-84fd-e367009c083e
          description: A unique request identifier.
        - name: x-api-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: lhgfaslk21490FAScVPkdsb53F9dNkfHG4faZSG5vfjndfcfgdssdgsdHF4663
          description: API key for authentication.
        - name: x-user-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: >-
              eyJlYW4iOiJVbnJlZ2lzdGVyZWRBcHBsaWNhdGlvbiIsImVrIjoiOE5sZ2cwcW5EUVdROUFNWGpXT2lmOWktZnpidG5KcUlqWGJ3WHJZZkpZcldrbG90ZEhvLVBjSWhQaU8xU1ZtMW84aU1WZGZqN2xWNzFjLXFxLmcybXE1dnh4Q1hUT25xaWRUaTFlcEhmVk1fIn0_
          description: User-specific authentication key.
        - name: userId
          in: path
          description: ID of the user whose feed posts should be retrieved
          required: true
          schema:
            type: string
          example: '7890'
        - name: requesterUserId
          in: query
          description: >-
            ID of the user making the request. Used for personalization and
            permission checks.
          schema:
            type: string
          example: '1111'
        - name: take
          in: query
          description: Number of feed posts to retrieve. Used for pagination.
          schema:
            type: integer
            format: int32
            default: 20
            minimum: 1
            maximum: 100
        - name: badgesExperimentIsEnabled
          in: query
          description: >-
            Flag indicating whether to include user badges in the response. Part
            of badges feature experiment.
          schema:
            type: boolean
            default: false
        - name: offset
          in: query
          description: >-
            Number of feed posts to skip. Used for pagination in combination
            with take parameter.
          schema:
            type: integer
            format: int32
            default: 0
            minimum: 0
        - name: reactionsPageSize
          in: query
          description: >-
            Number of reactions to include per post. Controls the pagination of
            post reactions.
          schema:
            type: integer
            format: int32
            default: 10
            minimum: 1
            maximum: 50
      responses:
        '200':
          description: Successfully retrieved user feed posts
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/DiscussionsResponse'
components:
  schemas:
    DiscussionsResponse:
      type: object
      properties:
        discussions:
          type: array
          items:
            $ref: '#/components/schemas/Discussion'
        paging:
          type: object
          properties:
            next:
              type: string
            offSet:
              type: integer
            take:
              type: integer
            version:
              type: string
        metadata:
          type: object
          properties:
            experimentName:
              type: string
            streamType:
              type: string
            designatedStreamType:
              type: string
    Discussion:
      type: object
      properties:
        id:
          type: string
        post:
          $ref: '#/components/schemas/DiscussionsPost'
        commentsData:
          type: object
          properties:
            reactionPaging:
              type: object
              properties:
                totalCount:
                  type: integer
            comments:
              type: array
              items:
                $ref: '#/components/schemas/Comment'
        emotionsData:
          type: object
          properties:
            like:
              type: object
              properties:
                paging:
                  type: object
                  properties:
                    totalCount:
                      type: integer
                emotions:
                  type: array
                  items:
                    $ref: '#/components/schemas/Emotion'
        requesterContext:
          type: object
          properties:
            isFlaggingAsSpam:
              type: boolean
            isSubscribed:
              type: boolean
            isLiking:
              type: boolean
            isSaved:
              type: boolean
            isPinned:
              type: boolean
            isRequesterBlocking:
              type: boolean
            isInteractionRestricted:
              type: boolean
            isFollowing:
              type: boolean
        summary:
          type: object
          properties:
            totalCommentsAndReplies:
              type: integer
            sharedCount:
              type: integer
        reason:
          nullable: true
          description: >-
            Reason for the discussion being shown - can be a string or an object
            with sourceId, owner, and type fields
    DiscussionsPost:
      type: object
      properties:
        id:
          type: string
        owner:
          $ref: '#/components/schemas/User'
        obsoleteId:
          type: string
          description: Legacy identifier for backward compatibility
        message:
          type: object
          properties:
            text:
              type: string
            languageCode:
              type: string
        created:
          type: string
          format: date-time
        updated:
          type: string
          format: date-time
        type:
          type: string
        isDeleted:
          type: boolean
          description: Whether the post has been deleted
        isSpam:
          type: boolean
          description: Whether the post is flagged as spam
        editStatus:
          type: string
          description: Edit status of the post
          enum:
            - None
            - Edited
            - Moderated
        attachments:
          type: array
          items:
            $ref: '#/components/schemas/Attachment'
        tags:
          type: array
          items:
            type: object
            properties:
              market:
                type: object
                properties:
                  id:
                    type: string
                  symbolName:
                    type: string
                  displayName:
                    type: string
                  updated:
                    type: string
                  assetType:
                    type: string
                  internalId:
                    type: integer
                  avatar:
                    type: object
                    properties:
                      small:
                        type: string
                      medium:
                        type: string
                      large:
                        type: string
                      svg:
                        type: object
                        nullable: true
                        properties:
                          url:
                            type: string
                          backgroundColor:
                            type: string
                          textColor:
                            type: string
                  application:
                    type: string
                  metadata:
                    type: string
                  assetTypeId:
                    type: integer
                  assetTypeSubCategoryId:
                    type: integer
        mentions:
          type: array
          items:
            type: object
            properties:
              user:
                $ref: '#/components/schemas/User'
              isDirect:
                type: boolean
        metadata:
          type: object
          description: Additional metadata about the post
          properties:
            share:
              type: object
              properties:
                sharedPost:
                  type: string
                sharedOriginPost:
                  type: string
            poll:
              type: object
              properties:
                id:
                  type: integer
                title:
                  type: string
                gcid:
                  type: integer
                options:
                  type: array
                  items:
                    type: object
                    properties:
                      id:
                        type: integer
                      index:
                        type: integer
                      text:
                        type: string
                      isUserVoted:
                        type: boolean
                      votesCount:
                        type: integer
    Comment:
      type: object
      properties:
        entity:
          type: object
          properties:
            message:
              type: object
              properties:
                text:
                  type: string
                languageCode:
                  type: string
            id:
              type: string
            owner:
              $ref: '#/components/schemas/User'
            created:
              type: string
              format: date-time
            obsoleteId:
              type: string
            attachments:
              type: array
              items:
                $ref: '#/components/schemas/Attachment'
            isSpam:
              type: boolean
            editStatus:
              type: string
              enum:
                - None
                - Edited
                - Moderated
            parent:
              type: object
              description: Parent post reference
              properties:
                id:
                  type: string
                obsoleteId:
                  type: string
                type:
                  type: string
        repliesCount:
          type: integer
        replies:
          type: array
          items:
            type: object
        emotionsData:
          type: object
          properties:
            like:
              type: object
              properties:
                paging:
                  type: object
                  properties:
                    totalCount:
                      type: integer
                emotions:
                  type: array
                  items:
                    $ref: '#/components/schemas/Emotion'
        requesterContext:
          type: object
          properties:
            isFlaggingAsSpam:
              type: boolean
            isSubscribed:
              type: boolean
            isLiking:
              type: boolean
            isSaved:
              type: boolean
            isPinned:
              type: boolean
            isRequesterBlocking:
              type: boolean
            isInteractionRestricted:
              type: boolean
            isFollowing:
              type: boolean
    Emotion:
      type: object
      properties:
        type:
          type: string
        id:
          type: string
        owner:
          $ref: '#/components/schemas/User'
        obsoleteId:
          type: string
        parent:
          type: object
          properties:
            id:
              type: string
            obsoleteId:
              type: string
            type:
              type: string
        created:
          type: string
          format: date-time
    User:
      type: object
      properties:
        id:
          type: string
        username:
          type: string
        firstName:
          type: string
        lastName:
          type: string
        avatar:
          type: object
          properties:
            small:
              type: string
              format: uri
            medium:
              type: string
              format: uri
            large:
              type: string
              format: uri
        roles:
          type: array
          items:
            type: string
        isBlocked:
          type: boolean
        isPrivate:
          type: boolean
        countryCode:
          type: integer
        piLevel:
          type: integer
    Attachment:
      type: object
      description: Represents a media attachment in a post
      properties:
        type:
          type: string
          enum:
            - image
            - video
            - link
          description: Type of the attachment
        url:
          type: string
          description: URL where the attachment content can be accessed
        thumbnailUrl:
          type: string
          description: URL of a thumbnail image for video attachments
        host:
          type: string
          description: Host domain of the attachment URL
        mediaType:
          type: string
          description: Type of media
          enum:
            - None
            - Image
            - Video
        media:
          type: object
          description: Media content details
        metadata:
          type: object
          description: Additional metadata about the attachment
          properties:
            width:
              type: integer
              description: Width of the media in pixels
            height:
              type: integer
              description: Height of the media in pixels
            duration:
              type: integer
              description: Duration in seconds for video attachments

````

Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Get authenticated user identity

> Returns the identity of the currently authenticated user including their Global Customer ID (GCID), Real account Customer ID, and Demo account Customer ID.



## OpenAPI

````yaml api-reference/openapi.json get /api/v1/me
openapi: 3.0.1
info:
  title: eToro Api
  version: v1.156.0
  description: >-
    eToro’s public API provides access to real-time financial data, trading
    insights, and account management features, allowing developers to integrate
    eToro’s services into their applications. With access to market prices,
    historical data, and social trading information, the API empowers users to
    enhance their trading strategies. Designed for security and scalability, the
    eToro API ensures smooth and reliable integration for a variety of financial
    applications.


    For more details on integrating with eToro's public WebSocket service,
    please refer to the dedicated [WebSocket
    documentation](./websocket/websocket-doc.html).
servers:
  - url: https://public-api.etoro.com
    description: eToro Public API
security: []
paths:
  /api/v1/me:
    get:
      tags:
        - Identity
      summary: Get authenticated user identity
      description: >-
        Returns the identity of the currently authenticated user including their
        Global Customer ID (GCID), Real account Customer ID, and Demo account
        Customer ID.
      parameters:
        - name: x-request-id
          in: header
          required: true
          schema:
            type: string
            format: uuid
            example: 06e63d90-11e2-4c24-8d95-46e53eb70a81
          description: A unique request identifier.
        - name: x-api-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: lhgfaslk21490FAScVPkdsb53F9dNkfHG4faZSG5vfjndfcfgdssdgsdHF4663
          description: API key for authentication.
        - name: x-user-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: >-
              eyJlYW4iOiJVbnJlZ2lzdGVyZWRBcHBsaWNhdGlvbiIsImVrIjoiOE5sZ2cwcW5EUVdROUFNWGpXT2lmOWktZnpidG5KcUlqWGJ3WHJZZkpZcldrbG90ZEhvLVBjSWhQaU8xU1ZtMW84aU1WZGZqN2xWNzFjLXFxLmcybXE1dnh4Q1hUT25xaWRUaTFlcEhmVk1fIn0_
          description: User-specific authentication key.
      responses:
        '200':
          description: OK
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/meResponse'
              examples:
                example:
                  summary: User Identity Response
                  value:
                    gcid: 123456
                    realCid: 789012
                    demoCid: 345678
        '401':
          description: Unauthorized - Missing or invalid authentication credentials
        '403':
          description: Forbidden - Insufficient permissions
components:
  schemas:
    meResponse:
      type: object
      properties:
        gcid:
          type: integer
          description: >-
            Global Customer ID - the unique identifier for the user across all
            eToro systems.
        realCid:
          type: integer
          description: >-
            Real account Customer ID - the identifier for the user's real
            trading account.
        demoCid:
          type: integer
          description: >-
            Demo account Customer ID - the identifier for the user's
            virtual/demo trading account.

````

Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Get copiers public info



## OpenAPI

````yaml api-reference/openapi.json get /api/v1/pi-data/copiers
openapi: 3.0.1
info:
  title: eToro Api
  version: v1.156.0
  description: >-
    eToro’s public API provides access to real-time financial data, trading
    insights, and account management features, allowing developers to integrate
    eToro’s services into their applications. With access to market prices,
    historical data, and social trading information, the API empowers users to
    enhance their trading strategies. Designed for security and scalability, the
    eToro API ensures smooth and reliable integration for a variety of financial
    applications.


    For more details on integrating with eToro's public WebSocket service,
    please refer to the dedicated [WebSocket
    documentation](./websocket/websocket-doc.html).
servers:
  - url: https://public-api.etoro.com
    description: eToro Public API
security: []
paths:
  /api/v1/pi-data/copiers:
    get:
      tags:
        - PI Data
      summary: Get copiers public info
      parameters:
        - name: x-request-id
          in: header
          required: true
          schema:
            type: string
            format: uuid
            example: 5e46e1fe-9026-43d9-aad2-80b3097310d7
          description: A unique request identifier.
        - name: x-api-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: lhgfaslk21490FAScVPkdsb53F9dNkfHG4faZSG5vfjndfcfgdssdgsdHF4663
          description: API key for authentication.
        - name: x-user-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: >-
              eyJlYW4iOiJVbnJlZ2lzdGVyZWRBcHBsaWNhdGlvbiIsImVrIjoiOE5sZ2cwcW5EUVdROUFNWGpXT2lmOWktZnpidG5KcUlqWGJ3WHJZZkpZcldrbG90ZEhvLVBjSWhQaU8xU1ZtMW84aU1WZGZqN2xWNzFjLXFxLmcybXE1dnh4Q1hUT25xaWRUaTFlcEhmVk1fIn0_
          description: User-specific authentication key.
      responses:
        '200':
          description: A list of copiers
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/GeCopiersResponse'
components:
  schemas:
    GeCopiersResponse:
      type: object
      description: Response object containing a list of portfolio copiers.
      properties:
        copiers:
          type: array
          nullable: true
          description: >-
            List of users copying your portfolio, with demographic and financial
            info.
          items:
            type: object
            properties:
              Gender:
                type: string
                example: M
                description: Gender of the copier
              Club:
                type: string
                example: Gold
                description: Membership club level
              Country:
                type: string
                example: Germany
                description: Country of residence
              CopyStartedAtCategory:
                type: string
                enum:
                  - less than 1 day
                  - less than 1 week
                  - less than 1 month
                  - less than 1 year
                  - more than 1 year
                description: How long ago the copy relationship started
                example: more than 1 year
              AmountCategory:
                type: string
                enum:
                  - <100
                  - 100-500
                  - 500-1000
                  - 1000-5000
                  - '>5000'
                description: Amount being copied
                example: 100-500
              AgeCategory:
                type: string
                enum:
                  - Under 18
                  - 18-29
                  - 30-44
                  - 45-59
                  - 60+
                description: Age range of the copier
                example: 30-44
              CopyRealizedEquity_pnl:
                type: string
                description: Total realized equity PnL of the copier
                example: '1589.2'
              AvailableCopyBalance:
                type: string
                description: Available copy balance of the copier
                example: '55.2'
            additionalProperties: false
      additionalProperties: false

````

Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Retrieve comprehensive user profile data and aggregated account information

> Returns detailed user profile information including account status, verification levels, biographical data, and associated metadata. This endpoint aggregates essential user information from multiple sources to provide a complete user profile overview.



## OpenAPI

````yaml api-reference/openapi.json get /api/v1/user-info/people
openapi: 3.0.1
info:
  title: eToro Api
  version: v1.156.0
  description: >-
    eToro’s public API provides access to real-time financial data, trading
    insights, and account management features, allowing developers to integrate
    eToro’s services into their applications. With access to market prices,
    historical data, and social trading information, the API empowers users to
    enhance their trading strategies. Designed for security and scalability, the
    eToro API ensures smooth and reliable integration for a variety of financial
    applications.


    For more details on integrating with eToro's public WebSocket service,
    please refer to the dedicated [WebSocket
    documentation](./websocket/websocket-doc.html).
servers:
  - url: https://public-api.etoro.com
    description: eToro Public API
security: []
paths:
  /api/v1/user-info/people:
    get:
      tags:
        - Users Info
      summary: >-
        Retrieve comprehensive user profile data and aggregated account
        information
      description: >-
        Returns detailed user profile information including account status,
        verification levels, biographical data, and associated metadata. This
        endpoint aggregates essential user information from multiple sources to
        provide a complete user profile overview.
      parameters:
        - name: x-request-id
          in: header
          required: true
          schema:
            type: string
            format: uuid
            example: c0fa5344-15e4-4a6c-a028-c50ddc047a2a
          description: A unique request identifier.
        - name: x-api-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: lhgfaslk21490FAScVPkdsb53F9dNkfHG4faZSG5vfjndfcfgdssdgsdHF4663
          description: API key for authentication.
        - name: x-user-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: >-
              eyJlYW4iOiJVbnJlZ2lzdGVyZWRBcHBsaWNhdGlvbiIsImVrIjoiOE5sZ2cwcW5EUVdROUFNWGpXT2lmOWktZnpidG5KcUlqWGJ3WHJZZkpZcldrbG90ZEhvLVBjSWhQaU8xU1ZtMW84aU1WZGZqN2xWNzFjLXFxLmcybXE1dnh4Q1hUT25xaWRUaTFlcEhmVk1fIn0_
          description: User-specific authentication key.
        - name: usernames
          in: query
          schema:
            type: array
            items:
              type: string
          explode: false
          required: false
        - name: cidList
          in: query
          schema:
            type: array
            items:
              type: integer
          explode: false
          required: false
      responses:
        '200':
          description: Successfully retrieved user information
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/PublicAggregatedInfoResponse'
              example:
                users:
                  - gcid: 1536861
                    realCID: 1563191
                    demoCID: 1563191
                    username: exampleuser
                    language: 1
                    languageIsoCode: en-GB
                    country: 54
                    allowDisplayFullName: false
                    userBio:
                      gcid: 1536861
                      languageCode: null
                    whiteLabel: 1
                    optOut: true
                    homepage: null
                    playerStatus: null
                    piLevel: 0
                    isPi: false
                    avatars:
                      - url: >-
                          https://openbook-static-files-test.s3.amazonaws.com/images/avatoros/35x35/cy.png
                        width: 35
                        height: 35
                        type: Resized
                      - url: >-
                          https://openbook-static-files-test.s3.amazonaws.com/images/avatoros/50x50/cy.png
                        width: 50
                        height: 50
                        type: Resized
                      - url: >-
                          https://openbook-static-files-test.s3.amazonaws.com/images/avatoros/150x150/cy.png
                        width: 150
                        height: 150
                        type: Resized
                    masterAccountCid: null
                    accountType: 1
                    fundType: null
                    isVerified: false
                    verificationLevel: 1
                    accountStatus: 1
                    gdprInfo: null
                    userFlowSignature: >-
                      233a065f3f8d7e344516fc75f7e6c4646a0c0d38798c00e4655fa0a9447ea223
        '400':
          description: >-
            Invalid request - Typically due to exceeding maximum usernames limit
            or invalid username format
        '404':
          description: One or more requested usernames not found
components:
  schemas:
    PublicAggregatedInfoResponse:
      type: object
      description: Container for the aggregated user information response
      properties:
        users:
          type: array
          description: Array of user profiles with their associated information
          items:
            $ref: '#/components/schemas/PublicAggregatedInfoUser'
    PublicAggregatedInfoUser:
      type: object
      description: >-
        Comprehensive user profile information including account details,
        verification status, and preferences
      properties:
        gcid:
          type: integer
          description: Global Customer ID - Unique identifier across all systems
        realCID:
          type: integer
          description: Customer ID for real trading account
        demoCID:
          type: integer
          nullable: true
          description: Customer ID for demo/practice account if available
        username:
          type: string
          description: Unique username identifier for the user
        language:
          type: integer
          nullable: true
          description: User's preferred language ID based on system language codes
        languageIsoCode:
          type: string
          description: ISO 639-1 language code for user's preferred language
        country:
          type: integer
          nullable: true
          description: User's registered country ID based on system country codes
        allowDisplayFullName:
          type: boolean
          description: >-
            Indicates whether the user has consented to displaying their full
            name publicly
        userBio:
          $ref: '#/components/schemas/PublicAggregatedInfoUiUserBio'
          description: Structured biographical information including trading strategy
        whiteLabel:
          type: integer
          nullable: true
          description: White label partner identifier if user belongs to a partner program
        optOut:
          type: boolean
          description: Indicates if user has opted out of public profile features
        homepage:
          type: integer
          nullable: true
        playerStatus:
          type: integer
          nullable: true
        piLevel:
          type: integer
          nullable: true
        isPi:
          type: boolean
          description: Indicates if user is a Professional Investor with special privileges
        avatars:
          type: array
          items:
            $ref: '#/components/schemas/PublicAggregatedInfoUiUserAvatar'
        masterAccountCid:
          type: integer
          nullable: true
        accountType:
          type: integer
          nullable: true
        fundType:
          type: string
          nullable: true
        isVerified:
          type: boolean
        verificationLevel:
          type: integer
          description: User's current verification level (0-3, where 3 is fully verified)
        accountStatus:
          type: integer
          nullable: true
          description: >-
            Current account status code indicating active, suspended, or other
            states
        gdprInfo:
          type: object
          nullable: true
          properties:
            accountStatus:
              $ref: '#/components/schemas/PublicAggregatedInfoAccountStatus'
            playerStatus:
              $ref: '#/components/schemas/PublicAggregatedInfoPlayerStatus'
            playerStatusReason:
              $ref: '#/components/schemas/PublicAggregatedInfoPlayerStatusReason'
        firstName:
          type: string
          nullable: true
          description: User's first name (visible if allowDisplayFullName is true)
        middleName:
          type: string
          nullable: true
          description: User's middle name
        lastName:
          type: string
          nullable: true
          description: User's last name (visible if allowDisplayFullName is true)
        aboutMe:
          type: string
          nullable: true
          description: User's full about me text
        aboutMeShort:
          type: string
          nullable: true
          description: Short summary of user's about me text
        customerRestrictions:
          type: array
          nullable: true
          items:
            type: object
            properties:
              CID:
                type: integer
                description: Customer ID
              restrictionTypeID:
                type: integer
                description: Type of restriction
              reasonID:
                type: integer
                description: Reason for restriction
              occured:
                type: string
                format: date-time
                description: When the restriction occurred
          description: List of customer restrictions applied to the account
        userFlowSignature:
          type: string
    PublicAggregatedInfoUiUserBio:
      type: object
      properties:
        gcid:
          type: integer
        languageCode:
          type: string
          nullable: true
        aboutMe:
          type: string
          nullable: true
          description: User's full about me text
        aboutMeShort:
          type: string
          nullable: true
          description: Short summary of user's about me text
        strategyID:
          type: integer
          nullable: true
          description: ID of the user's trading strategy
    PublicAggregatedInfoUiUserAvatar:
      type: object
      properties:
        url:
          type: string
        width:
          type: integer
        height:
          type: integer
        type:
          type: string
          enum:
            - Original
            - OriginalCropped
            - Resized
            - Retouched
          description: Type of avatar image
    PublicAggregatedInfoAccountStatus:
      type: integer
      enum:
        - 1
        - 2
      x-enumNames:
        - Open
        - Closed
      nullable: true
    PublicAggregatedInfoPlayerStatus:
      type: integer
      enum:
        - 1
        - 2
        - 3
        - 4
        - 5
        - 6
        - 7
        - 8
        - 9
        - 10
        - 11
        - 12
        - 13
        - 14
        - 15
      x-enumNames:
        - Normal
        - Blocked
        - ChatBlocked
        - BlockedUponRequest
        - Warning
        - BlockedUnderInvestigation
        - ScalpersBlock
        - BlockedPayPalInvestigation
        - TradeBlock
        - DepositBlocked
        - SocialIndex
        - CopyBlock
        - PendingVerification
        - BlockedFailedVerification
        - BlockTrading
      nullable: true
    PublicAggregatedInfoPlayerStatusReason:
      type: integer
      enum:
        - 0
        - 1
        - 2
        - 3
        - 4
        - 5
        - 6
        - 7
        - 8
        - 9
        - 10
        - 11
        - 12
        - 13
        - 14
        - 15
        - 16
        - 17
        - 18
        - 19
        - 20
        - 21
        - 22
        - 23
        - 24
        - 25
        - 26
        - 27
        - 28
        - 29
        - 30
        - 31
        - 32
        - 33
        - 34
        - 35
        - 36
        - 37
        - 38
        - 39
        - 40
        - 41
        - 42
      x-enumNames:
        - None
        - FailedVerification
        - ExpiredDocument
        - CloseAccountByUser
        - Risk
        - Chargeback
        - AMLAccountClosed
        - HRC
        - Underage
        - Deceased
        - AML
        - AMLreview
        - OffMarketAbuse
        - Overpayment
        - RiskCheck
        - ThirdParty
        - PayPalInvestigation
        - NOC_NOF_RFI
        - WCHMatch
        - Other
        - RightToBeForgotten
        - SelfService
        - ByRequest
        - ACHChargeback
        - PWMBChargeback
        - Abuse
        - AffiliateAccount
        - PendingDocs
        - EmployeeAccount
        - PIAccount
        - CheckoutChargeback
        - CheckoutRetrievel
        - CheckoutCaptureDecline
        - EToroMoneyRestriction
        - AbusiveTrading
        - HackedAccount
        - PartnersAndPIs
        - CS_ManagementDecision
        - Deposits
        - KYC
        - AccountClosed
        - Tax
        - Corporate
      nullable: true

````

Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Retrieve comprehensive user profile data and aggregated account information

> Returns detailed user profile information including account status, verification levels, biographical data, and associated metadata. This endpoint aggregates essential user information from multiple sources to provide a complete user profile overview.



## OpenAPI

````yaml api-reference/openapi.json get /api/v1/user-info/people
openapi: 3.0.1
info:
  title: eToro Api
  version: v1.156.0
  description: >-
    eToro’s public API provides access to real-time financial data, trading
    insights, and account management features, allowing developers to integrate
    eToro’s services into their applications. With access to market prices,
    historical data, and social trading information, the API empowers users to
    enhance their trading strategies. Designed for security and scalability, the
    eToro API ensures smooth and reliable integration for a variety of financial
    applications.


    For more details on integrating with eToro's public WebSocket service,
    please refer to the dedicated [WebSocket
    documentation](./websocket/websocket-doc.html).
servers:
  - url: https://public-api.etoro.com
    description: eToro Public API
security: []
paths:
  /api/v1/user-info/people:
    get:
      tags:
        - Users Info
      summary: >-
        Retrieve comprehensive user profile data and aggregated account
        information
      description: >-
        Returns detailed user profile information including account status,
        verification levels, biographical data, and associated metadata. This
        endpoint aggregates essential user information from multiple sources to
        provide a complete user profile overview.
      parameters:
        - name: x-request-id
          in: header
          required: true
          schema:
            type: string
            format: uuid
            example: c0fa5344-15e4-4a6c-a028-c50ddc047a2a
          description: A unique request identifier.
        - name: x-api-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: lhgfaslk21490FAScVPkdsb53F9dNkfHG4faZSG5vfjndfcfgdssdgsdHF4663
          description: API key for authentication.
        - name: x-user-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: >-
              eyJlYW4iOiJVbnJlZ2lzdGVyZWRBcHBsaWNhdGlvbiIsImVrIjoiOE5sZ2cwcW5EUVdROUFNWGpXT2lmOWktZnpidG5KcUlqWGJ3WHJZZkpZcldrbG90ZEhvLVBjSWhQaU8xU1ZtMW84aU1WZGZqN2xWNzFjLXFxLmcybXE1dnh4Q1hUT25xaWRUaTFlcEhmVk1fIn0_
          description: User-specific authentication key.
        - name: usernames
          in: query
          schema:
            type: array
            items:
              type: string
          explode: false
          required: false
        - name: cidList
          in: query
          schema:
            type: array
            items:
              type: integer
          explode: false
          required: false
      responses:
        '200':
          description: Successfully retrieved user information
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/PublicAggregatedInfoResponse'
              example:
                users:
                  - gcid: 1536861
                    realCID: 1563191
                    demoCID: 1563191
                    username: exampleuser
                    language: 1
                    languageIsoCode: en-GB
                    country: 54
                    allowDisplayFullName: false
                    userBio:
                      gcid: 1536861
                      languageCode: null
                    whiteLabel: 1
                    optOut: true
                    homepage: null
                    playerStatus: null
                    piLevel: 0
                    isPi: false
                    avatars:
                      - url: >-
                          https://openbook-static-files-test.s3.amazonaws.com/images/avatoros/35x35/cy.png
                        width: 35
                        height: 35
                        type: Resized
                      - url: >-
                          https://openbook-static-files-test.s3.amazonaws.com/images/avatoros/50x50/cy.png
                        width: 50
                        height: 50
                        type: Resized
                      - url: >-
                          https://openbook-static-files-test.s3.amazonaws.com/images/avatoros/150x150/cy.png
                        width: 150
                        height: 150
                        type: Resized
                    masterAccountCid: null
                    accountType: 1
                    fundType: null
                    isVerified: false
                    verificationLevel: 1
                    accountStatus: 1
                    gdprInfo: null
                    userFlowSignature: >-
                      233a065f3f8d7e344516fc75f7e6c4646a0c0d38798c00e4655fa0a9447ea223
        '400':
          description: >-
            Invalid request - Typically due to exceeding maximum usernames limit
            or invalid username format
        '404':
          description: One or more requested usernames not found
components:
  schemas:
    PublicAggregatedInfoResponse:
      type: object
      description: Container for the aggregated user information response
      properties:
        users:
          type: array
          description: Array of user profiles with their associated information
          items:
            $ref: '#/components/schemas/PublicAggregatedInfoUser'
    PublicAggregatedInfoUser:
      type: object
      description: >-
        Comprehensive user profile information including account details,
        verification status, and preferences
      properties:
        gcid:
          type: integer
          description: Global Customer ID - Unique identifier across all systems
        realCID:
          type: integer
          description: Customer ID for real trading account
        demoCID:
          type: integer
          nullable: true
          description: Customer ID for demo/practice account if available
        username:
          type: string
          description: Unique username identifier for the user
        language:
          type: integer
          nullable: true
          description: User's preferred language ID based on system language codes
        languageIsoCode:
          type: string
          description: ISO 639-1 language code for user's preferred language
        country:
          type: integer
          nullable: true
          description: User's registered country ID based on system country codes
        allowDisplayFullName:
          type: boolean
          description: >-
            Indicates whether the user has consented to displaying their full
            name publicly
        userBio:
          $ref: '#/components/schemas/PublicAggregatedInfoUiUserBio'
          description: Structured biographical information including trading strategy
        whiteLabel:
          type: integer
          nullable: true
          description: White label partner identifier if user belongs to a partner program
        optOut:
          type: boolean
          description: Indicates if user has opted out of public profile features
        homepage:
          type: integer
          nullable: true
        playerStatus:
          type: integer
          nullable: true
        piLevel:
          type: integer
          nullable: true
        isPi:
          type: boolean
          description: Indicates if user is a Professional Investor with special privileges
        avatars:
          type: array
          items:
            $ref: '#/components/schemas/PublicAggregatedInfoUiUserAvatar'
        masterAccountCid:
          type: integer
          nullable: true
        accountType:
          type: integer
          nullable: true
        fundType:
          type: string
          nullable: true
        isVerified:
          type: boolean
        verificationLevel:
          type: integer
          description: User's current verification level (0-3, where 3 is fully verified)
        accountStatus:
          type: integer
          nullable: true
          description: >-
            Current account status code indicating active, suspended, or other
            states
        gdprInfo:
          type: object
          nullable: true
          properties:
            accountStatus:
              $ref: '#/components/schemas/PublicAggregatedInfoAccountStatus'
            playerStatus:
              $ref: '#/components/schemas/PublicAggregatedInfoPlayerStatus'
            playerStatusReason:
              $ref: '#/components/schemas/PublicAggregatedInfoPlayerStatusReason'
        firstName:
          type: string
          nullable: true
          description: User's first name (visible if allowDisplayFullName is true)
        middleName:
          type: string
          nullable: true
          description: User's middle name
        lastName:
          type: string
          nullable: true
          description: User's last name (visible if allowDisplayFullName is true)
        aboutMe:
          type: string
          nullable: true
          description: User's full about me text
        aboutMeShort:
          type: string
          nullable: true
          description: Short summary of user's about me text
        customerRestrictions:
          type: array
          nullable: true
          items:
            type: object
            properties:
              CID:
                type: integer
                description: Customer ID
              restrictionTypeID:
                type: integer
                description: Type of restriction
              reasonID:
                type: integer
                description: Reason for restriction
              occured:
                type: string
                format: date-time
                description: When the restriction occurred
          description: List of customer restrictions applied to the account
        userFlowSignature:
          type: string
    PublicAggregatedInfoUiUserBio:
      type: object
      properties:
        gcid:
          type: integer
        languageCode:
          type: string
          nullable: true
        aboutMe:
          type: string
          nullable: true
          description: User's full about me text
        aboutMeShort:
          type: string
          nullable: true
          description: Short summary of user's about me text
        strategyID:
          type: integer
          nullable: true
          description: ID of the user's trading strategy
    PublicAggregatedInfoUiUserAvatar:
      type: object
      properties:
        url:
          type: string
        width:
          type: integer
        height:
          type: integer
        type:
          type: string
          enum:
            - Original
            - OriginalCropped
            - Resized
            - Retouched
          description: Type of avatar image
    PublicAggregatedInfoAccountStatus:
      type: integer
      enum:
        - 1
        - 2
      x-enumNames:
        - Open
        - Closed
      nullable: true
    PublicAggregatedInfoPlayerStatus:
      type: integer
      enum:
        - 1
        - 2
        - 3
        - 4
        - 5
        - 6
        - 7
        - 8
        - 9
        - 10
        - 11
        - 12
        - 13
        - 14
        - 15
      x-enumNames:
        - Normal
        - Blocked
        - ChatBlocked
        - BlockedUponRequest
        - Warning
        - BlockedUnderInvestigation
        - ScalpersBlock
        - BlockedPayPalInvestigation
        - TradeBlock
        - DepositBlocked
        - SocialIndex
        - CopyBlock
        - PendingVerification
        - BlockedFailedVerification
        - BlockTrading
      nullable: true
    PublicAggregatedInfoPlayerStatusReason:
      type: integer
      enum:
        - 0
        - 1
        - 2
        - 3
        - 4
        - 5
        - 6
        - 7
        - 8
        - 9
        - 10
        - 11
        - 12
        - 13
        - 14
        - 15
        - 16
        - 17
        - 18
        - 19
        - 20
        - 21
        - 22
        - 23
        - 24
        - 25
        - 26
        - 27
        - 28
        - 29
        - 30
        - 31
        - 32
        - 33
        - 34
        - 35
        - 36
        - 37
        - 38
        - 39
        - 40
        - 41
        - 42
      x-enumNames:
        - None
        - FailedVerification
        - ExpiredDocument
        - CloseAccountByUser
        - Risk
        - Chargeback
        - AMLAccountClosed
        - HRC
        - Underage
        - Deceased
        - AML
        - AMLreview
        - OffMarketAbuse
        - Overpayment
        - RiskCheck
        - ThirdParty
        - PayPalInvestigation
        - NOC_NOF_RFI
        - WCHMatch
        - Other
        - RightToBeForgotten
        - SelfService
        - ByRequest
        - ACHChargeback
        - PWMBChargeback
        - Abuse
        - AffiliateAccount
        - PendingDocs
        - EmployeeAccount
        - PIAccount
        - CheckoutChargeback
        - CheckoutRetrievel
        - CheckoutCaptureDecline
        - EToroMoneyRestriction
        - AbusiveTrading
        - HackedAccount
        - PartnersAndPIs
        - CS_ManagementDecision
        - Deposits
        - KYC
        - AccountClosed
        - Tax
        - Corporate
      nullable: true

````

Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Retrieve comprehensive user profile data and aggregated account information

> Returns detailed user profile information including account status, verification levels, biographical data, and associated metadata. This endpoint aggregates essential user information from multiple sources to provide a complete user profile overview.



## OpenAPI

````yaml api-reference/openapi.json get /api/v1/user-info/people
openapi: 3.0.1
info:
  title: eToro Api
  version: v1.156.0
  description: >-
    eToro’s public API provides access to real-time financial data, trading
    insights, and account management features, allowing developers to integrate
    eToro’s services into their applications. With access to market prices,
    historical data, and social trading information, the API empowers users to
    enhance their trading strategies. Designed for security and scalability, the
    eToro API ensures smooth and reliable integration for a variety of financial
    applications.


    For more details on integrating with eToro's public WebSocket service,
    please refer to the dedicated [WebSocket
    documentation](./websocket/websocket-doc.html).
servers:
  - url: https://public-api.etoro.com
    description: eToro Public API
security: []
paths:
  /api/v1/user-info/people:
    get:
      tags:
        - Users Info
      summary: >-
        Retrieve comprehensive user profile data and aggregated account
        information
      description: >-
        Returns detailed user profile information including account status,
        verification levels, biographical data, and associated metadata. This
        endpoint aggregates essential user information from multiple sources to
        provide a complete user profile overview.
      parameters:
        - name: x-request-id
          in: header
          required: true
          schema:
            type: string
            format: uuid
            example: c0fa5344-15e4-4a6c-a028-c50ddc047a2a
          description: A unique request identifier.
        - name: x-api-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: lhgfaslk21490FAScVPkdsb53F9dNkfHG4faZSG5vfjndfcfgdssdgsdHF4663
          description: API key for authentication.
        - name: x-user-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: >-
              eyJlYW4iOiJVbnJlZ2lzdGVyZWRBcHBsaWNhdGlvbiIsImVrIjoiOE5sZ2cwcW5EUVdROUFNWGpXT2lmOWktZnpidG5KcUlqWGJ3WHJZZkpZcldrbG90ZEhvLVBjSWhQaU8xU1ZtMW84aU1WZGZqN2xWNzFjLXFxLmcybXE1dnh4Q1hUT25xaWRUaTFlcEhmVk1fIn0_
          description: User-specific authentication key.
        - name: usernames
          in: query
          schema:
            type: array
            items:
              type: string
          explode: false
          required: false
        - name: cidList
          in: query
          schema:
            type: array
            items:
              type: integer
          explode: false
          required: false
      responses:
        '200':
          description: Successfully retrieved user information
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/PublicAggregatedInfoResponse'
              example:
                users:
                  - gcid: 1536861
                    realCID: 1563191
                    demoCID: 1563191
                    username: exampleuser
                    language: 1
                    languageIsoCode: en-GB
                    country: 54
                    allowDisplayFullName: false
                    userBio:
                      gcid: 1536861
                      languageCode: null
                    whiteLabel: 1
                    optOut: true
                    homepage: null
                    playerStatus: null
                    piLevel: 0
                    isPi: false
                    avatars:
                      - url: >-
                          https://openbook-static-files-test.s3.amazonaws.com/images/avatoros/35x35/cy.png
                        width: 35
                        height: 35
                        type: Resized
                      - url: >-
                          https://openbook-static-files-test.s3.amazonaws.com/images/avatoros/50x50/cy.png
                        width: 50
                        height: 50
                        type: Resized
                      - url: >-
                          https://openbook-static-files-test.s3.amazonaws.com/images/avatoros/150x150/cy.png
                        width: 150
                        height: 150
                        type: Resized
                    masterAccountCid: null
                    accountType: 1
                    fundType: null
                    isVerified: false
                    verificationLevel: 1
                    accountStatus: 1
                    gdprInfo: null
                    userFlowSignature: >-
                      233a065f3f8d7e344516fc75f7e6c4646a0c0d38798c00e4655fa0a9447ea223
        '400':
          description: >-
            Invalid request - Typically due to exceeding maximum usernames limit
            or invalid username format
        '404':
          description: One or more requested usernames not found
components:
  schemas:
    PublicAggregatedInfoResponse:
      type: object
      description: Container for the aggregated user information response
      properties:
        users:
          type: array
          description: Array of user profiles with their associated information
          items:
            $ref: '#/components/schemas/PublicAggregatedInfoUser'
    PublicAggregatedInfoUser:
      type: object
      description: >-
        Comprehensive user profile information including account details,
        verification status, and preferences
      properties:
        gcid:
          type: integer
          description: Global Customer ID - Unique identifier across all systems
        realCID:
          type: integer
          description: Customer ID for real trading account
        demoCID:
          type: integer
          nullable: true
          description: Customer ID for demo/practice account if available
        username:
          type: string
          description: Unique username identifier for the user
        language:
          type: integer
          nullable: true
          description: User's preferred language ID based on system language codes
        languageIsoCode:
          type: string
          description: ISO 639-1 language code for user's preferred language
        country:
          type: integer
          nullable: true
          description: User's registered country ID based on system country codes
        allowDisplayFullName:
          type: boolean
          description: >-
            Indicates whether the user has consented to displaying their full
            name publicly
        userBio:
          $ref: '#/components/schemas/PublicAggregatedInfoUiUserBio'
          description: Structured biographical information including trading strategy
        whiteLabel:
          type: integer
          nullable: true
          description: White label partner identifier if user belongs to a partner program
        optOut:
          type: boolean
          description: Indicates if user has opted out of public profile features
        homepage:
          type: integer
          nullable: true
        playerStatus:
          type: integer
          nullable: true
        piLevel:
          type: integer
          nullable: true
        isPi:
          type: boolean
          description: Indicates if user is a Professional Investor with special privileges
        avatars:
          type: array
          items:
            $ref: '#/components/schemas/PublicAggregatedInfoUiUserAvatar'
        masterAccountCid:
          type: integer
          nullable: true
        accountType:
          type: integer
          nullable: true
        fundType:
          type: string
          nullable: true
        isVerified:
          type: boolean
        verificationLevel:
          type: integer
          description: User's current verification level (0-3, where 3 is fully verified)
        accountStatus:
          type: integer
          nullable: true
          description: >-
            Current account status code indicating active, suspended, or other
            states
        gdprInfo:
          type: object
          nullable: true
          properties:
            accountStatus:
              $ref: '#/components/schemas/PublicAggregatedInfoAccountStatus'
            playerStatus:
              $ref: '#/components/schemas/PublicAggregatedInfoPlayerStatus'
            playerStatusReason:
              $ref: '#/components/schemas/PublicAggregatedInfoPlayerStatusReason'
        firstName:
          type: string
          nullable: true
          description: User's first name (visible if allowDisplayFullName is true)
        middleName:
          type: string
          nullable: true
          description: User's middle name
        lastName:
          type: string
          nullable: true
          description: User's last name (visible if allowDisplayFullName is true)
        aboutMe:
          type: string
          nullable: true
          description: User's full about me text
        aboutMeShort:
          type: string
          nullable: true
          description: Short summary of user's about me text
        customerRestrictions:
          type: array
          nullable: true
          items:
            type: object
            properties:
              CID:
                type: integer
                description: Customer ID
              restrictionTypeID:
                type: integer
                description: Type of restriction
              reasonID:
                type: integer
                description: Reason for restriction
              occured:
                type: string
                format: date-time
                description: When the restriction occurred
          description: List of customer restrictions applied to the account
        userFlowSignature:
          type: string
    PublicAggregatedInfoUiUserBio:
      type: object
      properties:
        gcid:
          type: integer
        languageCode:
          type: string
          nullable: true
        aboutMe:
          type: string
          nullable: true
          description: User's full about me text
        aboutMeShort:
          type: string
          nullable: true
          description: Short summary of user's about me text
        strategyID:
          type: integer
          nullable: true
          description: ID of the user's trading strategy
    PublicAggregatedInfoUiUserAvatar:
      type: object
      properties:
        url:
          type: string
        width:
          type: integer
        height:
          type: integer
        type:
          type: string
          enum:
            - Original
            - OriginalCropped
            - Resized
            - Retouched
          description: Type of avatar image
    PublicAggregatedInfoAccountStatus:
      type: integer
      enum:
        - 1
        - 2
      x-enumNames:
        - Open
        - Closed
      nullable: true
    PublicAggregatedInfoPlayerStatus:
      type: integer
      enum:
        - 1
        - 2
        - 3
        - 4
        - 5
        - 6
        - 7
        - 8
        - 9
        - 10
        - 11
        - 12
        - 13
        - 14
        - 15
      x-enumNames:
        - Normal
        - Blocked
        - ChatBlocked
        - BlockedUponRequest
        - Warning
        - BlockedUnderInvestigation
        - ScalpersBlock
        - BlockedPayPalInvestigation
        - TradeBlock
        - DepositBlocked
        - SocialIndex
        - CopyBlock
        - PendingVerification
        - BlockedFailedVerification
        - BlockTrading
      nullable: true
    PublicAggregatedInfoPlayerStatusReason:
      type: integer
      enum:
        - 0
        - 1
        - 2
        - 3
        - 4
        - 5
        - 6
        - 7
        - 8
        - 9
        - 10
        - 11
        - 12
        - 13
        - 14
        - 15
        - 16
        - 17
        - 18
        - 19
        - 20
        - 21
        - 22
        - 23
        - 24
        - 25
        - 26
        - 27
        - 28
        - 29
        - 30
        - 31
        - 32
        - 33
        - 34
        - 35
        - 36
        - 37
        - 38
        - 39
        - 40
        - 41
        - 42
      x-enumNames:
        - None
        - FailedVerification
        - ExpiredDocument
        - CloseAccountByUser
        - Risk
        - Chargeback
        - AMLAccountClosed
        - HRC
        - Underage
        - Deceased
        - AML
        - AMLreview
        - OffMarketAbuse
        - Overpayment
        - RiskCheck
        - ThirdParty
        - PayPalInvestigation
        - NOC_NOF_RFI
        - WCHMatch
        - Other
        - RightToBeForgotten
        - SelfService
        - ByRequest
        - ACHChargeback
        - PWMBChargeback
        - Abuse
        - AffiliateAccount
        - PendingDocs
        - EmployeeAccount
        - PIAccount
        - CheckoutChargeback
        - CheckoutRetrievel
        - CheckoutCaptureDecline
        - EToroMoneyRestriction
        - AbusiveTrading
        - HackedAccount
        - PartnersAndPIs
        - CS_ManagementDecision
        - Deposits
        - KYC
        - AccountClosed
        - Tax
        - Corporate
      nullable: true

````

Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Retrieve detailed historical performance metrics and analytics for a specified user

> Returns comprehensive historical monthly and yearly performance data including gain percentages, risk-adjusted returns, and detailed trading statistics. This endpoint provides both aggregated and time-series performance metrics.



## OpenAPI

````yaml api-reference/openapi.json get /api/v1/user-info/people/{username}/gain
openapi: 3.0.1
info:
  title: eToro Api
  version: v1.156.0
  description: >-
    eToro’s public API provides access to real-time financial data, trading
    insights, and account management features, allowing developers to integrate
    eToro’s services into their applications. With access to market prices,
    historical data, and social trading information, the API empowers users to
    enhance their trading strategies. Designed for security and scalability, the
    eToro API ensures smooth and reliable integration for a variety of financial
    applications.


    For more details on integrating with eToro's public WebSocket service,
    please refer to the dedicated [WebSocket
    documentation](./websocket/websocket-doc.html).
servers:
  - url: https://public-api.etoro.com
    description: eToro Public API
security: []
paths:
  /api/v1/user-info/people/{username}/gain:
    get:
      tags:
        - Users Info
      summary: >-
        Retrieve detailed historical performance metrics and analytics for a
        specified user
      description: >-
        Returns comprehensive historical monthly and yearly performance data
        including gain percentages, risk-adjusted returns, and detailed trading
        statistics. This endpoint provides both aggregated and time-series
        performance metrics.
      parameters:
        - name: x-request-id
          in: header
          required: true
          schema:
            type: string
            format: uuid
            example: 3f25ecae-702c-4f2c-a410-ef577a48f2bb
          description: A unique request identifier.
        - name: x-api-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: lhgfaslk21490FAScVPkdsb53F9dNkfHG4faZSG5vfjndfcfgdssdgsdHF4663
          description: API key for authentication.
        - name: x-user-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: >-
              eyJlYW4iOiJVbnJlZ2lzdGVyZWRBcHBsaWNhdGlvbiIsImVrIjoiOE5sZ2cwcW5EUVdROUFNWGpXT2lmOWktZnpidG5KcUlqWGJ3WHJZZkpZcldrbG90ZEhvLVBjSWhQaU8xU1ZtMW84aU1WZGZqN2xWNzFjLXFxLmcybXE1dnh4Q1hUT25xaWRUaTFlcEhmVk1fIn0_
          description: User-specific authentication key.
        - name: username
          description: >-
            Unique identifier of the user whose performance metrics are being
            requested
          in: path
          schema:
            type: string
          required: true
          example: trader123
      responses:
        '200':
          description: OK
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/getUserGainResponse'
components:
  schemas:
    getUserGainResponse:
      type: object
      properties:
        monthly:
          type: array
          items:
            $ref: '#/components/schemas/gainEntry'
        yearly:
          type: array
          items:
            $ref: '#/components/schemas/gainEntry'
    gainEntry:
      type: object
      properties:
        timestamp:
          type: string
          format: date-time
        gain:
          type: number

````

Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Get the live portfolio of a user



## OpenAPI

````yaml api-reference/openapi.json get /api/v1/user-info/people/{username}/portfolio/live
openapi: 3.0.1
info:
  title: eToro Api
  version: v1.156.0
  description: >-
    eToro’s public API provides access to real-time financial data, trading
    insights, and account management features, allowing developers to integrate
    eToro’s services into their applications. With access to market prices,
    historical data, and social trading information, the API empowers users to
    enhance their trading strategies. Designed for security and scalability, the
    eToro API ensures smooth and reliable integration for a variety of financial
    applications.


    For more details on integrating with eToro's public WebSocket service,
    please refer to the dedicated [WebSocket
    documentation](./websocket/websocket-doc.html).
servers:
  - url: https://public-api.etoro.com
    description: eToro Public API
security: []
paths:
  /api/v1/user-info/people/{username}/portfolio/live:
    get:
      tags:
        - Users Info
      summary: Get the live portfolio of a user
      parameters:
        - name: x-request-id
          in: header
          required: true
          schema:
            type: string
            format: uuid
            example: 33531c06-e05d-40f9-90fd-11cb127476bc
          description: A unique request identifier.
        - name: x-api-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: lhgfaslk21490FAScVPkdsb53F9dNkfHG4faZSG5vfjndfcfgdssdgsdHF4663
          description: API key for authentication.
        - name: x-user-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: >-
              eyJlYW4iOiJVbnJlZ2lzdGVyZWRBcHBsaWNhdGlvbiIsImVrIjoiOE5sZ2cwcW5EUVdROUFNWGpXT2lmOWktZnpidG5KcUlqWGJ3WHJZZkpZcldrbG90ZEhvLVBjSWhQaU8xU1ZtMW84aU1WZGZqN2xWNzFjLXFxLmcybXE1dnh4Q1hUT25xaWRUaTFlcEhmVk1fIn0_
          description: User-specific authentication key.
        - name: username
          description: The username of the user to retrieve the live portfolio for.
          in: path
          schema:
            type: string
          required: true
      responses:
        '200':
          description: OK
          content:
            application/json:
              schema:
                type: object
                properties:
                  realizedCreditPct:
                    type: number
                    format: decimal
                    description: Credit as a percentage of the realized credit
                  unrealizedCreditPct:
                    type: number
                    format: decimal
                    description: Credit as a percentage of the unrealized credit
                  positions:
                    type: array
                    items:
                      type: object
                      properties:
                        positionId:
                          type: integer
                          description: Position ID
                        openTimestamp:
                          type: string
                          format: date-time
                          description: Open Timestamp
                        openRate:
                          type: number
                          format: decimal
                          description: Open Rate
                        instrumentId:
                          type: integer
                          description: Instrument ID
                        isBuy:
                          type: boolean
                          description: Buy/Sell
                        leverage:
                          type: integer
                          description: Leverage
                        takeProfitRate:
                          type: number
                          format: decimal
                          description: Take Profit
                        stopLossRate:
                          type: number
                          format: decimal
                          description: Stop Loss
                        socialTradeId:
                          type: integer
                          description: Mirror ID
                        parentPositionId:
                          type: integer
                          description: Parent Position ID
                        investmentPct:
                          type: number
                          format: decimal
                          description: Realized Investment
                        netProfit:
                          type: number
                          format: decimal
                          description: Profit Percentage
                        trailingStopLoss:
                          type: boolean
                          description: Trailing Stop loss enabled
                  socialTrades:
                    type: array
                    items:
                      type: object
                      properties:
                        socialTradeId:
                          type: integer
                          description: Internal Mirror ID
                        parentUsername:
                          type: string
                          description: Parent Username
                        stopLossPercentage:
                          type: number
                          format: decimal
                          description: Stop Loss
                        openTimestamp:
                          type: string
                          format: date-time
                          description: Opening Timestamp
                        investmentPct:
                          type: number
                          format: decimal
                          description: Investment Pct
                        openInvestmentPct:
                          type: number
                          format: decimal
                          description: Open Trades in Mirror
                        netProfit:
                          type: number
                          format: decimal
                          description: Profit Pct
                        openNetProfit:
                          type: number
                          format: decimal
                          description: Net profit of opened trades
                        closedNetProfit:
                          type: number
                          format: decimal
                          description: Net profit of closed trades
                        realizedPct:
                          type: number
                          format: decimal
                          description: Live Realized percentage
                        unrealizedPct:
                          type: number
                          format: decimal
                          description: Unrealized
                        isClosing:
                          type: boolean
                          description: Pending Close
                        positions:
                          type: array
                          items:
                            type: object
                            properties:
                              positionId:
                                type: integer
                                description: Position ID
                              openTimestamp:
                                type: string
                                format: date-time
                                description: Open Timestamp
                              openRate:
                                type: number
                                format: decimal
                                description: Open Rate
                              instrumentId:
                                type: integer
                                description: Instrument ID
                              isBuy:
                                type: boolean
                                description: Buy/Sell
                              leverage:
                                type: integer
                                description: Leverage
                              takeProfitRate:
                                type: number
                                format: decimal
                                description: Take Profit
                              stopLossRate:
                                type: number
                                format: decimal
                                description: Stop Loss
                              socialTradeId:
                                type: integer
                                description: Mirror ID
                              parentPositionId:
                                type: integer
                                description: Parent Position ID
                              investmentPct:
                                type: number
                                format: decimal
                                description: Realized Investment
                              netProfit:
                                type: number
                                format: decimal
                                description: Profit Percentage
                              trailingStopLoss:
                                type: boolean
                                description: Trailing Stop loss enabled

````

Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Get trade info for a specific user



## OpenAPI

````yaml api-reference/openapi.json get /api/v1/user-info/people/{username}/tradeinfo
openapi: 3.0.1
info:
  title: eToro Api
  version: v1.156.0
  description: >-
    eToro’s public API provides access to real-time financial data, trading
    insights, and account management features, allowing developers to integrate
    eToro’s services into their applications. With access to market prices,
    historical data, and social trading information, the API empowers users to
    enhance their trading strategies. Designed for security and scalability, the
    eToro API ensures smooth and reliable integration for a variety of financial
    applications.


    For more details on integrating with eToro's public WebSocket service,
    please refer to the dedicated [WebSocket
    documentation](./websocket/websocket-doc.html).
servers:
  - url: https://public-api.etoro.com
    description: eToro Public API
security: []
paths:
  /api/v1/user-info/people/{username}/tradeinfo:
    get:
      tags:
        - Users Info
      summary: Get trade info for a specific user
      parameters:
        - name: x-request-id
          in: header
          required: true
          schema:
            type: string
            format: uuid
            example: 23a96cff-1312-4df9-a45d-868e89d53622
          description: A unique request identifier.
        - name: x-api-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: lhgfaslk21490FAScVPkdsb53F9dNkfHG4faZSG5vfjndfcfgdssdgsdHF4663
          description: API key for authentication.
        - name: x-user-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: >-
              eyJlYW4iOiJVbnJlZ2lzdGVyZWRBcHBsaWNhdGlvbiIsImVrIjoiOE5sZ2cwcW5EUVdROUFNWGpXT2lmOWktZnpidG5KcUlqWGJ3WHJZZkpZcldrbG90ZEhvLVBjSWhQaU8xU1ZtMW84aU1WZGZqN2xWNzFjLXFxLmcybXE1dnh4Q1hUT25xaWRUaTFlcEhmVk1fIn0_
          description: User-specific authentication key.
        - name: username
          description: The username of the user to retrieve the discovery info for.
          in: path
          schema:
            type: string
          required: true
        - name: period
          in: query
          description: The period filter (e.g., LastTwoYears).
          required: true
          schema:
            type: string
            enum:
              - CurrMonth
              - CurrQuarter
              - CurrYear
              - LastYear
              - LastTwoYears
              - OneMonthAgo
              - TwoMonthsAgo
              - ThreeMonthsAgo
              - SixMonthsAgo
              - OneYearAgo
      responses:
        '200':
          description: OK
          content:
            application/json:
              schema:
                type: object
                properties:
                  userName:
                    type: string
                    description: The username of the customer
                  fullName:
                    type: string
                    description: Full name of the customer
                  weeksSinceRegistration:
                    type: integer
                    description: Number of weeks since registration
                  countryId:
                    type: integer
                    description: The registered country ID of the user
                  affiliateId:
                    type: integer
                    description: The affiliate ID of the user
                  isPopularInvestor:
                    type: boolean
                    description: Is the customer a popular investor
                  isFund:
                    type: boolean
                    description: Does this customer represent a fund
                  hasAvatar:
                    type: boolean
                    description: Does the customer have a picture
                  gain:
                    type: number
                    format: float
                    description: The periodic gain of the user
                  dailyGain:
                    type: number
                    format: float
                    description: The user's last day gain
                  thisWeekGain:
                    type: number
                    format: float
                    description: The user's gain from the beginning of the trading week
                  riskScore:
                    type: integer
                    description: The current risk score of the user
                  maxDailyRiskScore:
                    type: integer
                    description: The maximum daily risk score of the user in this interval
                  maxMonthlyRiskScore:
                    type: integer
                    description: >-
                      The maximum monthly risk score of the user in this
                      interval
                  copiers:
                    type: integer
                    description: The current number of copiers of this user
                  copiedTrades:
                    type: integer
                    description: The total number of copied trades in this interval
                  copyTradesPct:
                    type: number
                    format: float
                    description: >-
                      The percentage of copied trades in this interval of all
                      trades
                  copyInvestmentPct:
                    type: number
                    format: float
                    description: >-
                      The percentage of invested amounts in copied trades in
                      this interval of all investments
                  baseLineCopiers:
                    type: integer
                    description: The number of copiers one week ago
                  copiersGain:
                    type: number
                    format: float
                    description: The gain percentage of the number of copiers in a week
                  aumTier:
                    type: integer
                    description: >-
                      The total assets under management of the user, in a scale
                      of 0-4, where 4 is the highest tier
                  aumTierDesc:
                    type: string
                    description: Description of the AUM Tier
                  fundType:
                    type: integer
                    description: Fund Type
                  virtualCopiers:
                    type: integer
                    description: The total amount of virtual copiers of this user
                  trades:
                    type: integer
                    description: The total number of trades in this interval
                  topTradedInstrumentId:
                    type: integer
                    description: Top Traded Instrument ID in this interval
                  topTradedAssetId:
                    type: integer
                    description: Top Traded Asset ID in this interval
                  winRatio:
                    type: number
                    format: float
                    description: The winning ratio of all closed trades in this interval
                  dailyDd:
                    type: number
                    format: float
                    description: The maximum daily draw-down of the user in this interval
                  weeklyDd:
                    type: number
                    format: float
                    description: The maximum weekly draw-down of this user in this interval
                  peakToValley:
                    type: number
                    format: float
                    description: The peak to valley draw-down in this interval
                  profitableWeeksPct:
                    type: number
                    format: float
                    description: >-
                      The percentage of trading weeks which were profitable in
                      this interval
                  profitableMonthsPct:
                    type: number
                    format: float
                    description: >-
                      The percentage of months which were profitable in this
                      interval
                  avgPosSize:
                    type: number
                    format: float
                    description: >-
                      Average position size relative to the realized equity on
                      opening the trade
                  highLeveragePct:
                    type: number
                    format: float
                    description: High leverage trades percentage in this interval
                  mediumLeveragePct:
                    type: number
                    format: float
                    description: Medium leverage trades percentage in this interval
                  lowLeveragePct:
                    type: number
                    format: float
                    description: Low leverage trades percentage in this interval
                  firstActivity:
                    type: integer
                    description: >-
                      Number of days since the beginning of the interval of a
                      user trading activity
                  lastActivity:
                    type: integer
                    description: >-
                      Number of days from the last trading activity till the end
                      of the interval
                  activeWeeksPct:
                    type: number
                    format: float
                    description: >-
                      The percentage of weeks in the interval which the user had
                      active trades
                  instrumentPct:
                    type: number
                    format: float
                    description: Percentage of investment in the requested instrument ID

````

Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Overview

> Overview of the eToro WebSocket API

### What is the WebSocket API

The eToro WebSocket API provides real-time streaming access to market data and trading events through a persistent connection. This API enables developers to build responsive applications that react instantly to market changes, order updates, and portfolio events without the overhead of constant HTTP polling.

The API uses JSON message format over WebSocket protocol, supporting both public market data streams and private authenticated feeds for personalized trading information.

### Key Features

* **Real-time Market Data:** Live price feeds for instruments with bid/ask prices

* **Trading Notifications:** Instant updates for order executions, position changes, and portfolio events

* **Flexible Subscriptions:** Subscribe to specific instruments or private data feeds based on your needs

* **Snapshot Support:** Optional initial snapshots when subscribing to topics for current state

* **Authentication Integration:** Secure access to private data using your eToro API credentials


Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Authentication

> Authentication of the eToro WebSocket API

To use certain WebSocket channels, you must authenticate with your User and API keys.

### Request

```json  theme={null}
{
    "id": "ed72693c-1545-4fa1-8a10-aca7cf5419a6",
    "data": {
        "userKey": "<your user key>",
        "apiKey": "<your API key>"
    }
}
```

### Successful Response

```json  theme={null}
{
    "id": "ed72693c-1545-4fa1-8a10-aca7cf5419a6",
    "success": true,
    "operation": "Authenticate"
}
```

### Unsuccessful Response

```json  theme={null}
{
    "id": "ed72693c-1545-4fa1-8a10-aca7cf5419a6",
    "success": false,
    "operation": "Authenticate",
    "errorMessage": "<Error Message>",
    "errorCode": "<Error Code>"
}
```

### Error Codes

<ResponseField name="SessionAlreadyAuthenticated" type="Error Code">
  Session is already authenticated
</ResponseField>

<ResponseField name="DataRequired" type="Error Code">
  Data is required
</ResponseField>

<ResponseField name="ApiKeyRequired" type="Error Code">
  ApiKey is required
</ResponseField>

<ResponseField name="UserKeyRequired" type="Error Code">
  UserKey is required
</ResponseField>

<ResponseField name="TooManyRequests" type="Error Code">
  Too many requests
</ResponseField>

<ResponseField name="Forbidden" type="Error Code">
  Access to this resource is forbidden. Please contact customer support for assistance
</ResponseField>

<ResponseField name="UnhandledException" type="Error Code">
  Global Error
</ResponseField>

<ResponseField name="InvalidKey" type="Error Code">
  Key is invalid
</ResponseField>

<ResponseField name="Unauthorized" type="Error Code">
  Unauthorized
</ResponseField>


Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Topics

> Topics for the eToro WebSocket API

## Instrument Topics

Subscribe to real-time market price updates for various instruments.

### Request

```json  theme={null}
{
    "id": "ed72693c-1545-4fa1-8a10-aca7cf5419a6",
    "operation": "Subscribe",
    "data": {
        "topics": ["instrument:<instrumentId>","instrument:<instrumentId>",...],
        "snapshot": <true / false>
    }
}
```

### Response

```json  theme={null}
{
    "messages": [
        {
            "topic": "instrument:100000",
            "content": "{\"Ask\":\"84917.73\",\"Bid\":\"83232.21\",\"LastExecution\":\"84072.94\",\"Date\":\"2025-04-01T08:36:02.8305456Z\",\"NewUnitMargin\":\"83232.21\",\"UnitMarginAsk\":\"84917.73\",\"UnitMarginBid\":\"83232.21\",\"PriceRateID\":\"106439224591\",\"BidDiscounted\":\"84072.94\",\"AskDiscounted\":\"84076.96\",\"UnitMarginBidDiscounted\":\"84072.94\",\"UnitMarginAskDiscounted\":\"84076.96\"}",
            "id": "f1992278-2c4a-4b8f-92d6-8b99f5e1cb00",
            "type": "Trading.Instrument.Rate"
        }
    ]
}
```

### Prettified Content Field

```json  theme={null}
{
    "Ask": "84917.73",
    "Bid": "83232.21",
    "LastExecution": "84072.94",
    "Date": "2025-04-01T08:36:02.8305456Z",
    "NewUnitMargin": "83232.21",
    "UnitMarginAsk": "84917.73",
    "UnitMarginBid": "83232.21",
    "PriceRateID": "106439224591",
    "BidDiscounted": "84072.94",
    "AskDiscounted": "84076.96",
    "UnitMarginBidDiscounted": "84072.94",
    "UnitMarginAskDiscounted": "84076.96"
}
```

### Rate Object Schema

<ResponseField name="Ask" type="number <float>">
  Current asking price (offer) for the instrument. This is the price at which you can buy the asset.
</ResponseField>

<ResponseField name="Bid" type="number <float>">
  Current bid price for the instrument. This is the price at which you can sell the asset.
</ResponseField>

<ResponseField name="LastExecution" type="number <float>">
  Price of the most recent trade execution for this instrument.
</ResponseField>

<ResponseField name="Date" type="string <date-time>">
  The date-time of the price in the system.
</ResponseField>

<ResponseField name="NewUnitMargin" type="number <float>" deprecated>
  USD equivalent of the instrument price.
</ResponseField>

<ResponseField name="UnitMarginAsk" type="number <float>" deprecated>
  USD equivalent of the instrument ask price.
</ResponseField>

<ResponseField name="UnitMarginBid" type="number <float>" deprecated>
  USD equivalent of the instrument bid price.
</ResponseField>

<ResponseField name="PriceRateID" type="integer">
  Unique identifier of the rate.
</ResponseField>

<ResponseField name="BidDiscounted" type="number <float>" deprecated>
  Obsolete.
</ResponseField>

<ResponseField name="AskDiscounted" type="number <float>" deprecated>
  Obsolete.
</ResponseField>

<ResponseField name="UnitMarginBidDiscounted" type="number <float>" deprecated>
  Obsolete.
</ResponseField>

<ResponseField name="UnitMarginAskDiscounted" type="number <float>" deprecated>
  Obsolete.
</ResponseField>

## Transaction Updates

Receive real-time updates on transactions and orders from your portfolio.

### Request

```json  theme={null}
{
    "id": "ed72693c-1545-4fa1-8a10-aca7cf5419a6",
    "operation": "Subscribe",
    "data": {
        "topics": ["private"],
        "snapshot": <true / false>
    }
}
```

### Response

```json  theme={null}
{
    "messages": [
        {
            "topic": "private",
            "content": "{\"OrderID\":981286176,\"OrderType\":20,\"CID\":32612044,\"StatusID\":11,\"InstrumentID\":1111,\"UnitsToDeduct\":0.0,\"RequestGuid\":\"fca38698-1fcf-407d-b930-3222e57274fa\",\"RequestOccurred\":\"2025-04-01T08:55:53.6910145Z\",\"RequestToken\":\"fca38698-1fcf-407d-b930-3222e57274fa\",\"ErrorCode\":0,\"RequestedUnits\":13.859902,\"ExecutedUnits\":0.0,\"EndRate\":0.0,\"NetProfit\":0.0,\"CloseReason\":0,\"PendingClosePositionIDs\":[2980225895],\"OpenDateTime\":\"2025-04-01T08:55:53.6910145Z\",\"IsInMirror\":false,\"StatusId\":11,\"TotalExternalFees\":0.0,\"TotalExternalTaxes\":0.0,\"LotsToDeduct\":0.0,\"RequestedLots\":13.859902,\"ExecutedLots\":0.0}",
            "id": "5263070a-c52f-436b-8ca8-10b3bd6d2970",
            "type": "Trading.OrderForCloseMultiple.Update"
        }
    ]
}
```

### Prettified Content Field

```json  theme={null}
{
    "OrderID": 981286176,
    "OrderType": 20,
    "CID": 32613364,
    "StatusID": 11,
    "InstrumentID": 1111,
    "UnitsToDeduct": 0,
    "RequestGuid": "fca38698-1fcf-407d-b930-3222e57274fa",
    "RequestOccurred": "2025-04-01T08:55:53.6910145Z",
    "RequestToken": "fca38698-1fcf-407d-b930-3222e57274fa",
    "ErrorCode": 0,
    "RequestedUnits": 13.859902,
    "ExecutedUnits": 0,
    "EndRate": 0,
    "NetProfit": 0,
    "CloseReason": 0,
    "PendingClosePositionIDs": [
        2980225895
    ],
    "OpenDateTime": "2025-04-01T08:55:53.6910145Z",
    "IsInMirror": false,
    "StatusId": 11,
    "TotalExternalFees": 0,
    "TotalExternalTaxes": 0,
    "LotsToDeduct": 0,
    "RequestedLots": 13.859902,
    "ExecutedLots": 0
}
```


Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Example code

> Example code for the eToro WebSocket API

Here is an example of how to connect to the WebSocket API using JavaScript:

```javascript  theme={null}
let ws;
ws = new WebSocket("wss://ws.etoro.com/ws");

ws.onmessage = (event) => {
    // Implement your own logic here to handle incoming messages.
    logMessage("Received: " + event.data);
};

// Authenticate
const authRequest = {
    id: "<random guid>",
    operation: "Authenticate",
    data: {
        userKey: "<your user key>",
        apiKey: "<your API key>"
    }
};

ws.send(JSON.stringify(authRequest));

// Subscribe
const subscribeRequest = {
    id: "<random guid>",
    operation: "Subscribe",
    data: {
        topics: ["instrument:100000"], // Sending topic as an array
        snapshot: false
    }
};

ws.send(JSON.stringify(subscribeRequest));

// Unsubscribe
const unsubscribeRequest = {
    id: "<random guid>", 
    operation: "Unsubscribe",
    data: {
        topics: [topic] // Sending topic as an array
    }
};

ws.send(JSON.stringify(unsubscribeRequest));

// Close websocket
if (ws) {
    ws.close();
}
```


Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Places a Market-if-touched order (similar to Limit order) to open a position when a threshold price is reached.

> A Market-if-touched order is an order to open a new long or short position when a specific price or better appears in the Market. The price threshold is used to trigger a Market Order. This endpoint allows traders to set up Market-if-touched orders with parameters like leverage, stop-loss, and take-profit settings.



## OpenAPI

````yaml api-reference/openapi.json post /api/v1/trading/execution/limit-orders
openapi: 3.0.1
info:
  title: eToro Api
  version: v1.156.0
  description: >-
    eToro’s public API provides access to real-time financial data, trading
    insights, and account management features, allowing developers to integrate
    eToro’s services into their applications. With access to market prices,
    historical data, and social trading information, the API empowers users to
    enhance their trading strategies. Designed for security and scalability, the
    eToro API ensures smooth and reliable integration for a variety of financial
    applications.


    For more details on integrating with eToro's public WebSocket service,
    please refer to the dedicated [WebSocket
    documentation](./websocket/websocket-doc.html).
servers:
  - url: https://public-api.etoro.com
    description: eToro Public API
security: []
paths:
  /api/v1/trading/execution/limit-orders:
    post:
      tags:
        - Trading - Real
      summary: >-
        Places a Market-if-touched order (similar to Limit order) to open a
        position when a threshold price is reached.
      description: >-
        A Market-if-touched order is an order to open a new long or short
        position when a specific price or better appears in the Market. The
        price threshold is used to trigger a Market Order. This endpoint allows
        traders to set up Market-if-touched orders with parameters like
        leverage, stop-loss, and take-profit settings.
      operationId: openLimitOrder
      parameters:
        - name: x-request-id
          in: header
          required: true
          schema:
            type: string
            format: uuid
            example: a80ba158-c2cb-4996-86ad-3d37098c2b0d
          description: A unique request identifier.
        - name: x-api-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: lhgfaslk21490FAScVPkdsb53F9dNkfHG4faZSG5vfjndfcfgdssdgsdHF4663
          description: API key for authentication.
        - name: x-user-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: >-
              eyJlYW4iOiJVbnJlZ2lzdGVyZWRBcHBsaWNhdGlvbiIsImVrIjoiOE5sZ2cwcW5EUVdROUFNWGpXT2lmOWktZnpidG5KcUlqWGJ3WHJZZkpZcldrbG90ZEhvLVBjSWhQaU8xU1ZtMW84aU1WZGZqN2xWNzFjLXFxLmcybXE1dnh4Q1hUT25xaWRUaTFlcEhmVk1fIn0_
          description: User-specific authentication key.
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                InstrumentID:
                  type: integer
                  format: int32
                  description: The unique identifier of the financial instrument.
                IsBuy:
                  type: boolean
                  description: >-
                    Indicates whether the order will open a long (true) or short
                    (false) position.
                Leverage:
                  type: integer
                  format: int32
                  description: The leverage ratio for the order.
                Amount:
                  type: number
                  format: double
                  nullable: true
                  description: >-
                    The amount of the trade in the account currency [USD].
                    Required if AmountInUnits is not provided.
                AmountInUnits:
                  type: number
                  format: double
                  nullable: true
                  description: >-
                    The number of units of the asset. Required if Amount is not
                    provided. For most assets this can be a fractional number.
                    Note that for Future Contracts this number should indicate
                    the number of underlying units, and not the number of
                    contracts, according to the formula: AmountInUnits =
                    contract multiplier * number of contracts.
                StopLossRate:
                  type: number
                  format: double
                  description: >-
                    The stop-loss trigger price at which the position will
                    generate a Market Order to close (after it was opened).
                    StopLoss trigger price must be worse than current price.
                TakeProfitRate:
                  type: number
                  format: double
                  description: >-
                    The take-profit trigger price at which the position will
                    generate a Market Order to close (after it has opened).
                    TakeProfit trigger price must be better than the current
                    price.
                Rate:
                  type: number
                  format: double
                  description: >-
                    The trigger price at which a Market order to open the
                    position will be sent for execution. The trigger price must
                    be better than the current price. This means that the
                    trigger price must be lower than current price for Long
                    positions, and higher than current price for Short
                    positions.
                IsTslEnabled:
                  type: boolean
                  nullable: true
                  description: >-
                    Indicates if a trailing stop loss (TSL) is enabled. This
                    means that the stoploss rate indicated will get updated
                    automatically whenever the asset price increases (for long
                    positions) or decreases (for short position) effectively
                    keeping the stoploss in a constant gap from the best price
                    achieved so far.
                IsDiscounted:
                  type: boolean
                  nullable: true
                  description: SHOULD NOT BE EXTERNALZIED
                IsNoStopLoss:
                  type: boolean
                  nullable: true
                  description: Indicates if stop-loss is disabled.
                IsNoTakeProfit:
                  type: boolean
                  nullable: true
                  description: Indicates if take-profit is disabled.
                CID:
                  type: integer
                  format: int32
                  description: SHOULD NOT BE EXTERNALIZED.
              additionalProperties: false
      responses:
        '200':
          description: >-
            Market-if-touched order successfully placed. The response includes a
            confirmation token.
          content:
            application/json:
              schema:
                type: object
                properties:
                  token:
                    type: string
                    format: uuid
                    description: A confirmation token indicating the order creation.
                required:
                  - token
              example:
                token: 9af05785-be29-482d-a892-9d9be4fd34bc

````

Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Cancels a Market-if-touched order that has not yet been executed.

> This endpoint allows traders to cancel a Market-if-touched order before it is executed. Once canceled, the order will no longer be processed.



## OpenAPI

````yaml api-reference/openapi.json delete /api/v1/trading/execution/limit-orders/{orderId}
openapi: 3.0.1
info:
  title: eToro Api
  version: v1.156.0
  description: >-
    eToro’s public API provides access to real-time financial data, trading
    insights, and account management features, allowing developers to integrate
    eToro’s services into their applications. With access to market prices,
    historical data, and social trading information, the API empowers users to
    enhance their trading strategies. Designed for security and scalability, the
    eToro API ensures smooth and reliable integration for a variety of financial
    applications.


    For more details on integrating with eToro's public WebSocket service,
    please refer to the dedicated [WebSocket
    documentation](./websocket/websocket-doc.html).
servers:
  - url: https://public-api.etoro.com
    description: eToro Public API
security: []
paths:
  /api/v1/trading/execution/limit-orders/{orderId}:
    delete:
      tags:
        - Trading - Real
      summary: Cancels a Market-if-touched order that has not yet been executed.
      description: >-
        This endpoint allows traders to cancel a Market-if-touched order before
        it is executed. Once canceled, the order will no longer be processed.
      operationId: cancelLimitOrder
      parameters:
        - name: x-request-id
          in: header
          required: true
          schema:
            type: string
            format: uuid
            example: 7fe3176e-e4b5-4471-8e05-1c290a894f41
          description: A unique request identifier.
        - name: x-api-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: lhgfaslk21490FAScVPkdsb53F9dNkfHG4faZSG5vfjndfcfgdssdgsdHF4663
          description: API key for authentication.
        - name: x-user-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: >-
              eyJlYW4iOiJVbnJlZ2lzdGVyZWRBcHBsaWNhdGlvbiIsImVrIjoiOE5sZ2cwcW5EUVdROUFNWGpXT2lmOWktZnpidG5KcUlqWGJ3WHJZZkpZcldrbG90ZEhvLVBjSWhQaU8xU1ZtMW84aU1WZGZqN2xWNzFjLXFxLmcybXE1dnh4Q1hUT25xaWRUaTFlcEhmVk1fIn0_
          description: User-specific authentication key.
        - name: orderId
          in: path
          required: true
          schema:
            type: integer
            format: int64
          description: The unique identifier of the Market-if-touched order to be canceled.
      responses:
        '200':
          description: >-
            Successfully canceled the Market-if-touched order. The response
            includes a confirmation token.
          content:
            application/json:
              schema:
                type: object
                properties:
                  token:
                    type: string
                    format: uuid
                    description: A confirmation token indicating the order cancellation.
                required:
                  - token
              example:
                token: 9af05785-be29-482d-a892-9d9be4fd34bc

````

Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Creates a market order to close a position or partially close it by specifying the number of units to deduct.

> This endpoint allows traders to close an entire position or a portion of it at the current market rate. If `UnitsToDeduct` is provided, only the specified portion will be closed. If `UnitsToDeduct` is omitted or set to null, the full position will be closed.



## OpenAPI

````yaml api-reference/openapi.json post /api/v1/trading/execution/market-close-orders/positions/{positionId}
openapi: 3.0.1
info:
  title: eToro Api
  version: v1.156.0
  description: >-
    eToro’s public API provides access to real-time financial data, trading
    insights, and account management features, allowing developers to integrate
    eToro’s services into their applications. With access to market prices,
    historical data, and social trading information, the API empowers users to
    enhance their trading strategies. Designed for security and scalability, the
    eToro API ensures smooth and reliable integration for a variety of financial
    applications.


    For more details on integrating with eToro's public WebSocket service,
    please refer to the dedicated [WebSocket
    documentation](./websocket/websocket-doc.html).
servers:
  - url: https://public-api.etoro.com
    description: eToro Public API
security: []
paths:
  /api/v1/trading/execution/market-close-orders/positions/{positionId}:
    post:
      tags:
        - Trading - Real
      summary: >-
        Creates a market order to close a position or partially close it by
        specifying the number of units to deduct.
      description: >-
        This endpoint allows traders to close an entire position or a portion of
        it at the current market rate. If `UnitsToDeduct` is provided, only the
        specified portion will be closed. If `UnitsToDeduct` is omitted or set
        to null, the full position will be closed.
      operationId: closePositionByMarketRate
      parameters:
        - name: x-request-id
          in: header
          required: true
          schema:
            type: string
            format: uuid
            example: efc96a05-454b-4867-8ecb-7443c14cd375
          description: A unique request identifier.
        - name: x-api-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: lhgfaslk21490FAScVPkdsb53F9dNkfHG4faZSG5vfjndfcfgdssdgsdHF4663
          description: API key for authentication.
        - name: x-user-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: >-
              eyJlYW4iOiJVbnJlZ2lzdGVyZWRBcHBsaWNhdGlvbiIsImVrIjoiOE5sZ2cwcW5EUVdROUFNWGpXT2lmOWktZnpidG5KcUlqWGJ3WHJZZkpZcldrbG90ZEhvLVBjSWhQaU8xU1ZtMW84aU1WZGZqN2xWNzFjLXFxLmcybXE1dnh4Q1hUT25xaWRUaTFlcEhmVk1fIn0_
          description: User-specific authentication key.
        - name: positionId
          in: path
          description: The unique identifier of the position to close.
          required: true
          schema:
            type: integer
            format: int64
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                InstrumentId:
                  type: integer
                  format: int32
                  description: >-
                    The ID of the financial instrument associated with the
                    position.
                UnitsToDeduct:
                  type: number
                  format: double
                  nullable: true
                  description: >-
                    The number of units to close. If omitted or null, the entire
                    position will be closed.
              required:
                - InstrumentId
      responses:
        '200':
          description: Successfully closed a position or a part of it.
          content:
            application/json:
              schema:
                type: object
                properties:
                  orderForClose:
                    type: object
                    properties:
                      positionID:
                        type: integer
                        description: The ID of the closed position.
                      instrumentID:
                        type: integer
                        description: The ID of the instrument traded.
                      unitsToDeduct:
                        type: number
                        format: double
                        description: The number of units closed in this order.
                      orderID:
                        type: integer
                        description: The unique identifier of the closing order.
                      orderType:
                        type: integer
                        description: The type of order executed.
                      statusID:
                        type: integer
                        description: The status of the closing order.
                      CID:
                        type: integer
                        description: Customer Account ID associated with the order.
                      openDateTime:
                        type: string
                        format: date-time
                        description: The timestamp when the order was placed.
                      lastUpdate:
                        type: string
                        format: date-time
                        description: The timestamp of the last update to this order.
                  token:
                    type: string
                    format: uuid
                    description: A unique confirmation token for the closing order.
              example:
                orderForClose:
                  positionID: 2150941015
                  instrumentID: 1111
                  unitsToDeduct: 2
                  orderID: 13904638
                  orderType: 19
                  statusID: 1
                  CID: 7765437
                  openDateTime: '2025-04-02T16:07:54.0880338Z'
                  lastUpdate: '2025-04-02T16:07:54.0880338Z'
                token: 5fe065bc-f6f9-4897-a2ce-c4fccef73ff8

````

Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Creates a market order to close a position or partially close it by specifying the number of units to deduct.

> This endpoint allows traders to close an entire position or a portion of it at the current market rate. If `UnitsToDeduct` is provided, only the specified portion will be closed. If `UnitsToDeduct` is omitted or set to null, the full position will be closed.



## OpenAPI

````yaml api-reference/openapi.json post /api/v1/trading/execution/market-close-orders/positions/{positionId}
openapi: 3.0.1
info:
  title: eToro Api
  version: v1.156.0
  description: >-
    eToro’s public API provides access to real-time financial data, trading
    insights, and account management features, allowing developers to integrate
    eToro’s services into their applications. With access to market prices,
    historical data, and social trading information, the API empowers users to
    enhance their trading strategies. Designed for security and scalability, the
    eToro API ensures smooth and reliable integration for a variety of financial
    applications.


    For more details on integrating with eToro's public WebSocket service,
    please refer to the dedicated [WebSocket
    documentation](./websocket/websocket-doc.html).
servers:
  - url: https://public-api.etoro.com
    description: eToro Public API
security: []
paths:
  /api/v1/trading/execution/market-close-orders/positions/{positionId}:
    post:
      tags:
        - Trading - Real
      summary: >-
        Creates a market order to close a position or partially close it by
        specifying the number of units to deduct.
      description: >-
        This endpoint allows traders to close an entire position or a portion of
        it at the current market rate. If `UnitsToDeduct` is provided, only the
        specified portion will be closed. If `UnitsToDeduct` is omitted or set
        to null, the full position will be closed.
      operationId: closePositionByMarketRate
      parameters:
        - name: x-request-id
          in: header
          required: true
          schema:
            type: string
            format: uuid
            example: efc96a05-454b-4867-8ecb-7443c14cd375
          description: A unique request identifier.
        - name: x-api-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: lhgfaslk21490FAScVPkdsb53F9dNkfHG4faZSG5vfjndfcfgdssdgsdHF4663
          description: API key for authentication.
        - name: x-user-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: >-
              eyJlYW4iOiJVbnJlZ2lzdGVyZWRBcHBsaWNhdGlvbiIsImVrIjoiOE5sZ2cwcW5EUVdROUFNWGpXT2lmOWktZnpidG5KcUlqWGJ3WHJZZkpZcldrbG90ZEhvLVBjSWhQaU8xU1ZtMW84aU1WZGZqN2xWNzFjLXFxLmcybXE1dnh4Q1hUT25xaWRUaTFlcEhmVk1fIn0_
          description: User-specific authentication key.
        - name: positionId
          in: path
          description: The unique identifier of the position to close.
          required: true
          schema:
            type: integer
            format: int64
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                InstrumentId:
                  type: integer
                  format: int32
                  description: >-
                    The ID of the financial instrument associated with the
                    position.
                UnitsToDeduct:
                  type: number
                  format: double
                  nullable: true
                  description: >-
                    The number of units to close. If omitted or null, the entire
                    position will be closed.
              required:
                - InstrumentId
      responses:
        '200':
          description: Successfully closed a position or a part of it.
          content:
            application/json:
              schema:
                type: object
                properties:
                  orderForClose:
                    type: object
                    properties:
                      positionID:
                        type: integer
                        description: The ID of the closed position.
                      instrumentID:
                        type: integer
                        description: The ID of the instrument traded.
                      unitsToDeduct:
                        type: number
                        format: double
                        description: The number of units closed in this order.
                      orderID:
                        type: integer
                        description: The unique identifier of the closing order.
                      orderType:
                        type: integer
                        description: The type of order executed.
                      statusID:
                        type: integer
                        description: The status of the closing order.
                      CID:
                        type: integer
                        description: Customer Account ID associated with the order.
                      openDateTime:
                        type: string
                        format: date-time
                        description: The timestamp when the order was placed.
                      lastUpdate:
                        type: string
                        format: date-time
                        description: The timestamp of the last update to this order.
                  token:
                    type: string
                    format: uuid
                    description: A unique confirmation token for the closing order.
              example:
                orderForClose:
                  positionID: 2150941015
                  instrumentID: 1111
                  unitsToDeduct: 2
                  orderID: 13904638
                  orderType: 19
                  statusID: 1
                  CID: 7765437
                  openDateTime: '2025-04-02T16:07:54.0880338Z'
                  lastUpdate: '2025-04-02T16:07:54.0880338Z'
                token: 5fe065bc-f6f9-4897-a2ce-c4fccef73ff8

````

Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Create a market order to open a position by specifying the amount of cash you would like to use in the trade.

> This endpoint allows traders to place a market order to open a position by specifying the investment amount instead of specifying the number of units. The trade will be executed at the market price, and leverage, stop-loss, and take-profit settings can be applied.



## OpenAPI

````yaml api-reference/openapi.json post /api/v1/trading/execution/market-open-orders/by-amount
openapi: 3.0.1
info:
  title: eToro Api
  version: v1.156.0
  description: >-
    eToro’s public API provides access to real-time financial data, trading
    insights, and account management features, allowing developers to integrate
    eToro’s services into their applications. With access to market prices,
    historical data, and social trading information, the API empowers users to
    enhance their trading strategies. Designed for security and scalability, the
    eToro API ensures smooth and reliable integration for a variety of financial
    applications.


    For more details on integrating with eToro's public WebSocket service,
    please refer to the dedicated [WebSocket
    documentation](./websocket/websocket-doc.html).
servers:
  - url: https://public-api.etoro.com
    description: eToro Public API
security: []
paths:
  /api/v1/trading/execution/market-open-orders/by-amount:
    post:
      tags:
        - Trading - Real
      summary: >-
        Create a market order to open a position by specifying the amount of
        cash you would like to use in the trade.
      description: >-
        This endpoint allows traders to place a market order to open a position
        by specifying the investment amount instead of specifying the number of
        units. The trade will be executed at the market price, and leverage,
        stop-loss, and take-profit settings can be applied.
      operationId: openMarketPositionByAmount
      parameters:
        - name: x-request-id
          in: header
          required: true
          schema:
            type: string
            format: uuid
            example: 80d10d1f-e095-49b9-886d-8d88b7ce0ba9
          description: A unique request identifier.
        - name: x-api-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: lhgfaslk21490FAScVPkdsb53F9dNkfHG4faZSG5vfjndfcfgdssdgsdHF4663
          description: API key for authentication.
        - name: x-user-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: >-
              eyJlYW4iOiJVbnJlZ2lzdGVyZWRBcHBsaWNhdGlvbiIsImVrIjoiOE5sZ2cwcW5EUVdROUFNWGpXT2lmOWktZnpidG5KcUlqWGJ3WHJZZkpZcldrbG90ZEhvLVBjSWhQaU8xU1ZtMW84aU1WZGZqN2xWNzFjLXFxLmcybXE1dnh4Q1hUT25xaWRUaTFlcEhmVk1fIn0_
          description: User-specific authentication key.
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                InstrumentID:
                  type: integer
                  format: int32
                  description: The unique identifier of the financial instrument to trade.
                IsBuy:
                  type: boolean
                  description: True for a long position, false for a short position.
                Leverage:
                  type: integer
                  format: int32
                  description: The leverage multiplier for the trade.
                Amount:
                  type: number
                  format: double
                  description: The amount of money to invest in the trade.
                StopLossRate:
                  type: number
                  format: double
                  nullable: true
                  description: >-
                    The stop-loss trigger price at which the position will
                    generate a Market Order to close (after it was opened).
                    StopLoss trigger price must be worse than current price.
                TakeProfitRate:
                  type: number
                  format: double
                  nullable: true
                  description: >-
                    The take-profit trigger price at which the position will
                    generate a Market Order to close (after it has opened).
                    TakeProfit trigger price must be better than the current
                    price.
                IsTslEnabled:
                  type: boolean
                  nullable: true
                  description: >-
                    Indicates if a trailing stop loss (TSL) is enabled. This
                    means that the stoploss rate indicated will get updated
                    automatically whenever the asset price increases (for long
                    positions) or decreases (for short position) effectively
                    keeping the stoploss in a constant gap from the best price
                    achieved so far.
                IsNoStopLoss:
                  type: boolean
                  nullable: true
                  description: True if no stop-loss is set for this order.
                IsNoTakeProfit:
                  type: boolean
                  nullable: true
                  description: True if no take-profit is set for this order.
              required:
                - InstrumentID
                - IsBuy
                - Leverage
                - Amount
      responses:
        '200':
          description: Successfully opened a market order.
          content:
            application/json:
              schema:
                type: object
                properties:
                  orderForOpen:
                    type: object
                    properties:
                      instrumentID:
                        type: integer
                        description: The ID of the traded instrument.
                      amount:
                        type: integer
                        description: The invested amount.
                      isBuy:
                        type: boolean
                        description: True for a long position, false for a short position.
                      leverage:
                        type: integer
                        description: The leverage applied to the trade.
                      stopLossRate:
                        type: integer
                        description: The stop-loss threshold rate, if applicable.
                      takeProfitRate:
                        type: integer
                        description: The take-profit thereshold rate, if applicable.
                      isTslEnabled:
                        type: boolean
                        description: Indicates if trailing stop-loss is enabled.
                      mirrorID:
                        type: integer
                        description: ID related to mirrored trades, if applicable.
                      totalExternalCosts:
                        type: integer
                        description: >-
                          Total external costs associated with opening the
                          trade.
                      orderID:
                        type: integer
                        description: The unique order identifier.
                      orderType:
                        type: integer
                        description: The type of order executed.
                      statusID:
                        type: integer
                        description: The status of the order.
                      CID:
                        type: integer
                        description: Customer ID associated with the order.
                      openDateTime:
                        type: string
                        format: date-time
                        description: The timestamp when the order was opened.
                      lastUpdate:
                        type: string
                        format: date-time
                        description: The last update timestamp of the order.
                  token:
                    type: string
                    format: uuid
                    description: A unique confirmation token for the order.
              example:
                orderForOpen:
                  instrumentID: 100000
                  amount: 150
                  isBuy: true
                  leverage: 1
                  stopLossRate: 0
                  takeProfitRate: 0
                  isTslEnabled: false
                  mirrorID: 0
                  totalExternalCosts: 0
                  orderID: 13902598
                  orderType: 17
                  statusID: 1
                  CID: 7765437
                  openDateTime: '2025-04-02T15:47:15.9370502Z'
                  lastUpdate: '2025-04-02T15:47:15.9370502Z'
                token: 066faaee-e1e9-49d2-a568-c6e1cc336ad8

````

Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Places a Market Order to open a position by specifying the number of units you would like to trade.

> This endpoint allows traders to place a market order to open a position by specifying the number of units (rather than an amount in cash). The trade is executed at the current market price, and optional settings like leverage, stop-loss, and take-profit can be applied.



## OpenAPI

````yaml api-reference/openapi.json post /api/v1/trading/execution/market-open-orders/by-units
openapi: 3.0.1
info:
  title: eToro Api
  version: v1.156.0
  description: >-
    eToro’s public API provides access to real-time financial data, trading
    insights, and account management features, allowing developers to integrate
    eToro’s services into their applications. With access to market prices,
    historical data, and social trading information, the API empowers users to
    enhance their trading strategies. Designed for security and scalability, the
    eToro API ensures smooth and reliable integration for a variety of financial
    applications.


    For more details on integrating with eToro's public WebSocket service,
    please refer to the dedicated [WebSocket
    documentation](./websocket/websocket-doc.html).
servers:
  - url: https://public-api.etoro.com
    description: eToro Public API
security: []
paths:
  /api/v1/trading/execution/market-open-orders/by-units:
    post:
      tags:
        - Trading - Real
      summary: >-
        Places a Market Order to open a position by specifying the number of
        units you would like to trade.
      description: >-
        This endpoint allows traders to place a market order to open a position
        by specifying the number of units (rather than an amount in cash). The
        trade is executed at the current market price, and optional settings
        like leverage, stop-loss, and take-profit can be applied.
      operationId: openMarketPositionByUnits
      parameters:
        - name: x-request-id
          in: header
          required: true
          schema:
            type: string
            format: uuid
            example: 9a662aae-3ab0-426b-ac6a-4f98582f4c06
          description: A unique request identifier.
        - name: x-api-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: lhgfaslk21490FAScVPkdsb53F9dNkfHG4faZSG5vfjndfcfgdssdgsdHF4663
          description: API key for authentication.
        - name: x-user-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: >-
              eyJlYW4iOiJVbnJlZ2lzdGVyZWRBcHBsaWNhdGlvbiIsImVrIjoiOE5sZ2cwcW5EUVdROUFNWGpXT2lmOWktZnpidG5KcUlqWGJ3WHJZZkpZcldrbG90ZEhvLVBjSWhQaU8xU1ZtMW84aU1WZGZqN2xWNzFjLXFxLmcybXE1dnh4Q1hUT25xaWRUaTFlcEhmVk1fIn0_
          description: User-specific authentication key.
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                InstrumentID:
                  type: integer
                  format: int32
                  description: The unique identifier of the financial instrument to trade.
                IsBuy:
                  type: boolean
                  description: True for a long position, false for a short position.
                Leverage:
                  type: integer
                  format: int32
                  description: The leverage multiplier for the trade.
                AmountInUnits:
                  type: number
                  format: double
                  description: >-
                    The number of units of the asset. Required if Amount is not
                    provided. For most assets this can be a fractional number.
                    Note that for Future Contracts this number should indicate
                    the number of underlying units, and not the number of
                    contracts, according to the formula: AmountInUnits =
                    contract multiplier * number of contracts.
                StopLossRate:
                  type: number
                  format: double
                  nullable: true
                  description: >-
                    The stop-loss trigger price at which the position will
                    generate a Market Order to close (after it was opened).
                    StopLoss trigger price must be worse than current price.
                TakeProfitRate:
                  type: number
                  format: double
                  nullable: true
                  description: >-
                    The take-profit trigger price at which the position will
                    generate a Market Order to close (after it has opened).
                    TakeProfit trigger price must be better than the current
                    price.
                IsTslEnabled:
                  type: boolean
                  nullable: true
                  description: >-
                    Indicates if a trailing stop loss (TSL) is enabled. This
                    means that the stoploss rate indicated will get updated
                    automatically whenever the asset price increases (for long
                    positions) or decreases (for short position) effectively
                    keeping the stoploss in a constant gap from the best price
                    achieved so far.
                IsNoStopLoss:
                  type: boolean
                  nullable: true
                  description: True if no stop-loss is set for this order.
                IsNoTakeProfit:
                  type: boolean
                  nullable: true
                  description: True if no take-profit is set for this order.
              required:
                - InstrumentID
                - IsBuy
                - Leverage
                - AmountInUnits
      responses:
        '200':
          description: Successfully opened a market order.
          content:
            application/json:
              schema:
                type: object
                properties:
                  orderForOpen:
                    type: object
                    properties:
                      instrumentID:
                        type: integer
                        description: The ID of the traded instrument.
                      amount:
                        type: integer
                        description: The amount invested in the trade.
                      amountInUnits:
                        type: number
                        format: double
                        description: The number of units traded.
                      isBuy:
                        type: boolean
                        description: True for a long position, false for a short position.
                      leverage:
                        type: integer
                        description: The leverage applied to the trade.
                      stopLossRate:
                        type: integer
                        description: The stop-loss threshold rate, if applicable.
                      takeProfitRate:
                        type: integer
                        description: The take-profit thereshold rate, if applicable.
                      isTslEnabled:
                        type: boolean
                        description: Indicates if trailing stop-loss is enabled.
                      mirrorID:
                        type: integer
                        description: ID related to mirrored trades, if applicable.
                      totalExternalCosts:
                        type: integer
                        description: Total external costs associated with the trade.
                      lotCount:
                        type: integer
                        description: The number of lots in the order.
                      orderID:
                        type: integer
                        description: The unique order identifier.
                      orderType:
                        type: integer
                        description: The type of order executed.
                      statusID:
                        type: integer
                        description: The status of the order.
                      CID:
                        type: integer
                        description: Customer Account ID associated with the order.
                      openDateTime:
                        type: string
                        format: date-time
                        description: The timestamp when the order was opened.
                      lastUpdate:
                        type: string
                        format: date-time
                        description: The last update timestamp of the order.
                  token:
                    type: string
                    format: uuid
                    description: A unique confirmation token for the order.
              example:
                orderForOpen:
                  instrumentID: 100000
                  amount: 0
                  amountInUnits: 0.001
                  isBuy: true
                  leverage: 1
                  stopLossRate: 0
                  takeProfitRate: 0
                  isTslEnabled: false
                  mirrorID: 0
                  totalExternalCosts: 0
                  lotCount: 0
                  orderID: 13906629
                  orderType: 18
                  statusID: 1
                  CID: 7765437
                  openDateTime: '2025-04-02T15:56:50.7496838Z'
                  lastUpdate: '2025-04-02T15:56:50.7496838Z'
                token: 43ceb769-cff6-45ec-8ad7-292b7401353f

````

Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Cancels a market order for open before it is executed.

> This endpoint allows traders to cancel a market order for open before execution. If the order has already been processed, cancellation will not be possible.



## OpenAPI

````yaml api-reference/openapi.json delete /api/v1/trading/execution/market-open-orders/{orderId}
openapi: 3.0.1
info:
  title: eToro Api
  version: v1.156.0
  description: >-
    eToro’s public API provides access to real-time financial data, trading
    insights, and account management features, allowing developers to integrate
    eToro’s services into their applications. With access to market prices,
    historical data, and social trading information, the API empowers users to
    enhance their trading strategies. Designed for security and scalability, the
    eToro API ensures smooth and reliable integration for a variety of financial
    applications.


    For more details on integrating with eToro's public WebSocket service,
    please refer to the dedicated [WebSocket
    documentation](./websocket/websocket-doc.html).
servers:
  - url: https://public-api.etoro.com
    description: eToro Public API
security: []
paths:
  /api/v1/trading/execution/market-open-orders/{orderId}:
    delete:
      tags:
        - Trading - Real
      summary: Cancels a market order for open before it is executed.
      description: >-
        This endpoint allows traders to cancel a market order for open before
        execution. If the order has already been processed, cancellation will
        not be possible.
      operationId: cancelOpenMarketOrder
      parameters:
        - name: x-request-id
          in: header
          required: true
          schema:
            type: string
            format: uuid
            example: d2872a09-ac4a-414b-b18d-de09a87d5de8
          description: A unique request identifier.
        - name: x-api-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: lhgfaslk21490FAScVPkdsb53F9dNkfHG4faZSG5vfjndfcfgdssdgsdHF4663
          description: API key for authentication.
        - name: x-user-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: >-
              eyJlYW4iOiJVbnJlZ2lzdGVyZWRBcHBsaWNhdGlvbiIsImVrIjoiOE5sZ2cwcW5EUVdROUFNWGpXT2lmOWktZnpidG5KcUlqWGJ3WHJZZkpZcldrbG90ZEhvLVBjSWhQaU8xU1ZtMW84aU1WZGZqN2xWNzFjLXFxLmcybXE1dnh4Q1hUT25xaWRUaTFlcEhmVk1fIn0_
          description: User-specific authentication key.
        - name: orderId
          in: path
          required: true
          schema:
            type: integer
            format: int64
          description: The unique identifier of the market order for open to be canceled.
      responses:
        '200':
          description: >-
            Successfully canceled the pending market order. The response
            includes a confirmation token.
          content:
            application/json:
              schema:
                type: object
                properties:
                  token:
                    type: string
                    format: uuid
                    description: A confirmation token indicating the order cancellation.
                required:
                  - token
              example:
                token: 9af05785-be29-482d-a892-9d9be4fd34bc

````

Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Retrieve comprehensive portfolio information including positions, orders, and account status

> Returns detailed portfolio information including active positions, pending orders, mirror trading details, and account balances. This endpoint provides a complete overview of the user's trading activity and current market exposure.



## OpenAPI

````yaml api-reference/openapi.json get /api/v1/trading/info/portfolio
openapi: 3.0.1
info:
  title: eToro Api
  version: v1.156.0
  description: >-
    eToro’s public API provides access to real-time financial data, trading
    insights, and account management features, allowing developers to integrate
    eToro’s services into their applications. With access to market prices,
    historical data, and social trading information, the API empowers users to
    enhance their trading strategies. Designed for security and scalability, the
    eToro API ensures smooth and reliable integration for a variety of financial
    applications.


    For more details on integrating with eToro's public WebSocket service,
    please refer to the dedicated [WebSocket
    documentation](./websocket/websocket-doc.html).
servers:
  - url: https://public-api.etoro.com
    description: eToro Public API
security: []
paths:
  /api/v1/trading/info/portfolio:
    get:
      tags:
        - Trading - Real
      summary: >-
        Retrieve comprehensive portfolio information including positions,
        orders, and account status
      description: >-
        Returns detailed portfolio information including active positions,
        pending orders, mirror trading details, and account balances. This
        endpoint provides a complete overview of the user's trading activity and
        current market exposure.
      operationId: getPortfolio
      parameters:
        - name: x-request-id
          in: header
          required: true
          schema:
            type: string
            format: uuid
            example: 269a98a4-9a55-4d81-a31c-7ef724dcdcd4
          description: A unique request identifier.
        - name: x-api-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: lhgfaslk21490FAScVPkdsb53F9dNkfHG4faZSG5vfjndfcfgdssdgsdHF4663
          description: API key for authentication.
        - name: x-user-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: >-
              eyJlYW4iOiJVbnJlZ2lzdGVyZWRBcHBsaWNhdGlvbiIsImVrIjoiOE5sZ2cwcW5EUVdROUFNWGpXT2lmOWktZnpidG5KcUlqWGJ3WHJZZkpZcldrbG90ZEhvLVBjSWhQaU8xU1ZtMW84aU1WZGZqN2xWNzFjLXFxLmcybXE1dnh4Q1hUT25xaWRUaTFlcEhmVk1fIn0_
          description: User-specific authentication key.
      responses:
        '200':
          description: Successfully retrieved portfolio information
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/PortfolioResponse'
              example:
                clientPortfolio:
                  positions:
                    - positionID: 2150896073
                      CID: 7765437
                      openDateTime: '2024-08-01T07:44:26.103Z'
                      openRate: 2020.7784
                      instrumentID: 1002
                      isBuy: true
                      takeProfitRate: 0
                      stopLossRate: 0.0001
                      mirrorID: 0
                      parentPositionID: 0
                      amount: 100
                      leverage: 1
                      orderID: 12402059
                      orderType: 17
                      units: 0.049485
                      totalFees: 0
                      initialAmountInDollars: 100
                      isTslEnabled: false
                      stopLossVersion: 3
                      isSettled: true
                      redeemStatusID: 0
                      initialUnits: 0.049485
                      isPartiallyAltered: false
                      unitsBaseValueDollars: 100
                      isDiscounted: true
                      openPositionActionType: 0
                      settlementTypeID: 1
                      isDetached: false
                      openConversionRate: 1
                      pnlVersion: 1
                      totalExternalFees: 0
                      totalExternalTaxes: 0
                      isNoTakeProfit: true
                      isNoStopLoss: true
                      lotCount: 0.049485
                  credit: 280.35
                  mirrors:
                    - mirrorID: 1841334
                      CID: 7765437
                      parentCID: 14370798
                      stopLossPercentage: 5
                      isPaused: false
                      copyExistingPositions: true
                      availableAmount: 560
                      stopLossAmount: 28
                      initialInvestment: 560
                      depositSummary: 0
                      withdrawalSummary: 0
                      positions: []
                      entryOrders: []
                      exitOrders: []
                      parentUsername: Deposit158990700
                      closedPositionsNetProfit: 0
                      startedCopyDate: '2024-05-23T13:31:57.007Z'
                      pendingForClosure: false
                      parentMirrors: []
                      mirrorCalculationType: 1
                      ordersForOpen: []
                      ordersForClose: []
                      ordersForCloseMultiple: []
                      delayedOrderForClose: []
                      delayedOrderForOpen: []
                      mirrorStatusId: 0
                  orders:
                    - orderID: 5669649
                      CID: 7765437
                      openDateTime: '2024-06-06T08:07:25.083Z'
                      instrumentID: 100043
                      isBuy: true
                      takeProfitRate: 0
                      stopLossRate: 0.00001
                      rate: 0.1453
                      amount: 100
                      leverage: 1
                      units: 688.231246
                      isTslEnabled: false
                      executionType: 0
                      isDiscounted: false
                  stockOrders: []
                  entryOrders: []
                  exitOrders: []
                  ordersForOpen: []
                  ordersForClose: []
                  ordersForCloseMultiple: []
                  bonusCredit: 0
      security:
        - bearerAuth: []
components:
  schemas:
    PortfolioResponse:
      type: object
      description: >-
        Comprehensive portfolio information including positions, orders, and
        account status
      properties:
        clientPortfolio:
          type: object
          description: Container for all portfolio-related information
          properties:
            positions:
              type: array
              description: List of currently open trading positions
              items:
                $ref: '#/components/schemas/Position'
            credit:
              type: number
              format: float
              description: >-
                Available trading balance in USD, representing funds available
                for new positions
            mirrors:
              type: array
              description: Copy trading configurations and positions
              items:
                type: object
                description: Individual mirror trading setup
                properties:
                  mirrorID:
                    type: integer
                    description: Unique identifier for the mirror trading configuration
                  CID:
                    type: integer
                    description: Customer ID associated with the mirror
                  parentCID:
                    type: integer
                    description: Customer ID of the trader being copied
                  stopLossPercentage:
                    type: number
                    format: float
                    description: >-
                      The precentage of the mirror value that the StopLossAmount
                      represented at the time of the last edit. Adding or
                      removing funds from the mirror will trigger recalculation
                      of StopLossAmount based on this value compared to the
                      current mirror value
                  isPaused:
                    type: boolean
                    description: >-
                      Indication if the mirror is currently paused, restricting
                      open of additional positions inside the mirror
                  copyExistingPositions:
                    type: boolean
                    description: >-
                      Indication if mirror originally copied all parent existing
                      position on mirror registration
                  availableAmount:
                    type: number
                    format: float
                    description: >-
                      Available to trade USD balance in the mirror. This balance
                      is reserved for mirror operations
                  stopLossAmount:
                    type: number
                    format: float
                    description: >-
                      USD value of the mirror at which MirrorStopLoss will be
                      triggered and cause liquidation of the mirror. Adding or
                      removing funds from the mirror will trigger recalculation
                      of this value based on StopLossPercentage compared to the
                      current mirror value
                  initialInvestment:
                    type: number
                    format: float
                    description: USD amount initially invested in the mirror
                  depositSummary:
                    type: number
                    format: float
                    description: >-
                      Total USD amount deposited into the mirror after initial
                      investment
                  withdrawalSummary:
                    type: number
                    format: float
                    description: Total USD amount withdrawn from the mirror
                  positions:
                    type: array
                    description: Positions within this copy trading mirror
                    items:
                      $ref: '#/components/schemas/Position'
                  entryOrders:
                    type: array
                    description: Obsolete
                    items:
                      type: object
                  exitOrders:
                    type: array
                    description: Obsolete
                    items:
                      type: object
                  parentUsername:
                    type: string
                    description: Username of the trader being copied
                  closedPositionsNetProfit:
                    type: number
                    format: float
                    description: >-
                      Total USD net profit of all positions that closed in the
                      mirror
                  startedCopyDate:
                    type: string
                    format: date-time
                    description: Date and time when the mirror trading was initiated
                  pendingForClosure:
                    type: boolean
                    description: Indication if the mirror is in closure process
                  parentMirrors:
                    type: array
                    items:
                      type: object
                  mirrorCalculationType:
                    type: integer
                    description: >-
                      (Obsolete) Mirror positions weights calculation
                      methodology
                  ordersForOpen:
                    type: array
                    description: Active orders in the mirror to open positions
                    items:
                      type: object
                  ordersForClose:
                    type: array
                    description: Active orders in the mirror to close positions
                    items:
                      type: object
                  ordersForCloseMultiple:
                    type: array
                    description: Active orders in the mirror to close positions
                    items:
                      type: object
                  delayedOrderForClose:
                    type: array
                    description: Obsolete
                    items:
                      type: object
                  delayedOrderForOpen:
                    type: array
                    description: Obsolete
                    items:
                      type: object
                  mirrorStatusID:
                    type: integer
                    description: >-
                      Current status of the mirror. 0 - Active, 1 - Paused, 2 -
                      Pending Closure, 3 - In Alignment Process
            orders:
              type: array
              description: List of pending orders
              items:
                type: object
                description: Individual order details
                properties:
                  orderID:
                    type: integer
                    description: Unique identifier for the order
                  CID:
                    type: integer
                    description: Customer ID associated with the order
                  openDateTime:
                    type: string
                    format: date-time
                    description: Date and time when the order was created
                  instrumentID:
                    type: integer
                    description: Identifier of the instrument being traded
                  isBuy:
                    type: boolean
                    description: Direction of the position. true - Long, false - Short
                  takeProfitRate:
                    type: number
                    format: float
                    description: >-
                      The take-profit trigger price at which the position will
                      generate a Market Order to close (after it has opened).
                      TakeProfit trigger price must be better than the current
                      price.
                  stopLossRate:
                    type: number
                    format: float
                    description: >-
                      The stop-loss trigger price at which the position will
                      generate a Market Order to close (after it was opened).
                      StopLoss trigger price must be worse than current price.
                  rate:
                    type: number
                    format: float
                    description: Asset rate at which to send market order to the market
                  amount:
                    type: number
                    format: float
                    description: USD amount to invest in the position
                  leverage:
                    type: number
                    format: float
                    description: Leverage multiplier to apply to the position
                  units:
                    type: number
                    format: float
                    description: >-
                      Units to open the position. If this value is greater than
                      zero the position will open on the requested units, and
                      not amount
                  isTslEnabled:
                    type: boolean
                    description: >-
                      Indicates if a trailing stop loss (TSL) is enabled. This
                      means that the stoploss rate indicated will get updated
                      automatically whenever the asset price increases (for long
                      positions) or decreases (for short position) effectively
                      keeping the stoploss in a constant gap from the best price
                      achieved so far.
                  executionType:
                    type: integer
                    description: Type of order execution
                  isDiscounted:
                    type: boolean
                    description: Obsolete
            stockOrders:
              type: array
              description: Obsolete
              items:
                type: object
            entryOrders:
              type: array
              description: Obsolete
              items:
                type: object
            exitOrders:
              type: array
              description: Obsolete
              items:
                type: object
            ordersForOpen:
              type: array
              description: Active orders to open positions
              items:
                type: object
            ordersForClose:
              type: array
              description: Active orders to close positions
              items:
                type: object
            ordersForCloseMultiple:
              type: array
              description: Active orders to close multiple positions
              items:
                type: object
            bonusCredit:
              type: number
              format: float
              description: Bonus credit amount in USD available for trading
    Position:
      type: object
      description: Individual position details
      properties:
        positionID:
          type: integer
          description: Unique identifier for the position
        CID:
          type: integer
          description: Customer ID associated with the position
        openDateTime:
          type: string
          format: date-time
          description: Timestamp when the position was opened in ISO 8601 format
        openRate:
          type: number
          format: float
          description: Entry price of the position in the instrument's currency
        instrumentID:
          type: integer
          description: Identifier of the traded instrument
        mirrorID:
          type: integer
          description: Mirror ID if the position is part of copy trading, 0 otherwise
        parentPositionID:
          type: integer
          description: Parent position ID for mirrored positions, 0 otherwise
        isBuy:
          type: boolean
          description: >-
            Position direction: true for long (buy) positions, false for short
            (sell) positions
        leverage:
          type: number
          format: float
          description: Leverage multiplier applied to the position
        takeProfitRate:
          type: number
          format: float
          description: >-
            The take-profit trigger price at which the position will generate a
            Market Order to close (after it has opened). TakeProfit trigger
            price must be better than the current price.
        stopLossRate:
          type: number
          format: float
          description: >-
            The stop-loss trigger price at which the position will generate a
            Market Order to close (after it was opened). StopLoss trigger price
            must be worse than current price.
        amount:
          type: number
          format: float
          description: >-
            USD amount allocated to the position. This amount includes both the
            initial investment, and additional margin allocated to the position
            as collateral
        orderID:
          type: integer
          description: >-
            Original orderID the position was opened by. Need to match together
            with orderType
        orderType:
          type: integer
          description: >-
            Original orderType of the order the position was opened by. Need to
            match together with orderID
        units:
          type: number
          format: float
          description: Number of units in the position
        totalFees:
          type: number
          format: float
          description: >-
            Total overnight fees and dividends charged/paid on the position in
            USD. Negative amount represents refund
        initialAmountInDollars:
          type: number
          format: float
          description: Initial investment USD amount in the position
        isTslEnabled:
          type: boolean
          description: Indication if TrailingStopLoss feature is active on this position
        stopLossVersion:
          type: integer
          description: >-
            Manual stop loss edit version. Each time StopLossRate is manually
            update this value is incremented
        isSettled:
          type: boolean
          description: Obsolete
        redeemStatusID:
          type: integer
          description: >-
            If the position is currently in redeem process, this value
            represents the current status
        initialUnits:
          type: number
          format: float
          description: Initial invested units in the position
        isPartiallyAltered:
          type: boolean
          description: Indication whether this position was partially closed
        unitsBaseValueDollars:
          type: number
          format: float
          description: Current units invested value in USD
        isDiscounted:
          type: boolean
          description: Obsolete
        openPositionActionType:
          type: integer
          description: Position open reason
        settlementTypeID:
          type: integer
          description: >-
            Position investment type. 0 - CFD, 1 - Real Asset, 2 - SWAP, 3 -
            Crypto MarginTrade, 4 - Future Contract
        isDetached:
          type: boolean
          description: >-
            Indication if the position was originally opened inside a mirror and
            detached from it
        openConversionRate:
          type: number
          format: float
          description: Conversion rate at position opening
        pnlVersion:
          type: integer
          description: Pnl formula used for calculating profit and loss
        totalExternalFees:
          type: number
          format: float
          description: >-
            Total fees in USD charged on the position. Example - TicketFee. This
            value does not include overnight fees and dividends
        totalExternalTaxes:
          type: number
          format: float
          description: Total taxes in USD charged on the position. Example - SDRT
        isNoTakeProfit:
          type: boolean
          description: >-
            Indication if TakeProfit is enabled for the position. false =
            enabled, true = disabled
        isNoStopLoss:
          type: boolean
          description: >-
            Indication if StopLoss is enabled for the position. false = enabled,
            true = disabled
        lotCount:
          type: number
          format: float
          description: >-
            Number of lots the position represents. For FutureContracts this
            value represents the number of contracts acquired

````

Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Retrieve comprehensive portfolio information including positions, orders, and account status

> Returns detailed portfolio information including active positions, pending orders, mirror trading details, and account balances. This endpoint provides a complete overview of the user's trading activity and current market exposure.



## OpenAPI

````yaml api-reference/openapi.json get /api/v1/trading/info/portfolio
openapi: 3.0.1
info:
  title: eToro Api
  version: v1.156.0
  description: >-
    eToro’s public API provides access to real-time financial data, trading
    insights, and account management features, allowing developers to integrate
    eToro’s services into their applications. With access to market prices,
    historical data, and social trading information, the API empowers users to
    enhance their trading strategies. Designed for security and scalability, the
    eToro API ensures smooth and reliable integration for a variety of financial
    applications.


    For more details on integrating with eToro's public WebSocket service,
    please refer to the dedicated [WebSocket
    documentation](./websocket/websocket-doc.html).
servers:
  - url: https://public-api.etoro.com
    description: eToro Public API
security: []
paths:
  /api/v1/trading/info/portfolio:
    get:
      tags:
        - Trading - Real
      summary: >-
        Retrieve comprehensive portfolio information including positions,
        orders, and account status
      description: >-
        Returns detailed portfolio information including active positions,
        pending orders, mirror trading details, and account balances. This
        endpoint provides a complete overview of the user's trading activity and
        current market exposure.
      operationId: getPortfolio
      parameters:
        - name: x-request-id
          in: header
          required: true
          schema:
            type: string
            format: uuid
            example: 269a98a4-9a55-4d81-a31c-7ef724dcdcd4
          description: A unique request identifier.
        - name: x-api-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: lhgfaslk21490FAScVPkdsb53F9dNkfHG4faZSG5vfjndfcfgdssdgsdHF4663
          description: API key for authentication.
        - name: x-user-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: >-
              eyJlYW4iOiJVbnJlZ2lzdGVyZWRBcHBsaWNhdGlvbiIsImVrIjoiOE5sZ2cwcW5EUVdROUFNWGpXT2lmOWktZnpidG5KcUlqWGJ3WHJZZkpZcldrbG90ZEhvLVBjSWhQaU8xU1ZtMW84aU1WZGZqN2xWNzFjLXFxLmcybXE1dnh4Q1hUT25xaWRUaTFlcEhmVk1fIn0_
          description: User-specific authentication key.
      responses:
        '200':
          description: Successfully retrieved portfolio information
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/PortfolioResponse'
              example:
                clientPortfolio:
                  positions:
                    - positionID: 2150896073
                      CID: 7765437
                      openDateTime: '2024-08-01T07:44:26.103Z'
                      openRate: 2020.7784
                      instrumentID: 1002
                      isBuy: true
                      takeProfitRate: 0
                      stopLossRate: 0.0001
                      mirrorID: 0
                      parentPositionID: 0
                      amount: 100
                      leverage: 1
                      orderID: 12402059
                      orderType: 17
                      units: 0.049485
                      totalFees: 0
                      initialAmountInDollars: 100
                      isTslEnabled: false
                      stopLossVersion: 3
                      isSettled: true
                      redeemStatusID: 0
                      initialUnits: 0.049485
                      isPartiallyAltered: false
                      unitsBaseValueDollars: 100
                      isDiscounted: true
                      openPositionActionType: 0
                      settlementTypeID: 1
                      isDetached: false
                      openConversionRate: 1
                      pnlVersion: 1
                      totalExternalFees: 0
                      totalExternalTaxes: 0
                      isNoTakeProfit: true
                      isNoStopLoss: true
                      lotCount: 0.049485
                  credit: 280.35
                  mirrors:
                    - mirrorID: 1841334
                      CID: 7765437
                      parentCID: 14370798
                      stopLossPercentage: 5
                      isPaused: false
                      copyExistingPositions: true
                      availableAmount: 560
                      stopLossAmount: 28
                      initialInvestment: 560
                      depositSummary: 0
                      withdrawalSummary: 0
                      positions: []
                      entryOrders: []
                      exitOrders: []
                      parentUsername: Deposit158990700
                      closedPositionsNetProfit: 0
                      startedCopyDate: '2024-05-23T13:31:57.007Z'
                      pendingForClosure: false
                      parentMirrors: []
                      mirrorCalculationType: 1
                      ordersForOpen: []
                      ordersForClose: []
                      ordersForCloseMultiple: []
                      delayedOrderForClose: []
                      delayedOrderForOpen: []
                      mirrorStatusId: 0
                  orders:
                    - orderID: 5669649
                      CID: 7765437
                      openDateTime: '2024-06-06T08:07:25.083Z'
                      instrumentID: 100043
                      isBuy: true
                      takeProfitRate: 0
                      stopLossRate: 0.00001
                      rate: 0.1453
                      amount: 100
                      leverage: 1
                      units: 688.231246
                      isTslEnabled: false
                      executionType: 0
                      isDiscounted: false
                  stockOrders: []
                  entryOrders: []
                  exitOrders: []
                  ordersForOpen: []
                  ordersForClose: []
                  ordersForCloseMultiple: []
                  bonusCredit: 0
      security:
        - bearerAuth: []
components:
  schemas:
    PortfolioResponse:
      type: object
      description: >-
        Comprehensive portfolio information including positions, orders, and
        account status
      properties:
        clientPortfolio:
          type: object
          description: Container for all portfolio-related information
          properties:
            positions:
              type: array
              description: List of currently open trading positions
              items:
                $ref: '#/components/schemas/Position'
            credit:
              type: number
              format: float
              description: >-
                Available trading balance in USD, representing funds available
                for new positions
            mirrors:
              type: array
              description: Copy trading configurations and positions
              items:
                type: object
                description: Individual mirror trading setup
                properties:
                  mirrorID:
                    type: integer
                    description: Unique identifier for the mirror trading configuration
                  CID:
                    type: integer
                    description: Customer ID associated with the mirror
                  parentCID:
                    type: integer
                    description: Customer ID of the trader being copied
                  stopLossPercentage:
                    type: number
                    format: float
                    description: >-
                      The precentage of the mirror value that the StopLossAmount
                      represented at the time of the last edit. Adding or
                      removing funds from the mirror will trigger recalculation
                      of StopLossAmount based on this value compared to the
                      current mirror value
                  isPaused:
                    type: boolean
                    description: >-
                      Indication if the mirror is currently paused, restricting
                      open of additional positions inside the mirror
                  copyExistingPositions:
                    type: boolean
                    description: >-
                      Indication if mirror originally copied all parent existing
                      position on mirror registration
                  availableAmount:
                    type: number
                    format: float
                    description: >-
                      Available to trade USD balance in the mirror. This balance
                      is reserved for mirror operations
                  stopLossAmount:
                    type: number
                    format: float
                    description: >-
                      USD value of the mirror at which MirrorStopLoss will be
                      triggered and cause liquidation of the mirror. Adding or
                      removing funds from the mirror will trigger recalculation
                      of this value based on StopLossPercentage compared to the
                      current mirror value
                  initialInvestment:
                    type: number
                    format: float
                    description: USD amount initially invested in the mirror
                  depositSummary:
                    type: number
                    format: float
                    description: >-
                      Total USD amount deposited into the mirror after initial
                      investment
                  withdrawalSummary:
                    type: number
                    format: float
                    description: Total USD amount withdrawn from the mirror
                  positions:
                    type: array
                    description: Positions within this copy trading mirror
                    items:
                      $ref: '#/components/schemas/Position'
                  entryOrders:
                    type: array
                    description: Obsolete
                    items:
                      type: object
                  exitOrders:
                    type: array
                    description: Obsolete
                    items:
                      type: object
                  parentUsername:
                    type: string
                    description: Username of the trader being copied
                  closedPositionsNetProfit:
                    type: number
                    format: float
                    description: >-
                      Total USD net profit of all positions that closed in the
                      mirror
                  startedCopyDate:
                    type: string
                    format: date-time
                    description: Date and time when the mirror trading was initiated
                  pendingForClosure:
                    type: boolean
                    description: Indication if the mirror is in closure process
                  parentMirrors:
                    type: array
                    items:
                      type: object
                  mirrorCalculationType:
                    type: integer
                    description: >-
                      (Obsolete) Mirror positions weights calculation
                      methodology
                  ordersForOpen:
                    type: array
                    description: Active orders in the mirror to open positions
                    items:
                      type: object
                  ordersForClose:
                    type: array
                    description: Active orders in the mirror to close positions
                    items:
                      type: object
                  ordersForCloseMultiple:
                    type: array
                    description: Active orders in the mirror to close positions
                    items:
                      type: object
                  delayedOrderForClose:
                    type: array
                    description: Obsolete
                    items:
                      type: object
                  delayedOrderForOpen:
                    type: array
                    description: Obsolete
                    items:
                      type: object
                  mirrorStatusID:
                    type: integer
                    description: >-
                      Current status of the mirror. 0 - Active, 1 - Paused, 2 -
                      Pending Closure, 3 - In Alignment Process
            orders:
              type: array
              description: List of pending orders
              items:
                type: object
                description: Individual order details
                properties:
                  orderID:
                    type: integer
                    description: Unique identifier for the order
                  CID:
                    type: integer
                    description: Customer ID associated with the order
                  openDateTime:
                    type: string
                    format: date-time
                    description: Date and time when the order was created
                  instrumentID:
                    type: integer
                    description: Identifier of the instrument being traded
                  isBuy:
                    type: boolean
                    description: Direction of the position. true - Long, false - Short
                  takeProfitRate:
                    type: number
                    format: float
                    description: >-
                      The take-profit trigger price at which the position will
                      generate a Market Order to close (after it has opened).
                      TakeProfit trigger price must be better than the current
                      price.
                  stopLossRate:
                    type: number
                    format: float
                    description: >-
                      The stop-loss trigger price at which the position will
                      generate a Market Order to close (after it was opened).
                      StopLoss trigger price must be worse than current price.
                  rate:
                    type: number
                    format: float
                    description: Asset rate at which to send market order to the market
                  amount:
                    type: number
                    format: float
                    description: USD amount to invest in the position
                  leverage:
                    type: number
                    format: float
                    description: Leverage multiplier to apply to the position
                  units:
                    type: number
                    format: float
                    description: >-
                      Units to open the position. If this value is greater than
                      zero the position will open on the requested units, and
                      not amount
                  isTslEnabled:
                    type: boolean
                    description: >-
                      Indicates if a trailing stop loss (TSL) is enabled. This
                      means that the stoploss rate indicated will get updated
                      automatically whenever the asset price increases (for long
                      positions) or decreases (for short position) effectively
                      keeping the stoploss in a constant gap from the best price
                      achieved so far.
                  executionType:
                    type: integer
                    description: Type of order execution
                  isDiscounted:
                    type: boolean
                    description: Obsolete
            stockOrders:
              type: array
              description: Obsolete
              items:
                type: object
            entryOrders:
              type: array
              description: Obsolete
              items:
                type: object
            exitOrders:
              type: array
              description: Obsolete
              items:
                type: object
            ordersForOpen:
              type: array
              description: Active orders to open positions
              items:
                type: object
            ordersForClose:
              type: array
              description: Active orders to close positions
              items:
                type: object
            ordersForCloseMultiple:
              type: array
              description: Active orders to close multiple positions
              items:
                type: object
            bonusCredit:
              type: number
              format: float
              description: Bonus credit amount in USD available for trading
    Position:
      type: object
      description: Individual position details
      properties:
        positionID:
          type: integer
          description: Unique identifier for the position
        CID:
          type: integer
          description: Customer ID associated with the position
        openDateTime:
          type: string
          format: date-time
          description: Timestamp when the position was opened in ISO 8601 format
        openRate:
          type: number
          format: float
          description: Entry price of the position in the instrument's currency
        instrumentID:
          type: integer
          description: Identifier of the traded instrument
        mirrorID:
          type: integer
          description: Mirror ID if the position is part of copy trading, 0 otherwise
        parentPositionID:
          type: integer
          description: Parent position ID for mirrored positions, 0 otherwise
        isBuy:
          type: boolean
          description: >-
            Position direction: true for long (buy) positions, false for short
            (sell) positions
        leverage:
          type: number
          format: float
          description: Leverage multiplier applied to the position
        takeProfitRate:
          type: number
          format: float
          description: >-
            The take-profit trigger price at which the position will generate a
            Market Order to close (after it has opened). TakeProfit trigger
            price must be better than the current price.
        stopLossRate:
          type: number
          format: float
          description: >-
            The stop-loss trigger price at which the position will generate a
            Market Order to close (after it was opened). StopLoss trigger price
            must be worse than current price.
        amount:
          type: number
          format: float
          description: >-
            USD amount allocated to the position. This amount includes both the
            initial investment, and additional margin allocated to the position
            as collateral
        orderID:
          type: integer
          description: >-
            Original orderID the position was opened by. Need to match together
            with orderType
        orderType:
          type: integer
          description: >-
            Original orderType of the order the position was opened by. Need to
            match together with orderID
        units:
          type: number
          format: float
          description: Number of units in the position
        totalFees:
          type: number
          format: float
          description: >-
            Total overnight fees and dividends charged/paid on the position in
            USD. Negative amount represents refund
        initialAmountInDollars:
          type: number
          format: float
          description: Initial investment USD amount in the position
        isTslEnabled:
          type: boolean
          description: Indication if TrailingStopLoss feature is active on this position
        stopLossVersion:
          type: integer
          description: >-
            Manual stop loss edit version. Each time StopLossRate is manually
            update this value is incremented
        isSettled:
          type: boolean
          description: Obsolete
        redeemStatusID:
          type: integer
          description: >-
            If the position is currently in redeem process, this value
            represents the current status
        initialUnits:
          type: number
          format: float
          description: Initial invested units in the position
        isPartiallyAltered:
          type: boolean
          description: Indication whether this position was partially closed
        unitsBaseValueDollars:
          type: number
          format: float
          description: Current units invested value in USD
        isDiscounted:
          type: boolean
          description: Obsolete
        openPositionActionType:
          type: integer
          description: Position open reason
        settlementTypeID:
          type: integer
          description: >-
            Position investment type. 0 - CFD, 1 - Real Asset, 2 - SWAP, 3 -
            Crypto MarginTrade, 4 - Future Contract
        isDetached:
          type: boolean
          description: >-
            Indication if the position was originally opened inside a mirror and
            detached from it
        openConversionRate:
          type: number
          format: float
          description: Conversion rate at position opening
        pnlVersion:
          type: integer
          description: Pnl formula used for calculating profit and loss
        totalExternalFees:
          type: number
          format: float
          description: >-
            Total fees in USD charged on the position. Example - TicketFee. This
            value does not include overnight fees and dividends
        totalExternalTaxes:
          type: number
          format: float
          description: Total taxes in USD charged on the position. Example - SDRT
        isNoTakeProfit:
          type: boolean
          description: >-
            Indication if TakeProfit is enabled for the position. false =
            enabled, true = disabled
        isNoStopLoss:
          type: boolean
          description: >-
            Indication if StopLoss is enabled for the position. false = enabled,
            true = disabled
        lotCount:
          type: number
          format: float
          description: >-
            Number of lots the position represents. For FutureContracts this
            value represents the number of contracts acquired

````

Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Get Real Account PnL and Portfolio Details

> Retrieves the real account's current portfolio, including credit, open positions, orders, mirrors, and PnL details.



## OpenAPI

````yaml api-reference/openapi.json get /api/v1/trading/info/real/pnl
openapi: 3.0.1
info:
  title: eToro Api
  version: v1.156.0
  description: >-
    eToro’s public API provides access to real-time financial data, trading
    insights, and account management features, allowing developers to integrate
    eToro’s services into their applications. With access to market prices,
    historical data, and social trading information, the API empowers users to
    enhance their trading strategies. Designed for security and scalability, the
    eToro API ensures smooth and reliable integration for a variety of financial
    applications.


    For more details on integrating with eToro's public WebSocket service,
    please refer to the dedicated [WebSocket
    documentation](./websocket/websocket-doc.html).
servers:
  - url: https://public-api.etoro.com
    description: eToro Public API
security: []
paths:
  /api/v1/trading/info/real/pnl:
    get:
      tags:
        - Trading - Real
      summary: Get Real Account PnL and Portfolio Details
      description: >-
        Retrieves the real account's current portfolio, including credit, open
        positions, orders, mirrors, and PnL details.
      operationId: getRealAccountPnl
      parameters:
        - name: x-request-id
          in: header
          required: true
          schema:
            type: string
            format: uuid
            example: 9ab4b46b-e254-4d06-8cad-2772f8decef6
          description: A unique request identifier.
        - name: x-api-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: lhgfaslk21490FAScVPkdsb53F9dNkfHG4faZSG5vfjndfcfgdssdgsdHF4663
          description: API key for authentication.
        - name: x-user-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: >-
              eyJlYW4iOiJVbnJlZ2lzdGVyZWRBcHBsaWNhdGlvbiIsImVrIjoiOE5sZ2cwcW5EUVdROUFNWGpXT2lmOWktZnpidG5KcUlqWGJ3WHJZZkpZcldrbG90ZEhvLVBjSWhQaU8xU1ZtMW84aU1WZGZqN2xWNzFjLXFxLmcybXE1dnh4Q1hUT25xaWRUaTFlcEhmVk1fIn0_
          description: User-specific authentication key.
      responses:
        '200':
          description: Successfully retrieved real account PnL and portfolio information.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/PortfolioResponseWithPnl'
              example:
                clientPortfolio:
                  credit: 10000.5
                  unrealizedPnL: 251
                  mirrors:
                    - mirrorId: 1
                      cid: 123
                      parentCid: 456
                      stopLossPercentage: 15.5
                      isPaused: false
                      copyExistingPositions: true
                      availableAmount: 5000
                      stopLossAmount: 750
                      initialInvestment: 10000
                      depositSummary: 12000
                      withdrawalSummary: 2000
                      positions:
                        - positionId: 9002
                          cid: 124
                          openDateTime: '2024-01-02T09:00:00Z'
                          openRate: 1.2346
                          instrumentId: 102
                          isBuy: false
                          takeProfitRate: 1.6
                          stopLossRate: 1.1
                          mirrorId: 1
                          parentPositionId: 8002
                          amount: 2000
                          leverage: 3
                          orderId: 5002
                          orderType: 2
                          units: 20.5
                          totalFees: 3.5
                          initialAmountInDollars: 2000
                          isTslEnabled: true
                          stopLossVersion: 2
                          isSettled: false
                          redeemStatusId: 1
                          initialUnits: 20.5
                          isPartiallyAltered: true
                          unitsBaseValueDollars: 2000
                          isDiscounted: true
                          openPositionActionType: 2
                          settlementTypeId: 2
                          isDetached: true
                          openConversionRate: 1.2
                          pnlVersion: 2
                          totalExternalFees: 1
                          totalExternalTaxes: 0.5
                          isNoTakeProfit: true
                          isNoStopLoss: false
                          lotCount: 2
                          externalOperation: null
                          pnL: 150.75
                          closeRate: 1.3
                          closeConversionRate: 1.15
                          timestamp: '2024-01-02T12:00:00Z'
                      parentUsername: parent_user
                      closedPositionsNetProfit: 350.75
                      startedCopyDate: '2024-01-01T09:00:00Z'
                      pendingForClosure: false
                      parentMirrors: []
                      mirrorCalculationType: 2
                      ordersForOpen:
                        - orderId: 1001
                          orderType: 1
                          statusId: 1
                          cid: 123
                          openDateTime: '2024-01-01T09:00:00Z'
                          lastUpdate: '2024-01-02T10:00:00Z'
                          instrumentId: 101
                          amount: 1000
                          amountInUnits: 10.5
                          isBuy: true
                          leverage: 2
                          stopLossRate: 1.2345
                          takeProfitRate: 1.3456
                          isTslEnabled: false
                          isDiscounted: true
                          mirrorId: 1
                          frozenAmount: 0
                          totalExternalCosts: 5
                          isNoTakeProfit: false
                          isNoStopLoss: false
                          lotCount: 1
                          openPositionActionType: 1
                          externalOperation: null
                      ordersForClose:
                        - orderId: 2001
                          orderType: 2
                          statusId: 1
                          cid: 123
                          openDateTime: '2024-01-01T09:00:00Z'
                          lastUpdate: '2024-01-02T10:00:00Z'
                          instrumentId: 101
                          unitsToDeduct: 5
                          lotsToDeduct: 0.5
                          positionId: 3001
                      ordersForCloseMultiple:
                        - orderId: 3001
                          orderType: 3
                          statusId: 1
                          cid: 123
                          openDateTime: '2024-01-01T09:00:00Z'
                          lastUpdate: '2024-01-02T10:00:00Z'
                          instrumentId: 101
                          unitsToDeduct: 10
                          lotsToDeduct: 1
                          pendingClosePositionIds:
                            - 3001
                            - 3002
                      mirrorStatusId: 1
                  orders:
                    - orderId: 5001
                      cid: 123
                      openDateTime: '2024-01-01T09:00:00Z'
                      instrumentId: 101
                      isBuy: true
                      takeProfitRate: 1.5
                      stopLossRate: 1.2
                      rate: 1.3
                      amount: 1000
                      leverage: 2
                      units: 10.5
                      isTslEnabled: false
                      executionType: 1
                      isDiscounted: false
                      isNoTakeProfit: false
                      isNoStopLoss: false
                  ordersForOpen:
                    - orderId: 1001
                      orderType: 1
                      statusId: 1
                      cid: 123
                      openDateTime: '2024-01-01T09:00:00Z'
                      lastUpdate: '2024-01-02T10:00:00Z'
                      instrumentId: 101
                      amount: 1000
                      amountInUnits: 10.5
                      isBuy: true
                      leverage: 2
                      stopLossRate: 1.2345
                      takeProfitRate: 1.3456
                      isTslEnabled: false
                      isDiscounted: true
                      mirrorId: 1
                      frozenAmount: 0
                      totalExternalCosts: 5
                      isNoTakeProfit: false
                      isNoStopLoss: false
                      lotCount: 1
                      openPositionActionType: 1
                      externalOperation: null
                  ordersForClose:
                    - orderId: 2001
                      orderType: 2
                      statusId: 1
                      cid: 123
                      openDateTime: '2024-01-01T09:00:00Z'
                      lastUpdate: '2024-01-02T10:00:00Z'
                      instrumentId: 101
                      unitsToDeduct: 5
                      lotsToDeduct: 0.5
                      positionId: 3001
                  ordersForCloseMultiple:
                    - orderId: 3001
                      orderType: 3
                      statusId: 1
                      cid: 123
                      openDateTime: '2024-01-01T09:00:00Z'
                      lastUpdate: '2024-01-02T10:00:00Z'
                      instrumentId: 101
                      unitsToDeduct: 10
                      lotsToDeduct: 1
                      pendingClosePositionIds:
                        - 3001
                        - 3002
                  bonusCredit: 500
                  positions:
                    - positionId: 9001
                      cid: 123
                      openDateTime: '2024-01-01T09:00:00Z'
                      openRate: 1.2345
                      instrumentId: 101
                      isBuy: true
                      takeProfitRate: 1.5
                      stopLossRate: 1.2
                      mirrorId: 1
                      parentPositionId: 8001
                      amount: 1000
                      leverage: 2
                      orderId: 5001
                      orderType: 1
                      units: 10.5
                      totalFees: 2.5
                      initialAmountInDollars: 1000
                      isTslEnabled: false
                      stopLossVersion: 1
                      isSettled: true
                      redeemStatusId: 0
                      initialUnits: 10.5
                      isPartiallyAltered: false
                      unitsBaseValueDollars: 1000
                      isDiscounted: false
                      openPositionActionType: 1
                      settlementTypeId: 1
                      isDetached: false
                      openConversionRate: 1
                      pnlVersion: 1
                      totalExternalFees: 0
                      totalExternalTaxes: 0
                      isNoTakeProfit: false
                      isNoStopLoss: false
                      lotCount: 1
                      externalOperation: null
                      pnL: 100.25
                      closeRate: 1.25
                      closeConversionRate: 1.1
                      timestamp: '2024-01-01T12:00:00Z'
      security:
        - bearerAuth: []
components:
  schemas:
    PortfolioResponseWithPnl:
      type: object
      description: >-
        Comprehensive portfolio information including positions, orders, and
        account status
      properties:
        clientPortfolio:
          $ref: '#/components/schemas/ClientPortfolio'
          description: Container for all portfolio-related information
    ClientPortfolio:
      type: object
      properties:
        positions:
          type: array
          description: List of currently open trading positions
          items:
            $ref: '#/components/schemas/Position'
        credit:
          type: number
          format: float
          description: >-
            Available trading balance in USD, representing funds available for
            new actions
        mirrors:
          type: array
          items:
            $ref: '#/components/schemas/Mirror'
          description: Copy trading configurations and positions
        orders:
          type: array
          items:
            $ref: '#/components/schemas/Order'
          description: List of pending orders
        ordersForOpen:
          type: array
          items:
            $ref: '#/components/schemas/OrderForOpen'
          description: Active orders to open positions
        ordersForClose:
          type: array
          items:
            $ref: '#/components/schemas/OrderForClose'
          description: Active orders to close positions
        ordersForCloseMultiple:
          type: array
          items:
            $ref: '#/components/schemas/OrderForCloseMultiple'
          description: Active orders to close multiple positions
        bonusCredit:
          type: number
          format: float
          description: Bonus credit amount in USD in the account
        unrealizedPnL:
          type: number
          format: float
          description: >-
            Total unrealized profit and loss across all open positions in the
            portfolio
        accountCurrencyId:
          type: integer
          description: Currency ID of the account (1 = USD)
        stockOrders:
          type: array
          items:
            type: object
          description: Stock-specific pending orders
        entryOrders:
          type: array
          items:
            type: object
          description: Entry orders awaiting execution
        exitOrders:
          type: array
          items:
            type: object
          description: Exit orders awaiting execution
    Position:
      type: object
      description: Individual position details
      properties:
        positionID:
          type: integer
          description: Unique identifier for the position
        CID:
          type: integer
          description: Customer ID associated with the position
        openDateTime:
          type: string
          format: date-time
          description: Timestamp when the position was opened in ISO 8601 format
        openRate:
          type: number
          format: float
          description: Entry price of the position in the instrument's currency
        instrumentID:
          type: integer
          description: Identifier of the traded instrument
        mirrorID:
          type: integer
          description: Mirror ID if the position is part of copy trading, 0 otherwise
        parentPositionID:
          type: integer
          description: Parent position ID for mirrored positions, 0 otherwise
        isBuy:
          type: boolean
          description: >-
            Position direction: true for long (buy) positions, false for short
            (sell) positions
        leverage:
          type: number
          format: float
          description: Leverage multiplier applied to the position
        takeProfitRate:
          type: number
          format: float
          description: >-
            The take-profit trigger price at which the position will generate a
            Market Order to close (after it has opened). TakeProfit trigger
            price must be better than the current price.
        stopLossRate:
          type: number
          format: float
          description: >-
            The stop-loss trigger price at which the position will generate a
            Market Order to close (after it was opened). StopLoss trigger price
            must be worse than current price.
        amount:
          type: number
          format: float
          description: >-
            USD amount allocated to the position. This amount includes both the
            initial investment, and additional margin allocated to the position
            as collateral
        orderID:
          type: integer
          description: >-
            Original orderID the position was opened by. Need to match together
            with orderType
        orderType:
          type: integer
          description: >-
            Original orderType of the order the position was opened by. Need to
            match together with orderID
        units:
          type: number
          format: float
          description: Number of units in the position
        totalFees:
          type: number
          format: float
          description: >-
            Total overnight fees and dividends charged/paid on the position in
            USD. Negative amount represents refund
        initialAmountInDollars:
          type: number
          format: float
          description: Initial investment USD amount in the position
        isTslEnabled:
          type: boolean
          description: Indication if TrailingStopLoss feature is active on this position
        stopLossVersion:
          type: integer
          description: >-
            Manual stop loss edit version. Each time StopLossRate is manually
            update this value is incremented
        isSettled:
          type: boolean
          description: Obsolete
        redeemStatusID:
          type: integer
          description: >-
            If the position is currently in redeem process, this value
            represents the current status
        initialUnits:
          type: number
          format: float
          description: Initial invested units in the position
        isPartiallyAltered:
          type: boolean
          description: Indication whether this position was partially closed
        unitsBaseValueDollars:
          type: number
          format: float
          description: Current units invested value in USD
        isDiscounted:
          type: boolean
          description: Obsolete
        openPositionActionType:
          type: integer
          description: Position open reason
        settlementTypeID:
          type: integer
          description: >-
            Position investment type. 0 - CFD, 1 - Real Asset, 2 - SWAP, 3 -
            Crypto MarginTrade, 4 - Future Contract
        isDetached:
          type: boolean
          description: >-
            Indication if the position was originally opened inside a mirror and
            detached from it
        openConversionRate:
          type: number
          format: float
          description: Conversion rate at position opening
        pnlVersion:
          type: integer
          description: Pnl formula used for calculating profit and loss
        totalExternalFees:
          type: number
          format: float
          description: >-
            Total fees in USD charged on the position. Example - TicketFee. This
            value does not include overnight fees and dividends
        totalExternalTaxes:
          type: number
          format: float
          description: Total taxes in USD charged on the position. Example - SDRT
        isNoTakeProfit:
          type: boolean
          description: >-
            Indication if TakeProfit is enabled for the position. false =
            enabled, true = disabled
        isNoStopLoss:
          type: boolean
          description: >-
            Indication if StopLoss is enabled for the position. false = enabled,
            true = disabled
        lotCount:
          type: number
          format: float
          description: >-
            Number of lots the position represents. For FutureContracts this
            value represents the number of contracts acquired
    Mirror:
      type: object
      properties:
        mirrorID:
          type: integer
          description: Unique identifier for the mirror
        CID:
          type: integer
          description: Customer ID associated with the mirror
        parentCID:
          type: integer
          description: Customer ID of the trader being copied
        stopLossPercentage:
          type: number
          format: float
          description: >-
            The precentage of the mirror value that the StopLossAmount
            represented at the time of the last edit. Adding or removing funds
            from the mirror will trigger recalculation of StopLossAmount based
            on this value compared to the current mirror value
        isPaused:
          type: boolean
          description: >-
            Indication if the mirror is currently paused, restricting open of
            additional positions inside the mirror
        copyExistingPositions:
          type: boolean
          description: >-
            Indication if mirror originally copied all parent existing position
            on mirror registration
        availableAmount:
          type: number
          format: float
          description: >-
            Available to trade USD balance in the mirror. This balance is
            reserved for mirror operations
        stopLossAmount:
          type: number
          format: float
          description: >-
            USD value of the mirror at which MirrorStopLoss will be triggered
            and cause liquidation of the mirror. Adding or removing funds from
            the mirror will trigger recalculation of this value based on
            StopLossPercentage compared to the current mirror value
        initialInvestment:
          type: number
          format: float
          description: USD amount initially invested in the mirror
        depositSummary:
          type: number
          format: float
          description: Total USD amount deposited into the mirror after initial investment
        withdrawalSummary:
          type: number
          format: float
          description: Total USD amount withdrawn from the mirror
        positions:
          type: array
          items:
            $ref: '#/components/schemas/Position'
          description: List of currently open trading positions in the mirror
        parentUsername:
          type: string
          description: Username of the trader being copied
        closedPositionsNetProfit:
          type: number
          format: float
          description: Total USD net profit of all positions that closed in the mirror
        startedCopyDate:
          type: string
          format: date-time
          description: Date and time when the mirror trading was initiated
        pendingForClosure:
          type: boolean
          description: Indication if the mirror is in closure process
        parentMirrors:
          type: array
          items:
            type: object
          description: Parent mirrors for this mirror (if any)
        mirrorCalculationType:
          type: integer
          description: (Obsolete) Mirror positions weights calculation methodology
        ordersForOpen:
          type: array
          items:
            $ref: '#/components/schemas/OrderForOpen'
          description: Active orders in the mirror to open positions
        ordersForClose:
          type: array
          items:
            $ref: '#/components/schemas/OrderForClose'
          description: Active orders in the mirror to close positions
        ordersForCloseMultiple:
          type: array
          items:
            $ref: '#/components/schemas/OrderForCloseMultiple'
          description: Active orders in the mirror to close positions
        mirrorStatusID:
          type: integer
          description: >-
            Current status of the mirror. 0 - Active, 1 - Paused, 2 - Pending
            Closure, 3 - In Alignment Process
        delayedOrderForClose:
          type: array
          items:
            type: object
          description: Delayed orders for closing positions
        delayedOrderForOpen:
          type: array
          items:
            type: object
          description: Delayed orders for opening positions
        entryOrders:
          type: array
          items:
            type: object
          description: Entry orders awaiting execution in the mirror
        exitOrders:
          type: array
          items:
            type: object
          description: Exit orders awaiting execution in the mirror
    Order:
      type: object
      properties:
        orderId:
          type: integer
          description: Unique identifier for the order
        cid:
          type: integer
          description: Customer ID associated with the order
        openDateTime:
          type: string
          format: date-time
          description: Date and time when the order was created
        instrumentId:
          type: integer
          description: Identifier of the instrument being traded
        isBuy:
          type: boolean
          description: Direction of the position. true - Long, false - Short
        takeProfitRate:
          type: number
          format: float
          description: >-
            Rate at which TakeProfit will trigger and send MarketOrder to close
            the position once it is open
        stopLossRate:
          type: number
          format: float
          description: >-
            Rate at which StopLoss will trigger and send MarketOrder to close
            the position once it is open
        rate:
          type: number
          format: float
          description: Asset rate at which to send market order to the market
        amount:
          type: number
          format: float
          description: USD amount to invest in the position
        leverage:
          type: integer
          description: Leverage multiplier to apply to the position
        units:
          type: number
          format: float
          description: >-
            Units to open the position. If this value is greater than zero the
            position will open on the requested units, and not amount
        isTslEnabled:
          type: boolean
          description: Indication if to enable TSL feature on the position once it is open
        executionType:
          type: integer
          description: Type of order execution
        isDiscounted:
          type: boolean
          description: Obsolete
        isNoTakeProfit:
          type: boolean
          description: >-
            Indication if TakeProfit is enabled for the order. false = enabled,
            true = disabled
        isNoStopLoss:
          type: boolean
          description: >-
            Indication if StopLoss is enabled for the order. false = enabled,
            true = disabled
    OrderForOpen:
      type: object
      properties:
        orderId:
          type: integer
          description: Unique identifier for the order
        orderType:
          type: integer
          description: Type of order executed
        statusId:
          type: integer
          description: Status of the order
        cid:
          type: integer
          description: Customer ID associated with the order
        openDateTime:
          type: string
          format: date-time
          description: The timestamp when the order was opened.
        lastUpdate:
          type: string
          format: date-time
          description: The last update timestamp of the order.
        instrumentId:
          type: integer
          description: The unique identifier of the financial instrument to trade.
        amount:
          type: number
          format: float
          description: The amount of money to invest in the trade.
        amountInUnits:
          type: number
          format: float
          description: The number of units to trade.
        isBuy:
          type: boolean
          description: True for a buy (long) order, false for a sell (short) order.
        leverage:
          type: integer
          description: The leverage multiplier for the trade.
        stopLossRate:
          type: number
          format: float
          description: >-
            The stop-loss rate at which the trade will automatically close to
            limit losses.
        takeProfitRate:
          type: number
          format: float
          description: >-
            The take-profit rate at which the trade will automatically close to
            secure profits.
        isTslEnabled:
          type: boolean
          description: Indicates whether a trailing stop-loss is enabled.
        isDiscounted:
          type: boolean
          description: Indicates if the order is eligible for a discount.
        mirrorId:
          type: integer
          description: ID related to mirrored trades, if applicable.
        frozenAmount:
          type: number
          format: float
          description: Amount frozen for the order.
        totalExternalCosts:
          type: number
          format: float
          description: Total external costs associated with the trade.
        isNoTakeProfit:
          type: boolean
          description: True if no take-profit is set for this order.
        isNoStopLoss:
          type: boolean
          description: True if no stop-loss is set for this order.
        lotCount:
          type: number
          format: float
          description: The number of lots in the order.
        openPositionActionType:
          type: integer
          description: Position open reason.
        externalOperation:
          type: object
          description: External operation details, if any.
          nullable: true
    OrderForClose:
      type: object
      properties:
        orderId:
          type: integer
          description: Unique identifier for the closing order.
        orderType:
          type: integer
          description: Type of order executed.
        statusId:
          type: integer
          description: Status of the closing order.
        cid:
          type: integer
          description: Customer ID associated with the order.
        openDateTime:
          type: string
          format: date-time
          description: The timestamp when the order was placed.
        lastUpdate:
          type: string
          format: date-time
          description: The timestamp of the last update to this order.
        instrumentId:
          type: integer
          description: The ID of the instrument traded.
        unitsToDeduct:
          type: number
          format: float
          description: The number of units closed in this order.
        lotsToDeduct:
          type: number
          format: float
          description: The number of lots closed in this order.
        positionId:
          type: integer
          description: The ID of the closed position.
    OrderForCloseMultiple:
      type: object
      properties:
        orderId:
          type: integer
          description: Unique identifier for the closing order.
        orderType:
          type: integer
          description: Type of order executed.
        statusId:
          type: integer
          description: Status of the closing order.
        cid:
          type: integer
          description: Customer ID associated with the order.
        openDateTime:
          type: string
          format: date-time
          description: The timestamp when the order was placed.
        lastUpdate:
          type: string
          format: date-time
          description: The timestamp of the last update to this order.
        instrumentId:
          type: integer
          description: The ID of the instrument traded.
        unitsToDeduct:
          type: number
          format: float
          description: The number of units closed in this order.
        lotsToDeduct:
          type: number
          format: float
          description: The number of lots closed in this order.
        pendingClosePositionIds:
          type: array
          items:
            type: integer
          description: IDs of positions pending close in this order.

````

Built with [Mintlify](https://mintlify.com).

> ## Documentation Index
> Fetch the complete documentation index at: https://api-portal.etoro.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Get Real Account PnL and Portfolio Details

> Retrieves the real account's current portfolio, including credit, open positions, orders, mirrors, and PnL details.



## OpenAPI

````yaml api-reference/openapi.json get /api/v1/trading/info/real/pnl
openapi: 3.0.1
info:
  title: eToro Api
  version: v1.156.0
  description: >-
    eToro’s public API provides access to real-time financial data, trading
    insights, and account management features, allowing developers to integrate
    eToro’s services into their applications. With access to market prices,
    historical data, and social trading information, the API empowers users to
    enhance their trading strategies. Designed for security and scalability, the
    eToro API ensures smooth and reliable integration for a variety of financial
    applications.


    For more details on integrating with eToro's public WebSocket service,
    please refer to the dedicated [WebSocket
    documentation](./websocket/websocket-doc.html).
servers:
  - url: https://public-api.etoro.com
    description: eToro Public API
security: []
paths:
  /api/v1/trading/info/real/pnl:
    get:
      tags:
        - Trading - Real
      summary: Get Real Account PnL and Portfolio Details
      description: >-
        Retrieves the real account's current portfolio, including credit, open
        positions, orders, mirrors, and PnL details.
      operationId: getRealAccountPnl
      parameters:
        - name: x-request-id
          in: header
          required: true
          schema:
            type: string
            format: uuid
            example: 9ab4b46b-e254-4d06-8cad-2772f8decef6
          description: A unique request identifier.
        - name: x-api-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: lhgfaslk21490FAScVPkdsb53F9dNkfHG4faZSG5vfjndfcfgdssdgsdHF4663
          description: API key for authentication.
        - name: x-user-key
          in: header
          required: true
          schema:
            type: string
            format: password
            example: >-
              eyJlYW4iOiJVbnJlZ2lzdGVyZWRBcHBsaWNhdGlvbiIsImVrIjoiOE5sZ2cwcW5EUVdROUFNWGpXT2lmOWktZnpidG5KcUlqWGJ3WHJZZkpZcldrbG90ZEhvLVBjSWhQaU8xU1ZtMW84aU1WZGZqN2xWNzFjLXFxLmcybXE1dnh4Q1hUT25xaWRUaTFlcEhmVk1fIn0_
          description: User-specific authentication key.
      responses:
        '200':
          description: Successfully retrieved real account PnL and portfolio information.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/PortfolioResponseWithPnl'
              example:
                clientPortfolio:
                  credit: 10000.5
                  unrealizedPnL: 251
                  mirrors:
                    - mirrorId: 1
                      cid: 123
                      parentCid: 456
                      stopLossPercentage: 15.5
                      isPaused: false
                      copyExistingPositions: true
                      availableAmount: 5000
                      stopLossAmount: 750
                      initialInvestment: 10000
                      depositSummary: 12000
                      withdrawalSummary: 2000
                      positions:
                        - positionId: 9002
                          cid: 124
                          openDateTime: '2024-01-02T09:00:00Z'
                          openRate: 1.2346
                          instrumentId: 102
                          isBuy: false
                          takeProfitRate: 1.6
                          stopLossRate: 1.1
                          mirrorId: 1
                          parentPositionId: 8002
                          amount: 2000
                          leverage: 3
                          orderId: 5002
                          orderType: 2
                          units: 20.5
                          totalFees: 3.5
                          initialAmountInDollars: 2000
                          isTslEnabled: true
                          stopLossVersion: 2
                          isSettled: false
                          redeemStatusId: 1
                          initialUnits: 20.5
                          isPartiallyAltered: true
                          unitsBaseValueDollars: 2000
                          isDiscounted: true
                          openPositionActionType: 2
                          settlementTypeId: 2
                          isDetached: true
                          openConversionRate: 1.2
                          pnlVersion: 2
                          totalExternalFees: 1
                          totalExternalTaxes: 0.5
                          isNoTakeProfit: true
                          isNoStopLoss: false
                          lotCount: 2
                          externalOperation: null
                          pnL: 150.75
                          closeRate: 1.3
                          closeConversionRate: 1.15
                          timestamp: '2024-01-02T12:00:00Z'
                      parentUsername: parent_user
                      closedPositionsNetProfit: 350.75
                      startedCopyDate: '2024-01-01T09:00:00Z'
                      pendingForClosure: false
                      parentMirrors: []
                      mirrorCalculationType: 2
                      ordersForOpen:
                        - orderId: 1001
                          orderType: 1
                          statusId: 1
                          cid: 123
                          openDateTime: '2024-01-01T09:00:00Z'
                          lastUpdate: '2024-01-02T10:00:00Z'
                          instrumentId: 101
                          amount: 1000
                          amountInUnits: 10.5
                          isBuy: true
                          leverage: 2
                          stopLossRate: 1.2345
                          takeProfitRate: 1.3456
                          isTslEnabled: false
                          isDiscounted: true
                          mirrorId: 1
                          frozenAmount: 0
                          totalExternalCosts: 5
                          isNoTakeProfit: false
                          isNoStopLoss: false
                          lotCount: 1
                          openPositionActionType: 1
                          externalOperation: null
                      ordersForClose:
                        - orderId: 2001
                          orderType: 2
                          statusId: 1
                          cid: 123
                          openDateTime: '2024-01-01T09:00:00Z'
                          lastUpdate: '2024-01-02T10:00:00Z'
                          instrumentId: 101
                          unitsToDeduct: 5
                          lotsToDeduct: 0.5
                          positionId: 3001
                      ordersForCloseMultiple:
                        - orderId: 3001
                          orderType: 3
                          statusId: 1
                          cid: 123
                          openDateTime: '2024-01-01T09:00:00Z'
                          lastUpdate: '2024-01-02T10:00:00Z'
                          instrumentId: 101
                          unitsToDeduct: 10
                          lotsToDeduct: 1
                          pendingClosePositionIds:
                            - 3001
                            - 3002
                      mirrorStatusId: 1
                  orders:
                    - orderId: 5001
                      cid: 123
                      openDateTime: '2024-01-01T09:00:00Z'
                      instrumentId: 101
                      isBuy: true
                      takeProfitRate: 1.5
                      stopLossRate: 1.2
                      rate: 1.3
                      amount: 1000
                      leverage: 2
                      units: 10.5
                      isTslEnabled: false
                      executionType: 1
                      isDiscounted: false
                      isNoTakeProfit: false
                      isNoStopLoss: false
                  ordersForOpen:
                    - orderId: 1001
                      orderType: 1
                      statusId: 1
                      cid: 123
                      openDateTime: '2024-01-01T09:00:00Z'
                      lastUpdate: '2024-01-02T10:00:00Z'
                      instrumentId: 101
                      amount: 1000
                      amountInUnits: 10.5
                      isBuy: true
                      leverage: 2
                      stopLossRate: 1.2345
                      takeProfitRate: 1.3456
                      isTslEnabled: false
                      isDiscounted: true
                      mirrorId: 1
                      frozenAmount: 0
                      totalExternalCosts: 5
                      isNoTakeProfit: false
                      isNoStopLoss: false
                      lotCount: 1
                      openPositionActionType: 1
                      externalOperation: null
                  ordersForClose:
                    - orderId: 2001
                      orderType: 2
                      statusId: 1
                      cid: 123
                      openDateTime: '2024-01-01T09:00:00Z'
                      lastUpdate: '2024-01-02T10:00:00Z'
                      instrumentId: 101
                      unitsToDeduct: 5
                      lotsToDeduct: 0.5
                      positionId: 3001
                  ordersForCloseMultiple:
                    - orderId: 3001
                      orderType: 3
                      statusId: 1
                      cid: 123
                      openDateTime: '2024-01-01T09:00:00Z'
                      lastUpdate: '2024-01-02T10:00:00Z'
                      instrumentId: 101
                      unitsToDeduct: 10
                      lotsToDeduct: 1
                      pendingClosePositionIds:
                        - 3001
                        - 3002
                  bonusCredit: 500
                  positions:
                    - positionId: 9001
                      cid: 123
                      openDateTime: '2024-01-01T09:00:00Z'
                      openRate: 1.2345
                      instrumentId: 101
                      isBuy: true
                      takeProfitRate: 1.5
                      stopLossRate: 1.2
                      mirrorId: 1
                      parentPositionId: 8001
                      amount: 1000
                      leverage: 2
                      orderId: 5001
                      orderType: 1
                      units: 10.5
                      totalFees: 2.5
                      initialAmountInDollars: 1000
                      isTslEnabled: false
                      stopLossVersion: 1
                      isSettled: true
                      redeemStatusId: 0
                      initialUnits: 10.5
                      isPartiallyAltered: false
                      unitsBaseValueDollars: 1000
                      isDiscounted: false
                      openPositionActionType: 1
                      settlementTypeId: 1
                      isDetached: false
                      openConversionRate: 1
                      pnlVersion: 1
                      totalExternalFees: 0
                      totalExternalTaxes: 0
                      isNoTakeProfit: false
                      isNoStopLoss: false
                      lotCount: 1
                      externalOperation: null
                      pnL: 100.25
                      closeRate: 1.25
                      closeConversionRate: 1.1
                      timestamp: '2024-01-01T12:00:00Z'
      security:
        - bearerAuth: []
components:
  schemas:
    PortfolioResponseWithPnl:
      type: object
      description: >-
        Comprehensive portfolio information including positions, orders, and
        account status
      properties:
        clientPortfolio:
          $ref: '#/components/schemas/ClientPortfolio'
          description: Container for all portfolio-related information
    ClientPortfolio:
      type: object
      properties:
        positions:
          type: array
          description: List of currently open trading positions
          items:
            $ref: '#/components/schemas/Position'
        credit:
          type: number
          format: float
          description: >-
            Available trading balance in USD, representing funds available for
            new actions
        mirrors:
          type: array
          items:
            $ref: '#/components/schemas/Mirror'
          description: Copy trading configurations and positions
        orders:
          type: array
          items:
            $ref: '#/components/schemas/Order'
          description: List of pending orders
        ordersForOpen:
          type: array
          items:
            $ref: '#/components/schemas/OrderForOpen'
          description: Active orders to open positions
        ordersForClose:
          type: array
          items:
            $ref: '#/components/schemas/OrderForClose'
          description: Active orders to close positions
        ordersForCloseMultiple:
          type: array
          items:
            $ref: '#/components/schemas/OrderForCloseMultiple'
          description: Active orders to close multiple positions
        bonusCredit:
          type: number
          format: float
          description: Bonus credit amount in USD in the account
        unrealizedPnL:
          type: number
          format: float
          description: >-
            Total unrealized profit and loss across all open positions in the
            portfolio
        accountCurrencyId:
          type: integer
          description: Currency ID of the account (1 = USD)
        stockOrders:
          type: array
          items:
            type: object
          description: Stock-specific pending orders
        entryOrders:
          type: array
          items:
            type: object
          description: Entry orders awaiting execution
        exitOrders:
          type: array
          items:
            type: object
          description: Exit orders awaiting execution
    Position:
      type: object
      description: Individual position details
      properties:
        positionID:
          type: integer
          description: Unique identifier for the position
        CID:
          type: integer
          description: Customer ID associated with the position
        openDateTime:
          type: string
          format: date-time
          description: Timestamp when the position was opened in ISO 8601 format
        openRate:
          type: number
          format: float
          description: Entry price of the position in the instrument's currency
        instrumentID:
          type: integer
          description: Identifier of the traded instrument
        mirrorID:
          type: integer
          description: Mirror ID if the position is part of copy trading, 0 otherwise
        parentPositionID:
          type: integer
          description: Parent position ID for mirrored positions, 0 otherwise
        isBuy:
          type: boolean
          description: >-
            Position direction: true for long (buy) positions, false for short
            (sell) positions
        leverage:
          type: number
          format: float
          description: Leverage multiplier applied to the position
        takeProfitRate:
          type: number
          format: float
          description: >-
            The take-profit trigger price at which the position will generate a
            Market Order to close (after it has opened). TakeProfit trigger
            price must be better than the current price.
        stopLossRate:
          type: number
          format: float
          description: >-
            The stop-loss trigger price at which the position will generate a
            Market Order to close (after it was opened). StopLoss trigger price
            must be worse than current price.
        amount:
          type: number
          format: float
          description: >-
            USD amount allocated to the position. This amount includes both the
            initial investment, and additional margin allocated to the position
            as collateral
        orderID:
          type: integer
          description: >-
            Original orderID the position was opened by. Need to match together
            with orderType
        orderType:
          type: integer
          description: >-
            Original orderType of the order the position was opened by. Need to
            match together with orderID
        units:
          type: number
          format: float
          description: Number of units in the position
        totalFees:
          type: number
          format: float
          description: >-
            Total overnight fees and dividends charged/paid on the position in
            USD. Negative amount represents refund
        initialAmountInDollars:
          type: number
          format: float
          description: Initial investment USD amount in the position
        isTslEnabled:
          type: boolean
          description: Indication if TrailingStopLoss feature is active on this position
        stopLossVersion:
          type: integer
          description: >-
            Manual stop loss edit version. Each time StopLossRate is manually
            update this value is incremented
        isSettled:
          type: boolean
          description: Obsolete
        redeemStatusID:
          type: integer
          description: >-
            If the position is currently in redeem process, this value
            represents the current status
        initialUnits:
          type: number
          format: float
          description: Initial invested units in the position
        isPartiallyAltered:
          type: boolean
          description: Indication whether this position was partially closed
        unitsBaseValueDollars:
          type: number
          format: float
          description: Current units invested value in USD
        isDiscounted:
          type: boolean
          description: Obsolete
        openPositionActionType:
          type: integer
          description: Position open reason
        settlementTypeID:
          type: integer
          description: >-
            Position investment type. 0 - CFD, 1 - Real Asset, 2 - SWAP, 3 -
            Crypto MarginTrade, 4 - Future Contract
        isDetached:
          type: boolean
          description: >-
            Indication if the position was originally opened inside a mirror and
            detached from it
        openConversionRate:
          type: number
          format: float
          description: Conversion rate at position opening
        pnlVersion:
          type: integer
          description: Pnl formula used for calculating profit and loss
        totalExternalFees:
          type: number
          format: float
          description: >-
            Total fees in USD charged on the position. Example - TicketFee. This
            value does not include overnight fees and dividends
        totalExternalTaxes:
          type: number
          format: float
          description: Total taxes in USD charged on the position. Example - SDRT
        isNoTakeProfit:
          type: boolean
          description: >-
            Indication if TakeProfit is enabled for the position. false =
            enabled, true = disabled
        isNoStopLoss:
          type: boolean
          description: >-
            Indication if StopLoss is enabled for the position. false = enabled,
            true = disabled
        lotCount:
          type: number
          format: float
          description: >-
            Number of lots the position represents. For FutureContracts this
            value represents the number of contracts acquired
    Mirror:
      type: object
      properties:
        mirrorID:
          type: integer
          description: Unique identifier for the mirror
        CID:
          type: integer
          description: Customer ID associated with the mirror
        parentCID:
          type: integer
          description: Customer ID of the trader being copied
        stopLossPercentage:
          type: number
          format: float
          description: >-
            The precentage of the mirror value that the StopLossAmount
            represented at the time of the last edit. Adding or removing funds
            from the mirror will trigger recalculation of StopLossAmount based
            on this value compared to the current mirror value
        isPaused:
          type: boolean
          description: >-
            Indication if the mirror is currently paused, restricting open of
            additional positions inside the mirror
        copyExistingPositions:
          type: boolean
          description: >-
            Indication if mirror originally copied all parent existing position
            on mirror registration
        availableAmount:
          type: number
          format: float
          description: >-
            Available to trade USD balance in the mirror. This balance is
            reserved for mirror operations
        stopLossAmount:
          type: number
          format: float
          description: >-
            USD value of the mirror at which MirrorStopLoss will be triggered
            and cause liquidation of the mirror. Adding or removing funds from
            the mirror will trigger recalculation of this value based on
            StopLossPercentage compared to the current mirror value
        initialInvestment:
          type: number
          format: float
          description: USD amount initially invested in the mirror
        depositSummary:
          type: number
          format: float
          description: Total USD amount deposited into the mirror after initial investment
        withdrawalSummary:
          type: number
          format: float
          description: Total USD amount withdrawn from the mirror
        positions:
          type: array
          items:
            $ref: '#/components/schemas/Position'
          description: List of currently open trading positions in the mirror
        parentUsername:
          type: string
          description: Username of the trader being copied
        closedPositionsNetProfit:
          type: number
          format: float
          description: Total USD net profit of all positions that closed in the mirror
        startedCopyDate:
          type: string
          format: date-time
          description: Date and time when the mirror trading was initiated
        pendingForClosure:
          type: boolean
          description: Indication if the mirror is in closure process
        parentMirrors:
          type: array
          items:
            type: object
          description: Parent mirrors for this mirror (if any)
        mirrorCalculationType:
          type: integer
          description: (Obsolete) Mirror positions weights calculation methodology
        ordersForOpen:
          type: array
          items:
            $ref: '#/components/schemas/OrderForOpen'
          description: Active orders in the mirror to open positions
        ordersForClose:
          type: array
          items:
            $ref: '#/components/schemas/OrderForClose'
          description: Active orders in the mirror to close positions
        ordersForCloseMultiple:
          type: array
          items:
            $ref: '#/components/schemas/OrderForCloseMultiple'
          description: Active orders in the mirror to close positions
        mirrorStatusID:
          type: integer
          description: >-
            Current status of the mirror. 0 - Active, 1 - Paused, 2 - Pending
            Closure, 3 - In Alignment Process
        delayedOrderForClose:
          type: array
          items:
            type: object
          description: Delayed orders for closing positions
        delayedOrderForOpen:
          type: array
          items:
            type: object
          description: Delayed orders for opening positions
        entryOrders:
          type: array
          items:
            type: object
          description: Entry orders awaiting execution in the mirror
        exitOrders:
          type: array
          items:
            type: object
          description: Exit orders awaiting execution in the mirror
    Order:
      type: object
      properties:
        orderId:
          type: integer
          description: Unique identifier for the order
        cid:
          type: integer
          description: Customer ID associated with the order
        openDateTime:
          type: string
          format: date-time
          description: Date and time when the order was created
        instrumentId:
          type: integer
          description: Identifier of the instrument being traded
        isBuy:
          type: boolean
          description: Direction of the position. true - Long, false - Short
        takeProfitRate:
          type: number
          format: float
          description: >-
            Rate at which TakeProfit will trigger and send MarketOrder to close
            the position once it is open
        stopLossRate:
          type: number
          format: float
          description: >-
            Rate at which StopLoss will trigger and send MarketOrder to close
            the position once it is open
        rate:
          type: number
          format: float
          description: Asset rate at which to send market order to the market
        amount:
          type: number
          format: float
          description: USD amount to invest in the position
        leverage:
          type: integer
          description: Leverage multiplier to apply to the position
        units:
          type: number
          format: float
          description: >-
            Units to open the position. If this value is greater than zero the
            position will open on the requested units, and not amount
        isTslEnabled:
          type: boolean
          description: Indication if to enable TSL feature on the position once it is open
        executionType:
          type: integer
          description: Type of order execution
        isDiscounted:
          type: boolean
          description: Obsolete
        isNoTakeProfit:
          type: boolean
          description: >-
            Indication if TakeProfit is enabled for the order. false = enabled,
            true = disabled
        isNoStopLoss:
          type: boolean
          description: >-
            Indication if StopLoss is enabled for the order. false = enabled,
            true = disabled
    OrderForOpen:
      type: object
      properties:
        orderId:
          type: integer
          description: Unique identifier for the order
        orderType:
          type: integer
          description: Type of order executed
        statusId:
          type: integer
          description: Status of the order
        cid:
          type: integer
          description: Customer ID associated with the order
        openDateTime:
          type: string
          format: date-time
          description: The timestamp when the order was opened.
        lastUpdate:
          type: string
          format: date-time
          description: The last update timestamp of the order.
        instrumentId:
          type: integer
          description: The unique identifier of the financial instrument to trade.
        amount:
          type: number
          format: float
          description: The amount of money to invest in the trade.
        amountInUnits:
          type: number
          format: float
          description: The number of units to trade.
        isBuy:
          type: boolean
          description: True for a buy (long) order, false for a sell (short) order.
        leverage:
          type: integer
          description: The leverage multiplier for the trade.
        stopLossRate:
          type: number
          format: float
          description: >-
            The stop-loss rate at which the trade will automatically close to
            limit losses.
        takeProfitRate:
          type: number
          format: float
          description: >-
            The take-profit rate at which the trade will automatically close to
            secure profits.
        isTslEnabled:
          type: boolean
          description: Indicates whether a trailing stop-loss is enabled.
        isDiscounted:
          type: boolean
          description: Indicates if the order is eligible for a discount.
        mirrorId:
          type: integer
          description: ID related to mirrored trades, if applicable.
        frozenAmount:
          type: number
          format: float
          description: Amount frozen for the order.
        totalExternalCosts:
          type: number
          format: float
          description: Total external costs associated with the trade.
        isNoTakeProfit:
          type: boolean
          description: True if no take-profit is set for this order.
        isNoStopLoss:
          type: boolean
          description: True if no stop-loss is set for this order.
        lotCount:
          type: number
          format: float
          description: The number of lots in the order.
        openPositionActionType:
          type: integer
          description: Position open reason.
        externalOperation:
          type: object
          description: External operation details, if any.
          nullable: true
    OrderForClose:
      type: object
      properties:
        orderId:
          type: integer
          description: Unique identifier for the closing order.
        orderType:
          type: integer
          description: Type of order executed.
        statusId:
          type: integer
          description: Status of the closing order.
        cid:
          type: integer
          description: Customer ID associated with the order.
        openDateTime:
          type: string
          format: date-time
          description: The timestamp when the order was placed.
        lastUpdate:
          type: string
          format: date-time
          description: The timestamp of the last update to this order.
        instrumentId:
          type: integer
          description: The ID of the instrument traded.
        unitsToDeduct:
          type: number
          format: float
          description: The number of units closed in this order.
        lotsToDeduct:
          type: number
          format: float
          description: The number of lots closed in this order.
        positionId:
          type: integer
          description: The ID of the closed position.
    OrderForCloseMultiple:
      type: object
      properties:
        orderId:
          type: integer
          description: Unique identifier for the closing order.
        orderType:
          type: integer
          description: Type of order executed.
        statusId:
          type: integer
          description: Status of the closing order.
        cid:
          type: integer
          description: Customer ID associated with the order.
        openDateTime:
          type: string
          format: date-time
          description: The timestamp when the order was placed.
        lastUpdate:
          type: string
          format: date-time
          description: The timestamp of the last update to this order.
        instrumentId:
          type: integer
          description: The ID of the instrument traded.
        unitsToDeduct:
          type: number
          format: float
          description: The number of units closed in this order.
        lotsToDeduct:
          type: number
          format: float
          description: The number of lots closed in this order.
        pendingClosePositionIds:
          type: array
          items:
            type: integer
          description: IDs of positions pending close in this order.

````

Built with [Mintlify](https://mintlify.com).
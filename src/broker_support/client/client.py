"""
eToro API client.

Confirmed working endpoints (OpenAPI v1.138.0 + empirical tests 2026-03-12):
  GET  /api/v1/watchlists                                                        — connection test
  GET  /api/v1/trading/info/demo/portfolio                                       — portfolio (Demo key)
  GET  /api/v1/trading/info/demo/orders/{orderId}                                — order info / positionID
  GET  /api/v1/trading/info/trade/history?minDate=YYYY-MM-DD                     — closed trade history
  GET  /api/v1/market-data/instruments/rates?instrumentIds=...                   — current prices
  GET  /api/v1/market-data/search?searchText=...&fields=...                      — instrument lookup
  POST /api/v1/trading/execution/demo/market-open-orders/by-amount               — open demo order
  POST /api/v1/trading/execution/demo/market-close-orders/positions/{positionId} — close demo position

_make_request() is the core HTTP engine — do not refactor.
All public methods delegate to _make_request(); none implement their own HTTP logic.

KEY TYPE REQUIREMENT:
  ETORO_USER_KEY must be the Demo Write key to access /demo/ endpoints.
  Real key → 403 on all /demo/ paths.
  fetch_closed_trades() requires Read+Write key (Read-only → 403).
"""
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from src.broker_support.config.settings import settings


class EToroClient:
    """Production-grade API client for eToro public API v1."""

    _BASE_PATH = "api/v1"

    def __init__(self) -> None:
        self.base_url = settings.etoro_base_url.rstrip('/')
        self._api_key = settings.etoro_api_key
        self._user_key = settings.etoro_user_key
        self.session = requests.Session()

    # ------------------------------------------------------------------
    # Core HTTP engine — do not modify
    # ------------------------------------------------------------------

    def _get_headers(self) -> Dict[str, str]:
        """Generate request headers. x-request-id is unique per call."""
        return {
            'x-api-key': self._api_key,
            'x-user-key': self._user_key,
            'x-request-id': str(uuid.uuid4()),
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'BrokerSupport/0.1.0',
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        **kwargs: Any,
    ) -> Any:
        """
        Execute an HTTP request against the eToro API.

        Args:
            method:   HTTP verb ('GET', 'POST', 'DELETE').
            endpoint: Path relative to base URL, e.g. 'api/v1/watchlists'.
            **kwargs: Forwarded to requests.Session.request (params, json, etc.).

        Returns:
            Parsed JSON response (dict or list).

        Raises:
            requests.exceptions.RequestException on HTTP or network errors.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        headers = self._get_headers()

        masked = {
            k: (v[:8] + '…' if k in ('x-api-key', 'x-user-key') else v)
            for k, v in headers.items()
        }
        logger.debug(f"{method} {url}  headers={masked}  kwargs={kwargs}")

        response = self.session.request(
            method,
            url,
            headers=headers,
            timeout=settings.timeout_seconds,
            **kwargs,
        )

        logger.debug(f"Response {response.status_code} from {url}")

        if not response.ok:
            logger.error(
                f"API error {response.status_code}: {response.text[:500]}"
            )
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Connection test
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(settings.max_retries),
        wait=wait_exponential(multiplier=1, min=4, max=10),
    )
    def test_connection(self) -> bool:
        """Verify API credentials via /watchlists. Returns True on success."""
        logger.info("Testing eToro API connection via /watchlists …")
        try:
            self._make_request('GET', f"{self._BASE_PATH}/watchlists")
            logger.info("Connection OK.")
            return True
        except Exception as exc:
            logger.error(f"Connection failed: {exc}")
            return False

    # ------------------------------------------------------------------
    # Portfolio
    # FIX (2026-03-12): endpoint changed from /trading/info/portfolio to
    # /trading/info/demo/portfolio. Requires Demo Write key.
    # Real key → 403. Key type determines account, not URL prefix.
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(settings.max_retries),
        wait=wait_exponential(multiplier=1, min=4, max=10),
    )
    def get_portfolio(self) -> Dict[str, Any]:
        """
        Fetch demo portfolio including open positions, orders, mirrors, credit.

        Endpoint: GET /api/v1/trading/info/demo/portfolio
        Returns the raw clientPortfolio dict (unwrapped from response root).

        Position field aliases: PascalCase + capital ID (positionID, instrumentID).
        Parsed by OpenPosition Pydantic model in models/portfolio.py.
        Credit field: 'credit' (note: /demo/pnl uses 'credits' — different name).
        """
        logger.info("Fetching demo portfolio …")
        result = self._make_request(
            'GET', f"{self._BASE_PATH}/trading/info/demo/portfolio"
        )
        portfolio = result.get('clientPortfolio', {})
        logger.info(
            f"Portfolio: credit={portfolio.get('credit')}, "
            f"positions={len(portfolio.get('positions', []))}"
        )
        return portfolio

    # ------------------------------------------------------------------
    # Order info — Step 5 two-step open flow
    # place_market_order() → get_order_info() → positionID
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(settings.max_retries),
        wait=wait_exponential(multiplier=1, min=4, max=10),
    )
    def get_order_info(self, order_id: int) -> Dict[str, Any]:
        """
        Fetch order details including positions opened by this order.

        Step 2 of two-step open flow to retrieve positionID:
          place_market_order() → orderForOpen.orderID
          get_order_info(orderID) → positions[0].positionID

        Endpoint: GET /api/v1/trading/info/demo/orders/{orderId}

        Key response fields:
          statusID: 0=Pending, 1=Executed, 2=Cancelled, 3=Rejected, 4=Partial
          positions[n].positionID: use this for all close calls
          positions[n].isOpen: True if currently open

        Args:
            order_id: The orderID from orderForOpen in place_market_order response.

        Returns:
            Raw order info dict.
        """
        logger.info(f"Fetching order info for orderId={order_id} …")
        result = self._make_request(
            'GET',
            f"{self._BASE_PATH}/trading/info/demo/orders/{order_id}",
        )
        logger.debug(
            f"Order info: statusID={result.get('statusID')}, "
            f"positions={len(result.get('positions', []))}"
        )
        return result

    # ------------------------------------------------------------------
    # Trade history
    # minDate=YYYY-MM-DD is the correct param (NOT 'from'/'fromDate').
    # Requires Read+Write key. Demo trades appear here (RESULT A confirmed).
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(settings.max_retries),
        wait=wait_exponential(multiplier=1, min=4, max=10),
    )
    def fetch_closed_trades(
        self,
        from_date: Optional[datetime] = None,
        page: int = 1,
        page_size: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Fetch closed trade history.

        Endpoint: GET /api/v1/trading/info/trade/history
        Required param: minDate=YYYY-MM-DD (confirmed official param name).
        Requires Read+Write key (Read-only → 403).
        Demo trades appear here (RESULT A confirmed 2026-03-12).
        Returns array directly (not wrapped in an object key).

        Args:
            from_date:  Start date. Defaults to settings.default_days_back ago.
            page:       Page number (1-based).
            page_size:  Items per page (max 100).
        """
        if from_date is None:
            from_date = datetime.now() - timedelta(days=settings.default_days_back)

        min_date_str = from_date.strftime('%Y-%m-%d')
        logger.info(f"Fetching trade history from {min_date_str} …")

        result = self._make_request(
            'GET',
            f"{self._BASE_PATH}/trading/info/trade/history",
            params={'minDate': min_date_str, 'page': page, 'pageSize': page_size},
        )

        trades: List[Dict] = result if isinstance(result, list) else result.get('data', [])
        logger.info(f"Received {len(trades)} trades (page {page}).")
        return trades

    # ------------------------------------------------------------------
    # Market data
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(settings.max_retries),
        wait=wait_exponential(multiplier=1, min=4, max=10),
    )
    def get_current_rates(self, instrument_ids: List[int]) -> Dict[str, Any]:
        """
        Fetch current bid/ask rates for one or more instruments.

        Endpoint: GET /api/v1/market-data/instruments/rates
        Required: instrumentIds comma-separated (max 100).
        Response: { rates: [{ instrumentID, ask, bid, lastExecution,
                               conversionRateAsk, conversionRateBid, date }] }

        ask = buy price (long entry / short close).
        bid = sell price (short entry / long close).
        """
        ids_param = ','.join(str(i) for i in instrument_ids)
        logger.info(f"Fetching rates for instrumentIds={ids_param}")
        return self._make_request(
            'GET',
            f"{self._BASE_PATH}/market-data/instruments/rates",
            params={'instrumentIds': ids_param},
        )

    @retry(
        stop=stop_after_attempt(settings.max_retries),
        wait=wait_exponential(multiplier=1, min=4, max=10),
    )
    def search_instrument(self, symbol: str) -> List[Dict[str, Any]]:
        """
        Resolve a ticker symbol to an instrumentId.

        Endpoint: GET /api/v1/market-data/search
        'fields' param is REQUIRED by the API — requests without it return empty.
        Uses searchText fuzzy matching; caller must exact-match on
        internalSymbolFull in results to avoid false matches.

        Response: { page, pageSize, totalItems, items: [...] }
        Items use lowercase 'instrumentId' (different from portfolio PascalCase).
        isCurrentlyTradable: False means instrument cannot be traded right now.

        Used by InstrumentResolver (Step 2).
        """
        logger.info(f"Searching instrument for symbol={symbol!r}")
        result = self._make_request(
            'GET',
            f"{self._BASE_PATH}/market-data/search",
            params={
                'searchText': symbol,
                'fields': 'instrumentId,internalSymbolFull,displayname,isCurrentlyTradable',
                'pageSize': 10,
            },
        )
        # Response wraps results under 'items'
        instruments: List[Dict] = (
            result.get('items')
            or result.get('instruments')
            or result.get('data')
            or (result if isinstance(result, list) else [])
        )
        return instruments

    # ------------------------------------------------------------------
    # Execution — Step 5
    # Two-step open: place_market_order → get_order_info → positionID
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(settings.max_retries),
        wait=wait_exponential(multiplier=1, min=4, max=10),
    )
    def place_market_order(
        self,
        instrument_id: int,
        is_buy: bool,
        amount: float,
        leverage: int = 1,
        stop_loss_rate: Optional[float] = None,
        take_profit_rate: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Open a demo market order by amount. Step 1 of two-step open flow.

        Endpoint: POST /api/v1/trading/execution/demo/market-open-orders/by-amount
        Body: PascalCase + capital ID per API convention.
        SL/TP are absolute price levels (not distances from entry).

        ⚠️ positionID is NOT in the response.
        Call get_order_info(response['orderForOpen']['orderID']) to get it.

        Response: { orderForOpen: { orderID, instrumentID, amount, isBuy,
                                    leverage, statusID, CID, openDateTime },
                    token: uuid }

        Args:
            instrument_id:    eToro instrumentId (e.g. 32 for DAX).
            is_buy:           True for BUY, False for SELL.
            amount:           USD amount to invest.
            leverage:         Leverage multiplier (default 1).
            stop_loss_rate:   Absolute SL price (optional).
            take_profit_rate: Absolute TP price (optional).
        """
        body: Dict[str, Any] = {
            'InstrumentID': instrument_id,   # capital ID — confirmed
            'IsBuy': is_buy,
            'Leverage': leverage,
            'Amount': amount,
        }
        if stop_loss_rate is not None:
            body['StopLossRate'] = stop_loss_rate
        if take_profit_rate is not None:
            body['TakeProfitRate'] = take_profit_rate

        logger.info(
            f"Placing demo market order: instrumentId={instrument_id}, "
            f"isBuy={is_buy}, amount={amount}, leverage={leverage}, "
            f"sl={stop_loss_rate}, tp={take_profit_rate}"
        )
        result = self._make_request(
            'POST',
            f"{self._BASE_PATH}/trading/execution/demo/market-open-orders/by-amount",
            json=body,
        )
        logger.info(f"Market order response: {result}")
        return result

    @retry(
        stop=stop_after_attempt(settings.max_retries),
        wait=wait_exponential(multiplier=1, min=4, max=10),
    )
    def close_position(
        self,
        position_id: int,
        instrument_id: int,
        units_to_deduct: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Close a demo position (full or partial).

        Endpoint: POST /api/v1/trading/execution/demo/market-close-orders/positions/{positionId}
        Body: { "InstrumentID": ..., "UnitsToDeduct": null }  ← null = full close.

        ⚠️ Body key is 'InstrumentID' (capital ID) — NOT 'InstrumentId' (lowercase d).

        Response: { orderForClose: { positionID, instrumentID, unitsToDeduct,
                                     orderID, statusID, CID, openDateTime },
                    token: uuid }

        Args:
            position_id:      positionID from get_order_info().positions[0].positionID.
            instrument_id:    instrumentID of the position.
            units_to_deduct:  Units to close. None → JSON null → full close.
        """
        body: Dict[str, Any] = {
            'InstrumentID': instrument_id,   # capital ID — confirmed
            'UnitsToDeduct': units_to_deduct,  # None → JSON null → full close
        }
        logger.info(
            f"Closing demo position: positionId={position_id}, "
            f"instrumentId={instrument_id}, units_to_deduct={units_to_deduct}"
        )
        result = self._make_request(
            'POST',
            f"{self._BASE_PATH}/trading/execution/demo"
            f"/market-close-orders/positions/{position_id}",
            json=body,
        )
        logger.info(f"Close position response: {result}")
        return result
"""
eToro API client.

Confirmed working endpoints (from official eToro SKILL.md / api-portal.etoro.com):
  GET  /api/v1/watchlists                                          — connection test
  GET  /api/v1/trading/info/portfolio                              — portfolio + open positions (empirically confirmed)
  GET  /api/v1/trading/info/trade/history?minDate=YYYY-MM-DD       — closed trade history (Bug 3 fix)
  GET  /api/v1/market-data/instruments/rates?instrumentIds=...     — current prices (Step 3)
  GET  /api/v1/market-data/search?searchText=...                   — instrument lookup (Step 2)
  POST /api/v1/trading/execution/demo/market-open-orders/by-amount — open demo order (Step 5)
  POST /api/v1/trading/execution/demo/market-close-orders/positions/{positionId} — close (Step 5)

_make_request() is the core engine — do not refactor.
All public methods delegate to _make_request(); none implement their own HTTP logic.
"""
import uuid
from datetime import datetime, date
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
        """
        Verify API credentials are valid using the watchlists endpoint.
        Returns True on success, False on failure.
        """
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
    # Empirically confirmed 2026-03-12: /demo/pnl and /demo/portfolio both 403.
    # Key environment (Virtual/Real) determines which account data is returned.
    # Correct endpoint: /trading/info/portfolio (no /demo/ prefix needed).
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(settings.max_retries),
        wait=wait_exponential(multiplier=1, min=4, max=10),
    )
    def get_portfolio(self) -> Dict[str, Any]:
        """
        Fetch portfolio including open positions, orders, mirrors, credit.

        Endpoint: GET /api/v1/trading/info/portfolio
        Account type (demo vs real) is determined by the key environment,
        not by the endpoint path. Returns the raw clientPortfolio dict.
        """
        logger.info("Fetching portfolio …")
        result = self._make_request(
            'GET', f"{self._BASE_PATH}/trading/info/portfolio"
        )
        portfolio = result.get('clientPortfolio', {})
        logger.info(f"Portfolio: credit={portfolio.get('credit')}, "
                    f"positions={len(portfolio.get('positions', []))}")
        return portfolio

    # ------------------------------------------------------------------
    # Trade history — Bug 2 fix (orphaned function → class method)
    #               — Bug 3 fix: 'from'/'fromDate' → 'minDate' (YYYY-MM-DD)
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
        Required param: minDate (YYYY-MM-DD format — confirmed official param name).

        NOTE: Whether demo-account trades appear here must be verified empirically
        (Phase 0 Step 1 — empirical demo history test). This is the only confirmed
        history endpoint in the official API.

        Args:
            from_date:  Start date. Defaults to settings.default_days_back ago.
            page:       Page number (1-based).
            page_size:  Items per page.

        Returns:
            List of raw trade dicts.
        """
        if from_date is None:
            from datetime import timedelta
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
        Required: instrumentIds as comma-separated string.

        Used by TradeEnricher (Step 3) when demo history endpoint is unavailable.
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
        Required: searchText (fuzzy text search). fields is required by the API.
        Note: internalSymbolFull as a filter param returns empty — use searchText.

        Used by InstrumentResolver (Step 2).
        """
        logger.info(f"Searching instrument for symbol={symbol!r}")
        result = self._make_request(
            'GET',
            f"{self._BASE_PATH}/market-data/search",
            params={
                'searchText': symbol,
                'fields': 'instrumentId,internalSymbolFull,displayname',
                'pageSize': 5,
            },
        )
        # Response wraps results under various keys depending on API version
        instruments = (
            result if isinstance(result, list)
            else result.get('instruments')
            or result.get('data')
            or []
        )
        return instruments

    # ------------------------------------------------------------------
    # Execution — Step 5 placeholders (correct endpoints from SKILL.md)
    # ------------------------------------------------------------------

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
        Open a demo market order by amount.

        Endpoint: POST /api/v1/trading/execution/demo/market-open-orders/by-amount
        Body uses PascalCase per official API convention.
        Note: StopLossRate / TakeProfitRate are absolute price levels, not distances.

        Implementation deferred to Step 5 (signal bridge). Raises NotImplementedError
        until Step 5 is complete to prevent accidental live calls.
        """
        raise NotImplementedError(
            "place_market_order() is a Step 5 deliverable. "
            "Implement in execution/order_router.py after empirical demo history test."
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
        Body: { "InstrumentId": ..., "UnitsToDeduct": null }  ← null = full close.

        Implementation deferred to Step 5.
        """
        raise NotImplementedError(
            "close_position() is a Step 5 deliverable. "
            "Implement in execution/order_router.py after empirical demo history test."
        )
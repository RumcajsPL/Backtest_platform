"""
OrderRouter — signal bridge between strategy signals and eToro demo execution.

Responsibilities:
  - Resolve symbol → instrumentId via InstrumentResolver
  - Gate all execution on is_trading_hours() (08:00–22:00 CET/CEST)
  - Delegate HTTP calls exclusively to EToroClient (never implement HTTP here)
  - Return positionId (str) on open, bool on close

Two-step open flow (CRITICAL — positionID is not in the open-order response):
  Step 1: client.place_market_order()  → orderForOpen.orderID
  Step 2: client.get_order_info(orderID) polling until statusID == 1 (Executed)
          → positions[0].positionID

  Fast-fill shortcut (empirically confirmed 2026-03-17):
  If orderForOpen.statusID == 1 in the POST response the order already executed
  before the first poll.  In that case _poll_for_position_id is skipped and
  positionID is resolved via _find_position_in_portfolio(orderID) instead,
  because the /demo/orders/{id} endpoint may return 404 or statusID=3 for
  several seconds after a fast-fill even though the position is live.

Usage:
    router = OrderRouter(client=EToroClient(), resolver=InstrumentResolver(map_path))

    position_id = router.open_position(
        symbol='DAX',
        direction='BUY',
        amount=500.0,
        leverage=5,
        stop_loss_rate=22000.0,
        take_profit_rate=22500.0,
    )

    success = router.close_position(
        position_id=position_id,
        instrument_id=32,
    )
"""
import time
from typing import Optional

import requests
from loguru import logger

from src.broker_support.client.client import EToroClient
from src.broker_support.enrichment.instrument_resolver import InstrumentResolver
from src.broker_support.utils.time_utils import is_trading_hours

# Order statusID values from API
_STATUS_PENDING   = 0
_STATUS_EXECUTED  = 1
_STATUS_CANCELLED = 2
_STATUS_REJECTED  = 3
_STATUS_PARTIAL   = 4

# Two-step open: polling config
_ORDER_POLL_INTERVAL_S   = 2.0   # seconds between status checks (increased from 1s)
_ORDER_POLL_MAX_ATTEMPTS = 15    # total wait ≤ 30s before giving up
_ORDER_POLL_404_GRACE    = 3     # treat 404 as transient for this many attempts

# Portfolio fallback: config for _find_position_in_portfolio
_PORTFOLIO_POLL_INTERVAL_S   = 2.0
_PORTFOLIO_POLL_MAX_ATTEMPTS = 10   # total wait ≤ 20s


class OutsideTradingHoursError(Exception):
    """Raised when an execution is attempted outside trading hours."""


class OrderExecutionError(Exception):
    """Raised when an order is rejected, cancelled, or times out."""


class OrderRouter:
    """
    Routes open/close signals to the eToro demo execution API.

    All calls are gated on is_trading_hours(). Callers that need to suppress
    the hours guard (e.g. tests, manual scripts) should mock is_trading_hours
    rather than adding a bypass flag here.
    """

    def __init__(
        self,
        client: EToroClient,
        resolver: InstrumentResolver,
    ) -> None:
        self._client = client
        self._resolver = resolver

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def open_position(
        self,
        symbol: str,
        direction: str,
        amount: float,
        leverage: int = 1,
        stop_loss_rate: Optional[float] = None,
        take_profit_rate: Optional[float] = None,
    ) -> str:
        """
        Open a demo market position. Uses two-step flow to get positionID.

        Step 1: place_market_order() → orderID
        Step 2a (normal):     poll get_order_info(orderID) until Executed → positionID
        Step 2b (fast-fill):  if POST response already has statusID==1, skip polling
                              and resolve positionID via portfolio scan instead.

        Args:
            symbol:           Instrument key from instrument_map.yaml, e.g. 'GER40'.
            direction:        'BUY' or 'SELL' (case-insensitive).
            amount:           USD amount to invest.
            leverage:         Leverage multiplier (default 1).
            stop_loss_rate:   Absolute stop-loss price level (optional).
            take_profit_rate: Absolute take-profit price level (optional).

        Returns:
            positionID as str.

        Raises:
            OutsideTradingHoursError: If called outside 08:00–22:00 CET/CEST.
            ValueError: If direction is not 'BUY'/'SELL', or symbol not resolved.
            OrderExecutionError: If order is rejected, cancelled, or times out.
        """
        self._assert_trading_hours()

        direction_upper = direction.upper()
        if direction_upper not in ('BUY', 'SELL'):
            raise ValueError(f"direction must be 'BUY' or 'SELL', got {direction!r}")

        instrument_id = self._resolve_symbol(symbol)
        is_buy = direction_upper == 'BUY'

        logger.info(
            f"OrderRouter.open_position: symbol={symbol}, direction={direction_upper}, "
            f"instrumentId={instrument_id}, amount={amount}, leverage={leverage}, "
            f"sl={stop_loss_rate}, tp={take_profit_rate}"
        )

        # Step 1 — place the order, get orderID
        response = self._client.place_market_order(
            instrument_id=instrument_id,
            is_buy=is_buy,
            amount=amount,
            leverage=leverage,
            stop_loss_rate=stop_loss_rate,
            take_profit_rate=take_profit_rate,
        )

        order_for_open = response.get('orderForOpen', {})
        order_id = order_for_open.get('orderID')
        if not order_id:
            raise OrderExecutionError(
                f"No orderID in place_market_order response. "
                f"Response keys: {list(response.keys())}"
            )

        # ── Fast-fill detection (empirical: 2026-03-17) ──────────────────
        # The POST response can already carry statusID=1 when the market fills
        # the order instantly.  In that case the /demo/orders/{id} endpoint
        # returns 404 or statusID=3 for several seconds — do NOT poll it.
        # Resolve positionID from the portfolio instead.
        post_status_id = order_for_open.get('statusID')
        if post_status_id == _STATUS_EXECUTED:
            logger.info(
                f"Order {order_id}: statusID=1 already set in POST response "
                f"(fast-fill). Skipping order-info polling — resolving positionID "
                f"from portfolio."
            )
            position_id = self._find_position_in_portfolio(order_id)
            logger.info(f"Position opened (fast-fill): positionID={position_id}")
            return position_id

        logger.info(f"Order placed: orderID={order_id}. Polling for positionID …")

        # Step 2 — poll until executed, extract positionID
        position_id = self._poll_for_position_id(order_id)
        logger.info(f"Position opened: positionID={position_id}")
        return position_id

    def close_position(
        self,
        position_id: str,
        instrument_id: int,
        units_to_deduct: Optional[float] = None,
    ) -> bool:
        """
        Close a demo position (full close by default).

        Args:
            position_id:     positionID as str (coerced to int for API call).
            instrument_id:   eToro instrumentID of the position.
            units_to_deduct: Units to close. None = full close (default).

        Returns:
            True on success.

        Raises:
            OutsideTradingHoursError: If called outside 08:00–22:00 CET/CEST.
        """
        self._assert_trading_hours()

        logger.info(
            f"OrderRouter.close_position: positionId={position_id}, "
            f"instrumentId={instrument_id}, units_to_deduct={units_to_deduct}"
        )

        self._client.close_position(
            position_id=int(position_id),
            instrument_id=instrument_id,
            units_to_deduct=units_to_deduct,
        )

        logger.info(f"Position closed: positionId={position_id}")
        return True

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _assert_trading_hours(self) -> None:
        """Raise OutsideTradingHoursError if outside 08:00–22:00 CET/CEST."""
        if not is_trading_hours():
            raise OutsideTradingHoursError(
                "Execution blocked: current time is outside trading hours "
                "(08:00–22:00 Europe/Berlin)."
            )

    def _resolve_symbol(self, symbol: str) -> int:
        """
        Resolve symbol to instrumentId. Raises ValueError if not found.
        Uses InstrumentResolver (YAML primary, API fallback).
        """
        instrument_id = self._resolver.instrument_id(symbol)
        if instrument_id is None:
            raise ValueError(
                f"Cannot resolve symbol {symbol!r} to an instrumentId. "
                f"Add it to instrument_map.yaml or verify the symbol spelling."
            )
        return instrument_id

    def _poll_for_position_id(self, order_id: int) -> str:
        """
        Poll get_order_info until the order is Executed, then return positionID.

        statusID: 0=Pending, 1=Executed, 2=Cancelled, 3=Rejected, 4=Partial.
        Polls every _ORDER_POLL_INTERVAL_S seconds up to _ORDER_POLL_MAX_ATTEMPTS.

        404 responses are treated as transient for the first _ORDER_POLL_404_GRACE
        attempts — the order-info endpoint can lag behind execution by several
        seconds even for successful orders (empirically confirmed 2026-03-17).

        Args:
            order_id: orderID from place_market_order response.

        Returns:
            positionID as str (from positions[0].positionID).

        Raises:
            OrderExecutionError: If rejected, cancelled, timed out, or no positions.
        """
        status_id = None

        for attempt in range(1, _ORDER_POLL_MAX_ATTEMPTS + 1):
            try:
                order_info = self._client.get_order_info(order_id)
            except requests.exceptions.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 404:
                    if attempt <= _ORDER_POLL_404_GRACE:
                        logger.debug(
                            f"Order poll {attempt}/{_ORDER_POLL_MAX_ATTEMPTS}: "
                            f"orderID={order_id} → 404 (transient, grace attempt "
                            f"{attempt}/{_ORDER_POLL_404_GRACE}). "
                            f"Retrying in {_ORDER_POLL_INTERVAL_S}s …"
                        )
                        time.sleep(_ORDER_POLL_INTERVAL_S)
                        continue
                    # Beyond grace period — fall back to portfolio
                    logger.warning(
                        f"Order poll {attempt}/{_ORDER_POLL_MAX_ATTEMPTS}: "
                        f"orderID={order_id} → 404 beyond grace period. "
                        f"Falling back to portfolio scan."
                    )
                    return self._find_position_in_portfolio(order_id)
                raise  # non-404 HTTP errors propagate

            status_id = order_info.get('statusID')

            logger.debug(
                f"Order poll {attempt}/{_ORDER_POLL_MAX_ATTEMPTS}: "
                f"orderID={order_id}, statusID={status_id}"
            )

            if status_id == _STATUS_EXECUTED:
                positions = order_info.get('positions', [])
                if not positions:
                    raise OrderExecutionError(
                        f"Order {order_id} is Executed but positions[] is empty. "
                        f"Cannot extract positionID."
                    )
                position_id = positions[0].get('positionID')
                if position_id is None:
                    raise OrderExecutionError(
                        f"Order {order_id} positions[0] has no positionID field. "
                        f"Keys: {list(positions[0].keys())}"
                    )
                return str(position_id)

            if status_id == _STATUS_REJECTED:
                # Could be stale state on a fast-fill — try portfolio before giving up
                logger.warning(
                    f"Order poll {attempt}/{_ORDER_POLL_MAX_ATTEMPTS}: "
                    f"orderID={order_id} → statusID=3 (REJECTED). "
                    f"Checking portfolio before raising — may be stale API state."
                )
                try:
                    return self._find_position_in_portfolio(order_id)
                except OrderExecutionError:
                    pass  # not in portfolio either — it really is rejected
                error_msg = order_info.get('errorMessage', 'no error message')
                raise OrderExecutionError(
                    f"Order {order_id} was REJECTED. errorMessage: {error_msg}"
                )

            if status_id == _STATUS_CANCELLED:
                raise OrderExecutionError(
                    f"Order {order_id} was CANCELLED before execution."
                )

            if status_id == _STATUS_PARTIAL:
                # Partial fill — use first available position if present
                positions = order_info.get('positions', [])
                if positions:
                    position_id = positions[0].get('positionID')
                    if position_id is not None:
                        logger.warning(
                            f"Order {order_id} partially filled. "
                            f"Using positionID={position_id}."
                        )
                        return str(position_id)

            # Status still Pending — wait and retry
            time.sleep(_ORDER_POLL_INTERVAL_S)

        raise OrderExecutionError(
            f"Order {order_id} did not reach Executed status after "
            f"{_ORDER_POLL_MAX_ATTEMPTS} polls "
            f"({_ORDER_POLL_MAX_ATTEMPTS * _ORDER_POLL_INTERVAL_S:.0f}s). "
            f"Last statusID={status_id}."
        )

    def _find_position_in_portfolio(self, order_id: int) -> str:
        """
        Resolve positionID by scanning the demo portfolio for a position whose
        orderID matches the given order_id.

        Used as fallback when:
          - The POST response already shows statusID=1 (fast-fill), so polling
            /demo/orders/{id} is skipped entirely.
          - /demo/orders/{id} returns 404 beyond the grace period.
          - /demo/orders/{id} returns statusID=3 (REJECTED) which may be stale.

        Polls the portfolio up to _PORTFOLIO_POLL_MAX_ATTEMPTS times with
        _PORTFOLIO_POLL_INTERVAL_S between attempts.  A short delay before the
        first attempt is intentional — the portfolio write can lag the execution
        endpoint by ~1s.

        Args:
            order_id: The orderID from the place_market_order response.

        Returns:
            positionID as str.

        Raises:
            OrderExecutionError: If the position is not found within the timeout.
        """
        logger.info(
            f"Portfolio scan: looking for position with orderID={order_id} "
            f"(max {_PORTFOLIO_POLL_MAX_ATTEMPTS} attempts, "
            f"{_PORTFOLIO_POLL_INTERVAL_S}s interval) …"
        )

        for attempt in range(1, _PORTFOLIO_POLL_MAX_ATTEMPTS + 1):
            # Small initial delay — portfolio write lags execution by ~1s
            time.sleep(_PORTFOLIO_POLL_INTERVAL_S)

            try:
                portfolio = self._client._make_request(
                    "GET", "api/v1/trading/info/demo/portfolio"
                )
            except Exception as exc:
                logger.warning(
                    f"Portfolio scan attempt {attempt}: fetch failed: {exc}. "
                    f"Retrying …"
                )
                continue

            positions = (
                portfolio.get("clientPortfolio", {}).get("positions", [])
            )

            for pos in positions:
                if pos.get("orderID") == order_id:
                    position_id = pos.get("positionID")
                    if position_id is not None:
                        logger.info(
                            f"Portfolio scan: found positionID={position_id} "
                            f"for orderID={order_id} (attempt {attempt})."
                        )
                        return str(position_id)

            logger.debug(
                f"Portfolio scan attempt {attempt}/{_PORTFOLIO_POLL_MAX_ATTEMPTS}: "
                f"orderID={order_id} not yet visible in {len(positions)} position(s)."
            )

        raise OrderExecutionError(
            f"Portfolio scan: positionID for orderID={order_id} not found after "
            f"{_PORTFOLIO_POLL_MAX_ATTEMPTS} attempts "
            f"({_PORTFOLIO_POLL_MAX_ATTEMPTS * _PORTFOLIO_POLL_INTERVAL_S:.0f}s). "
            f"Position may not have opened. Check portfolio manually."
        )
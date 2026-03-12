"""
Unit tests for OrderRouter (Step 5 — signal bridge).

All tests use mock objects — no live API calls.
EToroClient and InstrumentResolver are replaced with MagicMock instances
so tests remain isolated from network, credentials, and instrument_map.yaml.

Two-step open flow under test:
  Step 1: client.place_market_order()  → { 'orderForOpen': { 'orderID': int } }
  Step 2: client.get_order_info(orderID) polling until statusID == 1
          → { 'statusID': 1, 'positions': [{ 'positionID': int, ... }] }

Run:
    pytest tests/broker_support/test_order_router.py -v
"""
from unittest.mock import MagicMock, patch

import pytest

from src.broker_support.execution.order_router import (
    OrderExecutionError,
    OrderRouter,
    OutsideTradingHoursError,
)

TRADING_HOURS_PATH = 'src.broker_support.execution.order_router.is_trading_hours'
SLEEP_PATH = 'src.broker_support.execution.order_router.time.sleep'

# ---------------------------------------------------------------------------
# Response builders
# ---------------------------------------------------------------------------

def _open_response(order_id: int = 13902598) -> dict:
    """Minimal valid place_market_order response."""
    return {'orderForOpen': {'orderID': order_id}, 'token': 'abc'}


def _order_info_executed(position_id: int = 3464232739, order_id: int = 13902598) -> dict:
    """Minimal valid get_order_info response — fully executed."""
    return {
        'orderID': order_id,
        'statusID': 1,
        'positions': [
            {'positionID': position_id, 'isOpen': True, 'rate': 23556.77, 'units': 0.044}
        ],
    }


def _order_info_pending(order_id: int = 13902598) -> dict:
    return {'orderID': order_id, 'statusID': 0, 'positions': []}


def _order_info_rejected(order_id: int = 13902598) -> dict:
    return {
        'orderID': order_id, 'statusID': 3, 'positions': [],
        'errorMessage': 'Insufficient funds',
    }


def _order_info_cancelled(order_id: int = 13902598) -> dict:
    return {'orderID': order_id, 'statusID': 2, 'positions': []}


def _order_info_partial(position_id: int = 111) -> dict:
    return {
        'orderID': 13902598, 'statusID': 4,
        'positions': [{'positionID': position_id, 'isOpen': True}],
    }


# ---------------------------------------------------------------------------
# Fixture factory
# ---------------------------------------------------------------------------

def _make_router(
    instrument_id: int = 32,
    open_response: dict | None = None,
    order_info_response: dict | None = None,
    close_response: dict | None = None,
) -> tuple[OrderRouter, MagicMock, MagicMock]:
    """
    Return (router, mock_client, mock_resolver) pre-wired for happy-path tests.

    Defaults:
      resolver.instrument_id('DAX') → 32
      client.place_market_order()   → _open_response()
      client.get_order_info()       → _order_info_executed()
      client.close_position()       → {}
    """
    client = MagicMock()
    resolver = MagicMock()
    resolver.instrument_id.return_value = instrument_id
    client.place_market_order.return_value = open_response or _open_response()
    client.get_order_info.return_value = order_info_response or _order_info_executed()
    client.close_position.return_value = close_response or {}
    return OrderRouter(client=client, resolver=resolver), client, resolver


# ===========================================================================
# open_position — happy path
# ===========================================================================

class TestOpenPositionHappyPath:

    def test_returns_position_id_as_str(self):
        router, _, _ = _make_router(order_info_response=_order_info_executed(position_id=3464232739))
        with patch(TRADING_HOURS_PATH, return_value=True):
            result = router.open_position('DAX', 'BUY', amount=500.0, leverage=5)
        assert result == '3464232739'

    def test_position_id_coerced_to_str(self):
        """positionID from API is int — must be returned as str."""
        router, _, _ = _make_router(order_info_response=_order_info_executed(position_id=99999))
        with patch(TRADING_HOURS_PATH, return_value=True):
            result = router.open_position('DAX', 'BUY', amount=100.0)
        assert isinstance(result, str)
        assert result == '99999'

    def test_calls_place_market_order_with_correct_args(self):
        router, client, _ = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=True):
            router.open_position(
                'DAX', 'BUY', amount=500.0, leverage=5,
                stop_loss_rate=22000.0, take_profit_rate=22500.0,
            )
        client.place_market_order.assert_called_once_with(
            instrument_id=32,
            is_buy=True,
            amount=500.0,
            leverage=5,
            stop_loss_rate=22000.0,
            take_profit_rate=22500.0,
        )

    def test_calls_get_order_info_with_order_id_from_response(self):
        router, client, _ = _make_router(open_response=_open_response(order_id=335612901))
        with patch(TRADING_HOURS_PATH, return_value=True):
            router.open_position('DAX', 'BUY', amount=100.0)
        client.get_order_info.assert_called_with(335612901)

    def test_sell_passes_is_buy_false(self):
        router, client, _ = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=True):
            router.open_position('DAX', 'SELL', amount=300.0)
        _, kwargs = client.place_market_order.call_args
        assert kwargs['is_buy'] is False

    def test_direction_case_insensitive(self):
        router, client, _ = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=True):
            router.open_position('DAX', 'buy', amount=100.0)
        _, kwargs = client.place_market_order.call_args
        assert kwargs['is_buy'] is True

    def test_optional_sl_tp_none_by_default(self):
        router, client, _ = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=True):
            router.open_position('DAX', 'BUY', amount=200.0)
        _, kwargs = client.place_market_order.call_args
        assert kwargs['stop_loss_rate'] is None
        assert kwargs['take_profit_rate'] is None

    def test_resolver_called_with_symbol(self):
        router, _, resolver = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=True):
            router.open_position('DAX', 'BUY', amount=100.0)
        resolver.instrument_id.assert_called_once_with('DAX')

    def test_pending_then_executed_polls_twice(self):
        """Order is Pending on first poll, Executed on second — should succeed."""
        client = MagicMock()
        resolver = MagicMock()
        resolver.instrument_id.return_value = 32
        client.place_market_order.return_value = _open_response(order_id=1)
        client.get_order_info.side_effect = [
            _order_info_pending(order_id=1),
            _order_info_executed(position_id=555, order_id=1),
        ]
        router = OrderRouter(client=client, resolver=resolver)
        with patch(TRADING_HOURS_PATH, return_value=True):
            with patch(SLEEP_PATH):
                result = router.open_position('DAX', 'BUY', amount=100.0)
        assert result == '555'
        assert client.get_order_info.call_count == 2

    def test_partial_fill_returns_first_position_id(self):
        """Partial fill with positions present — use first positionID."""
        router, _, _ = _make_router(order_info_response=_order_info_partial(position_id=777))
        with patch(TRADING_HOURS_PATH, return_value=True):
            with patch(SLEEP_PATH):
                result = router.open_position('DAX', 'BUY', amount=100.0)
        assert result == '777'


# ===========================================================================
# open_position — error cases
# ===========================================================================

class TestOpenPositionErrors:

    def test_raises_outside_trading_hours(self):
        router, _, _ = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=False):
            with pytest.raises(OutsideTradingHoursError):
                router.open_position('DAX', 'BUY', amount=500.0)

    def test_no_api_call_when_outside_hours(self):
        router, client, _ = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=False):
            with pytest.raises(OutsideTradingHoursError):
                router.open_position('DAX', 'BUY', amount=500.0)
        client.place_market_order.assert_not_called()
        client.get_order_info.assert_not_called()

    def test_raises_on_invalid_direction(self):
        router, _, _ = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=True):
            with pytest.raises(ValueError, match="direction must be"):
                router.open_position('DAX', 'HOLD', amount=100.0)

    def test_no_api_call_on_invalid_direction(self):
        router, client, _ = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=True):
            with pytest.raises(ValueError):
                router.open_position('DAX', 'LONG', amount=100.0)
        client.place_market_order.assert_not_called()

    def test_raises_on_unresolvable_symbol(self):
        router, _, resolver = _make_router()
        resolver.instrument_id.return_value = None
        with patch(TRADING_HOURS_PATH, return_value=True):
            with pytest.raises(ValueError, match="Cannot resolve symbol"):
                router.open_position('UNKNOWN', 'BUY', amount=100.0)

    def test_raises_when_order_id_missing_from_open_response(self):
        router, _, _ = _make_router(open_response={'orderForOpen': {}})
        with patch(TRADING_HOURS_PATH, return_value=True):
            with pytest.raises(OrderExecutionError, match="orderID"):
                router.open_position('DAX', 'BUY', amount=100.0)

    def test_raises_when_orderForOpen_key_missing(self):
        router, _, _ = _make_router(open_response={'token': 'abc'})
        with patch(TRADING_HOURS_PATH, return_value=True):
            with pytest.raises(OrderExecutionError, match="orderID"):
                router.open_position('DAX', 'BUY', amount=100.0)

    def test_raises_when_order_rejected(self):
        router, _, _ = _make_router(order_info_response=_order_info_rejected())
        with patch(TRADING_HOURS_PATH, return_value=True):
            with pytest.raises(OrderExecutionError, match="REJECTED"):
                router.open_position('DAX', 'BUY', amount=100.0)

    def test_rejection_error_message_included(self):
        router, _, _ = _make_router(order_info_response=_order_info_rejected())
        with patch(TRADING_HOURS_PATH, return_value=True):
            with pytest.raises(OrderExecutionError, match="Insufficient funds"):
                router.open_position('DAX', 'BUY', amount=100.0)

    def test_raises_when_order_cancelled(self):
        router, _, _ = _make_router(order_info_response=_order_info_cancelled())
        with patch(TRADING_HOURS_PATH, return_value=True):
            with pytest.raises(OrderExecutionError, match="CANCELLED"):
                router.open_position('DAX', 'BUY', amount=100.0)

    def test_raises_when_executed_but_positions_empty(self):
        info = {'orderID': 1, 'statusID': 1, 'positions': []}
        router, _, _ = _make_router(order_info_response=info)
        with patch(TRADING_HOURS_PATH, return_value=True):
            with pytest.raises(OrderExecutionError, match="positions\\[\\] is empty"):
                router.open_position('DAX', 'BUY', amount=100.0)

    def test_raises_on_poll_timeout(self):
        """All polls return Pending — should time out and raise."""
        client = MagicMock()
        resolver = MagicMock()
        resolver.instrument_id.return_value = 32
        client.place_market_order.return_value = _open_response(order_id=1)
        client.get_order_info.return_value = _order_info_pending(order_id=1)
        router = OrderRouter(client=client, resolver=resolver)
        with patch(TRADING_HOURS_PATH, return_value=True):
            with patch(SLEEP_PATH):
                with pytest.raises(OrderExecutionError, match="did not reach Executed"):
                    router.open_position('DAX', 'BUY', amount=100.0)

    def test_poll_timeout_calls_sleep_each_iteration(self):
        """sleep() must be called once per pending poll."""
        client = MagicMock()
        resolver = MagicMock()
        resolver.instrument_id.return_value = 32
        client.place_market_order.return_value = _open_response(order_id=1)
        client.get_order_info.return_value = _order_info_pending(order_id=1)
        router = OrderRouter(client=client, resolver=resolver)
        with patch(TRADING_HOURS_PATH, return_value=True):
            with patch(SLEEP_PATH) as mock_sleep:
                with pytest.raises(OrderExecutionError):
                    router.open_position('DAX', 'BUY', amount=100.0)
        # 15 attempts, sleep called after each pending result
        assert mock_sleep.call_count == 15


# ===========================================================================
# close_position — happy path
# ===========================================================================

class TestClosePositionHappyPath:

    def test_returns_true_on_success(self):
        router, _, _ = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=True):
            result = router.close_position('3464232739', instrument_id=32)
        assert result is True

    def test_calls_client_close_with_correct_args(self):
        router, client, _ = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=True):
            router.close_position('3464232739', instrument_id=32)
        client.close_position.assert_called_once_with(
            position_id=3464232739,
            instrument_id=32,
            units_to_deduct=None,
        )

    def test_position_id_str_coerced_to_int(self):
        router, client, _ = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=True):
            router.close_position('111222333', instrument_id=32)
        call_kwargs = client.close_position.call_args[1]
        assert call_kwargs['position_id'] == 111222333
        assert isinstance(call_kwargs['position_id'], int)

    def test_partial_close_passes_units_to_deduct(self):
        router, client, _ = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=True):
            router.close_position('123', instrument_id=32, units_to_deduct=0.01)
        call_kwargs = client.close_position.call_args[1]
        assert call_kwargs['units_to_deduct'] == 0.01

    def test_full_close_passes_none_units(self):
        router, client, _ = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=True):
            router.close_position('123', instrument_id=32)
        call_kwargs = client.close_position.call_args[1]
        assert call_kwargs['units_to_deduct'] is None


# ===========================================================================
# close_position — error cases
# ===========================================================================

class TestClosePositionErrors:

    def test_raises_outside_trading_hours(self):
        router, _, _ = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=False):
            with pytest.raises(OutsideTradingHoursError):
                router.close_position('123', instrument_id=32)

    def test_no_api_call_when_outside_hours(self):
        router, client, _ = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=False):
            with pytest.raises(OutsideTradingHoursError):
                router.close_position('123', instrument_id=32)
        client.close_position.assert_not_called()

    def test_client_exception_propagates(self):
        router, client, _ = _make_router()
        client.close_position.side_effect = RuntimeError("API error")
        with patch(TRADING_HOURS_PATH, return_value=True):
            with pytest.raises(RuntimeError, match="API error"):
                router.close_position('123', instrument_id=32)


# ===========================================================================
# Error type contracts
# ===========================================================================

class TestErrorTypes:

    def test_outside_trading_hours_error_is_exception(self):
        assert issubclass(OutsideTradingHoursError, Exception)

    def test_order_execution_error_is_exception(self):
        assert issubclass(OrderExecutionError, Exception)

    def test_error_message_contains_hours(self):
        router, _, _ = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=False):
            with pytest.raises(OutsideTradingHoursError, match="08:00"):
                router.open_position('DAX', 'BUY', amount=100.0)
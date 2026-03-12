"""
Unit tests for OrderRouter (Step 5 — signal bridge).

All tests use mock objects — no live API calls.
EToroClient and InstrumentResolver are replaced with MagicMock instances
so tests remain isolated from network, credentials, and instrument_map.yaml.

Run:
    pytest tests/broker_support/test_order_router.py -v
"""
from unittest.mock import MagicMock, patch

import pytest

from src.broker_support.execution.order_router import OrderRouter, OutsideTradingHoursError


# ---------------------------------------------------------------------------
# Helpers / shared fixtures
# ---------------------------------------------------------------------------

def _make_router(
    instrument_id: int = 32,
    place_response: dict | None = None,
    close_response: dict | None = None,
) -> tuple[OrderRouter, MagicMock, MagicMock]:
    """
    Return (router, mock_client, mock_resolver) pre-wired for happy-path tests.

    Defaults:
      - resolver.instrument_id('DAX') → 32
      - client.place_market_order(...)  → {'positionId': 987654321}
      - client.close_position(...)      → {}
    """
    client = MagicMock()
    resolver = MagicMock()

    resolver.instrument_id.return_value = instrument_id
    client.place_market_order.return_value = (
        place_response if place_response is not None else {'positionId': 987654321}
    )
    client.close_position.return_value = (
        close_response if close_response is not None else {}
    )

    router = OrderRouter(client=client, resolver=resolver)
    return router, client, resolver


TRADING_HOURS_PATH = 'src.broker_support.execution.order_router.is_trading_hours'


# ---------------------------------------------------------------------------
# open_position — happy path
# ---------------------------------------------------------------------------

class TestOpenPositionHappyPath:

    def test_returns_position_id_as_str(self):
        router, client, _ = _make_router(place_response={'positionId': 123456})
        with patch(TRADING_HOURS_PATH, return_value=True):
            result = router.open_position('DAX', 'BUY', amount=500.0, leverage=5)
        assert result == '123456'

    def test_calls_place_market_order_with_correct_args(self):
        router, client, resolver = _make_router()
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

    def test_sell_direction_passes_is_buy_false(self):
        router, client, _ = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=True):
            router.open_position('DAX', 'SELL', amount=300.0)
        _, kwargs = client.place_market_order.call_args
        assert kwargs['is_buy'] is False

    def test_direction_is_case_insensitive(self):
        router, client, _ = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=True):
            router.open_position('DAX', 'buy', amount=100.0)
        _, kwargs = client.place_market_order.call_args
        assert kwargs['is_buy'] is True

    def test_optional_sl_tp_omitted_when_none(self):
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

    def test_position_id_coerced_to_str(self):
        """API may return positionId as int — must come back as str."""
        router, _, _ = _make_router(place_response={'positionId': 99999})
        with patch(TRADING_HOURS_PATH, return_value=True):
            result = router.open_position('DAX', 'BUY', amount=100.0)
        assert isinstance(result, str)
        assert result == '99999'

    def test_alternate_response_key_PositionID(self):
        """Handle PascalCase PositionID variant."""
        router, _, _ = _make_router(place_response={'PositionID': 111})
        with patch(TRADING_HOURS_PATH, return_value=True):
            result = router.open_position('DAX', 'BUY', amount=100.0)
        assert result == '111'

    def test_alternate_response_key_position_id(self):
        """Handle snake_case position_id variant."""
        router, _, _ = _make_router(place_response={'position_id': 222})
        with patch(TRADING_HOURS_PATH, return_value=True):
            result = router.open_position('DAX', 'BUY', amount=100.0)
        assert result == '222'


# ---------------------------------------------------------------------------
# open_position — error cases
# ---------------------------------------------------------------------------

class TestOpenPositionErrors:

    def test_raises_outside_trading_hours(self):
        router, _, _ = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=False):
            with pytest.raises(OutsideTradingHoursError):
                router.open_position('DAX', 'BUY', amount=500.0)

    def test_raises_on_invalid_direction(self):
        router, _, _ = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=True):
            with pytest.raises(ValueError, match="direction must be"):
                router.open_position('DAX', 'HOLD', amount=100.0)

    def test_raises_on_unresolvable_symbol(self):
        router, _, resolver = _make_router()
        resolver.instrument_id.return_value = None
        with patch(TRADING_HOURS_PATH, return_value=True):
            with pytest.raises(ValueError, match="Cannot resolve symbol"):
                router.open_position('UNKNOWN', 'BUY', amount=100.0)

    def test_raises_when_position_id_missing_from_response(self):
        router, _, _ = _make_router(place_response={'someOtherKey': 'value'})
        with patch(TRADING_HOURS_PATH, return_value=True):
            with pytest.raises(KeyError, match="positionId"):
                router.open_position('DAX', 'BUY', amount=100.0)

    def test_no_api_call_when_outside_hours(self):
        """client must not be called if hours guard fires."""
        router, client, _ = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=False):
            with pytest.raises(OutsideTradingHoursError):
                router.open_position('DAX', 'BUY', amount=500.0)
        client.place_market_order.assert_not_called()

    def test_no_api_call_on_invalid_direction(self):
        router, client, _ = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=True):
            with pytest.raises(ValueError):
                router.open_position('DAX', 'LONG', amount=100.0)
        client.place_market_order.assert_not_called()


# ---------------------------------------------------------------------------
# close_position — happy path
# ---------------------------------------------------------------------------

class TestClosePositionHappyPath:

    def test_returns_true_on_success(self):
        router, _, _ = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=True):
            result = router.close_position('987654321', instrument_id=32)
        assert result is True

    def test_calls_client_close_with_correct_args(self):
        router, client, _ = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=True):
            router.close_position('987654321', instrument_id=32)
        client.close_position.assert_called_once_with(
            position_id=987654321,
            instrument_id=32,
            units_to_deduct=None,
        )

    def test_position_id_str_coerced_to_int_for_client(self):
        """position_id comes in as str; client expects int."""
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


# ---------------------------------------------------------------------------
# close_position — error cases
# ---------------------------------------------------------------------------

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
        """Errors from client (e.g. HTTP 4xx) must bubble up uncaught."""
        router, client, _ = _make_router()
        client.close_position.side_effect = RuntimeError("API error")
        with patch(TRADING_HOURS_PATH, return_value=True):
            with pytest.raises(RuntimeError, match="API error"):
                router.close_position('123', instrument_id=32)


# ---------------------------------------------------------------------------
# OutsideTradingHoursError
# ---------------------------------------------------------------------------

class TestOutsideTradingHoursError:

    def test_is_exception_subclass(self):
        assert issubclass(OutsideTradingHoursError, Exception)

    def test_error_message_contains_hours(self):
        router, _, _ = _make_router()
        with patch(TRADING_HOURS_PATH, return_value=False):
            with pytest.raises(OutsideTradingHoursError, match="08:00"):
                router.open_position('DAX', 'BUY', amount=100.0)
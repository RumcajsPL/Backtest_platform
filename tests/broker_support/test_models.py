"""
Unit tests for the Trade Pydantic model.

Run:  pytest tests/broker_support/test_models.py -v
"""
import pytest
from datetime import datetime, timezone
from src.broker_support.models.trade import Trade


# Minimal valid payload matching eToro /trade/history response schema
VALID_PAYLOAD = {
    'positionId': 2150896073,
    'instrumentId': 1002,
    'isBuy': True,
    'openTimestamp': '2026-01-15T09:30:00Z',
    'closeTimestamp': '2026-01-15T16:45:00Z',
    'openRate': 21800.5,
    'closeRate': 22100.0,
    'investment': 1000.0,
    'units': 0.045,
    'netProfit': 63.42,
    'fees': 1.5,
    'leverage': 2,
    'stopLossRate': 21000.0,
    'takeProfitRate': 23000.0,
    'trailingStopLoss': False,
}


class TestTradeModel:

    def test_valid_payload_parses(self):
        trade = Trade.model_validate(VALID_PAYLOAD)
        assert trade.trade_id == str(2150896073)
        assert trade.instrument_id == 1002
        assert trade.direction == 'BUY'
        assert trade.entry_price == 21800.5
        assert trade.exit_price == 22100.0
        assert trade.profit_loss == 63.42
        assert trade.fees == 1.5
        assert trade.leverage == 2
        assert trade.sl_rate == 21000.0
        assert trade.tp_rate == 23000.0

    def test_sell_direction_derived(self):
        payload = {**VALID_PAYLOAD, 'isBuy': False}
        trade = Trade.model_validate(payload)
        assert trade.direction == 'SELL'

    def test_positionId_as_string(self):
        """trade_id must always be a string regardless of API int type."""
        trade = Trade.model_validate(VALID_PAYLOAD)
        assert isinstance(trade.trade_id, str)

    def test_optional_fields_default(self):
        """instrument is None until InstrumentResolver populates it."""
        trade = Trade.model_validate(VALID_PAYLOAD)
        assert trade.instrument is None

    def test_fees_defaults_to_zero(self):
        payload = {k: v for k, v in VALID_PAYLOAD.items() if k != 'fees'}
        trade = Trade.model_validate(payload)
        assert trade.fees == 0.0

    def test_missing_required_field_raises(self):
        payload = {k: v for k, v in VALID_PAYLOAD.items() if k != 'positionId'}
        with pytest.raises(Exception):
            Trade.model_validate(payload)

    def test_populate_by_name(self):
        """Construction by Python field name (not alias) must work."""
        trade = Trade(
            trade_id='999',
            instrument_id=1002,
            is_buy=True,
            open_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            close_time=datetime(2026, 1, 2, tzinfo=timezone.utc),
            entry_price=100.0,
            exit_price=110.0,
            volume=500.0,
            profit_loss=10.0,
        )
        assert trade.trade_id == '999'
        assert trade.direction == 'BUY'
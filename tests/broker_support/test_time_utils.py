"""
Unit tests for time_utils.

Run:  pytest tests/broker_support/test_time_utils.py -v
"""
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from src.broker_support.utils.time_utils import is_trading_hours, seconds_until_open

_BERLIN = ZoneInfo("Europe/Berlin")


def _dt(hour: int, minute: int = 0) -> datetime:
    """Helper — build a Berlin-tz datetime for today."""
    return datetime(2026, 3, 12, hour, minute, tzinfo=_BERLIN)


class TestIsTradingHours:

    def test_inside_hours(self):
        assert is_trading_hours(_dt(9, 0))   is True
        assert is_trading_hours(_dt(12, 0))  is True
        assert is_trading_hours(_dt(21, 59)) is True

    def test_at_open(self):
        assert is_trading_hours(_dt(8, 0)) is True

    def test_at_close_is_outside(self):
        # 22:00 exactly is outside (< 22:00 boundary)
        assert is_trading_hours(_dt(22, 0)) is False

    def test_before_open(self):
        assert is_trading_hours(_dt(7, 59)) is False
        assert is_trading_hours(_dt(0, 0))  is False

    def test_after_close(self):
        assert is_trading_hours(_dt(22, 1))  is False
        assert is_trading_hours(_dt(23, 59)) is False

    def test_none_uses_current_time(self):
        # Just verify it doesn't raise — actual value depends on when test runs
        result = is_trading_hours(None)
        assert isinstance(result, bool)


class TestSecondsUntilOpen:

    def test_inside_hours_returns_zero(self):
        assert seconds_until_open(_dt(10, 0)) == 0

    def test_before_open_same_day(self):
        # 07:00 → open at 08:00 = 3600s
        result = seconds_until_open(_dt(7, 0))
        assert result == 3600

    def test_after_close_next_day(self):
        # 22:30 → next open is tomorrow 08:00 = 9.5h = 34200s
        result = seconds_until_open(_dt(22, 30))
        assert result == 34200

    def test_at_midnight(self):
        # 00:00 → open at 08:00 = 8h = 28800s
        result = seconds_until_open(_dt(0, 0))
        assert result == 28800

    def test_returns_int(self):
        assert isinstance(seconds_until_open(_dt(7, 0)), int)
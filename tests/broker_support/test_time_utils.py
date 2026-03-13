"""
Unit tests for time_utils.

Run:  pytest tests/broker_support/test_time_utils.py -v
"""
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from src.broker_support.utils.time_utils import (
    is_trading_hours,
    is_valid_trading_window,
    seconds_until_open,
)

_BERLIN = ZoneInfo("Europe/Berlin")
_UTC    = ZoneInfo("UTC")


def _dt(hour: int, minute: int = 0) -> datetime:
    """Helper — build a Berlin-tz datetime for today."""
    return datetime(2026, 3, 12, hour, minute, tzinfo=_BERLIN)


def _dt_utc(hour: int, minute: int = 0) -> datetime:
    """Helper — build a UTC datetime for today."""
    return datetime(2026, 3, 12, hour, minute, tzinfo=_UTC)


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


class TestIsValidTradingWindow:
    """
    Tests for WBWS+ execution quality gate.

    Allowed hours (default): [9, 10, 11, 12, 13, 14, 15, 16] UTC
    Skip hours    (default): [17, 18] UTC
    """

    # ------------------------------------------------------------------
    # Default allowed hours (WBWS+ config)
    # ------------------------------------------------------------------

    def test_allowed_hour_9(self):
        assert is_valid_trading_window(_dt_utc(9)) is True

    def test_allowed_hour_14(self):
        assert is_valid_trading_window(_dt_utc(14)) is True

    def test_allowed_hour_16(self):
        assert is_valid_trading_window(_dt_utc(16)) is True

    def test_not_allowed_hour_8(self):
        # 08 UTC is not in default allowed_hours
        assert is_valid_trading_window(_dt_utc(8)) is False

    def test_not_allowed_hour_17(self):
        # 17 UTC is in skip_hours → False even if somehow in allowed
        assert is_valid_trading_window(_dt_utc(17)) is False

    def test_not_allowed_hour_18(self):
        assert is_valid_trading_window(_dt_utc(18)) is False

    def test_not_allowed_hour_22(self):
        assert is_valid_trading_window(_dt_utc(22)) is False

    def test_not_allowed_hour_0(self):
        assert is_valid_trading_window(_dt_utc(0)) is False

    # ------------------------------------------------------------------
    # skip_hours takes precedence over allowed_hours
    # ------------------------------------------------------------------

    def test_skip_overrides_allowed(self):
        """Hour 17 skipped even if explicitly in allowed_hours."""
        assert is_valid_trading_window(
            _dt_utc(17),
            allowed_hours_utc=[17, 18, 19],
            skip_hours_utc=[17],
        ) is False

    def test_skip_hour_not_in_allowed(self):
        """Hour 20 not in allowed and not in skip → False."""
        assert is_valid_trading_window(
            _dt_utc(20),
            allowed_hours_utc=[9, 10, 11],
            skip_hours_utc=[17, 18],
        ) is False

    # ------------------------------------------------------------------
    # Custom allowed_hours overrides
    # ------------------------------------------------------------------

    def test_custom_allowed_hours(self):
        assert is_valid_trading_window(
            _dt_utc(15),
            allowed_hours_utc=[15, 16],
            skip_hours_utc=[],
        ) is True

    def test_custom_allowed_excludes_default_hours(self):
        # Hour 9 is in default allowed but not in this custom override
        assert is_valid_trading_window(
            _dt_utc(9),
            allowed_hours_utc=[14, 15, 16],
            skip_hours_utc=[17, 18],
        ) is False

    def test_empty_allowed_hours_always_false(self):
        """Empty allowed_hours → no hour is valid."""
        for hour in [9, 12, 16]:
            assert is_valid_trading_window(
                _dt_utc(hour),
                allowed_hours_utc=[],
                skip_hours_utc=[],
            ) is False

    # ------------------------------------------------------------------
    # Timezone handling
    # ------------------------------------------------------------------

    def test_tz_aware_non_utc_converted_correctly(self):
        """Berlin CET (UTC+1) hour 10 = UTC hour 9 → allowed."""
        berlin_10am = datetime(2026, 3, 12, 10, 0, tzinfo=_BERLIN)
        # March 12 is CET (UTC+1), so 10:00 CET = 09:00 UTC → allowed
        assert is_valid_trading_window(berlin_10am) is True

    def test_tz_aware_berlin_hour_18_utc_17_skipped(self):
        """Berlin CET hour 18 = UTC 17 → skip_hours → False."""
        berlin_18 = datetime(2026, 3, 12, 18, 0, tzinfo=_BERLIN)
        assert is_valid_trading_window(berlin_18) is False

    def test_tz_naive_treated_as_utc(self):
        """tz-naive datetime assumed UTC."""
        naive_9am = datetime(2026, 3, 12, 9, 0)  # no tzinfo
        assert is_valid_trading_window(naive_9am) is True

    def test_none_uses_current_time(self):
        """Calling with no dt should not raise."""
        result = is_valid_trading_window(None)
        assert isinstance(result, bool)

    # ------------------------------------------------------------------
    # Return type
    # ------------------------------------------------------------------

    def test_returns_bool_true(self):
        assert type(is_valid_trading_window(_dt_utc(11))) is bool

    def test_returns_bool_false(self):
        assert type(is_valid_trading_window(_dt_utc(0))) is bool

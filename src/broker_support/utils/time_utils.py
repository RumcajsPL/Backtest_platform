"""
Trading hours guard for DAX (GER40).

DAX trading hours: 08:00–22:00 CET/CEST (Europe/Berlin).
The tracker loop only polls within these hours to avoid unnecessary
API calls and to match the instrument's active session.

WBWS+ trading window: applied as a separate gate AFTER the strategy
time_filter. Controls execution quality — not signal validity.
See is_valid_trading_window() for details.

Usage:
    from broker_support.utils.time_utils import (
        is_trading_hours,
        seconds_until_open,
        is_valid_trading_window,
    )

    if is_trading_hours():
        tracker.track()
    else:
        sleep(seconds_until_open())

    # WBWS+ gate — pass config from BrokerSupportConfig
    if is_valid_trading_window(dt, allowed_hours_utc=[9,10,11,12,13,14,15,16]):
        router.open_position(...)
"""
from datetime import datetime, time, timedelta
from typing import List, Optional
from zoneinfo import ZoneInfo

_TZ_BERLIN = ZoneInfo("Europe/Berlin")
_TZ_UTC    = ZoneInfo("UTC")

_MARKET_OPEN  = time(8, 0)
_MARKET_CLOSE = time(22, 0)


def now_berlin() -> datetime:
    """Return current datetime in Europe/Berlin (CET/CEST auto-handled)."""
    return datetime.now(tz=_TZ_BERLIN)


def is_trading_hours(dt: datetime | None = None) -> bool:
    """
    Return True if dt (default: now) falls within DAX trading hours.
    08:00 ≤ time < 22:00 Europe/Berlin, any day of week.

    eToro demo accounts allow 24/7 trading but DAX prices are only live
    during exchange hours. Restricting polling to 08:00–22:00 CET avoids
    stale-price enrichment and unnecessary API load.
    """
    if dt is None:
        dt = now_berlin()
    elif dt.tzinfo is None:
        dt = dt.replace(tzinfo=_TZ_BERLIN)
    else:
        dt = dt.astimezone(_TZ_BERLIN)

    t = dt.time()
    return _MARKET_OPEN <= t < _MARKET_CLOSE


def seconds_until_open(dt: datetime | None = None) -> int:
    """
    Return seconds until the next market open (08:00 CET).
    Returns 0 if already within trading hours.
    """
    if dt is None:
        dt = now_berlin()

    if is_trading_hours(dt):
        return 0

    # Build today's open datetime in Berlin tz
    today_open = dt.replace(
        hour=_MARKET_OPEN.hour,
        minute=_MARKET_OPEN.minute,
        second=0,
        microsecond=0,
    )

    if dt >= today_open:
        # Past close — next open is tomorrow
        today_open += timedelta(days=1)

    delta = today_open - dt
    return max(0, int(delta.total_seconds()))


def is_valid_trading_window(
    dt: Optional[datetime] = None,
    allowed_hours_utc: Optional[List[int]] = None,
    skip_hours_utc: Optional[List[int]] = None,
) -> bool:
    """
    WBWS+ execution quality gate.

    Returns True if dt (default: now) falls within the allowed UTC hours
    and is NOT in skip_hours_utc.

    This is a SEPARATE gate from is_trading_hours() and the strategy
    time_filter. It governs execution quality — whether a valid signal
    should actually be placed given the historical hour-of-day performance.

    Analysis basis (full history 1,498 trades + last 3 months 90 trades):
      - Hour 17 UTC: consistently negative both periods → always skipped
      - Hours 09–16 UTC (London core + early NY): 74–80% of all profit
      - Hours 10, 13 UTC: regime-dependent (opposite sign across periods)
        → not filtered here, included in allowed_hours for completeness

    Args:
        dt:                Datetime to check. Defaults to now (UTC).
                           If tz-naive, assumed UTC.
                           If tz-aware, converted to UTC.
        allowed_hours_utc: UTC hours during which execution is permitted.
                           Defaults to [9,10,11,12,13,14,15,16].
                           Pass from BrokerSupportConfig.trading_window.allowed_hours_utc.
        skip_hours_utc:    UTC hours to always skip, regardless of allowed_hours_utc.
                           Defaults to [17, 18].
                           Takes precedence over allowed_hours_utc.

    Returns:
        True  — hour is in allowed_hours_utc AND not in skip_hours_utc.
        False — window disabled, hour not allowed, or explicitly skipped.

    Note:
        Returns False when called with a tz-naive dt and UTC conversion
        is ambiguous — callers should always pass tz-aware datetimes.
    """
    if allowed_hours_utc is None:
        allowed_hours_utc = [9, 10, 11, 12, 13, 14, 15, 16]
    if skip_hours_utc is None:
        skip_hours_utc = [17, 18]

    if dt is None:
        dt_utc = datetime.now(tz=_TZ_UTC)
    elif dt.tzinfo is None:
        # Treat tz-naive as UTC — document this assumption clearly
        dt_utc = dt.replace(tzinfo=_TZ_UTC)
    else:
        dt_utc = dt.astimezone(_TZ_UTC)

    hour_utc = dt_utc.hour

    # skip_hours takes precedence
    if hour_utc in skip_hours_utc:
        return False

    return hour_utc in allowed_hours_utc

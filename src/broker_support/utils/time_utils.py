"""
Trading hours guard for DAX (GER40).

DAX trading hours: 08:00–22:00 CET/CEST (Europe/Berlin).
The tracker loop only polls within these hours to avoid unnecessary
API calls and to match the instrument's active session.

Usage:
    from broker_support.utils.time_utils import is_trading_hours, seconds_until_open

    if is_trading_hours():
        tracker.track()
    else:
        sleep(seconds_until_open())
"""
from datetime import datetime, time
from zoneinfo import ZoneInfo

_TZ_BERLIN = ZoneInfo("Europe/Berlin")
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
        from datetime import timedelta
        today_open += timedelta(days=1)

    delta = today_open - dt
    return max(0, int(delta.total_seconds()))
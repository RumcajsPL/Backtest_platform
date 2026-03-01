"""
wfo/window_generator.py
-----------------------
Reads WFO window definitions from the backtest config dict and returns a validated,
ordered list of WFOWindow contracts.

Single responsibility: config → List[WFOWindow].
Validation: min 3 windows (GA random sampling requirement), no overlaps, valid date order.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import List

from src.backtesting.contracts import WFOWindow

logger = logging.getLogger(__name__)


def generate_windows(config: dict) -> List[WFOWindow]:
    """
    Read WFO window definitions from the 'walk_forward.windows' section of the
    backtester config dict and return a validated list of WFOWindow contracts.

    Args:
        config: The full backtest config dict (from backtest_template.yaml).

    Returns:
        Ordered list of WFOWindow, ordered by start_date ascending.

    Raises:
        KeyError:   If required config keys are missing.
        ValueError: If fewer than 3 windows are defined, any window has
                    invalid dates, or windows overlap.
    """
    wf_config: dict = config["walk_forward"]
    raw_windows: list = wf_config.get("windows", [])

    if len(raw_windows) < 3:
        raise ValueError(
            f"Minimum 3 WFO windows are required for GA random sampling; "
            f"got {len(raw_windows)}. Add more windows to walk_forward.windows in the YAML."
        )

    windows: List[WFOWindow] = []
    for entry in raw_windows:
        window_id: str = entry["id"]
        start_date: date = _parse_date(entry["start"], window_id)
        end_date: date = _parse_date(entry["end"], window_id)
        # WFOWindow.__post_init__ validates start < end
        windows.append(WFOWindow(
            window_id=window_id,
            start_date=start_date,
            end_date=end_date,
        ))

    # Sort by start_date ascending — predictable ordering throughout pipeline
    windows.sort(key=lambda w: w.start_date)

    _validate_no_overlaps(windows)

    logger.info(
        "WFO windows loaded: %d windows, %s → %s",
        len(windows),
        windows[0].start_date,
        windows[-1].end_date,
    )
    return windows


def extract_window_ids(windows: List[WFOWindow]) -> tuple:
    """Return an ordered tuple of window IDs for RunMetadata.wfo_window_ids."""
    return tuple(w.window_id for w in windows)


# ── Private helpers ────────────────────────────────────────────────────────────

def _parse_date(value: str, window_id: str) -> date:
    """Parse YYYY-MM-DD string to date. Raises ValueError with context on failure."""
    try:
        return date.fromisoformat(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(
            f"Window '{window_id}': invalid date '{value}'. Expected format YYYY-MM-DD."
        ) from exc


def _validate_no_overlaps(windows: List[WFOWindow]) -> None:
    """
    Verify that no two windows overlap. Windows are assumed sorted by start_date.
    Two windows overlap if the earlier window's end_date is after the later window's start_date.
    """
    for i in range(len(windows) - 1):
        earlier = windows[i]
        later = windows[i + 1]
        if earlier.end_date > later.start_date:
            raise ValueError(
                f"WFO windows overlap: '{earlier.window_id}' ends {earlier.end_date} but "
                f"'{later.window_id}' starts {later.start_date}. Windows must not overlap."
            )
"""Time Filter — session hours gate.

Migrated:  Session 4  v3.0.0
Hardened:  Session 20 Block H — DEC-022 ("debug" → "analytics"); DEC-027 (always
           collect timing); P1-CH3-3 (count_by_type removed from hot path);
           DEC-021 (legacy methods removed); logging gated on analytics mode.

NOTE P1-CH3-8 (deferred to Block A):
    Constructor still accepts ``config: Dict[str, Any]`` because ``FilterPipeline``
    constructs ``TimeFilter`` directly from the raw trade-management config dict.
    Switching to typed parameters requires a coordinated change in filter_pipeline.py.
    The raw dict is unpacked immediately and ``self.config`` is NOT stored, which
    eliminates accidental downstream mutation of the config reference.

Removed (DEC-021):
    * ``is_in_trading_hours()``  — legacy scalar helper
    * ``get_session_info()``     — legacy debug dict
"""
from __future__ import annotations

import logging
from time import perf_counter
from typing import Any, Dict

import numpy as np
import pandas as pd

from src.strategies.contracts.filter_contracts import (
    FilterMetadata,
    FilterResult,
    FilterStatus,
)
from src.strategies.contracts.signal_contracts import SignalFrame

logger = logging.getLogger(__name__)


class TimeFilter:
    """Time-based signal filter — allows trades only during session hours.

    Uses pre-converted timezone timestamps on the ``SignalFrame`` index.
    Implements ``FilterProtocol`` for integration with ``FilterPipeline``.

    Constructor signature kept as ``(config, name)`` pending P1-CH3-8 refactor
    of ``FilterPipeline`` (see module docstring).
    """

    def __init__(self, config: Dict[str, Any], name: str = "time_filter") -> None:
        # Unpack immediately — do NOT store the raw dict reference (avoids
        # accidental mutation and makes dependencies explicit).
        tf = config.get("time_filter", {})

        self.name    = name
        self.enabled = tf.get("enabled", True)

        self.session_start_hour   = tf.get("session_start", {}).get("hour",   8)
        self.session_start_minute = tf.get("session_start", {}).get("minute", 30)
        self.session_end_hour     = tf.get("session_end",   {}).get("hour",   20)
        self.session_end_minute   = tf.get("session_end",   {}).get("minute", 30)

        # Convert to minutes once for fast vectorised comparison
        self.session_start_minutes = self.session_start_hour * 60 + self.session_start_minute
        self.session_end_minutes   = self.session_end_hour   * 60 + self.session_end_minute

        if self.enabled:
            if self.session_start_minutes >= self.session_end_minutes:
                msg = (
                    f"Invalid session config: start "
                    f"({self.session_start_hour:02d}:{self.session_start_minute:02d}) "
                    f"must be before end "
                    f"({self.session_end_hour:02d}:{self.session_end_minute:02d})"
                )
                logger.error(msg)
                raise ValueError(msg)

            logger.info(
                "%s: session %02d:%02d–%02d:%02d",
                self.name,
                self.session_start_hour, self.session_start_minute,
                self.session_end_hour,   self.session_end_minute,
            )
        else:
            logger.info("%s: DISABLED", self.name)

    # ------------------------------------------------------------------
    # Indicator computation (no-op — time filter uses index, not OHLCV)
    # ------------------------------------------------------------------

    def compute_indicators(
        self,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray],
    ) -> None:
        """No-op — time filter derives its mask from the DataFrame index."""

    # ------------------------------------------------------------------
    # Signal filtering
    # ------------------------------------------------------------------

    def apply_filter(
        self,
        signal_frame: SignalFrame,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray],
        mode: str = "core",
    ) -> FilterResult:
        """Filter signals to configured session hours — vectorised.

        Parameters
        ----------
        mode:
            ``"core"`` or ``"analytics"``.  Timing always collected (DEC-027).
            The removal-rate ``logger.info`` is gated on analytics mode.
        """
        start_time = perf_counter()

        # ---- disabled fast-path ----------------------------------------
        if not self.enabled:
            n = int(np.sum(signal_frame.signals.values != 0))
            return FilterResult(
                passed=True,
                signal_frame=signal_frame,
                metadata=FilterMetadata(
                    filter_name=self.name,
                    status=FilterStatus.SKIPPED,
                    signals_in=n,
                    signals_out=n,
                    signals_rejected=0,
                    reason="Filter disabled",
                    execution_time_ms=(perf_counter() - start_time) * 1000,
                ),
            )

        # ---- signal count (single numpy call) --------------------------
        signal_values = signal_frame.signals.values
        signals_in = int(np.sum(signal_values != 0))

        if signals_in == 0:
            return FilterResult(
                passed=False,
                signal_frame=signal_frame,
                metadata=FilterMetadata(
                    filter_name=self.name,
                    status=FilterStatus.SKIPPED,
                    signals_in=0,
                    signals_out=0,
                    signals_rejected=0,
                    reason="No input signals",
                    execution_time_ms=(perf_counter() - start_time) * 1000,
                ),
            )

        # ---- vectorised time mask --------------------------------------
        timestamps   = signal_frame.signals.index
        minutes_col  = timestamps.hour.values * 60 + timestamps.minute.values

        trading_hours_mask = (
            (minutes_col >= self.session_start_minutes) &
            (minutes_col <  self.session_end_minutes)
        )

        # Operate on numpy values — avoids pandas copy overhead
        filtered_signals = signal_values.copy()
        filtered_signals[~trading_hours_mask] = 0

        signals_out      = int(np.sum(filtered_signals != 0))
        signals_rejected = signals_in - signals_out

        filtered_frame = SignalFrame(
            signals=pd.Series(filtered_signals, index=signal_frame.signals.index, dtype="int8"),
            indicator_data=signal_frame.indicator_data if mode == "analytics" else None,
            signal_metadata={
                "source": self.name,
                "mode": mode,
                "session_hours": (
                    f"{self.session_start_hour:02d}:{self.session_start_minute:02d}"
                    f"–"
                    f"{self.session_end_hour:02d}:{self.session_end_minute:02d}"
                ),
            },
        )

        if signals_out == 0:
            status, reason = FilterStatus.REJECTED, "All signals outside trading hours"
        elif signals_rejected == 0:
            status, reason = FilterStatus.PASSED, "All signals within trading hours"
        else:
            status, reason = FilterStatus.PASSED, f"{signals_rejected} signals outside trading hours"

        # Removal-rate log — analytics mode only (DEC-022 logging gate)
        if mode == "analytics" and signals_rejected > 0:
            removal_rate = signals_rejected / signals_in
            logger.info(
                "%s: %d/%d removed (%.1f%%)",
                self.name, signals_rejected, signals_in, removal_rate * 100,
            )

        return FilterResult(
            passed=(signals_out > 0),
            signal_frame=filtered_frame,
            metadata=FilterMetadata(
                filter_name=self.name,
                status=status,
                signals_in=signals_in,
                signals_out=signals_out,
                signals_rejected=signals_rejected,
                reason=reason,
                indicator_values=None,
                execution_time_ms=(perf_counter() - start_time) * 1000,
            ),
        )
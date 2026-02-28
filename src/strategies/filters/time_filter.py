"""Time Filter — session hours gate.
Version: 4.0.0
"""
from __future__ import annotations

import logging
from time import perf_counter
from typing import Dict

import numpy as np
import pandas as pd

from src.strategies.contracts.filter_contracts import (
    FilterMetadata,
    FilterResult,
    FilterStatus,
)
from src.strategies.contracts.signal_contracts import SignalFrame
from src.strategies.config.config_schema import TimeFilterConfig

logger = logging.getLogger(__name__)


class TimeFilter:
    """Time-based signal filter — allows trades only during session hours.

    Uses pre-converted timezone timestamps on the SignalFrame index.
    Implements FilterProtocol for integration with FilterPipeline.
    """

    def __init__(self, config: TimeFilterConfig, name: str = "time_filter") -> None:
        """
        Initialize TimeFilter with typed configuration.

        Args:
            config: TimeFilterConfig instance
            name: Filter name for metadata
        """
        self.name = name
        self.enabled = config.enabled

        self.session_start_hour = config.session_start_hour
        self.session_start_minute = config.session_start_minute
        self.session_end_hour = config.session_end_hour
        self.session_end_minute = config.session_end_minute

        # Convert to minutes once for fast vectorised comparison
        self.session_start_minutes = self.session_start_hour * 60 + self.session_start_minute
        self.session_end_minutes = self.session_end_hour * 60 + self.session_end_minute

        # Validation already done in TimeFilterConfig.__post_init__

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
        pass

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
            "core" or "analytics". Timing always collected.
            The removal-rate logger.info is gated on analytics mode.
        """
        start_time = perf_counter()

        # Mode validation
        if mode not in {"core", "analytics"}:
            raise ValueError(f"Invalid mode '{mode}'. Must be 'core' or 'analytics'.")

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
        timestamps = signal_frame.signals.index
        minutes_col = timestamps.hour.values * 60 + timestamps.minute.values

        trading_hours_mask = (
            (minutes_col >= self.session_start_minutes) &
            (minutes_col <= self.session_end_minutes)
        )

        # Operate on numpy values — avoids pandas copy overhead
        filtered_signals = signal_values.copy()
        filtered_signals[~trading_hours_mask] = 0

        signals_out = int(np.sum(filtered_signals != 0))
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

        # Removal-rate log — analytics mode only
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
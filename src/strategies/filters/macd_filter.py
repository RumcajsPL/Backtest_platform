"""MACD Filter — momentum directional filter using histogram.
Version 3.0.1
Logic
------------------
* Only histogram used for filtering
* BUY:  histogram > 0  (strict)
* SELL: histogram < 0  (strict)
* NaN → False (numpy comparison with NaN returns False natively)
"""
from __future__ import annotations

import logging
from time import perf_counter
from typing import Dict

import numpy as np
import pandas as pd
import pandas_ta_classic as pta

from src.strategies.contracts.filter_contracts import (
    FilterMetadata,
    FilterResult,
    FilterStatus,
)
from src.strategies.contracts.signal_contracts import SignalFrame

logger = logging.getLogger(__name__)


class MACDFilter:
    """MACD filter — histogram-based directional momentum gate.

    Implements ``FilterProtocol`` for integration with ``FilterPipeline``.
    """

    def __init__(
        self,
        fast_length: int = 12,
        slow_length: int = 26,
        signal_length: int = 9,
        enabled: bool = True,
        name: str = "macd_filter",
    ) -> None:
        self.name = name
        self.fast_length   = int(fast_length)
        self.slow_length   = int(slow_length)
        self.signal_length = int(signal_length)
        self.enabled = enabled

        # Aliases used by pandas_ta call
        self.fast   = self.fast_length
        self.slow   = self.slow_length
        self.signal = self.signal_length

        if self.fast >= self.slow:
            raise ValueError(
                f"fast_length ({self.fast}) must be < slow_length ({self.slow})"
            )

    # ------------------------------------------------------------------
    # Indicator computation
    # ------------------------------------------------------------------
    def _calculate_macd(self, series: pd.Series) -> pd.Series:
        """Return MACD histogram only."""
        min_required = self.slow_length + self.signal_length + 1
        if len(series) < min_required:
            return pd.Series(np.nan, index=series.index, dtype="float32")

        macd_df = pta.macd(series, fast=self.fast, slow=self.slow, signal=self.signal)

        if macd_df is None or macd_df.empty:
            return pd.Series(np.nan, index=series.index, dtype="float32")

        # Guard against None columns — pandas_ta_classic bug on short series
        hist_col = f"MACDh_{self.fast}_{self.slow}_{self.signal}"
        if hist_col not in macd_df.columns:
            raise KeyError(f"MACD histogram column '{hist_col}' not found.")

        hist = macd_df[hist_col]
        if hist is None:
            return pd.Series(np.nan, index=series.index, dtype="float32")

        return hist.astype("float32")

    def compute_indicators(self, df, indicators, ind_np):
        min_length = self.slow_length + self.signal_length + 1  # match _calculate_macd
        if len(df) < min_length:
            empty = pd.Series(np.nan, index=df.index, dtype="float32")
            indicators["macd_histogram"] = empty
            ind_np["macd_histogram"]     = empty.to_numpy()
            return

        hist = self._calculate_macd(df["close"])
        indicators["macd_histogram"] = hist
        ind_np["macd_histogram"]     = hist.to_numpy()

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
        """Filter signals based on MACD histogram — vectorised.

        * BUY:  ``histogram > 0``  (strict; NaN → False)
        * SELL: ``histogram < 0``  (strict; NaN → False)

        Parameters
        ----------
        mode:
            ``"core"`` or ``"analytics"``.  Timing always.
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
                    reason="No input signals",
                    execution_time_ms=(perf_counter() - start_time) * 1000,
                ),
            )

        # ---- indicator guard -------------------------------------------
        macd_histogram = ind_np.get("macd_histogram")
        if macd_histogram is None:
            logger.error("%s: MACD histogram not found in cache.", self.name)
            return FilterResult(
                passed=False,
                signal_frame=SignalFrame(
                    signals=pd.Series(0, index=signal_frame.signals.index, dtype="int8"),
                    indicator_data=None,
                    signal_metadata={"error": "indicator_missing"},
                ),
                metadata=FilterMetadata(
                    filter_name=self.name,
                    status=FilterStatus.ERROR,
                    signals_in=signals_in,
                    signals_out=0,
                    reason="MACD histogram not computed",
                    execution_time_ms=(perf_counter() - start_time) * 1000,
                ),
            )

        # ---- vectorised filter -----------------------------------------
        hist_values = macd_histogram.astype(np.float32)

        # Default False (NaN comparisons naturally return False)
        mask      = np.zeros(len(signal_values), dtype=bool)
        buy_mask  = signal_values == 1
        sell_mask = signal_values == 2

        mask[buy_mask]  = hist_values[buy_mask]  > 0
        mask[sell_mask] = hist_values[sell_mask] < 0

        filtered_signals = signal_values.copy()
        filtered_signals[~mask] = 0

        signals_out      = int(np.sum(filtered_signals != 0))
        signals_rejected = signals_in - signals_out

        filtered_frame = SignalFrame(
            signals=pd.Series(filtered_signals, index=signal_frame.signals.index, dtype="int8"),
            indicator_data=signal_frame.indicator_data if mode == "analytics" else None,
            signal_metadata={
                "source": self.name,
                "mode": mode,
                "macd_params": {
                    "fast_length":   self.fast_length,
                    "slow_length":   self.slow_length,
                    "signal_length": self.signal_length,
                },
            },
        )

        if signals_out == 0:
            status, reason = FilterStatus.REJECTED, "All signals rejected (MACD histogram)"
        elif signals_rejected == 0:
            status, reason = FilterStatus.PASSED, "All signals passed (MACD aligned)"
        else:
            status, reason = FilterStatus.PASSED, f"{signals_rejected} signals rejected (MACD)"

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
                execution_time_ms=(perf_counter() - start_time) * 1000,
            ),
        )
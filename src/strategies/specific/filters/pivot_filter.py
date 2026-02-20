"""Pivot Filter — swing high/low structural bias (HH/HL vs LH/LL).

Migrated:  Session 5  v3.0.1  (EXACT legacy computation restored)
Hardened:  Session 20 Block H — DEC-022 ("debug" → "analytics"); DEC-027 (always
           collect timing); P1-CH3-3 (count_by_type removed from hot path).

EXACT legacy logic
------------------
* Swing detection via ``scipy.signal.argrelextrema``
* HH/HL/LH/LL sequence analysis with reversal_percent threshold
* Forward-fill bias for non-pivot bars
* BUY: pivot_bias == 1; SELL: pivot_bias == -1; neutral (0): reject
"""
from __future__ import annotations

import logging
from time import perf_counter
from typing import Dict

import numpy as np
import pandas as pd
from scipy.signal import argrelextrema

from src.strategies.contracts.filter_contracts import (
    FilterMetadata,
    FilterResult,
    FilterStatus,
)
from src.strategies.contracts.signal_contracts import SignalFrame

logger = logging.getLogger(__name__)


class PivotFilter:
    """Pivot filter — structural market bias via swing-point analysis.

    Implements ``FilterProtocol`` for integration with ``FilterPipeline``.
    """

    def __init__(
        self,
        reversal_percent: float = 0.2,
        order: int = 5,
        enabled: bool = True,
        name: str = "pivot_filter",
    ) -> None:
        self.name = name
        self.reversal_percent = float(reversal_percent) / 100  # % → decimal
        self.order = int(order)
        self.enabled = enabled

        if self.reversal_percent <= 0:
            raise ValueError(
                f"reversal_percent must be > 0, got {reversal_percent}"
            )
        if self.order < 1:
            raise ValueError(f"order must be >= 1, got {self.order}")

    # ------------------------------------------------------------------
    # Indicator computation
    # ------------------------------------------------------------------

    def _detect_swings(self, series: np.ndarray, is_high: bool) -> np.ndarray:
        """Detect swing highs or lows via ``argrelextrema``."""
        if len(series) < 2 * self.order + 1:
            return np.array([], dtype=int)
        comparator = np.greater if is_high else np.less
        return argrelextrema(series, comparator, order=self.order)[0]

    def _calculate_pivot_structure(self, df: pd.DataFrame) -> pd.Series:
        """Compute pivot bias (1 = bullish, -1 = bearish, 0 = neutral).

        Exact legacy forward-fill sequence:
        1. Replace 0 → NaN
        2. ffill
        3. fillna(0)
        """
        if len(df) < 3:
            return pd.Series(0, index=df.index, dtype="int8")

        high = df["high"].values
        low  = df["low"].values

        high_idx = self._detect_swings(high, is_high=True)
        low_idx  = self._detect_swings(low,  is_high=False)

        high_set = set(high_idx)
        low_set  = set(low_idx)

        all_pivots = np.sort(np.unique(np.concatenate([high_idx, low_idx])))
        if len(all_pivots) < 2:
            return pd.Series(0, index=df.index, dtype="int8")

        bias = pd.Series(0, index=df.index, dtype="int8")

        first_idx  = all_pivots[0]
        second_idx = all_pivots[1]
        is_first_high  = first_idx  in high_set
        is_second_high = second_idx in high_set

        if is_first_high and not is_second_high:
            trend    = -1
            last_high = high[first_idx]
            last_low  = low[second_idx]
            pivot_idx = 2
        elif not is_first_high and is_second_high:
            trend    = 1
            last_low  = low[first_idx]
            last_high = high[second_idx]
            pivot_idx = 2
        else:
            trend    = 0
            last_high = high[0]
            last_low  = low[0]
            pivot_idx = 1

        for i in range(pivot_idx, len(all_pivots)):
            idx      = all_pivots[i]
            curr_high = high[idx]
            curr_low  = low[idx]
            is_high_pivot = idx in high_set
            is_low_pivot  = idx in low_set

            if trend >= 0:  # Uptrend or neutral
                if is_high_pivot:
                    if curr_high > last_high and last_high > 0:
                        bias.iloc[idx] = 1   # HH — bullish
                    elif curr_high < last_high and last_high > 0:
                        bias.iloc[idx] = -1  # LH — bearish
                    last_high = curr_high
                    trend = 1
                elif is_low_pivot and trend == 1:
                    if curr_low < last_high * (1 - self.reversal_percent):
                        if curr_low > last_low:
                            bias.iloc[idx] = 1   # HL — bullish
                        elif curr_low < last_low:
                            bias.iloc[idx] = -1  # LL — bearish
                        last_low = curr_low
                        trend = -1
                    else:
                        last_low = max(last_low, curr_low)
            else:  # Downtrend
                if is_low_pivot:
                    if curr_low < last_low and last_low > 0:
                        bias.iloc[idx] = -1  # LL — bearish
                    elif curr_low > last_low and last_low > 0:
                        bias.iloc[idx] = 1   # HL — bullish
                    last_low = curr_low
                    trend = -1
                elif is_high_pivot and trend == -1:
                    if curr_high > last_low * (1 + self.reversal_percent):
                        if curr_high > last_high:
                            bias.iloc[idx] = 1   # HH — bullish
                        elif curr_high < last_high:
                            bias.iloc[idx] = -1  # LH — bearish
                        last_high = curr_high
                        trend = 1
                    else:
                        last_high = min(last_high, curr_high)

        # EXACT legacy forward-fill sequence
        bias = bias.replace(0, np.nan).ffill().fillna(0).astype("int8")
        return bias

    def compute_indicators(
        self,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray],
    ) -> None:
        """Compute pivot structural bias."""
        pivot_bias = self._calculate_pivot_structure(df)
        indicators["pivot_bias"] = pivot_bias
        ind_np["pivot_bias"]     = pivot_bias.to_numpy(dtype=np.int8)

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
        """Filter signals based on pivot structural bias — vectorised.

        * BUY:  ``pivot_bias == 1``
        * SELL: ``pivot_bias == -1``
        * Neutral (0): rejected

        Parameters
        ----------
        mode:
            ``"core"`` or ``"analytics"``.  Timing always collected (DEC-027).
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
        pivot_bias = ind_np.get("pivot_bias")
        if pivot_bias is None:
            logger.error("%s: Pivot bias not found in cache.", self.name)
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
                    reason="Pivot bias not computed",
                    execution_time_ms=(perf_counter() - start_time) * 1000,
                ),
            )

        # ---- vectorised filter -----------------------------------------
        bias_values = pivot_bias.astype(np.int8)

        mask      = np.zeros(len(signal_values), dtype=bool)
        buy_mask  = signal_values == 1
        sell_mask = signal_values == 2

        mask[buy_mask]  = bias_values[buy_mask]  == 1
        mask[sell_mask] = bias_values[sell_mask] == -1

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
                "pivot_params": {
                    "reversal_percent": self.reversal_percent * 100,
                    "order": self.order,
                },
            },
        )

        if signals_out == 0:
            status, reason = FilterStatus.REJECTED, "All signals rejected (neutral/opposing pivot bias)"
        elif signals_rejected == 0:
            status, reason = FilterStatus.PASSED, "All signals passed (pivot bias aligned)"
        else:
            status, reason = FilterStatus.PASSED, f"{signals_rejected} signals rejected (pivot bias)"

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
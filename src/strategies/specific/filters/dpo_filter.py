"""DPO Filter — Detrended Price Oscillator (cycle-based directional gate).

Migrated:  Session 5  v3.0.1  (EXACT legacy computation restored)
Hardened:  Session 20 Block H — DEC-022 ("debug" → "analytics"); DEC-027 (always
           collect timing); P1-CH3-3 (count_by_type removed from hot path).

EXACT legacy logic
------------------
* DPO normalised as percentage: (DPO / close) × 100
* Optional smoothing of raw DPO (smooth > 1)
* BUY:  −threshold < DPO_norm < 0   (strict inequalities)
* SELL: 0 < DPO_norm < threshold    (strict inequalities)
* NaN values → condition is False (fillna(False) semantics)
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


class DPOFilter:
    """Detrended Price Oscillator filter — cycle-based directional gate.

    Implements ``FilterProtocol`` for integration with ``FilterPipeline``.
    """

    def __init__(
        self,
        length: int = 20,
        smooth: int = 3,
        threshold: float = 0.2,
        centered: bool = False,
        enabled: bool = True,
        name: str = "dpo_filter",
    ) -> None:
        self.name = name
        self.length = int(length)
        self.smooth = int(smooth) if smooth > 0 else 1
        self.threshold = float(threshold)
        self.centered = centered
        self.enabled = enabled

        if self.length < 3:
            raise ValueError(f"DPO length must be >= 3, got {self.length}")

    # ------------------------------------------------------------------
    # Indicator computation
    # ------------------------------------------------------------------

    def _calculate_dpo(self, series: pd.Series) -> pd.Series:
        """DPO with optional smoothing, normalised as % of close.

        Matches legacy computation exactly — NaN preserved for short windows.
        """
        if len(series) < self.length:
            return pd.Series(np.nan, index=series.index, dtype="float32")

        dpo_raw = pta.dpo(series, length=self.length, centered=self.centered)
        if dpo_raw is None or dpo_raw.empty:
            return pd.Series(np.nan, index=series.index, dtype="float32")

        dpo_smoothed = (
            dpo_raw.rolling(window=self.smooth, min_periods=self.smooth).mean()
            if self.smooth > 1
            else dpo_raw
        )

        # Normalise: (DPO / close) × 100
        return ((dpo_smoothed / series) * 100).astype("float32")

    def compute_indicators(
        self,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray],
    ) -> None:
        """Compute normalised DPO — NaN retained for bars with insufficient history."""
        dpo_norm = self._calculate_dpo(df["close"])
        indicators["dpo_norm"] = dpo_norm
        ind_np["dpo_norm"] = dpo_norm.to_numpy(dtype=np.float32)

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
        """Filter signals based on normalised DPO with threshold — vectorised.

        Conditions (strict inequalities; NaN → False, matching legacy fillna(False)):

        * BUY:  ``-threshold < dpo_norm < 0``
        * SELL: ``0 < dpo_norm < threshold``

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
        dpo_norm = ind_np.get("dpo_norm")
        if dpo_norm is None:
            logger.error("%s: DPO indicator not found in cache.", self.name)
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
                    reason="DPO indicator not computed",
                    execution_time_ms=(perf_counter() - start_time) * 1000,
                ),
            )

        # ---- vectorised filter -----------------------------------------
        dpo_values = dpo_norm.astype(np.float32)

        # Default: all False (matches legacy fillna(False) on NaN positions)
        mask = np.zeros(len(signal_values), dtype=bool)

        buy_mask  = signal_values == 1
        sell_mask = signal_values == 2

        # BUY:  −threshold < dpo < 0  (strict; NaN → False via numpy comparison)
        if np.any(buy_mask):
            d = dpo_values[buy_mask]
            mask[buy_mask] = (d < 0) & (d > -self.threshold)

        # SELL: 0 < dpo < threshold  (strict; NaN → False)
        if np.any(sell_mask):
            d = dpo_values[sell_mask]
            mask[sell_mask] = (d > 0) & (d < self.threshold)

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
                "dpo_params": {
                    "length": self.length,
                    "smooth": self.smooth,
                    "threshold": self.threshold,
                    "centered": self.centered,
                },
            },
        )

        if signals_out == 0:
            status, reason = FilterStatus.REJECTED, "All signals rejected by DPO"
        elif signals_rejected == 0:
            status, reason = FilterStatus.PASSED, "All signals passed DPO filter"
        else:
            status, reason = FilterStatus.PASSED, f"{signals_rejected} signals rejected"

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
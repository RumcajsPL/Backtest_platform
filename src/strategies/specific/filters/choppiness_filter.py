"""Choppiness Index Filter — market trendiness / choppiness gate.

Migrated:  Session 5  v3.0.0
Hardened:  Session 20 Block H — DEC-022 ("debug" → "analytics"); DEC-027 (always
           collect timing); P1-CH3-3 (count_by_type removed from hot path).

Rejects ALL signals when Choppiness Index > threshold (market too choppy).
CI ranges 0–100: high (>61.8) = choppy; low (<38.2) = strongly trending.
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


class ChoppinessFilter:
    """Choppiness Index filter — rejects signals in sideways / choppy regimes.

    Implements ``FilterProtocol`` for integration with ``FilterPipeline``.
    """

    def __init__(
        self,
        length: int = 14,
        threshold: float = 61.8,
        enabled: bool = True,
        name: str = "choppiness_filter",
    ) -> None:
        self.name = name
        self.length = int(length)
        self.threshold = float(threshold)
        self.enabled = enabled

        if self.length < 2:
            raise ValueError(f"Choppiness length must be >= 2, got {self.length}")
        if not (0 <= self.threshold <= 100):
            raise ValueError(f"Threshold must be 0–100, got {self.threshold}")

    # ------------------------------------------------------------------
    # Indicator computation
    # ------------------------------------------------------------------

    def compute_indicators(
        self,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray],
    ) -> None:
        """Compute Choppiness Index using pandas_ta.

        Fills with 50.0 (neutral) when data is insufficient or calculation fails.
        """
        if len(df) < self.length:
            chop = pd.Series(50.0, index=df.index, dtype="float32")
        else:
            chop = pta.chop(
                high=df["high"],
                low=df["low"],
                close=df["close"],
                length=self.length,
            )
            if chop is None or chop.empty:
                logger.warning("%s: Choppiness Index calculation failed.", self.name)
                chop = pd.Series(50.0, index=df.index, dtype="float32")
            else:
                chop = chop.astype("float32").fillna(50.0)

        indicators["choppiness"] = chop
        ind_np["choppiness"] = chop.to_numpy()

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
        """Filter ALL signals based on Choppiness Index — vectorised.

        Passes when ``CI <= threshold`` (trending market).

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
        choppiness = ind_np.get("choppiness")
        if choppiness is None:
            logger.error("%s: Choppiness Index not found in cache.", self.name)
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
                    reason="Choppiness Index not computed",
                    execution_time_ms=(perf_counter() - start_time) * 1000,
                ),
            )

        # ---- vectorised filter (non-directional) -----------------------
        chop_values = choppiness.astype(np.float32)

        # mask applies to the full bar array, not just signal positions
        mask = chop_values <= self.threshold

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
                "choppiness_params": {
                    "length": self.length,
                    "threshold": self.threshold,
                },
            },
        )

        if signals_out == 0:
            status = FilterStatus.REJECTED
            reason = f"All signals rejected (market too choppy, CI > {self.threshold})"
        elif signals_rejected == 0:
            status = FilterStatus.PASSED
            reason = f"All signals passed (trending market, CI <= {self.threshold})"
        else:
            status = FilterStatus.PASSED
            reason = f"{signals_rejected} signals rejected (choppy periods)"

        indicator_values = None
        if mode == "analytics" and signals_out > 0:
            sig_idx = np.where(filtered_signals != 0)[0]
            if len(sig_idx):
                chop_at = chop_values[sig_idx]
                indicator_values = {
                    "avg_choppiness":    float(np.nanmean(chop_at)),
                    "min_choppiness":    float(np.nanmin(chop_at)),
                    "max_choppiness":    float(np.nanmax(chop_at)),
                    "trending_signals":  int(np.sum(chop_at < 38.2)),
                    "choppy_signals":    int(np.sum(chop_at > 61.8)),
                }

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
                indicator_values=indicator_values,
                execution_time_ms=(perf_counter() - start_time) * 1000,
            ),
        )
"""ADX Filter — trend strength (direction-agnostic).

Migrated:  Session 4  v3.0.0
Hardened:  Session 20 Block H — DEC-022 ("debug" → "analytics"); DEC-027 (always
           collect timing); P1-CH3-3 (count_by_type removed from hot path).

Rejects signals when ADX < threshold (weak / choppy trend).
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


class ADXFilter:
    """ADX filter — measures trend strength (direction-agnostic).

    Both BUY and SELL signals require ADX > ``threshold``.
    Implements the ``FilterProtocol`` interface for ``FilterPipeline``.
    """

    def __init__(
        self,
        adx_length: int = 14,
        threshold: float = 18.0,
        enabled: bool = True,
        name: str = "adx_filter",
    ) -> None:
        self.name = name
        self.adx_length = int(adx_length)
        self.threshold = float(threshold)
        self.enabled = enabled

        if self.adx_length < 2:
            raise ValueError(f"ADX length must be >= 2, got {self.adx_length}")

    # ------------------------------------------------------------------
    # Indicator computation
    # ------------------------------------------------------------------

    def compute_indicators(
        self,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray],
    ) -> None:
        """Compute ADX (Wilder's) and store in shared indicator dicts."""
        if len(df) < self.adx_length:
            adx = pd.Series(0.0, index=df.index, dtype="float32")
        else:
            adx_df = pta.adx(
                high=df["high"],
                low=df["low"],
                close=df["close"],
                length=self.adx_length,
            )
            if adx_df is None or adx_df.empty:
                adx = pd.Series(0.0, index=df.index, dtype="float32")
            else:
                adx_col = f"ADX_{self.adx_length}"
                adx = adx_df[adx_col].astype("float32").fillna(0.0)

        indicators["adx"] = adx
        ind_np["adx"] = adx.to_numpy()

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
        """Filter signals based on ADX trend strength — vectorised.

        Parameters
        ----------
        mode:
            ``"core"`` or ``"analytics"``.  Timing is always collected
            (DEC-027); ``indicator_data`` and ``indicator_values`` are only
            populated in analytics mode.
        """
        start_time = perf_counter()

        # ---- disabled fast-path ----------------------------------------
        if not self.enabled:
            # P1-CH3-3: avoid count_by_type() — use raw numpy
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

        # ---- no-signal short-circuit -----------------------------------
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
        adx = ind_np.get("adx")
        if adx is None:
            logger.error("%s: ADX indicator not found in cache.", self.name)
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
                    reason="ADX indicator not computed",
                    execution_time_ms=(perf_counter() - start_time) * 1000,
                ),
            )

        # ---- vectorised filter -----------------------------------------
        adx_values = adx.astype(np.float32)

        # mask: True = passes (ADX > threshold for any active signal)
        mask = np.ones(len(signal_values), dtype=bool)
        has_signal = signal_values > 0
        mask[has_signal] = adx_values[has_signal] > self.threshold

        filtered_signals = signal_values.copy()
        filtered_signals[~mask] = 0

        signals_out = int(np.sum(filtered_signals != 0))
        signals_rejected = signals_in - signals_out

        # ---- build output frame ----------------------------------------
        filtered_frame = SignalFrame(
            signals=pd.Series(filtered_signals, index=signal_frame.signals.index, dtype="int8"),
            indicator_data=signal_frame.indicator_data if mode == "analytics" else None,
            signal_metadata={
                "source": self.name,
                "mode": mode,
                "adx_params": {"length": self.adx_length, "threshold": self.threshold},
            },
        )

        # ---- status / reason -------------------------------------------
        if signals_out == 0:
            status, reason = FilterStatus.REJECTED, f"All signals rejected (ADX < {self.threshold})"
        elif signals_rejected == 0:
            status, reason = FilterStatus.PASSED, f"All signals passed (ADX > {self.threshold})"
        else:
            status, reason = FilterStatus.PASSED, f"{signals_rejected} signals rejected (weak trend)"

        # ---- analytics-only indicator values ---------------------------
        indicator_values = None
        if mode == "analytics" and signals_out > 0:
            sig_idx = np.where(filtered_signals != 0)[0]
            if len(sig_idx):
                indicator_values = {
                    "adx_mean": float(np.mean(adx_values[sig_idx])),
                    "adx_min":  float(np.min(adx_values[sig_idx])),
                    "adx_max":  float(np.max(adx_values[sig_idx])),
                }

        # DEC-027: always record execution time
        execution_time_ms = (perf_counter() - start_time) * 1000

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
                execution_time_ms=execution_time_ms,
            ),
        )
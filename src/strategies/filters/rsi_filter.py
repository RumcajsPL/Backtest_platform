"""RSI Filter — overbought / oversold conditions
Version: 3.0.0
BUY  rejected when RSI >= overbought.
SELL rejected when RSI <= oversold.
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


class RSIFilter:
    """RSI filter — rejects overbought BUY and oversold SELL signals.

    Implements ``FilterProtocol`` for integration with ``FilterPipeline``.
    """

    def __init__(
        self,
        length: int = 14,
        overbought: float = 70.0,
        oversold: float = 30.0,
        enabled: bool = True,
        name: str = "rsi_filter",
    ) -> None:
        self.name = name
        self.length = int(length)
        self.overbought = float(overbought)
        self.oversold = float(oversold)
        self.enabled = enabled

        if self.length < 2:
            raise ValueError(f"RSI length must be >= 2, got {self.length}")
        if self.oversold >= self.overbought:
            raise ValueError(
                f"oversold ({self.oversold}) must be < overbought ({self.overbought})"
            )

    # ------------------------------------------------------------------
    # Indicator computation
    # ------------------------------------------------------------------

    def compute_indicators(
        self,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray],
    ) -> None:
        """Compute RSI (Wilder's smoothing via pandas_ta). Fills 50.0 when insufficient data."""
        if len(df) < self.length:
            rsi = pd.Series(50.0, index=df.index, dtype="float32")
        else:
            rsi = pta.rsi(df["close"], length=self.length).astype("float32").fillna(50.0)

        indicators["rsi"] = rsi
        ind_np["rsi"]     = rsi.to_numpy()

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
        """Filter signals based on RSI levels — vectorised.

        * BUY:  rejected when ``RSI >= overbought``
        * SELL: rejected when ``RSI <= oversold``

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
        rsi = ind_np.get("rsi")
        if rsi is None:
            logger.error("%s: RSI indicator not found in cache.", self.name)
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
                    reason="RSI indicator not computed",
                    execution_time_ms=(perf_counter() - start_time) * 1000,
                ),
            )

        # ---- vectorised directional filter -----------------------------
        rsi_values = rsi.astype(np.float32)

        mask      = np.ones(len(signal_values), dtype=bool)
        buy_mask  = signal_values == 1
        sell_mask = signal_values == 2

        # BUY: pass when RSI < overbought
        mask[buy_mask]  = rsi_values[buy_mask]  < self.overbought
        # SELL: pass when RSI > oversold
        mask[sell_mask] = rsi_values[sell_mask] > self.oversold

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
                "rsi_params": {
                    "length":     self.length,
                    "overbought": self.overbought,
                    "oversold":   self.oversold,
                },
            },
        )

        if signals_out == 0:
            status, reason = FilterStatus.REJECTED, "All signals rejected by RSI"
        elif signals_rejected == 0:
            status, reason = FilterStatus.PASSED, "All signals passed RSI filter"
        else:
            status, reason = FilterStatus.PASSED, f"{signals_rejected} signals rejected (RSI out of bounds)"

        indicator_values = None
        if mode == "analytics" and signals_out > 0:
            sig_idx = np.where(filtered_signals != 0)[0]
            if len(sig_idx):
                indicator_values = {
                    "rsi_mean": float(np.mean(rsi_values[sig_idx])),
                    "rsi_min":  float(np.min(rsi_values[sig_idx])),
                    "rsi_max":  float(np.max(rsi_values[sig_idx])),
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
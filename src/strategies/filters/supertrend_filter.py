"""Supertrend Filter — ATR-based directional trend gate.
Version: 3.0.0
Logic:
-------------------------------------
* mask initialised to True (ones), then directional conditions narrow it
* NaN zeroed AFTER directional conditions (separate pass) — order matters
* BUY:  dir == 1  AND close > supertrend_price
* SELL: dir == -1 AND close < supertrend_price
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


class SupertrendFilter:
    """Supertrend filter — ATR-based directional trend gate.

    Rejects BUY signals when Supertrend is bearish (direction == -1).
    Rejects SELL signals when Supertrend is bullish (direction == 1).
    Implements ``FilterProtocol`` for integration with ``FilterPipeline``.
    """

    def __init__(
        self,
        atr_length: int = 10,
        factor: float = 3.0,
        enabled: bool = True,
        name: str = "supertrend_filter",
    ) -> None:
        self.name = name
        self.atr_length = int(atr_length)
        self.factor = float(factor)
        self.enabled = enabled

        if self.atr_length < 1:
            raise ValueError(f"ATR length must be >= 1, got {self.atr_length}")
        if self.factor <= 0:
            raise ValueError(f"Factor must be > 0, got {self.factor}")

    # ------------------------------------------------------------------
    # Indicator computation
    # ------------------------------------------------------------------

    def compute_indicators(
        self,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray],
    ) -> None:
        """Compute Supertrend (price line + direction) via pandas_ta."""
        st = pta.supertrend(
            high=df["high"],
            low=df["low"],
            close=df["close"],
            length=self.atr_length,
            multiplier=self.factor,
        )

        st_col  = f"SUPERT_{self.atr_length}_{self.factor}"
        dir_col = f"SUPERTd_{self.atr_length}_{self.factor}"

        if st is None or st.empty or st_col not in st.columns or dir_col not in st.columns:
            logger.warning("%s: Supertrend calculation failed or missing columns.", self.name)
            _nan = np.full(len(df), np.nan, dtype=np.float32)
            indicators["supertrend_price"] = pd.Series(np.nan, index=df.index, dtype="float32")
            indicators["supertrend_dir"]   = pd.Series(np.nan, index=df.index, dtype="float32")
            ind_np["supertrend_price"] = _nan.copy()
            ind_np["supertrend_dir"]   = _nan.copy()
        else:
            st_price = st[st_col].astype("float32")
            st_dir   = st[dir_col].astype("float32")

            indicators["supertrend_price"] = st_price
            indicators["supertrend_dir"]   = st_dir
            ind_np["supertrend_price"] = st_price.to_numpy()
            ind_np["supertrend_dir"]   = st_dir.to_numpy()

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
        """Filter signals based on Supertrend direction — vectorised.

        * BUY:  ``dir == 1  AND close > supertrend_price``
        * SELL: ``dir == -1 AND close < supertrend_price``
        * NaN values: zeroed out in a dedicated pass AFTER directional conditions
          (preserves exact legacy behaviour — mask starts True, not False).

        Parameters
        ----------
        mode:
            ``"core"`` or ``"analytics"``.  Timing always collected.
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
        st_price = ind_np.get("supertrend_price")
        st_dir   = ind_np.get("supertrend_dir")

        if st_price is None or st_dir is None:
            logger.error("%s: Supertrend indicator not found in cache.", self.name)
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
                    reason="Supertrend indicator not computed",
                    execution_time_ms=(perf_counter() - start_time) * 1000,
                ),
            )

        # ---- vectorised filter -----------------------------------------
        close_values    = df["close"].to_numpy(dtype=np.float32)
        st_price_values = st_price.astype(np.float32)
        st_dir_values   = st_dir.astype(np.float32)

        # EXACT legacy: mask starts True, directional conditions narrow it,
        # NaN zeroed in a separate pass afterwards.
        mask      = np.ones(len(signal_values), dtype=bool)
        buy_mask  = signal_values == 1
        sell_mask = signal_values == 2

        mask[buy_mask]  = (st_dir_values[buy_mask]  == 1)  & (close_values[buy_mask]  > st_price_values[buy_mask])
        mask[sell_mask] = (st_dir_values[sell_mask] == -1) & (close_values[sell_mask] < st_price_values[sell_mask])

        # NaN pass (after directional) — exact legacy order
        has_nan = np.isnan(st_price_values) | np.isnan(st_dir_values)
        mask[has_nan] = False

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
                "supertrend_params": {
                    "atr_length": self.atr_length,
                    "factor":     self.factor,
                },
            },
        )

        if signals_out == 0:
            status, reason = FilterStatus.REJECTED, "All signals rejected (Supertrend mismatch)"
        elif signals_rejected == 0:
            status, reason = FilterStatus.PASSED, "All signals passed (Supertrend aligned)"
        else:
            status, reason = FilterStatus.PASSED, f"{signals_rejected} signals rejected (wrong Supertrend direction)"

        indicator_values = None
        if mode == "analytics" and signals_out > 0:
            sig_idx = np.where(filtered_signals != 0)[0]
            if len(sig_idx):
                indicator_values = {
                    "bullish_signals":       int(np.sum(st_dir_values[sig_idx] == 1)),
                    "bearish_signals":       int(np.sum(st_dir_values[sig_idx] == -1)),
                    "avg_distance_to_band":  float(np.nanmean(
                        np.abs(close_values[sig_idx] - st_price_values[sig_idx])
                    )),
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
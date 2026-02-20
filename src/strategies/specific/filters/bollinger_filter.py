"""Bollinger Bands Filter — volatility regime (Bandwidth).

Migrated:  Session 5  v3.0.1
Hardened:  Session 20 Block H — P1-CH3-5 (6 unused indicator arrays removed);
           DEC-022 ("debug" → "analytics"); DEC-027 (always collect timing);
           P1-CH3-3 (count_by_type removed from hot path).

Passes signals when: bandwidth > (bandwidth_ma × filter_multiplier).
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


class BollingerFilter:
    """Bollinger Bands volatility-regime filter using Bandwidth.

    Only ``bb_bandwidth`` and ``bb_bandwidth_ma`` are stored — the raw band
    levels (upper / middle / lower) are intermediate values that ``apply_filter``
    never reads and are therefore not cached (P1-CH3-5).
    """

    def __init__(
        self,
        length: int = 14,
        width_ma_length: int = 30,
        filter_multiplier: float = 0.5,
        std_dev: float = 2.0,
        enabled: bool = True,
        name: str = "bollinger_filter",
    ) -> None:
        self.name = name
        self.length = int(length)
        self.width_ma_length = int(width_ma_length)
        self.filter_multiplier = float(filter_multiplier)
        self.std_dev = float(std_dev)
        self.enabled = enabled

        if self.length < 2:
            raise ValueError(f"Bollinger length must be >= 2, got {self.length}")
        if self.std_dev <= 0:
            raise ValueError(f"std_dev must be > 0, got {self.std_dev}")
        if self.width_ma_length < 1:
            raise ValueError(f"width_ma_length must be >= 1, got {self.width_ma_length}")

    # ------------------------------------------------------------------
    # Indicator computation
    # ------------------------------------------------------------------

    def compute_indicators(
        self,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray],
    ) -> None:
        """Compute Bandwidth and its MA.  Raw band levels are NOT stored.

        ``bb_bandwidth``    — ((upper − lower) / middle) × 100
        ``bb_bandwidth_ma`` — rolling mean of bandwidth (window = width_ma_length)

        P1-CH3-5: ``bb_lower``, ``bb_middle``, ``bb_upper`` (and their numpy
        counterparts) have been removed — they were computed but never read
        in ``apply_filter``.
        """
        _zero = lambda: pd.Series(0.0, index=df.index, dtype="float32")
        _zero_np = lambda: np.zeros(len(df), dtype=np.float32)

        if len(df) < self.length:
            indicators["bb_bandwidth"]    = _zero()
            indicators["bb_bandwidth_ma"] = _zero()
            ind_np["bb_bandwidth"]        = _zero_np()
            ind_np["bb_bandwidth_ma"]     = _zero_np()
            return

        bb = pta.bbands(df["close"], length=self.length, std=self.std_dev)
        if bb is None or bb.empty:
            logger.warning("%s: Bollinger Bands calculation failed.", self.name)
            indicators["bb_bandwidth"]    = _zero()
            indicators["bb_bandwidth_ma"] = _zero()
            ind_np["bb_bandwidth"]        = _zero_np()
            ind_np["bb_bandwidth_ma"]     = _zero_np()
            return

        lower_col  = f"BBL_{self.length}_{self.std_dev}"
        middle_col = f"BBM_{self.length}_{self.std_dev}"
        upper_col  = f"BBU_{self.length}_{self.std_dev}"

        bb_lower  = bb[lower_col].astype("float32")
        bb_middle = bb[middle_col].astype("float32")
        bb_upper  = bb[upper_col].astype("float32")

        # Bandwidth: ((upper − lower) / middle) × 100
        bandwidth = ((bb_upper - bb_lower) / bb_middle) * 100
        bandwidth = bandwidth.fillna(0.0).replace([np.inf, -np.inf], 0.0)

        bandwidth_ma = (
            bandwidth.rolling(self.width_ma_length, min_periods=self.width_ma_length)
            .mean()
            .fillna(0.0)
        )

        # Store only the two derived series needed by apply_filter
        indicators["bb_bandwidth"]    = bandwidth
        indicators["bb_bandwidth_ma"] = bandwidth_ma
        ind_np["bb_bandwidth"]        = bandwidth.to_numpy(dtype=np.float32)
        ind_np["bb_bandwidth_ma"]     = bandwidth_ma.to_numpy(dtype=np.float32)

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
        """Filter signals based on Bollinger Bandwidth — vectorised.

        Passes when: ``bandwidth > bandwidth_ma × filter_multiplier``.

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
        bandwidth    = ind_np.get("bb_bandwidth")
        bandwidth_ma = ind_np.get("bb_bandwidth_ma")

        if bandwidth is None or bandwidth_ma is None:
            logger.error("%s: Bandwidth indicators not found in cache.", self.name)
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
                    reason="Bandwidth indicators not computed",
                    execution_time_ms=(perf_counter() - start_time) * 1000,
                ),
            )

        # ---- vectorised filter -----------------------------------------
        threshold_arr = bandwidth_ma * self.filter_multiplier
        mask = bandwidth > threshold_arr

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
                "bollinger_params": {
                    "length": self.length,
                    "width_ma_length": self.width_ma_length,
                    "filter_multiplier": self.filter_multiplier,
                    "std_dev": self.std_dev,
                },
            },
        )

        if signals_out == 0:
            status, reason = FilterStatus.REJECTED, "All signals in low volatility regime"
        elif signals_rejected == 0:
            status, reason = FilterStatus.PASSED, "All signals in high volatility regime"
        else:
            status, reason = FilterStatus.PASSED, f"{signals_rejected} signals in low volatility regime"

        indicator_values = None
        if mode == "analytics" and signals_out > 0:
            sig_idx = np.where(filtered_signals != 0)[0]
            if len(sig_idx):
                indicator_values = {
                    "avg_bandwidth":    float(np.nanmean(bandwidth[sig_idx])),
                    "avg_bandwidth_ma": float(np.nanmean(bandwidth_ma[sig_idx])),
                    "avg_threshold":    float(np.nanmean(threshold_arr[sig_idx])),
                    "filter_multiplier": self.filter_multiplier,
                    "width_ma_length":  self.width_ma_length,
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
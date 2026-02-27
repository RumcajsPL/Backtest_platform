"""MA Filter — moving average slope for trend confirmation.

Migrated:  Session 5  v3.0.1  (EXACT legacy computation restored)
Hardened:  Session 20 Block H — DEC-022 ("debug" → "analytics"); DEC-027 (always
           collect timing); P1-CH3-3 (count_by_type removed from hot path).

EXACT legacy logic
------------------
* All MA types as legacy (SMA/EMA/WMA/HMA/DEMA/TEMA/KAMA/TRIMA/LSMA)
* Slope comparison: MA > MA_shift for BUY; MA < MA_shift for SELL (strict)
* NaN handling: fillna(False) — ANY NaN makes condition False
* Non-directional: same mask applied to BUY and SELL separately
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

_VALID_MA_TYPES = frozenset(
    {"SMA", "EMA", "WMA", "HMA", "DEMA", "TEMA", "KAMA", "TRIMA", "LSMA"}
)


class MAFilter:
    """MA filter — checks moving average slope for trend confirmation.

    Implements ``FilterProtocol`` for integration with ``FilterPipeline``.
    """

    def __init__(
        self,
        ma_type: str = "TEMA",
        length: int = 25,
        slope_length: int = 10,
        enabled: bool = True,
        name: str = "ma_filter",
    ) -> None:
        self.name = name
        self.ma_type = str(ma_type).upper()
        self.length = int(length)
        self.slope_length = int(slope_length)
        self.enabled = enabled

        if self.ma_type not in _VALID_MA_TYPES:
            raise ValueError(f"MA type must be one of {sorted(_VALID_MA_TYPES)}, got '{self.ma_type}'")
        if self.length < 2:
            raise ValueError(f"MA length must be >= 2, got {self.length}")
        if self.slope_length < 1:
            raise ValueError(f"slope_length must be >= 1, got {self.slope_length}")

    # ------------------------------------------------------------------
    # Indicator computation
    # ------------------------------------------------------------------

    def _calculate_ma(self, series: pd.Series) -> pd.Series:
        """Calculate MA — exact legacy function dispatch order."""
        if len(series) < self.length:
            return pd.Series(np.nan, index=series.index, dtype="float32")

        dispatch = {
            "SMA":   lambda: pta.sma(series,    length=self.length),
            "EMA":   lambda: pta.ema(series,    length=self.length),
            "WMA":   lambda: pta.wma(series,    length=self.length),
            "HMA":   lambda: pta.hma(series,    length=self.length),
            "DEMA":  lambda: pta.dema(series,   length=self.length),
            "TEMA":  lambda: pta.tema(series,   length=self.length),
            "KAMA":  lambda: pta.kama(series,   length=self.length),
            "TRIMA": lambda: pta.trima(series,  length=self.length),
            "LSMA":  lambda: pta.linreg(series, length=self.length),
        }
        return dispatch[self.ma_type]().astype("float32")

    def compute_indicators(
        self,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray],
    ) -> None:
        """Compute MA and slope-shifted MA."""
        ma = self._calculate_ma(df["close"])
        ma_ago = ma.shift(self.slope_length)

        indicators["ma"]     = ma
        indicators["ma_ago"] = ma_ago
        ind_np["ma"]         = ma.to_numpy()
        ind_np["ma_ago"]     = ma_ago.to_numpy()

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
        """Filter signals based on MA slope — vectorised.

        * BUY:  ``ma > ma_ago``  (strict; NaN → False)
        * SELL: ``ma < ma_ago``  (strict; NaN → False)

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
        ma     = ind_np.get("ma")
        ma_ago = ind_np.get("ma_ago")

        if ma is None or ma_ago is None:
            logger.error("%s: MA indicator not found in cache.", self.name)
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
                    reason="MA indicator not computed",
                    execution_time_ms=(perf_counter() - start_time) * 1000,
                ),
            )

        # ---- vectorised filter -----------------------------------------
        ma_values     = ma.astype(np.float32)
        ma_ago_values = ma_ago.astype(np.float32)

        # NaN-safe valid mask: NaN in either → condition False (legacy fillna)
        valid = ~(np.isnan(ma_values) | np.isnan(ma_ago_values))

        # Default all False
        mask      = np.zeros(len(signal_values), dtype=bool)
        buy_mask  = signal_values == 1
        sell_mask = signal_values == 2

        valid_buy  = buy_mask  & valid
        valid_sell = sell_mask & valid

        mask[valid_buy]  = ma_values[valid_buy]  > ma_ago_values[valid_buy]
        mask[valid_sell] = ma_values[valid_sell] < ma_ago_values[valid_sell]

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
                "ma_params": {
                    "type":         self.ma_type,
                    "length":       self.length,
                    "slope_length": self.slope_length,
                },
            },
        )

        if signals_out == 0:
            status, reason = FilterStatus.REJECTED, "All signals rejected (MA slope)"
        elif signals_rejected == 0:
            status, reason = FilterStatus.PASSED, "All signals passed (MA slope confirmed)"
        else:
            status, reason = FilterStatus.PASSED, f"{signals_rejected} signals rejected (MA slope)"

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
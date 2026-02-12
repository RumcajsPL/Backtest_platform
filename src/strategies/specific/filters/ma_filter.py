"""
MA Filter - Migration v3.0

Filters signals based on moving average slope for trend confirmation.
Migrated from dict-based to typed contract architecture.

Author: Migration Project
Version: 3.0.0
Date: 2025-02-11
Session: 4
"""

import pandas as pd
import pandas_ta_classic as pta
import numpy as np
import logging
from typing import Dict, Any
from time import perf_counter

from src.strategies.contracts.signal_contracts import SignalFrame
from src.strategies.contracts.filter_contracts import (
    FilterResult,
    FilterMetadata,
    FilterStatus,
    FilterProtocol
)

logger = logging.getLogger(__name__)


class MAFilter:
    """
    MA filter - checks moving average slope for trend confirmation.
    
    Supports multiple MA types (TEMA, SMA, EMA, etc.)
    Implements FilterProtocol for integration with FilterPipeline.
    """
    
    def __init__(self, ma_type: str = "TEMA", length: int = 25, slope_length: int = 10,
                 enabled: bool = True, name: str = "ma_filter"):
        """
        Initialize MA filter.
        
        Args:
            ma_type: MA type (TEMA, SMA, EMA, WMA, HMA, DEMA, KAMA, TRIMA, LSMA)
            length: MA calculation period
            slope_length: Lookback period for slope comparison
            enabled: Whether filter is active
            name: Filter name for logging
        """
        self.name = name
        self.ma_type = str(ma_type).upper()
        self.length = int(length)
        self.slope_length = int(slope_length)
        self.enabled = enabled
        
        # Available MA types
        valid_types = [
            "SMA", "EMA", "WMA", "HMA",
            "DEMA", "TEMA", "KAMA", "TRIMA", "LSMA"
        ]
        
        if self.ma_type not in valid_types:
            raise ValueError(f"MA type must be one of {valid_types}, got {self.ma_type}")
        if self.length < 2:
            raise ValueError(f"MA length must be >= 2")
        if self.slope_length < 1:
            raise ValueError(f"Slope length must be >= 1")
    
    def _calculate_ma(self, series: pd.Series) -> pd.Series:
        """Calculate moving average based on selected type."""
        if len(series) < self.length:
            return pd.Series(np.nan, index=series.index)
        
        # Standard MAs
        if self.ma_type == "SMA":
            ma = pta.sma(series, length=self.length)
        elif self.ma_type == "EMA":
            ma = pta.ema(series, length=self.length)
        elif self.ma_type == "WMA":
            ma = pta.wma(series, length=self.length)
        elif self.ma_type == "HMA":
            ma = pta.hma(series, length=self.length)
        # Advanced MAs
        elif self.ma_type == "DEMA":
            ma = pta.dema(series, length=self.length)
        elif self.ma_type == "TEMA":
            ma = pta.tema(series, length=self.length)
        elif self.ma_type == "KAMA":
            ma = pta.kama(series, length=self.length)
        elif self.ma_type == "TRIMA":
            ma = pta.trima(series, length=self.length)
        elif self.ma_type == "LSMA":
            ma = pta.linreg(series, length=self.length)
        
        return ma.astype('float32')
    
    def compute_indicators(
        self,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray]
    ) -> None:
        """
        Compute MA indicator.
        
        Args:
            df: OHLCV DataFrame
            indicators: Dict to store pandas Series
            ind_np: Dict to store numpy arrays
        """
        ma = self._calculate_ma(df['close'])
        
        # Compute slope reference (MA shifted by slope_length)
        ma_ago = ma.shift(self.slope_length)
        
        indicators['ma'] = ma
        indicators['ma_ago'] = ma_ago
        ind_np['ma'] = ma.to_numpy()
        ind_np['ma_ago'] = ma_ago.to_numpy()
    
    def apply_filter(
        self,
        signal_frame: SignalFrame,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray],
        mode: str = "core"
    ) -> FilterResult:
        """
        Filter signals based on MA slope - VECTORIZED.
        
        BUY: MA must be sloping up (MA > MA_ago)
        SELL: MA must be sloping down (MA < MA_ago)
        
        Args:
            signal_frame: Input signals to filter
            df: OHLCV DataFrame
            indicators: Cached indicators
            ind_np: Cached numpy indicators
            mode: Execution mode ("core" or "debug")
        
        Returns:
            FilterResult with MA-filtered signals
        """
        start_time = perf_counter()
        
        # If disabled, pass all signals through
        if not self.enabled:
            metadata = FilterMetadata(
                filter_name=self.name,
                status=FilterStatus.SKIPPED,
                signals_in=signal_frame.count_by_type()["total"],
                signals_out=signal_frame.count_by_type()["total"],
                signals_rejected=0,
                reason="Filter disabled"
            )
            return FilterResult(
                passed=True,
                signal_frame=signal_frame,
                metadata=metadata
            )
        
        # Count input signals
        signals_in = signal_frame.count_by_type()["total"]
        
        # If no signals, short-circuit
        if signals_in == 0:
            execution_time = (perf_counter() - start_time) * 1000
            metadata = FilterMetadata(
                filter_name=self.name,
                status=FilterStatus.SKIPPED,
                signals_in=0,
                signals_out=0,
                reason="No input signals",
                execution_time_ms=execution_time if mode == "debug" else None
            )
            return FilterResult(
                passed=False,
                signal_frame=signal_frame,
                metadata=metadata
            )
        
        # Get MA indicators
        ma = ind_np.get('ma')
        ma_ago = ind_np.get('ma_ago')
        
        if ma is None or ma_ago is None:
            logger.error(f"{self.name}: MA indicator not found in cache")
            execution_time = (perf_counter() - start_time) * 1000
            metadata = FilterMetadata(
                filter_name=self.name,
                status=FilterStatus.ERROR,
                signals_in=signals_in,
                signals_out=0,
                reason="MA indicator not computed",
                execution_time_ms=execution_time if mode == "debug" else None
            )
            return FilterResult(
                passed=False,
                signal_frame=SignalFrame(
                    signals=pd.Series(0, index=signal_frame.signals.index, dtype='int8'),
                    indicator_data=None,
                    signal_metadata={"error": "indicator_missing"}
                ),
                metadata=metadata
            )
        
        # Vectorized filtering
        signal_values = signal_frame.signals.values
        ma_values = ma.astype(np.float32)
        ma_ago_values = ma_ago.astype(np.float32)
        
        # Create mask: True = signal passes filter
        mask = np.ones(len(signal_values), dtype=bool)
        
        # BUY signals (code 1): MA must be sloping up
        buy_mask = (signal_values == 1)
        mask[buy_mask] = ma_values[buy_mask] > ma_ago_values[buy_mask]
        
        # SELL signals (code 2): MA must be sloping down
        sell_mask = (signal_values == 2)
        mask[sell_mask] = ma_values[sell_mask] < ma_ago_values[sell_mask]
        
        # Handle NaN values (early bars before slope_length)
        has_nan = np.isnan(ma_values) | np.isnan(ma_ago_values)
        mask[has_nan] = False
        
        # Apply mask to signals
        filtered_signals = signal_values.copy()
        filtered_signals[~mask] = 0  # Zero out rejected signals
        
        # Create new SignalFrame
        filtered_frame = SignalFrame(
            signals=pd.Series(filtered_signals, index=signal_frame.signals.index, dtype='int8'),
            indicator_data=signal_frame.indicator_data if mode == "debug" else None,
            signal_metadata={
                "source": "ma_filter",
                "mode": mode,
                "ma_params": {
                    "type": self.ma_type,
                    "length": self.length,
                    "slope_length": self.slope_length
                }
            }
        )
        
        # Count output signals
        signals_out = filtered_frame.count_by_type()["total"]
        signals_rejected = signals_in - signals_out
        
        # Determine status and reason
        if signals_out == 0:
            status = FilterStatus.REJECTED
            reason = "All signals rejected (MA slope mismatch)"
        elif signals_rejected == 0:
            status = FilterStatus.PASSED
            reason = "All signals passed (MA slope aligned)"
        else:
            status = FilterStatus.PASSED
            reason = f"{signals_rejected} signals rejected (wrong MA slope)"
        
        # Calculate execution time
        execution_time = (perf_counter() - start_time) * 1000
        
        # Collect indicator values for debug mode
        indicator_values = None
        if mode == "debug" and signals_out > 0:
            signal_indices = np.where(filtered_signals != 0)[0]
            if len(signal_indices) > 0:
                ma_slope = ma_values[signal_indices] - ma_ago_values[signal_indices]
                indicator_values = {
                    "ma_mean": float(np.nanmean(ma_values[signal_indices])),
                    "slope_mean": float(np.nanmean(ma_slope)),
                    "slope_max": float(np.nanmax(ma_slope))
                }
        
        # Build metadata
        metadata = FilterMetadata(
            filter_name=self.name,
            status=status,
            signals_in=signals_in,
            signals_out=signals_out,
            signals_rejected=signals_rejected,
            reason=reason,
            indicator_values=indicator_values,
            execution_time_ms=execution_time if mode == "debug" else None
        )
        
        return FilterResult(
            passed=(signals_out > 0),
            signal_frame=filtered_frame,
            metadata=metadata
        )
"""
MACD Filter - Migration v3.0 - CORRECTED
Filters signals based on MACD histogram direction.
EXACT legacy computation logic restored with new architecture.

Author: Migration Project
Version: 3.0.1
Date: 2025-02-12
Session: 5 - Final
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


class MACDFilter:
    """
    MACD filter - momentum directional filter using histogram.
    
    EXACT legacy logic restored:
    - Original parameter names: fast_length, slow_length, signal_length
    - Only histogram used for filtering
    - BUY: histogram > 0 (STRICT)
    - SELL: histogram < 0 (STRICT)
    - NaN handling: fillna(False) on condition
    - Error handling: return False on error
    """
    
    def __init__(self, 
                 fast_length: int = 12, 
                 slow_length: int = 26, 
                 signal_length: int = 9,
                 enabled: bool = True, 
                 name: str = "macd_filter"):
        """
        Initialize MACD filter with EXACT legacy parameters.
        
        Args:
            fast_length: Fast EMA period
            slow_length: Slow EMA period
            signal_length: Signal line EMA period
            enabled: Whether filter is active
            name: Filter name for logging
        """
        self.name = name
        self.fast_length = int(fast_length)
        self.slow_length = int(slow_length)
        self.signal_length = int(signal_length)
        self.enabled = enabled
        
        # Map to internal names for pandas_ta
        self.fast = self.fast_length
        self.slow = self.slow_length
        self.signal = self.signal_length
        
        if self.fast >= self.slow:
            raise ValueError(f"Fast period ({self.fast}) must be < slow period ({self.slow})")
    
    def _calculate_macd(self, series: pd.Series) -> pd.DataFrame:
        """
        Calculate MACD - EXACT legacy implementation.
        Returns DataFrame with ONLY histogram column.
        """
        if len(series) < self.slow_length:
            empty = pd.Series(np.nan, index=series.index)
            return pd.DataFrame({'histogram': empty})
        
        macd_df = pta.macd(series, fast=self.fast, slow=self.slow, signal=self.signal)
        
        if macd_df.empty:
            return pd.DataFrame({'histogram': pd.Series(np.nan, index=series.index)})
        
        histogram_col = f"MACDh_{self.fast}_{self.slow}_{self.signal}"
        
        if histogram_col not in macd_df.columns:
            raise KeyError(f"MACD histogram column '{histogram_col}' not found")
        
        # Return ONLY histogram - matches legacy
        return pd.DataFrame({'histogram': macd_df[histogram_col]})
    
    def compute_indicators(
        self,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray]
    ) -> None:
        """
        Compute MACD histogram only.
        EXACT legacy computation.
        """
        min_length = max(self.slow, self.fast) + self.signal
        
        if len(df) < min_length:
            # Insufficient data - fill with NaN (handled in apply_filter)
            indicators['macd_histogram'] = pd.Series(np.nan, index=df.index)
            ind_np['macd_histogram'] = np.full(len(df), np.nan, dtype=np.float32)
        else:
            macd_df = self._calculate_macd(df['close'])
            macd_hist = macd_df['histogram'].astype('float32')
            
            # Keep NaN values intact
            indicators['macd_histogram'] = macd_hist
            ind_np['macd_histogram'] = macd_hist.to_numpy()
    
    def apply_filter(
        self,
        signal_frame: SignalFrame,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray],
        mode: str = "core"
    ) -> FilterResult:
        """
        Filter signals based on MACD histogram.
        
        EXACT legacy logic:
        - BUY: histogram > 0 (STRICT, not >=)
        - SELL: histogram < 0 (STRICT, not <=)
        - NaN: fillna(False) - NaN values become False
        - Error: return False for all signals
        """
        start_time = perf_counter()
        
        if not self.enabled:
            metadata = FilterMetadata(
                filter_name=self.name,
                status=FilterStatus.SKIPPED,
                signals_in=signal_frame.count_by_type()["total"],
                signals_out=signal_frame.count_by_type()["total"],
                signals_rejected=0,
                reason="Filter disabled"
            )
            return FilterResult(passed=True, signal_frame=signal_frame, metadata=metadata)
        
        signals_in = signal_frame.count_by_type()["total"]
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
            return FilterResult(passed=False, signal_frame=signal_frame, metadata=metadata)
        
        # Get MACD histogram
        macd_histogram = ind_np.get('macd_histogram')
        
        if macd_histogram is None:
            execution_time = (perf_counter() - start_time) * 1000
            metadata = FilterMetadata(
                filter_name=self.name,
                status=FilterStatus.ERROR,
                signals_in=signals_in,
                signals_out=0,
                reason="MACD histogram not computed",
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
        
        signal_values = signal_frame.signals.values
        histogram_values = macd_histogram.astype(np.float32)
        
        # Initialize mask - all False by default (matches fillna(False))
        mask = np.zeros(len(signal_values), dtype=bool)
        
        # BUY: histogram > 0 (STRICT)
        buy_mask = (signal_values == 1)
        mask[buy_mask] = histogram_values[buy_mask] > 0
        
        # SELL: histogram < 0 (STRICT)
        sell_mask = (signal_values == 2)
        mask[sell_mask] = histogram_values[sell_mask] < 0
        
        # NaN handling: fillna(False) - NaN values are already False in mask
        # No need for explicit NaN handling as comparisons with NaN return False
        
        filtered_signals = signal_values.copy()
        filtered_signals[~mask] = 0
        
        filtered_frame = SignalFrame(
            signals=pd.Series(filtered_signals, index=signal_frame.signals.index, dtype='int8'),
            indicator_data=signal_frame.indicator_data if mode == "debug" else None,
            signal_metadata={
                "source": "macd_filter",
                "mode": mode,
                "macd_params": {
                    "fast_length": self.fast_length,
                    "slow_length": self.slow_length,
                    "signal_length": self.signal_length
                }
            }
        )
        
        signals_out = filtered_frame.count_by_type()["total"]
        signals_rejected = signals_in - signals_out
        execution_time = (perf_counter() - start_time) * 1000
        
        metadata = FilterMetadata(
            filter_name=self.name,
            status=FilterStatus.PASSED if signals_out > 0 else FilterStatus.REJECTED,
            signals_in=signals_in,
            signals_out=signals_out,
            signals_rejected=signals_rejected,
            reason=f"{signals_rejected} signals rejected" if signals_rejected > 0 else "All signals passed",
            execution_time_ms=execution_time if mode == "debug" else None
        )
        
        return FilterResult(
            passed=(signals_out > 0),
            signal_frame=filtered_frame,
            metadata=metadata
        )
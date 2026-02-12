"""
Pivot Filter - Migration v3.0 - CORRECTED
Filters signals based on pivot point structural analysis using swing high/low detection.
EXACT legacy computation logic restored with new architecture.

Author: Migration Project
Version: 3.0.1
Date: 2025-02-12
Session: 5 - Final
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, Tuple
from time import perf_counter
from scipy.signal import argrelextrema

from src.strategies.contracts.signal_contracts import SignalFrame
from src.strategies.contracts.filter_contracts import (
    FilterResult,
    FilterMetadata,
    FilterStatus,
    FilterProtocol
)

logger = logging.getLogger(__name__)


class PivotFilter:
    """
    Pivot filter - detects HH/HL vs LH/LL sequences for structural bias.
    
    EXACT legacy logic restored:
    - Swing high/low detection via scipy.signal.argrelextrema
    - HH/HL/LH/LL sequence analysis
    - Reversal percentage threshold
    - Forward fill bias for non-pivot bars
    - BUY: bullish bias (1), SELL: bearish bias (-1)
    """
    
    def __init__(self, 
                 reversal_percent: float = 0.2, 
                 order: int = 5,
                 enabled: bool = True, 
                 name: str = "pivot_filter"):
        """
        Initialize Pivot filter with EXACT legacy parameters.
        
        Args:
            reversal_percent: Minimum reversal percentage to confirm trend change
            order: Lookback order for extrema detection (sensitivity)
            enabled: Whether filter is active
            name: Filter name for logging
        """
        self.name = name
        self.reversal_percent = float(reversal_percent) / 100  # Convert % to decimal
        self.order = int(order)
        self.enabled = enabled
        
        if self.reversal_percent <= 0:
            raise ValueError(f"Reversal percent must be > 0, got {self.reversal_percent*100}")
        if self.order < 1:
            raise ValueError(f"Extrema order must be >= 1, got {self.order}")
    
    def _detect_swings(self, series: np.ndarray, is_high: bool = True) -> np.ndarray:
        """
        Detect swing highs/lows using argrelextrema.
        EXACT legacy implementation.
        """
        if len(series) < 2 * self.order + 1:
            return np.array([], dtype=int)
        
        comparator = np.greater if is_high else np.less
        extrema_idx = argrelextrema(series, comparator, order=self.order)[0]
        return extrema_idx
    
    def _calculate_pivot_structure(self, df: pd.DataFrame) -> pd.Series:
        """
        Calculate pivot bias (1 = bullish, -1 = bearish, 0 = neutral).
        EXACT legacy logic restored with proper forward fill.
        """
        if len(df) < 3:
            return pd.Series(0, index=df.index, dtype='int8')
        
        high = df['high'].values
        low = df['low'].values
        
        # Detect swing points
        high_idx = self._detect_swings(high, is_high=True)
        low_idx = self._detect_swings(low, is_high=False)
        
        # Convert to sets for O(1) lookup
        high_set = set(high_idx)
        low_set = set(low_idx)
        
        # Combine and sort all pivot points
        all_pivots = np.sort(np.unique(np.concatenate([high_idx, low_idx])))
        
        if len(all_pivots) < 2:
            return pd.Series(0, index=df.index, dtype='int8')
        
        bias = pd.Series(0, index=df.index, dtype='int8')
        
        # Initialize trend from first two pivots
        first_idx = all_pivots[0]
        second_idx = all_pivots[1]
        
        is_first_high = first_idx in high_set
        is_second_high = second_idx in high_set
        
        if is_first_high and not is_second_high:
            # High then low: downtrend
            trend = -1
            last_high = high[first_idx]
            last_low = low[second_idx]
            pivot_idx = 2
        elif not is_first_high and is_second_high:
            # Low then high: uptrend
            trend = 1
            last_low = low[first_idx]
            last_high = high[second_idx]
            pivot_idx = 2
        else:
            # Same type pivots - start neutral
            trend = 0
            last_high = high[0]
            last_low = low[0]
            pivot_idx = 1
        
        # Process remaining pivots
        for i in range(pivot_idx, len(all_pivots)):
            idx = all_pivots[i]
            curr_high = high[idx]
            curr_low = low[idx]
            
            is_high_pivot = idx in high_set
            is_low_pivot = idx in low_set
            
            if trend >= 0:  # Uptrend or neutral
                if is_high_pivot:
                    if curr_high > last_high and last_high > 0:
                        bias.iloc[idx] = 1  # HH - bullish
                    elif curr_high < last_high and last_high > 0:
                        bias.iloc[idx] = -1  # LH - bearish
                    last_high = curr_high
                    trend = 1
                elif is_low_pivot and trend == 1:
                    if curr_low < last_high * (1 - self.reversal_percent):
                        if curr_low > last_low:
                            bias.iloc[idx] = 1  # HL - bullish
                        elif curr_low < last_low:
                            bias.iloc[idx] = -1  # LL - bearish
                        last_low = curr_low
                        trend = -1
                    else:
                        last_low = max(last_low, curr_low)
            
            else:  # Downtrend
                if is_low_pivot:
                    if curr_low < last_low and last_low > 0:
                        bias.iloc[idx] = -1  # LL - bearish
                    elif curr_low > last_low and last_low > 0:
                        bias.iloc[idx] = 1  # HL - bullish
                    last_low = curr_low
                    trend = -1
                elif is_high_pivot and trend == -1:
                    if curr_high > last_low * (1 + self.reversal_percent):
                        if curr_high > last_high:
                            bias.iloc[idx] = 1  # HH - bullish
                        elif curr_high < last_high:
                            bias.iloc[idx] = -1  # LH - bearish
                        last_high = curr_high
                        trend = 1
                    else:
                        last_high = min(last_high, curr_high)
        
        # CRITICAL: EXACT legacy forward fill sequence
        # 1. Replace 0 with NaN
        # 2. Forward fill
        # 3. Fill remaining NaN with 0
        bias = bias.replace(0, np.nan)
        bias = bias.ffill()
        bias = bias.fillna(0)
        bias = bias.astype('int8')
        
        return bias
    
    def compute_indicators(
        self,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray]
    ) -> None:
        """
        Compute pivot structure bias.
        EXACT legacy computation.
        """
        pivot_bias = self._calculate_pivot_structure(df)
        
        indicators['pivot_bias'] = pivot_bias
        ind_np['pivot_bias'] = pivot_bias.to_numpy(dtype=np.int8)
    
    def apply_filter(
        self,
        signal_frame: SignalFrame,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray],
        mode: str = "core"
    ) -> FilterResult:
        """
        Filter signals based on pivot structure bias.
        
        EXACT legacy logic:
        - BUY: pivot_bias == 1 (bullish structure)
        - SELL: pivot_bias == -1 (bearish structure)
        - Neutral bias (0): reject all signals
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
        
        # Get pivot bias
        pivot_bias = ind_np.get('pivot_bias')
        
        if pivot_bias is None:
            execution_time = (perf_counter() - start_time) * 1000
            metadata = FilterMetadata(
                filter_name=self.name,
                status=FilterStatus.ERROR,
                signals_in=signals_in,
                signals_out=0,
                reason="Pivot bias not computed",
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
        bias_values = pivot_bias.astype(np.int8)
        
        # Initialize mask - all False by default
        mask = np.zeros(len(signal_values), dtype=bool)
        
        # BUY: bias == 1
        buy_mask = (signal_values == 1)
        mask[buy_mask] = bias_values[buy_mask] == 1
        
        # SELL: bias == -1
        sell_mask = (signal_values == 2)
        mask[sell_mask] = bias_values[sell_mask] == -1
        
        filtered_signals = signal_values.copy()
        filtered_signals[~mask] = 0
        
        filtered_frame = SignalFrame(
            signals=pd.Series(filtered_signals, index=signal_frame.signals.index, dtype='int8'),
            indicator_data=signal_frame.indicator_data if mode == "debug" else None,
            signal_metadata={
                "source": "pivot_filter",
                "mode": mode,
                "pivot_params": {
                    "reversal_percent": self.reversal_percent * 100,
                    "order": self.order
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
"""Pivot Structure Filter - Swing high/low detection using SciPy"""
import pandas as pd
import numpy as np
from scipy.signal import argrelextrema
import logging

logger = logging.getLogger(__name__)

class PivotFilter:
    """Pivot filter - detects HH/HL vs LH/LL sequences for structural bias"""
    
    def __init__(self, reversal_percent: float = 0.2, order: int = 5, enabled: bool = True):
        self.reversal_percent = float(reversal_percent) / 100  # Convert % to decimal
        self.order = int(order)  # Lookback/order for extrema (adjust for sensitivity)
        self.enabled = enabled
        
        if self.reversal_percent <= 0:
            raise ValueError(f"Reversal percent must be > 0, got {self.reversal_percent*100}")
        if self.order < 1:
            raise ValueError(f"Extrema order must be >= 1, got {self.order}")
    
    def _detect_swings(self, series: pd.Series, is_high: bool = True):
        """Detect swing highs/lows using argrelextrema (vectorized)"""
        if len(series) < 2 * self.order + 1:
            return np.array([], dtype=int)
        
        comparator = np.greater if is_high else np.less
        extrema_idx = argrelextrema(series.values, comparator, order=self.order)[0]
        return extrema_idx
    
    def _calculate_pivot_structure(self, df: pd.DataFrame) -> pd.Series:
        """Calculate pivot bias (1 = bullish, -1 = bearish, 0 = neutral) - FIXED"""
        if len(df) < 3:
            return pd.Series(0, index=df.index, dtype='int8')
        
        high = df['high']
        low = df['low']
        close = df['close']
        
        # Detect potential swing highs/lows
        high_idx = self._detect_swings(high, is_high=True)
        low_idx = self._detect_swings(low, is_high=False)
        
        # Combine and sort all pivot points
        all_pivots = np.sort(np.unique(np.concatenate([high_idx, low_idx])))
        if len(all_pivots) < 2:
            return pd.Series(0, index=df.index, dtype='int8')
        
        bias = pd.Series(0, index=df.index, dtype='int8')
        
        # Determine initial trend from first few bars (FIXED)
        # Look at first pivot to determine trend instead of assuming
        if len(all_pivots) >= 2:
            # Check if first pivot is high or low
            first_idx = all_pivots[0]
            second_idx = all_pivots[1]
            
            # Determine if first pivot is a high or low
            is_first_high = first_idx in high_idx
            is_second_high = second_idx in high_idx
            
            if is_first_high and not is_second_high:
                # High then low: currently in downtrend
                trend = -1
                last_high = high.iloc[first_idx]
                last_low = low.iloc[second_idx]
                pivot_idx = 2
            elif not is_first_high and is_second_high:
                # Low then high: currently in uptrend
                trend = 1
                last_low = low.iloc[first_idx]
                last_high = high.iloc[second_idx]
                pivot_idx = 2
            else:
                # Same type pivots, start neutral
                trend = 0
                last_high = high.iloc[0]
                last_low = low.iloc[0]
                pivot_idx = 1
        else:
            # Not enough pivots
            trend = 0
            last_high = high.iloc[0]
            last_low = low.iloc[0]
            pivot_idx = 1
        
        for i in range(pivot_idx, len(all_pivots)):
            idx = all_pivots[i]
            curr_high = high.iloc[idx]
            curr_low = low.iloc[idx]
            
            # Determine if this is a high or low pivot
            is_high_pivot = idx in high_idx
            is_low_pivot = idx in low_idx
            
            # Reversal threshold check
            if trend >= 0:  # In uptrend or neutral, expecting high
                if is_high_pivot:
                    if curr_high > last_high and last_high > 0:
                        bias.iloc[idx] = 1  # HH → bullish
                    elif curr_high < last_high and last_high > 0:
                        bias.iloc[idx] = -1  # LH → bearish
                    last_high = curr_high
                    trend = 1  # Confirm uptrend
                
                # Check for reversal down if we have a low pivot
                elif is_low_pivot and trend == 1:
                    if curr_low < last_high * (1 - self.reversal_percent):
                        # Reversal to downtrend
                        if curr_low > last_low:
                            bias.iloc[idx] = 1  # HL → bullish
                        elif curr_low < last_low:
                            bias.iloc[idx] = -1  # LL → bearish
                        last_low = curr_low
                        trend = -1
                    else:
                        # No reversal, just higher low
                        last_low = max(last_low, curr_low)
            
            else:  # In downtrend, expecting low
                if is_low_pivot:
                    if curr_low < last_low and last_low > 0:
                        bias.iloc[idx] = -1  # LL → bearish
                    elif curr_low > last_low and last_low > 0:
                        bias.iloc[idx] = 1  # HL → bullish
                    last_low = curr_low
                    trend = -1  # Confirm downtrend
                
                # Check for reversal up if we have a high pivot
                elif is_high_pivot and trend == -1:
                    if curr_high > last_low * (1 + self.reversal_percent):
                        # Reversal to uptrend
                        if curr_high > last_high:
                            bias.iloc[idx] = 1  # HH → bullish
                        elif curr_high < last_high:
                            bias.iloc[idx] = -1  # LH → bearish
                        last_high = curr_high
                        trend = 1
                    else:
                        # No reversal, just lower high
                        last_high = min(last_high, curr_high)
        
        # Forward fill bias for non-pivot bars
        bias = bias.replace(0, np.nan).ffill().fillna(0).astype('int8')
        
        return bias
    
    def apply_filter(self, df: pd.DataFrame, is_long: bool = True) -> pd.Series:
        if not self.enabled:
            return pd.Series(True, index=df.index)
        
        required_cols = ['high', 'low', 'close']
        if not all(col in df.columns for col in required_cols):
            logger.warning(f"Pivot filter requires {required_cols}")
            return pd.Series(False, index=df.index)
        
        pivot_bias = self._calculate_pivot_structure(df)
        
        if is_long:
            return pivot_bias == 1  # Bullish bias for longs
        else:
            return pivot_bias == -1  # Bearish bias for shorts
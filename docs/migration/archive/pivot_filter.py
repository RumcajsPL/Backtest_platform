# src/strategies/filters/pivot_filter.py
"""Pivot Structure Filter - Swing high/low detection for market structure"""
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class PivotFilter:
    """Pivot filter - detects HH/HL vs LH/LL sequences for structural bias"""
    
    def __init__(self, reversal_percent: float = 0.2, enabled: bool = True):
        self.reversal_percent = float(reversal_percent)  # Minimum reversal percentage
        self.enabled = enabled
        
        if self.reversal_percent <= 0:
            raise ValueError(f"Reversal percent must be > 0, got {self.reversal_percent}")
    
    def _calculate_pivot_structure(self, df: pd.DataFrame) -> pd.Series:
        """Calculate pivot structure bias (1 = bullish, -1 = bearish, 0 = neutral)"""
        if len(df) < 3:
            return pd.Series(0, index=df.index, dtype='int8')
        
        high = df['high']
        low = df['low']
        close = df['close']
        
        # Calculate reversal threshold in points
        threshold = close * (self.reversal_percent / 100)
        
        # Initialize arrays for ZigZag
        zz_high = high.copy()
        zz_low = low.copy()
        zz_last_high = 0.0
        zz_last_low = 0.0
        zz_trend = 1  # 1 = uptrend, -1 = downtrend
        zz_bias = pd.Series(0, index=df.index, dtype='int8')  # 1 = bullish, -1 = bearish
        
        # Initialize first bar
        zz_last_high = high.iloc[0]
        zz_last_low = low.iloc[0]
        
        # Process each bar
        for i in range(1, len(df)):
            current_high = high.iloc[i]
            current_low = low.iloc[i]
            current_threshold = threshold.iloc[i]
            
            if zz_trend == 1:  # Currently in uptrend
                if current_high > zz_high.iloc[i-1]:
                    zz_high.iloc[i] = current_high
                else:
                    zz_high.iloc[i] = zz_high.iloc[i-1]
                
                # Check for reversal down
                if current_low < (zz_high.iloc[i] - current_threshold):
                    # Reversal detected
                    zz_last_high = zz_high.iloc[i]
                    zz_low.iloc[i] = current_low
                    zz_trend = -1
                    
                    # Determine bias
                    if current_low > zz_last_low and zz_last_low > 0:
                        zz_bias.iloc[i] = 1  # HH + HL = bullish
                    elif current_low < zz_last_low and zz_last_low > 0:
                        zz_bias.iloc[i] = -1  # HH + LL = bearish
                    else:
                        zz_bias.iloc[i] = 0
                else:
                    zz_low.iloc[i] = zz_low.iloc[i-1]
                    zz_bias.iloc[i] = zz_bias.iloc[i-1]
                    
            else:  # Currently in downtrend
                if current_low < zz_low.iloc[i-1]:
                    zz_low.iloc[i] = current_low
                else:
                    zz_low.iloc[i] = zz_low.iloc[i-1]
                
                # Check for reversal up
                if current_high > (zz_low.iloc[i] + current_threshold):
                    # Reversal detected
                    zz_last_low = zz_low.iloc[i]
                    zz_high.iloc[i] = current_high
                    zz_trend = 1
                    
                    # Determine bias
                    if current_high > zz_last_high and zz_last_high > 0:
                        zz_bias.iloc[i] = 1  # HH + HL = bullish
                    elif current_high < zz_last_high and zz_last_high > 0:
                        zz_bias.iloc[i] = -1  # LH + LL = bearish
                    else:
                        zz_bias.iloc[i] = 0
                else:
                    zz_high.iloc[i] = zz_high.iloc[i-1]
                    zz_bias.iloc[i] = zz_bias.iloc[i-1]
        
        return zz_bias
    
    def apply_filter(self, df: pd.DataFrame, is_long: bool = True) -> pd.Series:
        """Apply Pivot Structure filter"""
        if not self.enabled:
            return pd.Series(True, index=df.index)
        
        required_cols = ['high', 'low', 'close']
        if not all(col in df.columns for col in required_cols):
            logger.warning(f"Pivot filter requires {required_cols}")
            return pd.Series(False, index=df.index)
        
        pivot_bias = self._calculate_pivot_structure(df)
        
        # Filter logic from Pine: getPivotStructureFilter(isLong) => isLong ? htfPivotBias == 1 : htfPivotBias == -1
        if is_long:
            condition = pivot_bias == 1  # Bullish bias for longs
        else:
            condition = pivot_bias == -1  # Bearish bias for shorts
        
        return condition
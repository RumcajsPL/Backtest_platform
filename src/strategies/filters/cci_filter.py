"""CCI Filter using pandas_ta"""
import pandas as pd
import pandas_ta_classic as pta
import logging
import numpy as np

logger = logging.getLogger(__name__)

class CCIFilter:
    """CCI filter - detects overbought/oversold conditions and momentum"""
    
    def __init__(self, length: int = 20, overbought: int = 100, oversold: int = -100, enabled: bool = True):
        self.length = int(length)
        self.overbought = int(overbought)
        self.oversold = int(oversold)
        self.enabled = enabled
        
        if self.length < 3:
            raise ValueError(f"CCI length must be >= 3, got {self.length}")
        if self.oversold >= self.overbought:
            raise ValueError(f"Oversold ({self.oversold}) must be < Overbought ({self.overbought})")
    
    def _calculate_cci(self, df: pd.DataFrame) -> pd.Series:
        if len(df) < self.length:
            return pd.Series(np.nan, index=df.index)
        
        cci = pta.cci(high=df['high'], low=df['low'], close=df['close'], length=self.length)
        return cci.astype('float32').fillna(0)  # Neutral fill
    
    def apply_filter(self, df: pd.DataFrame, is_long: bool = True) -> pd.Series:
        if not self.enabled:
            return pd.Series(True, index=df.index)
        
        required_cols = ['high', 'low', 'close']
        if not all(col in df.columns for col in required_cols):
            logger.warning(f"CCI filter requires {required_cols}")
            return pd.Series(False, index=df.index)
        
        cci = self._calculate_cci(df)
        
        if is_long:
            return cci < self.overbought  # Not overbought for longs
        else:
            return cci > self.oversold   # Not oversold for shorts
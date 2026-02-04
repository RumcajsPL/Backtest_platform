"""ADX Filter using pandas_ta"""
import pandas as pd
import pandas_ta_classic as pta
import logging
import numpy as np

logger = logging.getLogger(__name__)

class ADXFilter:
    """ADX filter - measures trend strength (direction-agnostic)"""
    
    def __init__(self, adx_length: int = 14, threshold: float = 18.0, enabled: bool = True):
        # di_length often same as adx_length in pandas_ta; keep for config compat
        self.adx_length = int(adx_length)
        self.threshold = float(threshold)
        self.enabled = enabled
        
        if self.adx_length < 2:
            raise ValueError(f"ADX length must be >= 2, got {self.adx_length}")
    
    def _calculate_adx(self, df: pd.DataFrame) -> pd.Series:
        if len(df) < self.adx_length:
            return pd.Series(np.nan, index=df.index)
        
        adx_df = pta.adx(high=df['high'], low=df['low'], close=df['close'], length=self.adx_length)
        if adx_df.empty:
            return pd.Series(np.nan, index=df.index)
        
        # Column: 'ADX_{length}'
        adx_col = f'ADX_{self.adx_length}'
        return adx_df[adx_col].astype('float32').fillna(0)  # No trend = 0
    
    def apply_filter(self, df: pd.DataFrame) -> pd.Series:
        if not self.enabled:
            return pd.Series(True, index=df.index)
        
        required_cols = ['high', 'low', 'close']
        if not all(col in df.columns for col in required_cols):
            logger.warning(f"ADX filter requires {required_cols}")
            return pd.Series(False, index=df.index)
        
        adx = self._calculate_adx(df)
        return adx > self.threshold
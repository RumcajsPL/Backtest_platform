# src/strategies/filters/choppiness_filter.py
import pandas as pd
import pandas_ta_classic as pta
import logging

logger = logging.getLogger(__name__)

class ChoppinessFilter:
    def __init__(self, length: int = 14, threshold: float = 61.8, ln: bool = False, enabled: bool = True):
        self.length = int(length)
        self.threshold = float(threshold)
        self.ln = ln  # False = log10 (matches original)
        self.enabled = enabled
        
        if self.length < 2:
            raise ValueError(f"Length must be >= 2")
    
    def _calculate_choppiness(self, df: pd.DataFrame) -> pd.Series:
        if len(df) < self.length:
            return pd.Series(50.0, index=df.index)
        
        ci = pta.chop(df['high'], df['low'], df['close'], length=self.length, ln=self.ln)
        ci = ci.clip(0, 100).astype('float32').fillna(50.0)
        return ci
    
    def apply_filter(self, df: pd.DataFrame) -> pd.Series:
        if not self.enabled:
            return pd.Series(True, index=df.index)
        
        required = ['high', 'low', 'close']
        if not all(col in df.columns for col in required):
            logger.warning(f"Choppiness requires {required}")
            return pd.Series(False, index=df.index)
        
        ci = self._calculate_choppiness(df)
        return ci < self.threshold
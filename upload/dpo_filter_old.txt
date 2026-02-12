# src/strategies/filters/dpo_filter.py
import pandas as pd
import pandas_ta_classic as pta
import logging
import numpy as np

logger = logging.getLogger(__name__)

class DPOFilter:
    def __init__(self, length: int = 20, smooth: int = 3, threshold: float = 0.2, 
                 centered: bool = False, enabled: bool = True):  # added centered for matching original
        self.length = int(length)
        self.smooth = int(smooth) if smooth > 0 else 1
        self.threshold = float(threshold)
        self.centered = centered  # False to match your shifted SMA original
        self.enabled = enabled
        
        if self.length < 3:
            raise ValueError(f"DPO length must be >= 3, got {self.length}")
    
    def _calculate_dpo(self, series: pd.Series) -> pd.Series:
        if len(series) < self.length:
            return pd.Series(np.nan, index=series.index)
        
        dpo_raw = pta.dpo(series, length=self.length, centered=self.centered)
        
        # Optional smoothing (not in pta, so apply if needed)
        if self.smooth > 1:
            dpo = dpo_raw.rolling(window=self.smooth, min_periods=self.smooth).mean()
        else:
            dpo = dpo_raw
        
        # Normalize as % (your original)
        dpo_norm = (dpo / series) * 100
        
        return dpo_norm.astype('float32').fillna(0)
    
    def apply_filter(self, df: pd.DataFrame, is_long: bool = True) -> pd.Series:
        if not self.enabled:
            return pd.Series(True, index=df.index)
        
        if 'close' not in df.columns:
            logger.warning("DPO filter requires 'close' column")
            return pd.Series(False, index=df.index)
        
        dpo = self._calculate_dpo(df['close'])
        
        if is_long:
            return (dpo < 0) & (dpo > -self.threshold)
        else:
            return (dpo > 0) & (dpo < self.threshold)
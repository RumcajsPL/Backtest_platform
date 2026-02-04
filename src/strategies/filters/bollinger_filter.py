# src/strategies/filters/bollinger_filter.py
import pandas as pd
import pandas_ta_classic as pta
import logging
import numpy as np

logger = logging.getLogger(__name__)

class BollingerFilter:
    def __init__(self, length: int = 14, width_ma_length: int = 30, 
                 filter_multiplier: float = 0.5, std_dev: float = 2.0, enabled: bool = True):
        self.length = int(length)
        self.width_ma_length = int(width_ma_length)
        self.filter_multiplier = float(filter_multiplier)
        self.std_dev = float(std_dev)
        self.enabled = enabled
        
        if self.length < 2:
            raise ValueError(f"Length must be >= 2")
    
    def _calculate_bbw(self, series: pd.Series) -> tuple[pd.Series, pd.Series]:
        if len(series) < self.length:
            empty = pd.Series(np.nan, index=series.index)
            return empty, empty
        
        bb = pta.bbands(series, length=self.length, std=self.std_dev)
        if bb is None or bb.empty:
            return pd.Series(np.nan, index=series.index), pd.Series(np.nan, index=series.index)
        
        basis = bb[f'BBM_{self.length}_{self.std_dev}']
        upper = bb[f'BBU_{self.length}_{self.std_dev}']
        lower = bb[f'BBL_{self.length}_{self.std_dev}']
        
        bandwidth = ((upper - lower) / basis) * 100
        bandwidth_ma = bandwidth.rolling(self.width_ma_length, min_periods=self.width_ma_length).mean() if self.width_ma_length > 0 else pd.Series(np.nan, index=series.index)
        
        return bandwidth.astype('float32'), bandwidth_ma.astype('float32')
    
    def apply_filter(self, df: pd.DataFrame) -> pd.Series:
        if not self.enabled:
            return pd.Series(True, index=df.index)
        
        if 'close' not in df.columns:
            logger.warning("Bollinger filter requires 'close'")
            return pd.Series(False, index=df.index)
        
        bandwidth, bandwidth_ma = self._calculate_bbw(df['close'])
        valid_mask = bandwidth.notna() & bandwidth_ma.notna()
        
        result = pd.Series(True, index=df.index)
        if valid_mask.any():
            result.loc[valid_mask] = bandwidth > (bandwidth_ma * self.filter_multiplier)
        
        return result
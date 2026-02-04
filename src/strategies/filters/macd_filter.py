"""MACD Filter (Classic only) using pandas_ta"""
import pandas as pd
import pandas_ta_classic as pta
import logging
import numpy as np

logger = logging.getLogger(__name__)

class MACDFilter:
    """MACD filter - classic histogram direction for trend/momentum confirmation"""
    
    def __init__(self, fast_length: int = 12, slow_length: int = 26, 
                 signal_length: int = 9, enabled: bool = True):
        self.fast_length = int(fast_length)
        self.slow_length = int(slow_length)
        self.signal_length = int(signal_length)
        self.enabled = enabled
        
        if self.fast_length >= self.slow_length:
            raise ValueError(f"Fast length ({self.fast_length}) must be < Slow length ({self.slow_length})")
    
    def _calculate_macd(self, series: pd.Series) -> pd.DataFrame:
        if len(series) < self.slow_length:
            empty = pd.Series(np.nan, index=series.index)
            return pd.DataFrame({
                'macd': empty, 'signal': empty, 'histogram': empty
            })
        
        macd_df = pta.macd(series, fast=self.fast_length, slow=self.slow_length, 
                           signal=self.signal_length)
        
        if macd_df.empty:
            return pd.DataFrame({
                'macd': pd.Series(np.nan, index=series.index),
                'signal': pd.Series(np.nan, index=series.index),
                'histogram': pd.Series(np.nan, index=series.index)
            })
        
        # Build the exact column names pandas_ta uses
        histogram_col = f"MACDh_{self.fast_length}_{self.slow_length}_{self.signal_length}"
        
        # Check if histogram column exists
        if histogram_col not in macd_df.columns:
            error_msg = f"MACD histogram column '{histogram_col}' not found. "
            error_msg += f"Available columns: {list(macd_df.columns)}"
            raise KeyError(error_msg)
        
        # Return with standard column names
        return pd.DataFrame({
            'histogram': macd_df[histogram_col]
        })
    
    def apply_filter(self, df: pd.DataFrame, is_long: bool = True) -> pd.Series:
        if not self.enabled:
            return pd.Series(True, index=df.index)
        
        if 'close' not in df.columns:
            logger.warning("MACD filter requires 'close' column")
            return pd.Series(False, index=df.index)
        
        try:
            macd_df = self._calculate_macd(df['close'])
            histogram = macd_df['histogram']
            
            # Classic mode: histogram direction
            condition = histogram > 0 if is_long else histogram < 0
            
            # Handle NaN as False (no confirmation)
            return condition.fillna(False)
            
        except KeyError as e:
            logger.error(f"MACD calculation error: {e}")
            return pd.Series(False, index=df.index)
        except Exception as e:
            logger.error(f"MACD filter failed: {e}")
            return pd.Series(False, index=df.index)
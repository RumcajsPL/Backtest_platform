"""MA Filter using pandas_ta"""
import pandas as pd
import pandas_ta_classic as pta
import logging
import numpy as np

logger = logging.getLogger(__name__)

class MAFilter:
    """MA filter - checks moving average slope for trend confirmation"""
    
    def __init__(self, ma_type: str = "TEMA", length: int = 25, 
                 slope_length: int = 10, enabled: bool = True):
        self.ma_type = str(ma_type).upper()
        self.length = int(length)
        self.slope_length = int(slope_length)
        self.enabled = enabled
        
        # Available MA types in pandas_ta_classic (excluding unavailable ones)
        valid_types = [
            # Standard MAs
            "SMA", "EMA", "WMA", "HMA",
            # Advanced MAs (available in your version)
            "DEMA", "TEMA", "KAMA", "TRIMA", "LSMA"
        ]
        
        if self.ma_type not in valid_types:
            raise ValueError(f"MA type must be one of {valid_types}, got {self.ma_type}")
        if self.length < 2:
            raise ValueError(f"MA length must be >= 2")
        if self.slope_length < 1:
            raise ValueError(f"Slope length must be >= 1")
    
    def _calculate_ma(self, series: pd.Series) -> pd.Series:
        """Calculate moving average based on selected type"""
        if len(series) < self.length:
            return pd.Series(np.nan, index=series.index)
        
        # Standard MAs
        if self.ma_type == "SMA":
            ma = pta.sma(series, length=self.length)
        elif self.ma_type == "EMA":
            ma = pta.ema(series, length=self.length)
        elif self.ma_type == "WMA":
            ma = pta.wma(series, length=self.length)
        elif self.ma_type == "HMA":
            ma = pta.hma(series, length=self.length)
        
        # Advanced MAs (available in your version)
        elif self.ma_type == "DEMA":
            ma = pta.dema(series, length=self.length)
        elif self.ma_type == "TEMA":
            ma = pta.tema(series, length=self.length)
        elif self.ma_type == "KAMA":
            ma = pta.kama(series, length=self.length)
        elif self.ma_type == "TRIMA":
            ma = pta.trima(series, length=self.length)
        elif self.ma_type == "LSMA":
            ma = pta.linreg(series, length=self.length)  # Linear regression/Least Squares
        
        return ma.astype('float32')
    
    def apply_filter(self, df: pd.DataFrame, is_long: bool = True) -> pd.Series:
        """Apply MA slope filter"""
        if not self.enabled:
            return pd.Series(True, index=df.index)
        
        if 'close' not in df.columns:
            logger.warning("MA filter requires 'close' column")
            return pd.Series(False, index=df.index)
        
        ma = self._calculate_ma(df['close'])
        ma_slope_len_ago = ma.shift(self.slope_length)
        
        if is_long:
            condition = ma > ma_slope_len_ago  # MA sloping up
        else:
            condition = ma < ma_slope_len_ago  # MA sloping down
        
        return condition.fillna(False)
"""RSI Filter using pandas-ta-classic"""
import pandas as pd
import pandas_ta_classic as pta
import logging

logger = logging.getLogger(__name__)

class RSIFilter:
    """RSI filter - rejects overbought BUY signals and oversold SELL signals"""
    
    def __init__(self, length: int = 14, overbought: float = 70.0, oversold: float = 30.0, enabled: bool = True):
        self.length = int(length)
        self.overbought = float(overbought)
        self.oversold = float(oversold)
        self.enabled = enabled
        
        if self.length < 2:
            raise ValueError(f"RSI length must be >= 2, got {self.length}")
    
    def _calculate_rsi(self, series: pd.Series) -> pd.Series:
        """Calculate RSI using pandas_ta (Wilder's smoothing, matches TradingView)"""
        if len(series) < self.length:
            return pd.Series(50.0, index=series.index)  # Neutral fill
        
        rsi = pta.rsi(series, length=self.length)
        return rsi.astype('float32').fillna(50.0)
    
    def apply_filter(self, df: pd.DataFrame, is_long: bool = True) -> pd.Series:
        if not self.enabled:
            return pd.Series(True, index=df.index)
        
        if 'close' not in df.columns:
            logger.warning("RSI filter requires 'close' column")
            return pd.Series(False, index=df.index)
        
        rsi = self._calculate_rsi(df['close'])
        
        if is_long:
            return rsi < self.overbought  # Not overbought for longs
        else:
            return rsi > self.oversold   # Not oversold for shorts
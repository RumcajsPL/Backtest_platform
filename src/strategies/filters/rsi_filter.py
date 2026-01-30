"""RSI filter using Wilder's smoothing (matches TradingView)"""
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

class RSIFilter:
    """RSI filter - rejects overbought BUY signals and oversold SELL signals"""

    def __init__(self, length=14, overbought=70, oversold=30, enabled=True):
        self.length = int(length)
        self.overbought = float(overbought)
        self.oversold = float(oversold)
        self.enabled = enabled

    def _calculate_rsi_wilder(self, series: pd.Series) -> pd.Series:
        """Calculate RSI using Wilder's Smoothing (RMA) - matches TradingView ta.rsi()"""
        delta = series.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)

        avg_gain = gain.ewm(alpha=1/self.length, min_periods=self.length, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/self.length, min_periods=self.length, adjust=False).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        # Fill NaN with neutral RSI value
        rsi = rsi.fillna(50.0)
        
        # Convert to float32 for memory efficiency
        return rsi.astype('float32')

    def apply_filter(self, df: pd.DataFrame, is_long: bool = True, price_col: str = 'close') -> pd.Series:
        """
        Apply RSI filter logic       
        """
        if not self.enabled:
            return pd.Series(True, index=df.index)
        
        # Calculate RSI directly on input df (no copy needed)
        rsi = self._calculate_rsi_wilder(df[price_col])
        
        # Apply filter: BUY when not overbought, SELL when not oversold
        if is_long:
            return rsi < self.overbought
        else:
            return rsi > self.oversold
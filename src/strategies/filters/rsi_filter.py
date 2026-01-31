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

    #def apply_filter_with_rsi(self, df: pd.DataFrame, rsi: pd.Series, is_long: bool = True) -> pd.Series:
        """Apply filter using pre-calculated RSI (avoids recalculation)"""
        if not self.enabled:
            return pd.Series(True, index=df.index)
        
        if is_long:
            return rsi < self.overbought
        else:
            return rsi > self.oversold
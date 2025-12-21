import pandas as pd
import numpy as np

class RSIFilter:
    """
    RSI Filter strategy translated from Pine Script.
    
    Pine Origin:
    rsi = ta.rsi(close, rsiLen)
    getRSIFilter(isLong) =>
        if not useRSI
            true
        else
            isLong ? rsi < rsiOverbought : rsi > rsiOversold
    """

    def __init__(self, length=14, overbought=70, oversold=30, enabled=True):
        self.length = int(length)
        self.overbought = float(overbought)
        self.oversold = float(oversold)
        self.enabled = enabled

    def _calculate_rsi_wilder(self, series):
        """
        Calculates RSI using Wilder's Smoothing (RMA) to match TradingView logic.
        """
        delta = series.diff()
        
        # separate gains and losses
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)

        # Calculate Exponential Moving Average (Wilder's method uses alpha = 1/n)
        # Note: adjust=False is crucial for recursiveness matching Pine's rma()
        avg_gain = gain.ewm(alpha=1/self.length, min_periods=self.length, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/self.length, min_periods=self.length, adjust=False).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        # Handle division by zero edge cases if avg_loss is 0
        rsi = rsi.fillna(100 if len(loss) > 0 else 50)
        
        return rsi

    def apply_filter(self, df: pd.DataFrame, is_long: bool) -> pd.Series:
        """
        Applies the filter logic.
        Returns a boolean Series: True if the trade is ALLOWED, False if filtered out.
        """
        # 1. Check if filter is disabled
        if not self.enabled:
            return pd.Series([True] * len(df), index=df.index)

        # 2. Ensure RSI is calculated
        # We check if 'rsi' exists to avoid recalculating if run multiple times
        if 'rsi' not in df.columns:
            # Assumes 'close' or 'Close' column exists
            price_col = 'close' if 'close' in df.columns else 'Close'
            df['rsi'] = self._calculate_rsi_wilder(df[price_col])

        # 3. Apply Logic
        # Pine: isLong ? rsi < rsiOverbought : rsi > rsiOversold
        if is_long:
            # Long Logic: Allow if RSI has NOT hit the overbought ceiling (Room to grow)
            return df['rsi'] < self.overbought
        else:
            # Short Logic: Allow if RSI has NOT hit the oversold floor (Room to drop)
            return df['rsi'] > self.oversold
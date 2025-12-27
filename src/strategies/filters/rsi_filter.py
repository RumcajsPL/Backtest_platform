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

    def apply_filter(self, df: pd.DataFrame, is_long: bool = True, price_col: str = 'close') -> pd.Series:
        """
        Apply RSI filter logic.
        
        Args:
            df: OHLCV DataFrame
            is_long: True for buy signals (check not overbought), False for sell (check not oversold)
            price_col: Column to use for RSI calculation
            
        Returns:
            Boolean Series: True where condition is met
        """
        if not self.enabled:
            return pd.Series(True, index=df.index)
        
        # CRITICAL FIX: Work on a copy to avoid SettingWithCopyWarning
        df_copy = df.copy()
        
        # Calculate RSI (will modify df_copy, not original df)
        df_copy['rsi'] = self._calculate_rsi_wilder(df_copy[price_col])
        
        # Apply filter logic
        if is_long:
            # For BUY signals: RSI should NOT be overbought
            result = df_copy['rsi'] < self.overbought
        else:
            # For SELL signals: RSI should NOT be oversold
            result = df_copy['rsi'] > self.oversold
        
        return result
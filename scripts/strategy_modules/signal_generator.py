"""
WBWS Signal Generation Module
"""
import pandas as pd
import numpy as np
from typing import Dict, Tuple

class SignalGenerator:
    def __init__(self, htf_period: str = "60min"):
        self.htf_period = htf_period
        
    def generate_signals(self, df: pd.DataFrame) -> Tuple[pd.Series, pd.DataFrame]:
        """
        Generate WBWS trigger signals
        
        Args:
            df: OHLCV DataFrame with timestamp index
            
        Returns:
            Tuple of (signal_series, signals_dataframe)
        """
        try:
            from src.indicators.wbws_trigger import WBWSTrigger
        except ImportError:
            print("⚠️  Could not import WBWSTrigger. Using mock signals for testing.")
            return self._generate_mock_signals(df)
        
        indicator = WBWSTrigger(htf_period=self.htf_period)
        signals_df = indicator.calculate_signals(df)
        
        # Align index
        if not signals_df.index.equals(df.index):
            signals_df.index = df.index
            
        # Create signal series
        raw_signals = pd.Series(index=df.index, dtype=object)
        raw_signals.loc[signals_df['we_buy'] == True] = 'BUY'
        raw_signals.loc[signals_df['we_sell'] == True] = 'SELL'
        
        return raw_signals, signals_df
    
    def _generate_mock_signals(self, df: pd.DataFrame) -> Tuple[pd.Series, pd.DataFrame]:
        """Generate mock signals for testing when WBWSTrigger is not available"""
        np.random.seed(42)
        n = len(df)
        
        # Create mock signals dataframe
        signals_df = pd.DataFrame(index=df.index)
        signals_df['we_buy'] = np.random.choice([True, False], n, p=[0.05, 0.95])
        signals_df['we_sell'] = np.random.choice([True, False], n, p=[0.05, 0.95])
        
        # Ensure no conflicting signals
        signals_df.loc[signals_df['we_buy'] & signals_df['we_sell'], 'we_sell'] = False
        
        # Create signal series
        raw_signals = pd.Series(index=df.index, dtype=object)
        raw_signals.loc[signals_df['we_buy']] = 'BUY'
        raw_signals.loc[signals_df['we_sell']] = 'SELL'
        
        return raw_signals, signals_df
    
    def get_signal_stats(self, signals: pd.Series) -> Dict:
        """Get statistics about generated signals"""
        if signals is None or signals.empty:
            return {'buy': 0, 'sell': 0, 'total': 0}
        
        buy_count = int((signals == 'BUY').sum())
        sell_count = int((signals == 'SELL').sum())
        
        return {
            'buy': buy_count,
            'sell': sell_count,
            'total': buy_count + sell_count,
            'buy_percentage': buy_count / (buy_count + sell_count) * 100 if (buy_count + sell_count) > 0 else 0,
            'sell_percentage': sell_count / (buy_count + sell_count) * 100 if (buy_count + sell_count) > 0 else 0
        }
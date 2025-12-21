# src/indicators/wbws_trigger.py
"""
We Buy / We Sell Trigger Indicator - Pure Calculation Engine

SCOPE: Signal calculation logic ONLY
- Candle classification (inside, outside, 2u, 2d)
- Reversal pattern detection
- HTF trend alignment
- Signal generation

ASSUMPTIONS: Input DataFrame MUST be preprocessed with:
- Index: DatetimeIndex (timestamp as index)
- Columns: 'open', 'high', 'low', 'close', 'volume' (lowercase, no missing values)
- Sorted by timestamp ascending
- No duplicates

PREPROCESSING: Use scripts/data_preprocessing/ to prepare data
"""
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Tuple, Optional

class WBWSTrigger:
    """
    Pure signal calculation engine for We Buy / We Sell Trigger indicator.
    
    Pine Script Translation:
    - Uses higher timeframe for trend bias (default 60min)
    - Classifies 1-minute candles into 4 types
    - Triggers on specific reversal patterns
    - Requires HTF trend alignment
    """
    
    def __init__(self, htf_period: str = '60min'):
        """
        Initialize the indicator.
        
        Args:
            htf_period: Higher timeframe period ('60min', '30min', '4H', '1D', etc.)
        """
        self.htf_period = htf_period
        self.signals_df = None
        self.execution_time = None
        self.execution_stats = None
        
    def _validate_input(self, df: pd.DataFrame):
        """
        Validate input DataFrame meets requirements.
        
        Raises:
            ValueError: If DataFrame doesn't meet requirements
        """
        # Check index is DatetimeIndex
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("DataFrame index must be DatetimeIndex. Use prepare_dataframe() first.")
        
        # Check required columns exist
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}. Use validate_schema() first.")
        
        # Check for missing values
        if df[required_cols].isnull().any().any():
            raise ValueError("DataFrame contains missing values. Clean data first.")
    
    def prepare_htf_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Resample to higher timeframe and calculate HTF conditions.
        
        Args:
            df: 1-minute OHLCV data (with DatetimeIndex)
            
        Returns:
            Tuple of (df_with_htf, df_htf)
        """
        # Create HTF data
        df_htf = df.resample(self.htf_period).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        # HTF conditions (exact Pine logic)
        df_htf['htf_bull'] = (
            df_htf['close'].notna() & 
            df_htf['open'].notna() & 
            (df_htf['close'] > df_htf['open'])
        )
        df_htf['htf_bear'] = (
            df_htf['close'].notna() & 
            df_htf['open'].notna() & 
            (df_htf['close'] < df_htf['open'])
        )
        
        # Forward fill to base timeframe
        df_copy = df.copy()
        df_copy['htf_bull'] = df_htf['htf_bull'].reindex(df.index, method='ffill').fillna(False)
        df_copy['htf_bear'] = df_htf['htf_bear'].reindex(df.index, method='ffill').fillna(False)
        
        return df_copy, df_htf
    
    def classify_candle(self, current_bar: pd.Series, previous_bar: pd.Series) -> Optional[int]:
        """
        Classify a candle according to Pine Script logic.
        
        Returns:
            1: Inside bar, 3: Outside bar, 2: 2u, -2: 2d, None: Not classified
        """
        # Check for NA values
        if (pd.isna(previous_bar['high']) or pd.isna(previous_bar['low']) or
            pd.isna(current_bar['high']) or pd.isna(current_bar['low'])):
            return None
        
        # Pine Script classification
        if (current_bar['high'] <= previous_bar['high'] and 
            current_bar['low'] >= previous_bar['low']):
            return 1  # Inside
            
        elif (current_bar['high'] > previous_bar['high'] and 
              current_bar['low'] < previous_bar['low']):
            return 3  # Outside
            
        elif (current_bar['high'] > previous_bar['high'] and 
              current_bar['low'] >= previous_bar['low']):
            return 2  # 2u
            
        elif (current_bar['low'] < previous_bar['low'] and 
              current_bar['high'] <= previous_bar['high']):
            return -2  # 2d
        
        return None
    
    def calculate_signals(self, df_ohlcv: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
        """
        Calculate We Buy/We Sell signals.
        
        Args:
            df_ohlcv: Preprocessed OHLCV DataFrame (DatetimeIndex, required columns)
            verbose: If True, print detailed progress
            
        Returns:
            DataFrame with signals added
            
        Raises:
            ValueError: If input DataFrame doesn't meet requirements
        """
        self.execution_time = datetime.now()
        
        # Validate input
        self._validate_input(df_ohlcv)
        
        # Prepare HTF data
        df, df_htf = self.prepare_htf_data(df_ohlcv)
        
        # Reset index for operations (but keep timestamp as column)
        df = df.reset_index()
        
        if verbose:
            print(f"      Processing {len(df):,} bars...")
        
        # Classify candles
        candle_types = []
        for i in range(len(df)):
            if i == 0:
                candle_types.append(np.nan)
            else:
                candle_type = self.classify_candle(df.iloc[i], df.iloc[i-1])
                candle_types.append(candle_type if candle_type is not None else np.nan)
        
        df['candle_type'] = candle_types
        
        # Detect reversals with explicit NA checks
        df['rev_2d_2u'] = (
            df['candle_type'].notna() &
            df['candle_type'].shift(1).notna() &
            (df['candle_type'].shift(1) == -2) &
            (df['candle_type'] == 2)
        )
        
        df['rev_2u_2d'] = (
            df['candle_type'].notna() &
            df['candle_type'].shift(1).notna() &
            (df['candle_type'].shift(1) == 2) &
            (df['candle_type'] == -2)
        )
        
        # Generate signals
        df['we_buy'] = df['rev_2d_2u'] & df['htf_bull']
        df['we_sell'] = df['rev_2u_2d'] & df['htf_bear']
        
        # Store results
        self.signals_df = df
        
        # Calculate stats
        buy_count = int(df['we_buy'].sum())
        sell_count = int(df['we_sell'].sum())
        total_signals = buy_count + sell_count
        
        if verbose:
            print(f"      Signals: {total_signals:,} total ({buy_count:,} buy, {sell_count:,} sell)")
        
        self.execution_stats = {
            'execution_time': self.execution_time.isoformat(),
            'htf_period': self.htf_period,
            'total_bars': len(df),
            'data_period': {
                'start': df['timestamp'].min().isoformat(),
                'end': df['timestamp'].max().isoformat()
            },
            'signals': {
                'buy': buy_count,
                'sell': sell_count,
                'total': total_signals
            }
        }
        
        return df
    
    def get_signals(self) -> pd.DataFrame:
        """
        Get the calculated signals DataFrame.
        
        Returns:
            DataFrame with all signals and calculations
            
        Raises:
            ValueError: If calculate_signals() hasn't been run yet
        """
        if self.signals_df is None:
            raise ValueError("No signals calculated. Run calculate_signals() first.")
        return self.signals_df
    
    def get_execution_stats(self) -> dict:
        """
        Get execution statistics.
        
        Returns:
            Dictionary with execution statistics
            
        Raises:
            ValueError: If calculate_signals() hasn't been run yet
        """
        if self.execution_stats is None:
            raise ValueError("No execution stats available. Run calculate_signals() first.")
        return self.execution_stats
    
    def print_summary(self):
        """
        Print execution summary (deprecated - use report_generator instead).
        Kept for backwards compatibility.
        """
        if self.execution_stats is None:
            return
        
        # Minimal output - just counts
        stats = self.execution_stats
        print(f"   Signals: {stats['signals']['total']:,} ({stats['signals']['buy']:,} buy, {stats['signals']['sell']:,} sell)")
import pandas as pd
import numpy as np
import logging
from typing import Tuple

logger = logging.getLogger(__name__)

class WBWSTrigger:
    def __init__(self, htf_period: str):
        if not htf_period:
            raise ValueError("htf_period argument is mandatory.")
        self.htf_period = htf_period
        self.signals_df = None
        
    def _validate_input(self, df: pd.DataFrame):
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("DataFrame index must be DatetimeIndex.")
        required = ['open', 'high', 'low', 'close']
        if not all(col in df.columns for col in required):
            raise ValueError(f"Missing required columns.")

    def prepare_htf_data(self, df: pd.DataFrame, df_htf: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if df_htf is None or df_htf.empty:
            raise ValueError("HTF data is required.")
        
        df_htf = df_htf.copy()
        df_htf['htf_bull'] = (df_htf['close'] > df_htf['open'])
        df_htf['htf_bear'] = (df_htf['close'] < df_htf['open'])
        
        # Anti-Lookahead with fill_value to maintain bool type
        df_htf['htf_bull'] = df_htf['htf_bull'].shift(1, fill_value=False)
        df_htf['htf_bear'] = df_htf['htf_bear'].shift(1, fill_value=False)
        
        df_copy = df.copy()
        with pd.option_context('future.no_silent_downcasting', True):
            # Alignment using boolean types
            df_copy['htf_bull'] = df_htf['htf_bull'].reindex(df.index, method='ffill').fillna(False).astype(bool)
            df_copy['htf_bear'] = df_htf['htf_bear'].reindex(df.index, method='ffill').fillna(False).astype(bool)
        
        return df_copy, df_htf

    def _classify_candles_vectorized(self, df: pd.DataFrame) -> np.ndarray:
       
        high = df['high'].values.astype(np.float32)
        low = df['low'].values.astype(np.float32)
        
        high_prev = np.full_like(high, np.nan)
        high_prev[1:] = high[:-1]
        low_prev = np.full_like(low, np.nan)
        low_prev[1:] = low[:-1]
                
        candle_types = np.full(len(df), -128, dtype=np.int8)
        
        # Masks
        outside = (high > high_prev) & (low < low_prev)
        inside = (high <= high_prev) & (low >= low_prev)
        two_u = (high > high_prev) & (low >= low_prev) & ~outside
        two_d = (low < low_prev) & (high <= high_prev) & ~outside
        
        candle_types[inside] = 1
        candle_types[outside] = 3
        candle_types[two_u] = 2
        candle_types[two_d] = -2
        
        return candle_types

    def calculate_signals(self, df_ohlcv: pd.DataFrame, df_htf: pd.DataFrame) -> pd.DataFrame:
        self._validate_input(df_ohlcv)
        df, _ = self.prepare_htf_data(df_ohlcv, df_htf)
                
        df['candle_type'] = self._classify_candles_vectorized(df)
        
        # Reversal Logic
        c = df['candle_type']
        c_prev = c.shift(1, fill_value=-128)
                
        df['rev_2d_2u'] = (c_prev == -2) & (c == 2)
        df['rev_2u_2d'] = (c_prev == 2) & (c == -2)
                
        df['we_buy'] = df['rev_2d_2u'] & df['htf_bull']
        df['we_sell'] = df['rev_2u_2d'] & df['htf_bear']
              
        df.drop(columns=['rev_2d_2u', 'rev_2u_2d'], inplace=True)
        
        self.signals_df = df
        return df

    def get_signals(self) -> pd.DataFrame:
        if self.signals_df is None: raise ValueError("Run calculate_signals() first.")
        return self.signals_df
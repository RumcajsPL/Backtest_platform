import pandas as pd
import numpy as np
import logging
from typing import Tuple

logger = logging.getLogger(__name__)

class WBWSTrigger:
    """
    Optimized WBWS Trigger:
    - Faster HTF alignment
    - Reduced DataFrame writes
    - Fully vectorized candle classification
    - Slimmer output (only needed columns)
    """

    def __init__(self, htf_period: str):
        if not htf_period:
            raise ValueError("htf_period argument is mandatory.")
        self.htf_period = htf_period
        self.signals_df = None

    def _validate_input(self, df: pd.DataFrame):
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("DataFrame index must be DatetimeIndex.")
        required = ["open", "high", "low", "close"]
        if not all(col in df.columns for col in required):
            raise ValueError("Missing required OHLC columns.")

    def prepare_htf_data(
        self, df: pd.DataFrame, df_htf: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:

        if df_htf is None or df_htf.empty:
            raise ValueError("HTF data is required.")

        # Compute HTF bull/bear with anti-lookahead
        df_htf = df_htf.copy()
        df_htf["htf_bull"] = (df_htf["close"] > df_htf["open"]).shift(1, fill_value=False)
        df_htf["htf_bear"] = (df_htf["close"] < df_htf["open"]).shift(1, fill_value=False)

        # Align HTF to LTF index (fast, minimal copies)
        #htf_bull = df_htf["htf_bull"].reindex(df.index, method="ffill").fillna(False).to_numpy(bool)
        #htf_bear = df_htf["htf_bear"].reindex(df.index, method="ffill").fillna(False).to_numpy(bool)

        htf_bull = (
            df_htf["htf_bull"]
            .astype("boolean")      # <-- convert BEFORE fillna
            .reindex(df.index, method="ffill")
            .fillna(False)          # <-- now safe, no warning
            .astype(bool)           # <-- convert to numpy-friendly bool
            .to_numpy()
        )

        htf_bear = (
            df_htf["htf_bear"]
            .astype("boolean")      # <-- convert BEFORE fillna
            .reindex(df.index, method="ffill")
            .fillna(False)
            .astype(bool)
            .to_numpy()
        )

        df_out = df.copy()
        df_out["htf_bull"] = htf_bull
        df_out["htf_bear"] = htf_bear

        return df_out, df_htf

    def _classify_candles_vectorized(self, df: pd.DataFrame) -> np.ndarray:
        high = df["high"].to_numpy(np.float32)
        low = df["low"].to_numpy(np.float32)

        high_prev = np.empty_like(high)
        low_prev = np.empty_like(low)
        high_prev[0] = np.nan
        low_prev[0] = np.nan
        high_prev[1:] = high[:-1]
        low_prev[1:] = low[:-1]

        candle_types = np.full(len(df), -128, dtype=np.int8)

        outside = (high > high_prev) & (low < low_prev)
        inside = (high <= high_prev) & (low >= low_prev)
        two_u = (high > high_prev) & (low >= low_prev) & ~outside
        two_d = (low < low_prev) & (high <= high_prev) & ~outside

        candle_types[inside] = 1
        candle_types[outside] = 3
        candle_types[two_u] = 2
        candle_types[two_d] = -2

        return candle_types

    def calculate_signals(
        self, df_ohlcv: pd.DataFrame, df_htf: pd.DataFrame
    ) -> pd.DataFrame:

        self._validate_input(df_ohlcv)
        df, _ = self.prepare_htf_data(df_ohlcv, df_htf)

        # Candle classification
        df["candle_type"] = self._classify_candles_vectorized(df)

        c = df["candle_type"].to_numpy()
        c_prev = np.empty_like(c)
        c_prev[0] = -128
        c_prev[1:] = c[:-1]

        rev_2d_2u = (c_prev == -2) & (c == 2)
        rev_2u_2d = (c_prev == 2) & (c == -2)

        we_buy = rev_2d_2u & df["htf_bull"].to_numpy()
        we_sell = rev_2u_2d & df["htf_bear"].to_numpy()

        df["we_buy"] = we_buy
        df["we_sell"] = we_sell

        # Keep only relevant columns (slimmer memory footprint)
        self.signals_df = df[["we_buy", "we_sell"]]

        return self.signals_df

    def get_signals(self) -> pd.DataFrame:
        if self.signals_df is None:
            raise ValueError("Run calculate_signals() first.")
        return self.signals_df
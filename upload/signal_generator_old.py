import pandas as pd
import numpy as np
import logging
from typing import Dict, Tuple
from src.indicators.wbws_trigger import WBWSTrigger

logger = logging.getLogger(__name__)

class SignalGenerator:
    """
    Optimized SignalGenerator:
    - Reuses WBWSTrigger instance
    - Vectorized raw signal creation
    - Zero behavioral changes
    """

    def __init__(self, htf_period: str):
        if not htf_period:
            raise ValueError("htf_period configuration is missing.")
        self.htf_period = htf_period
        self.trigger = WBWSTrigger(htf_period=self.htf_period)

    def generate_signals(
        self, df: pd.DataFrame, df_htf: pd.DataFrame
    ) -> Tuple[pd.Series, pd.DataFrame]:

        signals_df = self.trigger.calculate_signals(df, df_htf=df_htf)

        # Vectorized BUY/SELL signal creation
        buy_mask = signals_df["we_buy"].to_numpy()
        sell_mask = signals_df["we_sell"].to_numpy()

        raw_signals = pd.Series(
            np.where(
                buy_mask, "BUY",
                np.where(sell_mask, "SELL", None)
            ),
            index=df.index,
            dtype="object"
        )

        return raw_signals, signals_df

    def get_signal_stats(self, signals: pd.Series) -> Dict:
        if signals is None or signals.empty:
            return {"buy": 0, "sell": 0, "total": 0}

        counts = signals.value_counts()
        buy_count = int(counts.get("BUY", 0))
        sell_count = int(counts.get("SELL", 0))
        total = buy_count + sell_count

        return {
            "buy": buy_count,
            "sell": sell_count,
            "total": total,
            "buy_percentage": (buy_count / total * 100) if total > 0 else 0.0,
            "sell_percentage": (sell_count / total * 100) if total > 0 else 0.0,
        }
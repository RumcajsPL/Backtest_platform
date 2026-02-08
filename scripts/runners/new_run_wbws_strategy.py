"""
Runner for WBWS Strategy using the new Phase 1 architecture.
This file is the template/model for all future strategy runners.
"""

import pandas as pd

from src.strategies.trade_management import (
    SignalFrame,
    TradeDecision,
    TradeParameters,
    TradeDirection,
    DecisionType,
    Position,
    TradeRecord,
)

from src.strategies.core.trade_simulator import Simulator
from src.strategies.specific.wbws_strategy import WBWSStrategy   # to be refactored next


def run_wbws_strategy(
    full_df: pd.DataFrame,
    htf_df: pd.DataFrame,
    ltf_df: pd.DataFrame,
    indicators: dict,
    initial_equity: float = 100_000.0,
):
    """
    Main runner for WBWS strategy.
    This function is the canonical template for all future strategies.

    Parameters
    ----------
    full_df : pd.DataFrame
        Main timeframe OHLCV (e.g., 1-minute)
    htf_df : pd.DataFrame
        Higher timeframe OHLCV (e.g., 1-hour)
    ltf_df : pd.DataFrame
        Lower timeframe OHLCV (e.g., 1-second)
    indicators : dict[str, pd.Series]
        Precomputed indicators aligned to full_df index
    initial_equity : float
        Starting capital for the simulator

    Returns
    -------
    dict
        Simulator results: initial_equity, final_equity, trades
    """

    # 1. Instantiate strategy + simulator
    strategy = WBWSStrategy()
    simulator = Simulator(initial_equity=initial_equity)

    # 2. Iterate over bars in main timeframe
    for ts, row in full_df.iterrows():

        # --- Build SignalFrame -----------------------------------------
        sf = SignalFrame(
            timestamp=ts,
            open=row["open"],
            high=row["high"],
            low=row["low"],
            close=row["close"],
            volume=row["volume"],

            # HTF bar (if available)
            htf=htf_df.loc[ts] if ts in htf_df.index else None,

            # LTF window (example: last 10 seconds)
            ltf=(
                ltf_df.loc[ts - pd.Timedelta(seconds=10): ts]
                if len(ltf_df) > 0 else None
            ),

            # Indicators aligned to timestamp
            indicators={
                name: series.loc[ts]
                for name, series in indicators.items()
                if ts in series.index
            },

            # Strategy persistent state
            state=strategy.state,
        )

        # --- Strategy produces a TradeDecision --------------------------
        decision: TradeDecision = strategy.on_bar(sf)

        # --- Simulator processes the decision ---------------------------
        simulator.process_bar(sf, decision)

    # 3. Finalize and return results
    return simulator.finalize()
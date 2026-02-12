"""
Supertrend Filter - Migration v3.0

Filters signals based on Supertrend directional bias.
Migrated from dict-based to typed contract architecture.

Author: Migration Project
Version: 3.0.0
Date: 2025-02-11
Session: 4
"""

import pandas as pd
import pandas_ta_classic as pta
import numpy as np
import logging
from typing import Dict, Any
from time import perf_counter

from src.strategies.contracts.signal_contracts import SignalFrame
from src.strategies.contracts.filter_contracts import (
    FilterResult,
    FilterMetadata,
    FilterStatus,
    FilterProtocol
)

logger = logging.getLogger(__name__)


class SupertrendFilter:
    """
    Supertrend filter - ATR-based directional trend filter.
    
    Rejects BUY signals when Supertrend is bearish (direction == -1)
    Rejects SELL signals when Supertrend is bullish (direction == 1)
    Implements FilterProtocol for integration with FilterPipeline.
    """
    
    def __init__(self, atr_length: int = 10, factor: float = 3.0,
                 enabled: bool = True, name: str = "supertrend_filter"):
        """
        Initialize Supertrend filter.
        
        Args:
            atr_length: ATR calculation period
            factor: ATR multiplier for band calculation
            enabled: Whether filter is active
            name: Filter name for logging
        """
        self.name = name
        self.atr_length = int(atr_length)
        self.factor = float(factor)
        self.enabled = enabled
        
        if self.atr_length < 1:
            raise ValueError("ATR length must be >= 1")
        if self.factor <= 0:
            raise ValueError("Factor must be > 0")
    
    def compute_indicators(
        self,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray]
    ) -> None:
        """
        Compute Supertrend indicator using pandas_ta.
        
        Args:
            df: OHLCV DataFrame
            indicators: Dict to store pandas Series
            ind_np: Dict to store numpy arrays
        """
        st = pta.supertrend(
            high=df['high'],
            low=df['low'],
            close=df['close'],
            length=self.atr_length,
            multiplier=self.factor
        )
        
        # Column names from pandas_ta
        st_col = f"SUPERT_{self.atr_length}_{self.factor}"
        dir_col = f"SUPERTd_{self.atr_length}_{self.factor}"
        
        if st.empty or st_col not in st.columns or dir_col not in st.columns:
            logger.warning(f"Supertrend calculation failed or missing columns")
            # Create empty indicators
            indicators['supertrend_price'] = pd.Series(np.nan, index=df.index)
            indicators['supertrend_dir'] = pd.Series(np.nan, index=df.index)
            ind_np['supertrend_price'] = np.full(len(df), np.nan, dtype=np.float32)
            ind_np['supertrend_dir'] = np.full(len(df), np.nan, dtype=np.float32)
        else:
            st_price = st[st_col].astype('float32')
            st_dir = st[dir_col].astype('float32')
            
            indicators['supertrend_price'] = st_price
            indicators['supertrend_dir'] = st_dir
            ind_np['supertrend_price'] = st_price.to_numpy()
            ind_np['supertrend_dir'] = st_dir.to_numpy()
    
    def apply_filter(
        self,
        signal_frame: SignalFrame,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray],
        mode: str = "core"
    ) -> FilterResult:
        """
        Filter signals based on Supertrend direction - VECTORIZED.
        
        BUY: Supertrend must be bullish (dir == 1) AND close > supertrend_price
        SELL: Supertrend must be bearish (dir == -1) AND close < supertrend_price
        
        Args:
            signal_frame: Input signals to filter
            df: OHLCV DataFrame
            indicators: Cached indicators
            ind_np: Cached numpy indicators
            mode: Execution mode ("core" or "debug")
        
        Returns:
            FilterResult with Supertrend-filtered signals
        """
        start_time = perf_counter()
        
        # If disabled, pass all signals through
        if not self.enabled:
            metadata = FilterMetadata(
                filter_name=self.name,
                status=FilterStatus.SKIPPED,
                signals_in=signal_frame.count_by_type()["total"],
                signals_out=signal_frame.count_by_type()["total"],
                signals_rejected=0,
                reason="Filter disabled"
            )
            return FilterResult(
                passed=True,
                signal_frame=signal_frame,
                metadata=metadata
            )
        
        # Count input signals
        signals_in = signal_frame.count_by_type()["total"]
        
        # If no signals, short-circuit
        if signals_in == 0:
            execution_time = (perf_counter() - start_time) * 1000
            metadata = FilterMetadata(
                filter_name=self.name,
                status=FilterStatus.SKIPPED,
                signals_in=0,
                signals_out=0,
                reason="No input signals",
                execution_time_ms=execution_time if mode == "debug" else None
            )
            return FilterResult(
                passed=False,
                signal_frame=signal_frame,
                metadata=metadata
            )
        
        # Get Supertrend indicators
        st_price = ind_np.get('supertrend_price')
        st_dir = ind_np.get('supertrend_dir')
        
        if st_price is None or st_dir is None:
            logger.error(f"{self.name}: Supertrend indicator not found in cache")
            execution_time = (perf_counter() - start_time) * 1000
            metadata = FilterMetadata(
                filter_name=self.name,
                status=FilterStatus.ERROR,
                signals_in=signals_in,
                signals_out=0,
                reason="Supertrend indicator not computed",
                execution_time_ms=execution_time if mode == "debug" else None
            )
            return FilterResult(
                passed=False,
                signal_frame=SignalFrame(
                    signals=pd.Series(0, index=signal_frame.signals.index, dtype='int8'),
                    indicator_data=None,
                    signal_metadata={"error": "indicator_missing"}
                ),
                metadata=metadata
            )
        
        # Vectorized filtering
        signal_values = signal_frame.signals.values
        close_values = df['close'].to_numpy(np.float32)
        st_price_values = st_price.astype(np.float32)
        st_dir_values = st_dir.astype(np.float32)
        
        # Create mask: True = signal passes filter
        mask = np.ones(len(signal_values), dtype=bool)
        
        # BUY signals (code 1): Supertrend bullish AND close > supertrend_price
        buy_mask = (signal_values == 1)
        buy_condition = (st_dir_values[buy_mask] == 1) & (close_values[buy_mask] > st_price_values[buy_mask])
        mask[buy_mask] = buy_condition
        
        # SELL signals (code 2): Supertrend bearish AND close < supertrend_price
        sell_mask = (signal_values == 2)
        sell_condition = (st_dir_values[sell_mask] == -1) & (close_values[sell_mask] < st_price_values[sell_mask])
        mask[sell_mask] = sell_condition
        
        # Handle NaN values (early bars)
        has_nan = np.isnan(st_price_values) | np.isnan(st_dir_values)
        mask[has_nan] = False
        
        # Apply mask to signals
        filtered_signals = signal_values.copy()
        filtered_signals[~mask] = 0  # Zero out rejected signals
        
        # Create new SignalFrame
        filtered_frame = SignalFrame(
            signals=pd.Series(filtered_signals, index=signal_frame.signals.index, dtype='int8'),
            indicator_data=signal_frame.indicator_data if mode == "debug" else None,
            signal_metadata={
                "source": "supertrend_filter",
                "mode": mode,
                "supertrend_params": {
                    "atr_length": self.atr_length,
                    "factor": self.factor
                }
            }
        )
        
        # Count output signals
        signals_out = filtered_frame.count_by_type()["total"]
        signals_rejected = signals_in - signals_out
        
        # Determine status and reason
        if signals_out == 0:
            status = FilterStatus.REJECTED
            reason = "All signals rejected (Supertrend mismatch)"
        elif signals_rejected == 0:
            status = FilterStatus.PASSED
            reason = "All signals passed (Supertrend aligned)"
        else:
            status = FilterStatus.PASSED
            reason = f"{signals_rejected} signals rejected (wrong Supertrend direction)"
        
        # Calculate execution time
        execution_time = (perf_counter() - start_time) * 1000
        
        # Collect indicator values for debug mode
        indicator_values = None
        if mode == "debug" and signals_out > 0:
            signal_indices = np.where(filtered_signals != 0)[0]
            if len(signal_indices) > 0:
                # Count bullish vs bearish at signal locations
                bullish_count = int(np.sum(st_dir_values[signal_indices] == 1))
                bearish_count = int(np.sum(st_dir_values[signal_indices] == -1))
                indicator_values = {
                    "bullish_signals": bullish_count,
                    "bearish_signals": bearish_count,
                    "avg_distance_to_band": float(np.nanmean(
                        np.abs(close_values[signal_indices] - st_price_values[signal_indices])
                    ))
                }
        
        # Build metadata
        metadata = FilterMetadata(
            filter_name=self.name,
            status=status,
            signals_in=signals_in,
            signals_out=signals_out,
            signals_rejected=signals_rejected,
            reason=reason,
            indicator_values=indicator_values,
            execution_time_ms=execution_time if mode == "debug" else None
        )
        
        return FilterResult(
            passed=(signals_out > 0),
            signal_frame=filtered_frame,
            metadata=metadata
        )
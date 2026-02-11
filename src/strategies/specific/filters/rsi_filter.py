"""
RSI Filter - Migration v3.0

Filters signals based on RSI overbought/oversold conditions.
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


class RSIFilter:
    """
    RSI filter - rejects overbought BUY signals and oversold SELL signals.
    
    Implements FilterProtocol for integration with FilterPipeline.
    """
    
    def __init__(self, length: int = 14, overbought: float = 70.0, oversold: float = 30.0, 
                 enabled: bool = True, name: str = "rsi_filter"):
        """
        Initialize RSI filter.
        
        Args:
            length: RSI calculation period
            overbought: Upper threshold (reject BUY if RSI > this)
            oversold: Lower threshold (reject SELL if RSI < this)
            enabled: Whether filter is active
            name: Filter name for logging
        """
        self.name = name
        self.length = int(length)
        self.overbought = float(overbought)
        self.oversold = float(oversold)
        self.enabled = enabled
        
        if self.length < 2:
            raise ValueError(f"RSI length must be >= 2, got {self.length}")
    
    def compute_indicators(
        self,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray]
    ) -> None:
        """
        Compute RSI indicator using pandas_ta (Wilder's smoothing).
        
        Args:
            df: OHLCV DataFrame
            indicators: Dict to store pandas Series
            ind_np: Dict to store numpy arrays
        """
        if len(df) < self.length:
            rsi = pd.Series(50.0, index=df.index)  # Neutral fill
        else:
            rsi = pta.rsi(df['close'], length=self.length)
            rsi = rsi.astype('float32').fillna(50.0)
        
        indicators['rsi'] = rsi
        ind_np['rsi'] = rsi.to_numpy()
    
    def apply_filter(
        self,
        signal_frame: SignalFrame,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray],
        mode: str = "core"
    ) -> FilterResult:
        """
        Filter signals based on RSI levels - VECTORIZED.
        
        Args:
            signal_frame: Input signals to filter
            df: OHLCV DataFrame
            indicators: Cached indicators
            ind_np: Cached numpy indicators
            mode: Execution mode ("core" or "debug")
        
        Returns:
            FilterResult with RSI-filtered signals
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
        
        # Get RSI indicator
        rsi = ind_np.get('rsi')
        if rsi is None:
            # Indicator not computed - should not happen, but handle gracefully
            logger.error(f"{self.name}: RSI indicator not found in cache")
            execution_time = (perf_counter() - start_time) * 1000
            metadata = FilterMetadata(
                filter_name=self.name,
                status=FilterStatus.ERROR,
                signals_in=signals_in,
                signals_out=0,
                reason="RSI indicator not computed",
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
        rsi_values = rsi.astype(np.float32)
        
        # Create mask: True = signal passes filter
        mask = np.ones(len(signal_values), dtype=bool)
        
        # BUY signals (code 1): reject if RSI >= overbought
        buy_mask = (signal_values == 1)
        mask[buy_mask] = rsi_values[buy_mask] < self.overbought
        
        # SELL signals (code 2): reject if RSI <= oversold
        sell_mask = (signal_values == 2)
        mask[sell_mask] = rsi_values[sell_mask] > self.oversold
        
        # Apply mask to signals
        filtered_signals = signal_values.copy()
        filtered_signals[~mask] = 0  # Zero out rejected signals
        
        # Create new SignalFrame
        filtered_frame = SignalFrame(
            signals=pd.Series(filtered_signals, index=signal_frame.signals.index, dtype='int8'),
            indicator_data=signal_frame.indicator_data if mode == "debug" else None,
            signal_metadata={
                "source": "rsi_filter",
                "mode": mode,
                "rsi_params": {"length": self.length, "overbought": self.overbought, "oversold": self.oversold}
            }
        )
        
        # Count output signals
        signals_out = filtered_frame.count_by_type()["total"]
        signals_rejected = signals_in - signals_out
        
        # Determine status and reason
        if signals_out == 0:
            status = FilterStatus.REJECTED
            reason = "All signals rejected by RSI"
        elif signals_rejected == 0:
            status = FilterStatus.PASSED
            reason = "All signals passed RSI filter"
        else:
            status = FilterStatus.PASSED
            reason = f"{signals_rejected} signals rejected (RSI out of bounds)"
        
        # Calculate execution time
        execution_time = (perf_counter() - start_time) * 1000
        
        # Collect indicator values for debug mode
        indicator_values = None
        if mode == "debug" and signals_out > 0:
            # Sample RSI values at signal locations
            signal_indices = np.where(filtered_signals != 0)[0]
            if len(signal_indices) > 0:
                indicator_values = {
                    "rsi_mean": float(np.mean(rsi_values[signal_indices])),
                    "rsi_min": float(np.min(rsi_values[signal_indices])),
                    "rsi_max": float(np.max(rsi_values[signal_indices]))
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
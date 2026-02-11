"""
ADX Filter - Migration v3.0

Filters signals based on trend strength (direction-agnostic).
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


class ADXFilter:
    """
    ADX filter - measures trend strength (direction-agnostic).
    
    Rejects signals when ADX is below threshold (weak/choppy trend).
    Implements FilterProtocol for integration with FilterPipeline.
    """
    
    def __init__(self, adx_length: int = 14, threshold: float = 18.0,
                 enabled: bool = True, name: str = "adx_filter"):
        """
        Initialize ADX filter.
        
        Args:
            adx_length: ADX calculation period
            threshold: Minimum ADX value (reject if below)
            enabled: Whether filter is active
            name: Filter name for logging
        """
        self.name = name
        self.adx_length = int(adx_length)
        self.threshold = float(threshold)
        self.enabled = enabled
        
        if self.adx_length < 2:
            raise ValueError(f"ADX length must be >= 2, got {self.adx_length}")
    
    def compute_indicators(
        self,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray]
    ) -> None:
        """
        Compute ADX indicator using pandas_ta.
        
        Args:
            df: OHLCV DataFrame
            indicators: Dict to store pandas Series
            ind_np: Dict to store numpy arrays
        """
        if len(df) < self.adx_length:
            adx = pd.Series(0.0, index=df.index)  # No trend
        else:
            adx_df = pta.adx(high=df['high'], low=df['low'], close=df['close'], 
                            length=self.adx_length)
            if adx_df.empty:
                adx = pd.Series(0.0, index=df.index)
            else:
                adx_col = f'ADX_{self.adx_length}'
                adx = adx_df[adx_col].astype('float32').fillna(0.0)
        
        indicators['adx'] = adx
        ind_np['adx'] = adx.to_numpy()
    
    def apply_filter(
        self,
        signal_frame: SignalFrame,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray],
        mode: str = "core"
    ) -> FilterResult:
        """
        Filter signals based on ADX trend strength - VECTORIZED.
        
        Both BUY and SELL signals require ADX > threshold.
        
        Args:
            signal_frame: Input signals to filter
            df: OHLCV DataFrame
            indicators: Cached indicators
            ind_np: Cached numpy indicators
            mode: Execution mode ("core" or "debug")
        
        Returns:
            FilterResult with ADX-filtered signals
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
        
        # Get ADX indicator
        adx = ind_np.get('adx')
        if adx is None:
            logger.error(f"{self.name}: ADX indicator not found in cache")
            execution_time = (perf_counter() - start_time) * 1000
            metadata = FilterMetadata(
                filter_name=self.name,
                status=FilterStatus.ERROR,
                signals_in=signals_in,
                signals_out=0,
                reason="ADX indicator not computed",
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
        
        # Vectorized filtering: ADX > threshold (direction-agnostic)
        signal_values = signal_frame.signals.values
        adx_values = adx.astype(np.float32)
        
        # Create mask: True = signal passes filter
        # Both BUY (1) and SELL (2) signals need ADX > threshold
        mask = np.ones(len(signal_values), dtype=bool)
        has_signal = (signal_values > 0)
        mask[has_signal] = adx_values[has_signal] > self.threshold
        
        # Apply mask to signals
        filtered_signals = signal_values.copy()
        filtered_signals[~mask] = 0  # Zero out rejected signals
        
        # Create new SignalFrame
        filtered_frame = SignalFrame(
            signals=pd.Series(filtered_signals, index=signal_frame.signals.index, dtype='int8'),
            indicator_data=signal_frame.indicator_data if mode == "debug" else None,
            signal_metadata={
                "source": "adx_filter",
                "mode": mode,
                "adx_params": {"length": self.adx_length, "threshold": self.threshold}
            }
        )
        
        # Count output signals
        signals_out = filtered_frame.count_by_type()["total"]
        signals_rejected = signals_in - signals_out
        
        # Determine status and reason
        if signals_out == 0:
            status = FilterStatus.REJECTED
            reason = f"All signals rejected (ADX < {self.threshold})"
        elif signals_rejected == 0:
            status = FilterStatus.PASSED
            reason = f"All signals passed (ADX > {self.threshold})"
        else:
            status = FilterStatus.PASSED
            reason = f"{signals_rejected} signals rejected (weak trend)"
        
        # Calculate execution time
        execution_time = (perf_counter() - start_time) * 1000
        
        # Collect indicator values for debug mode
        indicator_values = None
        if mode == "debug" and signals_out > 0:
            signal_indices = np.where(filtered_signals != 0)[0]
            if len(signal_indices) > 0:
                indicator_values = {
                    "adx_mean": float(np.mean(adx_values[signal_indices])),
                    "adx_min": float(np.min(adx_values[signal_indices])),
                    "adx_max": float(np.max(adx_values[signal_indices]))
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
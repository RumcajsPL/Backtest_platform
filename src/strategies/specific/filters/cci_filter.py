"""
CCI Filter - Migration v3.0

Filters signals based on CCI overbought/oversold conditions.
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


class CCIFilter:
    """
    CCI filter - detects overbought/oversold conditions and momentum.
    
    Implements FilterProtocol for integration with FilterPipeline.
    """
    
    def __init__(self, length: int = 20, overbought: int = 100, oversold: int = -100,
                 enabled: bool = True, name: str = "cci_filter"):
        """
        Initialize CCI filter.
        
        Args:
            length: CCI calculation period
            overbought: Upper threshold (reject BUY if CCI > this)
            oversold: Lower threshold (reject SELL if CCI < this)
            enabled: Whether filter is active
            name: Filter name for logging
        """
        self.name = name
        self.length = int(length)
        self.overbought = int(overbought)
        self.oversold = int(oversold)
        self.enabled = enabled
        
        if self.length < 3:
            raise ValueError(f"CCI length must be >= 3, got {self.length}")
        if self.oversold >= self.overbought:
            raise ValueError(f"Oversold ({self.oversold}) must be < Overbought ({self.overbought})")
    
    def compute_indicators(
        self,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray]
    ) -> None:
        """
        Compute CCI indicator using pandas_ta.
        
        Args:
            df: OHLCV DataFrame
            indicators: Dict to store pandas Series
            ind_np: Dict to store numpy arrays
        """
        if len(df) < self.length:
            cci = pd.Series(0.0, index=df.index)  # Neutral fill
        else:
            cci = pta.cci(high=df['high'], low=df['low'], close=df['close'], length=self.length)
            cci = cci.astype('float32').fillna(0.0)
        
        indicators['cci'] = cci
        ind_np['cci'] = cci.to_numpy()
    
    def apply_filter(
        self,
        signal_frame: SignalFrame,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray],
        mode: str = "core"
    ) -> FilterResult:
        """
        Filter signals based on CCI levels - VECTORIZED.
        
        Args:
            signal_frame: Input signals to filter
            df: OHLCV DataFrame
            indicators: Cached indicators
            ind_np: Cached numpy indicators
            mode: Execution mode ("core" or "debug")
        
        Returns:
            FilterResult with CCI-filtered signals
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
        
        # Get CCI indicator
        cci = ind_np.get('cci')
        if cci is None:
            logger.error(f"{self.name}: CCI indicator not found in cache")
            execution_time = (perf_counter() - start_time) * 1000
            metadata = FilterMetadata(
                filter_name=self.name,
                status=FilterStatus.ERROR,
                signals_in=signals_in,
                signals_out=0,
                reason="CCI indicator not computed",
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
        cci_values = cci.astype(np.float32)
        
        # Create mask: True = signal passes filter
        mask = np.ones(len(signal_values), dtype=bool)
        
        # BUY signals (code 1): reject if CCI >= overbought
        buy_mask = (signal_values == 1)
        mask[buy_mask] = cci_values[buy_mask] < self.overbought
        
        # SELL signals (code 2): reject if CCI <= oversold
        sell_mask = (signal_values == 2)
        mask[sell_mask] = cci_values[sell_mask] > self.oversold
        
        # Apply mask to signals
        filtered_signals = signal_values.copy()
        filtered_signals[~mask] = 0  # Zero out rejected signals
        
        # Create new SignalFrame
        filtered_frame = SignalFrame(
            signals=pd.Series(filtered_signals, index=signal_frame.signals.index, dtype='int8'),
            indicator_data=signal_frame.indicator_data if mode == "debug" else None,
            signal_metadata={
                "source": "cci_filter",
                "mode": mode,
                "cci_params": {"length": self.length, "overbought": self.overbought, "oversold": self.oversold}
            }
        )
        
        # Count output signals
        signals_out = filtered_frame.count_by_type()["total"]
        signals_rejected = signals_in - signals_out
        
        # Determine status and reason
        if signals_out == 0:
            status = FilterStatus.REJECTED
            reason = "All signals rejected by CCI"
        elif signals_rejected == 0:
            status = FilterStatus.PASSED
            reason = "All signals passed CCI filter"
        else:
            status = FilterStatus.PASSED
            reason = f"{signals_rejected} signals rejected (CCI out of bounds)"
        
        # Calculate execution time
        execution_time = (perf_counter() - start_time) * 1000
        
        # Collect indicator values for debug mode
        indicator_values = None
        if mode == "debug" and signals_out > 0:
            # Sample CCI values at signal locations
            signal_indices = np.where(filtered_signals != 0)[0]
            if len(signal_indices) > 0:
                indicator_values = {
                    "cci_mean": float(np.mean(cci_values[signal_indices])),
                    "cci_min": float(np.min(cci_values[signal_indices])),
                    "cci_max": float(np.max(cci_values[signal_indices]))
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
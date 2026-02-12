"""
Bollinger Bands Filter - Migration v3.0

Filters signals based on Bollinger Bandwidth volatility regime.
Restores original logic from legacy filter.

Author: Migration Project
Version: 3.0.1
Date: 2025-02-12
Session: 5
Batch: 3
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


class BollingerFilter:
    """
    Bollinger Bands filter - volatility regime filter using Bandwidth.
    
    Rejects signals when volatility is contracting.
    Passes signals when current bandwidth > (bandwidth_ma * filter_multiplier)
    
    Restored original logic from legacy filter.
    Implements FilterProtocol for integration with FilterPipeline.
    """
    
    def __init__(self, 
                 length: int = 14, 
                 width_ma_length: int = 30,
                 filter_multiplier: float = 0.5,
                 std_dev: float = 2.0,
                 enabled: bool = True, 
                 name: str = "bollinger_filter"):
        """
        Initialize Bollinger Bands filter with original bandwidth logic.
        
        Args:
            length: Moving average period for center band
            width_ma_length: Rolling window for bandwidth moving average
            filter_multiplier: Multiplier for bandwidth threshold
            std_dev: Number of standard deviations for bands
            enabled: Whether filter is active
            name: Filter name for logging
        """
        self.name = name
        self.length = int(length)
        self.width_ma_length = int(width_ma_length)
        self.filter_multiplier = float(filter_multiplier)
        self.std_dev = float(std_dev)
        self.enabled = enabled
        
        if self.length < 2:
            raise ValueError(f"Bollinger length must be >= 2, got {self.length}")
        if self.std_dev <= 0:
            raise ValueError(f"Standard deviation must be > 0, got {self.std_dev}")
        if self.width_ma_length < 1:
            raise ValueError(f"width_ma_length must be >= 1, got {self.width_ma_length}")
    
    def compute_indicators(
        self,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray]
    ) -> None:
        """
        Compute Bollinger Bands and Bandwidth indicators.
        Restores original bandwidth calculation from legacy filter.
        
        Args:
            df: OHLCV DataFrame
            indicators: Dict to store pandas Series
            ind_np: Dict to store numpy arrays
        """
        if len(df) < self.length:
            # Insufficient data
            indicators['bb_bandwidth'] = pd.Series(0, index=df.index)
            indicators['bb_bandwidth_ma'] = pd.Series(0, index=df.index)
            ind_np['bb_bandwidth'] = np.zeros(len(df), dtype=np.float32)
            ind_np['bb_bandwidth_ma'] = np.zeros(len(df), dtype=np.float32)
            return
        
        # Calculate Bollinger Bands
        bb = pta.bbands(df['close'], length=self.length, std=self.std_dev)
        
        if bb.empty:
            logger.warning(f"Bollinger Bands calculation failed")
            indicators['bb_bandwidth'] = pd.Series(0, index=df.index)
            indicators['bb_bandwidth_ma'] = pd.Series(0, index=df.index)
            ind_np['bb_bandwidth'] = np.zeros(len(df), dtype=np.float32)
            ind_np['bb_bandwidth_ma'] = np.zeros(len(df), dtype=np.float32)
            return
        
        # Column names from pandas_ta
        lower_col = f"BBL_{self.length}_{self.std_dev}"
        middle_col = f"BBM_{self.length}_{self.std_dev}"
        upper_col = f"BBU_{self.length}_{self.std_dev}"
        
        # Extract bands
        bb_lower = bb[lower_col].astype('float32')
        bb_middle = bb[middle_col].astype('float32')
        bb_upper = bb[upper_col].astype('float32')
        
        # Calculate Bandwidth: ((upper - lower) / basis) * 100
        bandwidth = ((bb_upper - bb_lower) / bb_middle) * 100
        bandwidth = bandwidth.fillna(0).replace([np.inf, -np.inf], 0)
        
        # Calculate moving average of bandwidth
        bandwidth_ma = bandwidth.rolling(
            self.width_ma_length, 
            min_periods=self.width_ma_length
        ).mean()
        bandwidth_ma = bandwidth_ma.fillna(0)
        
        # Store results
        indicators['bb_bandwidth'] = bandwidth
        indicators['bb_bandwidth_ma'] = bandwidth_ma
        ind_np['bb_bandwidth'] = bandwidth.to_numpy(dtype=np.float32)
        ind_np['bb_bandwidth_ma'] = bandwidth_ma.to_numpy(dtype=np.float32)
        
        # Also store bands for potential debug info
        indicators['bb_lower'] = bb_lower
        indicators['bb_middle'] = bb_middle
        indicators['bb_upper'] = bb_upper
        ind_np['bb_lower'] = bb_lower.to_numpy(dtype=np.float32)
        ind_np['bb_middle'] = bb_middle.to_numpy(dtype=np.float32)
        ind_np['bb_upper'] = bb_upper.to_numpy(dtype=np.float32)
    
    def apply_filter(
        self,
        signal_frame: SignalFrame,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray],
        mode: str = "core"
    ) -> FilterResult:
        """
        Filter signals based on Bollinger Bandwidth - VECTORIZED.
        
        Original logic: signal passes if:
            bandwidth > (bandwidth_ma * filter_multiplier)
        
        Args:
            signal_frame: Input signals to filter
            df: OHLCV DataFrame
            indicators: Cached indicators
            ind_np: Cached numpy indicators
            mode: Execution mode ("core" or "debug")
        
        Returns:
            FilterResult with Bollinger-filtered signals
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
        
        # Get bandwidth indicators
        bandwidth = ind_np.get('bb_bandwidth')
        bandwidth_ma = ind_np.get('bb_bandwidth_ma')
        
        if bandwidth is None or bandwidth_ma is None:
            logger.error(f"{self.name}: Bandwidth indicators not found in cache")
            execution_time = (perf_counter() - start_time) * 1000
            metadata = FilterMetadata(
                filter_name=self.name,
                status=FilterStatus.ERROR,
                signals_in=signals_in,
                signals_out=0,
                reason="Bandwidth indicators not computed",
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
        
        # Create mask: True = signal passes filter
        # ORIGINAL LOGIC: bandwidth > (bandwidth_ma * filter_multiplier)
        threshold = bandwidth_ma * self.filter_multiplier
        mask = bandwidth > threshold
        
        # Apply mask to all signals (non-directional filter)
        filtered_signals = signal_values.copy()
        filtered_signals[~mask] = 0  # Zero out rejected signals
        
        # Create new SignalFrame
        filtered_frame = SignalFrame(
            signals=pd.Series(filtered_signals, index=signal_frame.signals.index, dtype='int8'),
            indicator_data=signal_frame.indicator_data if mode == "debug" else None,
            signal_metadata={
                "source": "bollinger_filter",
                "mode": mode,
                "bollinger_params": {
                    "length": self.length,
                    "width_ma_length": self.width_ma_length,
                    "filter_multiplier": self.filter_multiplier,
                    "std_dev": self.std_dev
                }
            }
        )
        
        # Count output signals
        signals_out = filtered_frame.count_by_type()["total"]
        signals_rejected = signals_in - signals_out
        
        # Determine status and reason
        if signals_out == 0:
            status = FilterStatus.REJECTED
            reason = "All signals in low volatility regime"
        elif signals_rejected == 0:
            status = FilterStatus.PASSED
            reason = "All signals in high volatility regime"
        else:
            status = FilterStatus.PASSED
            reason = f"{signals_rejected} signals in low volatility regime"
        
        # Calculate execution time
        execution_time = (perf_counter() - start_time) * 1000
        
        # Collect indicator values for debug mode
        indicator_values = None
        if mode == "debug" and signals_out > 0:
            signal_indices = np.where(filtered_signals != 0)[0]
            if len(signal_indices) > 0:
                indicator_values = {
                    "avg_bandwidth": float(np.nanmean(bandwidth[signal_indices])),
                    "avg_bandwidth_ma": float(np.nanmean(bandwidth_ma[signal_indices])),
                    "avg_threshold": float(np.nanmean(threshold[signal_indices])),
                    "filter_multiplier": self.filter_multiplier,
                    "width_ma_length": self.width_ma_length
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
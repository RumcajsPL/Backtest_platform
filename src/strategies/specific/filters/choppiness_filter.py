"""
Choppiness Index Filter - Migration v3.0

Filters signals based on market choppiness/trendiness.
Migrated from dict-based to typed contract architecture.

Author: Migration Project
Version: 3.0.0
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


class ChoppinessFilter:
    """
    Choppiness Index filter - market trendiness/choppiness filter.
    
    Choppiness Index ranges from 0-100:
    - High values (>61.8): choppy/sideways market (trend-following unreliable)
    - Low values (<38.2): strong trend (good for trend-following)
    
    This filter rejects ALL signals when choppiness is above threshold
    (market is too choppy for reliable signals).
    
    Implements FilterProtocol for integration with FilterPipeline.
    """
    
    def __init__(self, length: int = 14, threshold: float = 61.8,
                 enabled: bool = True, name: str = "choppiness_filter"):
        """
        Initialize Choppiness Index filter.
        
        Args:
            length: Lookback period for choppiness calculation
            threshold: Maximum acceptable choppiness (reject if CI > threshold)
            enabled: Whether filter is active
            name: Filter name for logging
        """
        self.name = name
        self.length = int(length)
        self.threshold = float(threshold)
        self.enabled = enabled
        
        if self.length < 2:
            raise ValueError(f"Choppiness length must be >= 2, got {self.length}")
        if not (0 <= self.threshold <= 100):
            raise ValueError(f"Threshold must be 0-100, got {self.threshold}")
    
    def compute_indicators(
        self,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray]
    ) -> None:
        """
        Compute Choppiness Index using pandas_ta.
        
        Args:
            df: OHLCV DataFrame
            indicators: Dict to store pandas Series
            ind_np: Dict to store numpy arrays
        """
        if len(df) < self.length:
            # Insufficient data - fill with neutral value (50)
            chop = pd.Series(50.0, index=df.index)
        else:
            chop = pta.chop(
                high=df['high'],
                low=df['low'],
                close=df['close'],
                length=self.length
            )
            
            if chop is None or chop.empty:
                logger.warning(f"Choppiness Index calculation failed")
                chop = pd.Series(50.0, index=df.index)
            else:
                chop = chop.astype('float32').fillna(50.0)
        
        indicators['choppiness'] = chop
        ind_np['choppiness'] = chop.to_numpy()
    
    def apply_filter(
        self,
        signal_frame: SignalFrame,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray],
        mode: str = "core"
    ) -> FilterResult:
        """
        Filter signals based on Choppiness Index - VECTORIZED.
        
        Rejects ALL signals (BUY and SELL) when choppiness exceeds threshold.
        
        Args:
            signal_frame: Input signals to filter
            df: OHLCV DataFrame
            indicators: Cached indicators
            ind_np: Cached numpy indicators
            mode: Execution mode ("core" or "debug")
        
        Returns:
            FilterResult with choppiness-filtered signals
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
        
        # Get Choppiness indicator
        choppiness = ind_np.get('choppiness')
        
        if choppiness is None:
            logger.error(f"{self.name}: Choppiness Index not found in cache")
            execution_time = (perf_counter() - start_time) * 1000
            metadata = FilterMetadata(
                filter_name=self.name,
                status=FilterStatus.ERROR,
                signals_in=signals_in,
                signals_out=0,
                reason="Choppiness Index not computed",
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
        chop_values = choppiness.astype(np.float32)
        
        # Create mask: True = signal passes filter (choppiness <= threshold)
        # This applies to ALL signals regardless of type
        mask = chop_values <= self.threshold
        
        # Apply mask to signals
        filtered_signals = signal_values.copy()
        filtered_signals[~mask] = 0  # Zero out signals where market is too choppy
        
        # Create new SignalFrame
        filtered_frame = SignalFrame(
            signals=pd.Series(filtered_signals, index=signal_frame.signals.index, dtype='int8'),
            indicator_data=signal_frame.indicator_data if mode == "debug" else None,
            signal_metadata={
                "source": "choppiness_filter",
                "mode": mode,
                "choppiness_params": {
                    "length": self.length,
                    "threshold": self.threshold
                }
            }
        )
        
        # Count output signals
        signals_out = filtered_frame.count_by_type()["total"]
        signals_rejected = signals_in - signals_out
        
        # Determine status and reason
        if signals_out == 0:
            status = FilterStatus.REJECTED
            reason = f"All signals rejected (market too choppy, CI > {self.threshold})"
        elif signals_rejected == 0:
            status = FilterStatus.PASSED
            reason = f"All signals passed (trending market, CI <= {self.threshold})"
        else:
            status = FilterStatus.PASSED
            reason = f"{signals_rejected} signals rejected (choppy periods)"
        
        # Calculate execution time
        execution_time = (perf_counter() - start_time) * 1000
        
        # Collect indicator values for debug mode
        indicator_values = None
        if mode == "debug" and signals_out > 0:
            signal_indices = np.where(filtered_signals != 0)[0]
            if len(signal_indices) > 0:
                chop_at_signals = chop_values[signal_indices]
                indicator_values = {
                    "avg_choppiness": float(np.nanmean(chop_at_signals)),
                    "min_choppiness": float(np.nanmin(chop_at_signals)),
                    "max_choppiness": float(np.nanmax(chop_at_signals)),
                    "trending_signals": int(np.sum(chop_at_signals < 38.2)),  # Strong trend
                    "choppy_signals": int(np.sum(chop_at_signals > 61.8))  # Choppy (shouldn't happen)
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
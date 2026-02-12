"""
DPO Filter - Migration v3.0 - CORRECTED
Filters signals based on Detrended Price Oscillator with smoothing and threshold.
EXACT legacy computation logic restored with new architecture.

Author: Migration Project
Version: 3.0.1
Date: 2025-02-12
Session: 5 - Final
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


class DPOFilter:
    """
    Detrended Price Oscillator (DPO) filter - cycle-based directional filter.
    
    EXACT legacy logic restored:
    - DPO normalized as percentage: (DPO / close) * 100
    - Optional smoothing of raw DPO
    - BUY: -threshold < DPO_norm < 0 (negative, near zero)
    - SELL: 0 < DPO_norm < threshold (positive, near zero)
    - STRICT inequalities (< and >, not <= or >=)
    - NaN handling: fillna(False) on conditions
    """
    
    def __init__(self, 
                 length: int = 20, 
                 smooth: int = 3,
                 threshold: float = 0.2,
                 centered: bool = False,
                 enabled: bool = True, 
                 name: str = "dpo_filter"):
        """
        Initialize DPO filter with EXACT legacy parameters.
        
        Args:
            length: Lookback period for DPO calculation
            smooth: Rolling window for smoothing (1 = no smoothing)
            threshold: Percentage threshold for DPO normalization
            centered: Whether to center the oscillator (False for shifted SMA)
            enabled: Whether filter is active
            name: Filter name for logging
        """
        self.name = name
        self.length = int(length)
        self.smooth = int(smooth) if smooth > 0 else 1
        self.threshold = float(threshold)
        self.centered = centered  # False to match legacy shifted SMA
        self.enabled = enabled
        
        if self.length < 3:
            raise ValueError(f"DPO length must be >= 3, got {self.length}")
    
    def _calculate_dpo(self, series: pd.Series) -> pd.Series:
        """
        Calculate DPO with smoothing and normalize as percentage.
        EXACT legacy computation logic.
        """
        if len(series) < self.length:
            return pd.Series(np.nan, index=series.index)
        
        # Calculate raw DPO
        dpo_raw = pta.dpo(series, length=self.length, centered=self.centered)
        
        if dpo_raw is None or dpo_raw.empty:
            return pd.Series(np.nan, index=series.index)
        
        # Optional smoothing (legacy: smooth > 1)
        if self.smooth > 1:
            dpo_smoothed = dpo_raw.rolling(
                window=self.smooth,
                min_periods=self.smooth
            ).mean()
        else:
            dpo_smoothed = dpo_raw
        
        # Normalize as percentage: (DPO / close) * 100
        dpo_norm = (dpo_smoothed / series) * 100
        dpo_norm = dpo_norm.astype('float32')
        
        return dpo_norm
    
    def compute_indicators(
        self,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray]
    ) -> None:
        """
        Compute DPO normalized values.
        EXACT legacy computation.
        """
        dpo_norm = self._calculate_dpo(df['close'])
        
        # Store with NaN intact (handled in apply_filter)
        indicators['dpo_norm'] = dpo_norm
        ind_np['dpo_norm'] = dpo_norm.to_numpy(dtype=np.float32)
    
    def apply_filter(
        self,
        signal_frame: SignalFrame,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray],
        mode: str = "core"
    ) -> FilterResult:
        """
        Filter signals based on normalized DPO with threshold.
        
        EXACT legacy logic:
        - BUY:  -threshold < DPO_norm < 0  (STRICT inequalities)
        - SELL: 0 < DPO_norm < threshold    (STRICT inequalities)
        - NaN:  fillna(False) on conditions
        """
        start_time = perf_counter()
        
        if not self.enabled:
            metadata = FilterMetadata(
                filter_name=self.name,
                status=FilterStatus.SKIPPED,
                signals_in=signal_frame.count_by_type()["total"],
                signals_out=signal_frame.count_by_type()["total"],
                signals_rejected=0,
                reason="Filter disabled"
            )
            return FilterResult(passed=True, signal_frame=signal_frame, metadata=metadata)
        
        signals_in = signal_frame.count_by_type()["total"]
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
            return FilterResult(passed=False, signal_frame=signal_frame, metadata=metadata)
        
        # Get DPO indicator
        dpo_norm = ind_np.get('dpo_norm')
        
        if dpo_norm is None:
            logger.error(f"{self.name}: DPO indicator not found in cache")
            execution_time = (perf_counter() - start_time) * 1000
            metadata = FilterMetadata(
                filter_name=self.name,
                status=FilterStatus.ERROR,
                signals_in=signals_in,
                signals_out=0,
                reason="DPO indicator not computed",
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
        dpo_values = dpo_norm.astype(np.float32)
        
        # Initialize mask - all False by default (matches legacy fillna(False))
        mask = np.zeros(len(signal_values), dtype=bool)
        
        # BUY signals (code 1): -threshold < DPO < 0 (STRICT)
        buy_mask = (signal_values == 1)
        buy_condition = (dpo_values[buy_mask] < 0) & (dpo_values[buy_mask] > -self.threshold)
        # Handle NaN: conditions with NaN become False automatically
        mask[buy_mask] = buy_condition
        
        # SELL signals (code 2): 0 < DPO < threshold (STRICT)
        sell_mask = (signal_values == 2)
        sell_condition = (dpo_values[sell_mask] > 0) & (dpo_values[sell_mask] < self.threshold)
        mask[sell_mask] = sell_condition
        
        # Apply mask to signals
        filtered_signals = signal_values.copy()
        filtered_signals[~mask] = 0
        
        # Create new SignalFrame
        filtered_frame = SignalFrame(
            signals=pd.Series(filtered_signals, index=signal_frame.signals.index, dtype='int8'),
            indicator_data=signal_frame.indicator_data if mode == "debug" else None,
            signal_metadata={
                "source": "dpo_filter",
                "mode": mode,
                "dpo_params": {
                    "length": self.length,
                    "smooth": self.smooth,
                    "threshold": self.threshold,
                    "centered": self.centered
                }
            }
        )
        
        signals_out = filtered_frame.count_by_type()["total"]
        signals_rejected = signals_in - signals_out
        execution_time = (perf_counter() - start_time) * 1000
        
        metadata = FilterMetadata(
            filter_name=self.name,
            status=FilterStatus.PASSED if signals_out > 0 else FilterStatus.REJECTED,
            signals_in=signals_in,
            signals_out=signals_out,
            signals_rejected=signals_rejected,
            reason=f"{signals_rejected} signals rejected" if signals_rejected > 0 else "All signals passed",
            execution_time_ms=execution_time if mode == "debug" else None
        )
        
        return FilterResult(
            passed=(signals_out > 0),
            signal_frame=filtered_frame,
            metadata=metadata
        )
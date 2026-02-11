"""
Time Filter - Migration v3.0

Filters signals based on configured session hours.
Migrated from dict-based to typed contract architecture.

Author: Migration Project
Version: 3.0.0
Date: 2025-02-11
Session: 4
"""

import pandas as pd
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


class TimeFilter:
    """
    Time-based signal filter using pre-converted timezone timestamps.
    
    Filters signals to only allow trades during configured session hours.
    Implements FilterProtocol for integration with FilterPipeline.
    """
    
    def __init__(self, config: Dict[str, Any], name: str = "time_filter"):
        """
        Initialize TimeFilter with session configuration.
        
        Args:
            config: Trade management config (contains time_filter section)
            name: Filter name for logging
        """
        self.name = name
        self.config = config
        self.time_filter_config = config.get('time_filter', {})
        
        self.enabled = self.time_filter_config.get('enabled', True)
        
        # Parse session times
        self.session_start_hour = self.time_filter_config.get('session_start', {}).get('hour', 8)
        self.session_start_minute = self.time_filter_config.get('session_start', {}).get('minute', 30)
        self.session_end_hour = self.time_filter_config.get('session_end', {}).get('hour', 20)
        self.session_end_minute = self.time_filter_config.get('session_end', {}).get('minute', 30)
        
        # Convert to minutes for fast comparison
        self.session_start_minutes = self.session_start_hour * 60 + self.session_start_minute
        self.session_end_minutes = self.session_end_hour * 60 + self.session_end_minute
        
        if self.enabled:
            # Validate session times
            if self.session_start_minutes >= self.session_end_minutes:
                error_msg = (
                    f"Invalid session config: Start ({self.session_start_hour:02d}:{self.session_start_minute:02d}) "
                    f"must be before End ({self.session_end_hour:02d}:{self.session_end_minute:02d})"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            logger.info(
                f"TimeFilter: session {self.session_start_hour:02d}:{self.session_start_minute:02d}-"
                f"{self.session_end_hour:02d}:{self.session_end_minute:02d}"
            )
        else:
            logger.info("TimeFilter: DISABLED")
    
    def compute_indicators(
        self,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray]
    ) -> None:
        """
        Time filter doesn't use indicators.
        
        Implements FilterProtocol interface but does nothing.
        """
        pass
    
    def apply_filter(
        self,
        signal_frame: SignalFrame,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray],
        mode: str = "core"
    ) -> FilterResult:
        """
        Filter signals based on trading hours - VECTORIZED.
        
        Args:
            signal_frame: Input signals to filter
            df: OHLCV DataFrame (not used, but required by protocol)
            indicators: Cached indicators (not used)
            ind_np: Cached numpy indicators (not used)
            mode: Execution mode ("core" or "debug")
        
        Returns:
            FilterResult with time-filtered signals
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
                reason="Filter disabled",
                execution_time_ms=None
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
                signals_rejected=0,
                reason="No input signals",
                execution_time_ms=execution_time if mode == "debug" else None
            )
            return FilterResult(
                passed=False,
                signal_frame=signal_frame,
                metadata=metadata
            )
        
        # Vectorized time filtering
        timestamps = signal_frame.signals.index
        hours = timestamps.hour.values
        minutes = timestamps.minute.values
        minutes_col = hours * 60 + minutes
        
        # Boolean mask for signals within trading hours
        trading_hours_mask = (
            (minutes_col >= self.session_start_minutes) &
            (minutes_col < self.session_end_minutes)
        )
        
        # Apply mask to signals
        filtered_signals = signal_frame.signals.copy()
        filtered_signals[~trading_hours_mask] = 0  # Zero out rejected signals
        
        # Create new SignalFrame with filtered signals
        filtered_frame = SignalFrame(
            signals=filtered_signals,
            indicator_data=signal_frame.indicator_data if mode == "debug" else None,
            signal_metadata={
                "source": "time_filter",
                "mode": mode,
                "session_hours": f"{self.session_start_hour:02d}:{self.session_start_minute:02d}-{self.session_end_hour:02d}:{self.session_end_minute:02d}"
            }
        )
        
        # Count output signals
        signals_out = filtered_frame.count_by_type()["total"]
        signals_rejected = signals_in - signals_out
        
        # Determine status
        if signals_out == 0:
            status = FilterStatus.REJECTED
            reason = "All signals outside trading hours"
        elif signals_rejected == 0:
            status = FilterStatus.PASSED
            reason = "All signals within trading hours"
        else:
            status = FilterStatus.PASSED
            reason = f"{signals_rejected} signals outside trading hours"
        
        # Calculate execution time
        execution_time = (perf_counter() - start_time) * 1000
        
        # Build metadata
        metadata = FilterMetadata(
            filter_name=self.name,
            status=status,
            signals_in=signals_in,
            signals_out=signals_out,
            signals_rejected=signals_rejected,
            reason=reason,
            indicator_values=None,  # Time filter doesn't use indicators
            execution_time_ms=execution_time if mode == "debug" else None
        )
        
        # Log significant removals
        if signals_rejected > 0:
            removal_rate = signals_rejected / signals_in
            if removal_rate > 0.1 or logger.isEnabledFor(logging.DEBUG):
                logger.info(
                    f"Time filtering: {signals_rejected}/{signals_in} removed ({removal_rate*100:.1f}%)"
                )
        
        return FilterResult(
            passed=(signals_out > 0),
            signal_frame=filtered_frame,
            metadata=metadata
        )
    
    def is_in_trading_hours(self, timestamp: pd.Timestamp) -> bool:
        """
        Check if single timestamp is within trading hours.
        
        Legacy method for backward compatibility.
        
        Args:
            timestamp: Timestamp to check
            
        Returns:
            True if within trading hours
        """
        if not self.enabled:
            return True
        
        current_minutes = timestamp.hour * 60 + timestamp.minute
        return self.session_start_minutes <= current_minutes < self.session_end_minutes
    
    def get_session_info(self, timestamp: pd.Timestamp) -> Dict[str, Any]:
        """
        Get session information for a timestamp.
        
        Legacy method for backward compatibility.
        
        Args:
            timestamp: Timestamp to query
            
        Returns:
            Dict with session information
        """
        return {
            'is_in_trading_hours': self.is_in_trading_hours(timestamp),
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'hour': timestamp.hour,
            'minute': timestamp.minute,
            'session_start': f"{self.session_start_hour:02d}:{self.session_start_minute:02d}",
            'session_end': f"{self.session_end_hour:02d}:{self.session_end_minute:02d}",
            'session_type': 'intraday'
        }
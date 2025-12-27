"""
Time Management Module for Backtesting Platform.
Filters trading signals based on configured session hours.
Assumes data timestamps are already converted to the desired timezone.
"""

import pandas as pd
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class TimeManager:
    """
    Manages time-based trading restrictions.
    Uses hour/minute directly from timestamps (assumes pre-converted timezone).
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize TimeManager with configuration.
        
        Args:
            config: Dictionary containing time management configuration
        
        Raises:
            ValueError: If session configuration is invalid (e.g. start >= end)
        """
        self.config = config
        self.time_filter_config = config.get('time_filter', {})
        
        self.enabled = self.time_filter_config.get('enabled', True)
        
        # Initialize session times
        self.session_start_hour = self.time_filter_config.get('session_start', {}).get('hour', 8)
        self.session_start_minute = self.time_filter_config.get('session_start', {}).get('minute', 30)
        self.session_end_hour = self.time_filter_config.get('session_end', {}).get('hour', 20)
        self.session_end_minute = self.time_filter_config.get('session_end', {}).get('minute', 30)
        
        # Calculate in minutes for comparison
        self.session_start_minutes = self.session_start_hour * 60 + self.session_start_minute
        self.session_end_minutes = self.session_end_hour * 60 + self.session_end_minute
        
        if self.enabled:
            # Strict validation: Start must be strictly before End
            if self.session_start_minutes >= self.session_end_minutes:
                error_msg = (f"Invalid Session Configuration: Start ({self.session_start_hour:02d}:{self.session_start_minute:02d}) "
                             f"must be strictly before End ({self.session_end_hour:02d}:{self.session_end_minute:02d}). "
                             "Overnight sessions are not supported in this mode.")
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            logger.info(f"TimeManager initialized: enabled={self.enabled}, "
                        f"session={self.session_start_hour:02d}:{self.session_start_minute:02d}-"
                        f"{self.session_end_hour:02d}:{self.session_end_minute:02d}")
        else:
            logger.info("TimeManager initialized: DISABLED")
    
    def is_in_trading_hours(self, timestamp: pd.Timestamp) -> bool:
        """
        Check if a single timestamp is within trading hours.
        Useful for individual checks, but use filter_signals_by_time for DataFrames.
        """
        if not self.enabled:
            return True
        
        current_minutes = timestamp.hour * 60 + timestamp.minute
        return self.session_start_minutes <= current_minutes < self.session_end_minutes
    
    def filter_signals_by_time(self, 
                              signals_df: pd.DataFrame,
                              timestamp_col: str = 'timestamp') -> pd.DataFrame:
        """
        Filter signals DataFrame based on trading hours using fast vectorization.
        """
        if signals_df.empty or not self.enabled:
            return signals_df
        
        # Make a copy to avoid SettingWithCopy warnings on the original DF
        df = signals_df.copy()
        
        # Ensure timestamp column is datetime
        if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
            df[timestamp_col] = pd.to_datetime(df[timestamp_col])
        
        # VECTORIZED FILTERING (High Performance)
        # Calculate minutes for the entire column at once
        # .dt accessor is much faster than .apply()
        minutes_col = df[timestamp_col].dt.hour * 60 + df[timestamp_col].dt.minute
        
        # Create boolean mask
        trading_hours_mask = (minutes_col >= self.session_start_minutes) & \
                             (minutes_col < self.session_end_minutes)
        
        # Logging statistics
        total_signals = len(df)
        filtered_signals = trading_hours_mask.sum()
        removed_signals = total_signals - filtered_signals
        
        if removed_signals > 0:
            logger.info(f"Time filtering: {removed_signals}/{total_signals} "
                      f"signals removed ({(removed_signals/total_signals*100):.1f}%)")
        
        # Return filtered dataframe
        return df[trading_hours_mask].copy()
    
    def get_session_info(self, timestamp: pd.Timestamp) -> Dict[str, Any]:
        """
        Get session information for a timestamp.
        """
        is_in_session = self.is_in_trading_hours(timestamp)
        
        return {
            'is_in_trading_hours': is_in_session,
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'hour': timestamp.hour,
            'minute': timestamp.minute,
            'session_start': f"{self.session_start_hour:02d}:{self.session_start_minute:02d}",
            'session_end': f"{self.session_end_hour:02d}:{self.session_end_minute:02d}",
            'session_type': 'intraday'
        }
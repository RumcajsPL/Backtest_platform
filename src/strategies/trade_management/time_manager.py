"""
Time Management Module for Backtesting Platform.
Filters trading signals based on configured session hours.
Works directly with timestamp hour/minute (assumes correct timezone in data).
"""

import pandas as pd
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class TimeManager:
    """
    Manages time-based trading restrictions.
    Uses hour/minute directly from timestamps without timezone conversion.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize TimeManager with configuration.
        
        Args:
            config: Dictionary containing time management configuration
        """
        self.config = config
        self.time_filter_config = config.get('time_filter', {})
        
        # Initialize session times
        self.enabled = self.time_filter_config.get('enabled', True)
        self.session_start_hour = self.time_filter_config.get('session_start', {}).get('hour', 8)
        self.session_start_minute = self.time_filter_config.get('session_start', {}).get('minute', 30)
        self.session_end_hour = self.time_filter_config.get('session_end', {}).get('hour', 20)
        self.session_end_minute = self.time_filter_config.get('session_end', {}).get('minute', 30)
        
        # Calculate in minutes for easier comparison
        self.session_start_minutes = self.session_start_hour * 60 + self.session_start_minute
        self.session_end_minutes = self.session_end_hour * 60 + self.session_end_minute
        
        # Validate session times (start must be before end for normal session)
        if self.session_start_minutes >= self.session_end_minutes:
            logger.warning(f"Session start {self.session_start_hour:02d}:{self.session_start_minute:02d} "
                          f"is not before end {self.session_end_hour:02d}:{self.session_end_minute:02d}")
        
        logger.info(f"TimeManager initialized: enabled={self.enabled}, "
                   f"session={self.session_start_hour:02d}:{self.session_start_minute:02d}-"
                   f"{self.session_end_hour:02d}:{self.session_end_minute:02d}")
    
    def is_in_trading_hours(self, timestamp: pd.Timestamp) -> bool:
        """
        Check if a given timestamp is within trading hours.
        
        Args:
            timestamp: Pandas Timestamp (naive, in desired timezone)
            
        Returns:
            bool: True if within trading hours
        """
        if not self.enabled:
            return True
        
        try:
            # Extract hour/minute directly
            current_hour = timestamp.hour
            current_minute = timestamp.minute
            current_minutes_total = current_hour * 60 + current_minute
            
            # Check if within session (start inclusive, end exclusive)
            return (self.session_start_minutes <= current_minutes_total < 
                    self.session_end_minutes)
                    
        except Exception as e:
            logger.error(f"Error checking trading hours: {e}")
            return True  # Default to allowing trade if error occurs
    
    def filter_signals_by_time(self, 
                              signals_df: pd.DataFrame,
                              timestamp_col: str = 'timestamp') -> pd.DataFrame:
        """
        Filter signals DataFrame based on trading hours.
        
        Args:
            signals_df: DataFrame containing signals with timestamps
            timestamp_col: Name of timestamp column
            
        Returns:
            DataFrame with time-filtered signals
        """
        if signals_df.empty:
            return signals_df
        
        # Make a copy to avoid warnings
        df = signals_df.copy()
        
        # Ensure timestamp column is datetime
        if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
            df[timestamp_col] = pd.to_datetime(df[timestamp_col])
        
        # Apply time filter
        if self.enabled:
            # Create mask for trading hours
            trading_hours_mask = df[timestamp_col].apply(self.is_in_trading_hours)
            
            # Log filtering results
            total_signals = len(df)
            filtered_signals = trading_hours_mask.sum()
            removed_signals = total_signals - filtered_signals
            
            if removed_signals > 0:
                logger.info(f"Time filtering: {removed_signals}/{total_signals} "
                          f"signals removed ({(removed_signals/total_signals*100):.1f}%)")
            
            # Filter the DataFrame
            df = df[trading_hours_mask].copy()
        
        return df
    
    def get_session_info(self, timestamp: pd.Timestamp) -> Dict[str, Any]:
        """
        Get session information for a timestamp.
        
        Args:
            timestamp: Pandas Timestamp
            
        Returns:
            Dictionary with session information
        """
        is_in_session = self.is_in_trading_hours(timestamp)
        
        session_info = {
            'is_in_trading_hours': is_in_session,
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'hour': timestamp.hour,
            'minute': timestamp.minute,
            'session_start': f"{self.session_start_hour:02d}:{self.session_start_minute:02d}",
            'session_end': f"{self.session_end_hour:02d}:{self.session_end_minute:02d}",
            'session_type': 'normal'  # Always normal session now
        }
        
        return session_info
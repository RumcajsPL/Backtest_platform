"""Time management: filters signals based on configured session hours"""
import pandas as pd
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class TimeManager:
    """Manages time-based trading restrictions using pre-converted timezone timestamps"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize TimeManager with session configuration.        
        """
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
                error_msg = (f"Invalid session config: Start ({self.session_start_hour:02d}:{self.session_start_minute:02d}) "
                             f"must be before End ({self.session_end_hour:02d}:{self.session_end_minute:02d})")
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            logger.info(f"TimeManager: session {self.session_start_hour:02d}:{self.session_start_minute:02d}-"
                       f"{self.session_end_hour:02d}:{self.session_end_minute:02d}")
        else:
            logger.info("TimeManager: DISABLED")
    
    def is_in_trading_hours(self, timestamp: pd.Timestamp) -> bool:
        """Check if single timestamp is within trading hours"""
        if not self.enabled:
            return True
        
        current_minutes = timestamp.hour * 60 + timestamp.minute
        return self.session_start_minutes <= current_minutes < self.session_end_minutes
    
    def filter_signals_by_time(self, 
                              signals_df: pd.DataFrame,
                              timestamp_col: str = 'timestamp') -> pd.DataFrame:
        """
        Filter signals DataFrame based on trading hours (vectorized for performance).
        """
        if signals_df.empty or not self.enabled:
            return signals_df
        
        df = signals_df.copy()
        
        # Ensure timestamp column is datetime
        if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
            df[timestamp_col] = pd.to_datetime(df[timestamp_col])
        
        # Vectorized filtering: calculate minutes for entire column
        # minutes_col = df[timestamp_col].dt.hour * 60 + df[timestamp_col].dt.minute
        hours = df[timestamp_col].dt.hour.values
        minutes = df[timestamp_col].dt.minute.values
        minutes_col = hours * 60 + minutes
        trading_hours_mask = (minutes_col >= self.session_start_minutes) & \
                             (minutes_col < self.session_end_minutes)
        
        # Log only if significant removal (>10%) or debug mode
        total_signals = len(df)
        filtered_signals = trading_hours_mask.sum()
        removed_signals = total_signals - filtered_signals
        
        if removed_signals > 0:
            removal_rate = removed_signals / total_signals
            if removal_rate > 0.1 or logger.isEnabledFor(logging.DEBUG):
                logger.info(f"Time filtering: {removed_signals}/{total_signals} removed ({removal_rate*100:.1f}%)")
        
        return df[trading_hours_mask].copy()
    
    def get_session_info(self, timestamp: pd.Timestamp) -> Dict[str, Any]:
        """Get session information for a timestamp"""
        return {
            'is_in_trading_hours': self.is_in_trading_hours(timestamp),
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'hour': timestamp.hour,
            'minute': timestamp.minute,
            'session_start': f"{self.session_start_hour:02d}:{self.session_start_minute:02d}",
            'session_end': f"{self.session_end_hour:02d}:{self.session_end_minute:02d}",
            'session_type': 'intraday'
        }
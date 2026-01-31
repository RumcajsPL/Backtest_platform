"""Filter pipeline with extensible architecture for multiple filter types"""
import pandas as pd
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class FilterPipeline:
    """Orchestrates all trading filters with extensible architecture"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.filters = {}
        self.progressive_tracker = None
        self.initialize_filters()
        
    def set_progressive_tracker(self, tracker):
        """Set the progressive tracker for detailed signal tracking"""
        self.progressive_tracker = tracker
        
    def initialize_filters(self):
        """Initialize all filter components - extensible for new filters"""
        # Time Manager
        from src.strategies.trade_management.time_manager import TimeManager
        time_cfg = self.config.get('trade_management', {})
        self.filters['time'] = TimeManager(time_cfg)
        
        # RSI Filter
        from src.strategies.filters.rsi_filter import RSIFilter
        rsi_cfg = self.config['filters']['rsi_filter']
        self.filters['rsi'] = RSIFilter(
            enabled=True,
            length=rsi_cfg['length'],
            overbought=rsi_cfg['overbought'],
            oversold=rsi_cfg['oversold']
        )
        
        # Future filters can be added here following same pattern:
        # if self.config.get('filters', {}).get('bollinger_enabled', False):
        #     from src.strategies.filters.bollinger_filter import BollingerFilter
        #     bb_cfg = self.config['filters']['bollinger']
        #     self.filters['bollinger'] = BollingerFilter(**bb_cfg)
        
        # Risk Manager initialized separately (needs full data)
        self.filters['risk'] = None
        
    def initialize_risk_manager(self, df_full: pd.DataFrame):
        """Initialize risk manager with full data"""
        from src.strategies.trade_management.risk_manager import RiskManager
        self.filters['risk'] = RiskManager(self.config, df_full)
        
    def _ensure_risk_manager(self):
        """Validate risk manager is initialized before use"""
        if self.filters['risk'] is None:
            raise RuntimeError("RiskManager not initialized. Call initialize_risk_manager() first.")
    
    def apply_time_filter(self, raw_signals: pd.Series, signal_id_map: Dict = None) -> pd.Series:
        """Apply time filter to signals"""
        if not self.filters['time'].enabled:
            return raw_signals.copy()
        
        raw_signals_df = pd.DataFrame({
            'timestamp': raw_signals.index,
            'signal': raw_signals.values
        }).dropna(subset=['signal'])
        
        time_filtered_df = self.filters['time'].filter_signals_by_time(
            raw_signals_df, 
            timestamp_col='timestamp'
        )
        
        time_filtered_signals = pd.Series(index=raw_signals.index, dtype=object)
        if not time_filtered_df.empty:
            time_filtered_signals.loc[time_filtered_df['timestamp'].values] = time_filtered_df['signal'].values
        
        # Update tracker if enabled
        if self.progressive_tracker and signal_id_map:
            self._update_time_filter_tracking(raw_signals, time_filtered_signals, signal_id_map)
        
        return time_filtered_signals
    
    def apply_rsi_filter(self, df: pd.DataFrame, time_filtered_signals: pd.Series, 
                     signal_id_map: Dict = None) -> pd.Series:
        rsi = self.filters['rsi']._calculate_rsi_wilder(df['close'])
        
        not_overbought = rsi < self.filters['rsi'].overbought
        not_oversold = rsi > self.filters['rsi'].oversold
            
        final_signals = time_filtered_signals.copy()
        final_signals.loc[(time_filtered_signals == 'BUY') & ~not_overbought] = None
        final_signals.loc[(time_filtered_signals == 'SELL') & ~not_oversold] = None
        
        # Update tracker if enabled
        if self.progressive_tracker and signal_id_map:
            self._update_rsi_filter_tracking(time_filtered_signals, final_signals, rsi, signal_id_map)
        
        return final_signals
    
    def _update_time_filter_tracking(self, raw_signals: pd.Series, time_filtered_signals: pd.Series, 
                                     signal_id_map: Dict):
        """Update progressive tracker with time filter details"""
        time_cfg = self.config.get('trade_management', {}).get('time_filter', {})
        session_start = f"{time_cfg.get('session_start', {}).get('hour', 0):02d}:{time_cfg.get('session_start', {}).get('minute', 0):02d}"
        session_end = f"{time_cfg.get('session_end', {}).get('hour', 23):02d}:{time_cfg.get('session_end', {}).get('minute', 59):02d}"
        
        for timestamp in raw_signals.dropna().index:
            if timestamp in signal_id_map:
                signal_id = signal_id_map[timestamp]
                passed = pd.notna(time_filtered_signals.loc[timestamp])
                reason = 'In trading session' if passed else 'Out of trading session'
                self.progressive_tracker.update_time_filter_details(
                    signal_id, passed, reason, session_start, session_end, 
                    timestamp.hour, timestamp.minute, passed
                )
    
    def _update_rsi_filter_tracking(self, time_filtered_signals: pd.Series, final_signals: pd.Series,
                                    rsi: pd.Series, signal_id_map: Dict):
        """Update progressive tracker with RSI filter details"""
        overbought = self.filters['rsi'].overbought
        oversold = self.filters['rsi'].oversold
        rsi_length = self.filters['rsi'].length
        
        for timestamp in time_filtered_signals.dropna().index:
            if timestamp in signal_id_map:
                signal_id = signal_id_map[timestamp]
                signal = time_filtered_signals.loc[timestamp]
                passed = pd.notna(final_signals.loc[timestamp])
                rsi_value = rsi.loc[timestamp] if timestamp in rsi.index else None
                
                if signal == 'BUY':
                    reason = 'Not overbought' if passed else 'Overbought'
                elif signal == 'SELL':
                    reason = 'Not oversold' if passed else 'Oversold'
                else:
                    reason = None
                
                self.progressive_tracker.update_rsi_details(
                    signal_id, passed, reason, rsi_value, rsi_length, overbought, oversold
                )
    
    def get_filter_stats(self, raw_signals: pd.Series, time_filtered: pd.Series, 
                         rsi_filtered: pd.Series) -> Dict:
        """Get comprehensive filter statistics"""
        return {
            'raw': {
                'buy': int((raw_signals == 'BUY').sum()),
                'sell': int((raw_signals == 'SELL').sum()),
                'total': int((raw_signals.notna()).sum())
            },
            'time_filtered': {
                'buy': int((time_filtered == 'BUY').sum()),
                'sell': int((time_filtered == 'SELL').sum()),
                'total': int((time_filtered.notna()).sum()),
                'rejected': int((raw_signals.notna()).sum()) - int((time_filtered.notna()).sum())
            },
            'rsi_filtered': {
                'buy': int((rsi_filtered == 'BUY').sum()),
                'sell': int((rsi_filtered == 'SELL').sum()),
                'total': int((rsi_filtered.notna()).sum()),
                'rejected': int((time_filtered.notna()).sum()) - int((rsi_filtered.notna()).sum())
            }
        }
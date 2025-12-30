"""
Filter Pipeline Module
Applies all filters: Time, RSI, Risk Management
"""
import pandas as pd
import numpy as np
from typing import Dict, Tuple, Optional

class FilterPipeline:
    def __init__(self, config: Dict):
        self.config = config
        self.filters = {}
        self.initialize_filters()
        
    def initialize_filters(self):
        """Initialize all filter components"""
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
        
        # Risk Manager (will be initialized with data later)
        self.filters['risk'] = None
        
    def initialize_risk_manager(self, df_full: pd.DataFrame):
        """Initialize risk manager with full data"""
        from src.strategies.trade_management.risk_manager import RiskManager
        self.filters['risk'] = RiskManager(
            self.config.get('trade_management', {}),
            df_full
        )
    
    def apply_time_filter(self, raw_signals: pd.Series) -> pd.Series:
        """Apply time filter to signals"""
        if not self.filters['time'].enabled:
            return raw_signals.copy()
        
        # Create signals DataFrame for time filtering
        raw_signals_df = pd.DataFrame({
            'timestamp': raw_signals.index,
            'signal': raw_signals.values
        }).dropna(subset=['signal'])
        
        # Apply time filter
        time_filtered_df = self.filters['time'].filter_signals_by_time(
            raw_signals_df, 
            timestamp_col='timestamp'
        )
        
        # Reconstruct time-filtered signals series
        time_filtered_signals = pd.Series(index=raw_signals.index, dtype=object)
        if not time_filtered_df.empty:
            time_filtered_signals.loc[time_filtered_df['timestamp'].values] = time_filtered_df['signal'].values
        
        return time_filtered_signals
    
    def apply_rsi_filter(self, df: pd.DataFrame, time_filtered_signals: pd.Series) -> pd.Series:
        """Apply RSI filter to signals"""
        not_overbought = self.filters['rsi'].apply_filter(df, is_long=True)
        not_oversold = self.filters['rsi'].apply_filter(df, is_long=False)
        
        final_signals = time_filtered_signals.copy()
        final_signals.loc[(time_filtered_signals == 'BUY') & ~not_overbought] = None
        final_signals.loc[(time_filtered_signals == 'SELL') & ~not_oversold] = None
        
        return final_signals
    
    def apply_risk_filter(self, df_strategy: pd.DataFrame, signals: pd.Series) -> Tuple[Dict, Dict]:
        """
        Apply risk management filter
        
        Returns:
            Tuple of (potential_trades_dict, risk_stats_dict)
        """
        if self.filters['risk'] is None:
            raise ValueError("Risk manager not initialized. Call initialize_risk_manager first.")
        
        risk_input_signals = signals.dropna()
        potential_trades = {}
        
        risk_stats = {
            'approved': {'buy': 0, 'sell': 0},
            'rejected': {'buy': 0, 'sell': 0},
            'adjusted': {'buy': 0, 'sell': 0},
            'total_approved': 0,
            'total_rejected': 0,
            'total_adjusted': 0
        }
        
        for timestamp, signal_type in risk_input_signals.items():
            is_long = (signal_type == 'BUY')
            entry_price = df_strategy.loc[timestamp, 'close']
            
            # Calculate SL/TP
            stop_loss, take_profit = self.filters['risk'].calculate_sl_tp(
                entry_price=entry_price,
                is_long=is_long,
                timestamp=timestamp
            )
            
            if stop_loss is None or take_profit is None:
                # No valid SL/TP (ATR not available)
                risk_stats['rejected']['buy' if is_long else 'sell'] += 1
                risk_stats['total_rejected'] += 1
                continue
            
            # Validate risk percentile
            is_valid, adjusted_sl, comment = self.filters['risk'].validate_risk_percentile(
                entry_price=entry_price,
                stop_loss=stop_loss,
                is_long=is_long,
                timestamp=timestamp
            )
            
            if not is_valid:
                # Trade rejected by risk management
                risk_stats['rejected']['buy' if is_long else 'sell'] += 1
                risk_stats['total_rejected'] += 1
                continue
            
            # Track if SL was adjusted
            original_sl = stop_loss
            if adjusted_sl != stop_loss:
                risk_stats['adjusted']['buy' if is_long else 'sell'] += 1
                risk_stats['total_adjusted'] += 1
                stop_loss = adjusted_sl
                # Recalculate TP based on adjusted SL
                sl_distance = abs(entry_price - stop_loss)
                rr_ratio = self.filters['risk'].sl_tp_config.get('risk_to_reward_ratio', 2.0)
                tp_distance = sl_distance * rr_ratio
                if is_long:
                    take_profit = entry_price + tp_distance
                else:
                    take_profit = entry_price - tp_distance
            
            # Trade approved by risk
            risk_stats['approved']['buy' if is_long else 'sell'] += 1
            risk_stats['total_approved'] += 1
            
            sl_distance = abs(entry_price - stop_loss)
            tp_distance = abs(entry_price - take_profit)
            
            potential_trades[timestamp] = {
                'timestamp': timestamp,
                'signal': signal_type,
                'entry': round(entry_price, 2),
                'sl': round(stop_loss, 2),
                'tp': round(take_profit, 2),
                'sl_distance': round(sl_distance, 2),
                'tp_distance': round(tp_distance, 2),
                'risk_reward_ratio': round(tp_distance / sl_distance, 2) if sl_distance > 0 else 0,
                'comment': comment,
                'sl_adjusted': (original_sl != stop_loss)
            }
        
        return potential_trades, risk_stats
    
    def get_filter_stats(self, raw_signals: pd.Series, time_filtered: pd.Series, 
                        rsi_filtered: pd.Series, risk_stats: Dict) -> Dict:
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
            },
            'risk_filtered': risk_stats
        }
"""Enhanced Progressive Signal Tracker - Production Optimized"""
import pandas as pd
import logging
from typing import Dict, Optional
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# Column definitions organized by logical groups for maintainability
BASIC_COLS = ['signal_id', 'timestamp', 'signal', 'mid_price', 'indicator_value', 'htf_signal']
INDICATOR_COLS = ['htf_bull', 'htf_bear', 'candle_type', 'rev_2d_2u', 'rev_2u_2d', 'we_buy_indicator', 'we_sell_indicator']
STAGE_COLS = ['stage_raw', 'stage_time', 'stage_rsi', 'stage_position', 'stage_risk', 'stage_trade']
STAGE_REASON_COLS = ['stage_time_reason', 'stage_rsi_reason', 'stage_position_reason', 'stage_risk_reason', 'stage_trade_reason']
TIME_FILTER_COLS = ['session_start', 'session_end', 'hour', 'minute', 'is_in_session']
RSI_FILTER_COLS = ['rsi_value', 'rsi_length', 'rsi_overbought', 'rsi_oversold', 'is_overbought', 'is_oversold']
POSITION_MGMT_COLS = ['current_direction', 'open_positions_count', 'pyramiding_enabled', 'close_on_opposite', 'can_open_new_position']
RISK_BASIC_COLS = ['entry_price', 'sl_price', 'tp_price', 'spread_cost']
ATR_COLS = ['atr_value', 'atr_length', 'atr_multiplier']
SL_CALC_COLS = ['sl_distance_raw', 'sl_price_raw']
RISK_PERCENTILE_COLS = ['annual_range_value', 'risk_percentile_calculated', 'max_risk_percentile', 'risk_percentile_passed']
FINAL_SLTP_COLS = ['sl_price_final', 'tp_price_final', 'rr_ratio']
SPREAD_COLS = ['spread_enabled', 'spread_type', 'spread_value', 'spread_points', 'entry_price_mid', 'entry_price_adjusted', 'spread_efficiency_percent']
TRADE_EXEC_COLS = ['trade_id', 'position_id', 'entry_time', 'entry_price_executed', 'sl_price_executed', 'tp_price_executed']
TRADE_EXIT_COLS = ['exit_time', 'exit_price', 'exit_reason', 'pnl_points', 'pnl_percent', 'duration_bars', 'duration_minutes', 'is_win', 'is_loss']
EXIT_CHECK_COLS = ['exit_check_high', 'exit_check_low', 'spread_adjusted_high', 'spread_adjusted_low']
META_COLS = ['final_status', 'processing_notes', 'execution_timestamp']

class EnhancedProgressiveTracker:
    """Production-optimized progressive tracker with O(1) lookups and memory-efficient storage"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.signals = []
        self.signal_dict = {}  # O(1) lookup by signal_id
        self.signal_counter = 0
        
        # Assemble all columns from logical groups
        self.columns = (BASIC_COLS + INDICATOR_COLS + STAGE_COLS + STAGE_REASON_COLS +
                       TIME_FILTER_COLS + RSI_FILTER_COLS + POSITION_MGMT_COLS +
                       RISK_BASIC_COLS + ATR_COLS + SL_CALC_COLS + RISK_PERCENTILE_COLS +
                       FINAL_SLTP_COLS + SPREAD_COLS + TRADE_EXEC_COLS + TRADE_EXIT_COLS +
                       EXIT_CHECK_COLS + META_COLS)
    
    def record_raw_signal(self, timestamp: pd.Timestamp, signal: str, mid_price: float, 
                         indicator_row: pd.Series = None, htf_signal: str = None) -> int:
        """Record raw signal with flattened indicator values"""
        self.signal_counter += 1
        
        # Initialize all columns with None
        signal_record = {col: None for col in self.columns}
        
        # Set basic info
        signal_record.update({
            'signal_id': self.signal_counter,
            'timestamp': timestamp,
            'signal': signal,
            'mid_price': mid_price,
            'htf_signal': htf_signal,
            'stage_raw': 'PASS',
            'final_status': 'RAW',
            'execution_timestamp': datetime.now().isoformat()
        })
        
        # Flatten indicator Series
        if indicator_row is not None and isinstance(indicator_row, pd.Series):
            signal_record['htf_bull'] = indicator_row.get('htf_bull')
            signal_record['htf_bear'] = indicator_row.get('htf_bear')
            signal_record['candle_type'] = indicator_row.get('candle_type')
            signal_record['rev_2d_2u'] = indicator_row.get('rev_2d_2u')
            signal_record['rev_2u_2d'] = indicator_row.get('rev_2u_2d')
            signal_record['we_buy_indicator'] = indicator_row.get('we_buy')
            signal_record['we_sell_indicator'] = indicator_row.get('we_sell')
        
        self.signals.append(signal_record)
        self.signal_dict[self.signal_counter] = signal_record  # O(1) lookup
        return self.signal_counter
    
    def update_time_filter_details(self, signal_id: int, passed: bool, reason: str = None,
                                   session_start: str = None, session_end: str = None,
                                   hour: int = None, minute: int = None, is_in_session: bool = None):
        """Update time filter stage"""
        sig = self.signal_dict.get(signal_id)
        if sig:
            sig['stage_time'] = 'PASS' if passed else 'REJECT'
            sig['stage_time_reason'] = reason
            sig['session_start'] = session_start
            sig['session_end'] = session_end
            sig['hour'] = hour
            sig['minute'] = minute
            sig['is_in_session'] = is_in_session
            sig['final_status'] = 'TIME_FILTERED' if passed else 'REJECTED_TIME'
            return True
        return False
    
    def update_rsi_details(self, signal_id: int, passed: bool, reason: str = None,
                          rsi_value: float = None, rsi_length: int = None,
                          rsi_overbought: float = None, rsi_oversold: float = None):
        """Update RSI filter stage"""
        sig = self.signal_dict.get(signal_id)
        if sig:
            sig['stage_rsi'] = 'PASS' if passed else 'REJECT'
            sig['stage_rsi_reason'] = reason
            sig['rsi_value'] = rsi_value
            sig['rsi_length'] = rsi_length
            sig['rsi_overbought'] = rsi_overbought
            sig['rsi_oversold'] = rsi_oversold
            sig['is_overbought'] = rsi_value >= rsi_overbought if rsi_value is not None else None
            sig['is_oversold'] = rsi_value <= rsi_oversold if rsi_value is not None else None
            
            if passed and sig['final_status'] != 'REJECTED_TIME':
                sig['final_status'] = 'RSI_FILTERED'
            elif not passed:
                sig['final_status'] = 'REJECTED_RSI'
            return True
        return False
    
    def update_position_management_details(self, signal_id: int, action: str, reason: str = None,
                                          current_direction: str = None, open_positions_count: int = 0,
                                          pyramiding_enabled: bool = False, close_on_opposite: bool = False,
                                          can_open_new_position: bool = False):
        """Update position management stage"""
        sig = self.signal_dict.get(signal_id)
        if sig:
            sig['stage_position'] = action
            sig['stage_position_reason'] = reason
            sig['current_direction'] = current_direction
            sig['open_positions_count'] = open_positions_count
            sig['pyramiding_enabled'] = pyramiding_enabled
            sig['close_on_opposite'] = close_on_opposite
            sig['can_open_new_position'] = can_open_new_position
            
            if action == 'OPEN':
                sig['final_status'] = 'POSITION_APPROVED'
            elif action == 'REJECT':
                sig['final_status'] = 'REJECTED_POSITION'
            elif action == 'CLOSE_AND_REVERSE':
                sig['final_status'] = 'POSITION_REVERSAL'
            return True
        return False
    
    def update_risk_management_details(self, signal_id: int, approved: bool, reason: str = None,
                                       entry_price: float = None, sl_price: float = None, 
                                       tp_price: float = None, spread_cost: float = None,
                                       atr_value: float = None, atr_length: int = None, atr_multiplier: float = None,
                                       sl_distance_raw: float = None, sl_price_raw: float = None,
                                       annual_range_value: float = None, risk_percentile_calculated: float = None,
                                       max_risk_percentile: float = None, risk_percentile_passed: bool = None,
                                       sl_price_final: float = None, tp_price_final: float = None, rr_ratio: float = None,
                                       spread_enabled: bool = None, spread_type: str = None,
                                       spread_value: float = None, spread_points: float = None,
                                       entry_price_mid: float = None, entry_price_adjusted: float = None,
                                       spread_efficiency_percent: float = None):
        """Update risk management stage with detailed calculations"""
        sig = self.signal_dict.get(signal_id)
        if sig:
            sig['stage_risk'] = 'APPROVED' if approved else 'REJECTED'
            sig['stage_risk_reason'] = reason
            sig['entry_price'] = entry_price
            sig['sl_price'] = sl_price
            sig['tp_price'] = tp_price
            sig['spread_cost'] = spread_cost
            sig['atr_value'] = atr_value
            sig['atr_length'] = atr_length
            sig['atr_multiplier'] = atr_multiplier
            sig['sl_distance_raw'] = sl_distance_raw
            sig['sl_price_raw'] = sl_price_raw
            sig['annual_range_value'] = annual_range_value
            sig['risk_percentile_calculated'] = risk_percentile_calculated
            sig['max_risk_percentile'] = max_risk_percentile
            sig['risk_percentile_passed'] = risk_percentile_passed
            sig['sl_price_final'] = sl_price_final
            sig['tp_price_final'] = tp_price_final
            sig['rr_ratio'] = rr_ratio
            sig['spread_enabled'] = spread_enabled
            sig['spread_type'] = spread_type
            sig['spread_value'] = spread_value
            sig['spread_points'] = spread_points
            sig['entry_price_mid'] = entry_price_mid
            sig['entry_price_adjusted'] = entry_price_adjusted
            sig['spread_efficiency_percent'] = spread_efficiency_percent
            
            sig['final_status'] = 'RISK_APPROVED' if approved else 'REJECTED_RISK'
            return True
        return False
    
    def update_trade_execution_details(self, signal_id: int, 
                                       trade_id: int = None, position_id: int = None,
                                       entry_time: pd.Timestamp = None, entry_price_executed: float = None,
                                       sl_price_executed: float = None, tp_price_executed: float = None,
                                       exit_time: pd.Timestamp = None, exit_price: float = None,
                                       exit_reason: str = None, pnl_points: float = None,
                                       pnl_percent: float = None, duration_bars: int = None,
                                       duration_minutes: float = None, is_win: bool = None,
                                       is_loss: bool = None, exit_check_high: float = None,
                                       exit_check_low: float = None, spread_adjusted_high: float = None,
                                       spread_adjusted_low: float = None, reason: str = None):
        """Update trade execution stage"""
        sig = self.signal_dict.get(signal_id)
        if sig:
            sig['stage_trade'] = 'EXECUTED' if trade_id else 'NOT_EXECUTED'
            sig['stage_trade_reason'] = reason
            sig['trade_id'] = trade_id
            sig['position_id'] = position_id
            sig['entry_time'] = entry_time
            sig['entry_price_executed'] = entry_price_executed
            sig['sl_price_executed'] = sl_price_executed
            sig['tp_price_executed'] = tp_price_executed
            sig['exit_time'] = exit_time
            sig['exit_price'] = exit_price
            sig['exit_reason'] = exit_reason
            sig['pnl_points'] = pnl_points
            sig['pnl_percent'] = pnl_percent
            sig['duration_bars'] = duration_bars
            sig['duration_minutes'] = duration_minutes
            sig['is_win'] = is_win
            sig['is_loss'] = is_loss
            sig['exit_check_high'] = exit_check_high
            sig['exit_check_low'] = exit_check_low
            sig['spread_adjusted_high'] = spread_adjusted_high
            sig['spread_adjusted_low'] = spread_adjusted_low
            
            if trade_id:
                sig['final_status'] = 'TRADE_CLOSED' if exit_price is not None else 'TRADE_OPEN'
            else:
                sig['final_status'] = 'NO_TRADE'
            return True
        return False
    
    def add_processing_note(self, signal_id: int, note: str):
        """Add processing note to signal"""
        sig = self.signal_dict.get(signal_id)
        if sig:
            sig['processing_notes'] = f"{sig['processing_notes']}; {note}" if sig['processing_notes'] else note
            return True
        return False
    
    def get_dataframe(self) -> pd.DataFrame:
        """Convert signals to memory-optimized DataFrame"""
        if not self.signals:
            return pd.DataFrame(columns=self.columns)
        
        df = pd.DataFrame(self.signals)
        
        # Ensure all columns present
        for col in self.columns:
            if col not in df.columns:
                df[col] = None
        
        # Memory optimization: convert to appropriate dtypes
        dtype_map = {
            # Booleans (1 byte)
            'is_in_session': 'bool',
            'is_overbought': 'bool',
            'is_oversold': 'bool',
            'pyramiding_enabled': 'bool',
            'close_on_opposite': 'bool',
            'can_open_new_position': 'bool',
            'risk_percentile_passed': 'bool',
            'spread_enabled': 'bool',
            'is_win': 'bool',
            'is_loss': 'bool',
            
            # Small integers (1-2 bytes)
            'hour': 'int8',
            'minute': 'int8',
            'rsi_length': 'int16',
            'atr_length': 'int16',
            'duration_bars': 'int16',
            
            # Standard integers (4 bytes)
            'signal_id': 'int32',
            'trade_id': 'int32',
            'position_id': 'int32',
            'open_positions_count': 'int32',
            
            # Floats (4 bytes - sufficient precision for prices)
            'mid_price': 'float32',
            'rsi_value': 'float32',
            'rsi_overbought': 'float32',
            'rsi_oversold': 'float32',
            'entry_price': 'float32',
            'sl_price': 'float32',
            'tp_price': 'float32',
            'spread_cost': 'float32',
            'atr_value': 'float32',
            'atr_multiplier': 'float32',
            'sl_distance_raw': 'float32',
            'sl_price_raw': 'float32',
            'annual_range_value': 'float32',
            'risk_percentile_calculated': 'float32',
            'max_risk_percentile': 'float32',
            'sl_price_final': 'float32',
            'tp_price_final': 'float32',
            'rr_ratio': 'float32',
            'spread_value': 'float32',
            'spread_points': 'float32',
            'entry_price_mid': 'float32',
            'entry_price_adjusted': 'float32',
            'spread_efficiency_percent': 'float32',
            'entry_price_executed': 'float32',
            'sl_price_executed': 'float32',
            'tp_price_executed': 'float32',
            'exit_price': 'float32',
            'pnl_points': 'float32',
            'pnl_percent': 'float32',
            'duration_minutes': 'float32',
            'exit_check_high': 'float32',
            'exit_check_low': 'float32',
            'spread_adjusted_high': 'float32',
            'spread_adjusted_low': 'float32',
        }
        
        for col, dtype in dtype_map.items():
            if col in df.columns:
                try:
                    df[col] = df[col].astype(dtype)
                except (ValueError, TypeError):
                    pass  # Keep original type if conversion fails
        
        return df[self.columns]  # Return in consistent order
    
    def save_to_csv(self, project_root: Path, timestamp_str: str = None) -> Path:
        """Save progressive signals to CSV with logging"""
        if timestamp_str is None:
            timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        progressive_dir = project_root / "outputs" / "signals" / "progressive"
        progressive_dir.mkdir(parents=True, exist_ok=True)
        
        csv_path = progressive_dir / f"signals_progressive_{timestamp_str}.csv"
        df = self.get_dataframe()
        df.to_csv(csv_path, index=False)
        
        logger.info(f"Progressive tracker: {len(df)} signals saved to CSV")
        return csv_path
    
    def get_statistics(self) -> Dict:
        """Get signal progression statistics"""
        df = self.get_dataframe()
        
        if df.empty:
            return {}
        
        stats = {
            'total_signals': len(df),
            'by_stage': {},
            'by_final_status': df['final_status'].value_counts().to_dict(),
            'rejection_breakdown': {}
        }
        
        # Count by stage
        for stage in ['time', 'rsi', 'position', 'risk', 'trade']:
            stage_col = f'stage_{stage}'
            if stage_col in df.columns:
                stage_counts = df[stage_col].value_counts().to_dict()
                stats['by_stage'][stage] = stage_counts
        
        # Rejection reasons
        for stage in ['TIME', 'RSI', 'POSITION', 'RISK']:
            rejected = df[df['final_status'] == f'REJECTED_{stage}']
            if not rejected.empty:
                reason_col = f'stage_{stage.lower()}_reason'
                if reason_col in rejected.columns:
                    reason_counts = rejected[reason_col].value_counts().to_dict()
                    stats['rejection_breakdown'][stage] = reason_counts
        
        return stats
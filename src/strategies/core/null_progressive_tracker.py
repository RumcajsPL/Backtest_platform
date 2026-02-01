"""
Null Progressive Tracker - Lightweight no-op version for Core mode
"""
from pathlib import Path
from typing import Dict, Any, Optional

class NullProgressiveTracker:
    """
    Null object implementation of progressive tracker.
    Does nothing but implements the same interface as EnhancedProgressiveTracker.
    Used in 'core' execution mode to skip expensive tracking operations.
    """
    
    def __init__(self, config: Dict = None):
        """Initialize null tracker (does nothing)"""
        self.columns = []
        pass
    
    def record_raw_signal(self, timestamp, signal, mid_price, indicator_row=None, htf_signal=None) -> int:
        """No-op: Return dummy signal_id"""
        return 0
    
    def update_time_filter(self, signal_id: int, passed: bool, reject_reason: Optional[str] = None):
        """No-op: Do nothing"""
        pass
    
    def update_rsi_filter(self, signal_id: int, passed: bool, reject_reason: Optional[str] = None):
        """No-op: Do nothing"""
        pass
    
    def update_risk_filter(self, signal_id: int, passed: bool, sl_price: float = None, 
                          tp_price: float = None, risk_amount: float = None,
                          reject_reason: Optional[str] = None):
        """No-op: Do nothing"""
        pass
    
    def update_position_management_details(self, signal_id: int, position_action: str,
                                          position_reason: str, trade_id: Optional[int] = None,
                                          position_id: Optional[int] = None):
        """No-op: Do nothing"""
        pass
    
    def update_execution_details(self, signal_id: int, executed: bool, 
                                exit_reason: Optional[str] = None,
                                exit_time=None, exit_price: float = None,
                                pnl_points: float = None):
        """No-op: Do nothing"""
        pass
    
    def save_to_csv(self, project_root: Path, timestamp_str: str) -> Path:
        """No-op: Return dummy path"""
        return Path('/dev/null')
    
    def get_statistics(self) -> Dict[str, Any]:
        """Return minimal empty statistics"""
        return {
            'total_signals': 0,
            'by_final_status': {},
            'rejection_breakdown': {},
            'mode': 'null_tracker'
        }
    
    def __bool__(self):
        """Allow truthiness checks: if progressive_tracker: ..."""
        return False
"""
Dashboard Modules Package
"""

from .data_loader import DashboardDataLoader
from .display_engine import DisplayEngine
from .drawdown_display import DrawdownDisplay
from .metrics_display import MetricsDisplay
from .position_management_display import PositionManagementDisplay
from .signal_flow_display import SignalFlowDisplay
from .time_based_display import TimeBasedDisplay
from .trade_analysis_display import TradeAnalysisDisplay
from .visualizations import DashboardVisualizations

# Import the new progressive analysis module
try:
    from .progressive_analysis import ProgressiveAnalysisDisplay
except ImportError:
    ProgressiveAnalysisDisplay = None
    print("Note: ProgressiveAnalysisDisplay not available")

__all__ = [
    'DashboardDataLoader',
    'DisplayEngine',
    'DrawdownDisplay',
    'MetricsDisplay',
    'PositionManagementDisplay',
    'SignalFlowDisplay',
    'TimeBasedDisplay',
    'TradeAnalysisDisplay',
    'DashboardVisualizations',
    'ProgressiveAnalysisDisplay',
]
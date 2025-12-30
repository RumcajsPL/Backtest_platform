"""
Strategy Modules Package
"""

__version__ = "1.0.0"
__author__ = "Backtesting Platform"

# Export main classes
from .data_loader import DataLoader
from .signal_generator import SignalGenerator
from .filter_pipeline import FilterPipeline
from .trade_simulator import TradeSimulator
from .trade_tracker import TradeTracker
from .report_generator import ReportGenerator
from .metrics_calculator import calculate_performance_metrics

__all__ = [
    'DataLoader',
    'SignalGenerator',
    'FilterPipeline',
    'TradeSimulator',
    'TradeTracker',
    'ReportGenerator',
    'calculate_performance_metrics'
]
"""
Contracts Package for WBWSStrategy Migration

This package contains all typed contracts that replace dict-based
communication between modules.

Available contracts:
- Data contracts: DataConfig, DataBundle, DataInfo, DataValidationResult
- Signal contracts: SignalType, Signal, SignalFrame, SignalStats
- Trade contracts: (to be added in Phase 4)

Author: Migration Project
Version: 1.0.0
Date: 2025-02-09
"""

# Data contracts
from .data_contracts import (
    DateRange,
    DataFileConfig,
    DataConfig,
    DataValidationResult,
    DataInfo,
    DataBundle,
    CacheStats,
)

# Signal contracts
from .signal_contracts import (
    SignalType,
    Signal,
    SignalFrame,
    SignalStats,
)

__all__ = [
    # Data contracts
    "DateRange",
    "DataFileConfig",
    "DataConfig",
    "DataValidationResult",
    "DataInfo",
    "DataBundle",
    "CacheStats",
    # Signal contracts
    "SignalType",
    "Signal",
    "SignalFrame",
    "SignalStats",
]

__version__ = "1.0.0"
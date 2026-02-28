"""
Filter Layer Contracts for Strategy Builder
This module defines typed contracts for filter execution and pipeline orchestration.
Version: 3.1.0
"""

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Protocol, Optional, Dict, Any
import pandas as pd
import numpy as np

from .signal_contracts import SignalFrame

# =============================================================================
# FILTER STATUS ENUM
# =============================================================================

class FilterStatus(Enum):
    """
    Enumeration of filter execution statuses.
    
    Used to track whether a filter passed, rejected, skipped, or encountered an error.
    """
    PASSED = auto()     # Signals passed filter criteria
    REJECTED = auto()   # Signals failed filter criteria
    SKIPPED = auto()    # Filter was disabled or not applicable
    ERROR = auto()      # Filter execution encountered an error
    
    def __str__(self) -> str:
        return self.name

# =============================================================================
# FILTER METADATA
# =============================================================================

@dataclass(frozen=True)
class FilterMetadata:
    """
    Metadata about a single filter's execution.
    
    Attributes:
        filter_name: Name of the filter (e.g., "rsi_filter", "time_filter")
        status: Execution status (PASSED/REJECTED/SKIPPED/ERROR)
        reason: Optional explanation (e.g., "RSI overbought", "Outside trading hours")
        signals_in: Number of signals before this filter
        signals_out: Number of signals after this filter
        signals_rejected: Number of signals rejected by this filter
        indicator_values: Indicator values at signal times (analytics mode only)
        execution_time_ms: Filter execution time in milliseconds (always collected)
    """
    filter_name: str
    status: FilterStatus
    signals_in: int
    signals_out: int
    signals_rejected: int = 0
    reason: Optional[str] = None
    indicator_values: Optional[Dict[str, float]] = None
    execution_time_ms: Optional[float] = None
    
    def __post_init__(self):
        """Validate metadata."""
        # Ensure rejection count is consistent
        expected_rejected = self.signals_in - self.signals_out
        if self.signals_rejected == 0:
            object.__setattr__(self, 'signals_rejected', expected_rejected)
    
    @property
    def rejection_rate(self) -> float:
        """Calculate percentage of signals rejected."""
        if self.signals_in == 0:
            return 0.0
        return (self.signals_rejected / self.signals_in) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = {
            "filter_name": self.filter_name,
            "status": self.status.name,
            "signals_in": self.signals_in,
            "signals_out": self.signals_out,
            "signals_rejected": self.signals_rejected,
            "rejection_rate": f"{self.rejection_rate:.1f}%"
        }
        
        if self.reason:
            result["reason"] = self.reason
        if self.execution_time_ms is not None:
            result["execution_time_ms"] = round(self.execution_time_ms, 2)
        if self.indicator_values:
            result["indicator_values"] = self.indicator_values
        
        return result
    
    def __str__(self) -> str:
        status_icon = {
            FilterStatus.PASSED: "✅",
            FilterStatus.REJECTED: "❌",
            FilterStatus.SKIPPED: "⏭️",
            FilterStatus.ERROR: "⚠️"
        }.get(self.status, "❓")
        
        base = f"{status_icon} {self.filter_name}: {self.signals_in} → {self.signals_out}"
        
        if self.signals_rejected > 0:
            base += f" (-{self.signals_rejected}, {self.rejection_rate:.1f}%)"
        
        if self.reason:
            base += f" | {self.reason}"
        
        return base

# =============================================================================
# FILTER RESULT
# =============================================================================

@dataclass(frozen=True)
class FilterResult:
    """
    Result of applying a single filter.
    
    Attributes:
        passed: Overall pass/fail status (True if any signals passed)
        signal_frame: Filtered SignalFrame (subset of input signals)
        metadata: Execution metadata
    """
    passed: bool
    signal_frame: SignalFrame
    metadata: FilterMetadata
    
    @property
    def signals_count(self) -> int:
        """Number of signals that passed this filter."""
        return int(np.sum(self.signal_frame.signals.values != 0))
    
    @property
    def is_empty(self) -> bool:
        """Returns True if no signals passed."""
        return not self.passed or self.signals_count == 0
    
    def __str__(self) -> str:
        return f"FilterResult({self.metadata})"

# =============================================================================
# FILTER PIPELINE RESULT
# =============================================================================

@dataclass(frozen=True)
class FilterPipelineResult:
    """
    Result of applying the entire filter pipeline.
    
    Attributes:
        final_signals: Final filtered SignalFrame
        raw_count: Number of signals before any filtering
        time_filtered_count: Number of signals after time filter
        technical_filtered_count: Number of signals after technical filters
        final_count: Final number of signals (same as technical_filtered_count)
        filter_results: List of metadata from each filter
        rejection_reasons: Summary of rejection reasons with counts
        execution_time_ms: Total pipeline execution time
    """
    final_signals: SignalFrame
    raw_count: int
    time_filtered_count: int
    technical_filtered_count: int
    final_count: int
    filter_results: list[FilterMetadata]
    rejection_reasons: Dict[str, int] = field(default_factory=dict)
    execution_time_ms: Optional[float] = None
    
    @property
    def time_rejection_count(self) -> int:
        """Number of signals rejected by time filter."""
        return self.raw_count - self.time_filtered_count
    
    @property
    def technical_rejection_count(self) -> int:
        """Number of signals rejected by technical filters."""
        return self.time_filtered_count - self.technical_filtered_count
    
    @property
    def total_rejection_count(self) -> int:
        """Total number of signals rejected."""
        return self.raw_count - self.final_count
    
    @property
    def pass_rate(self) -> float:
        """Percentage of signals that passed all filters."""
        if self.raw_count == 0:
            return 0.0
        return (self.final_count / self.raw_count) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = {
            "counts": {
                "raw": self.raw_count,
                "time_filtered": self.time_filtered_count,
                "technical_filtered": self.technical_filtered_count,
                "final": self.final_count
            },
            "rejections": {
                "time_filter": self.time_rejection_count,
                "technical_filters": self.technical_rejection_count,
                "total": self.total_rejection_count
            },
            "pass_rate": f"{self.pass_rate:.1f}%",
            "filters": [fm.to_dict() for fm in self.filter_results]
        }
        
        if self.rejection_reasons:
            result["rejection_reasons"] = self.rejection_reasons
        
        if self.execution_time_ms is not None:
            result["execution_time_ms"] = round(self.execution_time_ms, 2)
        
        return result
    
    def get_stats_summary(self) -> str:
        """Get human-readable statistics summary."""
        lines = [
            f"Raw signals: {self.raw_count:,}",
            f"Time filtered: {self.time_filtered_count:,} (-{self.time_rejection_count})",
            f"Technical filtered: {self.technical_filtered_count:,} (-{self.technical_rejection_count})",
            f"Final signals: {self.final_count:,}",
            f"Pass rate: {self.pass_rate:.1f}%"
        ]
        
        if self.execution_time_ms:
            lines.append(f"Execution time: {self.execution_time_ms:.1f}ms")
        
        return "\n".join(lines)
    
    def __str__(self) -> str:
        return f"FilterPipelineResult({self.final_count}/{self.raw_count} signals, {self.pass_rate:.1f}% pass rate)"

# =============================================================================
# FILTER PROTOCOL
# =============================================================================

class FilterProtocol(Protocol):
    """
    Standard interface for all filter implementations.
    
    All filters must implement these methods to be used in the FilterPipeline.
    """
    
    name: str
    enabled: bool
    
    def compute_indicators(
        self,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray]
    ) -> None:
        """
        Compute and cache indicators needed by this filter.
        
        Args:
            df: OHLCV DataFrame
            indicators: Dict to store pandas Series indicators
            ind_np: Dict to store numpy array indicators (for performance)
        """
        ...
    
    def apply_filter(
        self,
        signal_frame: SignalFrame,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray],
        mode: str = "core"
    ) -> FilterResult:
        """
        Apply filter logic to signals.
        
        Args:
            signal_frame: Input signals to filter
            df: OHLCV DataFrame
            indicators: Cached pandas Series indicators
            ind_np: Cached numpy array indicators
            mode: Execution mode ("core" or "analytics")
        
        Returns:
            FilterResult with filtered signals and metadata
        """
        ...
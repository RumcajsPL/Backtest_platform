"""
Signal Layer Contracts for WBWSStrategy Migration

This module defines typed contracts for signal generation and classification.
These contracts replace string-based signal communication ("BUY"/"SELL").

Author: Migration Project
Version: 1.0.0
Date: 2025-02-09
"""

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional, Dict, Any
from datetime import datetime
import pandas as pd


# =============================================================================
# SIGNAL TYPE ENUM
# =============================================================================

class SignalType(Enum):
    """
    Enumeration of possible signal types.
    
    Replaces string-based "BUY"/"SELL" with typed enum.
    """
    BUY = auto()
    SELL = auto()
    
    def __str__(self) -> str:
        return self.name
    
    @classmethod
    def from_string(cls, s: str) -> Optional["SignalType"]:
        """
        Convert string to SignalType.
        
        Args:
            s: String representation ("BUY" or "SELL")
            
        Returns:
            SignalType or None if invalid
        """
        s_upper = s.upper() if s else ""
        if s_upper == "BUY":
            return cls.BUY
        elif s_upper == "SELL":
            return cls.SELL
        return None
    
    @property
    def is_long(self) -> bool:
        """Returns True if this is a buy/long signal."""
        return self == SignalType.BUY
    
    @property
    def is_short(self) -> bool:
        """Returns True if this is a sell/short signal."""
        return self == SignalType.SELL


# =============================================================================
# SIGNAL CONTRACTS
# =============================================================================

@dataclass(frozen=True)
class Signal:
    """
    Represents a single trading signal at a specific timestamp.
    
    Attributes:
        timestamp: When the signal occurred
        signal_type: Type of signal (BUY/SELL)
        mid_price: Market price at signal time
        metadata: Additional signal context (indicator values, etc.)
    """
    timestamp: datetime
    signal_type: SignalType
    mid_price: float
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate signal."""
        if self.mid_price <= 0:
            raise ValueError(f"mid_price must be positive, got {self.mid_price}")
    
    def __str__(self) -> str:
        return f"{self.signal_type} @ {self.timestamp} (price: {self.mid_price:.2f})"
    
    @property
    def is_long(self) -> bool:
        """Returns True if this is a buy/long signal."""
        return self.signal_type.is_long
    
    @property
    def is_short(self) -> bool:
        """Returns True if this is a sell/short signal."""
        return self.signal_type.is_short


@dataclass
class SignalFrame:
    """
    Collection of signals with associated indicator data.
    
    This bridges the gap between vectorized signal generation
    and per-signal processing.
    
    Attributes:
        signals: Series of SignalType (index = timestamps, values = signal types)
        indicator_data: DataFrame of indicator values (optional)
        signal_metadata: Additional context about signal generation
    """
    signals: pd.Series
    indicator_data: Optional[pd.DataFrame] = None
    signal_metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate signal frame."""
        if not isinstance(self.signals.index, pd.DatetimeIndex):
            raise ValueError("signals must have DatetimeIndex")
    
    def __len__(self) -> int:
        return len(self.signals)
    
    def __iter__(self):
        """Iterate over individual Signal objects."""
        for ts, sig_type in self.signals.dropna().items():
            if not isinstance(sig_type, SignalType):
                # Handle legacy string conversion
                sig_type = SignalType.from_string(sig_type)
                if sig_type is None:
                    continue
            
            # Get mid price (from indicator data or metadata)
            mid_price = 0.0
            if self.indicator_data is not None and ts in self.indicator_data.index:
                # Try to get close price from indicator data
                if "close" in self.indicator_data.columns:
                    mid_price = float(self.indicator_data.loc[ts, "close"])
            
            # Get metadata for this specific signal
            metadata = {}
            if self.indicator_data is not None and ts in self.indicator_data.index:
                metadata = self.indicator_data.loc[ts].to_dict()
            
            yield Signal(
                timestamp=ts,
                signal_type=sig_type,
                mid_price=mid_price,
                metadata=metadata
            )
    
    def get_signal_at(self, timestamp: datetime) -> Optional[Signal]:
        """
        Get signal at specific timestamp.
        
        Args:
            timestamp: Timestamp to query
            
        Returns:
            Signal object or None if no signal at that time
        """
        if timestamp not in self.signals.index:
            return None
        
        sig_value = self.signals.loc[timestamp]
        if pd.isna(sig_value):
            return None
        
        sig_type = sig_value if isinstance(sig_value, SignalType) else SignalType.from_string(sig_value)
        if sig_type is None:
            return None
        
        # Get mid price
        mid_price = 0.0
        metadata = {}
        if self.indicator_data is not None and timestamp in self.indicator_data.index:
            if "close" in self.indicator_data.columns:
                mid_price = float(self.indicator_data.loc[timestamp, "close"])
            metadata = self.indicator_data.loc[timestamp].to_dict()
        
        return Signal(
            timestamp=timestamp,
            signal_type=sig_type,
            mid_price=mid_price,
            metadata=metadata
        )
    
    @property
    def buy_signals(self) -> pd.Series:
        """Return only BUY signals."""
        return self.signals[self.signals == SignalType.BUY]
    
    @property
    def sell_signals(self) -> pd.Series:
        """Return only SELL signals."""
        return self.signals[self.signals == SignalType.SELL]
    
    def count_by_type(self) -> Dict[str, int]:
        """
        Count signals by type.
        
        Returns:
            Dict with counts for each signal type
        """
        counts = self.signals.value_counts()
        return {
            "buy": int(counts.get(SignalType.BUY, 0)),
            "sell": int(counts.get(SignalType.SELL, 0)),
            "total": len(self.signals.dropna())
        }
    
    def __str__(self) -> str:
        counts = self.count_by_type()
        return f"SignalFrame({counts['total']} signals: {counts['buy']} BUY, {counts['sell']} SELL)"


# =============================================================================
# SIGNAL STATISTICS
# =============================================================================

@dataclass
class SignalStats:
    """
    Statistics about generated signals.
    
    Attributes:
        buy_count: Number of BUY signals
        sell_count: Number of SELL signals
        total_count: Total number of signals
        buy_percentage: Percentage of BUY signals
        sell_percentage: Percentage of SELL signals
        metadata: Additional statistics
    """
    buy_count: int = 0
    sell_count: int = 0
    total_count: int = 0
    buy_percentage: float = 0.0
    sell_percentage: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def from_signal_frame(cls, signal_frame: SignalFrame) -> "SignalStats":
        """
        Create SignalStats from a SignalFrame.
        
        Args:
            signal_frame: SignalFrame to analyze
            
        Returns:
            SignalStats instance
        """
        counts = signal_frame.count_by_type()
        total = counts["total"]
        buy_pct = (counts["buy"] / total * 100) if total > 0 else 0.0
        sell_pct = (counts["sell"] / total * 100) if total > 0 else 0.0
        
        return cls(
            buy_count=counts["buy"],
            sell_count=counts["sell"],
            total_count=total,
            buy_percentage=buy_pct,
            sell_percentage=sell_pct,
            metadata=signal_frame.signal_metadata
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "buy": self.buy_count,
            "sell": self.sell_count,
            "total": self.total_count,
            "buy_percentage": round(self.buy_percentage, 2),
            "sell_percentage": round(self.sell_percentage, 2),
            **self.metadata
        }
    
    def __str__(self) -> str:
        return (
            f"BUY: {self.buy_count} ({self.buy_percentage:.1f}%), "
            f"SELL: {self.sell_count} ({self.sell_percentage:.1f}%), "
            f"Total: {self.total_count}"
        )
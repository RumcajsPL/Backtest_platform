"""
Data Layer Contracts for WBWSStrategy Migration v2.1

Version: 2.2.0 (Hardening II Final)
Date: 2026-02-21
Session: 21 - Final Hardening

Changes from v2.2.0:
- Block B: Removed from_yaml_config legacy adapter (B-5)
- Block B: Enhanced validation for date ranges and data quality
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Dict, Any, Tuple
from datetime import datetime
import pandas as pd


# =============================================================================
# DATA CONFIGURATION CONTRACTS
# =============================================================================

@dataclass(frozen=True)
class DateRange:
    """
    Represents a time range for data slicing.
    
    Attributes:
        start: Start datetime (inclusive)
        end: End datetime (inclusive)
    """
    start: Optional[datetime] = None
    end: Optional[datetime] = None
    
    def __post_init__(self):
        """Validate date range."""
        if self.start and self.end and self.start > self.end:
            raise ValueError(f"Start date {self.start} is after end date {self.end}")
    
    @property
    def is_bounded(self) -> bool:
        """Returns True if both start and end are specified."""
        return self.start is not None and self.end is not None
    
    def __str__(self) -> str:
        start_str = self.start.strftime("%Y-%m-%d %H:%M:%S") if self.start else "unlimited"
        end_str = self.end.strftime("%Y-%m-%d %H:%M:%S") if self.end else "unlimited"
        return f"{start_str} → {end_str}"


@dataclass(frozen=True)
class DataFileConfig:
    """
    Configuration for a single data file.
    
    Attributes:
        path: Path to the data file (CSV or Parquet)
        format: File format ("csv" or "parquet")
        date_range: Optional date range for slicing
        description: Human-readable description
    """
    path: Path
    format: str = "parquet"
    date_range: Optional[DateRange] = None
    description: str = ""
    
    def __post_init__(self):
        """Validate file config."""
        # Convert path to Path object if string
        object.__setattr__(self, 'path', Path(self.path))
        
        # Validate format
        if self.format not in ("csv", "parquet"):
            raise ValueError(f"Unsupported format: {self.format}. Must be 'csv' or 'parquet'")
        
        # Validate file extension matches format
        expected_suffix = f".{self.format}"
        if self.path.suffix.lower() != expected_suffix:
            raise ValueError(
                f"File extension {self.path.suffix} doesn't match format {self.format}"
            )


@dataclass(frozen=True)
class DataConfig:
    """
    Complete data loading configuration.

    Constructed directly from StrategyConfig - no legacy adapters.

    Attributes:
        strategy_data: Main strategy timeframe data config
        htf_data: Higher timeframe data config (optional)
        ltf_data: Lower timeframe data config (optional)
        artf_data: Annual Range Timeframe / Monthly data (optional)
        date_range: Global date range (overrides individual configs)
        validation_rules: Data validation parameters
    """
    strategy_data: DataFileConfig
    htf_data: Optional[DataFileConfig] = None
    ltf_data: Optional[DataFileConfig] = None
    artf_data: Optional[DataFileConfig] = None
    date_range: Optional[DateRange] = None
    validation_rules: Dict[str, Any] = field(default_factory=dict)

    # NOTE: from_yaml_config has been removed (B-5)


# =============================================================================
# DATA BUNDLE CONTRACTS
# =============================================================================

@dataclass
class DataValidationResult:
    """
    Result of data validation checks.
    
    Attributes:
        is_valid: Overall validation status
        checks: Dict of individual check results
        errors: List of error messages
        warnings: List of warning messages
    """
    is_valid: bool
    checks: Dict[str, bool] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    
    def __str__(self) -> str:
        status = "✅ VALID" if self.is_valid else "❌ INVALID"
        details = []
        if self.errors:
            details.append(f"Errors: {len(self.errors)}")
        if self.warnings:
            details.append(f"Warnings: {len(self.warnings)}")
        
        detail_str = f" ({', '.join(details)})" if details else ""
        return f"{status}{detail_str}"


@dataclass
class DataInfo:
    """
    Metadata about loaded data.
    
    Attributes:
        total_bars: Total number of bars in full dataset
        strategy_bars: Number of bars in strategy period
        htf_bars: Number of bars in HTF data (0 if not loaded)
        ltf_bars: Number of bars in LTF data (0 if not loaded)
        artf_bars: Number of bars in ARTF data (0 if not loaded)
        date_range: Actual date range of strategy data
        ltf_timeframe: LTF timeframe (e.g., "1s")
        artf_timeframe: ARTF timeframe (e.g., "1ME" for month-end)
        cache_hit: Whether data was loaded from cache
    """
    total_bars: int
    strategy_bars: int
    htf_bars: int = 0
    ltf_bars: int = 0
    artf_bars: int = 0
    date_range: Optional[Tuple[datetime, datetime]] = None
    ltf_timeframe: str = "1s"
    artf_timeframe: str = "1ME"
    cache_hit: bool = False
    
    def __str__(self) -> str:
        lines = [
            f"Strategy period: {self.strategy_bars:,} bars",
            f"Full dataset: {self.total_bars:,} bars"
        ]
        if self.htf_bars > 0:
            lines.append(f"HTF data: {self.htf_bars:,} bars")
        if self.ltf_bars > 0:
            lines.append(f"LTF data: {self.ltf_bars:,} bars ({self.ltf_timeframe})")
        if self.artf_bars > 0:
            lines.append(f"ARTF data: {self.artf_bars:,} bars ({self.artf_timeframe})")
        if self.date_range:
            start, end = self.date_range
            lines.append(f"Date range: {start} → {end}")
        if self.cache_hit:
            lines.append("Cache: HIT ⚡")
        return "\n".join(lines)


@dataclass
class DataBundle:
    """
    Complete bundle of loaded market data.
    
    This is the primary contract returned by DataLoader.
    
    Attributes:
        full: Complete dataset (all available data)
        strategy: Data for strategy period (date-sliced)
        htf: Higher timeframe data (optional)
        ltf: Lower timeframe data (optional, typically 1s)
        artf: Annual Range Timeframe / Monthly data (optional)
        info: Metadata about the loaded data
        validation: Validation results
        config: Configuration used to load this data
    """
    full: pd.DataFrame
    strategy: pd.DataFrame
    htf: Optional[pd.DataFrame] = None
    ltf: Optional[pd.DataFrame] = None
    artf: Optional[pd.DataFrame] = None
    info: DataInfo = field(default_factory=lambda: DataInfo(0, 0))
    validation: DataValidationResult = field(default_factory=lambda: DataValidationResult(True))
    config: Optional[DataConfig] = None
    
    def __post_init__(self):
        """Validate DataFrame structure."""
        self._validate_dataframe(self.strategy, "strategy")
        
        if self.htf is not None:
            self._validate_dataframe(self.htf, "htf")
        if self.ltf is not None:
            self._validate_dataframe(self.ltf, "ltf")
        if self.artf is not None:
            self._validate_dataframe(self.artf, "artf")
    
    def _validate_dataframe(self, df: pd.DataFrame, name: str):
        """Validate a single DataFrame."""
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError(f"{name} DataFrame must have DatetimeIndex")
        
        required = ["open", "high", "low", "close"]
        missing = [col for col in required if col not in df.columns]
        if missing:
            raise ValueError(f"{name} DataFrame missing columns: {missing}")
    
    @property
    def has_htf(self) -> bool:
        return self.htf is not None and not self.htf.empty
    
    @property
    def has_ltf(self) -> bool:
        return self.ltf is not None and not self.ltf.empty
    
    @property
    def has_artf(self) -> bool:
        return self.artf is not None and not self.artf.empty
    
    def __str__(self) -> str:
        return f"DataBundle({self.info})"


# =============================================================================
# CACHE STATISTICS (for monitoring)
# =============================================================================

@dataclass
class CacheStats:
    """
    Statistics about data loading cache performance.
    
    Attributes:
        hits: Number of cache hits
        misses: Number of cache misses
        hit_rate: Cache hit rate percentage
        total_files: Number of cached files
        total_size_mb: Total cache size in MB
        cache_dir: Path to cache directory
    """
    hits: int = 0
    misses: int = 0
    hit_rate: float = 0.0
    total_files: int = 0
    total_size_mb: float = 0.0
    cache_dir: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": f"{self.hit_rate:.1f}%",
            "cache_files": self.total_files,
            "cache_size_mb": round(self.total_size_mb, 2),
            "cache_dir": self.cache_dir
        }
    
    def __str__(self) -> str:
        return f"Cache: {self.hits}/{self.hits + self.misses} hits ({self.hit_rate:.1f}%)"
"""
DataLoader v2 - Migration to Typed Contracts

This is the new DataLoader implementation that returns a typed DataBundle
instead of a 4-tuple. It reuses all the optimized caching and loading logic
from the original DataLoader.

Key differences from original:
- Returns DataBundle (typed) instead of 4-tuple
- Uses DataConfig contract for configuration
- Includes validation in DataBundle
- Explicit metadata (DataInfo, ValidationResult, CacheStats)

Author: Migration Project
Version: 2.0.0
Date: 2025-02-09
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional
import hashlib
import pickle
import logging

from src.utils.paths import PROJECT_ROOT
from src.strategies.contracts.data_contracts import (
    DataConfig,
    DataBundle,
    DataInfo,
    DataValidationResult,
    CacheStats,
    DateRange,
)

logger = logging.getLogger(__name__)


class DataLoader:
    """
    Modern DataLoader with typed contracts.
    
    Features:
    - Returns DataBundle (typed contract)
    - Supports CSV + Parquet
    - Intelligent caching with MD5 validation
    - Data sanitization (inf → nan → ffill/bfill)
    - Date range slicing
    - Comprehensive validation
    
    Performance:
    - Cold load (CSV): ~260ms
    - Cold load (Parquet): ~40ms
    - Cache hit: ~5-20ms
    """

    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(self, config_path: str, project_root: Optional[Path] = None):
        """
        Initialize DataLoader.
        
        Args:
            config_path: Path to YAML configuration file
            project_root: Project root for resolving relative paths (default: auto-detect)
        """
        self.config_path = Path(config_path).resolve()
        self.project_root = project_root or PROJECT_ROOT
        
        # Will be populated by load_config()
        self.raw_config = None
        self.data_config: Optional[DataConfig] = None
        
        # Cache management
        self.cache_dir = Path.home() / ".wbws_data_cache"
        self.cache_dir.mkdir(exist_ok=True)
        
        # Cache statistics
        self._cache_hits = 0
        self._cache_misses = 0

    # =============================================================================
    # CONFIGURATION LOADING
    # =============================================================================

    def load_config(self) -> DataConfig:
        """
        Load and parse configuration file.
        
        Returns:
            DataConfig: Typed configuration object
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config is malformed
        """
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        logger.info(f"Loading config from: {self.config_path.name}")
        
        # Load raw YAML
        import yaml
        with open(self.config_path, "r", encoding="utf-8") as f:
            self.raw_config = yaml.safe_load(f)
        
        # Parse into typed DataConfig
        data_section = self.raw_config.get("data", {})
        self.data_config = DataConfig.from_yaml_config(data_section, self.project_root)
        
        logger.info(f"  Strategy data: {self.data_config.strategy_data.path.name}")
        if self.data_config.htf_data:
            logger.info(f"  HTF data: {self.data_config.htf_data.path.name}")
        if self.data_config.ltf_data:
            logger.info(f"  LTF data: {self.data_config.ltf_data.path.name}")
        if self.data_config.date_range:
            logger.info(f"  Date range: {self.data_config.date_range}")
        
        return self.data_config

    # =============================================================================
    # CACHE MANAGEMENT (REUSED FROM ORIGINAL)
    # =============================================================================

    def _get_cache_key(self, file_path: Path, date_range: Optional[DateRange] = None) -> Optional[str]:
        """
        Generate cache key for a file.
        
        Args:
            file_path: Path to data file
            date_range: Optional date range (affects cache key)
            
        Returns:
            MD5 hash cache key or None if file doesn't exist
        """
        if not file_path.exists():
            return None

        stat = file_path.stat()
        key_parts = [
            str(file_path.resolve()),
            f"size:{stat.st_size}",
            f"mtime:{stat.st_mtime}",
        ]

        # Add content hash (first 256KB for speed)
        try:
            with open(file_path, "rb") as f:
                content = f.read(256 * 1024)
            key_parts.append(f"content:{hashlib.md5(content).hexdigest()}")
        except Exception as e:
            logger.warning(f"Failed to compute content hash for {file_path.name}: {e}")

        # Add date range to key
        if date_range:
            if date_range.start:
                key_parts.append(f"start:{date_range.start.isoformat()}")
            if date_range.end:
                key_parts.append(f"end:{date_range.end.isoformat()}")

        return hashlib.md5("|".join(key_parts).encode()).hexdigest()

    def _load_cached_data(self, cache_key: str) -> Optional[pd.DataFrame]:
        """Load DataFrame from cache."""
        if not cache_key:
            return None

        cache_file = self.cache_dir / f"{cache_key}.pkl"
        if not cache_file.exists():
            return None

        try:
            with open(cache_file, "rb") as f:
                return pickle.load(f)
        except Exception as e:
            logger.warning(f"Cache corrupted, deleting {cache_file.name}: {e}")
            cache_file.unlink(missing_ok=True)
            return None

    def _save_to_cache(self, cache_key: str, df: pd.DataFrame):
        """Save DataFrame to cache."""
        if not cache_key:
            return
        cache_file = self.cache_dir / f"{cache_key}.pkl"
        try:
            with open(cache_file, "wb") as f:
                pickle.dump(df, f)
        except Exception as e:
            logger.warning(f"Cache save failed for {cache_file.name}: {e}")

    # =============================================================================
    # FILE LOADING (REUSED FROM ORIGINAL)
    # =============================================================================

    def _load_file_with_cache(
        self, 
        file_path: Path, 
        data_type: str,
        date_range: Optional[DateRange] = None
    ) -> pd.DataFrame:
        """
        Load a single data file with caching.
        
        Args:
            file_path: Path to data file
            data_type: Description (e.g., "strategy", "htf", "ltf")
            date_range: Optional date range for slicing
            
        Returns:
            DataFrame with DatetimeIndex
        """
        # Validate file exists
        if not file_path.exists():
            raise FileNotFoundError(f"{data_type} file not found: {file_path}")
        
        # Try cache
        cache_key = self._get_cache_key(file_path, date_range)
        cached = self._load_cached_data(cache_key)

        if cached is not None:
            self._cache_hits += 1
            logger.info(f"  ⚡ Cache hit: {data_type} ({file_path.name})")
            return cached

        self._cache_misses += 1
        logger.info(f"  📁 Loading fresh: {data_type} ({file_path.name})")

        suffix = file_path.suffix.lower()

        # Load CSV
        if suffix == ".csv":
            df = pd.read_csv(file_path)
            df.columns = df.columns.str.lower()
            df["timestamp"] = pd.to_datetime(df["timestamp"], format=self.DATE_FORMAT)
            df = df.set_index("timestamp").sort_index()

        # Load Parquet
        elif suffix == ".parquet":
            df = pd.read_parquet(file_path)
            df.columns = df.columns.str.lower()

            # Case 1: timestamp is stored as index
            if df.index.name == "timestamp":
                df = df.sort_index()

                # Remove timezone if present
                if df.index.tz is not None:
                    df.index = df.index.tz_localize(None)

                # Remove microseconds
                df.index = df.index.floor("s")

                # Drop duplicates
                if df.index.duplicated().any():
                    dup_count = df.index.duplicated().sum()
                    logger.warning(f"  ⚠️ Found {dup_count} duplicate timestamps in {file_path.name}, keeping last")
                    df = df[~df.index.duplicated(keep="last")]

            # Case 2: timestamp is stored as a column
            elif "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
                df = df.dropna(subset=["timestamp"])
                df["timestamp"] = df["timestamp"].dt.floor("S")
                df = df.set_index("timestamp").sort_index()
                
                if df.index.duplicated().any():
                    dup_count = df.index.duplicated().sum()
                    logger.warning(f"  ⚠️ Found {dup_count} duplicate timestamps, keeping last")
                    df = df[~df.index.duplicated(keep="last")]

            else:
                raise ValueError(
                    f"Parquet file {file_path} has neither a timestamp column nor a timestamp index"
                )

        else:
            raise ValueError(f"Unsupported file format: {suffix}. Must be .csv or .parquet")

        df = df.copy()

        # Date range slicing
        if date_range:
            start = date_range.start
            end = date_range.end
            if start or end:
                df = df.loc[start:end]

        # Save to cache
        self._save_to_cache(cache_key, df)
        return df

    # =============================================================================
    # DATA SANITIZATION (REUSED FROM ORIGINAL)
    # =============================================================================

    def _sanitize_df(self, df: pd.DataFrame, name: str) -> pd.DataFrame:
        """
        Sanitize DataFrame: inf → nan → ffill → bfill.
        
        Args:
            df: DataFrame to sanitize
            name: Name for logging
            
        Returns:
            Sanitized DataFrame
        """
        # Check for inf values
        inf_count = np.isinf(df.select_dtypes(include=[np.number])).sum().sum()
        if inf_count > 0:
            logger.warning(f"  ⚠️ {name}: Found {inf_count} inf values, replacing with NaN")
        
        df = df.replace([np.inf, -np.inf], np.nan)
        
        # Check for NaN after replacement
        nan_count = df.select_dtypes(include=[np.number]).isnull().sum().sum()
        if nan_count > 0:
            logger.warning(f"  ⚠️ {name}: Found {nan_count} NaN values, forward/backward filling")
            df = df.ffill().bfill()
        
        return df

    # =============================================================================
    # VALIDATION
    # =============================================================================

    def _validate_dataframe(self, df: pd.DataFrame, name: str) -> DataValidationResult:
        """
        Validate a single DataFrame.
        
        Args:
            df: DataFrame to validate
            name: Name for error messages
            
        Returns:
            DataValidationResult
        """
        checks = {}
        errors = []
        warnings = []

        # Check: Has data
        checks["has_data"] = len(df) > 0
        if not checks["has_data"]:
            errors.append(f"{name}: DataFrame is empty")

        # Check: OHLC columns exist
        required_cols = ["open", "high", "low", "close"]
        checks["ohlc_columns"] = all(col in df.columns for col in required_cols)
        if not checks["ohlc_columns"]:
            missing = [col for col in required_cols if col not in df.columns]
            errors.append(f"{name}: Missing OHLC columns: {missing}")

        if checks["ohlc_columns"]:
            # Check: No NaN values
            checks["no_nan"] = not df[required_cols].isnull().any().any()
            if not checks["no_nan"]:
                nan_counts = df[required_cols].isnull().sum()
                warnings.append(f"{name}: NaN values found: {nan_counts[nan_counts > 0].to_dict()}")

            # Check: Positive prices
            checks["positive_prices"] = (df[required_cols] > 0).all().all()
            if not checks["positive_prices"]:
                errors.append(f"{name}: Found non-positive prices")

            # Check: High >= Low
            checks["high_low_valid"] = (df["high"] >= df["low"]).all()
            if not checks["high_low_valid"]:
                errors.append(f"{name}: Found bars where high < low")

            # Check: Open/Close within High/Low
            checks["open_close_valid"] = (
                (df["open"] >= df["low"]) &
                (df["open"] <= df["high"]) &
                (df["close"] >= df["low"]) &
                (df["close"] <= df["high"])
            ).all()
            if not checks["open_close_valid"]:
                errors.append(f"{name}: Found bars where open/close outside high/low range")

        is_valid = len(errors) == 0
        return DataValidationResult(
            is_valid=is_valid,
            checks=checks,
            errors=errors,
            warnings=warnings
        )

    # =============================================================================
    # MAIN DATA LOADING
    # =============================================================================

    def load_data(self) -> DataBundle:
        """
        Load all data files and return a typed DataBundle.
        
        Returns:
            DataBundle: Complete data package with validation
            
        Raises:
            ValueError: If data is invalid
            FileNotFoundError: If required files are missing
        """
        # Ensure config is loaded
        if self.data_config is None:
            self.load_config()

        logger.info("Loading data files...")

        # Load strategy data (required)
        df_full = self._load_file_with_cache(
            self.data_config.strategy_data.path,
            "strategy",
            None  # Load full file first
        )
        df_full = self._sanitize_df(df_full, "strategy_full")

        # Slice to strategy period
        if self.data_config.date_range:
            start = self.data_config.date_range.start
            end = self.data_config.date_range.end
            df_strategy = df_full.loc[start:end].copy()
        else:
            df_strategy = df_full.copy()
        
        df_strategy = self._sanitize_df(df_strategy, "strategy_period")

        # Load HTF data (optional)
        df_htf = None
        if self.data_config.htf_data:
            df_htf = self._load_file_with_cache(
                self.data_config.htf_data.path,
                "htf",
                self.data_config.date_range
            )
            df_htf = self._sanitize_df(df_htf, "htf")

        # Load LTF data (optional)
        df_ltf = None
        ltf_timeframe = "1s"
        if self.data_config.ltf_data:
            df_ltf = self._load_file_with_cache(
                self.data_config.ltf_data.path,
                "ltf",
                self.data_config.date_range
            )
            df_ltf = self._sanitize_df(df_ltf, "ltf")
            # Try to infer LTF timeframe from config
            if self.raw_config and "data" in self.raw_config:
                ltf_timeframe = self.raw_config["data"].get("ltf_timeframe", "1s")

        # Validate strategy data
        validation = self._validate_dataframe(df_strategy, "strategy")
        
        if not validation.is_valid:
            error_msg = "\n".join(validation.errors)
            raise ValueError(f"Data validation failed:\n{error_msg}")

        # Log warnings
        for warning in validation.warnings:
            logger.warning(f"  {warning}")

        # Create metadata
        info = DataInfo(
            total_bars=len(df_full),
            strategy_bars=len(df_strategy),
            htf_bars=len(df_htf) if df_htf is not None else 0,
            ltf_bars=len(df_ltf) if df_ltf is not None else 0,
            date_range=(
                df_strategy.index.min().to_pydatetime(),
                df_strategy.index.max().to_pydatetime()
            ) if not df_strategy.empty else None,
            ltf_timeframe=ltf_timeframe,
            cache_hit=(self._cache_hits > 0)
        )

        # Create DataBundle
        bundle = DataBundle(
            full=df_full,
            strategy=df_strategy,
            htf=df_htf,
            ltf=df_ltf,
            info=info,
            validation=validation,
            config=self.data_config
        )

        logger.info(f"  ✅ Data loaded successfully")
        logger.info(f"     {info}")

        return bundle

    # =============================================================================
    # CACHE STATISTICS
    # =============================================================================

    @property
    def cache_stats(self) -> CacheStats:
        """
        Get cache performance statistics.
        
        Returns:
            CacheStats object
        """
        total = self._cache_hits + self._cache_misses
        hit_rate = (self._cache_hits / total * 100) if total > 0 else 0.0

        cache_files = list(self.cache_dir.glob("*.pkl"))
        total_size = sum(f.stat().st_size for f in cache_files)

        return CacheStats(
            hits=self._cache_hits,
            misses=self._cache_misses,
            hit_rate=hit_rate,
            total_files=len(cache_files),
            total_size_mb=total_size / (1024 * 1024),
            cache_dir=str(self.cache_dir)
        )
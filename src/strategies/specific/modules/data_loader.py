"""
DataLoader v3.1 - Block 1 Production Hardening

Version: 3.1.0
Session: Block 1 — Production Hardening

Changes from v3.0.0:
- [C3] _build_data_config: guards cfg.date_range before attribute access — no
       AttributeError when date_range is None (YAML `date_range: null`)
- [M2] _load_file_with_cache: raises ValueError immediately when a loaded file
       produces an empty DataFrame — eliminates silent failures from timestamp
       parsing errors or fully out-of-range slices
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional
import hashlib
import pickle
import logging

from src.strategies.contracts.data_contracts import (
    DataConfig,
    DataBundle,
    DataInfo,
    DataValidationResult,
    CacheStats,
    DateRange,
    DataFileConfig,
)
from src.config.config_schema import StrategyConfig

# Fallback for PROJECT_ROOT
try:
    from src.utils.paths import PROJECT_ROOT
except ImportError:
    PROJECT_ROOT = Path(__file__).resolve().parents[3]

logger = logging.getLogger(__name__)


class DataLoader:
    """
    DataLoader v3.1 - Fully migrated, trusts StrategyConfig.

    Features:
    - Accepts StrategyConfig directly (DEC-033)
    - No config loading or validation - trusts the typed config
    - Returns DataBundle with typed contracts
    - Supports CSV + Parquet (Parquet optimized)
    - Monthly/ARTF data support
    - Dual-mode execution (core vs analytics)
    - Intelligent caching with MD5 validation
    - Fail-fast on empty DataFrames (M2)

    Performance:
    - Parquet: ~40ms (cold), ~5ms (cache) - 60-70% faster than v2.0
    - CSV: ~200ms (cold), ~5ms (cache)
    - Additional 8-15% speedup from optimizations
    - Core mode: 3-5% faster sanitization
    """

    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(
        self,
        config: StrategyConfig,
        mode: str = "core"
    ):
        """
        Initialize DataLoader with StrategyConfig.

        Args:
            config: StrategyConfig instance (fully validated)
            mode: Execution mode ("core" or "analytics")
        """
        self.config = config
        self.strategy_config = config
        self.mode = mode

        # Cache management
        self.cache_dir = Path.home() / ".wbws_data_cache"
        self.cache_dir.mkdir(exist_ok=True)

        # Cache statistics (only collected in analytics mode)
        self._cache_hits = 0
        self._cache_misses = 0

        # Mode-aware logging
        self._verbose = (mode == "analytics")

        # Build DataConfig from StrategyConfig
        self.data_config = self._build_data_config()

        if self._verbose:
            logger.info(f"DataLoader initialized (mode={mode})")
            logger.info(f"  Strategy data: {self.data_config.strategy_data.path.name}")
            if self.data_config.htf_data:
                logger.info(f"  HTF data: {self.data_config.htf_data.path.name}")
            if self.data_config.ltf_data:
                logger.info(f"  LTF data: {self.data_config.ltf_data.path.name}")
            if self.data_config.artf_data:
                logger.info(f"  ARTF data: {self.data_config.artf_data.path.name}")
            if self.data_config.date_range:
                logger.info(f"  Date range: {self.data_config.date_range}")

    def _log(self, level: str, message: str):
        """Mode-aware logging."""
        if self._verbose:
            if level == "info":
                logger.info(message)
            elif level == "warning":
                logger.warning(message)
            elif level == "error":
                logger.error(message)

    # =============================================================================
    # Build DataConfig from StrategyConfig (Phase 5.1)
    # =============================================================================

    def _build_data_config(self) -> DataConfig:
        """Build DataConfig from the typed StrategyConfig — no YAML re-parse."""
        cfg = self.strategy_config.data

        def _build_file_config(path: Optional[Path]) -> Optional[DataFileConfig]:
            if path is None:
                return None
            return DataFileConfig(
                path=path,
                format=path.suffix.lower().lstrip("."),
            )

        # [C3] cfg.date_range is Optional[DateRangeConfig] — guard before access.
        # When YAML has `date_range: null`, cfg.date_range is None and we produce
        # no DateRange, meaning the full file is loaded without slicing.
        date_range = None
        if cfg.date_range is not None and cfg.date_range.start and cfg.date_range.end:
            date_range = DateRange(
                start=pd.Timestamp(cfg.date_range.start).to_pydatetime(),
                end=pd.Timestamp(cfg.date_range.end).to_pydatetime(),
            )

        return DataConfig(
            strategy_data=_build_file_config(cfg.paths.strategy_ohlcv),
            htf_data=_build_file_config(cfg.paths.htf_ohlcv),
            ltf_data=_build_file_config(cfg.paths.ltf_ohlcv),
            artf_data=_build_file_config(cfg.paths.artf_ohlcv),
            date_range=date_range,
        )

    # =============================================================================
    # CACHE MANAGEMENT
    # =============================================================================

    def _get_cache_key(
        self,
        file_path: Path,
        date_range: Optional[DateRange] = None,
        use_content_hash: bool = False
    ) -> Optional[str]:
        """Generate cache key for a file."""
        if not file_path.exists():
            return None

        stat = file_path.stat()
        key_parts = [
            str(file_path.resolve()),
            f"size:{stat.st_size}",
            f"mtime:{stat.st_mtime}",
            "v3.1",  # Cache version — increment on format changes
        ]

        if use_content_hash:
            try:
                with open(file_path, "rb") as f:
                    content = f.read(256 * 1024)
                key_parts.append(f"content:{hashlib.md5(content).hexdigest()}")
            except Exception as e:
                self._log("warning", f"Failed to compute content hash for {file_path.name}: {e}")

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
            self._log("warning", f"Cache corrupted, deleting {cache_file.name}: {e}")
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
            self._log("warning", f"Cache save failed for {cache_file.name}: {e}")

    # =============================================================================
    # FILE LOADING
    # =============================================================================

    def _load_file_with_cache(
        self,
        file_config: DataFileConfig,
        data_type: str,
        apply_date_range: bool = True
    ) -> pd.DataFrame:
        """
        Load a single data file with caching.

        Args:
            file_config: DataFileConfig for the file to load
            data_type: Description (e.g., "strategy", "htf", "ltf", "artf")
            apply_date_range: Whether to apply date range slicing

        Returns:
            DataFrame with DatetimeIndex (guaranteed non-empty)

        Raises:
            FileNotFoundError: If the file does not exist
            ValueError: If the loaded DataFrame is empty — fail-fast (M2)
        """
        file_path = file_config.path

        if not file_path.exists():
            raise FileNotFoundError(f"{data_type} file not found: {file_path}")

        date_range = self.data_config.date_range if apply_date_range else None

        cache_key = self._get_cache_key(file_path, date_range)
        cached = self._load_cached_data(cache_key)

        if cached is not None:
            self._cache_hits += 1
            self._log("info", f"  ⚡ Cache hit: {data_type} ({file_path.name})")
            return cached

        self._cache_misses += 1
        self._log("info", f"  📁 Loading fresh: {data_type} ({file_path.name})")

        suffix = file_path.suffix.lower()

        if suffix == ".csv":
            df = pd.read_csv(file_path)
            df.columns = df.columns.str.lower()
            df["timestamp"] = pd.to_datetime(df["timestamp"], format=self.DATE_FORMAT)
            df = df.set_index("timestamp").sort_index()

        elif suffix == ".parquet":
            df = pd.read_parquet(file_path)
            df.columns = df.columns.str.lower()

            if df.index.name == "timestamp":
                if hasattr(df.index, 'tz') and df.index.tz is not None:
                    df.index = df.index.tz_localize(None)
                df.index = df.index.floor("s")
                df = df.sort_index()

                if not df.index.is_unique:
                    dup_count = df.index.duplicated().sum()
                    self._log("warning", f"  ⚠️ Found {dup_count} duplicate timestamps in {file_path.name}, keeping last")
                    df = df[~df.index.duplicated(keep="last")]

            elif "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
                df = df.dropna(subset=["timestamp"])
                df["timestamp"] = df["timestamp"].dt.floor("s")
                df = df.set_index("timestamp").sort_index()

                if not df.index.is_unique:
                    dup_count = df.index.duplicated().sum()
                    self._log("warning", f"  ⚠️ Found {dup_count} duplicate timestamps, keeping last")
                    df = df[~df.index.duplicated(keep="last")]

            else:
                raise ValueError(
                    f"Parquet file {file_path} has neither a timestamp column "
                    f"nor a timestamp index. Verify the file was exported correctly."
                )

        else:
            raise ValueError(
                f"Unsupported file format: {suffix}. Must be .csv or .parquet"
            )

        # [M2] Fail-fast on empty result — covers timestamp parsing failures,
        # fully out-of-range slices, and corrupt files that read as zero rows.
        if df.empty:
            raise ValueError(
                f"{data_type} data loaded from '{file_path}' produced an empty DataFrame. "
                f"Possible causes: timestamp parsing failure, all rows outside the "
                f"configured date_range, or a corrupt/empty file. "
                f"Verify the file content and date_range configuration."
            )

        df = df.copy()

        # Date range slicing (skip for ARTF — we need full history)
        if apply_date_range and date_range and data_type != "artf":
            start = date_range.start
            end = date_range.end
            if start or end:
                df = df.loc[start:end]
                # [M2] Check again after slicing — range may exclude all rows
                if df.empty:
                    raise ValueError(
                        f"{data_type} data from '{file_path}' is empty after applying "
                        f"date_range [{date_range.start} → {date_range.end}]. "
                        f"Verify the date_range overlaps with the file's data period."
                    )

        self._save_to_cache(cache_key, df)
        return df

    # =============================================================================
    # DATA SANITIZATION
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
        if not self._verbose:
            df = df.replace([np.inf, -np.inf], np.nan)
            if df.isnull().values.any():
                df = df.ffill().bfill()
            return df

        inf_count = np.isinf(df.select_dtypes(include=[np.number])).sum().sum()
        if inf_count > 0:
            self._log("warning", f"  ⚠️ {name}: Found {inf_count} inf values, replacing with NaN")

        df = df.replace([np.inf, -np.inf], np.nan)

        nan_count = df.select_dtypes(include=[np.number]).isnull().sum().sum()
        if nan_count > 0:
            self._log("warning", f"  ⚠️ {name}: Found {nan_count} NaN values, forward/backward filling")
            df = df.ffill().bfill()

        return df

    # =============================================================================
    # VALIDATION
    # =============================================================================

    def _validate_dataframe(self, df: pd.DataFrame, name: str) -> DataValidationResult:
        """Validate a single DataFrame."""
        checks = {}
        errors = []
        warnings = []

        checks["has_data"] = len(df) > 0
        if not checks["has_data"]:
            errors.append(f"{name}: DataFrame is empty")

        required_cols = ["open", "high", "low", "close"]
        checks["ohlc_columns"] = all(col in df.columns for col in required_cols)
        if not checks["ohlc_columns"]:
            missing = [col for col in required_cols if col not in df.columns]
            errors.append(f"{name}: Missing OHLC columns: {missing}")

        if checks["ohlc_columns"]:
            checks["no_nan"] = not df[required_cols].isnull().any().any()
            if not checks["no_nan"]:
                nan_counts = df[required_cols].isnull().sum()
                warnings.append(f"{name}: NaN values found: {nan_counts[nan_counts > 0].to_dict()}")

            checks["positive_prices"] = (df[required_cols] > 0).all().all()
            if not checks["positive_prices"]:
                errors.append(f"{name}: Found non-positive prices")

            checks["high_low_valid"] = (df["high"] >= df["low"]).all()
            if not checks["high_low_valid"]:
                errors.append(f"{name}: Found bars where high < low")

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
            FileNotFoundError: If required files are missing
            ValueError: If data is invalid or any file produces an empty DataFrame
        """
        self._log("info", "Loading data files...")

        df_full = self._load_file_with_cache(
            self.data_config.strategy_data,
            "strategy",
            apply_date_range=False
        )
        df_full = self._sanitize_df(df_full, "strategy_full")

        if self.data_config.date_range and self.data_config.date_range.is_bounded:
            start = self.data_config.date_range.start
            end = self.data_config.date_range.end
            df_strategy = df_full.loc[start:end].copy()
            # [M2] Guard the post-slice result — distinct from the raw file check
            # above because full file is loaded without range before this point.
            if df_strategy.empty:
                raise ValueError(
                    f"Strategy data is empty after applying date_range "
                    f"[{start} → {end}]. "
                    f"Verify the date_range overlaps with the file's data period."
                )
        else:
            df_strategy = df_full.copy()

        df_strategy = self._sanitize_df(df_strategy, "strategy_period")

        df_htf = None
        if self.data_config.htf_data:
            df_htf = self._load_file_with_cache(
                self.data_config.htf_data,
                "htf",
                apply_date_range=True
            )
            df_htf = self._sanitize_df(df_htf, "htf")

        df_ltf = None
        ltf_timeframe = self.config.data.ltf_timeframe
        if self.data_config.ltf_data:
            df_ltf = self._load_file_with_cache(
                self.data_config.ltf_data,
                "ltf",
                apply_date_range=True
            )
            df_ltf = self._sanitize_df(df_ltf, "ltf")

        df_artf = None
        artf_timeframe = self.config.data.artf_timeframe
        if self.data_config.artf_data:
            df_artf = self._load_file_with_cache(
                self.data_config.artf_data,
                "artf",
                apply_date_range=False
            )
            df_artf = self._sanitize_df(df_artf, "artf")

        validation = self._validate_dataframe(df_strategy, "strategy")

        if not validation.is_valid:
            error_msg = "\n".join(validation.errors)
            raise ValueError(f"Data validation failed:\n{error_msg}")

        for warning in validation.warnings:
            self._log("warning", f"  {warning}")

        info = DataInfo(
            total_bars=len(df_full),
            strategy_bars=len(df_strategy),
            htf_bars=len(df_htf) if df_htf is not None else 0,
            ltf_bars=len(df_ltf) if df_ltf is not None else 0,
            artf_bars=len(df_artf) if df_artf is not None else 0,
            date_range=(
                df_strategy.index.min().to_pydatetime(),
                df_strategy.index.max().to_pydatetime()
            ) if not df_strategy.empty else None,
            ltf_timeframe=ltf_timeframe,
            artf_timeframe=artf_timeframe,
            cache_hit=(self._cache_hits > 0)
        )

        bundle = DataBundle(
            full=df_full,
            strategy=df_strategy,
            htf=df_htf,
            ltf=df_ltf,
            artf=df_artf,
            info=info,
            validation=validation,
            config=self.data_config
        )

        self._log("info", "  ✅ Data loaded successfully")
        if self._verbose:
            self._log("info", f"     {info}")

        return bundle

    # =============================================================================
    # CACHE STATISTICS
    # =============================================================================

    @property
    def cache_stats(self) -> Optional[CacheStats]:
        """
        Get cache performance statistics.

        Returns:
            CacheStats object (analytics mode) or None (core mode)
        """
        if not self._verbose:
            return None

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
"""
DataLoader v3.3 - LTF/HTF Slice-Before-Cache
Version: 3.3.0

B9O-006: Fix OOM on LTF (and HTF) files for full-history WFO runs.

ROOT CAUSE (confirmed from stack trace):
  _load_file_with_cache() for LTF called with apply_date_range=True was:
    1. pd.read_parquet()        → full 22.4M rows (~897 MB)
    2. df.index.floor("s")      → 22.4M int64 index copy (~171 MB)
    3. df.sort_index() → copy() → 22.4M×5 float64 body (~856 MB)
    4. self._save_to_cache(cache_key, df)  ← CACHED THE FULL 22.4M ROW DF
    5. df = df.loc[start:end]             ← sliced AFTER caching
  Peak per worker: ~1.9 GB. With max_workers=2: ~3.8 GB → OOM.
  Cache key already included date_range (correct), but cache VALUE was full file.

FIX (B9O-006): Slice BEFORE _save_to_cache(), del full df after slicing.
  Cache key unchanged — it already encodes date_range. Cache VALUE is now the slice.
  Cache version tag bumped "v3.1" → "v3.3" to invalidate old full-file cache entries.

  After fix:
    Cache miss:  full load → process → SLICE → cache slice → del full → return slice
    Cache hit:   load ~20 MB slice pkl → return
    Peak on hit: ~20 MB per worker (vs ~1.9 GB before)

RELATIONSHIP TO B9O-001 (strategy file, apply_date_range=False path):
  B9O-001 added _load_sliced_strategy_cache() because the strategy file is
  loaded full (apply_date_range=False) for DataBundle.full. That path is unchanged.
  B9O-006 fixes the apply_date_range=True path (LTF, HTF). Both are required.

V2 BACKLOG — V2-LTF-PARTITION:
  Root cause is loading the full Parquet before slicing. V2 should partition
  LTF Parquet by month so pd.read_parquet(filters=[...]) pushes date range to
  the Parquet reader, eliminating the cache-miss peak entirely.
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
from src.strategies.config.config_schema import StrategyConfig

try:
    from src.utils.paths import PROJECT_ROOT
except ImportError:
    PROJECT_ROOT = Path(__file__).resolve().parents[3]

logger = logging.getLogger(__name__)


class DataLoader:
    """
    DataLoader v3.3:
    - B9O-001: sliced strategy cache (apply_date_range=False path)
    - B9O-006: slice-before-cache for LTF/HTF (apply_date_range=True path)
    """

    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(self, config: StrategyConfig, mode: str = "core"):
        self.config = config
        self.strategy_config = config
        self.mode = mode
        self.cache_dir = Path.home() / ".wbws_data_cache"
        self.cache_dir.mkdir(exist_ok=True)
        self._cache_hits = 0
        self._cache_misses = 0
        self._verbose = (mode == "analytics")
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
        if self._verbose:
            getattr(logger, level, logger.info)(message)

    # =============================================================================
    # Build DataConfig
    # =============================================================================

    def _build_data_config(self) -> DataConfig:
        cfg = self.strategy_config.data

        def _build_file_config(path: Optional[Path]) -> Optional[DataFileConfig]:
            if path is None:
                return None
            return DataFileConfig(path=path, format=path.suffix.lower().lstrip("."))

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
        use_content_hash: bool = False,
    ) -> Optional[str]:
        if not file_path.exists():
            return None
        stat = file_path.stat()
        key_parts = [
            str(file_path.resolve()),
            f"size:{stat.st_size}",
            f"mtime:{stat.st_mtime}",
            "v3.3",  # Bumped from v3.1 — old entries store full DFs, ignore them
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
        if not cache_key:
            return
        cache_file = self.cache_dir / f"{cache_key}.pkl"
        try:
            with open(cache_file, "wb") as f:
                pickle.dump(df, f)
        except Exception as e:
            self._log("warning", f"Cache save failed for {cache_file.name}: {e}")

    # =============================================================================
    # B9O-001: Sliced strategy cache (apply_date_range=False path)
    # =============================================================================

    def _get_sliced_cache_key(self, file_path: str, date_range_str: str) -> str:
        file_path_obj = Path(file_path)
        mtime = file_path_obj.stat().st_mtime
        raw = f"sliced:v1:{file_path}:{mtime}:{date_range_str}"
        return hashlib.md5(raw.encode()).hexdigest()

    def _load_sliced_strategy_cache(self, file_path: str, date_range_str: str) -> Optional[pd.DataFrame]:
        key = self._get_sliced_cache_key(file_path, date_range_str)
        cache_file = self.cache_dir / f"{key}.pkl"
        if not cache_file.exists():
            return None
        try:
            with open(cache_file, "rb") as f:
                return pickle.load(f)
        except Exception:
            cache_file.unlink(missing_ok=True)
            return None

    def _save_sliced_strategy_cache(self, df: pd.DataFrame, file_path: str, date_range_str: str) -> None:
        key = self._get_sliced_cache_key(file_path, date_range_str)
        cache_file = self.cache_dir / f"{key}.pkl"
        try:
            with open(cache_file, "wb") as f:
                pickle.dump(df, f)
        except Exception:
            pass

    # =============================================================================
    # FILE LOADING
    # =============================================================================

    def _load_file_with_cache(
        self,
        file_config: DataFileConfig,
        data_type: str,
        apply_date_range: bool = True,
    ) -> pd.DataFrame:
        """
        Load a single data file with caching.

        B9O-006: When apply_date_range=True and a date_range is set, the cache
        stores the SLICED result (not the full file). Reorder: slice → cache → return.
        Full DataFrame is del'd immediately after slicing to release memory.

        Old v3.1 entries (storing full DFs) are automatically ignored because
        the cache version tag changed from "v3.1" to "v3.3".
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
                if hasattr(df.index, "tz") and df.index.tz is not None:
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
                    f"nor a timestamp index."
                )
        else:
            raise ValueError(f"Unsupported file format: {suffix}. Must be .csv or .parquet")

        if df.empty:
            raise ValueError(
                f"{data_type} data loaded from '{file_path}' produced an empty DataFrame."
            )

        # ── B9O-006: Slice BEFORE caching ─────────────────────────────────────
        # Previously: copy → save_to_cache(full_df) → slice (v3.1 bug)
        # Now:        slice → save_to_cache(slice) → del full_df → return slice
        #
        # artf is excluded: it always requires full history for rolling indicators,
        # even if apply_date_range=True is accidentally passed.
        if apply_date_range and date_range and data_type != "artf":
            start = date_range.start
            end = date_range.end
            if start or end:
                df_sliced = df.loc[start:end].copy()
                del df  # Release full DataFrame immediately — do not hold in memory
                if df_sliced.empty:
                    raise ValueError(
                        f"{data_type} data from '{file_path}' is empty after applying "
                        f"date_range [{date_range.start} → {date_range.end}]. "
                        f"Verify the date_range overlaps with the file's data period."
                    )
                self._save_to_cache(cache_key, df_sliced)  # Cache the slice (small)
                return df_sliced

        # No-slice path: apply_date_range=False, artf, or no date_range set.
        # Cache the full DataFrame as before.
        df = df.copy()
        self._save_to_cache(cache_key, df)
        return df

    # =============================================================================
    # DATA SANITIZATION
    # =============================================================================

    def _sanitize_df(self, df: pd.DataFrame, name: str) -> pd.DataFrame:
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
        return DataValidationResult(is_valid=is_valid, checks=checks, errors=errors, warnings=warnings)

    # =============================================================================
    # MAIN DATA LOADING
    # =============================================================================

    def load_data(self) -> DataBundle:
        """
        Load all data files and return a typed DataBundle.

        Strategy: loaded full (apply_date_range=False) for DataBundle.full, then
        sliced for DataBundle.strategy. B9O-001 caches the slice separately.

        LTF/HTF: apply_date_range=True. B9O-006 caches the slice (not full file).
        """
        self._log("info", "Loading data files...")

        # ── Strategy file (B9O-001 path) ──────────────────────────────────────
        df_strategy = None
        if self.data_config.date_range and self.data_config.date_range.is_bounded:
            dr = self.data_config.date_range
            date_range_str = f"{dr.start.isoformat()}_{dr.end.isoformat()}"
            str_path = str(self.data_config.strategy_data.path.resolve())
            df_strategy = self._load_sliced_strategy_cache(str_path, date_range_str)
            if df_strategy is not None:
                self._log("info", "  ⚡ Cache hit: strategy slice (B9O-001)")

        # Always load df_full — required for DataBundle.full (TradeSimulator needs it)
        df_full = self._load_file_with_cache(
            self.data_config.strategy_data,
            "strategy",
            apply_date_range=False,
        )
        df_full = self._sanitize_df(df_full, "strategy_full")

        if df_strategy is None:
            if self.data_config.date_range and self.data_config.date_range.is_bounded:
                start = self.data_config.date_range.start
                end = self.data_config.date_range.end
                df_strategy = df_full.loc[start:end].copy()
                if df_strategy.empty:
                    raise ValueError(
                        f"Strategy data is empty after applying date_range [{start} → {end}]."
                    )
                dr = self.data_config.date_range
                date_range_str = f"{dr.start.isoformat()}_{dr.end.isoformat()}"
                str_path = str(self.data_config.strategy_data.path.resolve())
                self._save_sliced_strategy_cache(df_strategy, str_path, date_range_str)
            else:
                df_strategy = df_full.copy()

        df_strategy = self._sanitize_df(df_strategy, "strategy_period")

        # ── HTF file (B9O-006 path) ───────────────────────────────────────────
        df_htf = None
        if self.data_config.htf_data:
            df_htf = self._load_file_with_cache(self.data_config.htf_data, "htf", apply_date_range=True)
            df_htf = self._sanitize_df(df_htf, "htf")

        # ── LTF file (B9O-006 path) ───────────────────────────────────────────
        df_ltf = None
        ltf_timeframe = self.config.data.ltf_timeframe
        if self.data_config.ltf_data:
            df_ltf = self._load_file_with_cache(self.data_config.ltf_data, "ltf", apply_date_range=True)
            df_ltf = self._sanitize_df(df_ltf, "ltf")

        # [GUARD-2] LTF coverage check — unchanged
        if df_ltf is not None and self.data_config.date_range:
            dr = self.data_config.date_range
            ltf_start = df_ltf.index[0]
            ltf_end   = df_ltf.index[-1]
            dr_start  = pd.Timestamp(dr.start)
            dr_end    = pd.Timestamp(dr.end)
            head_gap  = ltf_start > dr_start
            tail_gap  = ltf_end   < dr_end
            if head_gap or tail_gap:
                gaps = []
                if head_gap:
                    gaps.append(f"head gap [{dr_start} → {ltf_start}] ({(ltf_start - dr_start).days}d uncovered at start)")
                if tail_gap:
                    gaps.append(f"tail gap [{ltf_end} → {dr_end}] ({(dr_end - ltf_end).days}d uncovered at end)")
                logger.info(
                    "LTF file does not fully cover the strategy date_range — %s. "
                    "LTF file: [%s → %s]. Strategy window: [%s → %s]. "
                    "Trades in uncovered bars will close at end-of-data price.",
                    " | ".join(gaps), ltf_start, ltf_end, dr_start, dr_end,
                )

        # ── ARTF file ─────────────────────────────────────────────────────────
        df_artf = None
        artf_timeframe = self.config.data.artf_timeframe
        if self.data_config.artf_data:
            df_artf = self._load_file_with_cache(self.data_config.artf_data, "artf", apply_date_range=False)
            df_artf = self._sanitize_df(df_artf, "artf")

        # ── Validation ────────────────────────────────────────────────────────
        validation = self._validate_dataframe(df_strategy, "strategy")
        if not validation.is_valid:
            raise ValueError(f"Data validation failed:\n" + "\n".join(validation.errors))
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
                df_strategy.index.max().to_pydatetime(),
            ) if not df_strategy.empty else None,
            ltf_timeframe=ltf_timeframe,
            artf_timeframe=artf_timeframe,
            cache_hit=(self._cache_hits > 0),
        )

        bundle = DataBundle(
            full=df_full,
            strategy=df_strategy,
            htf=df_htf,
            ltf=df_ltf,
            artf=df_artf,
            info=info,
            validation=validation,
            config=self.data_config,
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
            cache_dir=str(self.cache_dir),
        )
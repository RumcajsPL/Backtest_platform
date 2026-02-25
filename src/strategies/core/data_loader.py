"""
DataLoader v2.4 - Fixed ARTF loading
Changes:
- ARTF data now loaded WITHOUT date slicing (needs full history for annual range)
- Added explicit parameter to control date slicing per file type
- Better logging to show when ARTF is loaded with full history
"""

import pandas as pd
import yaml
import numpy as np
from pathlib import Path
from typing import Dict, Tuple, Optional
import hashlib
import pickle
import logging

from src.utils.paths import PROJECT_ROOT

logger = logging.getLogger(__name__)


class DataLoader:
    """
    Modernized DataLoader v2.4:
    - Supports CSV + Parquet
    - Unified loader with caching
    - Sanitization (inf → nan → ffill/bfill)
    - Fast timestamp parsing
    - Downcasting for memory efficiency
    - ARTF loaded with full history (no date slicing)
    """

    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(self, config_path: str):
        self.config_path = Path(config_path).resolve()
        self.config = None

        self.df_full = None
        self.df_strategy = None
        self.df_htf = None
        self.df_ltf = None
        self.df_artf = None

        self.cache_dir = Path.home() / ".wbws_data_cache"
        self.cache_dir.mkdir(exist_ok=True)

        self.cache_hits = 0
        self.cache_misses = 0

    # ---------------------------------------------------------
    # CONFIG LOADING
    # ---------------------------------------------------------
    def load_config(self) -> Dict:
        with open(self.config_path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)
        return self.config

    # ---------------------------------------------------------
    # STRICT DATE VALIDATION
    # ---------------------------------------------------------
    def _validate_date_format(self, date_str: str, date_type: str):
        if not date_str:
            return
        try:
            pd.to_datetime(date_str, format=self.DATE_FORMAT)
        except Exception:
            raise ValueError(
                f"{date_type} '{date_str}' must match format '{self.DATE_FORMAT}'. "
                f"Execution aborted. Check config: {self.config_path}"
            )

    # ---------------------------------------------------------
    # CACHE HELPERS
    # ---------------------------------------------------------
    def _get_cache_key(self, file_path: Path, start_date=None, end_date=None, include_dates=True) -> Optional[str]:
        """
        Generate cache key with optional date inclusion.
        
        Args:
            file_path: Path to data file
            start_date: Optional start date for slicing
            end_date: Optional end date for slicing
            include_dates: Whether to include dates in cache key (False for ARTF)
        """
        if not file_path.exists():
            return None

        stat = file_path.stat()
        key_parts = [
            str(file_path.resolve()),
            f"size:{stat.st_size}",
            f"mtime:{stat.st_mtime}",
        ]

        try:
            with open(file_path, "rb") as f:
                content = f.read(256 * 1024)
            key_parts.append(f"content:{hashlib.md5(content).hexdigest()}")
        except Exception as e:
            logger.warning(f"Failed to compute content hash for {file_path.name}: {e}")

        # Only include dates if requested (for ARTF, we exclude them)
        if include_dates:
            if start_date:
                start_norm = pd.to_datetime(start_date, format=self.DATE_FORMAT)
                key_parts.append(f"start:{start_norm.isoformat()}")

            if end_date:
                end_norm = pd.to_datetime(end_date, format=self.DATE_FORMAT)
                key_parts.append(f"end:{end_norm.isoformat()}")

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
            logger.warning(f"Cache corrupted, deleting {cache_file.name}: {e}")
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
            logger.warning(f"Cache save failed for {cache_file.name}: {e}")

    # ---------------------------------------------------------
    # UNIFIED CSV/PARQUET LOADER WITH CACHE
    # ---------------------------------------------------------
    def _load_file_with_cache(
        self, 
        file_path: Path, 
        data_type: str, 
        start_date=None, 
        end_date=None,
        apply_date_slice: bool = True  # New parameter to control date slicing
    ) -> pd.DataFrame:
        """
        Load file with optional date slicing.
        
        Args:
            file_path: Path to data file
            data_type: Type of data (for logging)
            start_date: Start date for slicing
            end_date: End date for slicing
            apply_date_slice: If True, apply date slicing; if False, load full file
        """
        self._validate_date_format(start_date, "start_date")
        self._validate_date_format(end_date, "end_date")

        # Only include dates in cache key if we're actually using them
        include_dates_in_key = apply_date_slice and (start_date or end_date)
        
        cache_key = self._get_cache_key(
            file_path, 
            start_date if apply_date_slice else None,
            end_date if apply_date_slice else None,
            include_dates=include_dates_in_key
        )
        
        cached = self._load_cached_data(cache_key)

        if cached is not None:
            self.cache_hits += 1
            logger.info(f"Cache hit for {data_type}: {file_path.name}")
            return cached

        self.cache_misses += 1
        slice_info = f" (sliced {start_date} to {end_date})" if apply_date_slice and (start_date or end_date) else " (full file)"
        logger.info(f"Loading fresh {data_type}: {file_path.name}{slice_info}")

        suffix = file_path.suffix.lower()

        # -----------------------------
        # CSV
        # -----------------------------
        if suffix == ".csv":
            df = pd.read_csv(file_path)
            df.columns = df.columns.str.lower()
            df["timestamp"] = pd.to_datetime(df["timestamp"], format=self.DATE_FORMAT)
            df = df.set_index("timestamp").sort_index()

        # -----------------------------
        # PARQUET
        # -----------------------------
        elif suffix == ".parquet":
            df = pd.read_parquet(file_path)
            df.columns = df.columns.str.lower()

            # Case 1: timestamp is stored as index (correct Parquet behavior)
            if df.index.name == "timestamp":
                df = df.sort_index()

                # Remove timezone if present
                if df.index.tz is not None:
                    df.index = df.index.tz_localize(None)

                # Remove microseconds
                df.index = df.index.floor("s")

                # Drop duplicates
                df = df[~df.index.duplicated(keep="last")]

            # Case 2: timestamp is stored as a column (rare)
            elif "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
                df = df.dropna(subset=["timestamp"])
                df["timestamp"] = df["timestamp"].dt.floor("S")
                df = df.set_index("timestamp").sort_index()
                df = df[~df.index.duplicated(keep="last")]

            else:
                raise ValueError(
                    f"Parquet file {file_path} has neither a timestamp column nor a timestamp index"
                )

        df = df.copy()

        # Date slicing - only if requested
        if apply_date_slice and (start_date or end_date):
            start = pd.to_datetime(start_date, format=self.DATE_FORMAT) if start_date else None
            end = pd.to_datetime(end_date, format=self.DATE_FORMAT) if end_date else None
            df = df.loc[start:end]
            logger.info(f"  Sliced {data_type} to {len(df)} rows ({start_date} to {end_date})")

        self._save_to_cache(cache_key, df)
        return df

    # ---------------------------------------------------------
    # SANITIZATION
    # ---------------------------------------------------------
    def _sanitize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.ffill().bfill()
        return df

    # ---------------------------------------------------------
    # MAIN DATA LOADING
    # ---------------------------------------------------------
    def load_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        data_cfg = self.config.get("data", {})

        dr = data_cfg.get("date_range", {})
        start_date = dr.get("start")
        end_date = dr.get("end")

        self._validate_date_format(start_date, "start_date")
        self._validate_date_format(end_date, "end_date")

        logger.info(f"Date range for strategy: {start_date} to {end_date}")

        # Resolve main file
        data_file = Path(data_cfg["file"])
        if not data_file.is_absolute():
            data_file = PROJECT_ROOT / data_file

        # FULL DATA (always load full file)
        logger.info("Loading full dataset (no date slicing)...")
        self.df_full = self._load_file_with_cache(data_file, "full", apply_date_slice=False)
        self.df_full = self._sanitize_df(self.df_full)
        logger.info(f"Full dataset: {len(self.df_full):,} bars")

        # STRATEGY SLICE (apply date range)
        if start_date or end_date:
            start = pd.to_datetime(start_date, format=self.DATE_FORMAT) if start_date else None
            end = pd.to_datetime(end_date, format=self.DATE_FORMAT) if end_date else None
            self.df_strategy = self.df_full.loc[start:end].copy()
            logger.info(f"Strategy slice: {len(self.df_strategy):,} bars ({start_date} to {end_date})")
        else:
            self.df_strategy = self.df_full.copy()
            logger.info(f"Strategy slice: full dataset ({len(self.df_strategy):,} bars)")

        self.df_strategy = self._sanitize_df(self.df_strategy)

        # HTF (apply date range)
        self.df_htf = None
        if "file_htf" in data_cfg:
            htf_file = Path(data_cfg["file_htf"])
            if not htf_file.is_absolute():
                htf_file = PROJECT_ROOT / htf_file

            self.df_htf = self._load_file_with_cache(
                htf_file, "htf", start_date, end_date, apply_date_slice=True
            )
            self.df_htf = self._sanitize_df(self.df_htf)
            logger.info(f"HTF data: {len(self.df_htf):,} bars")

        # LTF (apply date range)
        self.df_ltf = None
        if "file_ltf" in data_cfg:
            ltf_file = Path(data_cfg["file_ltf"])
            if not ltf_file.is_absolute():
                ltf_file = PROJECT_ROOT / ltf_file

            self.df_ltf = self._load_file_with_cache(
                ltf_file, "ltf", start_date, end_date, apply_date_slice=True
            )
            self.df_ltf = self._sanitize_df(self.df_ltf)
            logger.info(f"LTF data: {len(self.df_ltf):,} bars")
        
        # ARTF (monthly bars) - CRITICAL: NO DATE SLICING!
        self.df_artf = None
        if "file_artf" in data_cfg:
            artf_file = Path(data_cfg["file_artf"])
            if not artf_file.is_absolute():
                artf_file = PROJECT_ROOT / artf_file

            logger.info("Loading ARTF data with FULL HISTORY (no date slicing) for annual range calculation...")
            self.df_artf = self._load_file_with_cache(
                artf_file, "artf", apply_date_slice=False  # ← CRITICAL: No date slicing!
            )
            self.df_artf = self._sanitize_df(self.df_artf)
            logger.info(f"ARTF data: {len(self.df_artf):,} monthly bars from {self.df_artf.index.min()} to {self.df_artf.index.max()}")

        return self.df_full, self.df_strategy, self.df_htf, self.df_ltf, self.df_artf

    # ---------------------------------------------------------
    # INFO + VALIDATION
    # ---------------------------------------------------------
    def get_data_info(self) -> Dict:
        info = {
            "full_bars": len(self.df_full) if self.df_full is not None else 0,
            "strategy_bars": len(self.df_strategy) if self.df_strategy is not None else 0,
            "htf_bars": len(self.df_htf) if self.df_htf is not None else 0,
            "ltf_bars": len(self.df_ltf) if self.df_ltf is not None else 0,
            "artf_bars": len(self.df_artf) if self.df_artf is not None else 0,
            "date_range": (
                self.df_strategy.index.min().strftime(self.DATE_FORMAT)
                if self.df_strategy is not None and not self.df_strategy.empty else None,
                self.df_strategy.index.max().strftime(self.DATE_FORMAT)
                if self.df_strategy is not None and not self.df_strategy.empty else None,
            ),
        }

        if self.df_ltf is not None:
            info["ltf_tf"] = self.config.get("data", {}).get("ltf_timeframe", "1s")
        
        if self.df_artf is not None:
            info["artf_tf"] = self.config.get("data", {}).get("artf_timeframe", "1ME")

        return info

    def validate_data(self) -> Dict:
        df = self.df_strategy
        if df is None:
            return {"is_valid": False, "reason": "No strategy data loaded"}

        validation = {
            "has_data": len(df) > 0,
            "ohlc_columns": all(col in df.columns for col in ["open", "high", "low", "close"]),
            "no_nan": not df[["open", "high", "low", "close"]].isnull().any().any(),
            "positive_prices": (df[["open", "high", "low", "close"]] > 0).all().all(),
            "high_low_valid": (df["high"] >= df["low"]).all(),
            "open_close_valid": (
                (df["open"] >= df["low"]) &
                (df["open"] <= df["high"]) &
                (df["close"] >= df["low"]) &
                (df["close"] <= df["high"])
            ).all(),
        }

        validation["is_valid"] = all(validation.values())
        return validation

    # ---------------------------------------------------------
    # CACHE STATS
    # ---------------------------------------------------------
    def get_cache_stats(self) -> Dict:
        total = self.cache_hits + self.cache_misses
        hit_rate = (self.cache_hits / total * 100) if total > 0 else 0

        cache_files = list(self.cache_dir.glob("*.pkl"))
        total_size = sum(f.stat().st_size for f in cache_files)

        return {
            "hits": self.cache_hits,
            "misses": self.cache_misses,
            "hit_rate": f"{hit_rate:.1f}%",
            "cache_files": len(cache_files),
            "cache_size_mb": total_size / (1024 * 1024),
            "cache_dir": str(self.cache_dir),
        }
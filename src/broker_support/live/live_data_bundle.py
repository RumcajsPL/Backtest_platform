"""
LiveDataBundle — builds a DataBundle from live-fetched DataFrames.

Bypasses DataLoader entirely. DataLoader is coupled to parquet files and
the WFO/backtesting cache infrastructure — none of that is relevant in
live paper trading context.

This module produces a DataBundle with the exact same contract as DataLoader,
so all downstream pipeline stages (SignalGenerator, FilterPipeline,
RiskManager) receive the same typed contract they always expect.

What is populated:
  bundle.full      = df_strategy  (full window = strategy window in live context)
  bundle.strategy  = df_strategy  (same ref — no WFO slicing needed)
  bundle.htf       = df_htf
  bundle.ltf       = None         (LTF not fetched live)
  bundle.artf      = df_artf      (loaded from parquet — needed by RiskManager RAR)
  bundle.info      = DataInfo     (bar counts, date range)
  bundle.validation = DataValidationResult (basic OHLC checks)
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
from loguru import logger

from src.strategies.contracts.data_contracts import (
    DataBundle,
    DataInfo,
    DataValidationResult,
)


class LiveDataBundleError(Exception):
    """Raised when live DataBundle construction fails."""


def build_live_data_bundle(
    df_strategy: pd.DataFrame,
    df_htf: pd.DataFrame,
    artf_ohlcv_path: Path,
) -> DataBundle:
    """
    Build a DataBundle from live-fetched DataFrames + artf parquet.

    Args:
        df_strategy:     Live 1-min OHLCV bars. DatetimeIndex, sorted asc.
        df_htf:          Live 1H OHLCV bars. DatetimeIndex, sorted asc.
        artf_ohlcv_path: Path to monthly ARTF parquet (existing historical file).

    Returns:
        DataBundle ready for SignalGenerator → FilterPipeline → RiskManager.

    Raises:
        LiveDataBundleError: if any input is empty or malformed.
    """
    _validate_ohlcv_df(df_strategy, "strategy")
    _validate_ohlcv_df(df_htf, "htf")

    # Load artf from parquet
    df_artf = _load_artf(artf_ohlcv_path)

    # In live context: full = strategy (no WFO warmup needed — we fetched
    # enough bars for ATR warmup via strategy_bars_to_fetch)
    df_full = df_strategy

    date_range = (
        df_strategy.index.min().to_pydatetime(),
        df_strategy.index.max().to_pydatetime(),
    )

    info = DataInfo(
        total_bars=len(df_full),
        strategy_bars=len(df_strategy),
        htf_bars=len(df_htf),
        ltf_bars=0,
        artf_bars=len(df_artf) if df_artf is not None else 0,
        date_range=date_range,
        ltf_timeframe="1s",    # DataInfo.ltf_timeframe is str, not Optional — default value
        artf_timeframe="1ME",
        cache_hit=False,
    )

    validation = _validate_bundle_df(df_strategy)

    # DataBundle.config is Optional[DataConfig] = None.
    # DataConfig is not needed in live context — DataLoader is bypassed.
    # DataFileConfig.__post_init__ validates that file extension matches format,
    # which would require careful path management for a sentinel.
    # Passing None is correct and simpler.
    data_config = None

    bundle = DataBundle(
        full=df_full,
        strategy=df_strategy,
        htf=df_htf,
        ltf=None,
        artf=df_artf,
        info=info,
        validation=validation,
        config=data_config,
    )

    logger.info(
        f"LiveDataBundle built: "
        f"strategy={len(df_strategy)} bars "
        f"[{df_strategy.index[0]} → {df_strategy.index[-1]}], "
        f"htf={len(df_htf)} bars, "
        f"artf={len(df_artf) if df_artf is not None else 0} bars"
    )
    return bundle


def _validate_ohlcv_df(df: pd.DataFrame, label: str) -> None:
    """Fail fast on empty or missing-column DataFrames."""
    if df is None or df.empty:
        raise LiveDataBundleError(
            f"{label} DataFrame is None or empty. "
            f"LiveDataFetcher may have returned no data."
        )
    required = {"open", "high", "low", "close"}
    missing = required - set(df.columns)
    if missing:
        raise LiveDataBundleError(
            f"{label} DataFrame is missing required columns: {missing}. "
            f"Got: {list(df.columns)}"
        )
    if not isinstance(df.index, pd.DatetimeIndex):
        raise LiveDataBundleError(
            f"{label} DataFrame must have a DatetimeIndex. "
            f"Got: {type(df.index).__name__}"
        )


def _load_artf(path: Path) -> pd.DataFrame:
    """Load monthly ARTF parquet. Fail fast if missing."""
    if not path.exists():
        raise LiveDataBundleError(
            f"ARTF parquet not found: {path}. "
            f"Verify live_data.artf_ohlcv_path in broker_support_config.yaml."
        )
    df = pd.read_parquet(path)
    df.columns = df.columns.str.lower()
    if df.index.name == "timestamp":
        if hasattr(df.index, "tz") and df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        df.index = df.index.floor("s")
    df = df.sort_index()
    logger.debug(f"ARTF loaded: {len(df)} bars from {path.name}")
    return df


def _validate_bundle_df(df: pd.DataFrame) -> DataValidationResult:
    """Basic OHLC validation — mirrors DataLoader._validate_dataframe()."""
    checks: dict = {}
    errors = []
    warnings = []

    checks["has_data"] = len(df) > 0
    required_cols = ["open", "high", "low", "close"]
    checks["ohlc_columns"] = all(col in df.columns for col in required_cols)

    if checks["ohlc_columns"] and checks["has_data"]:
        checks["no_nan"]         = not df[required_cols].isnull().any().any()
        checks["positive_prices"] = (df[required_cols] > 0).all().all()
        checks["high_low_valid"]  = (df["high"] >= df["low"]).all()

        if not checks["positive_prices"]:
            errors.append("Found non-positive prices in live strategy data")
        if not checks["high_low_valid"]:
            errors.append("Found bars where high < low in live strategy data")

    is_valid = len(errors) == 0
    return DataValidationResult(
        is_valid=is_valid,
        checks=checks,
        errors=errors,
        warnings=warnings,
    )

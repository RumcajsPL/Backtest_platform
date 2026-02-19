"""
Filter Pipeline - Orchestrator for Signal Filtering

Coordinates time and technical filters with indicator caching and early exit.
Migrated from dict-based to typed contract architecture.

Author: Migration Project
Version: 2.1.0
Date: 2026-02-19
Session: 20 Block D

Changes from v2.0.0:
- P0-CH3-2: All logger.info() calls gated on self._mode == "analytics"
             logger.error/warning remain ungated (always surfaced)
- Fixed broken final log: was `logger.info(...) if mode == "debug" else ""`
  which passed empty string to logger.info in core mode (syntax bug)
- P0-E2 (DEC-026): cache key now includes filter config fingerprint
  self._filter_cfg_hash computed once at init, passed to compute_cache_id()
- DEC-027: execution_time_ms always collected (was None in core mode)
           timing collection is ~50ns and has no meaningful overhead
- P1-CH3-3: count_by_type() replaced with np.sum(values != 0) in all hot paths
- P1-CH3-7: __init__ now accepts StrategyConfig (typed) not raw Dict
"""

import hashlib
import json
import logging
from typing import Dict, List, Any, Optional
from time import perf_counter

import pandas as pd
import numpy as np

from src.strategies.contracts.signal_contracts import SignalFrame
from src.strategies.contracts.filter_contracts import (
    FilterResult,
    FilterMetadata,
    FilterPipelineResult,
    FilterStatus,
    FilterProtocol,
)
from src.strategies.contracts.cache import FilterPipelineCache
from src.config.config_schema import StrategyConfig

# Import all filter implementations
from src.strategies.specific.filters.time_filter import TimeFilter
from src.strategies.specific.filters.rsi_filter import RSIFilter
from src.strategies.specific.filters.cci_filter import CCIFilter
from src.strategies.specific.filters.adx_filter import ADXFilter
from src.strategies.specific.filters.bollinger_filter import BollingerFilter
from src.strategies.specific.filters.choppiness_filter import ChoppinessFilter
from src.strategies.specific.filters.dpo_filter import DPOFilter
from src.strategies.specific.filters.ma_filter import MAFilter
from src.strategies.specific.filters.macd_filter import MACDFilter
from src.strategies.specific.filters.pivot_filter import PivotFilter
from src.strategies.specific.filters.supertrend_filter import SupertrendFilter

logger = logging.getLogger(__name__)


class FilterPipeline:
    """
    Orchestrates signal filtering through time and technical filters.

    Features:
    - Accepts StrategyConfig (typed) — no raw dicts
    - Time filter always runs first (DEC-006)
    - Indicator caching with config-aware cache key (DEC-026)
    - All logging gated on analytics mode (P0-CH3-2)
    - Timing always collected (DEC-027)
    - Early exit on empty signals

    Performance:
    - Core mode: ~10-20ms for 200 signals (cached indicators), zero log overhead
    - Analytics mode: +2-5ms for metadata and logging
    - First run per config: +50-100ms for indicator computation
    """

    # Filter class mapping for auto-instantiation (DEC-005)
    FILTER_CLASSES = {
        "rsi_filter": RSIFilter,
        "cci_filter": CCIFilter,
        "adx_filter": ADXFilter,
        "bollinger_filter": BollingerFilter,
        "choppiness_filter": ChoppinessFilter,
        "dpo_filter": DPOFilter,
        "ma_filter": MAFilter,
        "macd_filter": MACDFilter,
        "pivot_filter": PivotFilter,
        "supertrend_filter": SupertrendFilter,
    }

    def __init__(
        self,
        config: StrategyConfig,
        mode: str = "core",
        cache: Optional[FilterPipelineCache] = None,
    ):
        """
        Initialize FilterPipeline with typed configuration.

        Args:
            config: Validated StrategyConfig (from config_schema.py)
            mode: Execution mode — "core" (fast) or "analytics" (full logging)
            cache: Optional cache instance (creates new if not provided)

        Raises:
            ValueError: If mode is invalid or "debug" (deprecated)
        """
        if mode == "debug":
            raise ValueError(
                "Mode 'debug' has been renamed to 'analytics' (DEC-022). "
                "Update your call: mode='analytics'"
            )
        valid_modes = {"core", "analytics"}
        if mode not in valid_modes:
            raise ValueError(
                f"Invalid mode '{mode}'. Must be one of: {valid_modes}"
            )

        self._mode = mode
        self.config = config

        # Filter execution order (time filter is not in this list — always first per DEC-006)
        self.filter_sequence: List[str] = list(config.filters.filter_sequence)

        # Compute filter config fingerprint ONCE at init (DEC-026)
        # This ensures cache key is unique per filter configuration
        self._filter_cfg_hash = self._compute_filter_cfg_hash(config)

        # Initialize cache
        self.cache = cache or FilterPipelineCache()

        # Shared indicator storage (populated by compute_indicators)
        self.indicators: Dict[str, pd.Series] = {}
        self.ind_np: Dict[str, np.ndarray] = {}

        # Filter instances
        self.time_filter: Optional[TimeFilter] = None
        self.technical_filters: List[FilterProtocol] = []

        self._load_filters()

        if self._mode == "analytics":
            logger.info(
                f"FilterPipeline initialized: "
                f"time_filter={'enabled' if self.time_filter and self.time_filter.enabled else 'disabled'}, "
                f"technical_filters={len(self.technical_filters)}, "
                f"cfg_hash={self._filter_cfg_hash}"
            )

    @staticmethod
    def _compute_filter_cfg_hash(config: StrategyConfig) -> str:
        """
        Compute stable hash of active filter configuration (DEC-026).

        Only enabled technical filters contribute to the hash.
        Time filter is excluded — its parameters don't affect indicator computation.

        Args:
            config: Validated StrategyConfig

        Returns:
            12-character MD5 hex digest
        """
        active = {
            name: fcfg.config
            for name, fcfg in config.filters.technical_filters.items()
            if fcfg.enabled
        }
        serialized = json.dumps(active, sort_keys=True, default=str)
        return hashlib.md5(serialized.encode()).hexdigest()[:12]

    def _load_filters(self) -> None:
        """Load and instantiate all filters from configuration."""
        self._load_time_filter()
        self._load_technical_filters()

    def _load_time_filter(self) -> None:
        """
        Initialize time filter from config.

        Time filter is special — no indicators needed, always runs first (DEC-006).
        Config sourced from filters.time_filters section of StrategyConfig.
        """
        try:
            time_filter_cfg = self.config.filters.time_filters.get("time_filter")
            if time_filter_cfg is None:
                if self._mode == "analytics":
                    logger.info("Time filter: not configured — skipped")
                return

            self.time_filter = TimeFilter(
                config=time_filter_cfg.config,
                name="time_filter"
            )

            if self._mode == "analytics":
                if self.time_filter.enabled:
                    logger.info(
                        f"Time filter: "
                        f"{self.time_filter.session_start_hour:02d}:{self.time_filter.session_start_minute:02d}"
                        f" – "
                        f"{self.time_filter.session_end_hour:02d}:{self.time_filter.session_end_minute:02d}"
                    )
                else:
                    logger.info("Time filter: DISABLED")

        except Exception as e:
            logger.error(f"Failed to load time filter: {e}")
            self.time_filter = None

    def _load_technical_filters(self) -> None:
        """
        Initialize technical filters from configuration.

        Only loads enabled filters, in filter_sequence order.
        Unknown filter names log a warning and are skipped (DEC-008).
        """
        for filter_name in self.filter_sequence:
            filter_cfg = self.config.filters.technical_filters.get(filter_name)

            if filter_cfg is None or not filter_cfg.enabled:
                continue

            filter_class = self.FILTER_CLASSES.get(filter_name)
            if filter_class is None:
                logger.warning(f"Unknown filter in sequence: '{filter_name}' — skipped")
                continue

            try:
                filter_instance = filter_class(
                    name=filter_name,
                    **filter_cfg.config
                )
                self.technical_filters.append(filter_instance)
                if self._mode == "analytics":
                    logger.info(f"Loaded filter: {filter_name}")
            except Exception as e:
                logger.error(f"Failed to load filter '{filter_name}': {e}")

    def compute_indicators(self, df: pd.DataFrame) -> None:
        """
        Compute indicators for all technical filters, or load from cache.

        Cache key includes both data fingerprint and filter config fingerprint
        (DEC-026) to prevent cross-config collisions.

        Args:
            df: OHLCV DataFrame (strategy timeframe)
        """
        cache_id = self.cache.compute_cache_id(df, self._filter_cfg_hash)

        if self.cache.has(cache_id):
            cached = self.cache.get(cache_id)
            self.indicators = cached["indicators"]
            self.ind_np = cached["indicators_np"]
            if self._mode == "analytics":
                logger.info(
                    f"Indicators: loaded {len(self.indicators)} from cache "
                    f"(id={cache_id[:8]})"
                )
            return

        # Cache miss — compute all indicators
        self.indicators = {}
        self.ind_np = {}

        df_f32 = df.astype("float32")
        compute_start = perf_counter()

        for filt in self.technical_filters:
            try:
                filt.compute_indicators(df_f32, self.indicators, self.ind_np)
            except Exception as e:
                logger.warning(
                    f"Failed to compute indicators for '{filt.name}': {e}"
                )

        elapsed_ms = (perf_counter() - compute_start) * 1000
        self.cache.store(cache_id, self.indicators, self.ind_np)

        if self._mode == "analytics":
            logger.info(
                f"Indicators: computed {len(self.indicators)} in {elapsed_ms:.1f}ms "
                f"(cached as {cache_id[:8]})"
            )

    def apply_filters(
        self,
        signal_frame: SignalFrame,
        df: pd.DataFrame,
        mode: Optional[str] = None,
    ) -> FilterPipelineResult:
        """
        Apply all filters to signals (time filter first, then technical).

        Execution flow:
        1. Count raw signals (numpy, no dict allocation)
        2. Apply time filter (if configured and enabled)
        3. Early exit if no signals remain
        4. Compute/load indicators
        5. Apply technical filters sequentially with early exit
        6. Return FilterPipelineResult with full counts and timing

        Args:
            signal_frame: Raw signals from SignalGenerator
            df: OHLCV DataFrame (strategy timeframe)
            mode: Execution mode override. If None, uses self._mode set at init.
                  Provided for call-site convenience only — prefer setting mode at init.

        Returns:
            FilterPipelineResult with final_signals, counts, filter_results,
            rejection_reasons, and execution_time_ms (always populated, DEC-027).
        """
        effective_mode = mode if mode is not None else self._mode
        pipeline_start = perf_counter()

        # Raw signal count — numpy direct, no dict (P1-CH3-3)
        raw_count = int(np.sum(signal_frame.signals.values != 0))

        filter_results: List[FilterMetadata] = []
        rejection_reasons: Dict[str, int] = {}

        if effective_mode == "analytics":
            logger.info(
                f"FilterPipeline starting: {raw_count} raw signals"
            )

        # ----------------------------------------------------------------
        # STAGE 1: Time Filter (always first — DEC-006)
        # ----------------------------------------------------------------
        current_signals = signal_frame
        time_filtered_count = raw_count

        if self.time_filter is not None:
            time_result = self.time_filter.apply_filter(
                signal_frame=current_signals,
                df=df,
                indicators=self.indicators,
                ind_np=self.ind_np,
                mode=effective_mode,
            )

            current_signals = time_result.signal_frame
            time_filtered_count = int(
                np.sum(current_signals.signals.values != 0)
            )

            filter_results.append(time_result.metadata)

            if time_result.metadata.signals_rejected > 0:
                rejection_reasons["time_filter"] = time_result.metadata.signals_rejected

            if effective_mode == "analytics":
                logger.info(
                    f"Time filter: {raw_count} → {time_filtered_count} "
                    f"({time_result.metadata.signals_rejected} rejected)"
                )

            if time_filtered_count == 0:
                pipeline_time_ms = (perf_counter() - pipeline_start) * 1000
                if effective_mode == "analytics":
                    logger.info("Pipeline early exit: no signals after time filter")
                return FilterPipelineResult(
                    final_signals=current_signals,
                    raw_count=raw_count,
                    time_filtered_count=0,
                    technical_filtered_count=0,
                    final_count=0,
                    filter_results=filter_results,
                    rejection_reasons=rejection_reasons,
                    execution_time_ms=pipeline_time_ms,  # DEC-027: always set
                )

        # ----------------------------------------------------------------
        # STAGE 2: Technical Filters
        # ----------------------------------------------------------------
        if self.technical_filters:
            self.compute_indicators(df)

        for filt in self.technical_filters:
            try:
                result = filt.apply_filter(
                    signal_frame=current_signals,
                    df=df,
                    indicators=self.indicators,
                    ind_np=self.ind_np,
                    mode=effective_mode,
                )
            except Exception as e:
                # DEC-008/DEC-028: log error, pass signals through unchanged
                logger.error(f"Filter '{filt.name}' raised an exception: {e}")
                signals_n = int(np.sum(current_signals.signals.values != 0))
                result = FilterResult(
                    passed=True,
                    signal_frame=current_signals,
                    metadata=FilterMetadata(
                        filter_name=filt.name,
                        status=FilterStatus.ERROR,
                        signals_in=signals_n,
                        signals_out=signals_n,
                        signals_rejected=0,
                        reason=str(e),
                        execution_time_ms=None,
                    ),
                )

            current_signals = result.signal_frame
            filter_results.append(result.metadata)

            if result.metadata.signals_rejected > 0:
                rejection_reasons[filt.name] = result.metadata.signals_rejected

            if effective_mode == "analytics":
                logger.info(
                    f"{filt.name}: {result.metadata.signals_in} → "
                    f"{result.metadata.signals_out} "
                    f"({result.metadata.signals_rejected} rejected)"
                )

            # Early exit — numpy direct (P1-CH3-3)
            if int(np.sum(current_signals.signals.values != 0)) == 0:
                if effective_mode == "analytics":
                    logger.info(
                        f"Pipeline early exit: no signals after {filt.name}"
                    )
                break

        # Final count — numpy direct (P1-CH3-3)
        final_count = int(np.sum(current_signals.signals.values != 0))
        pipeline_time_ms = (perf_counter() - pipeline_start) * 1000  # DEC-027: always

        pipeline_result = FilterPipelineResult(
            final_signals=current_signals,
            raw_count=raw_count,
            time_filtered_count=time_filtered_count,
            technical_filtered_count=final_count,
            final_count=final_count,
            filter_results=filter_results,
            rejection_reasons=rejection_reasons,
            execution_time_ms=pipeline_time_ms,
        )

        if effective_mode == "analytics":
            logger.info(
                f"FilterPipeline complete: {raw_count} → {final_count} signals "
                f"({pipeline_result.pass_rate:.1f}% pass rate, "
                f"{pipeline_time_ms:.1f}ms)"
            )

        return pipeline_result
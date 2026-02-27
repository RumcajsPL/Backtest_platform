"""
Filter Pipeline - Orchestrator for Signal Filtering

Version: 2.4.0

Changes from v2.3.0:
- [BUG-1] _load_time_filter: unwrap nested 'config' key when time_filter YAML
  uses the structured form:
      time_filter:
        enabled: True
        config:
          session_start: {hour: 0, minute: 0}
          session_end: {hour: 23, minute: 59}
  FilterConfig.from_dict() correctly stores {'config': {nested}} in FilterConfig.config
  (because 'config' is not a known_key). But _load_time_filter then spread that as
  **{'config': {nested}} into TimeFilterConfig.from_dict(), which expects flat keys
  (session_start, session_end, excluded_days). The nested 'config' key was silently
  ignored and TimeFilterConfig defaulted to 08:30–20:30.
  Fix: detect the nested 'config' key in time_filter_cfg.config and unwrap it one
  level before passing to TimeFilterConfig.from_dict(). Flat structure (no nesting)
  continues to work unchanged.
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
from src.config.config_schema import StrategyConfig, TimeFilterConfig

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
    """

    # Filter class mapping for auto-instantiation
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
            config: Validated StrategyConfig
            mode: Execution mode — "core" or "analytics"
            cache: Optional cache instance (creates new if not provided)

        Raises:
            ValueError: If mode is invalid
        """
        valid_modes = {"core", "analytics"}
        if mode not in valid_modes:
            raise ValueError(
                f"Invalid mode '{mode}'. Must be one of: {valid_modes}"
            )

        self._mode = mode
        self.config = config

        self.filter_sequence: List[str] = list(config.filters.filter_sequence)

        self._filter_cfg_hash = self._compute_filter_cfg_hash(config)

        self.cache = cache or FilterPipelineCache()

        self.indicators: Dict[str, pd.Series] = {}
        self.ind_np: Dict[str, np.ndarray] = {}

        self.time_filter: Optional[TimeFilter] = None
        self.technical_filters: List[FilterProtocol] = []

        self._load_filters()

        if self._mode == "analytics":
            # self.time_filter is now either an enabled filter or None —
            # no need to check .enabled separately.
            time_filter_status = "enabled" if self.time_filter is not None else "disabled/not configured"
            logger.info(
                f"FilterPipeline initialized: "
                f"time_filter={time_filter_status}, "
                f"technical_filters={len(self.technical_filters)}, "
                f"cfg_hash={self._filter_cfg_hash}"
            )

    @staticmethod
    def _compute_filter_cfg_hash(config: StrategyConfig) -> str:
        """Compute stable hash of active filter configuration.

        Includes both filter names and their parameter dicts — a change to
        any active filter's name or params produces a different hash.
        filter_sequence order is not included: the sequence governs execution
        order but the indicator computation is order-independent and the cache
        is keyed on what is computed, not in what order.
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
        Initialize time filter from config using typed TimeFilterConfig.

        [H3] A disabled time filter (enabled: false) is NOT instantiated.
        self.time_filter remains None, which apply_filters() treats as
        'no time filtering required'. This ensures disabled filters produce
        no entries in FilterPipelineResult.filter_results.

        [P6] No try/except: construction errors are config bugs and must
        propagate immediately. StrategyConfig validates the config before
        this method is ever called.

        [BUG-1] The time_filter YAML uses a nested 'config' block:
            time_filter:
              enabled: True
              config:
                session_start: {hour: 8, minute: 30}
                session_end: {hour: 20, minute: 30}
        FilterConfig.from_dict() stores this as FilterConfig.config = {'config': {nested}}.
        We must unwrap that one level before passing to TimeFilterConfig.from_dict(),
        which expects flat keys (session_start, session_end, excluded_days).
        If the config is already flat (no nested 'config' key), it passes through unchanged.
        """
        time_filter_cfg = self.config.filters.time_filters.get("time_filter")

        # Treat absent config and disabled config identically —
        # in both cases self.time_filter stays None.
        if time_filter_cfg is None or not time_filter_cfg.enabled:
            if self._mode == "analytics":
                reason = "not configured" if time_filter_cfg is None else "disabled"
                logger.info(f"Time filter: {reason} — skipped")
            return

        # [BUG-1] Unwrap nested 'config' key if present.
        # YAML form:  time_filter: {enabled: True, config: {session_start: ...}}
        #   → FilterConfig.config = {'config': {'session_start': ...}}   ← nested
        # Flat form:  time_filter: {enabled: True, session_start: ...}
        #   → FilterConfig.config = {'session_start': ...}               ← already flat
        raw_params = time_filter_cfg.config
        if "config" in raw_params and isinstance(raw_params.get("config"), dict):
            # Structured YAML form: unwrap the inner dict
            inner_params = raw_params["config"]
        else:
            # Flat form: use as-is
            inner_params = raw_params

        typed_config = TimeFilterConfig.from_dict({
            "enabled": time_filter_cfg.enabled,
            **inner_params
        })

        self.time_filter = TimeFilter(
            config=typed_config,
            name="time_filter"
        )

        if self._mode == "analytics":
            logger.info(
                f"Time filter: "
                f"{self.time_filter.session_start_hour:02d}:{self.time_filter.session_start_minute:02d}"
                f" – "
                f"{self.time_filter.session_end_hour:02d}:{self.time_filter.session_end_minute:02d}"
            )

    def _load_technical_filters(self) -> None:
        """Initialize technical filters from configuration."""
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
        """Compute indicators for all technical filters, or load from cache."""
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
        """Apply all filters to signals (time filter first, then technical)."""
        effective_mode = mode if mode is not None else self._mode
        pipeline_start = perf_counter()

        raw_count = int(np.sum(signal_frame.signals.values != 0))

        filter_results: List[FilterMetadata] = []
        rejection_reasons: Dict[str, int] = {}

        if effective_mode == "analytics":
            logger.info(
                f"FilterPipeline starting: {raw_count} raw signals"
            )

        # ----------------------------------------------------------------
        # STAGE 1: Time Filter (always first, only if configured and enabled)
        # ----------------------------------------------------------------
        current_signals = signal_frame
        time_filtered_count = raw_count

        # self.time_filter is None when disabled or not configured —
        # in that case the entire stage is skipped cleanly with no results entry.
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
                    execution_time_ms=pipeline_time_ms,
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

            if int(np.sum(current_signals.signals.values != 0)) == 0:
                if effective_mode == "analytics":
                    logger.info(
                        f"Pipeline early exit: no signals after {filt.name}"
                    )
                break

        final_count = int(np.sum(current_signals.signals.values != 0))
        pipeline_time_ms = (perf_counter() - pipeline_start) * 1000

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
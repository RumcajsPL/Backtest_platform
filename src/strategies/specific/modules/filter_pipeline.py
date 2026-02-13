"""
Filter Pipeline - Orchestrator for Signal Filtering

Coordinates time and technical filters with indicator caching and early exit.
Migrated from dict-based to typed contract architecture.

Author: Migration Project
Version: 2.0.0
Date: 2025-02-13
Session: 5
"""

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
    FilterProtocol
)
from src.strategies.contracts.cache import FilterPipelineCache

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
    - Auto-instantiates filters from config
    - Time filter always runs first
    - Indicator caching (compute once, reuse)
    - Early exit on empty signals
    - Dual-mode execution (core/debug)
    - Returns typed FilterPipelineResult
    
    Performance:
    - Core mode: ~10-20ms for 200 signals (cached indicators)
    - Debug mode: +2-5ms for metadata collection
    - First run: +50-100ms for indicator computation
    """
    
    # Filter class mapping for auto-instantiation
    FILTER_CLASSES = {
        'rsi_filter': RSIFilter,
        'cci_filter': CCIFilter,
        'adx_filter': ADXFilter,
        'bollinger_filter': BollingerFilter,
        'choppiness_filter': ChoppinessFilter,
        'dpo_filter': DPOFilter,
        'ma_filter': MAFilter,
        'macd_filter': MACDFilter,
        'pivot_filter': PivotFilter,
        'supertrend_filter': SupertrendFilter,
    }
    
    def __init__(self, config: Dict[str, Any], cache: Optional[FilterPipelineCache] = None):
        """
        Initialize FilterPipeline with configuration.
        
        Args:
            config: Strategy configuration with 'filters', 'filter_sequence', 'trade_management'
            cache: Optional cache instance (creates new if not provided)
        """
        self.config = config
        self.filters_cfg: Dict[str, Any] = config.get("filters", {})
        self.trade_mgmt_cfg: Dict[str, Any] = config.get("trade_management", {})
        
        # Filter execution order (time filter always first)
        self.filter_sequence: List[str] = config.get(
            "filter_sequence",
            [
                "rsi_filter",
                "choppiness_filter",
                "bollinger_filter",
                "adx_filter",
                "supertrend_filter",
                "ma_filter",
                "pivot_filter",
                "cci_filter",
                "macd_filter",
                "dpo_filter",
            ],
        )
        
        # Initialize cache
        self.cache = cache or FilterPipelineCache()
        
        # Storage for indicators (shared across filters)
        self.indicators: Dict[str, pd.Series] = {}
        self.ind_np: Dict[str, np.ndarray] = {}
        
        # Initialize filters
        self.time_filter: Optional[TimeFilter] = None
        self.technical_filters: List[FilterProtocol] = []
        
        self._load_filters()
        
        logger.info(
            f"FilterPipeline initialized: "
            f"time_filter={'enabled' if self.time_filter and self.time_filter.enabled else 'disabled'}, "
            f"technical_filters={len(self.technical_filters)}"
        )
    
    def _load_filters(self) -> None:
        """
        Load and instantiate filters from configuration.
        
        Time filter is loaded first and always runs first.
        Technical filters are loaded in configured sequence order.
        """
        # Load time filter (always first)
        self._load_time_filter()
        
        # Load technical filters
        self._load_technical_filters()
    
    def _load_time_filter(self) -> None:
        """
        Initialize time filter from trade_management config.
        
        Time filter is special - it doesn't use indicators and always runs first.
        """
        try:
            self.time_filter = TimeFilter(
                config=self.trade_mgmt_cfg,
                name="time_filter"
            )
            if self.time_filter.enabled:
                logger.info(
                    f"Time filter: "
                    f"{self.time_filter.session_start_hour:02d}:{self.time_filter.session_start_minute:02d} - "
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
        
        Only loads enabled filters. Filters are instantiated in the order
        specified by filter_sequence.
        """
        for filter_name in self.filter_sequence:
            filter_cfg = self.filters_cfg.get(filter_name)
            
            # Skip if not in config or not enabled
            if filter_cfg is None or not filter_cfg.get("enabled", False):
                continue
            
            # Get filter class
            filter_class = self.FILTER_CLASSES.get(filter_name)
            if filter_class is None:
                logger.warning(f"Unknown filter in sequence: {filter_name}")
                continue
            
            # Instantiate filter
            try:
                filter_instance = filter_class(
                    name=filter_name,
                    **filter_cfg  # Pass all config parameters
                )
                self.technical_filters.append(filter_instance)
                logger.info(f"Loaded filter: {filter_name}")
            except Exception as e:
                logger.error(f"Failed to load {filter_name}: {e}")
                continue
    
    def compute_indicators(self, df: pd.DataFrame) -> None:
        """
        Compute indicators for all filters OR load from cache.
        
        Indicators are shared across all filters to avoid redundant computation.
        Uses cache to skip computation on repeated runs with same data.
        
        Args:
            df: OHLCV DataFrame (strategy timeframe)
        """
        # Check cache first
        cache_id = self.cache.compute_cache_id(df)
        
        if self.cache.has(cache_id):
            cached = self.cache.get(cache_id)
            self.indicators = cached["indicators"]
            self.ind_np = cached["indicators_np"]
            logger.info(f"Loaded {len(self.indicators)} indicators from cache")
            return
        
        # Reset indicator storage
        self.indicators = {}
        self.ind_np = {}
        
        # Convert to float32 for performance
        df = df.astype("float32")
        
        # Compute indicators for each filter
        start_time = perf_counter()
        
        for filt in self.technical_filters:
            try:
                filt.compute_indicators(df, self.indicators, self.ind_np)
            except Exception as e:
                logger.warning(f"Failed to compute indicators for {filt.name}: {e}")
                continue
        
        elapsed = (perf_counter() - start_time) * 1000
        
        # Store in cache
        self.cache.store(cache_id, self.indicators, self.ind_np)
        
        logger.info(
            f"Computed {len(self.indicators)} indicators in {elapsed:.1f}ms "
            f"(cached for future runs)"
        )
    
    def apply_filters(
        self,
        signal_frame: SignalFrame,
        df: pd.DataFrame,
        mode: str = "core"
    ) -> FilterPipelineResult:
        """
        Apply all filters to signals (time filter + technical filters).
        
        Execution flow:
        1. Count raw signals
        2. Apply time filter (if enabled)
        3. Compute/load indicators
        4. Apply technical filters sequentially with early exit
        5. Build FilterPipelineResult with full stats
        
        Args:
            signal_frame: Raw signals from signal generator
            df: OHLCV DataFrame (strategy timeframe)
            mode: Execution mode ("core" or "debug")
        
        Returns:
            FilterPipelineResult with:
            - final_signals: Signals that passed all filters
            - Counts: raw, time_filtered, technical_filtered, final
            - filter_results: List of FilterMetadata from each filter
            - rejection_reasons: Dict of rejection counts by filter
            - execution_time_ms: Total pipeline execution time
        """
        pipeline_start = perf_counter()
        
        # Track counts at each stage
        raw_count = signal_frame.count_by_type()["total"]
        
        # Storage for filter results
        filter_results: List[FilterMetadata] = []
        rejection_reasons: Dict[str, int] = {}
        
        logger.info(f"FilterPipeline starting: {raw_count} raw signals, mode={mode}")
        
        # ----------------------------------------------------------------
        # STAGE 1: Time Filter
        # ----------------------------------------------------------------
        current_signals = signal_frame
        time_filtered_count = raw_count
        
        if self.time_filter is not None:
            time_result = self.time_filter.apply_filter(
                signal_frame=current_signals,
                df=df,
                indicators=self.indicators,
                ind_np=self.ind_np,
                mode=mode
            )
            
            current_signals = time_result.signal_frame
            time_filtered_count = time_result.signals_count
            
            filter_results.append(time_result.metadata)
            
            if time_result.metadata.signals_rejected > 0:
                rejection_reasons["time_filter"] = time_result.metadata.signals_rejected
            
            logger.info(
                f"Time filter: {raw_count} → {time_filtered_count} "
                f"({time_result.metadata.signals_rejected} rejected)"
            )
            
            # Early exit if no signals
            if time_filtered_count == 0:
                pipeline_time = (perf_counter() - pipeline_start) * 1000
                logger.info("Pipeline early exit: no signals after time filter")
                
                return FilterPipelineResult(
                    final_signals=current_signals,
                    raw_count=raw_count,
                    time_filtered_count=0,
                    technical_filtered_count=0,
                    final_count=0,
                    filter_results=filter_results,
                    rejection_reasons=rejection_reasons,
                    execution_time_ms=pipeline_time if mode == "debug" else None
                )
        
        # ----------------------------------------------------------------
        # STAGE 2: Technical Filters
        # ----------------------------------------------------------------
        
        # Compute indicators (or load from cache)
        if len(self.technical_filters) > 0:
            self.compute_indicators(df)
        
        # Apply each technical filter sequentially
        technical_start_count = time_filtered_count
        
        for filt in self.technical_filters:
            # Apply filter
            try:
                result = filt.apply_filter(
                    signal_frame=current_signals,
                    df=df,
                    indicators=self.indicators,
                    ind_np=self.ind_np,
                    mode=mode
                )
            except Exception as e:
                logger.error(f"Filter {filt.name} failed: {e}")
                # Create error metadata
                result = FilterResult(
                    passed=True,  # Don't block pipeline
                    signal_frame=current_signals,  # Pass through unchanged
                    metadata=FilterMetadata(
                        filter_name=filt.name,
                        status=FilterStatus.ERROR,
                        signals_in=current_signals.count_by_type()["total"],
                        signals_out=current_signals.count_by_type()["total"],
                        signals_rejected=0,
                        reason=str(e),
                        execution_time_ms=None
                    )
                )
            
            # Update current signals
            current_signals = result.signal_frame
            
            # Track metadata
            filter_results.append(result.metadata)
            
            # Track rejections
            if result.metadata.signals_rejected > 0:
                rejection_reasons[filt.name] = result.metadata.signals_rejected
            
            # Log progress
            logger.info(
                f"{filt.name}: {result.metadata.signals_in} → {result.metadata.signals_out} "
                f"({result.metadata.signals_rejected} rejected)"
            )
            
            # Early exit if no signals remain
            if result.metadata.signals_out == 0:
                logger.info(f"Pipeline early exit: no signals after {filt.name}")
                break
        
        # Final counts
        technical_filtered_count = current_signals.count_by_type()["total"]
        final_count = technical_filtered_count
        
        # Calculate total execution time
        pipeline_time = (perf_counter() - pipeline_start) * 1000
        
        # Build result
        result = FilterPipelineResult(
            final_signals=current_signals,
            raw_count=raw_count,
            time_filtered_count=time_filtered_count,
            technical_filtered_count=technical_filtered_count,
            final_count=final_count,
            filter_results=filter_results,
            rejection_reasons=rejection_reasons,
            execution_time_ms=pipeline_time if mode == "debug" else None
        )
        
        # Log summary
        logger.info(
            f"FilterPipeline complete: {raw_count} → {final_count} signals "
            f"(pass rate: {result.pass_rate:.1f}%) "
            f"[time: {pipeline_time:.1f}ms]" if mode == "debug" else ""
        )
        
        return result
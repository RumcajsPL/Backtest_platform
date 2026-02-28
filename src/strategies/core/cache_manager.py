"""
Cache Manager - Centralized Cache Management for Multi-Run Backtesting
Version: 1.0.0 (Hardening II Final)
Purpose:
    Provides a single source of truth for all module-level caches in the system.
    Enables clean cache lifecycle management between backtester runs.
    Modules integrated:
    - RiskManager (ATR cache, annual range cache)
    - SpreadManager (YAML config cache)
    - FilterPipeline (indicator cache via FilterPipelineCache)
Usage in backtester loop:
    cache_manager = CacheManager()
    for params in parameter_grid:
        config = build_config(params)
        orchestrator = StrategyOrchestrator(config, cache_manager=cache_manager)
        result = orchestrator.run()
        cache_manager.clear_all_caches()  # Reset between runs
"""

from __future__ import annotations

import logging
from typing import Dict, Optional, Any

import pandas as pd

logger = logging.getLogger(__name__)

class CacheManager:
    """
    Centralized cache manager for multi-run backtesting.

    All caches are stored as dictionaries keyed by stable fingerprints.
    The clear_all_caches() method should be called between runs in a backtester loop.
    """

    def __init__(self):
        """Initialize empty caches."""
        # RiskManager caches
        self._atr_cache: Dict[str, pd.Series] = {}
        self._annual_range_cache: Dict[str, pd.Series] = {}

        # SpreadManager caches
        self._spread_config_cache: Dict[str, Dict] = {}

        # FilterPipeline cache is handled separately via FilterPipelineCache
        # but can be registered here if needed

        # Cache statistics
        self._stats = {
            "atr_hits": 0,
            "atr_misses": 0,
            "annual_range_hits": 0,
            "annual_range_misses": 0,
            "spread_config_hits": 0,
            "spread_config_misses": 0,
        }

    # ------------------------------------------------------------------
    # RiskManager Caches
    # ------------------------------------------------------------------

    def get_atr(self, key: str) -> Optional[pd.Series]:
        """Get ATR series from cache."""
        if key in self._atr_cache:
            self._stats["atr_hits"] += 1
            return self._atr_cache[key]
        self._stats["atr_misses"] += 1
        return None

    def set_atr(self, key: str, series: pd.Series) -> None:
        """Store ATR series in cache."""
        self._atr_cache[key] = series

    def get_annual_range(self, key: str) -> Optional[pd.Series]:
        """Get annual range series from cache."""
        if key in self._annual_range_cache:
            self._stats["annual_range_hits"] += 1
            return self._annual_range_cache[key]
        self._stats["annual_range_misses"] += 1
        return None

    def set_annual_range(self, key: str, series: pd.Series) -> None:
        """Store annual range series in cache."""
        self._annual_range_cache[key] = series

    # ------------------------------------------------------------------
    # SpreadManager Caches
    # ------------------------------------------------------------------

    def get_spread_config(self, key: str) -> Optional[Dict]:
        """Get spread configuration from cache."""
        if key in self._spread_config_cache:
            self._stats["spread_config_hits"] += 1
            return self._spread_config_cache[key]
        self._stats["spread_config_misses"] += 1
        return None

    def set_spread_config(self, key: str, config: Dict) -> None:
        """Store spread configuration in cache."""
        self._spread_config_cache[key] = config

    # ------------------------------------------------------------------
    # Cache Management
    # ------------------------------------------------------------------

    def clear_all_caches(self) -> None:
        """Clear all caches - call between backtester runs."""
        self._atr_cache.clear()
        self._annual_range_cache.clear()
        self._spread_config_cache.clear()

        # Reset statistics (optional)
        for key in self._stats:
            self._stats[key] = 0

        logger.debug("All caches cleared.")

    def clear_atr_cache(self) -> None:
        """Clear only ATR cache."""
        self._atr_cache.clear()

    def clear_annual_range_cache(self) -> None:
        """Clear only annual range cache."""
        self._annual_range_cache.clear()

    def clear_spread_config_cache(self) -> None:
        """Clear only spread config cache."""
        self._spread_config_cache.clear()

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return {
            "atr": {
                "entries": len(self._atr_cache),
                "hits": self._stats["atr_hits"],
                "misses": self._stats["atr_misses"],
                "hit_rate": self._calculate_hit_rate(
                    self._stats["atr_hits"], self._stats["atr_misses"]
                ),
            },
            "annual_range": {
                "entries": len(self._annual_range_cache),
                "hits": self._stats["annual_range_hits"],
                "misses": self._stats["annual_range_misses"],
                "hit_rate": self._calculate_hit_rate(
                    self._stats["annual_range_hits"], self._stats["annual_range_misses"]
                ),
            },
            "spread_config": {
                "entries": len(self._spread_config_cache),
                "hits": self._stats["spread_config_hits"],
                "misses": self._stats["spread_config_misses"],
                "hit_rate": self._calculate_hit_rate(
                    self._stats["spread_config_hits"], self._stats["spread_config_misses"]
                ),
            },
        }

    @staticmethod
    def _calculate_hit_rate(hits: int, misses: int) -> float:
        """Calculate hit rate percentage."""
        total = hits + misses
        return (hits / total * 100) if total > 0 else 0.0

    def __repr__(self) -> str:
        stats = self.get_stats()
        return (
            f"CacheManager("
            f"ATR:{stats['atr']['entries']}, "
            f"AnnRange:{stats['annual_range']['entries']}, "
            f"Spread:{stats['spread_config']['entries']})"
        )
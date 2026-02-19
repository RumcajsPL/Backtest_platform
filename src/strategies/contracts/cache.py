"""
Filter Pipeline Cache - Indicator Caching System

Lightweight cache for precomputed indicators to avoid redundant calculations.
Uses SHA1 hashing of OHLCV data + filter config fingerprint for stable cache keys.

Author: Migration Project
Version: 2.1.0
Date: 2026-02-19
Session: 20 Block D

Changes from v2.0.0:
- P0-E2 (DEC-026): compute_cache_id() now requires filter_cfg_hash parameter.
  Cache key = data fingerprint + filter config fingerprint.
  Prevents cross-config cache collisions in multi-run backtester.
- Added hits/misses counters for observability (DEC-027)
"""

import hashlib
import json
import pickle
import logging
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class FilterPipelineCache:
    """
    Lightweight cache for indicator sets.

    Stores precomputed indicators keyed by a cache_id derived from:
    - OHLCV data fingerprint (timestamps + row count + sample close prices)
    - Filter config fingerprint (which filters enabled + their parameters)

    Including the filter config fingerprint (DEC-026) prevents the following bug:
    when the multi-run backtester alternates between filter configurations
    (e.g. ADX length 14 vs 20), both would previously share the same cache key,
    causing silent use of wrong precomputed indicators for the second config.

    Performance Impact:
    - First run per config: Compute all indicators (~50-100ms)
    - Cached runs: Load from memory (~1ms)
    - Typical speedup: 50-100x for repeated backtests with same config
    """

    def __init__(self):
        """Initialize empty cache with hit/miss counters."""
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._hits: int = 0
        self._misses: int = 0

    @staticmethod
    def compute_filter_config_hash(filter_configs: Dict[str, Any]) -> str:
        """
        Compute a stable hash of the active filter configuration.

        Includes: filter name, enabled status, all parameters.
        Excludes: disabled filters (they don't affect indicator computation).

        Args:
            filter_configs: Dict mapping filter name → config dict.
                            Only enabled filters are included in the hash.

        Returns:
            12-character MD5 hex digest (sufficient for cache differentiation)
        """
        # Include only enabled filters — disabled ones don't affect indicators
        active = {
            name: cfg
            for name, cfg in filter_configs.items()
            if isinstance(cfg, dict) and cfg.get("enabled", False)
        }
        # Sort for stability — dict ordering is insertion-order in Python 3.7+
        # but explicit sort ensures cross-version and cross-run consistency
        serialized = json.dumps(active, sort_keys=True, default=str)
        return hashlib.md5(serialized.encode()).hexdigest()[:12]

    @staticmethod
    def compute_cache_id(df: pd.DataFrame, filter_cfg_hash: str) -> str:
        """
        Compute a stable cache key for a dataset + filter config combination.

        Uses:
        - First/last timestamps (date range identity)
        - Row count (data completeness)
        - Head/tail close prices (data integrity sample)
        - filter_cfg_hash (filter configuration identity — DEC-026)

        Fast and reliable — avoids hashing the entire dataset.

        Args:
            df: OHLCV DataFrame with DatetimeIndex
            filter_cfg_hash: Hash from compute_filter_config_hash()

        Returns:
            40-character SHA1 hex digest
        """
        if df is None or df.empty:
            return f"empty_{filter_cfg_hash}"

        h = hashlib.sha1()

        # Data fingerprint
        h.update(str(df.index[0]).encode())
        h.update(str(df.index[-1]).encode())
        h.update(str(len(df)).encode())
        h.update(pickle.dumps(df["close"].head(50).to_numpy()))
        h.update(pickle.dumps(df["close"].tail(50).to_numpy()))

        # Filter config fingerprint (DEC-026 — prevents cross-config collisions)
        h.update(filter_cfg_hash.encode())

        return h.hexdigest()

    def has(self, cache_id: str) -> bool:
        """
        Check if cache contains entry for given ID.

        Args:
            cache_id: Hash from compute_cache_id()

        Returns:
            True if cached, False otherwise
        """
        return cache_id in self._cache

    def get(self, cache_id: str) -> Dict[str, Any]:
        """
        Retrieve cached indicators for given ID.

        Updates hit/miss counters for observability.

        Args:
            cache_id: Hash from compute_cache_id()

        Returns:
            Dict with keys "indicators" and "indicators_np", or empty dict if not found.
        """
        if cache_id in self._cache:
            self._hits += 1
            return self._cache[cache_id]
        self._misses += 1
        return {}

    def store(
        self,
        cache_id: str,
        indicators: Dict[str, pd.Series],
        indicators_np: Dict[str, np.ndarray]
    ) -> None:
        """
        Store computed indicators in cache.

        Args:
            cache_id: Hash from compute_cache_id()
            indicators: Dict of pandas Series indicators
            indicators_np: Dict of numpy array indicators (for performance)
        """
        self._cache[cache_id] = {
            "indicators": indicators,
            "indicators_np": indicators_np,
        }
        logger.debug(f"Cached {len(indicators)} indicators for ID {cache_id[:8]}...")

    def clear(self) -> None:
        """Clear all cached indicators and reset counters."""
        self._cache.clear()
        self._hits = 0
        self._misses = 0
        logger.debug("Cache cleared")

    def size(self) -> int:
        """Get number of cached datasets."""
        return len(self._cache)

    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics including hit/miss counts.

        Returns:
            Dict with entries, hit_rate, hits, misses, cache_ids
        """
        total = self._hits + self._misses
        hit_rate = (self._hits / total * 100) if total > 0 else 0.0
        return {
            "entries": len(self._cache),
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate_pct": round(hit_rate, 1),
            "cache_ids": list(self._cache.keys()),
        }
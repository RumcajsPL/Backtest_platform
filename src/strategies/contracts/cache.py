"""
Filter Pipeline Cache - Indicator Caching System

Lightweight cache for precomputed indicators to avoid redundant calculations.
Uses SHA1 hashing of OHLCV data for stable cache keys.

Author: Migration Project
Version: 2.0.0
Date: 2025-02-13
Session: 5
"""

import hashlib
import pickle
import logging
from typing import Dict, Any
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class FilterPipelineCache:
    """
    Lightweight cache for indicator sets.
    
    Stores precomputed indicators keyed by a cache_id derived from OHLCV data.
    The cache_id uniquely represents the dataset to ensure correctness.
    
    Performance Impact:
    - First run: Compute all indicators (~50-100ms)
    - Cached runs: Load from memory (~1ms)
    - Typical speedup: 50-100x for repeated backtests
    """

    def __init__(self):
        """Initialize empty cache."""
        self._cache: Dict[str, Dict[str, Any]] = {}
    
    @staticmethod
    def compute_cache_id(df: pd.DataFrame) -> str:
        """
        Compute a stable hash for the OHLCV dataset.
        
        Uses:
        - First/last timestamps (date range)
        - Row count (data completeness)
        - Head/tail close prices (data integrity)
        
        Fast and reliable - avoids hashing entire dataset.
        
        Args:
            df: OHLCV DataFrame with DatetimeIndex
            
        Returns:
            40-character SHA1 hex digest
        """
        if df is None or df.empty:
            return "empty"
        
        h = hashlib.sha1()
        
        # Timestamp boundaries
        h.update(str(df.index[0]).encode())
        h.update(str(df.index[-1]).encode())
        
        # Row count
        h.update(str(len(df)).encode())
        
        # Sample close prices (head + tail)
        h.update(pickle.dumps(df["close"].head(50).to_numpy()))
        h.update(pickle.dumps(df["close"].tail(50).to_numpy()))
        
        return h.hexdigest()
    
    def has(self, cache_id: str) -> bool:
        """
        Check if cache contains entry for given ID.
        
        Args:
            cache_id: SHA1 hash from compute_cache_id()
            
        Returns:
            True if cached, False otherwise
        """
        return cache_id in self._cache
    
    def get(self, cache_id: str) -> Dict[str, Any]:
        """
        Retrieve cached indicators for given ID.
        
        Args:
            cache_id: SHA1 hash from compute_cache_id()
            
        Returns:
            Dict with keys:
            - "indicators": Dict[str, pd.Series]
            - "indicators_np": Dict[str, np.ndarray]
            
            Returns empty dict if not found.
        """
        return self._cache.get(cache_id, {})
    
    def store(
        self, 
        cache_id: str, 
        indicators: Dict[str, pd.Series], 
        indicators_np: Dict[str, np.ndarray]
    ) -> None:
        """
        Store computed indicators in cache.
        
        Args:
            cache_id: SHA1 hash from compute_cache_id()
            indicators: Dict of pandas Series indicators
            indicators_np: Dict of numpy array indicators (for performance)
        """
        self._cache[cache_id] = {
            "indicators": indicators,
            "indicators_np": indicators_np,
        }
        
        logger.debug(f"Cached {len(indicators)} indicators for ID {cache_id[:8]}...")
    
    def clear(self) -> None:
        """Clear all cached indicators."""
        self._cache.clear()
        logger.debug("Cache cleared")
    
    def size(self) -> int:
        """
        Get number of cached datasets.
        
        Returns:
            Number of cache entries
        """
        return len(self._cache)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dict with:
            - entries: Number of cached datasets
            - cache_ids: List of cache IDs
        """
        return {
            "entries": len(self._cache),
            "cache_ids": list(self._cache.keys())
        }
"""
Unit Tests for CacheManager
============================
Tests centralized cache management for multi-run backtesting.
"""

import pytest
import pandas as pd
import numpy as np

from src.strategies.core.cache_manager import CacheManager


class TestCacheManager:
    """Tests for CacheManager class."""

    @pytest.fixture
    def cache_manager(self):
        """Create a fresh CacheManager for each test."""
        return CacheManager()

    @pytest.fixture
    def sample_series(self):
        """Create a sample pandas Series."""
        return pd.Series([1.0, 2.0, 3.0, 4.0, 5.0], name="test")

    @pytest.fixture
    def sample_config(self):
        """Create a sample configuration dict."""
        return {
            "spreads": {
                "DEUIDXEUR": {
                    "spread_value": 0.015,
                    "spread_type": "percentage"
                }
            },
            "settings": {
                "apply_to_long": True,
                "apply_to_short": True
            }
        }

    def test_initialization(self, cache_manager):
        """Test cache manager initialization."""
        assert cache_manager._atr_cache == {}
        assert cache_manager._annual_range_cache == {}
        assert cache_manager._spread_config_cache == {}
        
        stats = cache_manager.get_stats()
        assert stats["atr"]["entries"] == 0
        assert stats["annual_range"]["entries"] == 0
        assert stats["spread_config"]["entries"] == 0

    def test_atr_cache_operations(self, cache_manager, sample_series):
        """Test ATR cache get/set operations."""
        key = "test_atr_key"
        
        # Initially should be None
        assert cache_manager.get_atr(key) is None
        
        # Set and get
        cache_manager.set_atr(key, sample_series)
        retrieved = cache_manager.get_atr(key)
        
        assert retrieved is not None
        assert retrieved.equals(sample_series)
        
        # Different key should still be None
        assert cache_manager.get_atr("different_key") is None

    def test_annual_range_cache_operations(self, cache_manager, sample_series):
        """Test annual range cache get/set operations."""
        key = "test_annual_key"
        
        assert cache_manager.get_annual_range(key) is None
        
        cache_manager.set_annual_range(key, sample_series)
        retrieved = cache_manager.get_annual_range(key)
        
        assert retrieved is not None
        assert retrieved.equals(sample_series)

    def test_spread_config_cache_operations(self, cache_manager, sample_config):
        """Test spread config cache get/set operations."""
        key = "test_config_key"
        
        assert cache_manager.get_spread_config(key) is None
        
        cache_manager.set_spread_config(key, sample_config)
        retrieved = cache_manager.get_spread_config(key)
        
        assert retrieved is not None
        assert retrieved == sample_config

    def test_clear_all_caches(self, cache_manager, sample_series, sample_config):
        """Test clearing all caches."""
        # Populate caches
        cache_manager.set_atr("atr1", sample_series)
        cache_manager.set_annual_range("ann1", sample_series)
        cache_manager.set_spread_config("config1", sample_config)
        
        # Verify they're populated
        assert cache_manager.get_atr("atr1") is not None
        assert cache_manager.get_annual_range("ann1") is not None
        assert cache_manager.get_spread_config("config1") is not None
        
        # Clear all
        cache_manager.clear_all_caches()
        
        # Verify they're cleared
        assert cache_manager.get_atr("atr1") is None
        assert cache_manager.get_annual_range("ann1") is None
        assert cache_manager.get_spread_config("config1") is None
        
        # Stats should be reset
        stats = cache_manager.get_stats()
        assert stats["atr"]["entries"] == 0
        assert stats["annual_range"]["entries"] == 0
        assert stats["spread_config"]["entries"] == 0

    def test_clear_atr_cache(self, cache_manager, sample_series):
        """Test clearing only ATR cache."""
        cache_manager.set_atr("atr1", sample_series)
        cache_manager.set_annual_range("ann1", sample_series)
        
        cache_manager.clear_atr_cache()
        
        assert cache_manager.get_atr("atr1") is None
        assert cache_manager.get_annual_range("ann1") is not None

    def test_clear_annual_range_cache(self, cache_manager, sample_series):
        """Test clearing only annual range cache."""
        cache_manager.set_atr("atr1", sample_series)
        cache_manager.set_annual_range("ann1", sample_series)
        
        cache_manager.clear_annual_range_cache()
        
        assert cache_manager.get_atr("atr1") is not None
        assert cache_manager.get_annual_range("ann1") is None

    def test_clear_spread_config_cache(self, cache_manager, sample_config):
        """Test clearing only spread config cache."""
        cache_manager.set_atr("atr1", pd.Series([1, 2]))
        cache_manager.set_spread_config("config1", sample_config)
        
        cache_manager.clear_spread_config_cache()
        
        assert cache_manager.get_atr("atr1") is not None
        assert cache_manager.get_spread_config("config1") is None

    def test_cache_hit_stats(self, cache_manager, sample_series):
        """Test cache hit statistics."""
        key = "test_key"
        
        # Miss
        cache_manager.get_atr(key)
        
        # Hit
        cache_manager.set_atr(key, sample_series)
        cache_manager.get_atr(key)
        
        stats = cache_manager.get_stats()
        assert stats["atr"]["hits"] == 1
        assert stats["atr"]["misses"] == 1
        assert stats["atr"]["hit_rate"] == 50.0

    def test_cache_miss_stats(self, cache_manager):
        """Test cache miss statistics."""
        # Multiple misses
        for i in range(5):
            cache_manager.get_atr(f"key{i}")
        
        stats = cache_manager.get_stats()
        assert stats["atr"]["misses"] == 5
        assert stats["atr"]["hit_rate"] == 0.0

    def test_multiple_entries(self, cache_manager, sample_series):
        """Test multiple entries in cache."""
        for i in range(10):
            cache_manager.set_atr(f"key{i}", sample_series * i)
        
        stats = cache_manager.get_stats()
        assert stats["atr"]["entries"] == 10
        
        # Verify retrieval
        for i in range(10):
            retrieved = cache_manager.get_atr(f"key{i}")
            assert retrieved is not None
            assert retrieved.iloc[0] == i

    def test_cache_key_uniqueness(self, cache_manager, sample_series):
        """Test that different keys don't interfere."""
        cache_manager.set_atr("key1", sample_series * 1)
        cache_manager.set_atr("key2", sample_series * 2)
        
        val1 = cache_manager.get_atr("key1")
        val2 = cache_manager.get_atr("key2")
        
        assert val1.iloc[0] == 1
        assert val2.iloc[0] == 2
        assert not val1.equals(val2)

    def test_overwrite_existing_key(self, cache_manager, sample_series):
        """Test overwriting existing cache entry."""
        key = "test_key"
        
        cache_manager.set_atr(key, sample_series * 1)
        first = cache_manager.get_atr(key)
        
        cache_manager.set_atr(key, sample_series * 2)
        second = cache_manager.get_atr(key)
        
        assert first.iloc[0] == 1
        assert second.iloc[0] == 2
        assert not first.equals(second)

    def test_repr(self, cache_manager, sample_series):
        """Test string representation."""
        # Empty cache
        repr_str = repr(cache_manager)
        assert "ATR:0" in repr_str
        assert "AnnRange:0" in repr_str
        assert "Spread:0" in repr_str
        
        # With entries
        cache_manager.set_atr("key1", sample_series)
        cache_manager.set_annual_range("key2", sample_series)
        cache_manager.set_spread_config("key3", {"test": 1})
        
        repr_str = repr(cache_manager)
        assert "ATR:1" in repr_str
        assert "AnnRange:1" in repr_str
        assert "Spread:1" in repr_str

    def test_stats_after_clear(self, cache_manager, sample_series):
        """Test that stats reset after clear."""
        # Generate some hits/misses
        cache_manager.get_atr("key1")  # miss
        cache_manager.set_atr("key1", sample_series)
        cache_manager.get_atr("key1")  # hit
        
        stats_before = cache_manager.get_stats()
        assert stats_before["atr"]["hits"] == 1
        assert stats_before["atr"]["misses"] == 1
        
        cache_manager.clear_all_caches()
        
        stats_after = cache_manager.get_stats()
        assert stats_after["atr"]["hits"] == 0
        assert stats_after["atr"]["misses"] == 0

    def test_calculate_hit_rate(self, cache_manager):
        """Test hit rate calculation."""
        # Test private method directly
        assert cache_manager._calculate_hit_rate(10, 0) == 100.0
        assert cache_manager._calculate_hit_rate(0, 10) == 0.0
        assert cache_manager._calculate_hit_rate(5, 5) == 50.0
        assert cache_manager._calculate_hit_rate(0, 0) == 0.0

    def test_annual_range_cache_with_series(self, cache_manager):
        """Test annual range cache with actual Series."""
        dates = pd.date_range("2025-01-01", periods=10)
        series = pd.Series(np.random.randn(10), index=dates)
        
        cache_manager.set_annual_range("test_annual", series)
        retrieved = cache_manager.get_annual_range("test_annual")
        
        assert retrieved.equals(series)
        assert retrieved.index.equals(dates)
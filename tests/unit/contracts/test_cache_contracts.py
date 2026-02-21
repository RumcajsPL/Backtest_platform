"""
Unit Tests for Cache Contracts
================================
Tests FilterPipelineCache and related functionality.
"""

import pytest
import pandas as pd
import numpy as np
import hashlib
import json

from src.strategies.contracts.cache import FilterPipelineCache


class TestFilterPipelineCache:
    """Tests for FilterPipelineCache class."""

    @pytest.fixture
    def cache(self):
        """Create fresh cache instance."""
        return FilterPipelineCache()

    @pytest.fixture
    def sample_df(self):
        """Sample DataFrame for testing."""
        dates = pd.date_range(start="2025-01-01", periods=100, freq="1min")
        return pd.DataFrame({
            "open": np.random.randn(100) * 10 + 100,
            "high": np.random.randn(100) * 10 + 102,
            "low": np.random.randn(100) * 10 + 98,
            "close": np.random.randn(100) * 10 + 100,
            "volume": np.random.randint(100, 1000, 100)
        }, index=dates)

    @pytest.fixture
    def sample_filter_configs(self):
        """Sample filter configurations."""
        return {
            "rsi_filter": {
                "enabled": True,
                "config": {"length": 14, "overbought": 70, "oversold": 30}
            },
            "adx_filter": {
                "enabled": True,
                "config": {"length": 14, "threshold": 25}
            }
        }

    def test_initialization(self, cache):
        """Test cache initialization."""
        assert cache.size() == 0
        assert cache._hits == 0
        assert cache._misses == 0

    def test_compute_filter_config_hash(self, cache, sample_filter_configs):
        """Test computing filter config hash."""
        hash1 = cache.compute_filter_config_hash(sample_filter_configs)
        
        # Same config should produce same hash
        hash2 = cache.compute_filter_config_hash(sample_filter_configs)
        assert hash1 == hash2
        assert len(hash1) == 12  # 12-character MD5 digest

    def test_filter_config_hash_excludes_disabled(self, cache, sample_filter_configs):
        """Test that disabled filters are excluded from hash (DEC-026)."""
        hash_all_enabled = cache.compute_filter_config_hash(sample_filter_configs)
        
        # Disable one filter
        disabled_configs = sample_filter_configs.copy()
        disabled_configs["rsi_filter"]["enabled"] = False
        
        hash_one_disabled = cache.compute_filter_config_hash(disabled_configs)
        
        # Hashes should be different
        assert hash_all_enabled != hash_one_disabled

    def test_filter_config_hash_order_stable(self, cache, sample_filter_configs):
        """Test that hash is stable regardless of dict order."""
        # Create same config with different order
        import json
        
        # Hash with original order
        hash1 = cache.compute_filter_config_hash(sample_filter_configs)
        
        # Reverse the items
        reversed_items = list(sample_filter_configs.items())[::-1]
        reversed_configs = dict(reversed_items)
        
        # Should be same because we sort keys
        hash2 = cache.compute_filter_config_hash(reversed_configs)
        assert hash1 == hash2

    def test_compute_cache_id(self, cache, sample_df):
        """Test computing cache ID."""
        filter_hash = "abc123def456"
        
        cache_id1 = cache.compute_cache_id(sample_df, filter_hash)
        cache_id2 = cache.compute_cache_id(sample_df, filter_hash)
        
        # Same inputs should produce same ID
        assert cache_id1 == cache_id2
        assert len(cache_id1) == 40  # SHA1 hex digest

    def test_cache_id_with_empty_df(self, cache):
        """Test cache ID with empty DataFrame."""
        empty_df = pd.DataFrame()
        filter_hash = "abc123"
        
        cache_id = cache.compute_cache_id(empty_df, filter_hash)
        assert cache_id == f"empty_{filter_hash}"

    def test_cache_id_with_different_filter_hash(self, cache, sample_df):
        """Test that different filter hashes produce different cache IDs (DEC-026)."""
        hash1 = "abc123def456"
        hash2 = "xyz789uvw123"
        
        id1 = cache.compute_cache_id(sample_df, hash1)
        id2 = cache.compute_cache_id(sample_df, hash2)
        
        assert id1 != id2

    def test_cache_id_with_different_data(self, cache, sample_df):
        """Test that different data produces different cache IDs."""
        filter_hash = "abc123"
        
        # Modify data slightly
        df2 = sample_df.copy()
        df2.iloc[-1, df2.columns.get_loc("close")] += 1.0
        
        id1 = cache.compute_cache_id(sample_df, filter_hash)
        id2 = cache.compute_cache_id(df2, filter_hash)
        
        assert id1 != id2

    def test_store_and_get(self, cache, sample_df):
        """Test storing and retrieving from cache."""
        filter_hash = "abc123"
        cache_id = cache.compute_cache_id(sample_df, filter_hash)
        
        indicators = {
            "rsi": pd.Series([1, 2, 3], index=sample_df.index),
            "adx": pd.Series([4, 5, 6], index=sample_df.index)
        }
        indicators_np = {
            "rsi": np.array([1, 2, 3]),
            "adx": np.array([4, 5, 6])
        }
        
        # Store
        cache.store(cache_id, indicators, indicators_np)
        
        # Check existence
        assert cache.has(cache_id) is True
        
        # Retrieve
        retrieved = cache.get(cache_id)
        assert "indicators" in retrieved
        assert "indicators_np" in retrieved
        assert retrieved["indicators"]["rsi"].equals(indicators["rsi"])
        assert np.array_equal(retrieved["indicators_np"]["rsi"], indicators_np["rsi"])

    def test_get_missing(self, cache):
        """Test getting non-existent cache entry."""
        result = cache.get("nonexistent")
        assert result == {}
        
        # Check stats updated
        assert cache._misses == 1
        assert cache._hits == 0

    def test_has_missing(self, cache):
        """Test checking non-existent cache entry."""
        assert cache.has("nonexistent") is False

    def test_hit_miss_counters(self, cache, sample_df):
        """Test hit/miss counters."""
        filter_hash = "abc123"
        cache_id = cache.compute_cache_id(sample_df, filter_hash)
        
        # Miss
        cache.get(cache_id)
        assert cache._misses == 1
        assert cache._hits == 0
        
        # Store and hit
        indicators = {"test": pd.Series([1, 2, 3])}
        indicators_np = {"test": np.array([1, 2, 3])}
        cache.store(cache_id, indicators, indicators_np)
        
        cache.get(cache_id)  # Hit
        assert cache._hits == 1
        assert cache._misses == 1

    def test_clear(self, cache, sample_df):
        """Test clearing cache."""
        filter_hash = "abc123"
        cache_id = cache.compute_cache_id(sample_df, filter_hash)
        
        indicators = {"test": pd.Series([1, 2, 3])}
        indicators_np = {"test": np.array([1, 2, 3])}
        cache.store(cache_id, indicators, indicators_np)
        
        assert cache.size() == 1
        assert cache._hits == 0
        assert cache._misses == 0
        
        cache.clear()
        
        assert cache.size() == 0
        assert cache._hits == 0
        assert cache._misses == 0
        assert cache.has(cache_id) is False

    def test_size(self, cache, sample_df):
        """Test size method."""
        assert cache.size() == 0
        
        filter_hash = "abc123"
        cache_id = cache.compute_cache_id(sample_df, filter_hash)
        
        indicators = {"test": pd.Series([1, 2, 3])}
        indicators_np = {"test": np.array([1, 2, 3])}
        cache.store(cache_id, indicators, indicators_np)
        
        assert cache.size() == 1
        
        # Store another
        cache_id2 = cache.compute_cache_id(sample_df, "different_hash")
        cache.store(cache_id2, indicators, indicators_np)
        
        assert cache.size() == 2

    def test_get_stats(self, cache, sample_df):
        """Test getting cache statistics."""
        filter_hash = "abc123"
        cache_id = cache.compute_cache_id(sample_df, filter_hash)
        
        # Empty cache stats
        stats = cache.get_stats()
        assert stats["entries"] == 0
        assert stats["hits"] == 0
        assert stats["misses"] == 0
        assert stats["hit_rate_pct"] == 0.0
        assert stats["cache_ids"] == []
        
        # Add some entries and hits/misses
        indicators = {"test": pd.Series([1, 2, 3])}
        indicators_np = {"test": np.array([1, 2, 3])}
        cache.store(cache_id, indicators, indicators_np)
        
        # Miss
        cache.get("nonexistent")
        
        # Hit
        cache.get(cache_id)
        
        stats = cache.get_stats()
        assert stats["entries"] == 1
        assert stats["hits"] == 1
        assert stats["misses"] == 1
        assert stats["hit_rate_pct"] == 50.0
        assert cache_id in stats["cache_ids"]

    def test_multiple_entries(self, cache, sample_df):
        """Test multiple cache entries."""
        # Create multiple cache IDs
        ids = []
        for i in range(5):
            filter_hash = f"hash{i}"
            cache_id = cache.compute_cache_id(sample_df, filter_hash)
            ids.append(cache_id)
            
            indicators = {f"indicator{i}": pd.Series([i, i+1, i+2])}
            indicators_np = {f"indicator{i}": np.array([i, i+1, i+2])}
            cache.store(cache_id, indicators, indicators_np)
        
        assert cache.size() == 5
        
        # Retrieve each
        for i, cache_id in enumerate(ids):
            retrieved = cache.get(cache_id)
            assert f"indicator{i}" in retrieved["indicators"]
            assert f"indicator{i}" in retrieved["indicators_np"]

    def test_cache_persistence_across_calls(self, cache, sample_df):
        """Test that cache persists across method calls."""
        filter_hash = "abc123"
        cache_id = cache.compute_cache_id(sample_df, filter_hash)
        
        indicators = {"test": pd.Series([1, 2, 3])}
        indicators_np = {"test": np.array([1, 2, 3])}
        
        # Store
        cache.store(cache_id, indicators, indicators_np)
        
        # Should still be there after other operations
        cache.get("nonexistent")  # Miss
        cache.get_stats()
        
        assert cache.has(cache_id) is True
        retrieved = cache.get(cache_id)
        assert retrieved["indicators"]["test"].iloc[0] == 1

    def test_compute_cache_id_includes_filter_hash(self, cache, sample_df):
        """Test that cache ID includes filter hash (DEC-026 verification)."""
        id1 = cache.compute_cache_id(sample_df, "hash1")
        id2 = cache.compute_cache_id(sample_df, "hash2")
        
        assert id1 != id2
"""
Unit Tests for DataLoader
==========================
Tests data loading, caching, and validation.
DataLoader trusts the already-validated StrategyConfig - it does NOT validate config.
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import pickle
import hashlib
from dataclasses import asdict

from src.strategies.specific.modules.data_loader import DataLoader
from src.strategies.contracts.data_contracts import (
    DataBundle,
    DataInfo,
    DataValidationResult,
    DateRange,
    DataFileConfig
)
from src.config.config_schema import StrategyConfig


class TestDataLoader:
    """Tests for DataLoader class."""

    @pytest.fixture
    def sample_csv_data(self, tmp_path):
        """Create a sample CSV file with valid OHLC data."""
        dates = pd.date_range(start="2025-01-01 00:00:00", periods=100, freq="1min")
        
        # Generate realistic price movements
        np.random.seed(42)
        prices = 100 + np.cumsum(np.random.randn(100) * 0.1)
        
        df = pd.DataFrame({
            "timestamp": dates.strftime("%Y-%m-%d %H:%M:%S"),
            "open": prices * 0.999,
            "high": prices * 1.002,
            "low": prices * 0.998,
            "close": prices,
            "volume": np.random.randint(100, 1000, 100)
        })
        
        # Ensure OHLC integrity
        df["high"] = df[["open", "high", "close"]].max(axis=1)
        df["low"] = df[["open", "low", "close"]].min(axis=1)
        
        file_path = tmp_path / "test_data.csv"
        df.to_csv(file_path, index=False)
        return file_path

    @pytest.fixture
    def sample_parquet_data(self, tmp_path):
        """Create a sample Parquet file with valid OHLC data."""
        dates = pd.date_range(start="2025-01-01 00:00:00", periods=100, freq="1min")
        
        # Generate realistic price movements
        np.random.seed(42)
        prices = 100 + np.cumsum(np.random.randn(100) * 0.1)
        
        df = pd.DataFrame({
            "open": prices * 0.999,
            "high": prices * 1.002,
            "low": prices * 0.998,
            "close": prices,
            "volume": np.random.randint(100, 1000, 100)
        }, index=dates)
        df.index.name = "timestamp"
        
        # Ensure OHLC integrity
        df["high"] = df[["open", "high", "close"]].max(axis=1)
        df["low"] = df[["open", "low", "close"]].min(axis=1)
        
        file_path = tmp_path / "test_data.parquet"
        df.to_parquet(file_path)
        return file_path

    @pytest.fixture
    def config_with_paths(self, base_config_dict, sample_parquet_data):
        """Create StrategyConfig with valid file paths and date range."""
        from src.config.config_schema import StrategyConfig
        
        # Create a copy to avoid modifying the original
        config_dict = base_config_dict.copy()
        
        # Set up paths
        config_dict["data"]["paths"] = {
            "strategy_ohlcv": str(sample_parquet_data),
            "htf_ohlcv": str(sample_parquet_data),
            "ltf_ohlcv": str(sample_parquet_data),
            "artf_ohlcv": str(sample_parquet_data)
        }
        
        # Set date range within the data range - note: pandas slicing includes both endpoints
        # So from 00:00 to 01:00 inclusive gives 61 minutes
        config_dict["data"]["date_range"] = {
            "start": "2025-01-01 00:00:00",
            "end": "2025-01-01 01:00:00"  # 61 minutes (00:00 through 01:00)
        }
        
        return StrategyConfig.from_dict(config_dict)

    @pytest.fixture
    def config_without_date_range(self, base_config_dict, sample_parquet_data):
        """Create StrategyConfig without date range for tests that don't need slicing."""
        from src.config.config_schema import StrategyConfig
        
        # Create a copy to avoid modifying the original
        config_dict = base_config_dict.copy()
        
        # Set up paths
        config_dict["data"]["paths"] = {
            "strategy_ohlcv": str(sample_parquet_data),
            "htf_ohlcv": None,
            "ltf_ohlcv": None,
            "artf_ohlcv": None
        }
        
        # Set date_range to None - this is valid in the schema
        config_dict["data"]["date_range"] = None
        
        return StrategyConfig.from_dict(config_dict)

    @pytest.fixture
    def config_with_full_date_range(self, base_config_dict, sample_parquet_data):
        """Create StrategyConfig with date range covering the entire data."""
        from src.config.config_schema import StrategyConfig
        
        # Create a copy to avoid modifying the original
        config_dict = base_config_dict.copy()
        
        # Set up paths
        config_dict["data"]["paths"] = {
            "strategy_ohlcv": str(sample_parquet_data),
            "htf_ohlcv": str(sample_parquet_data),
            "ltf_ohlcv": str(sample_parquet_data),
            "artf_ohlcv": str(sample_parquet_data)
        }
        
        # Set date range covering the entire data period
        config_dict["data"]["date_range"] = {
            "start": "2025-01-01 00:00:00",
            "end": "2025-01-01 01:39:00"  # Last timestamp in the data
        }
        
        return StrategyConfig.from_dict(config_dict)

    def test_initialization_with_valid_config(self, config_with_paths):
        """Test initializing DataLoader with valid StrategyConfig."""
        loader = DataLoader(config=config_with_paths, mode="core")
        
        assert loader.config == config_with_paths
        assert loader.mode == "core"
        assert loader.data_config is not None
        assert loader.data_config.strategy_data is not None
        assert loader.cache_dir.exists()

    def test_initialization_analytics_mode(self, config_with_paths):
        """Test initialization in analytics mode."""
        loader = DataLoader(config=config_with_paths, mode="analytics")
        
        assert loader.mode == "analytics"
        assert loader._verbose is True

    def test_build_data_config(self, config_with_paths, sample_parquet_data):
        """Test building DataConfig from StrategyConfig."""
        loader = DataLoader(config=config_with_paths, mode="core")
        
        data_config = loader.data_config
        
        assert data_config.strategy_data.path == sample_parquet_data
        assert data_config.strategy_data.format == "parquet"
        assert data_config.htf_data is not None
        assert data_config.ltf_data is not None
        assert data_config.artf_data is not None
        
        # Check date range
        assert data_config.date_range is not None
        assert data_config.date_range.start.year == 2025
        assert data_config.date_range.end.year == 2025

    def test_build_data_config_missing_paths(self, base_config_dict):
        """Test building DataConfig with missing optional paths."""
        from src.config.config_schema import StrategyConfig
        
        # Remove optional paths
        base_config_dict["data"]["paths"]["htf_ohlcv"] = None
        base_config_dict["data"]["paths"]["ltf_ohlcv"] = None
        base_config_dict["data"]["paths"]["artf_ohlcv"] = None
        
        config = StrategyConfig.from_dict(base_config_dict)
        loader = DataLoader(config=config, mode="core")
        
        assert loader.data_config.htf_data is None
        assert loader.data_config.ltf_data is None
        assert loader.data_config.artf_data is None

    def test_get_cache_key(self, config_with_paths, sample_parquet_data):
        """Test cache key generation."""
        loader = DataLoader(config=config_with_paths, mode="core")
        
        # Test without date range
        key1 = loader._get_cache_key(sample_parquet_data)
        assert key1 is not None
        assert isinstance(key1, str)
        assert len(key1) == 32  # MD5 hex digest
        
        # Test with date range
        date_range = DateRange(
            start=datetime(2025, 1, 1),
            end=datetime(2025, 1, 31)
        )
        key2 = loader._get_cache_key(sample_parquet_data, date_range)
        assert key2 is not None
        assert key2 != key1
        
        # Same inputs should produce same key
        key3 = loader._get_cache_key(sample_parquet_data, date_range)
        assert key2 == key3

    def test_cache_key_with_content_hash(self, config_with_paths, sample_parquet_data):
        """Test cache key generation with content hash."""
        loader = DataLoader(config=config_with_paths, mode="analytics")
        
        key = loader._get_cache_key(sample_parquet_data, use_content_hash=True)
        assert key is not None
        assert len(key) == 32

    def test_cache_key_nonexistent_file(self, config_with_paths):
        """Test cache key for nonexistent file."""
        loader = DataLoader(config=config_with_paths, mode="core")
        
        key = loader._get_cache_key(Path("/nonexistent/file.parquet"))
        assert key is None

    def test_load_cached_data(self, config_with_paths, tmp_path):
        """Test loading data from cache."""
        loader = DataLoader(config=config_with_paths, mode="core")
        
        # Create test cache file
        test_df = pd.DataFrame({"test": [1, 2, 3]})
        cache_key = "test_cache_key_123"
        cache_file = loader.cache_dir / f"{cache_key}.pkl"
        
        with open(cache_file, "wb") as f:
            pickle.dump(test_df, f)
        
        # Load from cache
        loaded = loader._load_cached_data(cache_key)
        assert loaded is not None
        assert loaded.equals(test_df)
        
        # Invalid key returns None
        assert loader._load_cached_data("invalid_key") is None

    def test_load_cached_data_corrupted(self, config_with_paths, tmp_path, caplog):
        """Test handling of corrupted cache file."""
        loader = DataLoader(config=config_with_paths, mode="analytics")
        
        cache_key = "corrupted_key"
        cache_file = loader.cache_dir / f"{cache_key}.pkl"
        
        # Write invalid data
        cache_file.write_text("not a pickle file")
        
        # Should handle gracefully
        with caplog.at_level("WARNING"):
            loaded = loader._load_cached_data(cache_key)
        
        assert loaded is None
        assert "Cache corrupted" in caplog.text
        assert not cache_file.exists()  # Should be deleted

    def test_save_to_cache(self, config_with_paths):
        """Test saving data to cache."""
        loader = DataLoader(config=config_with_paths, mode="core")
        
        test_df = pd.DataFrame({"test": [1, 2, 3]})
        cache_key = "test_save_key"
        
        loader._save_to_cache(cache_key, test_df)
        
        cache_file = loader.cache_dir / f"{cache_key}.pkl"
        assert cache_file.exists()
        
        # Verify content
        with open(cache_file, "rb") as f:
            loaded = pickle.load(f)
        assert loaded.equals(test_df)

    def test_load_file_with_cache_csv(self, config_without_date_range, tmp_path):
        """Test loading CSV file with caching."""
        # Create properly formatted CSV with timestamp column
        dates = pd.date_range(start="2025-01-01 00:00:00", periods=100, freq="1min")
        np.random.seed(42)
        prices = 100 + np.cumsum(np.random.randn(100) * 0.1)
        
        df = pd.DataFrame({
            "timestamp": dates.strftime("%Y-%m-%d %H:%M:%S"),
            "open": prices * 0.999,
            "high": prices * 1.002,
            "low": prices * 0.998,
            "close": prices,
            "volume": np.random.randint(100, 1000, 100)
        })
        
        # Ensure OHLC integrity
        df["high"] = df[["open", "high", "close"]].max(axis=1)
        df["low"] = df[["open", "low", "close"]].min(axis=1)
        
        csv_path = tmp_path / "test_data.csv"
        df.to_csv(csv_path, index=False)
        
        loader = DataLoader(config=config_without_date_range, mode="analytics")
        
        file_config = DataFileConfig(
            path=csv_path,
            format="csv"
        )
        
        # First load - cache miss
        df1 = loader._load_file_with_cache(file_config, "test_csv")
        assert len(df1) == 100
        assert isinstance(df1.index, pd.DatetimeIndex)
        assert all(col in df1.columns for col in ["open", "high", "low", "close", "volume"])
        
        # Should have cache miss stats
        assert loader._cache_misses > 0
        
        # Second load - cache hit
        df2 = loader._load_file_with_cache(file_config, "test_csv")
        assert loader._cache_hits > 0
        assert df1.equals(df2)

    def test_load_file_with_cache_parquet(self, config_without_date_range, sample_parquet_data):
        """Test loading Parquet file with caching."""
        loader = DataLoader(config=config_without_date_range, mode="analytics")
        
        file_config = DataFileConfig(
            path=sample_parquet_data,
            format="parquet"
        )
        
        df = loader._load_file_with_cache(file_config, "test_parquet")
        
        assert len(df) == 100
        assert isinstance(df.index, pd.DatetimeIndex)
        assert df.index.name == "timestamp"
        assert all(col in df.columns for col in ["open", "high", "low", "close", "volume"])

    def test_load_file_nonexistent(self, config_with_paths, tmp_path):
        """Test loading nonexistent file raises error."""
        loader = DataLoader(config=config_with_paths, mode="core")
        
        file_config = DataFileConfig(
            path=tmp_path / "nonexistent.parquet",
            format="parquet"
        )
        
        with pytest.raises(FileNotFoundError, match="file not found"):
            loader._load_file_with_cache(file_config, "test")

    def test_load_file_unsupported_format(self, config_with_paths, tmp_path):
        """Test that unsupported format is caught by DataFileConfig validation."""
        unsupported = tmp_path / "test.txt"
        unsupported.write_text("dummy")
        
        # This should raise from DataFileConfig, not from _load_file_with_cache
        with pytest.raises(ValueError, match="Unsupported format"):
            DataFileConfig(path=unsupported, format="txt")

    def test_sanitize_df(self, config_with_paths):
        """Test DataFrame sanitization."""
        loader = DataLoader(config=config_with_paths, mode="analytics")
        
        # Create DataFrame with inf and NaN
        df = pd.DataFrame({
            "open": [100.0, np.inf, 102.0, np.nan],
            "high": [101.0, 102.0, np.inf, 104.0],
            "low": [99.0, 98.0, 97.0, np.nan],
            "close": [100.5, np.inf, 101.5, 103.0]
        })
        
        sanitized = loader._sanitize_df(df, "test")
        
        # Should have no inf or nan
        assert not sanitized.isnull().any().any()
        assert not np.isinf(sanitized).any().any()
        
        # Values should be filled
        assert sanitized.loc[1, "open"] is not None
        assert sanitized.loc[3, "low"] is not None

    def test_validate_dataframe_valid(self, config_with_paths):
        """Test DataFrame validation with valid data."""
        loader = DataLoader(config=config_with_paths, mode="core")
        
        dates = pd.date_range("2025-01-01", periods=10, freq="1min")
        df = pd.DataFrame({
            "open": [100.0] * 10,
            "high": [101.0] * 10,
            "low": [99.0] * 10,
            "close": [100.5] * 10,
            "volume": [1000] * 10
        }, index=dates)
        
        result = loader._validate_dataframe(df, "test")
        
        assert isinstance(result, DataValidationResult)
        assert result.is_valid is True
        assert len(result.errors) == 0
        assert result.checks["has_data"] == True
        assert result.checks["ohlc_columns"] == True
        assert result.checks["positive_prices"] == True
        assert result.checks["high_low_valid"] == True

    def test_validate_dataframe_empty(self, config_with_paths):
        """Test validation with empty DataFrame."""
        loader = DataLoader(config=config_with_paths, mode="core")
        
        df = pd.DataFrame()
        
        result = loader._validate_dataframe(df, "test")
        
        assert result.is_valid is False
        assert len(result.errors) > 0
        assert any("empty" in e.lower() for e in result.errors)

    def test_validate_dataframe_missing_columns(self, config_with_paths):
        """Test validation with missing OHLC columns."""
        loader = DataLoader(config=config_with_paths, mode="core")
        
        dates = pd.date_range("2025-01-01", periods=10)
        df = pd.DataFrame({
            "open": [100.0] * 10,
            "close": [100.5] * 10
        }, index=dates)
        
        result = loader._validate_dataframe(df, "test")
        
        assert result.is_valid is False
        assert not result.checks["ohlc_columns"]
        assert any("Missing OHLC" in e for e in result.errors)

    def test_validate_dataframe_invalid_high_low(self, config_with_paths):
        """Test validation with high < low."""
        loader = DataLoader(config=config_with_paths, mode="core")
        
        dates = pd.date_range("2025-01-01", periods=10)
        df = pd.DataFrame({
            "open": [100.0] * 10,
            "high": [95.0] * 10,  # Lower than low
            "low": [105.0] * 10,
            "close": [100.0] * 10
        }, index=dates)
        
        result = loader._validate_dataframe(df, "test")
        
        assert result.is_valid is False
        assert not result.checks["high_low_valid"]
        assert any("high < low" in e.lower() for e in result.errors)

    def test_validate_dataframe_negative_prices(self, config_with_paths):
        """Test validation with negative prices."""
        loader = DataLoader(config=config_with_paths, mode="core")
        
        dates = pd.date_range("2025-01-01", periods=10)
        df = pd.DataFrame({
            "open": [100.0] * 10,
            "high": [101.0] * 10,
            "low": [-1.0] * 10,  # Negative
            "close": [100.0] * 10
        }, index=dates)
        
        result = loader._validate_dataframe(df, "test")
        
        assert result.is_valid is False
        assert not result.checks["positive_prices"]
        assert any("non-positive" in e.lower() for e in result.errors)

    def test_load_data_full(self, config_with_full_date_range):
        """Test full data loading with all files."""
        loader = DataLoader(config=config_with_full_date_range, mode="analytics")
        
        bundle = loader.load_data()
        
        assert isinstance(bundle, DataBundle)
        assert bundle.full is not None
        assert bundle.strategy is not None
        assert bundle.htf is not None
        assert bundle.ltf is not None
        assert bundle.artf is not None
        assert bundle.info is not None
        assert bundle.validation.is_valid is True
        
        # Check date slicing
        start_date = config_with_full_date_range.data.date_range.start
        end_date = config_with_full_date_range.data.date_range.end
        
        # Strategy data should be within date range
        assert bundle.strategy.index.min() >= pd.Timestamp(start_date)
        assert bundle.strategy.index.max() <= pd.Timestamp(end_date)
        
        # Should have all 100 rows since date range covers full data
        assert len(bundle.strategy) == 100

    def test_load_data_missing_optional(self, base_config_dict, sample_parquet_data):
        """Test loading with missing optional files."""
        from src.config.config_schema import StrategyConfig
        
        # Set only strategy data
        base_config_dict["data"]["paths"]["strategy_ohlcv"] = str(sample_parquet_data)
        base_config_dict["data"]["paths"]["htf_ohlcv"] = None
        base_config_dict["data"]["paths"]["ltf_ohlcv"] = None
        base_config_dict["data"]["paths"]["artf_ohlcv"] = None
        
        config = StrategyConfig.from_dict(base_config_dict)
        loader = DataLoader(config=config, mode="core")
        
        bundle = loader.load_data()
        
        assert bundle.htf is None
        assert bundle.ltf is None
        assert bundle.artf is None
        assert bundle.strategy is not None

    def test_load_data_with_date_range_slicing(self, config_with_paths):
        """Test date range slicing."""
        loader = DataLoader(config=config_with_paths, mode="core")
        
        bundle = loader.load_data()
        
        # All data files should be sliced (except ARTF)
        assert len(bundle.strategy) <= len(bundle.full)
        
        # Strategy data should be exactly the sliced portion
        # Note: pandas slicing includes both endpoints, so from 00:00 to 01:00 inclusive gives 61 minutes
        expected_rows = 61  # 00:00 through 01:00 inclusive
        assert len(bundle.strategy) == expected_rows
        
        # Verify the date range
        assert bundle.strategy.index[0] == pd.Timestamp("2025-01-01 00:00:00")
        assert bundle.strategy.index[-1] == pd.Timestamp("2025-01-01 01:00:00")
        
        # ARTF should have full history
        if bundle.artf is not None:
            assert len(bundle.artf) >= len(bundle.strategy)

    def test_cache_stats_property(self, config_without_date_range, sample_parquet_data):
        """Test cache statistics property."""
        loader = DataLoader(config=config_without_date_range, mode="analytics")
        
        file_config = DataFileConfig(
            path=sample_parquet_data,
            format="parquet"
        )
        
        # First load - miss
        loader._load_file_with_cache(file_config, "test")
        
        # Second load - hit
        loader._load_file_with_cache(file_config, "test")
        
        stats = loader.cache_stats
        assert stats.hits == 1
        assert stats.misses == 1
        assert stats.hit_rate == 50.0
        assert stats.total_files >= 1
        assert stats.total_size_mb > 0
        assert stats.cache_dir is not None

    def test_cache_stats_core_mode(self, config_with_paths):
        """Test cache stats are None in core mode."""
        loader = DataLoader(config=config_with_paths, mode="core")
        
        assert loader.cache_stats is None

    def test_load_data_validation_failure(self, base_config_dict, tmp_path):
        """Test handling of data validation failure."""
        from src.config.config_schema import StrategyConfig
        
        # Create invalid data (missing required columns)
        dates = pd.date_range("2025-01-01", periods=3, freq="1min")
        df = pd.DataFrame({
            "timestamp": dates.strftime("%Y-%m-%d %H:%M:%S"),
            "wrong_col": [1, 2, 3],
            "another": [4, 5, 6]
        })
        
        invalid_path = tmp_path / "invalid.parquet"
        df.to_parquet(invalid_path, index=False)
        
        # Set only strategy data, set optional paths to None
        base_config_dict["data"]["paths"]["strategy_ohlcv"] = str(invalid_path)
        base_config_dict["data"]["paths"]["htf_ohlcv"] = None
        base_config_dict["data"]["paths"]["ltf_ohlcv"] = None
        base_config_dict["data"]["paths"]["artf_ohlcv"] = None
        
        config = StrategyConfig.from_dict(base_config_dict)
        loader = DataLoader(config=config, mode="core")
        
        # Should raise ValueError about missing columns
        with pytest.raises(ValueError, match="missing columns|Data validation failed"):
            loader.load_data()

    def test_duplicate_timestamp_handling(self, base_config_dict, tmp_path, caplog):
        """Test handling of duplicate timestamps."""
        from src.config.config_schema import StrategyConfig
        
        # Create a new config dict
        config_dict = base_config_dict.copy()
        
        # Create data with duplicate timestamps
        dates = pd.date_range("2025-01-01 00:00:00", periods=5, freq="1min")
        dates = dates.append(dates[-1:])  # Duplicate last timestamp
        
        # Generate valid OHLC data
        np.random.seed(42)
        prices = 100 + np.cumsum(np.random.randn(6) * 0.1)
        
        df = pd.DataFrame({
            "open": prices * 0.999,
            "high": prices * 1.002,
            "low": prices * 0.998,
            "close": prices,
            "volume": np.random.randint(100, 1000, 6)
        }, index=dates)
        df.index.name = "timestamp"
        
        # Ensure OHLC integrity
        df["high"] = df[["open", "high", "close"]].max(axis=1)
        df["low"] = df[["open", "low", "close"]].min(axis=1)
        
        dup_path = tmp_path / "duplicates.parquet"
        df.to_parquet(dup_path)
        
        # Set up config with duplicate data and no date range
        config_dict["data"]["paths"] = {
            "strategy_ohlcv": str(dup_path),
            "htf_ohlcv": None,
            "ltf_ohlcv": None,
            "artf_ohlcv": None
        }
        config_dict["data"]["date_range"] = None
        
        config = StrategyConfig.from_dict(config_dict)
        loader = DataLoader(config=config, mode="analytics")
        
        with caplog.at_level("WARNING"):
            bundle = loader.load_data()
        
        assert "duplicate timestamps" in caplog.text.lower()
        assert bundle.strategy is not None
        assert len(bundle.strategy) == 5  # After removing duplicates
        assert bundle.strategy.index.is_unique
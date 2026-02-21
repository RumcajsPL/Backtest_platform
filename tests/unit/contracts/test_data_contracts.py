"""
Unit Tests for Data Contracts
===============================
Tests DateRange, DataFileConfig, DataConfig, DataValidationResult,
DataInfo, DataBundle, and CacheStats.
"""

import pytest
import pandas as pd
from pathlib import Path
from datetime import datetime

from src.strategies.contracts.data_contracts import (
    DateRange,
    DataFileConfig,
    DataConfig,
    DataValidationResult,
    DataInfo,
    DataBundle,
    CacheStats
)


class TestDateRange:
    """Tests for DateRange contract."""

    def test_valid_range(self):
        """Test creating valid date range."""
        start = datetime(2025, 1, 1)
        end = datetime(2025, 12, 31)
        
        dr = DateRange(start=start, end=end)
        
        assert dr.start == start
        assert dr.end == end
        assert dr.is_bounded is True

    def test_unbounded_range(self):
        """Test date range with missing bounds."""
        dr = DateRange(start=None, end=None)
        
        assert dr.start is None
        assert dr.end is None
        assert dr.is_bounded is False

    def test_partially_bounded(self):
        """Test partially bounded range."""
        start = datetime(2025, 1, 1)
        
        dr = DateRange(start=start, end=None)
        
        assert dr.start == start
        assert dr.end is None
        assert dr.is_bounded is False

    def test_start_after_end_raises(self):
        """Test that start > end raises error."""
        with pytest.raises(ValueError, match="Start date .* is after end date"):
            DateRange(
                start=datetime(2025, 12, 31),
                end=datetime(2025, 1, 1)
            )

    def test_str_representation(self):
        """Test string representation."""
        # Bounded
        dr = DateRange(
            start=datetime(2025, 1, 1, 10, 30),
            end=datetime(2025, 1, 2, 14, 45)
        )
        assert "2025-01-01 10:30:00 → 2025-01-02 14:45:00" in str(dr)
        
        # Unbounded
        dr = DateRange(start=None, end=None)
        assert "unlimited → unlimited" in str(dr)


class TestDataFileConfig:
    """Tests for DataFileConfig contract."""

    def test_valid_config(self, tmp_path):
        """Test creating valid file config."""
        file_path = tmp_path / "test.parquet"
        file_path.touch()
        
        config = DataFileConfig(
            path=file_path,
            format="parquet",
            description="Test data"
        )
        
        assert config.path == file_path
        assert config.format == "parquet"
        assert config.description == "Test data"

    def test_path_conversion(self, tmp_path):
        """Test that string path is converted to Path."""
        file_path = tmp_path / "test.parquet"
        file_path.touch()
        
        config = DataFileConfig(
            path=str(file_path),
            format="parquet"
        )
        
        assert isinstance(config.path, Path)
        assert config.path == file_path

    def test_invalid_format(self, tmp_path):
        """Test that invalid format raises error."""
        file_path = tmp_path / "test.txt"
        file_path.touch()
        
        with pytest.raises(ValueError, match="Unsupported format"):
            DataFileConfig(
                path=file_path,
                format="txt"
            )

    def test_format_extension_mismatch(self, tmp_path):
        """Test that format mismatched with extension raises error."""
        file_path = tmp_path / "test.csv"
        file_path.touch()
        
        with pytest.raises(ValueError, match="File extension .* doesn't match format"):
            DataFileConfig(
                path=file_path,
                format="parquet"
            )

    def test_csv_format_valid(self, tmp_path):
        """Test CSV format is accepted."""
        file_path = tmp_path / "test.csv"
        file_path.touch()
        
        config = DataFileConfig(
            path=file_path,
            format="csv"
        )
        
        assert config.format == "csv"

    def test_with_date_range(self, tmp_path):
        """Test config with date range."""
        file_path = tmp_path / "test.parquet"
        file_path.touch()
        
        dr = DateRange(
            start=datetime(2025, 1, 1),
            end=datetime(2025, 12, 31)
        )
        
        config = DataFileConfig(
            path=file_path,
            format="parquet",
            date_range=dr
        )
        
        assert config.date_range == dr
        assert config.date_range.is_bounded is True


class TestDataConfig:
    """Tests for DataConfig contract."""

    @pytest.fixture
    def strategy_file(self, tmp_path):
        """Create strategy data file."""
        path = tmp_path / "strategy.parquet"
        path.touch()
        return path

    @pytest.fixture
    def htf_file(self, tmp_path):
        """Create HTF data file."""
        path = tmp_path / "htf.parquet"
        path.touch()
        return path

    @pytest.fixture
    def ltf_file(self, tmp_path):
        """Create LTF data file."""
        path = tmp_path / "ltf.parquet"
        path.touch()
        return path

    @pytest.fixture
    def artf_file(self, tmp_path):
        """Create ARTF data file."""
        path = tmp_path / "artf.parquet"
        path.touch()
        return path

    def test_valid_config(self, strategy_file):
        """Test creating valid data config with only required fields."""
        strategy_config = DataFileConfig(
            path=strategy_file,
            format="parquet"
        )
        
        config = DataConfig(
            strategy_data=strategy_config
        )
        
        assert config.strategy_data == strategy_config
        assert config.htf_data is None
        assert config.ltf_data is None
        assert config.artf_data is None
        assert config.date_range is None

    def test_full_config(self, strategy_file, htf_file, ltf_file, artf_file):
        """Test config with all optional fields."""
        strategy_config = DataFileConfig(path=strategy_file, format="parquet")
        htf_config = DataFileConfig(path=htf_file, format="parquet")
        ltf_config = DataFileConfig(path=ltf_file, format="parquet")
        artf_config = DataFileConfig(path=artf_file, format="parquet")
        
        dr = DateRange(
            start=datetime(2025, 1, 1),
            end=datetime(2025, 12, 31)
        )
        
        config = DataConfig(
            strategy_data=strategy_config,
            htf_data=htf_config,
            ltf_data=ltf_config,
            artf_data=artf_config,
            date_range=dr,
            validation_rules={"require_ohlc": True}
        )
        
        assert config.htf_data == htf_config
        assert config.ltf_data == ltf_config
        assert config.artf_data == artf_config
        assert config.date_range == dr
        assert config.validation_rules == {"require_ohlc": True}


class TestDataValidationResult:
    """Tests for DataValidationResult contract."""

    def test_valid_result(self):
        """Test creating valid validation result."""
        result = DataValidationResult(
            is_valid=True,
            checks={"has_data": True, "ohlc_columns": True},
            errors=[],
            warnings=[]
        )
        
        assert result.is_valid is True
        assert result.checks["has_data"] is True
        assert result.errors == []
        assert result.warnings == []

    def test_invalid_result(self):
        """Test creating invalid validation result."""
        result = DataValidationResult(
            is_valid=False,
            checks={"has_data": False},
            errors=["No data found"],
            warnings=["Missing columns"]
        )
        
        assert result.is_valid is False
        assert result.errors == ["No data found"]
        assert result.warnings == ["Missing columns"]

    def test_str_valid(self):
        """Test string representation for valid result."""
        result = DataValidationResult(
            is_valid=True,
            checks={},
            errors=[],
            warnings=["Warning 1"]
        )
        
        assert "✅ VALID" in str(result)
        assert "Warnings: 1" in str(result)

    def test_str_invalid(self):
        """Test string representation for invalid result."""
        result = DataValidationResult(
            is_valid=False,
            checks={},
            errors=["Error 1", "Error 2"],
            warnings=["Warning 1"]
        )
        
        assert "❌ INVALID" in str(result)
        assert "Errors: 2" in str(result)
        assert "Warnings: 1" in str(result)


class TestDataInfo:
    """Tests for DataInfo contract."""

    def test_minimal_info(self):
        """Test minimal data info."""
        info = DataInfo(
            total_bars=1000,
            strategy_bars=500
        )
        
        assert info.total_bars == 1000
        assert info.strategy_bars == 500
        assert info.htf_bars == 0
        assert info.ltf_bars == 0
        assert info.artf_bars == 0
        assert info.date_range is None
        assert info.ltf_timeframe == "1s"
        assert info.artf_timeframe == "1ME"
        assert info.cache_hit is False

    def test_full_info(self):
        """Test full data info."""
        date_range = (datetime(2025, 1, 1), datetime(2025, 12, 31))
        
        info = DataInfo(
            total_bars=10000,
            strategy_bars=5000,
            htf_bars=200,
            ltf_bars=300000,
            artf_bars=60,
            date_range=date_range,
            ltf_timeframe="1s",
            artf_timeframe="1ME",
            cache_hit=True
        )
        
        assert info.htf_bars == 200
        assert info.ltf_bars == 300000
        assert info.artf_bars == 60
        assert info.date_range == date_range
        assert info.cache_hit is True

    def test_str_representation(self):
        """Test string representation."""
        info = DataInfo(
            total_bars=10000,
            strategy_bars=5000,
            htf_bars=200,
            ltf_bars=300000,
            artf_bars=60,
            date_range=(datetime(2025, 1, 1), datetime(2025, 12, 31)),
            cache_hit=True
        )
        
        s = str(info)
        assert "Strategy period: 5,000 bars" in s
        assert "Full dataset: 10,000 bars" in s
        assert "HTF data: 200 bars" in s
        assert "LTF data: 300,000 bars" in s
        assert "ARTF data: 60 bars" in s
        assert "Cache: HIT ⚡" in s


class TestDataBundle:
    """Tests for DataBundle contract."""

    @pytest.fixture
    def valid_df(self):
        """Create valid DataFrame with OHLCV columns."""
        dates = pd.date_range(start="2025-01-01", periods=100, freq="1min")
        return pd.DataFrame({
            "open": [100.0] * 100,
            "high": [101.0] * 100,
            "low": [99.0] * 100,
            "close": [100.5] * 100,
            "volume": [1000] * 100
        }, index=dates)

    @pytest.fixture
    def invalid_df_no_index(self):
        """Create DataFrame without DatetimeIndex."""
        return pd.DataFrame({
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [1000]
        })

    @pytest.fixture
    def invalid_df_missing_columns(self):
        """Create DataFrame missing required columns."""
        dates = pd.date_range(start="2025-01-01", periods=10)
        return pd.DataFrame({
            "open": [100.0] * 10,
            "close": [100.5] * 10
        }, index=dates)

    def test_valid_bundle(self, valid_df):
        """Test creating valid data bundle."""
        info = DataInfo(total_bars=100, strategy_bars=100)
        validation = DataValidationResult(is_valid=True)
        
        bundle = DataBundle(
            full=valid_df,
            strategy=valid_df,
            info=info,
            validation=validation
        )
        
        assert bundle.full.equals(valid_df)
        assert bundle.strategy.equals(valid_df)
        assert bundle.htf is None
        assert bundle.ltf is None
        assert bundle.artf is None
        assert bundle.has_htf is False
        assert bundle.has_ltf is False
        assert bundle.has_artf is False

    def test_bundle_with_optional_data(self, valid_df):
        """Test bundle with optional data."""
        htf_df = valid_df.copy()
        ltf_df = valid_df.copy()
        artf_df = valid_df.copy()
        
        bundle = DataBundle(
            full=valid_df,
            strategy=valid_df,
            htf=htf_df,
            ltf=ltf_df,
            artf=artf_df,
            info=DataInfo(total_bars=100, strategy_bars=100)
        )
        
        assert bundle.has_htf is True
        assert bundle.has_ltf is True
        assert bundle.has_artf is True

    def test_validate_datetimeindex(self, valid_df, invalid_df_no_index):
        """Test that DatetimeIndex validation works."""
        info = DataInfo(total_bars=100, strategy_bars=100)
        
        # Should not raise
        DataBundle(full=valid_df, strategy=valid_df, info=info)
        
        # Should raise for invalid index
        with pytest.raises(ValueError, match="must have DatetimeIndex"):
            DataBundle(
                full=invalid_df_no_index,
                strategy=invalid_df_no_index,
                info=info
            )

    def test_validate_ohlc_columns(self, valid_df, invalid_df_missing_columns):
        """Test that OHLC column validation works."""
        info = DataInfo(total_bars=100, strategy_bars=100)
        
        # Should raise for missing columns
        with pytest.raises(ValueError, match="missing columns"):
            DataBundle(
                full=invalid_df_missing_columns,
                strategy=invalid_df_missing_columns,
                info=info
            )

    def test_str_representation(self, valid_df):
        """Test string representation."""
        bundle = DataBundle(
            full=valid_df,
            strategy=valid_df,
            info=DataInfo(total_bars=100, strategy_bars=100)
        )
        
        assert "DataBundle" in str(bundle)


class TestCacheStats:
    """Tests for CacheStats contract."""

    def test_default_stats(self):
        """Test default cache stats."""
        stats = CacheStats()
        
        assert stats.hits == 0
        assert stats.misses == 0
        assert stats.hit_rate == 0.0
        assert stats.total_files == 0
        assert stats.total_size_mb == 0.0
        assert stats.cache_dir == ""

    def test_custom_stats(self):
        """Test custom cache stats."""
        stats = CacheStats(
            hits=50,
            misses=10,
            hit_rate=83.33,
            total_files=15,
            total_size_mb=125.5,
            cache_dir="/tmp/cache"
        )
        
        assert stats.hits == 50
        assert stats.misses == 10
        assert stats.hit_rate == 83.33
        assert stats.total_files == 15
        assert stats.total_size_mb == 125.5
        assert stats.cache_dir == "/tmp/cache"

    def test_to_dict(self):
        """Test serialization to dict."""
        stats = CacheStats(
            hits=50,
            misses=10,
            hit_rate=83.33,
            total_files=15,
            total_size_mb=125.5,
            cache_dir="/tmp/cache"
        )
        
        d = stats.to_dict()
        
        assert d["hits"] == 50
        assert d["misses"] == 10
        assert d["hit_rate"] == "83.3%"
        assert d["cache_files"] == 15
        assert d["cache_size_mb"] == 125.5
        assert d["cache_dir"] == "/tmp/cache"

    def test_str_representation(self):
        """Test string representation."""
        stats = CacheStats(hits=50, misses=10)
        
        s = str(stats)
        assert "Cache: 50/60 hits" in s
        assert "83.3%" in s
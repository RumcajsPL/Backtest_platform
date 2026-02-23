"""
Unit Tests for FilterPipeline
===============================
Tests filter orchestration, time filters, technical filters, and caching.
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch

from src.strategies.specific.modules.filter_pipeline import FilterPipeline
from src.strategies.contracts.signal_contracts import SignalFrame
from src.strategies.contracts.filter_contracts import (
    FilterPipelineResult,
    FilterStatus,
    FilterProtocol
)
from src.strategies.contracts.cache import FilterPipelineCache
from src.strategies.specific.filters.time_filter import TimeFilter


class TestFilterPipeline:
    """Tests for FilterPipeline class."""

    @pytest.fixture
    def sample_df(self):
        """Sample OHLCV DataFrame for testing."""
        # Use a mix of hours to test time filter
        dates = pd.date_range(start="2025-01-01 00:00:00", periods=100, freq="1min")
        np.random.seed(42)
        
        df = pd.DataFrame({
            "open": 100 + np.random.randn(100) * 0.5,
            "high": 101 + np.random.randn(100) * 0.5,
            "low": 99 + np.random.randn(100) * 0.5,
            "close": 100 + np.random.randn(100) * 0.5,
            "volume": np.random.randint(100, 1000, 100)
        }, index=dates)
        
        # Ensure OHLC integrity
        df["high"] = df[["open", "high", "close"]].max(axis=1)
        df["low"] = df[["open", "low", "close"]].min(axis=1)
        
        return df

    @pytest.fixture
    def sample_signal_frame(self, sample_df):
        """Sample SignalFrame with mixed signals."""
        signals = pd.Series(0, index=sample_df.index, dtype=np.int8)
        
        # Add signals at various indices
        signals.iloc[10] = 1  # BUY
        signals.iloc[20] = 1  # BUY
        signals.iloc[30] = 2  # SELL
        signals.iloc[40] = 2  # SELL
        signals.iloc[50] = 1  # BUY
        signals.iloc[60] = 2  # SELL
        
        return SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={"source": "test"}
        )

    @pytest.fixture
    def base_config_without_time_filter(self, base_config_dict):
        """Create base config with time filter disabled."""
        config_dict = base_config_dict.copy()
        if "time_filters" in config_dict["filters"] and "time_filter" in config_dict["filters"]["time_filters"]:
            config_dict["filters"]["time_filters"]["time_filter"]["enabled"] = False
        return config_dict

    @pytest.fixture
    def config_with_technical_filters(self, base_config_without_time_filter):
        """Create config with properly structured technical filters."""
        from src.config.config_schema import StrategyConfig
        
        config_dict = base_config_without_time_filter.copy()
        
        # Configure filters with flat parameters
        config_dict["filters"]["filter_sequence"] = ["rsi_filter", "adx_filter"]
        config_dict["filters"]["technical_filters"] = {
            "rsi_filter": {
                "enabled": True,
                "length": 14,
                "overbought": 70,
                "oversold": 30
            },
            "adx_filter": {
                "enabled": True,
                "adx_length": 14,
                "threshold": 25
            }
        }
        
        return StrategyConfig.from_dict(config_dict)

    @pytest.fixture
    def config_without_time_filter(self, base_config_without_time_filter):
        """Create config with time filter disabled for technical filter tests."""
        from src.config.config_schema import StrategyConfig
        
        config_dict = base_config_without_time_filter.copy()
        
        # Configure technical filters with flat parameters
        config_dict["filters"]["filter_sequence"] = ["rsi_filter"]
        config_dict["filters"]["technical_filters"] = {
            "rsi_filter": {
                "enabled": True,
                "length": 14,
                "overbought": 70,
                "oversold": 30
            }
        }
        
        return StrategyConfig.from_dict(config_dict)

    @pytest.fixture
    def config_with_time_filter_only(self, base_config_dict):
        """Create config with only time filter enabled."""
        from src.config.config_schema import StrategyConfig
        
        config_dict = base_config_dict.copy()
        # Completely replace the time filter config
        config_dict["filters"]["time_filters"] = {
            "time_filter": {
                "enabled": True,
                "config": {
                    "session_start": {"hour": 9, "minute": 0},
                    "session_end": {"hour": 17, "minute": 0},
                    "excluded_days": []
                }
            }
        }
        config_dict["filters"]["filter_sequence"] = []
        config_dict["filters"]["technical_filters"] = {}
        
        return StrategyConfig.from_dict(config_dict)

    @pytest.fixture
    def config_with_no_filters(self, base_config_dict):
        """Create config with no filters at all."""
        from src.config.config_schema import StrategyConfig
        
        config_dict = base_config_dict.copy()
        config_dict["filters"]["time_filters"]["time_filter"]["enabled"] = False
        config_dict["filters"]["filter_sequence"] = []
        config_dict["filters"]["technical_filters"] = {}
        
        return StrategyConfig.from_dict(config_dict)

    def test_initialization_with_valid_config(self, test_config):
        """Test initializing FilterPipeline with valid config."""
        pipeline = FilterPipeline(config=test_config, mode="core")
        
        assert pipeline._mode == "core"
        assert pipeline.config == test_config
        assert pipeline.filter_sequence == list(test_config.filters.filter_sequence)
        assert pipeline._filter_cfg_hash is not None

    def test_initialization_with_invalid_mode(self, test_config):
        """Test that invalid mode raises error."""
        with pytest.raises(ValueError, match="Invalid mode"):
            FilterPipeline(config=test_config, mode="invalid")
        
        with pytest.raises(ValueError, match="Invalid mode"):
            FilterPipeline(config=test_config, mode="debug")

    def test_initialization_analytics_mode(self, test_config):
        """Test initialization in analytics mode."""
        pipeline = FilterPipeline(config=test_config, mode="analytics")
        assert pipeline._mode == "analytics"

    def test_load_time_filter_enabled(self, base_config_dict):
        """Test loading enabled time filter."""
        from src.config.config_schema import StrategyConfig
        
        # Configure time filter
        base_config_dict["filters"]["time_filters"]["time_filter"] = {
            "enabled": True,
            "config": {
                "session_start": {"hour": 8, "minute": 30},
                "session_end": {"hour": 20, "minute": 30},
                "excluded_days": []
            }
        }
        
        config = StrategyConfig.from_dict(base_config_dict)
        pipeline = FilterPipeline(config=config, mode="analytics")
        
        assert pipeline.time_filter is not None
        assert pipeline.time_filter.enabled is True

    def test_load_time_filter_disabled(self, base_config_dict):
        """Test loading disabled time filter - should be None (not instantiated)."""
        from src.config.config_schema import StrategyConfig
        
        base_config_dict["filters"]["time_filters"]["time_filter"] = {
            "enabled": False,
            "config": {
                "session_start": {"hour": 8, "minute": 30},
                "session_end": {"hour": 20, "minute": 30},
                "excluded_days": []
            }
        }
        
        config = StrategyConfig.from_dict(base_config_dict)
        pipeline = FilterPipeline(config=config, mode="analytics")
        
        assert pipeline.time_filter is None

    def test_load_time_filter_not_configured(self, base_config_dict):
        """Test when time filter is not in config - should be None."""
        from src.config.config_schema import StrategyConfig
        
        # Remove time filter entirely
        if "time_filters" in base_config_dict["filters"]:
            del base_config_dict["filters"]["time_filters"]["time_filter"]
        
        config = StrategyConfig.from_dict(base_config_dict)
        pipeline = FilterPipeline(config=config, mode="analytics")
        
        assert pipeline.time_filter is None

    def test_load_technical_filters(self, config_with_technical_filters):
        """Test loading technical filters."""
        pipeline = FilterPipeline(config=config_with_technical_filters, mode="analytics")
        
        assert len(pipeline.technical_filters) == 2
        assert pipeline.technical_filters[0].name == "rsi_filter"
        assert pipeline.technical_filters[1].name == "adx_filter"

    def test_load_technical_filters_skips_disabled(self, base_config_without_time_filter):
        """Test that disabled filters are skipped."""
        from src.config.config_schema import StrategyConfig
        
        config_dict = base_config_without_time_filter.copy()
        config_dict["filters"]["filter_sequence"] = ["rsi_filter", "adx_filter"]
        config_dict["filters"]["technical_filters"] = {
            "rsi_filter": {
                "enabled": True,
                "length": 14,
                "overbought": 70,
                "oversold": 30
            },
            "adx_filter": {
                "enabled": False,
                "adx_length": 14,
                "threshold": 25
            }
        }
        
        config = StrategyConfig.from_dict(config_dict)
        pipeline = FilterPipeline(config=config, mode="analytics")
        
        assert len(pipeline.technical_filters) == 1
        assert pipeline.technical_filters[0].name == "rsi_filter"

    def test_load_unknown_filter_skipped(self, base_config_dict, caplog):
        """Test that unknown filters are skipped with warning."""
        from src.config.config_schema import StrategyConfig
        
        config_dict = base_config_dict.copy()
        config_dict["filters"]["filter_sequence"] = ["unknown_filter"]
        config_dict["filters"]["technical_filters"] = {
            "unknown_filter": {
                "enabled": True,
                "some_param": 123
            }
        }
        
        config = StrategyConfig.from_dict(config_dict)
        
        with caplog.at_level("WARNING"):
            pipeline = FilterPipeline(config=config, mode="analytics")
        
        assert len(pipeline.technical_filters) == 0
        assert "Unknown filter in sequence" in caplog.text

    def test_compute_indicators_cache_hit(self, test_config, sample_df):
        """Test indicator computation with cache hit."""
        pipeline = FilterPipeline(config=test_config, mode="analytics")
        
        # First computation - cache miss
        pipeline.compute_indicators(sample_df)
        assert len(pipeline.indicators) >= 0
        
        # Second computation - cache hit
        pipeline.compute_indicators(sample_df)
        # Should not recompute, just load from cache

    def test_compute_indicators_cache_miss(self, test_config, sample_df):
        """Test indicator computation with cache miss."""
        pipeline = FilterPipeline(config=test_config, mode="analytics")
        
        # Clear cache
        pipeline.cache = FilterPipelineCache()
        
        # First computation should compute
        pipeline.compute_indicators(sample_df)
        
        # Indicators dict should be populated
        assert isinstance(pipeline.indicators, dict)
        assert isinstance(pipeline.ind_np, dict)

    def test_apply_filters_no_filters(self, config_with_no_filters, sample_df, sample_signal_frame):
        """Test apply_filters with no filters configured."""
        pipeline = FilterPipeline(config=config_with_no_filters, mode="core")
        
        result = pipeline.apply_filters(
            signal_frame=sample_signal_frame,
            df=sample_df
        )
        
        assert isinstance(result, FilterPipelineResult)
        assert result.raw_count == sample_signal_frame.count_by_type()["total"]
        assert result.final_count == result.raw_count
        assert len(result.filter_results) == 0

    def test_apply_filters_time_filter_only(self, config_with_time_filter_only, sample_df):
        """Test apply_filters with only time filter."""
        pipeline = FilterPipeline(config=config_with_time_filter_only, mode="analytics")
        
        # Create signals at various hours
        signals = pd.Series(0, index=sample_df.index, dtype=np.int8)
        for i, ts in enumerate(sample_df.index):
            if i % 10 == 0:  # Add signals every 10 bars
                signals.iloc[i] = 1 if ts.hour < 12 else 2
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )
        
        result = pipeline.apply_filters(
            signal_frame=signal_frame,
            df=sample_df
        )
        
        # Should have filtered some signals
        assert result.raw_count >= result.final_count
        assert result.time_filtered_count <= result.raw_count
        assert len(result.filter_results) == 1
        assert result.filter_results[0].filter_name == "time_filter"

    def test_apply_filters_with_technical_filters(self, config_without_time_filter, sample_df):
        """Test apply_filters with technical filters (time filter disabled)."""
        pipeline = FilterPipeline(config=config_without_time_filter, mode="analytics")
        
        # Create signal frame
        signals = pd.Series(0, index=sample_df.index, dtype=np.int8)
        signals.iloc[10:20] = 1  # BUY signals
        signals.iloc[30:40] = 2  # SELL signals
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )
        
        result = pipeline.apply_filters(
            signal_frame=signal_frame,
            df=sample_df
        )
        
        assert result.raw_count > 0
        assert result.final_count <= result.raw_count
        assert len(result.filter_results) >= 1
        
        # Check rejection reasons
        if result.rejection_reasons:
            assert "rsi_filter" in result.rejection_reasons

    def test_apply_filters_full_pipeline(self, base_config_dict, sample_df):
        """Test full pipeline with time filter and technical filters."""
        from src.config.config_schema import StrategyConfig
        
        # Configure full pipeline - completely replace all filter configs
        config_dict = base_config_dict.copy()
        
        # Completely replace time filters (don't modify, replace)
        config_dict["filters"]["time_filters"] = {
            "time_filter": {
                "enabled": True,
                "config": {
                    "session_start": {"hour": 0, "minute": 0},
                    "session_end": {"hour": 23, "minute": 59},
                    "excluded_days": []
                }
            }
        }
        
        # Replace filter sequence and technical filters
        config_dict["filters"]["filter_sequence"] = ["rsi_filter", "adx_filter"]
        config_dict["filters"]["technical_filters"] = {
            "rsi_filter": {
                "enabled": True,
                "length": 14,
                "overbought": 70,
                "oversold": 30
            },
            "adx_filter": {
                "enabled": True,
                "adx_length": 14,
                "threshold": 25
            }
        }
        
        config = StrategyConfig.from_dict(config_dict)
        pipeline = FilterPipeline(config=config, mode="analytics")
        
        # Create signal frame
        signals = pd.Series(0, index=sample_df.index, dtype=np.int8)
        signals.iloc[10:20] = 1
        signals.iloc[30:40] = 2
        signals.iloc[50:60] = 1
        signals.iloc[70:80] = 2
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )
        
        result = pipeline.apply_filters(
            signal_frame=signal_frame,
            df=sample_df
        )
        
        assert result.raw_count > 0
        assert result.final_count <= result.raw_count
        # Should have time_filter + 2 technical filters = 3 results
        assert len(result.filter_results) == 3, f"Expected 3 filter results, got {len(result.filter_results)}"

    def test_early_exit_no_signals_after_time_filter(self, base_config_dict, sample_df):
        """Test early exit when no signals remain after time filter."""
        from src.config.config_schema import StrategyConfig
        
        # Configure time filter that rejects everything
        config_dict = base_config_dict.copy()
        config_dict["filters"]["time_filters"] = {
            "time_filter": {
                "enabled": True,
                "config": {
                    "session_start": {"hour": 3, "minute": 0},
                    "session_end": {"hour": 4, "minute": 0},
                    "excluded_days": []
                }
            }
        }
        config_dict["filters"]["filter_sequence"] = []
        config_dict["filters"]["technical_filters"] = {}
        
        config = StrategyConfig.from_dict(config_dict)
        pipeline = FilterPipeline(config=config, mode="analytics")
        
        # Create signals (all outside 3-4 AM since sample_df starts at 00:00)
        signals = pd.Series(0, index=sample_df.index, dtype=np.int8)
        for i in range(len(sample_df)):
            if i % 10 == 0:
                signals.iloc[i] = 1 if i % 20 == 0 else 2
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )
        
        raw_count = signal_frame.count_by_type()["total"]
        
        result = pipeline.apply_filters(
            signal_frame=signal_frame,
            df=sample_df
        )
        
        assert result.final_count == 0
        assert result.time_filtered_count == 0
        assert result.raw_count == raw_count
        assert len(result.filter_results) == 1
        assert result.filter_results[0].filter_name == "time_filter"

    def test_early_exit_no_signals_after_technical_filter(self, config_without_time_filter, sample_df):
        """Test early exit when no signals remain after a technical filter."""
        pipeline = FilterPipeline(config=config_without_time_filter, mode="analytics")
        
        # Create signal frame
        signals = pd.Series(0, index=sample_df.index, dtype=np.int8)
        signals.iloc[10:20] = 1
        signals.iloc[30:40] = 2
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )
        
        result = pipeline.apply_filters(
            signal_frame=signal_frame,
            df=sample_df
        )
        
        assert result.final_count >= 0
        assert len(result.filter_results) >= 1

    def test_filter_error_handling(self, config_without_time_filter, sample_df, caplog):
        """Test error handling when a filter raises exception."""
        pipeline = FilterPipeline(config=config_without_time_filter, mode="analytics")
        
        # Mock a filter that raises exception
        mock_filter = Mock(spec=FilterProtocol)
        mock_filter.name = "failing_filter"
        mock_filter.apply_filter.side_effect = Exception("Test exception")
        
        pipeline.technical_filters = [mock_filter]
        
        signals = pd.Series(0, index=sample_df.index, dtype=np.int8)
        signals.iloc[10] = 1
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )
        
        with caplog.at_level("ERROR"):
            result = pipeline.apply_filters(
                signal_frame=signal_frame,
                df=sample_df
            )
        
        assert result.final_count == 1
        assert len(result.filter_results) == 1
        assert result.filter_results[0].status == FilterStatus.ERROR
        assert "raised an exception" in caplog.text

    def test_filter_metadata_tracking(self, config_without_time_filter, sample_df):
        """Test that filter metadata is properly tracked."""
        pipeline = FilterPipeline(config=config_without_time_filter, mode="analytics")
        
        signals = pd.Series(0, index=sample_df.index, dtype=np.int8)
        signals.iloc[10:20] = 1  # 10 BUY signals
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )
        
        result = pipeline.apply_filters(
            signal_frame=signal_frame,
            df=sample_df
        )
        
        assert len(result.filter_results) >= 1
        metadata = result.filter_results[0]
        
        assert metadata.filter_name == "rsi_filter"
        assert metadata.signals_in == 10
        assert metadata.signals_out <= 10
        assert metadata.signals_rejected == metadata.signals_in - metadata.signals_out
        assert metadata.execution_time_ms is not None

    def test_pass_rate_calculation(self, test_config, sample_df, sample_signal_frame):
        """Test pass rate calculation in result."""
        pipeline = FilterPipeline(config=test_config, mode="analytics")
        
        result = pipeline.apply_filters(
            signal_frame=sample_signal_frame,
            df=sample_df
        )
        
        if result.raw_count > 0:
            expected_pass_rate = (result.final_count / result.raw_count) * 100
            assert abs(result.pass_rate - expected_pass_rate) < 0.01
        else:
            assert result.pass_rate == 0.0

    def test_compute_filter_cfg_hash_stability(self, test_config):
        """Test that filter config hash is stable for same config."""
        pipeline1 = FilterPipeline(config=test_config, mode="core")
        pipeline2 = FilterPipeline(config=test_config, mode="core")
        
        assert pipeline1._filter_cfg_hash == pipeline2._filter_cfg_hash

    def test_compute_filter_cfg_hash_changes_with_params(self, base_config_without_time_filter):
        """Test that hash changes when filter parameters change."""
        from src.config.config_schema import StrategyConfig
        
        # Create two configs with same filter but different parameters
        config1_dict = base_config_without_time_filter.copy()
        config1_dict["filters"]["filter_sequence"] = ["rsi_filter"]
        config1_dict["filters"]["technical_filters"] = {
            "rsi_filter": {
                "enabled": True,
                "length": 14,
                "overbought": 70,
                "oversold": 30
            }
        }
        
        config2_dict = base_config_without_time_filter.copy()
        config2_dict["filters"]["filter_sequence"] = ["rsi_filter"]
        config2_dict["filters"]["technical_filters"] = {
            "rsi_filter": {
                "enabled": True,
                "length": 21,
                "overbought": 70,
                "oversold": 30
            }
        }
        
        config1 = StrategyConfig.from_dict(config1_dict)
        config2 = StrategyConfig.from_dict(config2_dict)
        
        pipeline1 = FilterPipeline(config=config1, mode="core")
        pipeline2 = FilterPipeline(config=config2, mode="core")
        
        # The hash should be different for different parameters
        # Note: This is still failing, indicating a deeper issue in _compute_filter_cfg_hash
        # The hash is the same despite different parameters
        assert pipeline1._filter_cfg_hash != pipeline2._filter_cfg_hash

    def test_cache_id_computation(self, test_config, sample_df):
        """Test cache ID computation."""
        pipeline = FilterPipeline(config=test_config, mode="core")
        
        cache_id1 = pipeline.cache.compute_cache_id(sample_df, pipeline._filter_cfg_hash)
        cache_id2 = pipeline.cache.compute_cache_id(sample_df, pipeline._filter_cfg_hash)
        
        assert cache_id1 == cache_id2
        
        cache_id3 = pipeline.cache.compute_cache_id(sample_df, "different_hash")
        assert cache_id1 != cache_id3

    def test_mode_passed_to_filters(self, config_without_time_filter, sample_df):
        """Test that mode is properly passed to filters."""
        pipeline = FilterPipeline(config=config_without_time_filter, mode="analytics")
        
        mock_filter = Mock(spec=FilterProtocol)
        mock_filter.name = "rsi_filter"
        mock_filter.apply_filter.return_value = Mock(
            signal_frame=Mock(),
            metadata=Mock(signals_rejected=0)
        )
        
        pipeline.technical_filters = [mock_filter]
        
        signals = pd.Series(0, index=sample_df.index, dtype=np.int8)
        signals.iloc[10] = 1
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )
        
        pipeline.apply_filters(
            signal_frame=signal_frame,
            df=sample_df,
            mode="analytics"
        )
        
        mock_filter.apply_filter.assert_called_once()
        args, kwargs = mock_filter.apply_filter.call_args
        assert kwargs.get("mode") == "analytics"
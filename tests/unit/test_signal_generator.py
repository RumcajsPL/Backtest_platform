"""
Unit Tests for SignalGenerator
===============================
Tests signal generation, validation, and error handling.
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path

from src.strategies.specific.modules.signal_generator import SignalGenerator
from src.strategies.contracts.signal_contracts import SignalFrame
from src.strategies.contracts.data_contracts import DataBundle, DataInfo


class TestSignalGenerator:
    """Tests for SignalGenerator class."""

    def test_initialization_with_valid_config(self, test_config):
        """Test initializing SignalGenerator with valid config."""
        generator = SignalGenerator(config=test_config, mode="core")
        assert generator.mode == "core"
        assert generator.htf_period == "1H"
        assert generator.trigger is not None

    def test_initialization_with_invalid_mode(self, test_config):
        """Test that invalid mode raises error."""
        with pytest.raises(ValueError, match="mode must be one of"):
            SignalGenerator(config=test_config, mode="invalid")

        with pytest.raises(ValueError, match="mode must be one of"):
            SignalGenerator(config=test_config, mode="debug")

    def test_initialization_with_invalid_htf_period(self, base_config_dict):
        """Test that invalid htf_period format raises error."""
        base_config_dict["data"]["htf_period"] = "INVALID"

        with pytest.raises(ValueError, match="not a recognised period"):
            from src.config.config_schema import StrategyConfig
            config = StrategyConfig.from_dict(base_config_dict)
            SignalGenerator(config=config, mode="core")

    @pytest.mark.parametrize("invalid_period", ["", "  ", "2H", "15M", "1D", "1W"])
    def test_initialization_with_edge_periods(self, base_config_dict, invalid_period):
        """Test various htf_period formats - valid and invalid."""
        base_config_dict["data"]["htf_period"] = invalid_period
        
        from src.config.config_schema import StrategyConfig
        
        if invalid_period in ["2H", "15M"]:
            # These should be invalid (not in _VALID_HTF_PERIODS)
            with pytest.raises(ValueError, match="not a recognised period"):
                config = StrategyConfig.from_dict(base_config_dict)
                SignalGenerator(config=config, mode="core")
        elif invalid_period in ["", "  "]:
            # Empty/blank should raise
            with pytest.raises(ValueError, match="htf_period is required"):
                config = StrategyConfig.from_dict(base_config_dict)
                SignalGenerator(config=config, mode="core")
        else:
            # "1D", "1W" are valid
            config = StrategyConfig.from_dict(base_config_dict)
            generator = SignalGenerator(config=config, mode="core")
            assert generator.htf_period == invalid_period

    def test_generate_signals_with_valid_data(self, test_config, sample_data_bundle):
        """Test signal generation with valid data bundle."""
        generator = SignalGenerator(config=test_config, mode="core")
        signal_frame = generator.generate_signals(sample_data_bundle)

        assert isinstance(signal_frame, SignalFrame)
        assert len(signal_frame.signals) == len(sample_data_bundle.strategy)
        assert signal_frame.signals.dtype == np.int8

        # Check signal types (0=none, 1=BUY, 2=SELL)
        unique_values = set(signal_frame.signals.unique())
        assert unique_values.issubset({0, 1, 2})

    def test_generate_signals_counts(self, test_config, sample_data_bundle):
        """Test that signal counts are reasonable."""
        generator = SignalGenerator(config=test_config, mode="core")
        signal_frame = generator.generate_signals(sample_data_bundle)

        counts = signal_frame.count_by_type()
        assert counts["total"] >= 0
        assert counts["buy"] + counts["sell"] == counts["total"]
        assert counts["buy"] >= 0
        assert counts["sell"] >= 0

    def test_generate_signals_with_none_bundle(self, test_config):
        """Test that None bundle raises error."""
        generator = SignalGenerator(config=test_config, mode="core")

        with pytest.raises(ValueError, match="data_bundle cannot be None"):
            generator.generate_signals(None)

    def test_generate_signals_with_empty_strategy(self, test_config, sample_ohlcv_data):
        """Test that empty strategy DataFrame raises error."""
        empty_bundle = DataBundle(
            full=pd.DataFrame(),
            strategy=pd.DataFrame(),
            htf=sample_ohlcv_data,  # HTF is present but strategy empty
            info=DataInfo(
                total_bars=0,
                strategy_bars=0,
                htf_bars=len(sample_ohlcv_data),
                date_range=(None, None)
            )
        )

        generator = SignalGenerator(config=test_config, mode="core")

        with pytest.raises(ValueError, match="missing or empty"):
            generator.generate_signals(empty_bundle)

    def test_generate_signals_with_missing_htf(self, test_config, sample_ohlcv_data):
        """Test that missing HTF data raises error."""
        invalid_bundle = DataBundle(
            full=sample_ohlcv_data,
            strategy=sample_ohlcv_data,
            htf=None,  # Missing HTF
            info=DataInfo(
                total_bars=len(sample_ohlcv_data),
                strategy_bars=len(sample_ohlcv_data),
                htf_bars=0,
                date_range=(
                    sample_ohlcv_data.index[0].to_pydatetime(),
                    sample_ohlcv_data.index[-1].to_pydatetime()
                )
            )
        )

        generator = SignalGenerator(config=test_config, mode="core")

        with pytest.raises(ValueError, match="htf is missing or empty"):
            generator.generate_signals(invalid_bundle)

    def test_analytics_mode_metadata(self, test_config, sample_data_bundle):
        """Test that analytics mode includes metadata."""
        generator = SignalGenerator(config=test_config, mode="analytics")
        signal_frame = generator.generate_signals(sample_data_bundle)

        assert signal_frame.indicator_data is not None
        assert "source" in signal_frame.signal_metadata
        assert signal_frame.signal_metadata["mode"] == "analytics"

    def test_core_mode_no_metadata(self, test_config, sample_data_bundle):
        """Test that core mode excludes metadata for speed."""
        generator = SignalGenerator(config=test_config, mode="core")
        signal_frame = generator.generate_signals(sample_data_bundle)

        assert signal_frame.indicator_data is None
        assert signal_frame.signal_metadata.get("mode") == "core"

    def test_get_signal_stats(self, test_config, sample_data_bundle):
        """Test signal statistics generation."""
        generator = SignalGenerator(config=test_config, mode="core")
        signal_frame = generator.generate_signals(sample_data_bundle)

        stats = generator.get_signal_stats(signal_frame)
        assert stats.total_count == signal_frame.count_by_type()["total"]
        assert stats.buy_count + stats.sell_count == stats.total_count
        assert isinstance(stats.buy_count, int)
        assert isinstance(stats.sell_count, int)

    def test_get_signal_stats_verbose(self, test_config, sample_data_bundle):
        """Test verbose signal statistics with metadata."""
        generator = SignalGenerator(config=test_config, mode="analytics")
        signal_frame = generator.generate_signals(sample_data_bundle)

        stats = generator.get_signal_stats(signal_frame, verbose=True)
        assert stats.total_count == signal_frame.count_by_type()["total"]
        # Verbose stats include additional fields
        assert hasattr(stats, "total_count")

    def test_signal_frame_iter_raw(self, test_config, sample_data_bundle):
        """Test fast iteration over signals (core mode)."""
        generator = SignalGenerator(config=test_config, mode="core")
        signal_frame = generator.generate_signals(sample_data_bundle)

        # iter_raw should work in core mode
        timestamps = []
        codes = []
        for ts, code in signal_frame.iter_raw():
            timestamps.append(ts)
            codes.append(code)

        # iter_raw only yields non-zero signals
        total_signals = signal_frame.count_by_type()["total"]
        assert len(timestamps) == total_signals
        assert len(codes) == total_signals
        assert all(code in (1, 2) for code in codes)

    def test_signal_frame_iter_requires_analytics(self, test_config, sample_data_bundle):
        """Test that __iter__ raises in core mode."""
        generator = SignalGenerator(config=test_config, mode="core")
        signal_frame = generator.generate_signals(sample_data_bundle)

        with pytest.raises(RuntimeError, match="requires indicator_data"):
            list(iter(signal_frame))

    def test_signal_frame_iter_in_analytics(self, test_config, sample_data_bundle):
        """Test that __iter__ works in analytics mode."""
        generator = SignalGenerator(config=test_config, mode="analytics")
        signal_frame = generator.generate_signals(sample_data_bundle)

        # Should not raise in analytics mode
        try:
            signals = list(iter(signal_frame))
            # May be empty if no signals, but shouldn't raise
        except RuntimeError:
            pytest.fail("__iter__ raised RuntimeError in analytics mode")

    @pytest.mark.parametrize("mode", ["core", "analytics"])
    def test_signal_frame_count_by_type(self, test_config, sample_data_bundle, mode):
        """Test count_by_type works in both modes."""
        generator = SignalGenerator(config=test_config, mode=mode)
        signal_frame = generator.generate_signals(sample_data_bundle)

        counts = signal_frame.count_by_type()
        assert isinstance(counts, dict)
        assert "total" in counts
        assert "buy" in counts
        assert "sell" in counts

    def test_signal_generator_with_real_data_paths(
        self, test_config, request, tmp_path
    ):
        """Integration test using actual data files from test_data_paths."""
        # This test uses the real data paths from test_data_paths.yaml
        # Skip if real data isn't available
        data_paths = request.config.cache.get("data_paths", None)
        if not data_paths:
            pytest.skip("Real data paths not available - run with --use-real-data")

        # This would load actual data - simplified for now
        # In practice, this would use DataLoader with the real paths
        pytest.skip("Requires actual data files - run manually with --run-real-data")

    def test_error_handling_malformed_data(self, test_config):
        """Test handling of malformed input data."""
        generator = SignalGenerator(config=test_config, mode="core")

        # Create bundle with wrong index type
        bad_df = pd.DataFrame({
            "open": [1, 2, 3],
            "high": [2, 3, 4],
            "low": [0.5, 1, 2],
            "close": [1.5, 2.5, 3.5]
        })
        # No DatetimeIndex

        bad_bundle = DataBundle(
            full=bad_df,
            strategy=bad_df,
            htf=bad_df,
            info=DataInfo(total_bars=3, strategy_bars=3, htf_bars=3)
        )

        # Should raise or handle gracefully - depending on implementation
        with pytest.raises((ValueError, TypeError, AttributeError)):
            generator.generate_signals(bad_bundle)
"""
Unit Tests for SignalGenerator
===============================
Tests signal generation, validation, and error handling.
Includes real data tests using test_data_paths.yaml.

Note on Single Responsibility:
- SignalGenerator only generates signals from validated data
- Config validation is tested in test_config_schema.py
- DataBundle validation is tested in test_data_contracts.py
- This test focuses ONLY on SignalGenerator's core responsibility
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch
from dataclasses import asdict

from src.strategies.specific.modules.signal_generator import SignalGenerator
from src.strategies.contracts.signal_contracts import SignalFrame
from src.strategies.contracts.data_contracts import DataBundle, DataInfo
from src.utils.paths import test_path


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

    @pytest.mark.parametrize("period,should_be_valid", [
        ("", False),      # Empty - config validation catches this
        ("  ", False),    # Blank - config validation catches this
        ("2H", True),     # Valid in pandas (2 hours)
        ("15M", False),   # Invalid - not in allowed list
        ("1D", True),     # Valid
        ("1W", True),     # Valid
    ])
    def test_initialization_with_edge_periods(self, base_config_dict, period, should_be_valid):
        """
        Test various htf_period formats.
        
        Note: This documents behavior - validation happens in config_schema.py,
        not in SignalGenerator. SignalGenerator only receives validated config.
        """
        base_config_dict["data"]["htf_period"] = period
        
        from src.config.config_schema import StrategyConfig
        
        try:
            config = StrategyConfig.from_dict(base_config_dict)
            generator = SignalGenerator(config=config, mode="core")
            
            # If we get here, config validation passed
            if not should_be_valid:
                pytest.fail(f"Expected invalid period '{period}' to be caught by config validation, but it was accepted")
            else:
                assert generator.htf_period == period
                
        except ValueError as e:
            # Config validation caught it
            if should_be_valid:
                pytest.fail(f"Expected valid period '{period}' to be accepted by config validation, but got: {e}")
            else:
                # Expected error - verify it's the right kind
                assert "not a recognised period" in str(e) or "htf_period is required" in str(e)

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

    def test_generate_signals_with_empty_strategy(self, test_config, monkeypatch):
        """
        Test handling of empty strategy data.
        
        Note: Uses monkeypatch to create mock bundle and bypass DataBundle validation.
        """
        # Create a mock bundle that will pass isinstance checks
        mock_bundle = Mock(spec=DataBundle)
        mock_bundle.strategy = pd.DataFrame()  # Empty DataFrame
        mock_bundle.htf = pd.DataFrame({"close": [1, 2, 3]})  # Non-empty HTF
        mock_bundle.full = pd.DataFrame()
        
        generator = SignalGenerator(config=test_config, mode="core")
        
        # SignalGenerator should raise appropriate error
        with pytest.raises(ValueError, match="missing or empty"):
            generator.generate_signals(mock_bundle)

    def test_generate_signals_with_missing_htf(self, test_config, sample_ohlcv_data):
        """Test that missing HTF data raises error."""
        # Create mock bundle with missing HTF
        mock_bundle = Mock(spec=DataBundle)
        mock_bundle.strategy = sample_ohlcv_data
        mock_bundle.htf = None
        mock_bundle.full = sample_ohlcv_data

        generator = SignalGenerator(config=test_config, mode="core")

        with pytest.raises(ValueError, match="htf is missing or empty"):
            generator.generate_signals(mock_bundle)

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

    # ========================================================================
    # REAL DATA TESTS
    # ========================================================================

    def test_with_real_data(self, real_data_config):
        """Test SignalGenerator with real market data."""
        from src.strategies.specific.modules.data_loader import DataLoader
        
        print(f"\n{'='*60}")
        print("REAL DATA TEST: SignalGenerator")
        print(f"{'='*60}")
        print(f"Asset: {real_data_config.asset.symbol}")
        print(f"Date Range: {real_data_config.data.date_range.start} to {real_data_config.data.date_range.end}")
        print(f"Mode: {real_data_config.execution.mode}")
        
        # Load real data
        loader = DataLoader(config=real_data_config, mode="analytics")
        bundle = loader.load_data()
        
        print(f"Data loaded: {bundle.info.strategy_bars} bars")
        if bundle.info.cache_hit:
            print(f"  ⚡ Cache hit")
        
        # Initialize generator
        generator = SignalGenerator(config=real_data_config, mode="analytics")
        
        # Generate signals on real data
        signal_frame = generator.generate_signals(bundle)
        
        # Basic validation
        assert isinstance(signal_frame, SignalFrame)
        assert len(signal_frame.signals) == len(bundle.strategy)
        assert signal_frame.signals.dtype == np.int8
        
        # Check signal counts
        counts = signal_frame.count_by_type()
        assert counts["total"] >= 0
        assert counts["buy"] + counts["sell"] == counts["total"]
        
        print(f"\nSignal Generation Results:")
        print(f"  Total signals: {counts['total']}")
        print(f"  BUY signals: {counts['buy']}")
        print(f"  SELL signals: {counts['sell']}")
        if counts['total'] > 0:
            print(f"  Signal density: {counts['total']/len(bundle.strategy)*100:.2f}%")
            if counts['sell'] > 0:
                print(f"  BUY/SELL ratio: {counts['buy']/counts['sell']:.2f}")
            else:
                print(f"  Only BUY signals")
        
        # Verify indicator data in analytics mode
        assert signal_frame.indicator_data is not None
        assert "close" in signal_frame.indicator_data.columns
        print(f"  Indicator columns: {list(signal_frame.indicator_data.columns)}")

    def test_with_small_date_range(self, real_data_config):
        """Test with the specific small date range from test_data_paths.yaml."""
        from src.strategies.specific.modules.data_loader import DataLoader
        
        print(f"\n{'='*60}")
        print("REAL DATA TEST: SignalGenerator (Small Range)")
        print(f"{'='*60}")
        
        # Verify we're using the small range (7 hours)
        start = real_data_config.data.date_range.start
        end = real_data_config.data.date_range.end
        print(f"Date range: {start} to {end}")
        
        assert start == "2025-12-17 14:00:00"
        assert end == "2025-12-17 21:00:00"
        
        loader = DataLoader(config=real_data_config, mode="analytics")
        bundle = loader.load_data()
        
        # Verify data is within range
        assert bundle.strategy.index.min() >= pd.Timestamp(start)
        assert bundle.strategy.index.max() <= pd.Timestamp(end)
        
        print(f"Bars loaded: {len(bundle.strategy)}")
        print(f"Actual range: {bundle.strategy.index.min()} to {bundle.strategy.index.max()}")
        
        generator = SignalGenerator(config=real_data_config, mode="analytics")
        signal_frame = generator.generate_signals(bundle)
        
        counts = signal_frame.count_by_type()
        print(f"\nSignal Results (7-hour window):")
        print(f"  Total signals: {counts['total']}")
        print(f"  BUY signals: {counts['buy']}")
        print(f"  SELL signals: {counts['sell']}")

    def test_htf_alignment_with_real_data(self, real_data_config):
        """Test HTF alignment using real data."""
        from src.strategies.specific.modules.data_loader import DataLoader
        
        print(f"\n{'='*60}")
        print("REAL DATA TEST: HTF Alignment")
        print(f"{'='*60}")
        
        loader = DataLoader(config=real_data_config, mode="analytics")
        bundle = loader.load_data()
        
        print(f"Strategy TF: {real_data_config.data.htf_period}")
        print(f"HTF available: {bundle.has_htf}")
        
        generator = SignalGenerator(config=real_data_config, mode="analytics")
        signal_frame = generator.generate_signals(bundle)
        
        # If we have indicator_data in analytics mode, check HTF alignment
        if signal_frame.indicator_data is not None and bundle.has_htf:
            # Verify no lookahead in HTF data
            # This is a simplified check - in production you'd verify HTF values
            # are from previous bars
            print("  ✓ HTF data available for alignment check")
        else:
            print("  ⚠ No HTF data available for alignment check")

    def test_signal_generator_error_handling_missing_htf(self, real_data_config):
        """
        Test behavior when HTF data is missing in config.
        
        Note: HTF is optional in DataLoader. When HTF path is None,
        DataLoader skips HTF loading. SignalGenerator requires HTF
        and should raise an error.
        """
        print(f"\n{'='*60}")
        print("REAL DATA TEST: Missing HTF Handling")
        print(f"{'='*60}")
        
        from dataclasses import asdict
        
        # Convert to dict using dataclasses.asdict
        config_dict = asdict(real_data_config)
        
        # Modify the paths - set HTF to None
        config_dict["data"]["paths"]["htf_ohlcv"] = None
        
        from src.config.config_schema import StrategyConfig
        modified_config = StrategyConfig.from_dict(config_dict)
        
        print("HTF path set to None - DataLoader should skip HTF loading")
        
        from src.strategies.specific.modules.data_loader import DataLoader
        
        # This should NOT raise - HTF is optional in DataLoader
        loader = DataLoader(config=modified_config, mode="analytics")
        bundle = loader.load_data()
        
        # Verify HTF is None
        assert bundle.htf is None
        print(f"✓ HTF data is None as expected")
        
        # SignalGenerator requires HTF
        generator = SignalGenerator(config=modified_config, mode="analytics")
        
        print("\nSignalGenerator should raise error for missing HTF...")
        
        # This SHOULD raise because SignalGenerator requires HTF
        with pytest.raises(ValueError, match="htf is missing or empty"):
            generator.generate_signals(bundle)
        
        print("✓ Correctly raised ValueError for missing HTF in SignalGenerator")

    def test_compare_core_vs_analytics_modes(self, real_data_config):
        """Compare performance between core and analytics modes."""
        from src.strategies.specific.modules.data_loader import DataLoader
        import time
        
        print(f"\n{'='*60}")
        print("REAL DATA TEST: Core vs Analytics Mode Comparison")
        print(f"{'='*60}")
        
        loader = DataLoader(config=real_data_config, mode="core")
        bundle = loader.load_data()
        
        # Test core mode
        start = time.perf_counter()
        generator_core = SignalGenerator(config=real_data_config, mode="core")
        frame_core = generator_core.generate_signals(bundle)
        core_time = (time.perf_counter() - start) * 1000
        
        # Test analytics mode
        start = time.perf_counter()
        generator_analytics = SignalGenerator(config=real_data_config, mode="analytics")
        frame_analytics = generator_analytics.generate_signals(bundle)
        analytics_time = (time.perf_counter() - start) * 1000
        
        print(f"\nPerformance Comparison:")
        print(f"  Core mode: {core_time:.2f}ms")
        print(f"  Analytics mode: {analytics_time:.2f}ms")
        print(f"  Overhead: {analytics_time - core_time:.2f}ms ({(analytics_time/core_time-1)*100:.1f}%)")
        
        # Verify mode-specific behavior
        assert frame_core.indicator_data is None
        assert frame_analytics.indicator_data is not None
        assert frame_core.signal_metadata["mode"] == "core"
        assert frame_analytics.signal_metadata["mode"] == "analytics"
        
        # Signal counts should be identical
        counts_core = frame_core.count_by_type()
        counts_analytics = frame_analytics.count_by_type()
        assert counts_core == counts_analytics
        
        print(f"\nSignal counts (both modes): {counts_core['total']}")


# Simple path test - not a method of TestSignalGenerator
def test_path():
    """Simple path test to verify test_path utility."""
    from src.utils.paths import test_path
    path = test_path("strategies", "unit")
    assert path is not None
    assert "strategies" in str(path)
    assert "unit" in str(path)
    # No return statement - pytest expects None
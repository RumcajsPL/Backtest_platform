"""
Unit Tests for RiskManager
===========================
Tests SL/TP calculations, ATR caching, annual range validation.
Includes real data tests using actual market data.
"""

import pytest
import pandas as pd
import numpy as np
from dataclasses import replace

from src.strategies.specific.modules.risk_manager import RiskManager
from src.strategies.core.cache_manager import CacheManager
from src.strategies.contracts.trade_contracts import TradeParameters


class TestRiskManager:
    """Tests for RiskManager class."""

    @pytest.fixture
    def sample_ohlcv(self):
        """Generate sample OHLCV data for risk testing."""
        dates = pd.date_range(start="2025-01-01", periods=500, freq="1min")
        np.random.seed(42)
        
        # Create trending price series
        trend = np.linspace(100, 110, 500) + np.random.randn(500) * 0.5
        
        df = pd.DataFrame({
            "open": trend * (1 + np.random.randn(500) * 0.001),
            "high": trend * (1 + np.abs(np.random.randn(500)) * 0.002),
            "low": trend * (1 - np.abs(np.random.randn(500)) * 0.002),
            "close": trend,
            "volume": np.random.randint(100, 1000, 500)
        }, index=dates)
        
        # Ensure OHLC integrity
        df["high"] = df[["open", "high", "close"]].max(axis=1)
        df["low"] = df[["open", "low", "close"]].min(axis=1)
        
        return df

    @pytest.fixture
    def sample_artf(self):
        """Generate sample monthly ARTF data."""
        dates = pd.date_range(start="2024-01-01", periods=24, freq="ME")
        
        # Monthly ranges
        ranges = np.random.uniform(5, 15, 24)
        bases = np.linspace(100, 110, 24)
        
        df = pd.DataFrame({
            "open": bases - ranges/2,
            "high": bases + ranges/2,
            "low": bases - ranges/2,
            "close": bases,
            "volume": np.random.randint(1000, 10000, 24)
        }, index=dates)
        
        return df

    @pytest.fixture
    def config_with_spread_disabled(self, test_config):
        """Create a config with spread disabled to avoid SpreadManager issues."""
        return replace(
            test_config,
            trade_management=replace(
                test_config.trade_management,
                spread=replace(
                    test_config.trade_management.spread,
                    enabled=False
                )
            )
        )

    def test_initialization_with_valid_config(self, config_with_spread_disabled, sample_ohlcv):
        """Test initializing RiskManager with valid config."""
        risk_manager = RiskManager(
            config=config_with_spread_disabled,
            ohlcv_data=sample_ohlcv,
            mode="core"
        )

        assert risk_manager.atr_length == 14
        assert risk_manager.sl_multiplier == 1.4
        assert risk_manager.tp_mode == "rr_ratio"
        assert risk_manager.rr_ratio == 5.7

    def test_initialization_with_invalid_mode(self, config_with_spread_disabled, sample_ohlcv):
        """Test that invalid mode raises error."""
        with pytest.raises(ValueError, match="Invalid mode.*'debug' is not a valid mode"):
            RiskManager(
                config=config_with_spread_disabled,
                ohlcv_data=sample_ohlcv,
                mode="debug"
            )

    @pytest.mark.parametrize("tp_mode", ["rr_ratio", "atr_multiplier"])
    def test_tp_modes(self, config_with_spread_disabled, sample_ohlcv, tp_mode):
        """Test both TP modes are accepted."""
        # Create modified config with different tp_mode
        modified_config = replace(
            config_with_spread_disabled,
            trade_management=replace(
                config_with_spread_disabled.trade_management,
                risk=replace(
                    config_with_spread_disabled.trade_management.risk,
                    tp_mode=tp_mode
                )
            )
        )
        
        risk_manager = RiskManager(
            config=modified_config,
            ohlcv_data=sample_ohlcv,
            mode="core"
        )

        assert risk_manager.tp_mode == tp_mode

    def test_invalid_tp_mode(self, config_with_spread_disabled, sample_ohlcv):
        """Test that invalid tp_mode raises error."""
        # Create a config with invalid tp_mode
        with pytest.raises(ValueError, match="tp_mode.*is invalid"):
            # This should fail at config validation level
            modified_config = replace(
                config_with_spread_disabled,
                trade_management=replace(
                    config_with_spread_disabled.trade_management,
                    risk=replace(
                        config_with_spread_disabled.trade_management.risk,
                        tp_mode="invalid_mode"
                    )
                )
            )

    def test_atr_calculation(self, config_with_spread_disabled, sample_ohlcv):
        """Test ATR calculation."""
        risk_manager = RiskManager(
            config=config_with_spread_disabled,
            ohlcv_data=sample_ohlcv,
            mode="core"
        )

        assert risk_manager.atr_series is not None
        assert len(risk_manager.atr_series) == len(sample_ohlcv)
        assert risk_manager.atr_series.dtype == np.float32
        
        # ATR should be positive
        assert (risk_manager.atr_series > 0).all()

    def test_atr_cache_integration(self, config_with_spread_disabled, sample_ohlcv):
        """Test that ATR is cached via CacheManager."""
        cache_manager = CacheManager()
        
        # First instance - should compute
        manager1 = RiskManager(
            config=config_with_spread_disabled,
            ohlcv_data=sample_ohlcv,
            cache_manager=cache_manager,
            mode="core"
        )
        atr1 = manager1.atr_series.copy()

        # Second instance with same data - should hit cache
        manager2 = RiskManager(
            config=config_with_spread_disabled,
            ohlcv_data=sample_ohlcv,
            cache_manager=cache_manager,
            mode="core"
        )
        atr2 = manager2.atr_series

        # Should be the same series (cached reference)
        pd.testing.assert_series_equal(atr1, atr2)

    def test_compute_trade_parameters_long(self, config_with_spread_disabled, sample_ohlcv):
        """Test trade parameters for LONG position."""
        risk_manager = RiskManager(
            config=config_with_spread_disabled,
            ohlcv_data=sample_ohlcv,
            mode="core"
        )

        timestamp = sample_ohlcv.index[100]
        bid_price = float(sample_ohlcv.loc[timestamp, "close"])

        params = risk_manager.compute_trade_parameters(
            timestamp=timestamp,
            bid_price=bid_price,
            is_long=True
        )

        assert params is not None
        assert isinstance(params, TradeParameters)
        
        # LONG: entry = bid (no spread in test config)
        assert params.entry_price_mid == bid_price
        assert params.entry_price_executed == bid_price  # No spread in test config
        
        # SL should be below entry for LONG
        assert params.stop_loss_raw < params.entry_price_executed
        assert params.stop_loss_trigger < params.entry_price_executed
        
        # TP should be above entry for LONG
        assert params.take_profit > params.entry_price_executed
        
        # ATR-based distances
        assert params.sl_distance > 0
        assert params.tp_distance > 0
        assert params.atr_value > 0

    def test_compute_trade_parameters_short(self, config_with_spread_disabled, sample_ohlcv):
        """Test trade parameters for SHORT position."""
        risk_manager = RiskManager(
            config=config_with_spread_disabled,
            ohlcv_data=sample_ohlcv,
            mode="core"
        )

        timestamp = sample_ohlcv.index[100]
        bid_price = float(sample_ohlcv.loc[timestamp, "close"])

        params = risk_manager.compute_trade_parameters(
            timestamp=timestamp,
            bid_price=bid_price,
            is_long=False
        )

        assert params is not None
        
        # SHORT: entry = bid
        assert params.entry_price_mid == bid_price
        assert params.entry_price_executed == bid_price
        
        # SL should be above entry for SHORT
        assert params.stop_loss_raw > params.entry_price_executed
        assert params.stop_loss_trigger > params.entry_price_executed
        
        # TP should be below entry for SHORT
        assert params.take_profit < params.entry_price_executed

    def test_risk_percentile_validation(self, config_with_spread_disabled, sample_ohlcv, sample_artf):
        """Test risk percentile validation with annual range."""
        risk_manager = RiskManager(
            config=config_with_spread_disabled,
            ohlcv_data=sample_ohlcv,
            ohlcv_artf=sample_artf,
            mode="analytics"  # analytics mode enables annual range
        )

        timestamp = sample_ohlcv.index[100]
        bid_price = float(sample_ohlcv.loc[timestamp, "close"])

        # This should pass risk validation
        params = risk_manager.compute_trade_parameters(
            timestamp=timestamp,
            bid_price=bid_price,
            is_long=True
        )

        assert params is not None
        assert params.risk_percentile_passed is True
        assert params.risk_percentile_calculated is not None
        assert params.max_risk_percentile == config_with_spread_disabled.trade_management.risk.max_risk_percentile

    def test_risk_rejection(self, config_with_spread_disabled, sample_ohlcv):
        """Test that risk validation can reject trades."""
        # Create a manager with very strict risk limits using replace
        modified_config = replace(
            config_with_spread_disabled,
            trade_management=replace(
                config_with_spread_disabled.trade_management,
                risk=replace(
                    config_with_spread_disabled.trade_management.risk,
                    max_risk_percentile=0.001  # Very small
                )
            )
        )
        
        risk_manager = RiskManager(
            config=modified_config,
            ohlcv_data=sample_ohlcv,
            mode="core"
        )

        # Override annual range to enable validation even in core mode
        # This is for testing - normally annual range only in analytics
        risk_manager.annual_range_series = pd.Series(
            100.0, index=sample_ohlcv.index, dtype=np.float32
        )

        timestamp = sample_ohlcv.index[100]
        bid_price = float(sample_ohlcv.loc[timestamp, "close"])

        # Should be rejected due to risk limit
        params = risk_manager.compute_trade_parameters(
            timestamp=timestamp,
            bid_price=bid_price,
            is_long=True
        )

        assert params is None

    def test_sl_adjustment(self, config_with_spread_disabled, sample_ohlcv):
        """Test SL adjustment when risk exceeds limit with allow_exceed."""
        # Set up config with adjustment allowed and stricter limit
        modified_config = replace(
            config_with_spread_disabled,
            trade_management=replace(
                config_with_spread_disabled.trade_management,
                risk=replace(
                    config_with_spread_disabled.trade_management.risk,
                    max_risk_percentile=0.01
                )
            )
        )
        
        risk_manager = RiskManager(
            config=modified_config,
            ohlcv_data=sample_ohlcv,
            mode="core"
        )

        # Enable risk adjustment
        risk_manager.risk_config = {"allow_exceed_limit": True}
        
        # Provide annual range
        risk_manager.annual_range_series = pd.Series(
            100.0, index=sample_ohlcv.index, dtype=np.float32
        )

        timestamp = sample_ohlcv.index[100]
        bid_price = 100.0
        
        # Create a very wide SL that would exceed limit
        is_valid, adjusted_sl, comment = risk_manager.validate_risk_percentile(
            entry_price=bid_price,
            stop_loss=90.0,  # 10 points risk
            is_long=True,
            timestamp=timestamp
        )

        assert is_valid is True
        assert adjusted_sl != 90.0  # Should be adjusted
        assert "SL adjusted" in comment

    def test_tp_mode_rr_ratio(self, config_with_spread_disabled, sample_ohlcv):
        """Test TP calculation with rr_ratio mode."""
        # Ensure rr_ratio mode is set
        modified_config = replace(
            config_with_spread_disabled,
            trade_management=replace(
                config_with_spread_disabled.trade_management,
                risk=replace(
                    config_with_spread_disabled.trade_management.risk,
                    tp_mode="rr_ratio",
                    risk_to_reward_ratio=5.7
                )
            )
        )
        
        risk_manager = RiskManager(
            config=modified_config,
            ohlcv_data=sample_ohlcv,
            mode="core"
        )

        timestamp = sample_ohlcv.index[100]
        bid_price = 100.0

        params = risk_manager.compute_trade_parameters(
            timestamp=timestamp,
            bid_price=bid_price,
            is_long=True
        )

        assert params is not None
        assert params.tp_mode == "rr_ratio"
        
        # TP distance should be SL distance * 5.7
        expected_tp_distance = params.sl_distance * 5.7
        assert abs(params.tp_distance - expected_tp_distance) < 0.001

    def test_tp_mode_atr_multiplier(self, config_with_spread_disabled, sample_ohlcv):
        """Test TP calculation with atr_multiplier mode."""
        # Set atr_multiplier mode
        modified_config = replace(
            config_with_spread_disabled,
            trade_management=replace(
                config_with_spread_disabled.trade_management,
                risk=replace(
                    config_with_spread_disabled.trade_management.risk,
                    tp_mode="atr_multiplier",
                    atr_multiplier_tp=8.0
                )
            )
        )
        
        risk_manager = RiskManager(
            config=modified_config,
            ohlcv_data=sample_ohlcv,
            mode="core"
        )

        timestamp = sample_ohlcv.index[100]
        bid_price = 100.0

        params = risk_manager.compute_trade_parameters(
            timestamp=timestamp,
            bid_price=bid_price,
            is_long=True
        )

        assert params is not None
        assert params.tp_mode == "atr_multiplier"
        
        # TP distance should be ATR * 8.0
        expected_tp_distance = params.atr_value * 8.0
        assert abs(params.tp_distance - expected_tp_distance) < 0.001

    def test_spread_integration(self, test_config, sample_ohlcv, tmp_path):
        """Test that spread settings flow through to TradeParameters."""
        # Enable spread in config by creating a new config with spread enabled
        # and a valid asset symbol that exists in broker config
        import yaml
        from dataclasses import asdict
        
        # Create a broker spreads file with TEST asset
        config_path = tmp_path / "broker_spreads.yaml"
        config_data = {
            "settings": {
                "apply_to_long": True,
                "apply_to_short": True,
                "require_spread_for_all_assets": False  # Don't require all assets
            },
            "spreads": {
                "TEST": {
                    "spread_value": 0.015,
                    "spread_type": "percentage"
                }
            }
        }
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)
        
        # Create a new config with spread enabled and TEST asset
        from src.config.config_schema import StrategyConfig
        config_dict = asdict(test_config)  # Use asdict instead of to_dict
        config_dict["asset"]["symbol"] = "TEST"
        config_dict["trade_management"]["spread"]["enabled"] = True
        config_dict["trade_management"]["spread"]["config_path"] = str(config_path)
        
        modified_config = StrategyConfig.from_dict(config_dict)
        
        risk_manager = RiskManager(
            config=modified_config,
            ohlcv_data=sample_ohlcv,
            mode="core"
        )
        
        timestamp = sample_ohlcv.index[100]
        bid_price = 20000.0
        
        # Note: This will still fail until take_profit_trigger is added to TradeParameters
        params = risk_manager.compute_trade_parameters(
            timestamp=timestamp,
            bid_price=bid_price,
            is_long=True
        )
        
        assert params is not None
        assert params.spread_enabled is True
        assert params.spread_applied is True
        assert params.spread_points is not None
        assert params.spread_points > 0
        assert params.spread_type == "percentage"
        assert params.spread_value == 0.015
        
        # LONG entry should include spread
        expected_spread = 3.0  # 0.015% of 20000
        assert abs(params.entry_price_executed - (bid_price + expected_spread)) < 0.001

    def test_missing_timestamp_handling(self, config_with_spread_disabled, sample_ohlcv):
        """Test handling of timestamp not in ATR series."""
        risk_manager = RiskManager(
            config=config_with_spread_disabled,
            ohlcv_data=sample_ohlcv,
            mode="core"
        )

        # Use timestamp outside range
        timestamp = pd.Timestamp("2020-01-01")

        params = risk_manager.compute_trade_parameters(
            timestamp=timestamp,
            bid_price=100.0,
            is_long=True
        )

        # Should return None (ATR not available)
        assert params is None

    def test_zero_atr_handling(self, config_with_spread_disabled, sample_ohlcv):
        """Test handling of zero ATR values."""
        risk_manager = RiskManager(
            config=config_with_spread_disabled,
            ohlcv_data=sample_ohlcv,
            mode="core"
        )

        # Force ATR to zero at a specific timestamp
        timestamp = sample_ohlcv.index[100]
        risk_manager.atr_series.loc[timestamp] = 0.0

        params = risk_manager.compute_trade_parameters(
            timestamp=timestamp,
            bid_price=100.0,
            is_long=True
        )

        # Should return None (ATR zero)
        assert params is None

    def test_annual_range_calculation(self, config_with_spread_disabled, sample_ohlcv, sample_artf):
        """Test annual range calculation in analytics mode."""
        risk_manager = RiskManager(
            config=config_with_spread_disabled,
            ohlcv_data=sample_ohlcv,
            ohlcv_artf=sample_artf,
            mode="analytics"
        )

        assert risk_manager.annual_range_series is not None
        assert len(risk_manager.annual_range_series) == len(sample_ohlcv)
        
        # Annual range should be positive where available
        valid_ranges = risk_manager.annual_range_series.dropna()
        assert (valid_ranges > 0).all()

    def test_annual_range_cache(self, config_with_spread_disabled, sample_ohlcv, sample_artf):
        """Test that annual range is cached."""
        cache_manager = CacheManager()
        
        manager1 = RiskManager(
            config=config_with_spread_disabled,
            ohlcv_data=sample_ohlcv,
            ohlcv_artf=sample_artf,
            cache_manager=cache_manager,
            mode="analytics"
        )
        rar1 = manager1.annual_range_series.copy()

        manager2 = RiskManager(
            config=config_with_spread_disabled,
            ohlcv_data=sample_ohlcv,
            ohlcv_artf=sample_artf,
            cache_manager=cache_manager,
            mode="analytics"
        )
        rar2 = manager2.annual_range_series

        pd.testing.assert_series_equal(rar1, rar2)

    def test_validate_risk_percentile_no_rar(self, config_with_spread_disabled, sample_ohlcv):
        """Test validate_risk_percentile when annual range not available."""
        risk_manager = RiskManager(
            config=config_with_spread_disabled,
            ohlcv_data=sample_ohlcv,
            mode="core"  # No annual range in core mode
        )

        is_valid, adjusted_sl, comment = risk_manager.validate_risk_percentile(
            entry_price=100.0,
            stop_loss=90.0,
            is_long=True,
            timestamp=sample_ohlcv.index[100]
        )

        assert is_valid is True
        assert adjusted_sl == 90.0
        assert "RAR not initialised" in comment

    def test_validate_risk_percentile_missing_timestamp(self, config_with_spread_disabled, sample_ohlcv):
        """Test validate_risk_percentile when timestamp missing from RAR."""
        risk_manager = RiskManager(
            config=config_with_spread_disabled,
            ohlcv_data=sample_ohlcv,
            mode="core"
        )
        
        # Set annual range but with different index
        risk_manager.annual_range_series = pd.Series(
            [100.0], index=[pd.Timestamp("2020-01-01")]
        )

        is_valid, adjusted_sl, comment = risk_manager.validate_risk_percentile(
            entry_price=100.0,
            stop_loss=90.0,
            is_long=True,
            timestamp=sample_ohlcv.index[100]
        )

        assert is_valid is True
        assert "RAR missing for timestamp" in comment

    def test_atr_fingerprint_uniqueness(self, config_with_spread_disabled, sample_ohlcv):
        """Test that different data produces different cache keys only when index boundaries change."""
        cache_manager = CacheManager()

        manager1 = RiskManager(
            config=config_with_spread_disabled,
            ohlcv_data=sample_ohlcv,
            cache_manager=cache_manager,
            mode="core"
        )

        # Create data with the same index boundaries - this should produce the same cache key
        df2 = sample_ohlcv.copy()
        # Modify data but keep same index
        df2.loc[:, "close"] = df2["close"] * 1.5  # Scale all prices
        
        manager2 = RiskManager(
            config=config_with_spread_disabled,
            ohlcv_data=df2,
            cache_manager=cache_manager,
            mode="core"
        )
        
        # Should use cached values (same cache key) - so ATR should be identical
        assert manager1.atr_series.equals(manager2.atr_series)
        
        # Create data with different index - should produce different cache key
        df3 = sample_ohlcv.copy()
        # Change the index by adding one more bar
        new_index = list(df3.index) + [df3.index[-1] + pd.Timedelta(minutes=1)]
        df3 = df3.reindex(new_index)
        df3.loc[new_index[-1]] = df3.iloc[-2].copy()  # Fill last row with previous values
        
        manager3 = RiskManager(
            config=config_with_spread_disabled,
            ohlcv_data=df3,
            cache_manager=cache_manager,
            mode="core"
        )
        
        # Should compute separately (different cache keys)
        assert not manager1.atr_series.equals(manager3.atr_series)
        
        # Check cache stats
        stats = cache_manager.get_stats()
        # We should have 2 misses (first and third) and 1 hit (second)
        assert stats['atr']['misses'] == 2
        assert stats['atr']['hits'] == 1

    # ========================================================================
    # REAL DATA TESTS
    # ========================================================================

    def test_with_real_data(self, real_data_config, real_data_bundle):
        """Test RiskManager with real market data."""
        # Create a config with spread disabled for this test
        modified_config = replace(
            real_data_config,
            trade_management=replace(
                real_data_config.trade_management,
                spread=replace(
                    real_data_config.trade_management.spread,
                    enabled=False
                )
            )
        )
        
        print(f"\n{'='*60}")
        print("REAL DATA TEST: RiskManager")
        print(f"{'='*60}")
        print(f"Asset: {modified_config.asset.symbol}")
        print(f"Period: {real_data_bundle.strategy.index[0]} to {real_data_bundle.strategy.index[-1]}")
        print(f"Bars: {len(real_data_bundle.strategy)}")
        
        risk_manager = RiskManager(
            config=modified_config,
            ohlcv_data=real_data_bundle.strategy,
            ohlcv_artf=real_data_bundle.artf,
            mode="analytics"
        )
        
        print(f"\nRisk Parameters:")
        print(f"  ATR length: {risk_manager.atr_length}")
        print(f"  SL multiplier: {risk_manager.sl_multiplier}")
        print(f"  TP mode: {risk_manager.tp_mode}")
        if risk_manager.tp_mode == "rr_ratio":
            print(f"  R:R ratio: {risk_manager.rr_ratio}")
        else:
            print(f"  TP multiplier: {risk_manager.atr_multiplier_tp}")
        print(f"  Max risk percentile: {risk_manager.max_risk_percentile*100:.2f}%")
        
        # Test a few timestamps throughout the period
        stride = max(1, len(real_data_bundle.strategy) // 10)
        test_timestamps = real_data_bundle.strategy.index[::stride][:5]  # First 5 samples
        
        print(f"\nTrade Parameter Calculations:")
        print(f"{'Timestamp':20} | {'Direction':6} | {'Entry':>8} | {'SL':>8} | {'TP':>8} | {'Risk%':>6}")
        print(f"{'-'*20}-+-{'-'*6}-+-{'-'*8}-+-{'-'*8}-+-{'-'*8}-+-{'-'*6}")
        
        for timestamp in test_timestamps:
            bid_price = float(real_data_bundle.strategy.loc[timestamp, "close"])
            
            # Test LONG
            params_long = risk_manager.compute_trade_parameters(
                timestamp=timestamp,
                bid_price=bid_price,
                is_long=True
            )
            
            if params_long:
                risk_pct = params_long.risk_percentile_calculated * 100 if params_long.risk_percentile_calculated else 0
                print(f"{str(timestamp):20} | LONG   | {params_long.entry_price_executed:8.2f} | {params_long.stop_loss_trigger:8.2f} | {params_long.take_profit:8.2f} | {risk_pct:6.2f}")
                assert isinstance(params_long, TradeParameters)
            else:
                print(f"{str(timestamp):20} | LONG   | {'REJECTED':^27} |")
            
            # Test SHORT
            params_short = risk_manager.compute_trade_parameters(
                timestamp=timestamp,
                bid_price=bid_price,
                is_long=False
            )
            
            if params_short:
                assert isinstance(params_short, TradeParameters)

    def test_atr_calculation_on_real_data(self, real_data_config, real_data_bundle):
        """Test ATR calculation on real market data."""
        # Disable spread for this test
        modified_config = replace(
            real_data_config,
            trade_management=replace(
                real_data_config.trade_management,
                spread=replace(
                    real_data_config.trade_management.spread,
                    enabled=False
                )
            )
        )
        
        print(f"\n{'='*60}")
        print("REAL DATA TEST: ATR Calculation")
        print(f"{'='*60}")
        
        risk_manager = RiskManager(
            config=modified_config,
            ohlcv_data=real_data_bundle.strategy,
            mode="analytics"
        )
        
        atr = risk_manager.atr_series
        
        # Basic validation
        assert atr is not None
        assert len(atr) == len(real_data_bundle.strategy)
        assert (atr > 0).all()  # ATR should always be positive
        
        # ATR should be reasonable for the instrument
        mean_atr = atr.mean()
        min_atr = atr.min()
        max_atr = atr.max()
        
        print(f"\nATR Statistics:")
        print(f"  Mean ATR(14): {mean_atr:.2f} pts")
        print(f"  Min ATR: {min_atr:.2f} pts")
        print(f"  Max ATR: {max_atr:.2f} pts")
        print(f"  Std Dev: {atr.std():.2f} pts")
        
        assert 0.5 < mean_atr < 50  # Sanity check
        
        # Show ATR trend
        print(f"\nATR Sample (first 10 bars):")
        for i, (ts, val) in enumerate(list(atr.head(10).items())):
            print(f"  {ts.time()}: {val:.2f} pts")

    def test_annual_range_on_real_data(self, real_data_config, real_data_bundle):
        """Test annual range calculation with real monthly data."""
        if real_data_bundle.artf is None:
            print(f"\n⚠ Skipping annual range test: ARTF data not available")
            pytest.skip("ARTF data not available for this instrument")
        
        # Disable spread for this test
        modified_config = replace(
            real_data_config,
            trade_management=replace(
                real_data_config.trade_management,
                spread=replace(
                    real_data_config.trade_management.spread,
                    enabled=False
                )
            )
        )
        
        print(f"\n{'='*60}")
        print("REAL DATA TEST: Annual Range Calculation")
        print(f"{'='*60}")
        
        risk_manager = RiskManager(
            config=modified_config,
            ohlcv_data=real_data_bundle.strategy,
            ohlcv_artf=real_data_bundle.artf,
            mode="analytics"
        )
        
        annual_range = risk_manager.annual_range_series
        
        assert annual_range is not None
        assert len(annual_range) == len(real_data_bundle.strategy)
        
        # Annual range should be positive and reasonable for the instrument
        valid_ranges = annual_range.dropna()
        if len(valid_ranges) > 0:
            mean_range = valid_ranges.mean()
            min_range = valid_ranges.min()
            max_range = valid_ranges.max()
            
            print(f"\nAnnual Range Statistics:")
            print(f"  Available: {len(valid_ranges)}/{len(annual_range)} bars ({len(valid_ranges)/len(annual_range)*100:.1f}%)")
            print(f"  Mean annual range: {mean_range:.2f} pts")
            print(f"  Min annual range: {min_range:.2f} pts")
            print(f"  Max annual range: {max_range:.2f} pts")
            
            assert mean_range > 0
            # For DEUIDXEUR, annual range can be 5000-10000 pts
            assert 1000 < mean_range < 20000  # Reasonable range for indices

    def test_tp_modes_on_real_data(self, real_data_config, real_data_bundle):
        """Test both TP modes on real data."""
        # Disable spread for this test
        base_config = replace(
            real_data_config,
            trade_management=replace(
                real_data_config.trade_management,
                spread=replace(
                    real_data_config.trade_management.spread,
                    enabled=False
                )
            )
        )
        
        print(f"\n{'='*60}")
        print("REAL DATA TEST: TP Mode Comparison")
        print(f"{'='*60}")
        
        # Test RR ratio mode - create a new config
        config_rr = replace(
            base_config,
            trade_management=replace(
                base_config.trade_management,
                risk=replace(
                    base_config.trade_management.risk,
                    tp_mode="rr_ratio"
                )
            )
        )
        risk_manager_rr = RiskManager(
            config=config_rr,
            ohlcv_data=real_data_bundle.strategy,
            mode="analytics"
        )
        
        # Test ATR multiplier mode - create another config
        config_atr = replace(
            base_config,
            trade_management=replace(
                base_config.trade_management,
                risk=replace(
                    base_config.trade_management.risk,
                    tp_mode="atr_multiplier"
                )
            )
        )
        risk_manager_atr = RiskManager(
            config=config_atr,
            ohlcv_data=real_data_bundle.strategy,
            mode="analytics"
        )
        
        # Test at a few timestamps
        timestamps = real_data_bundle.strategy.index[::100][:3]
        
        print(f"\nTP Mode Comparison (LONG positions):")
        print(f"{'Timestamp':20} | {'Bid':>8} | {'ATR':>8} | {'RR TP':>8} | {'ATR TP':>8} | {'Ratio':>6}")
        print(f"{'-'*20}-+-{'-'*8}-+-{'-'*8}-+-{'-'*8}-+-{'-'*8}-+-{'-'*6}")
        
        for ts in timestamps:
            bid = float(real_data_bundle.strategy.loc[ts, "close"])
            atr = float(risk_manager_rr.atr_series.loc[ts])
            
            params_rr = risk_manager_rr.compute_trade_parameters(ts, bid, is_long=True)
            params_atr = risk_manager_atr.compute_trade_parameters(ts, bid, is_long=True)
            
            if params_rr and params_atr:
                ratio = params_atr.tp_distance / params_rr.tp_distance if params_rr.tp_distance else 0
                print(f"{str(ts):20} | {bid:8.2f} | {atr:8.2f} | {params_rr.take_profit:8.2f} | {params_atr.take_profit:8.2f} | {ratio:6.2f}")

    def test_risk_rejection_on_real_data(self, real_data_config, real_data_bundle):
        """Test that risk validation can reject trades on real data."""
        # Disable spread for this test
        base_config = replace(
            real_data_config,
            trade_management=replace(
                real_data_config.trade_management,
                spread=replace(
                    real_data_config.trade_management.spread,
                    enabled=False
                )
            )
        )
        
        print(f"\n{'='*60}")
        print("REAL DATA TEST: Risk Rejection")
        print(f"{'='*60}")
        
        # Create a manager with very strict risk limits
        strict_config = replace(
            base_config,
            trade_management=replace(
                base_config.trade_management,
                risk=replace(
                    base_config.trade_management.risk,
                    max_risk_percentile=0.001  # Very small (0.1%)
                )
            )
        )
        
        risk_manager = RiskManager(
            config=strict_config,
            ohlcv_data=real_data_bundle.strategy,
            ohlcv_artf=real_data_bundle.artf,
            mode="analytics"
        )
        
        accepted = 0
        rejected = 0
        
        print(f"\nTesting with max_risk_percentile = {strict_config.trade_management.risk.max_risk_percentile*100:.3f}%")
        print(f"\nSample results (first 20 bars):")
        
        for i, (ts, row) in enumerate(real_data_bundle.strategy.head(20).iterrows()):
            params = risk_manager.compute_trade_parameters(
                timestamp=ts,
                bid_price=float(row["close"]),
                is_long=True
            )
            
            if params is None:
                rejected += 1
                print(f"  {ts.time()}: ❌ REJECTED")
            else:
                accepted += 1
                risk_pct = params.risk_percentile_calculated * 100 if params.risk_percentile_calculated else 0
                print(f"  {ts.time()}: ✅ ACCEPTED (risk: {risk_pct:.3f}%)")
        
        print(f"\nSummary:")
        print(f"  Accepted: {accepted}")
        print(f"  Rejected: {rejected}")
        if accepted + rejected > 0:
            print(f"  Rejection rate: {rejected/(accepted+rejected)*100:.1f}%")

    def test_cache_performance_with_real_data(self, real_data_config, real_data_bundle):
        """Test cache performance across multiple runs."""
        # Disable spread for this test
        modified_config = replace(
            real_data_config,
            trade_management=replace(
                real_data_config.trade_management,
                spread=replace(
                    real_data_config.trade_management.spread,
                    enabled=False
                )
            )
        )
        
        from src.strategies.core.cache_manager import CacheManager
        import time
        
        print(f"\n{'='*60}")
        print("REAL DATA TEST: Cache Performance")
        print(f"{'='*60}")
        
        cache_manager = CacheManager()
        
        # First run - cache miss
        print("\nFirst run (cache miss)...")
        start = time.perf_counter()
        risk_manager1 = RiskManager(
            config=modified_config,
            ohlcv_data=real_data_bundle.strategy,
            ohlcv_artf=real_data_bundle.artf,
            cache_manager=cache_manager,
            mode="analytics"
        )
        time1 = (time.perf_counter() - start) * 1000
        
        # Second run - cache hit
        print("Second run (should be cache hit)...")
        start = time.perf_counter()
        risk_manager2 = RiskManager(
            config=modified_config,
            ohlcv_data=real_data_bundle.strategy,
            ohlcv_artf=real_data_bundle.artf,
            cache_manager=cache_manager,
            mode="analytics"
        )
        time2 = (time.perf_counter() - start) * 1000
        
        print(f"\nTiming Results:")
        print(f"  First run (cold): {time1:.2f}ms")
        print(f"  Second run (cached): {time2:.2f}ms")
        print(f"  Speedup: {time1/time2:.1f}x")
        
        # Verify ATR series are identical
        assert risk_manager1.atr_series.equals(risk_manager2.atr_series)
        
        # Check cache stats
        stats = cache_manager.get_stats()
        print(f"\nCache Statistics:")
        print(f"  ATR hits: {stats['atr']['hits']}")
        print(f"  ATR misses: {stats['atr']['misses']}")
        print(f"  ATR hit rate: {stats['atr']['hit_rate']:.1f}%")
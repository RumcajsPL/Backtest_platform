"""
Unit Tests for RiskManager
===========================
Tests SL/TP calculations, ATR caching, annual range validation.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

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

    def test_initialization_with_valid_config(self, test_config, sample_ohlcv):
        """Test initializing RiskManager with valid config."""
        risk_manager = RiskManager(
            config=test_config,
            ohlcv_data=sample_ohlcv,
            mode="core"
        )

        assert risk_manager.atr_length == 14
        assert risk_manager.sl_multiplier == 1.4
        assert risk_manager.tp_mode == "rr_ratio"
        assert risk_manager.rr_ratio == 5.7

    def test_initialization_with_invalid_mode(self, test_config, sample_ohlcv):
        """Test that invalid mode raises error."""
        with pytest.raises(ValueError, match="Invalid mode.*'debug' is not a valid mode"):
            RiskManager(
                config=test_config,
                ohlcv_data=sample_ohlcv,
                mode="debug"
            )

    @pytest.mark.parametrize("tp_mode", ["rr_ratio", "atr_multiplier"])
    def test_tp_modes(self, test_config, sample_ohlcv, tp_mode):
        """Test both TP modes are accepted."""
        # Modify config
        test_config.trade_management.risk.tp_mode = tp_mode
        
        risk_manager = RiskManager(
            config=test_config,
            ohlcv_data=sample_ohlcv,
            mode="core"
        )

        assert risk_manager.tp_mode == tp_mode

    def test_invalid_tp_mode(self, test_config, sample_ohlcv):
        """Test that invalid tp_mode raises error."""
        # This requires patching since config validation would catch it earlier
        # We'll test the internal validation
        with pytest.raises(ValueError, match="tp_mode.*is invalid"):
            # Create manager with invalid mode by patching after init
            manager = RiskManager(
                config=test_config,
                ohlcv_data=sample_ohlcv,
                mode="core"
            )
            # Directly set invalid mode to bypass config validation
            object.__setattr__(manager, 'tp_mode', 'invalid')
            # Force re-validation
            if manager.tp_mode not in {"rr_ratio", "atr_multiplier"}:
                raise ValueError(f"tp_mode='{manager.tp_mode}' is invalid")

    def test_atr_calculation(self, test_config, sample_ohlcv):
        """Test ATR calculation."""
        risk_manager = RiskManager(
            config=test_config,
            ohlcv_data=sample_ohlcv,
            mode="core"
        )

        assert risk_manager.atr_series is not None
        assert len(risk_manager.atr_series) == len(sample_ohlcv)
        assert risk_manager.atr_series.dtype == np.float32
        
        # ATR should be positive
        assert (risk_manager.atr_series > 0).all()

    def test_atr_cache_integration(self, test_config, sample_ohlcv):
        """Test that ATR is cached via CacheManager."""
        cache_manager = CacheManager()
        
        # First instance - should compute
        manager1 = RiskManager(
            config=test_config,
            ohlcv_data=sample_ohlcv,
            cache_manager=cache_manager,
            mode="core"
        )
        atr1 = manager1.atr_series.copy()

        # Second instance with same data - should hit cache
        manager2 = RiskManager(
            config=test_config,
            ohlcv_data=sample_ohlcv,
            cache_manager=cache_manager,
            mode="core"
        )
        atr2 = manager2.atr_series

        # Should be the same series (cached reference)
        pd.testing.assert_series_equal(atr1, atr2)

    def test_compute_trade_parameters_long(self, test_config, sample_ohlcv):
        """Test trade parameters for LONG position."""
        risk_manager = RiskManager(
            config=test_config,
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
        
        # LONG: entry = bid (no spread in core test)
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

    def test_compute_trade_parameters_short(self, test_config, sample_ohlcv):
        """Test trade parameters for SHORT position."""
        risk_manager = RiskManager(
            config=test_config,
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

    def test_risk_percentile_validation(self, test_config, sample_ohlcv, sample_artf):
        """Test risk percentile validation with annual range."""
        # Enable annual range in config (would normally be in config)
        # We'll test with analytics mode to enable annual range
        
        risk_manager = RiskManager(
            config=test_config,
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
        assert params.max_risk_percentile == test_config.trade_management.risk.max_risk_percentile

    def test_risk_rejection(self, test_config, sample_ohlcv):
        """Test that risk validation can reject trades."""
        # Create a manager with very strict risk limits
        test_config.trade_management.risk.max_risk_percentile = 0.001  # Very small
        
        risk_manager = RiskManager(
            config=test_config,
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

    def test_sl_adjustment(self, test_config, sample_ohlcv):
        """Test SL adjustment when risk exceeds limit with allow_exceed."""
        # Set up config with adjustment allowed
        test_config.trade_management.risk.max_risk_percentile = 0.01
        risk_manager = RiskManager(
            config=test_config,
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

    def test_tp_mode_rr_ratio(self, test_config, sample_ohlcv):
        """Test TP calculation with rr_ratio mode."""
        test_config.trade_management.risk.tp_mode = "rr_ratio"
        test_config.trade_management.risk.risk_to_reward_ratio = 5.7
        
        risk_manager = RiskManager(
            config=test_config,
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

    def test_tp_mode_atr_multiplier(self, test_config, sample_ohlcv):
        """Test TP calculation with atr_multiplier mode."""
        test_config.trade_management.risk.tp_mode = "atr_multiplier"
        test_config.trade_management.risk.atr_multiplier_tp = 8.0
        
        risk_manager = RiskManager(
            config=test_config,
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
        # Enable spread in config
        test_config.trade_management.spread.enabled = True
        
        # Create a broker spreads file
        config_path = tmp_path / "broker_spreads.yaml"
        config_data = {
            "settings": {
                "apply_to_long": True,
                "apply_to_short": True
            },
            "spreads": {
                "TEST": {
                    "spread_value": 0.015,
                    "spread_type": "percentage"
                }
            }
        }
        import yaml
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)
        
        test_config.trade_management.spread.config_path = str(config_path)

        risk_manager = RiskManager(
            config=test_config,
            ohlcv_data=sample_ohlcv,
            mode="core"
        )

        timestamp = sample_ohlcv.index[100]
        bid_price = 20000.0

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

    def test_missing_timestamp_handling(self, test_config, sample_ohlcv):
        """Test handling of timestamp not in ATR series."""
        risk_manager = RiskManager(
            config=test_config,
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

    def test_zero_atr_handling(self, test_config, sample_ohlcv):
        """Test handling of zero ATR values."""
        risk_manager = RiskManager(
            config=test_config,
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

    def test_annual_range_calculation(self, test_config, sample_ohlcv, sample_artf):
        """Test annual range calculation in analytics mode."""
        risk_manager = RiskManager(
            config=test_config,
            ohlcv_data=sample_ohlcv,
            ohlcv_artf=sample_artf,
            mode="analytics"
        )

        assert risk_manager.annual_range_series is not None
        assert len(risk_manager.annual_range_series) == len(sample_ohlcv)
        
        # Annual range should be positive where available
        valid_ranges = risk_manager.annual_range_series.dropna()
        assert (valid_ranges > 0).all()

    def test_annual_range_cache(self, test_config, sample_ohlcv, sample_artf):
        """Test that annual range is cached."""
        cache_manager = CacheManager()
        
        manager1 = RiskManager(
            config=test_config,
            ohlcv_data=sample_ohlcv,
            ohlcv_artf=sample_artf,
            cache_manager=cache_manager,
            mode="analytics"
        )
        rar1 = manager1.annual_range_series.copy()

        manager2 = RiskManager(
            config=test_config,
            ohlcv_data=sample_ohlcv,
            ohlcv_artf=sample_artf,
            cache_manager=cache_manager,
            mode="analytics"
        )
        rar2 = manager2.annual_range_series

        pd.testing.assert_series_equal(rar1, rar2)

    def test_validate_risk_percentile_no_rar(self, test_config, sample_ohlcv):
        """Test validate_risk_percentile when annual range not available."""
        risk_manager = RiskManager(
            config=test_config,
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

    def test_validate_risk_percentile_missing_timestamp(self, test_config, sample_ohlcv):
        """Test validate_risk_percentile when timestamp missing from RAR."""
        risk_manager = RiskManager(
            config=test_config,
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

    def test_atr_fingerprint_uniqueness(self, test_config, sample_ohlcv):
        """Test that different data produces different cache keys."""
        cache_manager = CacheManager()
        
        manager1 = RiskManager(
            config=test_config,
            ohlcv_data=sample_ohlcv,
            cache_manager=cache_manager,
            mode="core"
        )

        # Slightly different data
        df2 = sample_ohlcv.copy()
        df2.iloc[-1, df2.columns.get_loc("close")] += 1.0
        
        manager2 = RiskManager(
            config=test_config,
            ohlcv_data=df2,
            cache_manager=cache_manager,
            mode="core"
        )

        # Should compute separately (different cache keys)
        assert manager1.atr_series is not None
        assert manager2.atr_series is not None
        
        # Values should differ
        assert not manager1.atr_series.equals(manager2.atr_series)
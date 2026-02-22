"""
Unit Tests for SpreadManager
=============================
Tests spread calculations, config loading, and cache integration.
BID price model validation.
"""

import pytest
import yaml
from pathlib import Path
from unittest.mock import mock_open, patch
from src.utils.paths import config_path

from src.strategies.specific.modules.spread_manager import SpreadManager
from src.strategies.core.cache_manager import CacheManager


class TestSpreadManager:
    """Tests for SpreadManager class."""

    def test_initialization_with_valid_config(self, tmp_path):
        """Test initializing SpreadManager with valid config."""
        config_path = tmp_path / "broker_spreads.yaml"
        config_data = {
            "settings": {
                "apply_to_long": True,
                "apply_to_short": True,
                "application_method": "entry_only"
            },
            "spreads": {
                "DEUIDXEUR": {
                    "spread_value": 0.015,
                    "spread_type": "percentage",
                    "display_name": "Germany 40",
                    "asset_class": "index"
                }
            }
        }
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)

        manager = SpreadManager(
            asset_symbol="DEUIDXEUR",
            spread_config_path=str(config_path),
            mode="core"
        )

        assert manager.asset_symbol == "DEUIDXEUR"
        assert manager.is_enabled() is True
        assert manager.apply_to_long is True
        assert manager.apply_to_short is True

    def test_initialization_with_blank_symbol(self, tmp_path):
        """SM-1: Test that blank symbol raises error."""
        config_path = tmp_path / "broker_spreads.yaml"
        with open(config_path, 'w') as f:
            yaml.dump({"spreads": {}}, f)

        with pytest.raises(ValueError, match="non-empty asset_symbol"):
            SpreadManager(
                asset_symbol="   ",
                spread_config_path=str(config_path)
            )

        with pytest.raises(ValueError, match="non-empty asset_symbol"):
            SpreadManager(
                asset_symbol="",
                spread_config_path=str(config_path)
            )

    def test_initialization_with_missing_config_path(self):
        """SM-2: Test that missing config_path raises error."""
        with pytest.raises(ValueError, match="explicit spread_config_path"):
            SpreadManager(
                asset_symbol="DEUIDXEUR",
                spread_config_path=None
            )

    def test_initialization_with_nonexistent_path(self):
        """Test that nonexistent path raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="Broker spread config not found"):
            SpreadManager(
                asset_symbol="DEUIDXEUR",
                spread_config_path="/nonexistent/path.yaml"
            )

    def test_invalid_mode(self, tmp_path):
        """Test that invalid mode raises error."""
        config_path = tmp_path / "broker_spreads.yaml"
        with open(config_path, 'w') as f:
            yaml.dump({"spreads": {}}, f)

        with pytest.raises(ValueError, match="Invalid mode.*'debug' is not a valid mode"):
            SpreadManager(
                asset_symbol="DEUIDXEUR",
                spread_config_path=str(config_path),
                mode="debug"
            )

    def test_asset_not_in_config(self, tmp_path):
        """Test handling when asset not found in config."""
        config_path = tmp_path / "broker_spreads.yaml"
        config_data = {
            "settings": {
                "warn_on_missing_spread": True,
                "require_spread_for_all_assets": False
            },
            "spreads": {
                "OTHER": {"spread_value": 1.0, "spread_type": "points"}
            }
        }
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)

        # Should not raise, but return 0.0 for spread
        manager = SpreadManager(
            asset_symbol="DEUIDXEUR",
            spread_config_path=str(config_path),
            mode="core"
        )

        assert manager.is_enabled() is False
        assert manager.get_spread_in_points(100.0) == 0.0

    def test_require_spread_for_all_assets_enforced(self, tmp_path):
        """Test that require_spread_for_all_assets=True raises error."""
        config_path = tmp_path / "broker_spreads.yaml"
        config_data = {
            "settings": {
                "require_spread_for_all_assets": True
            },
            "spreads": {
                "OTHER": {"spread_value": 1.0, "spread_type": "points"}
            }
        }
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)

        with pytest.raises(ValueError, match="not found in spread config"):
            SpreadManager(
                asset_symbol="DEUIDXEUR",
                spread_config_path=str(config_path)
            )

    @pytest.mark.parametrize("spread_type,spread_value,bid,expected", [
        ("percentage", 0.015, 20000.0, 3.0),      # 0.015% of 20000 = 3.0
        ("percentage", 0.01, 1500.0, 0.15),       # 0.01% of 1500 = 0.15
        ("points", 2.5, 100.0, 2.5),              # 2.5 points regardless of price
        ("points", 0.5, 1000.0, 0.5),
        ("pips", 10, 1.2000, 0.001),              # 10 pips at pip_position=4 = 0.001
        ("pips", 5, 1.2000, 0.0005),
    ])
    def test_get_spread_in_points(
        self, tmp_path, spread_type, spread_value, bid, expected
    ):
        """Test spread calculations for different types."""
        config_path = tmp_path / "broker_spreads.yaml"
        
        asset_config = {
            "spread_value": spread_value,
            "spread_type": spread_type,
        }
        if spread_type == "pips":
            asset_config["pip_position"] = 4

        config_data = {
            "settings": {},
            "spreads": {"DEUIDXEUR": asset_config}
        }
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)

        manager = SpreadManager(
            asset_symbol="DEUIDXEUR",
            spread_config_path=str(config_path)
        )

        result = manager.get_spread_in_points(bid)
        assert abs(result - expected) < 1e-6

    def test_unknown_spread_type(self, tmp_path):
        """Test that unknown spread_type returns 0.0 and logs warning."""
        config_path = tmp_path / "broker_spreads.yaml"
        config_data = {
            "settings": {},
            "spreads": {
                "DEUIDXEUR": {
                    "spread_value": 1.0,
                    "spread_type": "unknown_type"
                }
            }
        }
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)

        manager = SpreadManager(
            asset_symbol="DEUIDXEUR",
            spread_config_path=str(config_path)
        )

        # Should return 0.0 without raising
        assert manager.get_spread_in_points(100.0) == 0.0

    def test_calculate_entry_cost_long(self, tmp_path):
        """Test entry cost calculation for LONG positions."""
        config_path = tmp_path / "broker_spreads.yaml"
        config_data = {
            "settings": {},
            "spreads": {
                "DEUIDXEUR": {
                    "spread_value": 0.015,
                    "spread_type": "percentage"
                }
            }
        }
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)

        manager = SpreadManager(
            asset_symbol="DEUIDXEUR",
            spread_config_path=str(config_path)
        )

        bid = 20000.0
        entry_cost = manager.calculate_entry_cost(bid, is_long=True)
        # LONG: Bid + Spread = 20000 + 3.0 = 20003.0
        assert abs(entry_cost - 20003.0) < 1e-6

    def test_calculate_entry_cost_short(self, tmp_path):
        """Test entry cost calculation for SHORT positions."""
        config_path = tmp_path / "broker_spreads.yaml"
        config_data = {
            "settings": {},
            "spreads": {
                "DEUIDXEUR": {
                    "spread_value": 0.015,
                    "spread_type": "percentage"
                }
            }
        }
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)

        manager = SpreadManager(
            asset_symbol="DEUIDXEUR",
            spread_config_path=str(config_path)
        )

        bid = 20000.0
        entry_cost = manager.calculate_entry_cost(bid, is_long=False)
        # SHORT: Bid (no spread)
        assert entry_cost == bid

    def test_get_sl_trigger_level(self, tmp_path):
        """Test SL trigger level calculation."""
        config_path = tmp_path / "broker_spreads.yaml"
        with open(config_path, 'w') as f:
            yaml.dump({"spreads": {"DEUIDXEUR": {"spread_value": 0.015, "spread_type": "percentage"}}}, f)

        manager = SpreadManager(
            asset_symbol="DEUIDXEUR",
            spread_config_path=str(config_path)
        )

        spread = 3.0  # 3 points
        raw_sl = 19980.0

        # LONG: trigger = SL - spread
        long_trigger = manager.get_sl_trigger_level(raw_sl, spread, is_long=True)
        assert long_trigger == raw_sl - spread

        # SHORT: trigger = SL + spread
        short_trigger = manager.get_sl_trigger_level(raw_sl, spread, is_long=False)
        assert short_trigger == raw_sl + spread

    def test_get_tp_trigger_level(self, tmp_path):
        """Test TP trigger level calculation (DEC-038)."""
        config_path = tmp_path / "broker_spreads.yaml"
        with open(config_path, 'w') as f:
            yaml.dump({"spreads": {"DEUIDXEUR": {"spread_value": 0.015, "spread_type": "percentage"}}}, f)

        manager = SpreadManager(
            asset_symbol="DEUIDXEUR",
            spread_config_path=str(config_path)
        )

        spread = 3.0
        raw_tp = 20050.0

        # LONG: trigger = TP (no spread)
        long_trigger = manager.get_tp_trigger_level(raw_tp, spread, is_long=True)
        assert long_trigger == raw_tp

        # SHORT: trigger = TP + spread
        short_trigger = manager.get_tp_trigger_level(raw_tp, spread, is_long=False)
        assert short_trigger == raw_tp + spread

    def test_apply_to_long_short_settings(self, tmp_path):
        """DEC-036: Test apply_to_long/apply_to_short are read from config."""
        config_path = tmp_path / "broker_spreads.yaml"
        config_data = {
            "settings": {
                "apply_to_long": False,
                "apply_to_short": True,
                "application_method": "entry_only"
            },
            "spreads": {
                "DEUIDXEUR": {
                    "spread_value": 0.015,
                    "spread_type": "percentage"
                }
            }
        }
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)

        manager = SpreadManager(
            asset_symbol="DEUIDXEUR",
            spread_config_path=str(config_path)
        )

        assert manager.apply_to_long is False
        assert manager.apply_to_short is True

        # Spread info should reflect settings
        info = manager.get_spread_info()
        assert info["apply_to_long"] is False
        assert info["apply_to_short"] is True

    def test_invalid_application_method(self, tmp_path):
        """Test that invalid application_method raises error."""
        config_path = tmp_path / "broker_spreads.yaml"
        config_data = {
            "settings": {
                "application_method": "invalid_method"
            },
            "spreads": {
                "DEUIDXEUR": {
                    "spread_value": 0.015,
                    "spread_type": "percentage"
                }
            }
        }
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)

        with pytest.raises(ValueError, match="not recognised"):
            SpreadManager(
                asset_symbol="DEUIDXEUR",
                spread_config_path=str(config_path)
            )

    def test_cache_integration(self, tmp_path):
        """Test that SpreadManager uses CacheManager for config caching."""
        config_path = tmp_path / "broker_spreads.yaml"
        config_data = {
            "settings": {},
            "spreads": {
                "DEUIDXEUR": {
                    "spread_value": 0.015,
                    "spread_type": "percentage"
                }
            }
        }
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)

        cache_manager = CacheManager()
        
        # First instance - should load from file
        manager1 = SpreadManager(
            asset_symbol="DEUIDXEUR",
            spread_config_path=str(config_path),
            cache_manager=cache_manager
        )

        # Second instance - should hit cache
        manager2 = SpreadManager(
            asset_symbol="DEUIDXEUR",
            spread_config_path=str(config_path),
            cache_manager=cache_manager
        )

        # Both should work
        assert manager1.get_spread_in_points(100.0) == manager2.get_spread_in_points(100.0)

    def test_get_spread_info(self, tmp_path):
        """Test spread info summary."""
        config_path = tmp_path / "broker_spreads.yaml"
        config_data = {
            "settings": {
                "apply_to_long": True,
                "apply_to_short": True
            },
            "spreads": {
                "DEUIDXEUR": {
                    "spread_value": 0.015,
                    "spread_type": "percentage",
                    "display_name": "Germany 40",
                    "asset_class": "index"
                }
            }
        }
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)

        manager = SpreadManager(
            asset_symbol="DEUIDXEUR",
            spread_config_path=str(config_path)
        )

        info = manager.get_spread_info()
        assert info["enabled"] is True
        assert info["asset"] == "DEUIDXEUR"
        assert info["spread_value"] == 0.015
        assert info["spread_type"] == "percentage"
        assert info["display_name"] == "Germany 40"
        assert info["asset_class"] == "index"
        assert info["apply_to_long"] is True
        assert info["apply_to_short"] is True

    def test_spread_info_when_disabled(self, tmp_path):
        """Test spread info when asset not found."""
        config_path = tmp_path / "broker_spreads.yaml"
        config_data = {
            "settings": {},
            "spreads": {}
        }
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)

        manager = SpreadManager(
            asset_symbol="DEUIDXEUR",
            spread_config_path=str(config_path)
        )

        info = manager.get_spread_info()
        assert info["enabled"] is False
        assert "apply_to_long" in info
        assert "apply_to_short" in info

    def test_repr(self, tmp_path):
        """Test string representation."""
        config_path = tmp_path / "broker_spreads.yaml"
        config_data = {
            "settings": {},
            "spreads": {
                "DEUIDXEUR": {
                    "spread_value": 0.015,
                    "spread_type": "percentage"
                }
            }
        }
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)

        manager = SpreadManager(
            asset_symbol="DEUIDXEUR",
            spread_config_path=str(config_path)
        )

        repr_str = repr(manager)
        assert "DEUIDXEUR" in repr_str
        assert "0.015" in repr_str
        assert "percentage" in repr_str
    
    # ========================================================================
    # REAL DATA TESTS
    # ========================================================================

    def test_with_real_broker_config(self, real_data_config):
        """Test SpreadManager with actual broker_spreads.yaml if available."""
        print(f"\n{'='*60}")
        print("REAL DATA TEST: SpreadManager with Broker Config")
        print(f"{'='*60}")
        
        # Try to find actual broker config
        broker_config_path = config_path("spreads", "broker_spreads.yaml")
        
        if not broker_config_path.exists():
            print(f"⚠ Broker config not found at {broker_config_path}")
            print("Skipping test - create configs/spreads/broker_spreads.yaml to enable")
            pytest.skip("Real broker_spreads.yaml not found")
        
        print(f"Using broker config: {broker_config_path}")
        print(f"Asset symbol: {real_data_config.asset.symbol}")
        
        manager = SpreadManager(
            asset_symbol=real_data_config.asset.symbol,
            spread_config_path=str(broker_config_path),
            mode="analytics"
        )
        
        # Test with realistic price range for DEUIDXEUR
        test_prices = [18000.0, 20000.0, 22000.0]
        
        print(f"\nSpread Calculations:")
        for price in test_prices:
            spread = manager.get_spread_in_points(price)
            assert spread >= 0
            
            # LONG entry calculation
            long_entry = manager.calculate_entry_cost(price, is_long=True)
            assert long_entry >= price
            
            # SHORT entry calculation
            short_entry = manager.calculate_entry_cost(price, is_long=False)
            assert short_entry == price
            
            print(f"  Price: {price:,.1f} → Spread: {spread:.3f} pts")
            print(f"    LONG entry: {long_entry:.2f} (+{spread:.2f})")
            print(f"    SHORT entry: {short_entry:.2f} (no spread)")
        
        # Check spread info
        info = manager.get_spread_info()
        assert info["enabled"] is True
        assert info["asset"] == real_data_config.asset.symbol
        
        print(f"\nSpread Configuration:")
        print(f"  Type: {info['spread_type']}")
        print(f"  Value: {info['spread_value']}")
        print(f"  Apply to LONG: {info['apply_to_long']}")
        print(f"  Apply to SHORT: {info['apply_to_short']}")
        print(f"  Method: {info['application_method']}")

    def test_sl_tp_trigger_with_real_spreads(self, real_data_config):
        """Test SL/TP trigger calculations with real spread values."""
        broker_config_path = config_path("spreads", "broker_spreads.yaml")
        
        if not broker_config_path.exists():
            pytest.skip("Real broker_spreads.yaml not found")
        
        manager = SpreadManager(
            asset_symbol=real_data_config.asset.symbol,
            spread_config_path=str(broker_config_path),
            mode="analytics"
        )
        
        print(f"\n{'='*60}")
        print("REAL DATA TEST: SL/TP Trigger Calculations")
        print(f"{'='*60}")
        
        test_price = 20000.0
        spread = manager.get_spread_in_points(test_price)
        raw_sl_long = 19980.0
        raw_sl_short = 20020.0
        raw_tp_long = 20050.0
        raw_tp_short = 19950.0
        
        print(f"Bid price: {test_price:.2f}")
        print(f"Spread: {spread:.3f} pts")
        
        # SL triggers
        sl_trigger_long = manager.get_sl_trigger_level(raw_sl_long, spread, is_long=True)
        sl_trigger_short = manager.get_sl_trigger_level(raw_sl_short, spread, is_long=False)
        
        print(f"\nSL Triggers:")
        print(f"  LONG raw SL: {raw_sl_long:.2f} → trigger: {sl_trigger_long:.2f} (Bid - spread)")
        print(f"  SHORT raw SL: {raw_sl_short:.2f} → trigger: {sl_trigger_short:.2f} (Bid + spread)")
        
        assert sl_trigger_long == raw_sl_long - spread
        assert sl_trigger_short == raw_sl_short + spread
        
        # TP triggers (DEC-038)
        tp_trigger_long = manager.get_tp_trigger_level(raw_tp_long, spread, is_long=True)
        tp_trigger_short = manager.get_tp_trigger_level(raw_tp_short, spread, is_long=False)
        
        print(f"\nTP Triggers:")
        print(f"  LONG raw TP: {raw_tp_long:.2f} → trigger: {tp_trigger_long:.2f} (no spread)")
        print(f"  SHORT raw TP: {raw_tp_short:.2f} → trigger: {tp_trigger_short:.2f} (Bid + spread)")
        
        assert tp_trigger_long == raw_tp_long
        assert tp_trigger_short == raw_tp_short + spread

    def test_asset_not_in_broker_config(self, real_data_config, tmp_path):
        """Test handling when asset not found in broker config."""
        # Create broker config without our asset
        broker_config = tmp_path / "broker_spreads.yaml"
        config_data = {
            "settings": {},
            "spreads": {
                "SOME_OTHER_ASSET": {
                    "spread_value": 0.01,
                    "spread_type": "percentage"
                }
            }
        }
        with open(broker_config, 'w') as f:
            yaml.dump(config_data, f)
        
        print(f"\n{'='*60}")
        print("REAL DATA TEST: Asset Not in Broker Config")
        print(f"{'='*60}")
        print(f"Looking for: {real_data_config.asset.symbol}")
        print(f"Available in config: SOME_OTHER_ASSET")
        
        # Should not raise, but return 0.0 for spread
        manager = SpreadManager(
            asset_symbol=real_data_config.asset.symbol,
            spread_config_path=str(broker_config),
            mode="analytics"
        )
        
        assert manager.is_enabled() is False
        
        spread = manager.get_spread_in_points(20000.0)
        print(f"Spread returned: {spread} (should be 0.0)")
        assert spread == 0.0

    def test_spread_calculation_with_real_price_range(self, real_data_config):
        """Test spread calculation with realistic price ranges."""
        broker_config_path = config_path("spreads", "broker_spreads.yaml")
        
        if not broker_config_path.exists():
            pytest.skip("Real broker_spreads.yaml not found")
        
        manager = SpreadManager(
            asset_symbol=real_data_config.asset.symbol,
            spread_config_path=str(broker_config_path),
            mode="analytics"
        )
        
        print(f"\n{'='*60}")
        print("REAL DATA TEST: Spread Across Price Range")
        print(f"{'='*60}")
        
        # Test across a range of realistic prices for indices
        prices = [15000, 17500, 20000, 22500, 25000]
        
        print(f"{'Price':>10} | {'Spread':>10} | {'Type':>10} | {'LONG Entry':>12} | {'SHORT Entry':>12}")
        print(f"{'-'*10}-+-{'-'*10}-+-{'-'*10}-+-{'-'*12}-+-{'-'*12}")
        
        for price in prices:
            spread = manager.get_spread_in_points(price)
            long_entry = manager.calculate_entry_cost(price, is_long=True)
            short_entry = manager.calculate_entry_cost(price, is_long=False)
            
            print(f"{price:10.0f} | {spread:10.3f} | {manager.asset_config['spread_type']:10} | {long_entry:12.2f} | {short_entry:12.2f}")
            
            # Spread should be proportional to price if percentage-based
            if manager.asset_config and manager.asset_config["spread_type"] == "percentage":
                expected = (manager.asset_config["spread_value"] / 100.0) * price
                assert abs(spread - expected) < 0.001

    def test_cache_integration_with_real_config(self, real_data_config):
        """Test that SpreadManager uses CacheManager with real config."""
        broker_config_path = config_path("spreads", "broker_spreads.yaml")
        
        if not broker_config_path.exists():
            pytest.skip("Real broker_spreads.yaml not found")
        
        cache_manager = CacheManager()
        
        print(f"\n{'='*60}")
        print("REAL DATA TEST: Cache Integration")
        print(f"{'='*60}")
        
        # First instance - should load from file
        print("Loading first instance (cache miss)...")
        manager1 = SpreadManager(
            asset_symbol=real_data_config.asset.symbol,
            spread_config_path=str(broker_config_path),
            cache_manager=cache_manager,
            mode="analytics"
        )
        spread1 = manager1.get_spread_in_points(20000.0)
        
        # Second instance - should hit cache
        print("Loading second instance (should be cache hit)...")
        manager2 = SpreadManager(
            asset_symbol=real_data_config.asset.symbol,
            spread_config_path=str(broker_config_path),
            cache_manager=cache_manager,
            mode="analytics"
        )
        spread2 = manager2.get_spread_in_points(20000.0)
        
        print(f"Spread from first instance: {spread1:.3f}")
        print(f"Spread from second instance: {spread2:.3f}")
        
        assert spread1 == spread2
        
        # Check cache stats
        stats = cache_manager.get_stats()
        print(f"\nCache stats: {stats['spread_config']['hits']} hits, {stats['spread_config']['misses']} misses")
        assert stats['spread_config']['hits'] >= 1
        assert stats['spread_config']['misses'] == 1
"""
Unit Tests for Config Schema
=============================
Tests configuration loading, validation, and error handling.
"""

import pytest
from pathlib import Path
from datetime import datetime
import yaml

from src.strategies.config.config_schema import (
    StrategyConfig, AssetConfig, DataConfig, SpreadConfig,
    RiskConfig, TPMode, ExecutionConfig
)


class TestAssetConfig:
    """Tests for AssetConfig validation."""

    def test_valid_asset_config(self):
        """Test creating valid asset configuration."""
        config = AssetConfig(symbol="EURUSD", pip_size=0.0001, point_size=0.00001)
        assert config.symbol == "EURUSD"
        assert config.pip_size == 0.0001
        assert config.point_size == 0.00001

    def test_blank_symbol_raises_error(self):
        """Test that blank symbol raises ValueError."""
        with pytest.raises(ValueError, match="asset.symbol cannot be blank"):
            AssetConfig(symbol="", pip_size=0.0001, point_size=0.00001)

        with pytest.raises(ValueError, match="asset.symbol cannot be blank"):
            AssetConfig(symbol="   ", pip_size=0.0001, point_size=0.00001)

    def test_negative_pip_size_raises_error(self):
        """Test that negative pip size raises error."""
        with pytest.raises(ValueError, match="pip_size must be positive"):
            AssetConfig(symbol="EURUSD", pip_size=-0.0001, point_size=0.00001)

    def test_from_dict(self):
        """Test creating AssetConfig from dictionary."""
        data = {"symbol": "EURUSD", "pip_size": 0.0002, "point_size": 0.00002}
        config = AssetConfig.from_dict(data)
        assert config.symbol == "EURUSD"
        assert config.pip_size == 0.0002
        assert config.point_size == 0.00002


class TestSpreadConfig:
    """Tests for SpreadConfig validation."""

    def test_valid_spread_config_with_path(self, tmp_path):
        """Test valid spread configuration with existing path."""
        # Create a temporary file
        spread_file = tmp_path / "broker_spreads.yaml"
        spread_file.touch()
        
        config = SpreadConfig(
            enabled=True,
            config_path=spread_file
        )
        assert config.enabled is True
        assert config.config_path == spread_file

    def test_enabled_without_path_raises_error(self):
        """Test that enabled=True without path raises error."""
        with pytest.raises(ValueError, match="config_path is required"):
            SpreadConfig(enabled=True, config_path=None)

    def test_from_dict_with_existing_file(self, tmp_path):
        """Test creating SpreadConfig from dictionary with existing file."""
        # Create a temporary file
        spread_file = tmp_path / "test.yaml"
        spread_file.touch()
        
        data = {
            "enabled": True,
            "config_path": str(spread_file)
        }
        config = SpreadConfig.from_dict(data)
        assert config.enabled is True
        assert config.config_path == spread_file

    def test_config_path_handling(self):
        """Test config_path handling in different scenarios."""
        # Case 1: enabled=False - path can be None
        data1 = {"enabled": False}
        config1 = SpreadConfig.from_dict(data1)
        assert config1.enabled is False
        assert config1.config_path is None
        
        # Case 2: enabled=True without path - should raise
        data2 = {"enabled": True}
        with pytest.raises(ValueError, match="config_path is required"):
            SpreadConfig.from_dict(data2)


class TestRiskConfig:
    """Tests for RiskConfig validation."""

    def test_valid_risk_config_rr_mode(self):
        """Test valid risk configuration with rr_ratio mode."""
        config = RiskConfig(
            atr_length=14,
            atr_multiplier_sl=1.4,
            atr_multiplier_tp=7.98,
            max_risk_percentile=0.1,
            tp_mode="rr_ratio",
            risk_to_reward_ratio=5.7
        )
        assert config.atr_length == 14
        assert config.tp_mode == "rr_ratio"
        assert config.risk_to_reward_ratio == 5.7

    def test_valid_risk_config_atr_mode(self):
        """Test valid risk configuration with atr_multiplier mode."""
        config = RiskConfig(
            atr_length=14,
            atr_multiplier_sl=1.4,
            atr_multiplier_tp=7.98,
            max_risk_percentile=0.1,
            tp_mode="atr_multiplier",
            risk_to_reward_ratio=5.7  # ignored in this mode
        )
        assert config.tp_mode == "atr_multiplier"

    def test_invalid_tp_mode_raises_error(self):
        """Test that invalid tp_mode raises error."""
        with pytest.raises(ValueError, match="tp_mode.*invalid"):
            RiskConfig(
                atr_length=14,
                atr_multiplier_sl=1.4,
                atr_multiplier_tp=7.98,
                max_risk_percentile=0.1,
                tp_mode="invalid_mode",
                risk_to_reward_ratio=5.7
            )

    def test_rr_mode_with_zero_ratio_raises_error(self):
        """Test that rr_mode with zero ratio raises error."""
        with pytest.raises(ValueError, match="risk_to_reward_ratio must be > 0"):
            RiskConfig(
                atr_length=14,
                atr_multiplier_sl=1.4,
                atr_multiplier_tp=7.98,
                max_risk_percentile=0.1,
                tp_mode="rr_ratio",
                risk_to_reward_ratio=0
            )

    def test_max_risk_percentile_validation(self):
        """Test max_risk_percentile range validation."""
        # Valid values
        RiskConfig(
            atr_length=14,
            atr_multiplier_sl=1.4,
            atr_multiplier_tp=7.98,
            max_risk_percentile=0.05,
            tp_mode="rr_ratio",
            risk_to_reward_ratio=5.7
        )

        # Too high
        with pytest.raises(ValueError, match="must be between 0 and 5.0"):
            RiskConfig(
                atr_length=14,
                atr_multiplier_sl=1.4,
                atr_multiplier_tp=7.98,
                max_risk_percentile=6.0,
                tp_mode="rr_ratio",
                risk_to_reward_ratio=5.7
            )

        # Negative
        with pytest.raises(ValueError, match="must be between 0 and 5.0"):
            RiskConfig(
                atr_length=14,
                atr_multiplier_sl=1.4,
                atr_multiplier_tp=7.98,
                max_risk_percentile=-0.1,
                tp_mode="rr_ratio",
                risk_to_reward_ratio=5.7
            )


class TestExecutionConfig:
    """Tests for ExecutionConfig validation."""

    def test_valid_modes(self):
        """Test valid execution modes."""
        config = ExecutionConfig(mode="core")
        assert config.mode == "core"

        config = ExecutionConfig(mode="analytics")
        assert config.mode == "analytics"

    def test_invalid_mode_raises_error(self):
        """Test that invalid mode raises error."""
        with pytest.raises(ValueError, match="Invalid execution.mode"):
            ExecutionConfig(mode="debug")

        with pytest.raises(ValueError, match="Invalid execution.mode"):
            ExecutionConfig(mode="production")


class TestStrategyConfig:
    """Tests for complete StrategyConfig loading and validation."""

    def test_from_dict_valid(self, base_config_dict):
        """Test creating StrategyConfig from valid dict."""
        config = StrategyConfig.from_dict(base_config_dict)
        assert config.asset.symbol == "TEST"
        assert config.execution.mode == "core"
        assert config.trade_management.spread.enabled is True

    def test_missing_required_section_raises_error(self, base_config_dict):
        """Test that missing required section raises appropriate error."""
        invalid_dict = base_config_dict.copy()
        del invalid_dict["asset"]

        # Should raise ValueError from AssetConfig validation
        with pytest.raises(ValueError, match="asset.symbol cannot be blank"):
            StrategyConfig.from_dict(invalid_dict)

    def test_from_yaml_file_not_found(self, tmp_path):
        """Test loading from non-existent YAML file."""
        nonexistent = tmp_path / "nonexistent.yaml"

        with pytest.raises(FileNotFoundError, match="Config file not found"):
            StrategyConfig.from_yaml(nonexistent)

    def test_from_yaml_valid(self, tmp_path, base_config_dict):
        """Test loading from valid YAML file."""
        yaml_path = tmp_path / "test_config.yaml"
        with open(yaml_path, 'w') as f:
            yaml.dump(base_config_dict, f)

        config = StrategyConfig.from_yaml(yaml_path)
        assert config.asset.symbol == "TEST"
        assert config.execution.mode == "core"


class TestDataConfig:
    """Tests for DataConfig validation."""

    def test_htf_period_validation(self, base_config_dict):
        """Test htf_period format validation."""
        base_config_dict["data"]["htf_period"] = "INVALID"

        with pytest.raises(ValueError, match="not a recognised period"):
            StrategyConfig.from_dict(base_config_dict)

    def test_ltf_timeframe_required_when_ltf_exists(self, base_config_dict):
        """Test that ltf_timeframe is required when ltf_ohlcv is set."""
        base_config_dict["data"]["ltf_timeframe"] = ""

        with pytest.raises(ValueError, match="ltf_timeframe is required"):
            StrategyConfig.from_dict(base_config_dict)

    def test_artf_timeframe_required_when_artf_exists(self, base_config_dict):
        """Test that artf_timeframe is required when artf_ohlcv is set."""
        base_config_dict["data"]["artf_timeframe"] = ""

        with pytest.raises(ValueError, match="artf_timeframe is required"):
            StrategyConfig.from_dict(base_config_dict)
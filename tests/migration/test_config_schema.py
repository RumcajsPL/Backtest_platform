"""
Unit Tests for Config Schema Validation

Session 12 - Infrastructure Testing
Version: 1.0.0

Tests type-safe configuration validation for correctness.
"""

# Add project root to path for proper module resolution
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import pytest
import tempfile
import yaml

# Import from configs directory using the correct path
from configs.config_schema import (
    SpreadConfig,
    SpreadType,
    RiskConfig,
    TradeManagementConfig,
    FilterConfig,
    FilterPipelineConfig,
    DataPathsConfig,
    DateRangeConfig,
    DataConfig,
    StrategyConfig,
    ErrorStrategy,
    validate_config,
    check_config_compatibility,
)


class TestSpreadConfig:
    """Test SpreadConfig validation"""
    
    def test_valid_spread_config(self):
        """Valid spread config should create successfully"""
        config = SpreadConfig(
            enabled=True,
            spread_type="percentage",
            spread_value=0.0001
        )
        assert config.enabled is True
        assert config.spread_value == 0.0001
    
    def test_invalid_spread_type(self):
        """Invalid spread_type should raise ValueError"""
        with pytest.raises(ValueError, match="Invalid spread_type"):
            SpreadConfig(
                enabled=True,
                spread_type="invalid_type",
                spread_value=0.0001
            )
    
    def test_negative_spread_value(self):
        """Negative spread_value should raise ValueError"""
        with pytest.raises(ValueError, match="must be non-negative"):
            SpreadConfig(
                enabled=True,
                spread_type="percentage",
                spread_value=-0.0001
            )
    
    def test_zero_spread_when_enabled(self):
        """Zero spread when enabled should raise ValueError"""
        with pytest.raises(ValueError, match="cannot be 0 when spread is enabled"):
            SpreadConfig(
                enabled=True,
                spread_type="percentage",
                spread_value=0.0
            )
    
    def test_from_dict(self):
        """from_dict should create valid config"""
        config = SpreadConfig.from_dict({
            'enabled': True,
            'spread_type': 'points',
            'spread_value': 0.5
        })
        assert config.spread_type == 'points'
        assert config.spread_value == 0.5


class TestRiskConfig:
    """Test RiskConfig validation"""
    
    def test_valid_risk_config(self):
        """Valid risk config should create successfully"""
        config = RiskConfig(
            atr_length=14,
            atr_multiplier_sl=2.0,
            atr_multiplier_tp=4.0,
            max_risk_percentile=3.0
        )
        assert config.atr_length == 14
        assert config.max_risk_percentile == 3.0
    
    def test_invalid_atr_length(self):
        """Zero/negative ATR length should raise ValueError"""
        with pytest.raises(ValueError, match="atr_length must be positive"):
            RiskConfig(
                atr_length=0,
                atr_multiplier_sl=2.0,
                atr_multiplier_tp=4.0,
                max_risk_percentile=3.0
            )
    
    def test_invalid_max_risk_percentile(self):
        """Invalid max_risk_percentile should raise ValueError"""
        with pytest.raises(ValueError, match="must be between 0 and 100"):
            RiskConfig(
                atr_length=14,
                atr_multiplier_sl=2.0,
                atr_multiplier_tp=4.0,
                max_risk_percentile=150.0
            )
    
    def test_from_dict(self):
        """from_dict should create valid config"""
        config = RiskConfig.from_dict({
            'atr_length': 20,
            'atr_multiplier_sl': 3.0,
            'atr_multiplier_tp': 6.0,
            'max_risk_percentile': 2.5
        })
        assert config.atr_length == 20


class TestTradeManagementConfig:
    """Test TradeManagementConfig validation"""
    
    def test_valid_trade_management_config(self):
        """Valid config should create successfully"""
        config = TradeManagementConfig.from_dict({
            'spread': {
                'enabled': True,
                'spread_type': 'percentage',
                'spread_value': 0.0001
            },
            'risk': {
                'atr_length': 14,
                'atr_multiplier_sl': 2.0,
                'atr_multiplier_tp': 4.0,
                'max_risk_percentile': 3.0
            },
            'pyramiding_enabled': True,
            'close_on_opposite': True,
            'max_positions': 3
        })
        assert config.pyramiding_enabled is True
        assert config.max_positions == 3
    
    def test_invalid_max_positions(self):
        """max_positions < 1 should raise ValueError"""
        with pytest.raises(ValueError, match="max_positions must be >= 1"):
            TradeManagementConfig(
                spread=SpreadConfig(False, "percentage", 0.0),
                risk=RiskConfig(14, 2.0, 4.0, 3.0),
                pyramiding_enabled=False,
                close_on_opposite=True,
                max_positions=0
            )
    
    def test_pyramiding_contradiction(self):
        """Pyramiding enabled with max_positions=1 should raise ValueError"""
        with pytest.raises(ValueError, match="Pyramiding enabled but max_positions=1"):
            TradeManagementConfig(
                spread=SpreadConfig(False, "percentage", 0.0),
                risk=RiskConfig(14, 2.0, 4.0, 3.0),
                pyramiding_enabled=True,
                close_on_opposite=True,
                max_positions=1
            )


class TestFilterConfig:
    """Test FilterConfig validation"""
    
    def test_valid_filter_config(self):
        """Valid filter config should create successfully"""
        config = FilterConfig(
            enabled=True,
            error_strategy="pass_through",
            config={"threshold": 25.0}
        )
        assert config.enabled is True
        assert config.error_strategy == "pass_through"
    
    def test_invalid_error_strategy(self):
        """Invalid error_strategy should raise ValueError"""
        with pytest.raises(ValueError, match="Invalid error_strategy"):
            FilterConfig(
                enabled=True,
                error_strategy="invalid_strategy",
                config={}
            )
    
    def test_from_dict(self):
        """from_dict should create valid config"""
        config = FilterConfig.from_dict({
            'enabled': False,
            'error_strategy': 'fail_fast',
            'config': {'param': 'value'}
        })
        assert config.enabled is False
        assert config.error_strategy == 'fail_fast'


class TestDateRangeConfig:
    """Test DateRangeConfig validation"""
    
    def test_valid_date_range(self):
        """Valid date range should create successfully"""
        config = DateRangeConfig(
            start="2025-01-01 00:00:00",  # Added time component
            end="2025-12-31 23:59:59"      # Added time component
        )
        assert config.start == "2025-01-01 00:00:00"
        assert config.end == "2025-12-31 23:59:59"
    
    def test_invalid_date_format(self):
        """Invalid date format should raise ValueError"""
        with pytest.raises(ValueError, match="Invalid datetime format"):
            DateRangeConfig(
                start="01/01/2025 00:00:00",  # Wrong format with time
                end="2025-12-31 23:59:59"
            )
    
    def test_start_after_end(self):
        """start >= end should raise ValueError"""
        with pytest.raises(ValueError, match="start datetime.*must be before end datetime"):
            DateRangeConfig(
                start="2025-12-31 23:59:59",
                end="2025-01-01 00:00:00"
            )
    
    def test_missing_time_component(self):
        """Missing time component should raise ValueError"""
        with pytest.raises(ValueError, match="Invalid datetime format"):
            DateRangeConfig(
                start="2025-01-01",  # Missing time
                end="2025-12-31 23:59:59"
            )


class TestDataConfig:
    """Test DataConfig validation"""
    
    def test_valid_data_config(self):
        """Valid data config should create successfully"""
        config = DataConfig.from_dict({
            'paths': {
                'strategy_ohlcv': 'data/strategy.parquet',
                'ltf_ohlcv': 'data/ltf.parquet',
            },
            'date_range': {
                'start': '2025-01-01 00:00:00',  # Added time component
                'end': '2025-12-31 23:59:59'      # Added time component
            },
            'timezone': 'UTC'
        })
        assert config.timezone == 'UTC'
        # Fix: Use as_posix() to get forward slashes on all platforms
        assert config.paths.strategy_ohlcv.as_posix() == 'data/strategy.parquet'
    
    def test_invalid_timezone(self):
        """Invalid timezone should raise ValueError"""
        with pytest.raises(ValueError, match="Invalid timezone.*Must be a valid timezone"):
            DataConfig(
                paths=DataPathsConfig(
                    strategy_ohlcv=Path('data/strategy.parquet')
                ),
                date_range=DateRangeConfig(
                    start='2025-01-01 00:00:00',
                    end='2025-12-31 23:59:59'
                ),
                timezone='InvalidTimezone'
            )


class TestStrategyConfig:
    """Test complete StrategyConfig validation"""
    
    def test_valid_strategy_config(self):
        """Valid strategy config should create successfully"""
        config_dict = {
            'data': {
                'paths': {
                    'strategy_ohlcv': 'data/strategy.parquet'
                },
                'date_range': {
                    'start': '2025-01-01 00:00:00',  # Added time component
                    'end': '2025-12-31 23:59:59'      # Added time component
                }
            },
            'trade_management': {
                'spread': {
                    'enabled': False,
                    'spread_type': 'percentage',
                    'spread_value': 0.0
                },
                'risk': {
                    'atr_length': 14,
                    'atr_multiplier_sl': 2.0,
                    'atr_multiplier_tp': 4.0,
                    'max_risk_percentile': 3.0
                },
                'pyramiding_enabled': False,
                'close_on_opposite': True,
                'max_positions': 1
            },
            'filters': {
                'time_filters': {},
                'technical_filters': {}
            }
        }
        
        config = StrategyConfig.from_dict(config_dict)
        assert config.data.timezone == 'UTC'
        assert config.trade_management.max_positions == 1
    
    def test_from_yaml(self, tmp_path):
        """from_yaml should load and validate config file"""
        config_dict = {
            'data': {
                'paths': {
                    'strategy_ohlcv': 'data/strategy.parquet'
                },
                'date_range': {
                    'start': '2025-01-01 00:00:00',  # Added time component
                    'end': '2025-12-31 23:59:59'      # Added time component
                }
            },
            'trade_management': {
                'spread': {
                    'enabled': False,
                    'spread_type': 'percentage',
                    'spread_value': 0.0
                },
                'risk': {
                    'atr_length': 14,
                    'atr_multiplier_sl': 2.0,
                    'atr_multiplier_tp': 4.0,
                    'max_risk_percentile': 3.0
                },
                'pyramiding_enabled': False,
                'close_on_opposite': True,
                'max_positions': 1
            },
            'filters': {
                'time_filters': {},
                'technical_filters': {}
            }
        }
        
        # Write to YAML file
        config_file = tmp_path / "test_config.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(config_dict, f)
        
        # Load and validate
        config = StrategyConfig.from_yaml(config_file)
        assert config.data.timezone == 'UTC'
    
    def test_missing_config_file(self):
        """from_yaml should raise FileNotFoundError for missing file"""
        with pytest.raises(FileNotFoundError):
            StrategyConfig.from_yaml(Path("nonexistent.yaml"))


class TestValidationHelpers:
    """Test validation helper functions"""
    
    def test_validate_config(self, tmp_path):
        """validate_config should work correctly"""
        config_dict = {
            'data': {
                'paths': {'strategy_ohlcv': 'data/strategy.parquet'},
                'date_range': {
                    'start': '2025-01-01 00:00:00',  # Added time component
                    'end': '2025-12-31 23:59:59'      # Added time component
                }
            },
            'trade_management': {
                'spread': {'enabled': False, 'spread_type': 'percentage', 'spread_value': 0.0},
                'risk': {'atr_length': 14, 'atr_multiplier_sl': 2.0, 'atr_multiplier_tp': 4.0, 'max_risk_percentile': 3.0},
                'pyramiding_enabled': False,
                'close_on_opposite': True,
                'max_positions': 1
            },
            'filters': {'time_filters': {}, 'technical_filters': {}}
        }
        
        config_file = tmp_path / "config.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(config_dict, f)
        
        config = validate_config(config_file)
        assert isinstance(config, StrategyConfig)
    
    def test_check_config_compatibility(self):
        """check_config_compatibility should detect missing features"""
        config_dict = {
            'data': {
                'paths': {
                    'strategy_ohlcv': 'data/strategy.parquet',
                    'ltf_ohlcv': 'data/ltf.parquet'  # LTF present
                },
                'date_range': {
                    'start': '2025-01-01 00:00:00',  # Added time component
                    'end': '2025-12-31 23:59:59'      # Added time component
                }
            },
            'trade_management': {
                'spread': {
                    'enabled': True,  # Spread enabled
                    'spread_type': 'percentage',
                    'spread_value': 0.0001
                },
                'risk': {'atr_length': 14, 'atr_multiplier_sl': 2.0, 'atr_multiplier_tp': 4.0, 'max_risk_percentile': 3.0},
                'pyramiding_enabled': False,
                'close_on_opposite': True,
                'max_positions': 1
            },
            'filters': {'time_filters': {}, 'technical_filters': {}}
        }
        
        config = StrategyConfig.from_dict(config_dict)
        
        # Should have ltf_data and spread
        assert check_config_compatibility(config, ["ltf_data", "spread"]) is True
        
        # Should not have pyramiding
        assert check_config_compatibility(config, ["pyramiding"]) is False


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
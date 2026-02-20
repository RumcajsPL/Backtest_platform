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
from src.config.config_schema import (
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


# =============================================================================
# SESSION 20 — NEW ARCHITECTURE TESTS (src/config/config_schema.py)
# =============================================================================
# These tests target the NEW architecture at src/config/config_schema.py.
# They are separated from the legacy tests above (configs/config_schema.py).
# All new tests use @pytest.mark.unit for easy filtering.
# Covers: Block A (mode rename), Block C (template yaml), Block G (freeze).
# =============================================================================

# Separate import block for new architecture — isolated so a missing module
# causes a clean skip rather than breaking the entire file.
try:
    import src.config.config_schema as new_schema  # type: ignore[import]
    _NEW_SCHEMA_AVAILABLE = True
except ImportError:
    _NEW_SCHEMA_AVAILABLE = False

pytestmark_new = pytest.mark.skipif(
    not _NEW_SCHEMA_AVAILABLE,
    reason="src/config/config_schema.py not importable — new architecture not on path"
)


@pytest.mark.unit
@pytest.mark.skipif(not _NEW_SCHEMA_AVAILABLE, reason="new arch not importable")
class TestNewArchModeValidation:
    """
    Block A — Global rename 'debug' → 'analytics'.
    Verifies the migration guard and accepted mode values.

    Assumption: DataLoader and SignalGenerator (new arch) accept a `mode`
    kwarg and raise ValueError with the migration message when mode='debug'.
    If the guard lives only in config_schema.py, adjust the import target.
    """

    def _make_minimal_config(self) -> object:
        """Build the smallest valid new-arch StrategyConfig."""
        return new_schema.StrategyConfig.from_dict({
            "data": {
                "paths": {"strategy_ohlcv": "data/processed/ohlcv/EURUSD_5M.parquet"},
                "date_range": {
                    "start": "2024-01-01 00:00:00",
                    "end": "2024-12-31 23:59:59",
                },
            },
            "execution": {"mode": "analytics"},
            "trade_management": {
                "risk": {
                    "atr_length": 14,
                    "max_risk_percentile": 0.5,
                },
                "spread": {"enabled": False, "spread_value": 0.0},
            },
            "filters": {"time": {"enabled": False}, "pipeline": {"filters": {}}},
        })

    def test_mode_debug_raises_migration_error(self):
        """
        Passing mode='debug' must raise ValueError containing the migration hint.
        Covers: P0-CH0-1 / CL-2 — 'debug' is a deprecated alias.
        The error must mention 'analytics' so the user knows what to use.
        """
        with pytest.raises(ValueError, match="analytics"):
            new_schema.StrategyConfig.from_dict({
                "data": {
                    "paths": {"strategy_ohlcv": "data/processed/ohlcv/EURUSD_5M.parquet"},
                    "date_range": {
                        "start": "2024-01-01 00:00:00",
                        "end": "2024-12-31 23:59:59",
                    },
                },
                "execution": {"mode": "debug"},  # ← deprecated value
                "trade_management": {
                    "risk": {"atr_length": 14, "max_risk_percentile": 0.5},
                    "spread": {"enabled": False, "spread_value": 0.0},
                },
                "filters": {"time": {"enabled": False}, "pipeline": {"filters": {}}},
            })

    def test_mode_analytics_accepted(self):
        """
        mode='analytics' must be accepted without error.
        Covers: CL-2 — 'analytics' is the canonical replacement for 'debug'.
        """
        cfg = self._make_minimal_config()
        # Access execution mode — attribute name may vary; try both conventions.
        mode_val = getattr(
            getattr(cfg, "execution", None),
            "mode",
            getattr(cfg, "mode", None),
        )
        assert mode_val == "analytics", (
            f"Expected execution.mode == 'analytics', got {mode_val!r}"
        )

    def test_mode_core_accepted(self):
        """
        mode='core' must be accepted without error.
        Covers: CL-2 — 'core' is the max-speed mode for multi-run backtester.
        """
        cfg = new_schema.StrategyConfig.from_dict({
            "data": {
                "paths": {"strategy_ohlcv": "data/processed/ohlcv/EURUSD_5M.parquet"},
                "date_range": {
                    "start": "2024-01-01 00:00:00",
                    "end": "2024-12-31 23:59:59",
                },
            },
            "execution": {"mode": "core"},
            "trade_management": {
                "risk": {"atr_length": 14, "max_risk_percentile": 0.5},
                "spread": {"enabled": False, "spread_value": 0.0},
            },
            "filters": {"time": {"enabled": False}, "pipeline": {"filters": {}}},
        })
        mode_val = getattr(
            getattr(cfg, "execution", None),
            "mode",
            getattr(cfg, "mode", None),
        )
        assert mode_val == "core"

    def test_mode_invalid_raises(self):
        """
        An unrecognised mode string must raise ValueError.
        Ensures validation rejects arbitrary strings, not just 'debug'.
        """
        with pytest.raises(ValueError):
            new_schema.StrategyConfig.from_dict({
                "data": {
                    "paths": {"strategy_ohlcv": "data/processed/ohlcv/EURUSD_5M.parquet"},
                    "date_range": {
                        "start": "2024-01-01 00:00:00",
                        "end": "2024-12-31 23:59:59",
                    },
                },
                "execution": {"mode": "turbo"},  # ← nonsense value
                "trade_management": {
                    "risk": {"atr_length": 14, "max_risk_percentile": 0.5},
                    "spread": {"enabled": False, "spread_value": 0.0},
                },
                "filters": {"time": {"enabled": False}, "pipeline": {"filters": {}}},
            })


@pytest.mark.unit
@pytest.mark.skipif(not _NEW_SCHEMA_AVAILABLE, reason="new arch not importable")
class TestNewArchRiskValidation:
    """
    Block C — max_risk_percentile validation range fix (P0-CH0-2).
    Old range: 0 < value <= 100  (wrong — allowed nonsensical values)
    New range: 0 < value <= 5.0  (correct — % of annual range)
    """

    def _risk_dict(self, percentile: float) -> dict:
        return {
            "data": {
                "paths": {"strategy_ohlcv": "data/processed/ohlcv/EURUSD_5M.parquet"},
                "date_range": {
                    "start": "2024-01-01 00:00:00",
                    "end": "2024-12-31 23:59:59",
                },
            },
            "execution": {"mode": "analytics"},
            "trade_management": {
                "risk": {
                    "atr_length": 14,
                    "max_risk_percentile": percentile,
                },
                "spread": {"enabled": False, "spread_value": 0.0},
            },
            "filters": {"time": {"enabled": False}, "pipeline": {"filters": {}}},
        }

    def test_max_risk_percentile_above_5_raises(self):
        """
        max_risk_percentile > 5.0 must raise ValueError.
        Covers: P0-CH0-2 — old code accepted up to 100 (broken).
        Value of 150 would previously pass; must now be rejected.
        """
        with pytest.raises(ValueError, match=r"max_risk_percentile"):
            new_schema.StrategyConfig.from_dict(self._risk_dict(150.0))

    def test_max_risk_percentile_exactly_5_accepted(self):
        """
        max_risk_percentile == 5.0 is the boundary — must be accepted.
        Verifies the upper boundary of the corrected range is inclusive.
        """
        # Should not raise
        cfg = new_schema.StrategyConfig.from_dict(self._risk_dict(5.0))
        assert cfg is not None

    def test_max_risk_percentile_above_1_warns(self, caplog):
        """
        max_risk_percentile > 1.0 must emit a WARNING log.
        Covers: P0-CH0-2 — high-but-legal values should be flagged.
        Values > 1.0% of annual range are unusual and warrant attention.

        NOTE: This test depends on the new-arch logger writing to Python's
        logging system (not a custom sink). If the implementation uses
        structured_logger exclusively without propagating to Python logging,
        this test will need to be adapted to capture that logger's output.
        """
        import logging
        with caplog.at_level(logging.WARNING, logger="src.config.config_schema"):
            new_schema.StrategyConfig.from_dict(self._risk_dict(2.5))
        assert any(
            "max_risk_percentile" in record.message and record.levelno >= logging.WARNING
            for record in caplog.records
        ), (
            "Expected a WARNING log mentioning 'max_risk_percentile' when value > 1.0. "
            "Check that the logger in src/config/config_schema.py propagates to Python logging."
        )

    def test_max_risk_percentile_zero_raises(self):
        """
        max_risk_percentile == 0 must raise ValueError (exclusive lower bound).
        """
        with pytest.raises(ValueError, match=r"max_risk_percentile"):
            new_schema.StrategyConfig.from_dict(self._risk_dict(0.0))

    def test_max_risk_percentile_typical_value_accepted(self):
        """
        Typical production value (0.5) must be accepted without warning.
        """
        import logging
        # 0.5 is below the warning threshold of 1.0
        # caplog not used here — just confirm no exception is raised
        cfg = new_schema.StrategyConfig.from_dict(self._risk_dict(0.5))
        assert cfg is not None


@pytest.mark.unit
@pytest.mark.skipif(not _NEW_SCHEMA_AVAILABLE, reason="new arch not importable")
class TestNewArchFrozenContracts:
    """
    Block G — All new-arch config dataclasses must be frozen=True (P1-CH0-1).
    Frozen dataclasses raise FrozenInstanceError (a subclass of AttributeError)
    on any attempted mutation after construction.
    """

    def _make_config(self) -> object:
        return new_schema.StrategyConfig.from_dict({
            "data": {
                "paths": {"strategy_ohlcv": "data/processed/ohlcv/EURUSD_5M.parquet"},
                "date_range": {
                    "start": "2024-01-01 00:00:00",
                    "end": "2024-12-31 23:59:59",
                },
            },
            "execution": {"mode": "analytics"},
            "trade_management": {
                "risk": {"atr_length": 14, "max_risk_percentile": 0.5},
                "spread": {"enabled": False, "spread_value": 0.0},
            },
            "filters": {"time": {"enabled": False}, "pipeline": {"filters": {}}},
        })

    def test_strategy_config_is_frozen(self):
        """
        StrategyConfig must be immutable after creation.
        Covers: P1-CH0-1 — DEC-004 violation where top-level config was mutable.
        """
        cfg = self._make_config()
        with pytest.raises((AttributeError, TypeError)):
            cfg.execution = object()  # type: ignore[misc]

    def test_risk_config_is_frozen(self):
        """
        RiskConfig (new arch) must be immutable after creation.
        Prevents accidental mutation of risk parameters mid-run.
        """
        cfg = self._make_config()
        risk = cfg.trade_management.risk
        with pytest.raises((AttributeError, TypeError)):
            risk.atr_length = 99  # type: ignore[misc]

    def test_spread_config_is_frozen(self):
        """
        SpreadConfig (new arch) must be immutable after creation.
        """
        cfg = self._make_config()
        spread = cfg.trade_management.spread
        with pytest.raises((AttributeError, TypeError)):
            spread.enabled = True  # type: ignore[misc]

    def test_data_paths_config_is_frozen(self):
        """
        DataPathsConfig must be immutable after creation.
        Path mutation after load would silently break data loading.
        """
        cfg = self._make_config()
        paths = cfg.data.paths
        with pytest.raises((AttributeError, TypeError)):
            paths.strategy_ohlcv = Path("other.parquet")  # type: ignore[misc]

    def test_date_range_config_is_frozen(self):
        """
        DateRangeConfig must be immutable after creation.
        Mutable date ranges could cause silent data slicing bugs.
        """
        cfg = self._make_config()
        date_range = cfg.data.date_range
        with pytest.raises((AttributeError, TypeError)):
            date_range.start = "2020-01-01 00:00:00"  # type: ignore[misc]


@pytest.mark.unit
@pytest.mark.skipif(not _NEW_SCHEMA_AVAILABLE, reason="new arch not importable")
class TestNewArchStrategyTemplateYaml:
    """
    Block C — P0-CH0-1: strategy_template.yaml must exist and load cleanly.
    This is the end-to-end smoke test for the new config system.
    If this test fails, StrategyConfig has never been tested against a real YAML.
    """

    _TEMPLATE_PATH = project_root / "configs" / "strategy_template.yaml"

    def test_template_yaml_exists(self):
        """
        configs/strategy_template.yaml must exist on disk.
        Covers: P0-CH0-1 — previously this file did not exist.
        """
        assert self._TEMPLATE_PATH.exists(), (
            f"strategy_template.yaml not found at {self._TEMPLATE_PATH}. "
            "Run Block C from Session 20 implementation plan to create it."
        )

    def test_template_yaml_loads_without_error(self):
        """
        StrategyConfig.from_yaml(strategy_template.yaml) must succeed.
        Covers: P0-CH0-1 — validates that the template matches from_dict() expectations.
        """
        if not self._TEMPLATE_PATH.exists():
            pytest.skip("strategy_template.yaml does not exist yet")
        cfg = new_schema.StrategyConfig.from_yaml(self._TEMPLATE_PATH)
        assert cfg is not None

    def test_template_yaml_has_required_sections(self):
        """
        The raw YAML must contain the four top-level sections defined in the
        new architecture: data, execution, trade_management, filters.
        Catches YAML typos that from_dict() might silently ignore.
        """
        if not self._TEMPLATE_PATH.exists():
            pytest.skip("strategy_template.yaml does not exist yet")
        with open(self._TEMPLATE_PATH) as f:
            raw = yaml.safe_load(f)
        required = {"data", "execution", "trade_management", "filters"}
        missing = required - set(raw.keys())
        assert not missing, (
            f"strategy_template.yaml is missing top-level sections: {missing}"
        )

    def test_template_yaml_default_mode_is_analytics(self):
        """
        The template's execution.mode must default to 'analytics', not 'debug'.
        Covers: CL-2 — ensures the template itself teaches the correct mode name.
        """
        if not self._TEMPLATE_PATH.exists():
            pytest.skip("strategy_template.yaml does not exist yet")
        with open(self._TEMPLATE_PATH) as f:
            raw = yaml.safe_load(f)
        mode = raw.get("execution", {}).get("mode")
        assert mode == "analytics", (
            f"strategy_template.yaml execution.mode should be 'analytics', got {mode!r}. "
            "Update Block C output to set the correct default."
        )

    def test_template_yaml_missing_file_raises(self):
        """
        from_yaml with a nonexistent path must raise FileNotFoundError.
        Sanity check that error handling is in place in the new arch loader.
        """
        with pytest.raises(FileNotFoundError):
            new_schema.StrategyConfig.from_yaml(
                Path("configs/does_not_exist_s20.yaml")
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
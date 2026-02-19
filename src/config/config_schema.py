"""
Config Schema Validation - Type-Safe Configuration

Version: 1.1.0
Session: 20 Block C

Changes from v1.0.3:
- P0-CH0-2: Fixed max_risk_percentile validation range (0-100 → 0-5.0)
             Added warning for values > 1.0
- P1-CH0-1: All config dataclasses now frozen=True (DEC-004)
- P1-CH0-2: Coerce Path objects at from_dict boundary, not in __post_init__
- P1-CH0-4: Added filter_sequence: List[str] to FilterPipelineConfig
- Added htf_ohlcv to DataPathsConfig (was missing — HTF data has no path)
- Added ExecutionConfig dataclass for execution.mode
- Renamed StrategyConfig.debug field to StrategyConfig.metadata (avoid "debug" name)
- Added migration guard: mode="debug" raises ValueError with message (DEC-022)

Design Principles:
- Single Responsibility: Only config validation
- Explicit Contracts: All config fields typed and frozen
- Type Safety: Dataclasses with validation
- Production-Ready: Fail fast with clear error messages
- Performance-Driven: Validate once at startup
"""
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from pathlib import Path
from enum import Enum
import logging
import re
import yaml
import pandas as pd
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


class SpreadType(Enum):
    """Spread calculation method"""
    PERCENTAGE = "percentage"
    POINTS = "points"
    PIPS = "pips"


class ErrorStrategy(Enum):
    """Error handling strategy for filters"""
    FAIL_FAST = "fail_fast"        # Stop on first error (development)
    PASS_THROUGH = "pass_through"  # Skip failing filters (production)
    REJECT_ALL = "reject_all"      # Reject all signals on error (conservative)


# ============================================================================
# SPREAD CONFIGURATION
# ============================================================================

@dataclass(frozen=True)
class SpreadConfig:
    """Spread configuration for realistic execution"""
    enabled: bool
    spread_type: str      # Validated against SpreadType enum
    spread_value: float

    def __post_init__(self):
        """Validate spread configuration"""
        try:
            SpreadType(self.spread_type)
        except ValueError:
            valid_types = [t.value for t in SpreadType]
            raise ValueError(
                f"Invalid spread_type '{self.spread_type}'. "
                f"Must be one of: {valid_types}"
            )

        if self.spread_value < 0:
            raise ValueError(
                f"spread_value must be non-negative, got {self.spread_value}"
            )

        if self.enabled and self.spread_value == 0:
            raise ValueError(
                "spread_value cannot be 0 when spread is enabled"
            )

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'SpreadConfig':
        """Create from dict with validation"""
        return cls(
            enabled=bool(d.get('enabled', False)),
            spread_type=str(d.get('spread_type', 'percentage')),
            spread_value=float(d.get('spread_value', 0.0))
        )


# ============================================================================
# RISK MANAGEMENT CONFIGURATION
# ============================================================================

@dataclass(frozen=True)
class RiskConfig:
    """Risk management configuration"""
    atr_length: int
    atr_multiplier_sl: float
    atr_multiplier_tp: float
    max_risk_percentile: float

    def __post_init__(self):
        """Validate risk configuration"""
        if self.atr_length <= 0:
            raise ValueError(
                f"atr_length must be positive, got {self.atr_length}"
            )

        if self.atr_multiplier_sl <= 0:
            raise ValueError(
                f"atr_multiplier_sl must be positive, got {self.atr_multiplier_sl}"
            )

        if self.atr_multiplier_tp <= 0:
            raise ValueError(
                f"atr_multiplier_tp must be positive, got {self.atr_multiplier_tp}"
            )

        # P0-CH0-2: Corrected range from (0, 100] to (0, 5.0]
        # This field is a % of annual instrument range. Values > 1.0 are unusual.
        if not (0 < self.max_risk_percentile <= 5.0):
            raise ValueError(
                f"max_risk_percentile must be between 0 and 5.0 (% of annual range), "
                f"got {self.max_risk_percentile}. "
                f"Typical values: 0.05–0.5. Maximum accepted: 5.0."
            )

        if self.max_risk_percentile > 1.0:
            logger.warning(
                f"max_risk_percentile={self.max_risk_percentile} is unusually high "
                f"(>1.0% of annual range). Verify this is intentional."
            )

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'RiskConfig':
        """Create from dict with validation"""
        return cls(
            atr_length=int(d.get('atr_length', 14)),
            atr_multiplier_sl=float(d.get('atr_multiplier_sl', 2.0)),
            atr_multiplier_tp=float(d.get('atr_multiplier_tp', 4.0)),
            max_risk_percentile=float(d.get('max_risk_percentile', 0.5))
        )


# ============================================================================
# POSITION CONTROL CONFIGURATION
# ============================================================================

@dataclass(frozen=True)
class PositionControlConfig:
    """Position control rules"""
    pyramiding_enabled: bool
    close_on_opposite: bool
    max_positions: int

    def __post_init__(self):
        if self.max_positions < 1:
            raise ValueError(
                f"max_positions must be >= 1, got {self.max_positions}"
            )
        if self.pyramiding_enabled and self.max_positions == 1:
            raise ValueError(
                "Pyramiding enabled but max_positions=1 (contradiction)"
            )

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'PositionControlConfig':
        return cls(
            pyramiding_enabled=bool(d.get('pyramiding_enabled', False)),
            close_on_opposite=bool(d.get('close_on_opposite', False)),
            max_positions=int(d.get('max_positions', 1))
        )


# ============================================================================
# TRADE MANAGEMENT CONFIGURATION
# ============================================================================

@dataclass(frozen=True)
class TradeManagementConfig:
    """Trade management and position rules"""
    spread: SpreadConfig
    risk: RiskConfig
    position_control: PositionControlConfig

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'TradeManagementConfig':
        """Create from dict with validation"""
        return cls(
            spread=SpreadConfig.from_dict(d.get('spread', {})),
            risk=RiskConfig.from_dict(d.get('risk', {})),
            position_control=PositionControlConfig.from_dict(
                d.get('position_control', {})
            )
        )


# ============================================================================
# FILTER CONFIGURATION
# ============================================================================

@dataclass(frozen=True)
class FilterConfig:
    """Single filter configuration"""
    enabled: bool
    error_strategy: str = "pass_through"
    config: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        try:
            ErrorStrategy(self.error_strategy)
        except ValueError:
            valid_strategies = [s.value for s in ErrorStrategy]
            raise ValueError(
                f"Invalid error_strategy '{self.error_strategy}'. "
                f"Must be one of: {valid_strategies}"
            )

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'FilterConfig':
        # All filter params except 'enabled' and 'error_strategy' go into config
        known_keys = {'enabled', 'error_strategy'}
        config_params = {k: v for k, v in d.items() if k not in known_keys}
        return cls(
            enabled=bool(d.get('enabled', True)),
            error_strategy=str(d.get('error_strategy', 'pass_through')),
            config=config_params
        )


@dataclass(frozen=True)
class FilterPipelineConfig:
    """Complete filter pipeline configuration"""
    time_filters: Dict[str, FilterConfig]
    technical_filters: Dict[str, FilterConfig]
    filter_sequence: List[str]           # P1-CH0-4: was missing
    default_error_strategy: str = "pass_through"

    def __post_init__(self):
        try:
            ErrorStrategy(self.default_error_strategy)
        except ValueError:
            valid_strategies = [s.value for s in ErrorStrategy]
            raise ValueError(
                f"Invalid default_error_strategy '{self.default_error_strategy}'. "
                f"Must be one of: {valid_strategies}"
            )

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'FilterPipelineConfig':
        time_filters = {
            name: FilterConfig.from_dict(cfg)
            for name, cfg in d.get('time_filters', {}).items()
        }
        technical_filters = {
            name: FilterConfig.from_dict(cfg)
            for name, cfg in d.get('technical_filters', {}).items()
        }
        return cls(
            time_filters=time_filters,
            technical_filters=technical_filters,
            filter_sequence=list(d.get('filter_sequence', [])),
            default_error_strategy=str(d.get('default_error_strategy', 'pass_through'))
        )


# ============================================================================
# DATA CONFIGURATION
# ============================================================================

@dataclass(frozen=True)
class DataPathsConfig:
    """Data file paths configuration"""
    strategy_ohlcv: Path
    htf_ohlcv: Optional[Path] = None    # Added: was missing in v1.0.3
    ltf_ohlcv: Optional[Path] = None
    artf_ohlcv: Optional[Path] = None

    def __post_init__(self):
        """Validate that strategy_ohlcv is present."""
        # Path coercion happens at from_dict boundary (P1-CH0-2)
        # __post_init__ only validates, does not mutate
        if self.strategy_ohlcv is None:
            raise ValueError("data.paths.strategy_ohlcv is required")

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'DataPathsConfig':
        """Create from dict. Path coercion happens here, not in __post_init__."""
        if 'strategy_ohlcv' not in d or not d['strategy_ohlcv']:
            raise ValueError(
                "data.paths.strategy_ohlcv is required and cannot be empty"
            )
        return cls(
            strategy_ohlcv=Path(d['strategy_ohlcv']),
            htf_ohlcv=Path(d['htf_ohlcv']) if d.get('htf_ohlcv') else None,
            ltf_ohlcv=Path(d['ltf_ohlcv']) if d.get('ltf_ohlcv') else None,
            artf_ohlcv=Path(d['artf_ohlcv']) if d.get('artf_ohlcv') else None
        )


@dataclass(frozen=True)
class DateRangeConfig:
    """Date range configuration"""
    start: str
    end: str

    # Strict format: YYYY-MM-DD HH:MM:SS
    _DATETIME_PATTERN: str = field(
        default=r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$',
        init=False,
        repr=False,
        compare=False
    )

    def __post_init__(self):
        pattern = r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'

        if not re.match(pattern, self.start):
            raise ValueError(
                f"Invalid datetime format for start: '{self.start}'. "
                f"Required: 'YYYY-MM-DD HH:MM:SS' (e.g. '2025-01-15 08:00:00')"
            )
        if not re.match(pattern, self.end):
            raise ValueError(
                f"Invalid datetime format for end: '{self.end}'. "
                f"Required: 'YYYY-MM-DD HH:MM:SS' (e.g. '2025-12-31 21:00:00')"
            )

        try:
            start_ts = pd.Timestamp(self.start)
            end_ts = pd.Timestamp(self.end)
        except Exception as e:
            raise ValueError(f"Invalid datetime values: {e}") from e

        if start_ts >= end_ts:
            raise ValueError(
                f"start ({self.start}) must be before end ({self.end})"
            )

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'DateRangeConfig':
        if 'start' not in d or 'end' not in d:
            raise ValueError(
                "data.date_range requires both 'start' and 'end' keys"
            )
        return cls(start=str(d['start']), end=str(d['end']))


@dataclass(frozen=True)
class DataConfig:
    """Complete data configuration"""
    paths: DataPathsConfig
    date_range: DateRangeConfig
    # Informational only — data is already in correct timezone (DEC-035).
    # No conversion is performed at load time.
    timezone: str = "CET"

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'DataConfig':
        return cls(
            paths=DataPathsConfig.from_dict(d.get('paths', {})),
            date_range=DateRangeConfig.from_dict(d.get('date_range', {})),
            timezone=str(d.get('timezone', 'CET'))
        )


# ============================================================================
# EXECUTION CONFIGURATION
# ============================================================================

@dataclass(frozen=True)
class ExecutionConfig:
    """Execution mode configuration"""
    mode: str = "core"

    def __post_init__(self):
        if self.mode == "debug":
            raise ValueError(
                "Execution mode 'debug' has been renamed to 'analytics' "
                "in the new architecture (DEC-022). "
                "Update your YAML: execution.mode: analytics"
            )
        valid_modes = {"core", "analytics"}
        if self.mode not in valid_modes:
            raise ValueError(
                f"Invalid execution.mode '{self.mode}'. "
                f"Must be one of: {valid_modes}"
            )

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'ExecutionConfig':
        return cls(mode=str(d.get('mode', 'core')))


# ============================================================================
# OUTPUT CONFIGURATION
# ============================================================================

@dataclass(frozen=True)
class ReportOutputConfig:
    """Report output settings (analytics mode only)"""
    enabled: bool = True
    output_dir: Path = Path("outputs/strategies/reports")
    theme: str = "dark"
    chart_height_px: int = 300
    brand_name: str = "Strategy"
    include_raw_data: bool = True

    def __post_init__(self):
        if self.theme not in {"dark", "light"}:
            raise ValueError(
                f"output.reports.theme must be 'dark' or 'light', got '{self.theme}'"
            )
        if not (100 <= self.chart_height_px <= 800):
            raise ValueError(
                f"output.reports.chart_height_px must be 100–800, "
                f"got {self.chart_height_px}"
            )

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'ReportOutputConfig':
        return cls(
            enabled=bool(d.get('enabled', True)),
            output_dir=Path(d.get('output_dir', 'outputs/strategies/reports')),
            theme=str(d.get('theme', 'dark')),
            chart_height_px=int(d.get('chart_height_px', 300)),
            brand_name=str(d.get('brand_name', 'Strategy')),
            include_raw_data=bool(d.get('include_raw_data', True))
        )


@dataclass(frozen=True)
class LoggingOutputConfig:
    """Logging output settings"""
    level: str = "INFO"
    output_dir: Path = Path("outputs/strategies/logs")

    def __post_init__(self):
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR"}
        if self.level not in valid_levels:
            raise ValueError(
                f"output.logging.level must be one of {valid_levels}, got '{self.level}'"
            )

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'LoggingOutputConfig':
        return cls(
            level=str(d.get('level', 'INFO')).upper(),
            output_dir=Path(d.get('output_dir', 'outputs/strategies/logs'))
        )


@dataclass(frozen=True)
class OutputConfig:
    """Complete output configuration"""
    reports: ReportOutputConfig = field(default_factory=ReportOutputConfig)
    logging: LoggingOutputConfig = field(default_factory=LoggingOutputConfig)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'OutputConfig':
        return cls(
            reports=ReportOutputConfig.from_dict(d.get('reports', {})),
            logging=LoggingOutputConfig.from_dict(d.get('logging', {}))
        )


# ============================================================================
# COMPLETE STRATEGY CONFIGURATION
# ============================================================================

@dataclass(frozen=True)
class StrategyConfig:
    """
    Complete strategy configuration (type-safe, frozen).
    
    Loaded from strategy_template.yaml via from_yaml().
    All sub-configs are validated at construction time — fail fast.
    """
    data: DataConfig
    execution: ExecutionConfig
    trade_management: TradeManagementConfig
    filters: FilterPipelineConfig
    output: OutputConfig = field(default_factory=OutputConfig)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'StrategyConfig':
        """Create from dict with full validation"""
        return cls(
            data=DataConfig.from_dict(d.get('data', {})),
            execution=ExecutionConfig.from_dict(d.get('execution', {})),
            trade_management=TradeManagementConfig.from_dict(
                d.get('trade_management', {})
            ),
            filters=FilterPipelineConfig.from_dict(d.get('filters', {})),
            output=OutputConfig.from_dict(d.get('output', {}))
        )

    @classmethod
    def from_yaml(cls, yaml_path: Path) -> 'StrategyConfig':
        """
        Load and validate config from YAML file.

        Args:
            yaml_path: Path to strategy YAML config file

        Returns:
            Validated StrategyConfig

        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If any field fails validation
            yaml.YAMLError: If YAML is malformed
        """
        if not yaml_path.exists():
            raise FileNotFoundError(
                f"Config file not found: {yaml_path}. "
                f"Check the path or copy from configs/strategy_template.yaml"
            )

        with open(yaml_path, 'r') as f:
            raw_config = yaml.safe_load(f)

        if not isinstance(raw_config, dict):
            raise ValueError(
                f"Config file {yaml_path} must be a YAML mapping, "
                f"got {type(raw_config).__name__}"
            )

        try:
            config = cls.from_dict(raw_config)
        except Exception as e:
            raise ValueError(
                f"Config validation failed for {yaml_path}:\n  {e}"
            ) from e

        logger.debug(
            f"Config loaded: {yaml_path} | "
            f"mode={config.execution.mode} | "
            f"range={config.data.date_range.start} → {config.data.date_range.end}"
        )
        return config


# ============================================================================
# VALIDATION HELPERS
# ============================================================================

def validate_config(config_path: Path) -> StrategyConfig:
    """
    Validate a YAML config file and return typed config.

    Args:
        config_path: Path to config YAML

    Returns:
        Validated StrategyConfig

    Example:
        try:
            config = validate_config(Path("configs/strategy_template.yaml"))
            print("✅ Config valid!")
        except (FileNotFoundError, ValueError) as e:
            print(f"❌ Config invalid: {e}")
    """
    return StrategyConfig.from_yaml(config_path)


def check_config_compatibility(
    config: StrategyConfig,
    required_features: Optional[List[str]] = None
) -> bool:
    """
    Check if config has required features enabled.

    Args:
        config: Strategy configuration
        required_features: Features to check.
            Supported: "htf_data", "ltf_data", "artf_data", "spread", "pyramiding"

    Returns:
        True if all required features are present and enabled
    """
    if required_features is None:
        return True

    for feature in required_features:
        if feature == "htf_data":
            if config.data.paths.htf_ohlcv is None:
                return False
        elif feature == "ltf_data":
            if config.data.paths.ltf_ohlcv is None:
                return False
        elif feature == "artf_data":
            if config.data.paths.artf_ohlcv is None:
                return False
        elif feature == "spread":
            if not config.trade_management.spread.enabled:
                return False
        elif feature == "pyramiding":
            if not config.trade_management.position_control.pyramiding_enabled:
                return False

    return True
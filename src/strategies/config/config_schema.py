"""
Config Schema Validation - Type-Safe Configuration
Version: 2.3.0
This module defines a comprehensive, type-safe configuration schema for the trading strategy.
"""
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from pathlib import Path
from enum import Enum
import logging
import re
import yaml
import pandas as pd

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

class TPMode(Enum):
    """Take profit calculation mode"""
    RR_RATIO = "rr_ratio"              # TP = entry ± ATR × sl_mult × rr_ratio
    ATR_MULTIPLIER = "atr_multiplier"  # TP = entry ± ATR × atr_multiplier_tp

# Valid pandas offset aliases for HTF periods
_VALID_HTF_PERIODS = frozenset({
    "1min", "5min", "10min", "15min", "30min", "1H", "4H", "1D", "1W"
})

# ============================================================================
# ASSET CONFIGURATION
# ============================================================================
@dataclass(frozen=True)
class AssetConfig:
    """Asset-specific configuration"""
    symbol: str
    pip_size: float = 0.0001
    point_size: float = 0.00001

    def __post_init__(self):
        if not self.symbol or not self.symbol.strip():
            raise ValueError("asset.symbol cannot be blank")
        if self.pip_size <= 0:
            raise ValueError(f"pip_size must be positive, got {self.pip_size}")
        if self.point_size <= 0:
            raise ValueError(f"point_size must be positive, got {self.point_size}")

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'AssetConfig':
        return cls(
            symbol=str(d.get('symbol', '')),
            pip_size=float(d.get('pip_size', 0.0001)),
            point_size=float(d.get('point_size', 0.00001)),
        )

# ============================================================================
# SPREAD CONFIGURATION - broker_spreads.yaml based (optional)
# ============================================================================
@dataclass(frozen=True)
class SpreadConfig:
    """Spread configuration - values loaded from broker file only"""
    enabled: bool
    config_path: Optional[Path] = None

    def __post_init__(self):
        """Validate spread configuration"""
        if self.enabled and self.config_path is None:
            raise ValueError(
                "spread.config_path is required when spread.enabled=True. "
                "Path must point to broker_spreads.yaml containing spread definitions."
            )
        if self.enabled and self.config_path is not None:
            resolved = Path(self.config_path).resolve()
            if not resolved.exists():
                raise FileNotFoundError(
                    f"Spread config file not found: {resolved}. "
                    f"Verify trade_management.spread.config_path in your strategy YAML."
                )

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'SpreadConfig':
        return cls(
            enabled=bool(d.get('enabled', False)),
            config_path=Path(d['config_path']) if d.get('config_path') else None,
        )

# ============================================================================
# RISK MANAGEMENT CONFIGURATION
# ============================================================================

@dataclass(frozen=True)
class RiskConfig:
    """Risk management configuration
    
    Note: max_risk_percentile is interpreted as a PERCENTAGE value.
    Examples:
        - 0.5  → 0.5% of annual range (conservative)
        - 1.5  → 1.5% of annual range (moderate)
        - 3.0  → 3.0% of annual range (aggressive)
        - 100.0 → 100% of annual range (effectively disables filter)
        - 500.0 → 500% of annual range (effectively disables filter)
    
    The filter is ACTIVE when max_risk_percentile < 100.0
    The filter is DISABLED when max_risk_percentile >= 100.0
    """
    atr_length: int
    atr_multiplier_sl: float
    atr_multiplier_tp: float
    max_risk_percentile: float
    tp_mode: str = "rr_ratio"
    risk_to_reward_ratio: float = 5.7
    
    # Reference to DataConfig for cross-validation (set by StrategyConfig)
    _data_config: Optional['DataConfig'] = field(default=None, repr=False, compare=False)

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

        if not (0 < self.max_risk_percentile <= 500.0):
            raise ValueError(
                f"max_risk_percentile must be between 0 and 500.0 (% of annual range), "
                f"got {self.max_risk_percentile}"
            )

        # Warning for unusually high values (but still valid)
        if self.max_risk_percentile > 5.0 and self.max_risk_percentile < 100.0:
            logger.warning(
                f"max_risk_percentile={self.max_risk_percentile}% is unusually high "
                f"(>5.0% of annual range). Verify this is intentional."
            )
        elif self.max_risk_percentile >= 100.0:
            logger.info(
                f"Risk filter DISABLED: max_risk_percentile={self.max_risk_percentile}% "
                f"(values >= 100% disable filtering)"
            )

        try:
            TPMode(self.tp_mode)
        except ValueError:
            valid_modes = [t.value for t in TPMode]
            raise ValueError(
                f"risk.tp_mode='{self.tp_mode}' is invalid. "
                f"Valid values: {valid_modes}."
            )

        if self.tp_mode == "rr_ratio" and self.risk_to_reward_ratio <= 0:
            raise ValueError(
                f"risk.risk_to_reward_ratio must be > 0 when tp_mode='rr_ratio'. "
                f"Got: {self.risk_to_reward_ratio}"
            )

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'RiskConfig':
        return cls(
            atr_length=int(d.get('atr_length', 14)),
            atr_multiplier_sl=float(d.get('atr_multiplier_sl', 2.0)),
            atr_multiplier_tp=float(d.get('atr_multiplier_tp', 4.0)),
            max_risk_percentile=float(d.get('max_risk_percentile', 0.5)),
            tp_mode=str(d.get('tp_mode', 'rr_ratio')),
            risk_to_reward_ratio=float(d.get('risk_to_reward_ratio', 5.7)),
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
        known_keys = {'enabled', 'error_strategy'}
        config_params = {k: v for k, v in d.items() if k not in known_keys}
        return cls(
            enabled=bool(d.get('enabled', True)),
            error_strategy=str(d.get('error_strategy', 'pass_through')),
            config=config_params
        )

@dataclass(frozen=True)
class TimeFilterConfig:
    """Typed configuration for time filter"""
    enabled: bool
    session_start_hour: int
    session_start_minute: int
    session_end_hour: int
    session_end_minute: int

    def __post_init__(self):
        start_minutes = self.session_start_hour * 60 + self.session_start_minute
        end_minutes = self.session_end_hour * 60 + self.session_end_minute

        if not (0 <= self.session_start_hour < 24):
            raise ValueError(f"session_start_hour must be 0-23, got {self.session_start_hour}")
        if not (0 <= self.session_start_minute < 60):
            raise ValueError(f"session_start_minute must be 0-59, got {self.session_start_minute}")
        if not (0 <= self.session_end_hour < 24):
            raise ValueError(f"session_end_hour must be 0-23, got {self.session_end_hour}")
        if not (0 <= self.session_end_minute < 60):
            raise ValueError(f"session_end_minute must be 0-59, got {self.session_end_minute}")
        if start_minutes >= end_minutes:
            raise ValueError(
                f"Session start ({self.session_start_hour:02d}:{self.session_start_minute:02d}) "
                f"must be before end ({self.session_end_hour:02d}:{self.session_end_minute:02d})"
            )

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'TimeFilterConfig':
        start = d.get('session_start', {})
        end = d.get('session_end', {})
        return cls(
            enabled=bool(d.get('enabled', True)),
            session_start_hour=int(start.get('hour', 8)),
            session_start_minute=int(start.get('minute', 30)),
            session_end_hour=int(end.get('hour', 20)),
            session_end_minute=int(end.get('minute', 30)),
        )


@dataclass(frozen=True)
class FilterPipelineConfig:
    """Complete filter pipeline configuration"""
    time_filters: Dict[str, FilterConfig]
    technical_filters: Dict[str, FilterConfig]
    filter_sequence: List[str]
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
    htf_ohlcv: Optional[Path] = None
    ltf_ohlcv: Optional[Path] = None
    artf_ohlcv: Optional[Path] = None

    def __post_init__(self):
        if self.strategy_ohlcv is None:
            raise ValueError("data.paths.strategy_ohlcv is required")

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'DataPathsConfig':
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
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> Optional['DateRangeConfig']:
        """
        Build DateRangeConfig from a dict.

        Returns None if d is None — supports `date_range: null` in YAML
        without raising. Raises ValueError if d is present but malformed.
        """
        if d is None:
            return None
        if 'start' not in d or 'end' not in d:
            raise ValueError(
                "data.date_range requires both 'start' and 'end' keys. "
                "To disable date filtering entirely, set: date_range: null"
            )
        return cls(start=str(d['start']), end=str(d['end']))

@dataclass(frozen=True)
class DataConfig:
    """Complete data configuration"""
    paths: DataPathsConfig
    date_range: Optional[DateRangeConfig]
    timezone: str = "CET"
    htf_period: str = "1H"
    ltf_timeframe: str = "1s"
    artf_timeframe: str = "1ME"

    def __post_init__(self):
        if self.htf_period not in _VALID_HTF_PERIODS:
            raise ValueError(
                f"data.htf_period='{self.htf_period}' is not a recognised period. "
                f"Valid values: {sorted(_VALID_HTF_PERIODS)}"
            )

        if self.paths.ltf_ohlcv is not None and not self.ltf_timeframe.strip():
            raise ValueError(
                "data.ltf_timeframe is required when data.paths.ltf_ohlcv is set. "
                "Example: ltf_timeframe: '1s'"
            )

        if self.paths.artf_ohlcv is not None and not self.artf_timeframe.strip():
            raise ValueError(
                "data.artf_timeframe is required when data.paths.artf_ohlcv is set. "
                "Example: artf_timeframe: '1ME'"
            )

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'DataConfig':
        return cls(
            paths=DataPathsConfig.from_dict(d.get('paths', {})),
            date_range=DateRangeConfig.from_dict(d.get('date_range')),
            timezone=str(d.get('timezone', 'CET')),
            htf_period=str(d.get('htf_period', '1H')),
            ltf_timeframe=str(d.get('ltf_timeframe', '1s')),
            artf_timeframe=str(d.get('artf_timeframe', '1ME')),
        )

# ============================================================================
# EXECUTION CONFIGURATION
# ============================================================================

@dataclass(frozen=True)
class ExecutionConfig:
    """Execution mode configuration"""
    mode: str = "core"

    def __post_init__(self):
        valid_modes = {"core", "analytics"}
        if self.mode not in valid_modes:
            raise ValueError(
                f"Invalid execution.mode '{self.mode}'. "
                f"Must be one of: {valid_modes}. "
                f"Note: 'debug' is not a valid mode and has been removed."
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
        if not self.brand_name.strip():
            raise ValueError("brand_name must not be blank")

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
    asset: AssetConfig
    data: DataConfig
    execution: ExecutionConfig
    trade_management: TradeManagementConfig
    filters: FilterPipelineConfig
    output: OutputConfig = field(default_factory=OutputConfig)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'StrategyConfig':
        """Create from dict with full validation"""
        # First construct all sub-configs independently
        asset = AssetConfig.from_dict(d.get('asset', {}))
        data = DataConfig.from_dict(d.get('data', {}))
        execution = ExecutionConfig.from_dict(d.get('execution', {}))
        trade_mgmt = TradeManagementConfig.from_dict(d.get('trade_management', {}))
        filters = FilterPipelineConfig.from_dict(d.get('filters', {}))
        output = OutputConfig.from_dict(d.get('output', {}))
        
        # Then perform cross-validation between config sections
        cls._validate_risk_data_dependency(trade_mgmt.risk, data)
        
        # Link RiskConfig to DataConfig for potential future use
        # This is done via object.__setattr__ because the dataclass is frozen
        risk_with_data = RiskConfig(
            atr_length=trade_mgmt.risk.atr_length,
            atr_multiplier_sl=trade_mgmt.risk.atr_multiplier_sl,
            atr_multiplier_tp=trade_mgmt.risk.atr_multiplier_tp,
            max_risk_percentile=trade_mgmt.risk.max_risk_percentile,
            tp_mode=trade_mgmt.risk.tp_mode,
            risk_to_reward_ratio=trade_mgmt.risk.risk_to_reward_ratio,
            _data_config=data
        )
        
        # Reconstruct trade_management with linked risk config
        trade_mgmt_with_link = TradeManagementConfig(
            spread=trade_mgmt.spread,
            risk=risk_with_data,
            position_control=trade_mgmt.position_control
        )
        
        return cls(
            asset=asset,
            data=data,
            execution=execution,
            trade_management=trade_mgmt_with_link,
            filters=filters,
            output=output
        )

    @classmethod
    def _validate_risk_data_dependency(cls, risk: RiskConfig, data: DataConfig) -> None:
        """
        Validate that ARTF data is available when risk percentile filtering is enabled.
        
        Fail Fast Principle: If risk filter is enabled (<100.0%) but ARTF data is missing,
        abort immediately with clear error message.
        
        Note: max_risk_percentile is interpreted as a PERCENTAGE value.
        Filter is ACTIVE when < 100.0, DISABLED when >= 100.0
        """
        if risk.max_risk_percentile >= 100.0:
            # Risk filter disabled - no ARTF data needed
            return
        
        # Risk filter enabled (max_risk_percentile < 100.0) - ARTF data REQUIRED
        if data.paths.artf_ohlcv is None:
            raise ValueError(
                f"Risk filter ENABLED (max_risk_percentile={risk.max_risk_percentile}% < 100%) "
                f"but ARTF monthly data path is not configured.\n\n"
                f"To use risk percentile filtering, you MUST provide monthly ARTF data:\n"
                f"  data.paths.artf_ohlcv: 'data/processed/ohlcv/DEUIDXEUR_1ME_20210101_20260207.parquet'\n\n"
                f"To disable risk filtering, set max_risk_percentile >= 100% "
                f"(e.g., 100.0 for 100% of annual range, or 500.0 for 500%)."
            )
        
        # Check that the file exists (optional but helpful fail-fast)
        if not data.paths.artf_ohlcv.exists():
            raise FileNotFoundError(
                f"ARTF monthly data file configured but not found:\n"
                f"  {data.paths.artf_ohlcv}\n\n"
                f"Risk filter enabled (max_risk_percentile={risk.max_risk_percentile}%) "
                f"requires this file to exist.\n"
                f"Please verify the file path or generate the monthly data first."
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
            "Config loaded: %s | mode=%s | range=%s",
            yaml_path,
            config.execution.mode,
            f"{config.data.date_range.start} → {config.data.date_range.end}"
            if config.data.date_range else "full file (no date range)",
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
    """
    return StrategyConfig.from_yaml(config_path)
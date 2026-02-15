"""
Config Schema Validation - Type-Safe Configuration

Session 12 - Task 3
Version: 1.0.0

Provides type-safe configuration loading with validation.
Replaces fragile dict-based configs with typed dataclasses.

Design Principles:
- Single Responsibility: Only config validation
- Explicit Contracts: All config fields typed
- Type Safety: Dataclasses with validation
- Production-Ready: Fail fast with clear error messages
- Performance-Driven: Validate once at startup
"""
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from pathlib import Path
from enum import Enum
import yaml


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

@dataclass
class SpreadConfig:
    """Spread configuration for realistic execution"""
    enabled: bool
    spread_type: str  # Will validate against SpreadType
    spread_value: float
    
    def __post_init__(self):
        """Validate spread configuration"""
        # Validate spread_type
        try:
            SpreadType(self.spread_type)
        except ValueError:
            valid_types = [t.value for t in SpreadType]
            raise ValueError(
                f"Invalid spread_type '{self.spread_type}'. "
                f"Must be one of: {valid_types}"
            )
        
        # Validate spread_value
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
            enabled=d.get('enabled', False),
            spread_type=d.get('spread_type', 'percentage'),
            spread_value=float(d.get('spread_value', 0.0))
        )


# ============================================================================
# RISK MANAGEMENT CONFIGURATION
# ============================================================================

@dataclass
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
        
        if not (0 < self.max_risk_percentile <= 100):
            raise ValueError(
                f"max_risk_percentile must be between 0 and 100, "
                f"got {self.max_risk_percentile}"
            )
    
    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'RiskConfig':
        """Create from dict with validation"""
        return cls(
            atr_length=int(d.get('atr_length', 14)),
            atr_multiplier_sl=float(d.get('atr_multiplier_sl', 2.0)),
            atr_multiplier_tp=float(d.get('atr_multiplier_tp', 4.0)),
            max_risk_percentile=float(d.get('max_risk_percentile', 3.0))
        )


# ============================================================================
# TRADE MANAGEMENT CONFIGURATION
# ============================================================================

@dataclass
class TradeManagementConfig:
    """Trade management and position rules"""
    spread: SpreadConfig
    risk: RiskConfig
    pyramiding_enabled: bool
    close_on_opposite: bool
    max_positions: int
    
    def __post_init__(self):
        """Validate trade management configuration"""
        if self.max_positions < 1:
            raise ValueError(
                f"max_positions must be >= 1, got {self.max_positions}"
            )
        
        if self.pyramiding_enabled and self.max_positions == 1:
            raise ValueError(
                "Pyramiding enabled but max_positions=1 (contradiction)"
            )
    
    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'TradeManagementConfig':
        """Create from dict with validation"""
        return cls(
            spread=SpreadConfig.from_dict(d.get('spread', {})),
            risk=RiskConfig.from_dict(d.get('risk', {})),
            pyramiding_enabled=d.get('pyramiding_enabled', False),
            close_on_opposite=d.get('close_on_opposite', True),
            max_positions=int(d.get('max_positions', 1))
        )


# ============================================================================
# FILTER CONFIGURATION
# ============================================================================

@dataclass
class FilterConfig:
    """Filter configuration"""
    enabled: bool
    error_strategy: str = "pass_through"
    config: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate filter configuration"""
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
        """Create from dict with validation"""
        return cls(
            enabled=d.get('enabled', True),
            error_strategy=d.get('error_strategy', 'pass_through'),
            config=d.get('config', {})
        )


@dataclass
class FilterPipelineConfig:
    """Complete filter pipeline configuration"""
    time_filters: Dict[str, FilterConfig]
    technical_filters: Dict[str, FilterConfig]
    default_error_strategy: str = "pass_through"
    
    def __post_init__(self):
        """Validate pipeline configuration"""
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
        """Create from dict with validation"""
        time_filters = {}
        for name, config in d.get('time_filters', {}).items():
            time_filters[name] = FilterConfig.from_dict(config)
        
        technical_filters = {}
        for name, config in d.get('technical_filters', {}).items():
            technical_filters[name] = FilterConfig.from_dict(config)
        
        return cls(
            time_filters=time_filters,
            technical_filters=technical_filters,
            default_error_strategy=d.get('default_error_strategy', 'pass_through')
        )


# ============================================================================
# DATA CONFIGURATION
# ============================================================================

@dataclass
class DataPathsConfig:
    """Data file paths configuration"""
    strategy_ohlcv: Path
    ltf_ohlcv: Optional[Path] = None
    artf_ohlcv: Optional[Path] = None
    
    def __post_init__(self):
        """Validate paths"""
        # Ensure Path objects
        if not isinstance(self.strategy_ohlcv, Path):
            object.__setattr__(self, 'strategy_ohlcv', Path(self.strategy_ohlcv))
        
        if self.ltf_ohlcv and not isinstance(self.ltf_ohlcv, Path):
            object.__setattr__(self, 'ltf_ohlcv', Path(self.ltf_ohlcv))
        
        if self.artf_ohlcv and not isinstance(self.artf_ohlcv, Path):
            object.__setattr__(self, 'artf_ohlcv', Path(self.artf_ohlcv))
    
    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'DataPathsConfig':
        """Create from dict with validation"""
        return cls(
            strategy_ohlcv=Path(d['strategy_ohlcv']),
            ltf_ohlcv=Path(d['ltf_ohlcv']) if d.get('ltf_ohlcv') else None,
            artf_ohlcv=Path(d['artf_ohlcv']) if d.get('artf_ohlcv') else None
        )


@dataclass
class DateRangeConfig:
    """Date range configuration"""
    start: str
    end: str
    
    def __post_init__(self):
        """Validate date format"""
        import pandas as pd
        try:
            pd.Timestamp(self.start)
            pd.Timestamp(self.end)
        except Exception as e:
            raise ValueError(
                f"Invalid date format. Use YYYY-MM-DD format. Error: {e}"
            )
        
        if pd.Timestamp(self.start) >= pd.Timestamp(self.end):
            raise ValueError(
                f"start date ({self.start}) must be before end date ({self.end})"
            )
    
    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'DateRangeConfig':
        """Create from dict with validation"""
        return cls(
            start=d['start'],
            end=d['end']
        )


@dataclass
class DataConfig:
    """Complete data configuration"""
    paths: DataPathsConfig
    date_range: DateRangeConfig
    timezone: str = "UTC"
    
    def __post_init__(self):
        """Validate data configuration"""
        # Validate timezone
        from zoneinfo import ZoneInfo, available_timezones
        try:
            ZoneInfo(self.timezone)
        except Exception:
            raise ValueError(
                f"Invalid timezone '{self.timezone}'. "
                f"Must be a valid timezone (e.g., 'UTC', 'America/New_York')"
            )
    
    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'DataConfig':
        """Create from dict with validation"""
        return cls(
            paths=DataPathsConfig.from_dict(d.get('paths', {})),
            date_range=DateRangeConfig.from_dict(d.get('date_range', {})),
            timezone=d.get('timezone', 'UTC')
        )


# ============================================================================
# COMPLETE STRATEGY CONFIGURATION
# ============================================================================

@dataclass
class StrategyConfig:
    """Complete strategy configuration (type-safe)"""
    data: DataConfig
    trade_management: TradeManagementConfig
    filters: FilterPipelineConfig
    
    # Optional sections
    output: Dict[str, Any] = field(default_factory=dict)
    debug: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'StrategyConfig':
        """Create from dict with full validation"""
        return cls(
            data=DataConfig.from_dict(d.get('data', {})),
            trade_management=TradeManagementConfig.from_dict(
                d.get('trade_management', {})
            ),
            filters=FilterPipelineConfig.from_dict(d.get('filters', {})),
            output=d.get('output', {}),
            debug=d.get('debug', {})
        )
    
    @classmethod
    def from_yaml(cls, yaml_path: Path) -> 'StrategyConfig':
        """
        Load and validate config from YAML file.
        
        Args:
            yaml_path: Path to YAML config file
        
        Returns:
            Validated StrategyConfig
        
        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If validation fails
            yaml.YAMLError: If YAML parsing fails
        
        Example:
            config = StrategyConfig.from_yaml(Path("config.yaml"))
            print(f"Max risk: {config.trade_management.risk.max_risk_percentile}%")
        """
        if not yaml_path.exists():
            raise FileNotFoundError(f"Config file not found: {yaml_path}")
        
        with open(yaml_path, 'r') as f:
            raw_config = yaml.safe_load(f)
        
        try:
            return cls.from_dict(raw_config)
        except Exception as e:
            raise ValueError(
                f"Config validation failed for {yaml_path}: {e}"
            ) from e


# ============================================================================
# VALIDATION HELPERS
# ============================================================================

def validate_config(config_path: Path) -> StrategyConfig:
    """
    Validate configuration file and return typed config.
    
    Args:
        config_path: Path to config YAML
    
    Returns:
        Validated StrategyConfig
    
    Example:
        try:
            config = validate_config(Path("config.yaml"))
            print("✅ Config valid!")
        except ValueError as e:
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
        required_features: List of required features
            (e.g., ["ltf_data", "spread", "pyramiding"])
    
    Returns:
        True if all required features present
    
    Example:
        config = StrategyConfig.from_yaml(path)
        if check_config_compatibility(config, ["ltf_data", "spread"]):
            print("✅ Config compatible with LTF execution")
    """
    if required_features is None:
        return True
    
    for feature in required_features:
        if feature == "ltf_data":
            if config.data.paths.ltf_ohlcv is None:
                return False
        elif feature == "artf_data":
            if config.data.paths.artf_ohlcv is None:
                return False
        elif feature == "spread":
            if not config.trade_management.spread.enabled:
                return False
        elif feature == "pyramiding":
            if not config.trade_management.pyramiding_enabled:
                return False
    
    return True


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    import sys
    
    print("=" * 70)
    print("CONFIG SCHEMA VALIDATION - DEMO")
    print("=" * 70)
    
    # Example 1: Valid config
    print("\n1️⃣  Testing VALID config...")
    valid_config = {
        'data': {
            'paths': {
                'strategy_ohlcv': 'data/strategy.parquet',
                'ltf_ohlcv': 'data/ltf.parquet',
            },
            'date_range': {
                'start': '2025-01-01',
                'end': '2025-12-31'
            }
        },
        'trade_management': {
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
        },
        'filters': {
            'time_filters': {},
            'technical_filters': {}
        }
    }
    
    try:
        config = StrategyConfig.from_dict(valid_config)
        print("✅ Valid config loaded successfully!")
        print(f"   - ATR Length: {config.trade_management.risk.atr_length}")
        print(f"   - Max Risk: {config.trade_management.risk.max_risk_percentile}%")
        print(f"   - Spread Enabled: {config.trade_management.spread.enabled}")
    except ValueError as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)
    
    # Example 2: Invalid spread_type
    print("\n2️⃣  Testing INVALID spread_type...")
    invalid_spread = valid_config.copy()
    invalid_spread['trade_management']['spread']['spread_type'] = 'invalid_type'
    
    try:
        config = StrategyConfig.from_dict(invalid_spread)
        print("❌ Should have failed validation!")
        sys.exit(1)
    except ValueError as e:
        print(f"✅ Correctly rejected: {e}")
    
    # Example 3: Invalid date range
    print("\n3️⃣  Testing INVALID date range...")
    invalid_dates = valid_config.copy()
    invalid_dates['data']['date_range']['start'] = '2025-12-31'
    invalid_dates['data']['date_range']['end'] = '2025-01-01'
    
    try:
        config = StrategyConfig.from_dict(invalid_dates)
        print("❌ Should have failed validation!")
        sys.exit(1)
    except ValueError as e:
        print(f"✅ Correctly rejected: {e}")
    
    # Example 4: Pyramiding contradiction
    print("\n4️⃣  Testing PYRAMIDING contradiction...")
    invalid_pyramiding = valid_config.copy()
    invalid_pyramiding['trade_management']['pyramiding_enabled'] = True
    invalid_pyramiding['trade_management']['max_positions'] = 1
    
    try:
        config = StrategyConfig.from_dict(invalid_pyramiding)
        print("❌ Should have failed validation!")
        sys.exit(1)
    except ValueError as e:
        print(f"✅ Correctly rejected: {e}")
    
    print("\n" + "=" * 70)
    print("✅ ALL VALIDATION TESTS PASSED!")
    print("=" * 70)
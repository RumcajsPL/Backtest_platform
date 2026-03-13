"""
BrokerSupportConfig — typed configuration for live paper trading.

Loaded from configs/broker_support/broker_support_config.yaml.
All sub-configs are validated at construction time — fail fast.
No defaults that silently hide misconfiguration.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import yaml

# ---------------------------------------------------------------------------
# Valid eToro candle interval strings
# ---------------------------------------------------------------------------
_VALID_INTERVALS = frozenset({
    "OneMinute", "FiveMinutes", "TenMinutes", "FifteenMinutes",
    "ThirtyMinutes", "OneHour", "FourHours", "OneDay", "OneWeek",
})
_VALID_DIRECTIONS = frozenset({"asc", "desc"})


# ---------------------------------------------------------------------------
# Sub-configs
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class StrategyRef:
    yaml_path: Path

    def __post_init__(self) -> None:
        if not self.yaml_path.exists():
            raise FileNotFoundError(
                f"strategy.yaml_path not found: {self.yaml_path}. "
                f"Verify the path in broker_support_config.yaml."
            )

    @classmethod
    def from_dict(cls, d: dict) -> "StrategyRef":
        raw = d.get("yaml_path")
        if not raw:
            raise ValueError("strategy.yaml_path is required in broker_support_config.yaml")
        return cls(yaml_path=Path(raw))


@dataclass(frozen=True)
class LiveDataConfig:
    artf_ohlcv_path: Path
    strategy_bars_to_fetch: int
    htf_bars_to_fetch: int
    strategy_interval: str
    htf_interval: str
    candle_direction: str

    def __post_init__(self) -> None:
        if not self.artf_ohlcv_path.exists():
            raise FileNotFoundError(
                f"live_data.artf_ohlcv_path not found: {self.artf_ohlcv_path}. "
                f"Verify the path in broker_support_config.yaml."
            )
        if self.strategy_bars_to_fetch < 50:
            raise ValueError(
                f"live_data.strategy_bars_to_fetch must be >= 50 "
                f"(need enough bars for ATR warmup + filters), "
                f"got {self.strategy_bars_to_fetch}"
            )
        if self.htf_bars_to_fetch < 10:
            raise ValueError(
                f"live_data.htf_bars_to_fetch must be >= 10, "
                f"got {self.htf_bars_to_fetch}"
            )
        if self.strategy_bars_to_fetch > 1000 or self.htf_bars_to_fetch > 1000:
            raise ValueError(
                "eToro candles endpoint maximum is 1000 bars per request. "
                f"Got strategy={self.strategy_bars_to_fetch}, htf={self.htf_bars_to_fetch}"
            )
        if self.strategy_interval not in _VALID_INTERVALS:
            raise ValueError(
                f"live_data.strategy_interval='{self.strategy_interval}' is invalid. "
                f"Valid values: {sorted(_VALID_INTERVALS)}"
            )
        if self.htf_interval not in _VALID_INTERVALS:
            raise ValueError(
                f"live_data.htf_interval='{self.htf_interval}' is invalid. "
                f"Valid values: {sorted(_VALID_INTERVALS)}"
            )
        if self.candle_direction not in _VALID_DIRECTIONS:
            raise ValueError(
                f"live_data.candle_direction='{self.candle_direction}' is invalid. "
                f"Must be 'asc' or 'desc'."
            )

    @classmethod
    def from_dict(cls, d: dict) -> "LiveDataConfig":
        return cls(
            artf_ohlcv_path=Path(d["artf_ohlcv_path"]),
            strategy_bars_to_fetch=int(d.get("strategy_bars_to_fetch", 500)),
            htf_bars_to_fetch=int(d.get("htf_bars_to_fetch", 120)),
            strategy_interval=str(d.get("strategy_interval", "OneMinute")),
            htf_interval=str(d.get("htf_interval", "OneHour")),
            candle_direction=str(d.get("candle_direction", "desc")),
        )


@dataclass(frozen=True)
class TradingWindowConfig:
    enabled: bool
    allowed_hours_utc: List[int]
    skip_hours_utc: List[int]
    monday_size_factor: float

    def __post_init__(self) -> None:
        for h in self.allowed_hours_utc:
            if not (0 <= h <= 23):
                raise ValueError(
                    f"trading_window.allowed_hours_utc contains invalid hour {h}. "
                    f"Must be 0–23."
                )
        for h in self.skip_hours_utc:
            if not (0 <= h <= 23):
                raise ValueError(
                    f"trading_window.skip_hours_utc contains invalid hour {h}. "
                    f"Must be 0–23."
                )
        if not (0.0 < self.monday_size_factor <= 1.0):
            raise ValueError(
                f"trading_window.monday_size_factor must be (0, 1.0], "
                f"got {self.monday_size_factor}"
            )

    @classmethod
    def from_dict(cls, d: dict) -> "TradingWindowConfig":
        return cls(
            enabled=bool(d.get("enabled", True)),
            allowed_hours_utc=list(d.get("allowed_hours_utc", [9,10,11,12,13,14,15,16])),
            skip_hours_utc=list(d.get("skip_hours_utc", [17, 18])),
            monday_size_factor=float(d.get("monday_size_factor", 1.0)),
        )


@dataclass(frozen=True)
class ExecutionConfig:
    instrument_map_path: Path
    symbol: str
    amount_usd: float
    leverage: int

    def __post_init__(self) -> None:
        if not self.instrument_map_path.exists():
            raise FileNotFoundError(
                f"execution.instrument_map_path not found: {self.instrument_map_path}"
            )
        if not self.symbol.strip():
            raise ValueError("execution.symbol cannot be blank")
        if self.amount_usd <= 0:
            raise ValueError(
                f"execution.amount_usd must be positive, got {self.amount_usd}"
            )
        if self.leverage < 1:
            raise ValueError(
                f"execution.leverage must be >= 1, got {self.leverage}"
            )

    @classmethod
    def from_dict(cls, d: dict) -> "ExecutionConfig":
        return cls(
            instrument_map_path=Path(d.get("instrument_map_path",
                                           "configs/broker_support/instrument_map.yaml")),
            symbol=str(d.get("symbol", "DAX")),
            amount_usd=float(d["amount_usd"]),
            leverage=int(d.get("leverage", 1)),
        )


@dataclass(frozen=True)
class SafetyConfig:
    max_open_positions: int
    min_available_cash_usd: float
    max_consecutive_losses: int
    kill_switch_file: str

    def __post_init__(self) -> None:
        if self.max_open_positions < 1:
            raise ValueError(
                f"safety.max_open_positions must be >= 1, got {self.max_open_positions}"
            )
        if self.min_available_cash_usd < 0:
            raise ValueError(
                f"safety.min_available_cash_usd must be >= 0, "
                f"got {self.min_available_cash_usd}"
            )
        if self.max_consecutive_losses < 1:
            raise ValueError(
                f"safety.max_consecutive_losses must be >= 1, "
                f"got {self.max_consecutive_losses}"
            )

    @classmethod
    def from_dict(cls, d: dict) -> "SafetyConfig":
        return cls(
            max_open_positions=int(d.get("max_open_positions", 3)),
            min_available_cash_usd=float(d.get("min_available_cash_usd", 200.0)),
            max_consecutive_losses=int(d.get("max_consecutive_losses", 5)),
            kill_switch_file=str(d.get("kill_switch_file", "STOP")),
        )


# ---------------------------------------------------------------------------
# Root config
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class BrokerSupportConfig:
    """
    Complete live paper trading configuration.

    Loaded from broker_support_config.yaml via from_yaml().
    Validated at construction — fail fast on any misconfiguration.
    """
    strategy: StrategyRef
    live_data: LiveDataConfig
    trading_window: TradingWindowConfig
    execution: ExecutionConfig
    safety: SafetyConfig

    @classmethod
    def from_dict(cls, d: dict) -> "BrokerSupportConfig":
        return cls(
            strategy=StrategyRef.from_dict(d.get("strategy", {})),
            live_data=LiveDataConfig.from_dict(d.get("live_data", {})),
            trading_window=TradingWindowConfig.from_dict(d.get("trading_window", {})),
            execution=ExecutionConfig.from_dict(d.get("execution", {})),
            safety=SafetyConfig.from_dict(d.get("safety", {})),
        )

    @classmethod
    def from_yaml(cls, path: Path) -> "BrokerSupportConfig":
        """
        Load and validate config from YAML file.

        Raises:
            FileNotFoundError: if config file does not exist.
            ValueError:        if any field fails validation.
            yaml.YAMLError:    if YAML is malformed.
        """
        if not path.exists():
            raise FileNotFoundError(
                f"BrokerSupportConfig not found: {path}. "
                f"Copy configs/broker_support/broker_support_config.yaml as a starting point."
            )
        with open(path, "r") as f:
            raw = yaml.safe_load(f)
        if not isinstance(raw, dict):
            raise ValueError(
                f"broker_support_config.yaml must be a YAML mapping, "
                f"got {type(raw).__name__}"
            )
        try:
            return cls.from_dict(raw)
        except Exception as exc:
            raise ValueError(
                f"BrokerSupportConfig validation failed for {path}:\n  {exc}"
            ) from exc

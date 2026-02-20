"""Spread management: broker spread calculations based on BID price data.

MIGRATED: Session 7 — Task 2 (pure utility; no contract dependencies).
HARDENED: Session 20 (Block F) — class-level YAML config cache (DEC-030);
          mode-aware logging (DEC-022); dead utility function removed (DEC-021).

Location: src/strategies/specific/modules/spread_manager.py
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import ClassVar, Dict, Optional, Tuple

import yaml

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module-level config cache
# Each unique YAML path is loaded exactly once per process.  This removes
# repeated file I/O across thousands of TradeSimulator signal iterations.
# ---------------------------------------------------------------------------
_CONFIG_CACHE: Dict[str, Dict] = {}


class SpreadManager:
    """Manages broker spread application assuming input data is BID PRICE.

    Spread application rules
    ------------------------
    * LONG entry:      Bid + Spread  (buy at Ask)
    * LONG SL trigger: Bid_SL − Spread  (sell at Bid)
    * SHORT entry:     Bid  (sell at Bid — no spread on entry)
    * SHORT SL trigger: Bid_SL + Spread  (buy at Ask)

    Supported spread types
    ----------------------
    * ``"percentage"`` — spread as % of price (e.g. 0.05 = 0.05 %)
    * ``"points"``     — absolute price points (e.g. 1.0 = 1 point)
    * ``"pips"``       — forex pips (pip_position key required in config)

    Session 20 changes
    ------------------
    * YAML config is cached at the class level — subsequent instantiations with
      the same path skip file I/O entirely.
    * ``mode`` parameter accepted; logging only emitted in ``"analytics"`` mode.
    * ``calculate_spread_impact()`` module function removed (dead code, DEC-021).
    * ``"debug"`` mode raises ``ValueError`` with migration message (DEC-022).
    """

    _config_cache: ClassVar[Dict[str, Dict]] = _CONFIG_CACHE

    # ------------------------------------------------------------------
    def __init__(
        self,
        asset_symbol: str,
        spread_config_path: Optional[str] = None,
        mode: str = "core",
    ) -> None:
        """
        Parameters
        ----------
        asset_symbol:
            Instrument symbol, e.g. ``"DEUIDXEUR"``.
        spread_config_path:
            Path to ``broker_spreads.yaml``.  Defaults to
            ``<project_root>/configs/spreads/broker_spreads.yaml``.
        mode:
            ``"core"`` or ``"analytics"``.  ``"debug"`` raises ``ValueError``.
        """
        if mode == "debug":
            raise ValueError(
                "Mode 'debug' has been renamed to 'analytics' in the new architecture. "
                "Update your config: execution.mode: analytics"
            )
        if mode not in {"core", "analytics"}:
            raise ValueError(f"Invalid mode '{mode}'. Must be 'core' or 'analytics'.")

        self._mode = mode
        self.asset_symbol = asset_symbol.upper()
        self.spread_config: Optional[Dict] = None
        self.asset_config: Optional[Dict] = None

        config_path = self._resolve_config_path(spread_config_path)
        self._load_config(config_path)

    # ------------------------------------------------------------------
    # Config loading with caching
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_config_path(spread_config_path: Optional[str]) -> Path:
        if spread_config_path is None:
            # Navigate from src/strategies/specific/modules/ → project root
            project_root = Path(__file__).resolve().parents[4]
            return project_root / "configs" / "spreads" / "broker_spreads.yaml"
        return Path(spread_config_path)

    def _load_config(self, config_path: Path) -> None:
        """Load spread configuration, using class-level cache to avoid repeat I/O."""
        path_key = str(config_path.resolve())

        if path_key not in SpreadManager._config_cache:
            # First access for this path — load from disk
            if not config_path.exists():
                raise FileNotFoundError(
                    f"Spread config file not found: {config_path}"
                )
            with open(config_path, "r") as fh:
                loaded = yaml.safe_load(fh)
            SpreadManager._config_cache[path_key] = loaded
            if self._mode == "analytics":
                logger.info(f"Spread config loaded and cached from {config_path}.")
        else:
            if self._mode == "analytics":
                logger.debug(f"Spread config cache hit for {config_path}.")

        self.spread_config = SpreadManager._config_cache[path_key]

        spreads = self.spread_config.get("spreads", {})
        if self.asset_symbol not in spreads:
            if self._mode == "analytics":
                logger.warning(
                    f"Asset '{self.asset_symbol}' not found in spread config. "
                    f"Available: {sorted(spreads.keys())}"
                )
            return

        self.asset_config = spreads[self.asset_symbol]
        if self._mode == "analytics":
            logger.info(
                f"Spread config ready for {self.asset_symbol}: "
                f"{self.asset_config['spread_value']} {self.asset_config['spread_type']}"
            )

    @classmethod
    def clear_config_cache(cls) -> None:
        """Clear the YAML config cache (useful in tests or when config files change)."""
        cls._config_cache.clear()

    @classmethod
    def cache_stats(cls) -> Dict[str, int]:
        """Return cache size (for observability / tests)."""
        return {"config_entries": len(cls._config_cache)}

    # ------------------------------------------------------------------
    # Public spread calculations
    # ------------------------------------------------------------------

    def get_spread_in_points(self, bid_price: float) -> float:
        """Calculate spread in price points for the given bid price.

        Returns ``0.0`` when no asset config is loaded.
        """
        if self.asset_config is None:
            return 0.0

        spread_type: str = self.asset_config["spread_type"]
        spread_value: float = self.asset_config["spread_value"]

        if spread_type == "percentage":
            return (spread_value / 100.0) * bid_price

        if spread_type == "points":
            return spread_value

        if spread_type == "pips":
            pip_position = self.asset_config.get("pip_position", 4)
            return spread_value * (10 ** (-pip_position))

        logger.warning(f"Unknown spread_type '{spread_type}' — returning 0.0.")
        return 0.0

    def calculate_entry_cost(self, bid_price: float, is_long: bool) -> float:
        """Return actual entry price after spread adjustment.

        * LONG:  Bid + Spread  (buy at Ask)
        * SHORT: Bid           (sell at Bid; no spread on entry)
        """
        spread = self.get_spread_in_points(bid_price)
        return bid_price + spread if is_long else bid_price

    def get_sl_trigger_level(
        self, raw_sl_price: float, spread: float, is_long: bool
    ) -> float:
        """Return adjusted SL trigger level accounting for spread.

        * LONG:  trigger = SL − Spread  (exit at Bid)
        * SHORT: trigger = SL + Spread  (exit at Ask)
        """
        return raw_sl_price - spread if is_long else raw_sl_price + spread

    def get_spread_info(self) -> Dict:
        """Return spread configuration summary.

        Returns ``{"enabled": False}`` when no asset config is loaded.
        """
        if self.asset_config is None:
            return {"enabled": False}
        return {
            "enabled": True,
            "asset": self.asset_symbol,
            "spread_value": self.asset_config["spread_value"],
            "spread_type": self.asset_config["spread_type"],
        }

    def is_enabled(self) -> bool:
        """``True`` when asset spread config was found in the YAML."""
        return self.asset_config is not None

    # ------------------------------------------------------------------
    def __repr__(self) -> str:
        if self.asset_config is None:
            return f"SpreadManager({self.asset_symbol}, disabled)"
        return (
            f"SpreadManager({self.asset_symbol}, "
            f"{self.asset_config['spread_value']} {self.asset_config['spread_type']})"
        )
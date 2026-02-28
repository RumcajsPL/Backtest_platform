"""Spread management: broker spread calculations based on BID price data.
Version: 2.0.0 (Hardening II Final)
Session: 21 - Final Hardening

Changes from v1.0.0 (Session 21 updates):
- Block B: Removed all "debug" mode references - strict mode validation
- Block C: Integrated with CacheManager for multi-run cache lifecycle
- DEC-036: Fail-fast config path resolution; _load_global_settings()
- SM-1: Blank symbol guard
- SM-2: Removed hardcoded fallback path - requires explicit config_path

BID price convention
--------------------
All OHLCV data is BID price. Spread model (one spread per round trip):
  LONG:  spread paid at OPEN  → executed_entry = Bid + spread (buy at Ask)
  LONG:  SL exit at Bid       → no spread at SL close
  LONG:  TP exit at Bid       → no spread at TP close
  SHORT: no spread at OPEN    → executed_entry = Bid (sell at Bid)
  SHORT: SL exit at Ask       → trigger_sl = sl_bid + spread (buy to close)
  SHORT: TP exit at Ask       → trigger_tp = tp_bid + spread (buy to close)
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Optional

import yaml

from src.strategies.core.cache_manager import CacheManager

logger = logging.getLogger(__name__)

class SpreadManager:
    """Manages broker spread application assuming input data is BID PRICE.

    Spread application rules
    ------------------------
    * LONG entry:      Bid + Spread  (buy at Ask)
    * LONG SL trigger: Bid_SL − Spread  (exit at Bid — no adjustment needed;
                       SL is a BID level, exit when Bid ≤ SL)
    * LONG TP:         No trigger adjustment — TP exit at Bid, no spread
    * SHORT entry:     Bid  (sell at Bid — no spread on entry)
    * SHORT SL trigger: Bid_SL + Spread  (buy at Ask to close short)
    * SHORT TP trigger: Bid_TP + Spread  (buy at Ask to close short)
    """

    def __init__(
        self,
        asset_symbol: str,
        spread_config_path: Optional[str] = None,
        mode: str = "core",
        cache_manager: Optional[CacheManager] = None,
    ) -> None:
        """
        Parameters
        ----------
        asset_symbol:
            Instrument symbol, e.g. "DEUIDXEUR". Must be non-blank.
        spread_config_path:
            Path to broker_spreads.yaml. Required — no default.
        mode:
            "core" or "analytics". "debug" raises ValueError.
        cache_manager:
            Central cache manager for multi-run state.
        """
        # ── Mode validation ───────────────────────────────────────────────────
        if mode not in {"core", "analytics"}:
            raise ValueError(
                f"Invalid mode '{mode}'. Must be 'core' or 'analytics'. "
                f"'debug' is not a valid mode and has been removed."
            )
        self._mode = mode
        self._cache_manager = cache_manager

        # ── SM-1: Blank symbol guard ──────────────────────────────────────────
        if not asset_symbol or not asset_symbol.strip():
            raise ValueError(
                "SpreadManager requires a non-empty asset_symbol. "
                "Set asset.symbol in your strategy YAML. "
                "The symbol must match a key in broker_spreads.yaml "
                "(e.g. 'DEUIDXEUR', 'EURUSD')."
            )

        self.asset_symbol = asset_symbol.strip().upper()
        self.spread_config: Optional[Dict] = None
        self.asset_config: Optional[Dict] = None

        # Global broker settings — populated by _load_global_settings()
        self.apply_to_long: bool = True
        self.apply_to_short: bool = True
        self.application_method: str = "entry_only"

        config_path = self._resolve_config_path(spread_config_path)
        self._load_config(config_path)

    # ------------------------------------------------------------------
    # Config loading with caching (via CacheManager)
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_config_path(spread_config_path: Optional[str]) -> Path:
        """Resolve broker spread config path — fail-fast, no hardcoded default.
        Raises
        ------
        ValueError
            When spread_config_path is None.
        FileNotFoundError
            When the resolved path does not exist on disk.
        """
        if spread_config_path is None:
            raise ValueError(
                "SpreadManager requires an explicit spread_config_path. "
                "Set trade_management.spread.config_path in your strategy YAML "
                "and ensure it points to a valid broker_spreads.yaml file. "
                "To disable spread: set trade_management.spread.enabled: false."
            )
        path = Path(spread_config_path)
        if not path.exists():
            raise FileNotFoundError(
                f"Broker spread config not found: {path.resolve()}. "
                f"Verify trade_management.spread.config_path in your strategy YAML "
                f"and ensure the file exists at that location."
            )
        return path

    def _load_config(self, config_path: Path) -> None:
        """Load spread configuration, using cache to avoid repeat I/O."""
        path_key = str(config_path.resolve())

        if self._cache_manager:
            cached = self._cache_manager.get_spread_config(path_key)
            if cached is not None:
                self.spread_config = cached
                if self._mode == "analytics":
                    logger.debug(f"Spread config cache hit for {config_path}.")
                self._load_global_settings()
                self._load_asset_config()
                return

        # Cache miss - load from file
        with open(config_path, "r") as fh:
            loaded = yaml.safe_load(fh)

        if self._cache_manager:
            self._cache_manager.set_spread_config(path_key, loaded)

        self.spread_config = loaded
        if self._mode == "analytics":
            logger.info(f"Spread config loaded from {config_path}.")

        self._load_global_settings()
        self._load_asset_config()

    def _load_global_settings(self) -> None:
        """Read and validate global broker settings from broker_spreads.yaml."""
        settings = self.spread_config.get("settings", {})
        self.apply_to_long = settings.get("apply_to_long", True)
        self.apply_to_short = settings.get("apply_to_short", True)
        self.application_method = settings.get("application_method", "entry_only")

        _VALID_METHODS = {"entry_only", "entry_and_exit"}
        if self.application_method not in _VALID_METHODS:
            raise ValueError(
                f"broker_spreads.yaml settings.application_method="
                f"'{self.application_method}' is not recognised. "
                f"Valid values: {sorted(_VALID_METHODS)}. "
                f"Use 'entry_only' (recommended — matches broker CFD reality)."
            )

    def _load_asset_config(self) -> None:
        """Load per-asset configuration from spread config."""
        spreads = self.spread_config.get("spreads", {})
        if self.asset_symbol not in spreads:
            available = sorted(spreads.keys())
            msg = (
                f"Asset '{self.asset_symbol}' not found in spread config. "
                f"Available assets: {available}. "
                f"Add an entry for '{self.asset_symbol}' or correct asset.symbol in your YAML."
            )
            if self.spread_config.get("settings", {}).get("require_spread_for_all_assets", False):
                raise ValueError(msg)
            if self._mode == "analytics" or self.spread_config.get("settings", {}).get(
                "warn_on_missing_spread", True
            ):
                logger.warning(msg)
            return

        self.asset_config = spreads[self.asset_symbol]
        if self._mode == "analytics":
            logger.info(
                f"Spread config ready for {self.asset_symbol}: "
                f"{self.asset_config['spread_value']} {self.asset_config['spread_type']} "
                f"(application: {self.application_method})"
            )

    # ------------------------------------------------------------------
    # Public spread calculations
    # ------------------------------------------------------------------

    def get_spread_in_points(self, bid_price: float) -> float:
        """Calculate spread in price points for the given bid price.

        Returns 0.0 when no asset config is loaded (asset not in broker file).

        Examples
        --------
        DEUIDXEUR (percentage, 0.015%):
            bid=20000 → 0.015/100 × 20000 = 3.0 pts
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

        logger.warning(
            f"Unknown spread_type '{spread_type}' for {self.asset_symbol} — returning 0.0."
        )
        return 0.0

    def calculate_entry_cost(self, bid_price: float, is_long: bool) -> float:
        """Return actual entry price after spread adjustment.

        * LONG:  Bid + Spread  (buy at Ask)
        * SHORT: Bid           (sell at Bid; spread paid at close, not open)
        """
        spread = self.get_spread_in_points(bid_price)
        return bid_price + spread if is_long else bid_price

    def get_sl_trigger_level(
        self, raw_sl_price: float, spread: float, is_long: bool
    ) -> float:
        """Return adjusted SL trigger level accounting for spread.

        * LONG:  trigger = SL − Spread  (exit at Bid when Bid falls to SL level)
        * SHORT: trigger = SL + Spread  (buy at Ask to close; Ask = Bid + spread)
        """
        return raw_sl_price - spread if is_long else raw_sl_price + spread

    def get_tp_trigger_level(
        self, raw_tp_price: float, spread: float, is_long: bool
    ) -> float:
        """Return adjusted TP trigger level accounting for spread.

        * LONG:  trigger = TP  (exit at Bid when Bid rises to TP level — no spread at exit)
        * SHORT: trigger = TP + Spread  (buy at Ask to close when Bid falls to TP level)
        """
        return raw_tp_price if is_long else raw_tp_price + spread

    def get_spread_info(self) -> Dict:
        """Return spread configuration summary including global broker settings."""
        if self.asset_config is None:
            return {
                "enabled": False,
                "apply_to_long": self.apply_to_long,
                "apply_to_short": self.apply_to_short,
                "application_method": self.application_method,
            }
        return {
            "enabled": True,
            "asset": self.asset_symbol,
            "spread_value": self.asset_config["spread_value"],
            "spread_type": self.asset_config["spread_type"],
            "display_name": self.asset_config.get("display_name", self.asset_symbol),
            "asset_class": self.asset_config.get("asset_class", "unknown"),
            "apply_to_long": self.apply_to_long,
            "apply_to_short": self.apply_to_short,
            "application_method": self.application_method,
        }

    def is_enabled(self) -> bool:
        """True when asset spread config was found in the broker YAML."""
        return self.asset_config is not None

    # ------------------------------------------------------------------
    def __repr__(self) -> str:
        if self.asset_config is None:
            return f"SpreadManager({self.asset_symbol}, not found in broker config)"
        return (
            f"SpreadManager({self.asset_symbol}, "
            f"{self.asset_config['spread_value']} {self.asset_config['spread_type']}, "
            f"method={self.application_method})"
        )
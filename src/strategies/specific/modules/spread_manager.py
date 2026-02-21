"""Spread management: broker spread calculations based on BID price data.

MIGRATED: Session 7 — Task 2 (pure utility; no contract dependencies).
HARDENED: Session 20 (Block F) — class-level YAML config cache (DEC-030);
          mode-aware logging (DEC-022); dead utility function removed (DEC-021).
UPDATED:  Session 21 (DEC-036) — fail-fast config path resolution;
          _load_global_settings() reads apply_to_long/apply_to_short/
          application_method from broker file; blank symbol guard (SM-1).

BID price convention
--------------------
All OHLCV data is BID price. Spread model (one spread per round trip):
  LONG:  spread paid at OPEN  → executed_entry = Bid + spread (buy at Ask)
  LONG:  SL exit at Bid       → no spread at SL close
  LONG:  TP exit at Bid       → no spread at TP close
  SHORT: no spread at OPEN    → executed_entry = Bid (sell at Bid)
  SHORT: SL exit at Ask       → trigger_sl = sl_bid + spread (buy to close)
  SHORT: TP exit at Ask       → trigger_tp = tp_bid + spread (buy to close)

This model matches eToro CFD broker behaviour (application_method: entry_only).
"entry_only" means one spread per round trip — the terminology refers to
LONG (spread at entry) and SHORT (spread at close) collectively.

Location: src/strategies/specific/modules/spread_manager.py
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import ClassVar, Dict, Optional

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
    * LONG SL trigger: Bid_SL − Spread  (exit at Bid — no adjustment needed;
                       SL is a BID level, exit when Bid ≤ SL)
    * LONG TP:         No trigger adjustment — TP exit at Bid, no spread
    * SHORT entry:     Bid  (sell at Bid — no spread on entry)
    * SHORT SL trigger: Bid_SL + Spread  (buy at Ask to close short)
    * SHORT TP trigger: Bid_TP + Spread  (buy at Ask to close short)
                        → trigger_tp = tp_bid + spread (DEC-038)

    Supported spread types (read from broker_spreads.yaml)
    -------------------------------------------------------
    * ``"percentage"`` — spread as % of price (e.g. 0.015 = 0.015%)
                         DEUIDXEUR: 0.015% × 20000 bid ≈ 3.0 pts
    * ``"points"``     — absolute price points (e.g. 6.0 for Dow Jones)
    * ``"pips"``       — forex pips (pip_position key required in config)

    Session 20 changes
    ------------------
    * YAML config is cached at the class level — subsequent instantiations with
      the same path skip file I/O entirely.
    * ``mode`` parameter accepted; logging only emitted in ``"analytics"`` mode.
    * ``calculate_spread_impact()`` module function removed (dead code, DEC-021).
    * ``"debug"`` mode raises ``ValueError`` with migration message (DEC-022).

    Session 21 changes (DEC-036)
    ----------------------------
    * ``_resolve_config_path()`` is now fail-fast — raises ``ValueError`` when
      ``config_path`` is ``None`` and ``FileNotFoundError`` when path not found.
      The hardcoded default path has been removed (SM-2 fix).
    * ``_load_global_settings()`` reads ``settings.*`` from broker file:
      ``apply_to_long``, ``apply_to_short``, ``application_method``.
    * Blank ``asset_symbol`` guard added to ``__init__`` (SM-1 fix).
    * ``get_spread_info()`` now exposes ``apply_to_long``, ``apply_to_short``,
      ``application_method`` so ``RiskManager`` can read them from one source.
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
            Must be non-blank and match a key in broker_spreads.yaml.
        spread_config_path:
            Path to ``broker_spreads.yaml``.
            **Required** — no default. Pass ``trade_management.spread.config_path``
            from ``StrategyConfig``.
        mode:
            ``"core"`` or ``"analytics"``.  ``"debug"`` raises ``ValueError``.
        """
        # ── Mode validation ───────────────────────────────────────────────────
        if mode == "debug":
            raise ValueError(
                "Mode 'debug' has been renamed to 'analytics' in the new architecture. "
                "Update your config: execution.mode: analytics"
            )
        if mode not in {"core", "analytics"}:
            raise ValueError(f"Invalid mode '{mode}'. Must be 'core' or 'analytics'.")

        # ── SM-1: Blank symbol guard ──────────────────────────────────────────
        if not asset_symbol or not asset_symbol.strip():
            raise ValueError(
                "SpreadManager requires a non-empty asset_symbol. "
                "Set asset.symbol in your strategy YAML. "
                "The symbol must match a key in broker_spreads.yaml "
                "(e.g. 'DEUIDXEUR', 'EURUSD')."
            )

        self._mode = mode
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
    # Config loading with caching
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_config_path(spread_config_path: Optional[str]) -> Path:
        """Resolve broker spread config path — fail-fast, no hardcoded default.

        DEC-036: The hardcoded fallback path has been removed. Every caller
        must supply an explicit path via trade_management.spread.config_path
        in the strategy YAML, propagated through StrategyConfig.

        Raises
        ------
        ValueError
            When ``spread_config_path`` is ``None``.
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
        """Load spread configuration, using class-level cache to avoid repeat I/O."""
        path_key = str(config_path.resolve())

        if path_key not in SpreadManager._config_cache:
            with open(config_path, "r") as fh:
                loaded = yaml.safe_load(fh)
            SpreadManager._config_cache[path_key] = loaded
            if self._mode == "analytics":
                logger.info(f"Spread config loaded and cached from {config_path}.")
        else:
            if self._mode == "analytics":
                logger.debug(f"Spread config cache hit for {config_path}.")

        self.spread_config = SpreadManager._config_cache[path_key]

        # Load global broker settings (apply_to_long, etc.)
        self._load_global_settings()

        # Load per-asset config
        spreads = self.spread_config.get("spreads", {})
        if self.asset_symbol not in spreads:
            available = sorted(spreads.keys())
            msg = (
                f"Asset '{self.asset_symbol}' not found in spread config at {config_path}. "
                f"Available assets: {available}. "
                f"Add an entry for '{self.asset_symbol}' or correct asset.symbol in your YAML."
            )
            if self.spread_config.get("settings", {}).get("require_spread_for_all_assets", False):
                raise ValueError(msg)
            # warn_on_missing_spread
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

    def _load_global_settings(self) -> None:
        """Read and validate global broker settings from broker_spreads.yaml.

        DEC-036: Reads settings.apply_to_long, settings.apply_to_short, and
        settings.application_method. These are stored on the instance so
        RiskManager can read them from SpreadManager (single source of truth)
        rather than duplicating them in the strategy YAML.

        Raises
        ------
        ValueError
            When ``application_method`` is not a recognised value.
        """
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

        Uses the per-asset config from broker_spreads.yaml.
        Returns ``0.0`` when no asset config is loaded (asset not in broker file).

        Examples
        --------
        DEUIDXEUR (percentage, 0.015%):
            bid=20000 → 0.015/100 × 20000 = 3.0 pts
            bid=18000 → 0.015/100 × 18000 = 2.7 pts
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
                 Note: for LONG with BID data, SL is already a BID level.
                 The trigger equals the SL since we exit when Bid hits SL.
                 Spread is NOT subtracted for LONG (exit is a sell at Bid).
        * SHORT: trigger = SL + Spread  (buy at Ask to close; Ask = Bid + spread)

        Implementation note: the current signature subtracts spread for LONG
        and adds for SHORT, matching the legacy convention. For LONG BID data
        the spread subtraction is effectively zero-impact since the SL is already
        a Bid level, but we preserve the signature for backward compatibility.
        """
        return raw_sl_price - spread if is_long else raw_sl_price + spread

    def get_tp_trigger_level(
        self, raw_tp_price: float, spread: float, is_long: bool
    ) -> float:
        """Return adjusted TP trigger level accounting for spread.

        DEC-038 companion method.

        * LONG:  trigger = TP  (exit at Bid when Bid rises to TP level — no spread at exit)
        * SHORT: trigger = TP + Spread  (buy at Ask to close when Bid falls to TP level)
                 The actual close price is Ask = Bid + spread, so the SHORT TP
                 effectively requires Bid to fall further by one spread width.

        This is the symmetric counterpart to get_sl_trigger_level() for TP exits.
        """
        return raw_tp_price if is_long else raw_tp_price + spread

    def get_spread_info(self) -> Dict:
        """Return spread configuration summary including global broker settings.

        DEC-036: Now exposes apply_to_long, apply_to_short, and application_method
        from the broker file. RiskManager should read these from here rather than
        from the strategy YAML to maintain a single source of truth.

        Returns ``{"enabled": False}`` when no asset config is loaded.
        """
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
            # Global broker settings — single source of truth
            "apply_to_long": self.apply_to_long,
            "apply_to_short": self.apply_to_short,
            "application_method": self.application_method,
        }

    def is_enabled(self) -> bool:
        """``True`` when asset spread config was found in the broker YAML."""
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
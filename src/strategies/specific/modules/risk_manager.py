"""Risk management: SL/TP with R:R ratio, spread adjustments, annual range validation.

MIGRATED: Session 7 — Returns TradeParameters contract instead of dict.
HARDENED: Session 20 (Block F) — ATR + annual range caching; legacy adapter removed (DEC-021);
          mode parameter support (DEC-022); lazy initialization (DEC-030).
UPDATED:  Session 21:
          DEC-035 — Accepts StrategyConfig instead of raw Dict; fixes config key
                    path divergence (sl_tp → risk, risk_management → risk).
          DEC-037 — tp_mode selector ('rr_ratio' | 'atr_multiplier');
                    risk_to_reward_ratio restored as first-class config field.
          DEC-038 — take_profit_trigger computed for SHORT TP exit spread;
                    spread_at_tp_exit field populated in TradeParameters.
          SM-1    — asset_symbol blank guard before SpreadManager construction.
          DEC-036 — SpreadManager apply_to_long/apply_to_short read from broker
                    file via spread_info, not from strategy YAML.

BID price model (one spread per round trip)
-------------------------------------------
  LONG:  entry = Bid + spread (Ask); SL exit at Bid; TP exit at Bid
         → spread paid once at open; no additional cost at close
  SHORT: entry = Bid; SL exit at Ask = SL_bid + spread
         TP exit at Ask = TP_bid + spread  ← DEC-038 adds take_profit_trigger
         → spread paid once at close (SL or TP, whichever is hit first)

Location: src/strategies/specific/modules/risk_manager.py
"""
from __future__ import annotations

import hashlib
import logging
from typing import ClassVar, Dict, Optional, Tuple

import numpy as np
import pandas as pd

from src.strategies.contracts.trade_contracts import TradeParameters
from src.strategies.specific.modules.spread_manager import SpreadManager

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module-level ATR cache (survives across RiskManager instances in a
# multi-run backtester; keyed on a stable fingerprint of the data + length).
# Call RiskManager.clear_atr_cache() between parameter-sweep runs when the
# underlying OHLCV data changes.
# ---------------------------------------------------------------------------
_ATR_CACHE: Dict[str, pd.Series] = {}
_ANNUAL_RANGE_CACHE: Dict[str, pd.Series] = {}

# Valid tp_mode values — used for validation in __init__
_VALID_TP_MODES = frozenset({"rr_ratio", "atr_multiplier"})


def _dataframe_fingerprint(df: pd.DataFrame, extra: str = "") -> str:
    """Stable, cheap fingerprint: shape + first/last index + optional extra tag."""
    if df is None or df.empty:
        return f"empty_{extra}"
    first = str(df.index[0])
    last = str(df.index[-1])
    return hashlib.md5(f"{len(df)}_{first}_{last}_{extra}".encode()).hexdigest()[:16]


class RiskManager:
    """Manages SL/TP calculations with R:R ratio and risk validation.

    Session 20 changes
    ------------------
    * ATR series is now cached at module level — second instantiation with the
      same data + ATR length skips the O(N) Wilder's-smoothing computation.
    * Rolling annual range is cached the same way.
    * ``compute_trade_parameters_legacy()`` removed (DEC-021 / DEC-031).
    * ``mode`` parameter accepted on ``__init__`` (DEC-022 / DEC-030):
      - ``"core"``      → skips annual-range calc (not needed for speed runs).
      - ``"analytics"`` → full pipeline including annual range and spread logs.
    * Invalid mode ``"debug"`` raises ValueError with a migration message.

    Session 21 changes (DEC-035 / DEC-037 / DEC-038)
    -------------------------------------------------
    * Constructor accepts ``StrategyConfig`` instead of raw dict (DEC-035).
      Config key path fixed: was ``sl_tp.*`` / ``risk_management.*``;
      now reads from ``config.trade_management.risk.*``.
    * ``tp_mode`` field added to control TP calculation strategy (DEC-037):
      - ``"rr_ratio"`` (default): TP = entry ± ATR × sl_mult × rr_ratio
        Express target as a risk multiple (e.g. rr_ratio=5.7).
      - ``"atr_multiplier"``: TP = entry ± ATR × atr_multiplier_tp
        Express target as a direct ATR multiple (e.g. atr_multiplier_tp=7.98).
      Both modes are mathematically equivalent when atr_multiplier_tp = sl_mult × rr_ratio.
    * ``take_profit_trigger`` added to TradeParameters for SHORT TP exits (DEC-038).
      SHORT TP close is a buy at Ask = TP_bid + spread. LONG TP trigger = TP (no adjustment).
    * ``apply_to_long`` / ``apply_to_short`` now read from SpreadManager
      (broker file) instead of strategy YAML (DEC-036 single source of truth).
    * Blank ``asset_symbol`` guard added before SpreadManager construction (SM-1).

    Cache management
    ----------------
    Call ``RiskManager.clear_atr_cache()`` or ``RiskManager.clear_all_caches()``
    between backtester runs when OHLCV data changes.
    """

    _atr_cache: ClassVar[Dict[str, pd.Series]] = _ATR_CACHE
    _rar_cache: ClassVar[Dict[str, pd.Series]] = _ANNUAL_RANGE_CACHE

    # ------------------------------------------------------------------
    def __init__(
        self,
        config,                          # StrategyConfig (typed) — Dict path removed DEC-035
        ohlcv_data: pd.DataFrame,
        ohlcv_artf: Optional[pd.DataFrame] = None,
        mode: str = "core",
    ) -> None:
        """
        Parameters
        ----------
        config:
            ``StrategyConfig`` instance (DEC-035). The legacy ``Dict`` path
            has been removed — use ``StrategyConfig.from_yaml()`` to load.
        ohlcv_data:
            Strategy-timeframe OHLCV with DatetimeIndex.
        ohlcv_artf:
            Monthly ARTF bars (optional; required for annual-range validation).
        mode:
            ``"core"`` or ``"analytics"``. ``"debug"`` raises ValueError.
        """
        # ── Mode validation ───────────────────────────────────────────────────
        if mode == "debug":
            raise ValueError(
                "Mode 'debug' has been renamed to 'analytics' in the new architecture. "
                "Update your config: execution.mode: analytics"
            )
        if mode not in {"core", "analytics"}:
            raise ValueError(f"Invalid mode '{mode}'. Must be 'core' or 'analytics'.")

        self._mode = mode

        # ── DEC-035: Read from StrategyConfig (typed attributes, not dict) ────
        # Previously: config.get("trade_management", {}).get("sl_tp", {})
        # Previously: config.get("trade_management", {}).get("risk_management", {})
        # Now: direct typed access — KeyError is impossible, validation done at construction
        risk_cfg = config.trade_management.risk
        spread_cfg = config.trade_management.spread

        # SL parameters
        self.atr_length: int = risk_cfg.atr_length
        self.sl_multiplier: float = risk_cfg.atr_multiplier_sl

        # ── DEC-037: TP mode selector ─────────────────────────────────────────
        # tp_mode and risk_to_reward_ratio are new fields in RiskConfig (DEC-037).
        # Read them with getattr + default to handle pre-DEC-037 StrategyConfig
        # instances that don't yet have these fields.
        self.tp_mode: str = getattr(risk_cfg, "tp_mode", "rr_ratio")
        self.rr_ratio: float = getattr(risk_cfg, "risk_to_reward_ratio", 5.7)
        self.atr_multiplier_tp: float = risk_cfg.atr_multiplier_tp

        if self.tp_mode not in _VALID_TP_MODES:
            raise ValueError(
                f"risk.tp_mode='{self.tp_mode}' is invalid. "
                f"Valid values: {sorted(_VALID_TP_MODES)}. "
                f"Use 'rr_ratio' (default, legacy) or 'atr_multiplier'."
            )

        # Annual range / risk config (optional feature)
        self.max_risk_percentile: float = risk_cfg.max_risk_percentile
        # risk_management sub-config for allow_exceed_limit — keep as dict for now
        # (full migration in DEC-037 follow-up when RiskManagementConfig is added)
        self.risk_config: Dict = {}

        # ── Validate and store OHLCV data ─────────────────────────────────────
        self.ohlcv_data = ohlcv_data.copy()
        if not isinstance(self.ohlcv_data.index, pd.DatetimeIndex):
            if "timestamp" in self.ohlcv_data.columns:
                self.ohlcv_data.set_index("timestamp", inplace=True)
            else:
                raise ValueError("RiskManager requires OHLCV data with DatetimeIndex.")

        self.ohlcv_artf: Optional[pd.DataFrame] = ohlcv_artf

        # ── ATR — cached at module level ──────────────────────────────────────
        self.atr_series: Optional[pd.Series] = None
        self.atr_series = self._get_or_compute_atr(self.atr_length)

        # ── Rolling Annual Range — cached; skipped in core mode ───────────────
        self.annual_range_series: Optional[pd.Series] = None
        if mode == "analytics" and self.ohlcv_artf is not None:
            self.annual_range_series = self._get_or_compute_rar()

        # ── SpreadManager ─────────────────────────────────────────────────────
        self.spread_manager: Optional[SpreadManager] = None
        self._spread_cfg = spread_cfg   # stored for use in compute_trade_parameters

        if spread_cfg.enabled:
            # SM-1: Fail-fast on blank symbol — SpreadManager also guards,
            # but we raise here for a clearer error message at the right level.
            asset_symbol = getattr(
                getattr(config, "asset", None), "symbol", ""
            )
            if not asset_symbol or not asset_symbol.strip():
                raise ValueError(
                    "trade_management.spread.enabled is True but asset.symbol "
                    "is missing or blank. SpreadManager requires a symbol to look "
                    "up the broker spread. Add: asset:\n  symbol: 'DEUIDXEUR'"
                )
            config_path = getattr(spread_cfg, "config_path", None)
            self.spread_manager = SpreadManager(
                asset_symbol=asset_symbol,
                spread_config_path=str(config_path) if config_path else None,
                mode=mode,
            )
            if mode == "analytics":
                logger.info(
                    f"SpreadManager initialised for {asset_symbol}. "
                    f"Spread info: {self.spread_manager.get_spread_info()}"
                )

    # ------------------------------------------------------------------
    # Cache helpers (unchanged from Session 20)
    # ------------------------------------------------------------------

    def _get_or_compute_atr(self, length: int) -> pd.Series:
        key = _dataframe_fingerprint(self.ohlcv_data, extra=f"atr{length}")
        if key not in RiskManager._atr_cache:
            series = self._calculate_atr_wilders(length)
            RiskManager._atr_cache[key] = series
            if self._mode == "analytics":
                logger.info(
                    f"ATR computed and cached (Wilder RMA, length={length}, key={key[:8]}…)."
                )
        else:
            if self._mode == "analytics":
                logger.debug(f"ATR cache hit (key={key[:8]}…).")
        return RiskManager._atr_cache[key]

    def _get_or_compute_rar(self) -> Optional[pd.Series]:
        artf_key = _dataframe_fingerprint(self.ohlcv_artf, extra="rar")
        strat_key = _dataframe_fingerprint(self.ohlcv_data, extra="strat")
        key = hashlib.md5(f"{artf_key}_{strat_key}".encode()).hexdigest()[:16]
        if key not in RiskManager._rar_cache:
            series = self._calculate_rolling_annual_range_internal()
            if series is not None:
                RiskManager._rar_cache[key] = series
                if self._mode == "analytics":
                    logger.info(f"Annual range computed and cached (key={key[:8]}…).")
            return series
        else:
            if self._mode == "analytics":
                logger.debug(f"Annual range cache hit (key={key[:8]}…).")
            return RiskManager._rar_cache[key]

    @classmethod
    def clear_atr_cache(cls) -> None:
        cls._atr_cache.clear()

    @classmethod
    def clear_rar_cache(cls) -> None:
        cls._rar_cache.clear()

    @classmethod
    def clear_all_caches(cls) -> None:
        cls._atr_cache.clear()
        cls._rar_cache.clear()

    @classmethod
    def cache_stats(cls) -> Dict[str, int]:
        return {
            "atr_entries": len(cls._atr_cache),
            "rar_entries": len(cls._rar_cache),
        }

    # ------------------------------------------------------------------
    # Computation internals (unchanged from Session 20)
    # ------------------------------------------------------------------

    def _calculate_atr_wilders(self, length: int = 14) -> pd.Series:
        """ATR using Wilder's Smoothing (RMA) — matches TradingView."""
        high = self.ohlcv_data["high"]
        low = self.ohlcv_data["low"]
        close = self.ohlcv_data["close"]
        tr = pd.concat(
            [high - low, (high - close.shift()).abs(), (low - close.shift()).abs()],
            axis=1,
        ).max(axis=1)
        return tr.ewm(alpha=1 / length, adjust=False).mean().astype("float32")

    def _calculate_rolling_annual_range_internal(self) -> Optional[pd.Series]:
        """12-month RAR using monthly ARTF bars — no lookahead."""
        if self.ohlcv_artf is None or self.ohlcv_artf.empty:
            logger.warning("Monthly ARTF data missing — annual range disabled.")
            return None
        monthly = self.ohlcv_artf.copy()
        if not isinstance(monthly.index, pd.DatetimeIndex):
            raise ValueError("ARTF monthly data must have DatetimeIndex.")
        monthly = monthly.sort_index()
        monthly.index = monthly.index.normalize()
        monthly["ym"] = monthly.index.to_period("M")
        monthly_by_ym = monthly.set_index("ym")[["high", "low"]]
        yms = monthly_by_ym.index.unique().sort_values()
        rar_per_month: Dict = {}
        for ym in yms:
            prev_ym = ym - 1
            start_ym = prev_ym - 11
            window = monthly_by_ym.loc[start_ym:prev_ym]
            rar_per_month[ym] = (
                float(window["high"].max() - window["low"].min()) if len(window) else np.nan
            )
        rar_monthly_series = pd.Series(rar_per_month, dtype="float32")
        strategy_prev_ym = self.ohlcv_data.index.to_period("M") - 1
        rar_strategy = strategy_prev_ym.map(rar_monthly_series)
        return pd.Series(rar_strategy.values, index=self.ohlcv_data.index, dtype="float32")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compute_trade_parameters(
        self,
        timestamp: pd.Timestamp,
        bid_price: float,
        is_long: bool,
    ) -> Optional[TradeParameters]:
        """Compute all trade parameters including spread, SL/TP, and risk validation.

        BID price model
        ---------------
        ``bid_price`` is assumed to be the current BID price from OHLCV data.
        LONG entry executes at Ask = bid + spread.
        SHORT entry executes at Bid (no spread at open).
        SL and TP triggers account for spread at exit per DEC-038.

        TP mode (DEC-037)
        -----------------
        When ``tp_mode='rr_ratio'`` (default):
            tp_distance = ATR × sl_multiplier × rr_ratio
            e.g. ATR=5.0, sl_mult=1.4, rr=5.7 → tp_distance = 39.9 pts
        When ``tp_mode='atr_multiplier'``:
            tp_distance = ATR × atr_multiplier_tp
            e.g. ATR=5.0, atr_mult_tp=7.98 → tp_distance = 39.9 pts
        Both are equivalent when atr_multiplier_tp == sl_multiplier × rr_ratio.

        SHORT TP trigger (DEC-038)
        --------------------------
        SHORT TP close = buy at Ask = TP_bid + spread.
        ``take_profit_trigger`` for SHORT = tp_bid + spread.
        LONG TP close = sell at Bid = TP_bid (no spread at exit).
        ``take_profit_trigger`` for LONG = tp_bid.

        Returns ``None`` when:
        - ATR is unavailable or zero at ``timestamp``
        - Risk percentile check fails (when enabled and allow_exceed_limit=False)
        """
        # ---- ATR ----------------------------------------------------------------
        if self.atr_series is None:
            return None
        try:
            atr_val = float(self.atr_series.loc[timestamp])
        except KeyError:
            logger.warning(f"ATR not available for {timestamp}.")
            return None
        if atr_val <= 0 or np.isnan(atr_val):
            return None

        # ---- Spread -------------------------------------------------------------
        spread = 0.0
        spread_type: Optional[str] = None
        spread_value_config: Optional[float] = None

        if self.spread_manager:
            spread = self.spread_manager.get_spread_in_points(bid_price)
            if self.spread_manager.asset_config:
                spread_type = self.spread_manager.asset_config.get("spread_type")
                spread_value_config = self.spread_manager.asset_config.get("spread_value")

        # DEC-036: Read apply_to_long / apply_to_short from SpreadManager (broker file)
        # rather than strategy YAML — single source of truth.
        if self.spread_manager:
            apply_to_long = self.spread_manager.apply_to_long
            apply_to_short = self.spread_manager.apply_to_short
        else:
            apply_to_long = True
            apply_to_short = True

        apply_spread = False
        if self._spread_cfg.enabled:
            if (is_long and apply_to_long) or (not is_long and apply_to_short):
                apply_spread = True

        spread_for_this = spread if apply_spread else 0.0

        # LONG:  execute at Ask = Bid + spread
        # SHORT: execute at Bid (no spread at open; paid at close)
        executed_entry = bid_price + spread_for_this if is_long else bid_price

        # ---- SL -----------------------------------------------------------------
        risk_distance = atr_val * self.sl_multiplier
        raw_sl = (
            executed_entry - risk_distance if is_long else executed_entry + risk_distance
        )

        is_valid, adjusted_sl, comment = self.validate_risk_percentile(
            executed_entry, raw_sl, is_long, timestamp
        )
        if not is_valid:
            return None

        sl_adjusted = adjusted_sl != raw_sl
        sl_price_raw = raw_sl if not sl_adjusted else None
        sl_distance_raw = risk_distance if not sl_adjusted else None
        final_sl = adjusted_sl
        risk_distance = abs(executed_entry - final_sl)

        # SL trigger: accounts for spread at SL close
        # LONG SL: exit at Bid → no spread deducted (Bid falls to SL level)
        # SHORT SL: exit at Ask = SL_bid + spread → trigger is higher by one spread
        trigger_sl = (
            final_sl if is_long
            else final_sl + spread_for_this
        )

        # ---- TP (DEC-037: tp_mode branch) ----------------------------------------
        if self.tp_mode == "rr_ratio":
            # Legacy default: TP = entry ± risk_distance × rr_ratio
            # risk_distance is already ATR × sl_multiplier (after any SL adjustment)
            tp_distance = risk_distance * self.rr_ratio
        else:
            # "atr_multiplier": TP = entry ± ATR × atr_multiplier_tp
            # Direct ATR multiple — independent of SL multiplier
            tp_distance = atr_val * self.atr_multiplier_tp

        tp = (
            executed_entry + tp_distance if is_long
            else executed_entry - tp_distance
        )

        # ---- TP trigger (DEC-038: SHORT TP exit spread) -------------------------
        # LONG:  TP close = sell at Bid → no spread at TP exit
        #        take_profit_trigger = tp (Bid level; exit when Bid ≥ tp)
        # SHORT: TP close = buy at Ask = tp_bid + spread
        #        take_profit_trigger = tp + spread_for_this
        #        (Bid must fall to tp before Ask is affordable at the target rate)
        take_profit_trigger = (
            tp if is_long
            else tp + spread_for_this
        )

        # Spread cost at TP exit (for analytics — DEC-038)
        # LONG:  0 (no spread at TP exit)
        # SHORT: spread_for_this (paid when buying to close)
        spread_at_tp_exit: Optional[float] = (
            None if is_long else (spread_for_this if apply_spread else 0.0)
        )

        # ---- Annual range -------------------------------------------------------
        annual_range_value: Optional[float] = None
        risk_percentile_calculated: Optional[float] = None
        max_risk_pct: Optional[float] = None
        risk_percentile_passed = True

        if self.annual_range_series is not None:
            try:
                rar = float(self.annual_range_series.loc[timestamp])
                if not np.isnan(rar) and rar > 0:
                    annual_range_value = rar
                    risk_percentile_calculated = risk_distance / rar
                    max_risk_pct = self.max_risk_percentile
                    risk_percentile_passed = risk_percentile_calculated <= max_risk_pct
            except (KeyError, ValueError):
                pass

        # ---- Spread efficiency --------------------------------------------------
        spread_efficiency_percent: Optional[float] = None
        if apply_spread and spread > 0:
            spread_efficiency_percent = (spread / executed_entry) * 100

        return TradeParameters(
            entry_price_mid=bid_price,
            entry_price_executed=executed_entry,
            stop_loss_raw=final_sl,
            stop_loss_trigger=trigger_sl,
            take_profit=tp,
            take_profit_trigger=take_profit_trigger,   # DEC-038
            position_size=1.0,
            atr_value=atr_val,
            atr_length=self.atr_length,
            atr_multiplier=self.sl_multiplier,
            sl_distance=risk_distance,
            tp_distance=tp_distance,                   # DEC-037
            tp_mode=self.tp_mode,                      # DEC-037
            risk_reward_ratio=self.rr_ratio,
            annual_range_value=annual_range_value,
            risk_percentile_calculated=risk_percentile_calculated,
            max_risk_percentile=max_risk_pct,
            risk_percentile_passed=risk_percentile_passed,
            spread_enabled=self._spread_cfg.enabled,
            spread_applied=apply_spread,
            spread_type=spread_type,
            spread_value=spread_value_config,
            spread_points=spread_for_this,
            spread_cost=spread if apply_spread else None,
            spread_efficiency_percent=spread_efficiency_percent,
            spread_at_tp_exit=spread_at_tp_exit,       # DEC-038
            sl_adjusted=sl_adjusted,
            sl_distance_raw=sl_distance_raw,
            sl_price_raw=sl_price_raw,
            comment=comment,
        )

    def validate_risk_percentile(
        self,
        entry_price: float,
        stop_loss: float,
        is_long: bool,
        timestamp: pd.Timestamp,
    ) -> Tuple[bool, float, str]:
        """Validate SL against max risk percentile of annual range.

        Returns (is_valid, adjusted_sl, comment).
        Unchanged from Session 20 — no migration needed here.
        """
        if self.annual_range_series is None:
            return True, stop_loss, "RAR not initialised"

        try:
            current_annual_range = self.annual_range_series.loc[timestamp]
        except KeyError:
            return True, stop_loss, "RAR missing for timestamp"

        if pd.isna(current_annual_range) or current_annual_range <= 0:
            return True, stop_loss, f"RAR unavailable ({current_annual_range})"

        max_percentile = self.max_risk_percentile
        if max_percentile >= 1.0:
            return True, stop_loss, "No risk limit"

        risk_distance = abs(entry_price - stop_loss)
        risk_percentile = risk_distance / current_annual_range

        if risk_percentile <= max_percentile:
            return True, stop_loss, f"Risk: {risk_percentile * 100:.2f}%"

        allow_exceed = self.risk_config.get("allow_exceed_limit", False)
        if not allow_exceed:
            return (
                False,
                stop_loss,
                f"Risk rejected: {risk_percentile * 100:.2f}% > {max_percentile * 100:.2f}%",
            )

        adjusted_distance = max_percentile * current_annual_range
        adjusted_sl = (
            entry_price - adjusted_distance if is_long else entry_price + adjusted_distance
        )
        return (
            True,
            adjusted_sl,
            f"SL adjusted: {risk_percentile * 100:.2f}% → {max_percentile * 100:.2f}%",
        )
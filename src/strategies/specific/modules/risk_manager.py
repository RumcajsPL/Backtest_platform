"""Risk management: SL/TP with R:R ratio, spread adjustments, annual range validation.

MIGRATED: Session 7 — Returns TradeParameters contract instead of dict.
HARDENED: Session 20 (Block F) — ATR + annual range caching; legacy adapter removed (DEC-021);
          mode parameter support (DEC-022); lazy initialization (DEC-030).

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

    Cache management
    ----------------
    Call ``RiskManager.clear_atr_cache()`` or ``RiskManager.clear_all_caches()``
    between backtester runs when OHLCV data changes.
    """

    # ------------------------------------------------------------------
    # Class-level cache references (thin wrappers around module globals)
    # ------------------------------------------------------------------
    _atr_cache: ClassVar[Dict[str, pd.Series]] = _ATR_CACHE
    _rar_cache: ClassVar[Dict[str, pd.Series]] = _ANNUAL_RANGE_CACHE

    # ------------------------------------------------------------------
    def __init__(
        self,
        config: Dict,
        ohlcv_data: pd.DataFrame,
        ohlcv_artf: Optional[pd.DataFrame] = None,
        mode: str = "core",
    ) -> None:
        """
        Parameters
        ----------
        config:
            Full strategy config dict (same shape as StrategyConfig.to_dict()).
        ohlcv_data:
            Strategy-timeframe OHLCV with DatetimeIndex.
        ohlcv_artf:
            Monthly ARTF bars (optional; required for annual-range validation).
        mode:
            ``"core"`` or ``"analytics"``. ``"debug"`` raises ValueError.
        """
        if mode == "debug":
            raise ValueError(
                "Mode 'debug' has been renamed to 'analytics' in the new architecture. "
                "Update your config: execution.mode: analytics"
            )
        if mode not in {"core", "analytics"}:
            raise ValueError(f"Invalid mode '{mode}'. Must be 'core' or 'analytics'.")

        self._mode = mode
        self.config = config

        tm_config = config.get("trade_management", {})
        self.sl_tp_config = tm_config.get("sl_tp", {})
        self.risk_config = tm_config.get("risk_management", {})
        self.spread_config = tm_config.get("spread", {})

        # ------------------------------------------------------------------
        # Validate and store OHLCV data
        # ------------------------------------------------------------------
        self.ohlcv_data = ohlcv_data.copy()
        if not isinstance(self.ohlcv_data.index, pd.DatetimeIndex):
            if "timestamp" in self.ohlcv_data.columns:
                self.ohlcv_data.set_index("timestamp", inplace=True)
            else:
                raise ValueError("RiskManager requires OHLCV data with DatetimeIndex.")

        # Monthly ARTF data (prefer explicit arg, fallback to config injection)
        self.ohlcv_artf: Optional[pd.DataFrame] = ohlcv_artf or config.get("data", {}).get("df_artf")

        # ------------------------------------------------------------------
        # ATR — cached at module level
        # ------------------------------------------------------------------
        self.atr_series: Optional[pd.Series] = None
        if self.sl_tp_config.get("enabled", True):
            atr_length = self.sl_tp_config.get("atr_length", 14)
            self.atr_series = self._get_or_compute_atr(atr_length)

        # ------------------------------------------------------------------
        # Rolling Annual Range — cached; skipped in core mode
        # ------------------------------------------------------------------
        self.annual_range_series: Optional[pd.Series] = None
        rar_enabled = self.risk_config.get("enabled", False)
        if rar_enabled and mode == "analytics":
            self.annual_range_series = self._get_or_compute_rar()
        elif rar_enabled and mode == "core":
            # Annual range validation is analytics-only — skip for speed
            if self._mode == "analytics":
                logger.debug("Annual range skipped in core mode.")

        # ------------------------------------------------------------------
        # SpreadManager — only instantiate when spread is enabled
        # ------------------------------------------------------------------
        self.spread_manager: Optional[SpreadManager] = None
        if self.spread_config.get("enabled", False):
            asset_symbol = config.get("asset", {}).get("symbol", "")
            config_path = self.spread_config.get("config_path")
            self.spread_manager = SpreadManager(asset_symbol, config_path)
            if mode == "analytics":
                logger.info(f"SpreadManager initialised for {asset_symbol}.")

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def _get_or_compute_atr(self, length: int) -> pd.Series:
        """Return cached ATR series or compute and cache it."""
        key = _dataframe_fingerprint(self.ohlcv_data, extra=f"atr{length}")
        if key not in RiskManager._atr_cache:
            series = self._calculate_atr_wilders(length)
            RiskManager._atr_cache[key] = series
            if self._mode == "analytics":
                logger.info(f"ATR computed and cached (Wilder RMA, length={length}, key={key[:8]}…).")
        else:
            if self._mode == "analytics":
                logger.debug(f"ATR cache hit (key={key[:8]}…).")
        return RiskManager._atr_cache[key]

    def _get_or_compute_rar(self) -> Optional[pd.Series]:
        """Return cached annual-range series or compute and cache it."""
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
        """Clear the ATR cache. Call between backtester runs when data changes."""
        cls._atr_cache.clear()

    @classmethod
    def clear_rar_cache(cls) -> None:
        """Clear the annual-range cache."""
        cls._rar_cache.clear()

    @classmethod
    def clear_all_caches(cls) -> None:
        """Clear all module-level caches. Use between full backtester sweeps."""
        cls._atr_cache.clear()
        cls._rar_cache.clear()

    @classmethod
    def cache_stats(cls) -> Dict[str, int]:
        """Return current cache sizes (for observability / tests)."""
        return {
            "atr_entries": len(cls._atr_cache),
            "rar_entries": len(cls._rar_cache),
        }

    # ------------------------------------------------------------------
    # Computation internals
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
        rar_per_month: Dict[pd.Period, float] = {}

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

        Returns ``None`` when:
        - SL/TP is disabled in config
        - ATR is unavailable or zero at ``timestamp``
        - Risk percentile check fails (when enabled and ``allow_exceed_limit=False``)
        """
        if not self.sl_tp_config.get("enabled", True):
            return None

        # ---- ATR ----------------------------------------------------------------
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

        apply_spread = False
        if self.spread_config.get("enabled", False):
            if (is_long and self.spread_config.get("apply_to_long", True)) or (
                not is_long and self.spread_config.get("apply_to_short", True)
            ):
                apply_spread = True

        spread_for_this = spread if apply_spread else 0.0
        executed_entry = bid_price + spread_for_this if is_long else bid_price

        # ---- SL / TP ------------------------------------------------------------
        sl_mult = self.sl_tp_config.get("sl_multiplier", 1.4)
        rr_ratio = self.sl_tp_config.get("risk_to_reward_ratio", 2.0)

        risk_distance = atr_val * sl_mult
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

        tp = (
            executed_entry + risk_distance * rr_ratio
            if is_long
            else executed_entry - risk_distance * rr_ratio
        )
        trigger_sl = (
            final_sl - spread_for_this if is_long else final_sl + spread_for_this
        )

        # ---- Annual range -------------------------------------------------------
        annual_range_value: Optional[float] = None
        risk_percentile_calculated: Optional[float] = None
        max_risk_percentile: Optional[float] = None
        risk_percentile_passed = True

        if (
            self.risk_config.get("enabled", False)
            and self.annual_range_series is not None
        ):
            try:
                rar = float(self.annual_range_series.loc[timestamp])
                if not np.isnan(rar) and rar > 0:
                    annual_range_value = rar
                    risk_percentile_calculated = risk_distance / rar
                    max_risk_percentile = self.risk_config.get("max_risk_percentile", 1.0)
                    risk_percentile_passed = risk_percentile_calculated <= max_risk_percentile
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
            position_size=1.0,
            atr_value=atr_val,
            atr_length=self.sl_tp_config.get("atr_length", 14),
            atr_multiplier=sl_mult,
            sl_distance=risk_distance,
            tp_distance=abs(tp - executed_entry),
            risk_reward_ratio=rr_ratio,
            annual_range_value=annual_range_value,
            risk_percentile_calculated=risk_percentile_calculated,
            max_risk_percentile=max_risk_percentile,
            risk_percentile_passed=risk_percentile_passed,
            spread_enabled=self.spread_config.get("enabled", False),
            spread_applied=apply_spread,
            spread_type=spread_type,
            spread_value=spread_value_config,
            spread_points=spread_for_this,
            spread_cost=spread if apply_spread else None,
            spread_efficiency_percent=spread_efficiency_percent,
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

        Returns
        -------
        (is_valid, adjusted_sl, comment)
        """
        if not self.risk_config.get("enabled", False):
            return True, stop_loss, "Risk mgmt disabled"

        if self.annual_range_series is None:
            return True, stop_loss, "RAR not initialised"

        try:
            current_annual_range = self.annual_range_series.loc[timestamp]
        except KeyError:
            return True, stop_loss, "RAR missing for timestamp"

        if pd.isna(current_annual_range) or current_annual_range <= 0:
            return True, stop_loss, f"RAR unavailable ({current_annual_range})"

        max_percentile = self.risk_config.get("max_risk_percentile", 1.0)

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
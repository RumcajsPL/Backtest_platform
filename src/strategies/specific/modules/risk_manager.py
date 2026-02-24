"""Risk management: SL/TP with R:R ratio, spread adjustments, annual range validation.

Version: 2.3.0
Block 5 (R5 / L1) — Production Hardening

Changes from v2.2.0:
- [Risk Filter Intuition] max_risk_percentile now interpreted as PERCENTAGE value
- [Risk Filter Intuition] 1.5 in config means 1.5% of annual range
- [Risk Filter Intuition] 100.0 means 100% (effectively disables filter)
- [Risk Filter Intuition] Clean threshold at 100.0 instead of 1.0
- [Risk Filter Intuition] Clear logging with % symbols matching config values

BID price model (one spread per round trip)
-------------------------------------------
  LONG:  entry = Bid + spread (Ask); SL exit at Bid; TP exit at Bid
         → spread paid once at open; no additional cost at close
  SHORT: entry = Bid; SL exit at Ask = SL_bid + spread
         TP exit at Ask = TP_bid + spread
         → spread paid once at close (SL or TP, whichever is hit first)
"""
from __future__ import annotations

import hashlib
import logging
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd

from src.strategies.contracts.trade_contracts import TradeParameters
from src.strategies.specific.modules.spread_manager import SpreadManager
from src.strategies.core.cache_manager import CacheManager

if TYPE_CHECKING:
    from src.config.config_schema import StrategyConfig

logger = logging.getLogger(__name__)


# Valid tp_mode values
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

    Cache management
    ----------------
    All caches are managed by the central CacheManager for multi-run backtesting.
    Call cache_manager.clear_all_caches() between runs when OHLCV data changes.
    
    Risk Percentile Interpretation
    ------------------------------
    max_risk_percentile is interpreted as a PERCENTAGE value.
    Examples:
        - 0.5  → 0.5% of annual range (conservative)
        - 1.5  → 1.5% of annual range (moderate)
        - 3.0  → 3.0% of annual range (aggressive)
        - 100.0 → 100% of annual range (effectively disables filter)
        - 500.0 → 500% of annual range (effectively disables filter)
    
    The filter is ACTIVE when max_risk_percentile < 100.0
    The filter is DISABLED when max_risk_percentile >= 100.0
    """

    def __init__(
        self,
        config: "StrategyConfig",       # TYPE_CHECKING guard avoids circular import
        ohlcv_data: pd.DataFrame,
        ohlcv_artf: Optional[pd.DataFrame] = None,
        mode: str = "core",
        cache_manager: Optional[CacheManager] = None,
    ) -> None:
        """
        Parameters
        ----------
        config: StrategyConfig instance
        ohlcv_data: Strategy-timeframe OHLCV with DatetimeIndex
        ohlcv_artf: Monthly ARTF bars (optional)
        mode: "core" or "analytics". "debug" raises ValueError.
        cache_manager: Central cache manager for multi-run state
        """
        # ── Mode validation (no "debug") ─────────────────────────────────
        if mode not in {"core", "analytics"}:
            raise ValueError(
                f"Invalid mode '{mode}'. Must be 'core' or 'analytics'. "
                f"'debug' is not a valid mode and has been removed."
            )
        self._mode = mode
        self._cache_manager = cache_manager

        # ── Read from StrategyConfig ─────────────────────────────────────
        risk_cfg = config.trade_management.risk
        spread_cfg = config.trade_management.spread

        self.atr_length: int = risk_cfg.atr_length
        self.sl_multiplier: float = risk_cfg.atr_multiplier_sl

        # TP mode (DEC-037)
        self.tp_mode: str = risk_cfg.tp_mode
        self.rr_ratio: float = risk_cfg.risk_to_reward_ratio
        self.atr_multiplier_tp: float = risk_cfg.atr_multiplier_tp

        if self.tp_mode not in _VALID_TP_MODES:
            raise ValueError(
                f"risk.tp_mode='{self.tp_mode}' is invalid. "
                f"Valid values: {sorted(_VALID_TP_MODES)}."
            )

        # Annual range config - interpreted as PERCENTAGE
        # Examples: 0.5 = 0.5%, 1.5 = 1.5%, 100.0 = 100% (disabled)
        self.max_risk_percentile: float = risk_cfg.max_risk_percentile
        self.risk_filter_active = self.max_risk_percentile < 100.0
        self.risk_config: Dict[str, Any] = {}

        # ── Validate and store OHLCV data ───────────────────────────────
        self.ohlcv_data = ohlcv_data.copy()
        if not isinstance(self.ohlcv_data.index, pd.DatetimeIndex):
            if "timestamp" in self.ohlcv_data.columns:
                self.ohlcv_data.set_index("timestamp", inplace=True)
            else:
                raise ValueError("RiskManager requires OHLCV data with DatetimeIndex.")

        self.ohlcv_artf: Optional[pd.DataFrame] = ohlcv_artf

        # ── ATR — cached via CacheManager ───────────────────────────────
        self.atr_series: Optional[pd.Series] = None
        self.atr_series = self._get_or_compute_atr(self.atr_length)

        # ── Rolling Annual Range — computed when filter active ──────────
        # StrategyConfig has already validated that ARTF data exists when
        # max_risk_percentile < 100.0. We can safely compute here.
        self.annual_range_series: Optional[pd.Series] = None
        
        if self.risk_filter_active:
            # Risk filter is active - annual range REQUIRED
            if self.ohlcv_artf is None or self.ohlcv_artf.empty:
                # This should never happen due to config validation
                raise RuntimeError(
                    f"Risk filter active (max_risk_percentile={self.max_risk_percentile}% < 100%) "
                    f"but ARTF data is missing. This indicates a configuration validation "
                    f"failure - the config should have been rejected at load time."
                )
            self.annual_range_series = self._get_or_compute_rar()
            logger.info(
                f"Risk filter ACTIVE: max {self.max_risk_percentile:.3f}% "
                f"of annual range (ARTF data loaded)"
            )
        else:
            logger.info(
                f"Risk filter DISABLED: max_risk_percentile={self.max_risk_percentile}% "
                f"(values >= 100% disable filtering)"
            )

        # ── SpreadManager ───────────────────────────────────────────────
        self.spread_manager: Optional[SpreadManager] = None
        self._spread_cfg = spread_cfg

        if spread_cfg.enabled:
            # SM-1: Fail-fast on blank symbol
            asset_symbol = getattr(config.asset, "symbol", "")
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
                cache_manager=cache_manager,
            )
            if mode == "analytics":
                logger.info(
                    f"SpreadManager initialised for {asset_symbol}. "
                    f"Spread info: {self.spread_manager.get_spread_info()}"
                )

    # ------------------------------------------------------------------
    # Cache helpers (via CacheManager)
    # ------------------------------------------------------------------

    def _get_or_compute_atr(self, length: int) -> pd.Series:
        """Get ATR from cache or compute and store."""
        key = _dataframe_fingerprint(self.ohlcv_data, extra=f"atr{length}")

        if self._cache_manager:
            cached = self._cache_manager.get_atr(key)
            if cached is not None:
                if self._mode == "analytics":
                    logger.debug(f"ATR cache hit (key={key[:8]}…).")
                return cached

        series = self._calculate_atr_wilders(length)
        if self._cache_manager:
            self._cache_manager.set_atr(key, series)
            if self._mode == "analytics":
                logger.info(
                    f"ATR computed and cached (Wilder RMA, length={length}, key={key[:8]}…)."
                )
        return series

    def _get_or_compute_rar(self) -> Optional[pd.Series]:
        """Get annual range from cache or compute and store."""
        if self.ohlcv_artf is None:
            return None

        artf_key = _dataframe_fingerprint(self.ohlcv_artf, extra="rar")
        strat_key = _dataframe_fingerprint(self.ohlcv_data, extra="strat")
        key = hashlib.md5(f"{artf_key}_{strat_key}".encode()).hexdigest()[:16]

        if self._cache_manager:
            cached = self._cache_manager.get_annual_range(key)
            if cached is not None:
                if self._mode == "analytics":
                    logger.debug(f"Annual range cache hit (key={key[:8]}…).")
                return cached

        series = self._calculate_rolling_annual_range_internal()
        if series is not None and self._cache_manager:
            self._cache_manager.set_annual_range(key, series)
            if self._mode == "analytics":
                logger.info(f"Annual range computed and cached (key={key[:8]}…).")
        return series

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

        Returns None when:
        - ATR is unavailable or zero at timestamp
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
            spread_info = self.spread_manager.get_spread_info()
            spread_type = spread_info.get("spread_type")
            spread_value_config = spread_info.get("spread_value")

        # DEC-036: Read apply_to_long/apply_to_short from SpreadManager
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

        # LONG: execute at Ask = Bid + spread
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
        trigger_sl = (
            final_sl if is_long
            else final_sl + spread_for_this
        )

        # ---- TP (DEC-037: tp_mode branch) ----------------------------------------
        if self.tp_mode == "rr_ratio":
            tp_distance = risk_distance * self.rr_ratio
        else:  # "atr_multiplier"
            tp_distance = atr_val * self.atr_multiplier_tp

        tp = (
            executed_entry + tp_distance if is_long
            else executed_entry - tp_distance
        )

        # ---- TP trigger (DEC-038: SHORT TP exit spread) -------------------------
        take_profit_trigger = (
            tp if is_long
            else tp + spread_for_this
        )

        # Spread cost at TP exit (for analytics)
        spread_at_tp_exit: Optional[float] = (
            None if is_long else (spread_for_this if apply_spread else 0.0)
        )

        # ---- Annual range -------------------------------------------------------
        annual_range_value: Optional[float] = None
        risk_percentile_calculated: Optional[float] = None
        max_risk_pct: Optional[float] = None
        risk_percentile_passed = True

        # Safe access to annual_range_series - may be None if risk filter disabled
        if self.annual_range_series is not None:
            try:
                rar = float(self.annual_range_series.loc[timestamp])
                if not np.isnan(rar) and rar > 0:
                    annual_range_value = rar
                    risk_percentile_calculated = (risk_distance / rar) * 100.0  # ← AS PERCENTAGE
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
            take_profit_trigger=take_profit_trigger,
            position_size=1.0,
            atr_value=atr_val,
            atr_length=self.atr_length,
            atr_multiplier=self.sl_multiplier,
            sl_distance=risk_distance,
            tp_distance=tp_distance,
            tp_mode=self.tp_mode,
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
            spread_at_tp_exit=spread_at_tp_exit,
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
        
        Note: max_risk_percentile is interpreted as a PERCENTAGE value.
        Examples:
            - 0.5 means 0.5% of annual range
            - 1.5 means 1.5% of annual range
            - 100.0 means 100% (effectively disables filter)
        
        Returns:
            Tuple[is_valid, adjusted_stop_loss, comment]
        """
        # If risk filter is disabled (≥ 100%), always approve
        if not self.risk_filter_active:
            return True, stop_loss, "Risk filter disabled"
        
        # If we get here, risk filter is enabled (max_risk_percentile < 100.0)
        # annual_range_series MUST be available (guaranteed by config validation)
        if self.annual_range_series is None:
            # This should never happen - indicates config validation failure
            raise RuntimeError(
                f"Risk filter enabled (max_risk_percentile={self.max_risk_percentile}% < 100%) "
                f"but annual_range_series is None. This is a system error - "
                f"configuration validation should have prevented this state."
            )

        try:
            current_annual_range = self.annual_range_series.loc[timestamp]
        except KeyError:
            # If RAR missing for specific timestamp, log warning but don't block trade
            logger.warning(f"Annual range missing for timestamp {timestamp}")
            return True, stop_loss, f"RAR missing for timestamp"

        if pd.isna(current_annual_range) or current_annual_range <= 0:
            logger.warning(f"Annual range invalid at {timestamp}: {current_annual_range}")
            return True, stop_loss, f"RAR invalid"

        risk_distance = abs(entry_price - stop_loss)
        # Calculate risk as PERCENTAGE of annual range
        risk_percentile = (risk_distance / current_annual_range) * 100.0

        if risk_percentile <= self.max_risk_percentile:
            return True, stop_loss, f"Risk: {risk_percentile:.3f}%"

        allow_exceed = self.risk_config.get("allow_exceed_limit", False)
        if not allow_exceed:
            return (
                False,
                stop_loss,
                f"Risk rejected: {risk_percentile:.3f}% > {self.max_risk_percentile:.3f}%",
            )

        # Adjust SL to meet limit
        # Convert max_risk_percentile from percentage back to decimal for distance calculation
        adjusted_distance = (self.max_risk_percentile / 100.0) * current_annual_range
        adjusted_sl = (
            entry_price - adjusted_distance if is_long else entry_price + adjusted_distance
        )
        return (
            True,
            adjusted_sl,
            f"SL adjusted: {risk_percentile:.3f}% → {self.max_risk_percentile:.3f}%",
        )
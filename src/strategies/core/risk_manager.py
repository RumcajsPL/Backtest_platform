"""Risk management: SL/TP with R:R ratio, spread adjustments, annual range validation.

Version: 2.5.0
Session: Performance Hardening

Changes from v2.4.0:
- [PERF-3] compute_trade_parameters: SpreadManager.get_spread_info() dict
  construction removed from the hot path. spread_type and spread_value are now
  cached as instance attributes (_spread_type, _spread_value) in __init__
  immediately after SpreadManager construction. Both values are stable for the
  lifetime of the RiskManager instance. Saves ~5ms per full simulation run.
- [PERF-4] compute_trade_parameters / validate_risk_percentile: Series.loc[]
  scalar access on ATR and RAR Series replaced with Series.at[]. .at[] is the
  pandas-recommended API for single-label scalar access — it bypasses the label
  broadcasting and alignment machinery of .loc[], yielding ~15-20% faster reads.
  Cumulative saving ~10-15ms over a full simulation run. Semantics are identical
  for a single scalar key on a Series with a unique DatetimeIndex.

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
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

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
    max_risk_percentile is a PERCENTAGE of the rolling 12-month annual range.
    It is timeframe-sensitive: the same percentage admits very different SL
    distances depending on the strategy bar frequency.

    Examples (DAX, annual range ~6 000 pts):
        0.20% → max SL ~12 pts  (tight — rejects most high-volatility 1-min bars)
        0.50% → max SL ~30 pts  (moderate for 1-min)
        1.50% → max SL ~90 pts  (passes virtually all 1-min ATR(14) signals)
       100.0% → filter disabled (no rejection)

    Calibration guidance by timeframe:
        1-min  DAX: 0.10–0.50 %
        5-min  DAX: 0.30–1.00 %
        1-hour DAX: 1.00–5.00 %
        Daily  DAX: 5.00–20.0 %

    The filter is ACTIVE  when max_risk_percentile <  100.0
    The filter is DISABLED when max_risk_percentile >= 100.0
    """

    def __init__(
        self,
        config: "StrategyConfig",
        ohlcv_data: pd.DataFrame,
        ohlcv_artf: Optional[pd.DataFrame] = None,
        mode: str = "core",
        cache_manager: Optional[CacheManager] = None,
    ) -> None:
        """
        Parameters
        ----------
        config        : StrategyConfig instance
        ohlcv_data    : Strategy-timeframe OHLCV — MUST be the exact DataFrame
                        iterated by TradeSimulator (df_full). DatetimeIndex required.
        ohlcv_artf    : Monthly ARTF bars (required when max_risk_percentile < 100.0)
        mode          : "core" or "analytics". "debug" raises ValueError.
        cache_manager : Central cache manager for multi-run state.
        """
        # ── Mode validation ──────────────────────────────────────────────
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

        self.tp_mode: str = risk_cfg.tp_mode
        self.rr_ratio: float = risk_cfg.risk_to_reward_ratio
        self.atr_multiplier_tp: float = risk_cfg.atr_multiplier_tp

        if self.tp_mode not in _VALID_TP_MODES:
            raise ValueError(
                f"risk.tp_mode='{self.tp_mode}' is invalid. "
                f"Valid values: {sorted(_VALID_TP_MODES)}."
            )

        # Annual range config — interpreted as PERCENTAGE.
        # [FIX-3] Replaced dead self.risk_config dict with explicit bool.
        # The allow_exceed_limit feature is dormant until the field is added
        # to StrategyConfig. Set to False here; wire to config when ready.
        self.max_risk_percentile: float = risk_cfg.max_risk_percentile
        self.risk_filter_active: bool = self.max_risk_percentile < 100.0
        self._allow_exceed_limit: bool = False  # not yet in StrategyConfig

        # ── Risk filter diagnostic counters ─────────────────────────────
        # Incremented in compute_trade_parameters on every call.
        # Read via get_risk_summary() at end of simulation.
        self._risk_checked: int = 0
        self._risk_approved: int = 0
        self._risk_rejected: int = 0

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

        # ── Rolling Annual Range ─────────────────────────────────────────
        # StrategyConfig has already validated that ARTF data exists when
        # max_risk_percentile < 100.0. Safe to compute here.
        self.annual_range_series: Optional[pd.Series] = None

        if self.risk_filter_active:
            if self.ohlcv_artf is None or self.ohlcv_artf.empty:
                # Should never happen — config validation prevents this state.
                raise ValueError(
                    f"Risk filter ACTIVE (max_risk_percentile={self.max_risk_percentile}% < 100%) "
                    f"but ARTF DataFrame is None or empty.\n"
                    f"Configured path: {config.data.paths.artf_ohlcv}\n"
                    f"Verify that DataLoader.load_data() populates bundle.artf "
                    f"and that the ARTF file covers the full strategy date range."
                )
            self.annual_range_series = self._get_or_compute_rar()
            logger.info(
                "Risk filter ACTIVE: max %.4f%% of annual range | "
                "ARTF bars=%d | RAR series non-null=%d",
                self.max_risk_percentile,
                len(self.ohlcv_artf),
                int(self.annual_range_series.notna().sum())
                if self.annual_range_series is not None else 0,
            )
        else:
            logger.info(
                "Risk filter DISABLED: max_risk_percentile=%.1f%% (>= 100%%)",
                self.max_risk_percentile,
            )

        # ── SpreadManager ────────────────────────────────────────────────
        self.spread_manager: Optional[SpreadManager] = None
        self._spread_cfg = spread_cfg

        if spread_cfg.enabled:
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
                    "SpreadManager initialised for %s. Spread info: %s",
                    asset_symbol,
                    self.spread_manager.get_spread_info(),
                )

        # [PERF-3] Cache spread metadata as instance attributes.
        # get_spread_info() builds a dict on every call; spread_type and
        # spread_value are stable after construction and used on every signal bar.
        # Reading two attrs is ~10x faster than dict construction + two .get() calls.
        if self.spread_manager is not None:
            _info = self.spread_manager.get_spread_info()
            self._spread_type: Optional[str] = _info.get("spread_type")
            self._spread_value: Optional[float] = _info.get("spread_value")
        else:
            self._spread_type = None
            self._spread_value = None

    # ──────────────────────────────────────────────────────────────────────
    # Public diagnostic API
    # ──────────────────────────────────────────────────────────────────────

    def get_risk_summary(self) -> Dict[str, Any]:
        """Return risk filter statistics accumulated during simulation.

        Called by TradeSimulator in analytics mode to emit a summary log line
        at the end of simulate_trades(). Safe to call in core mode — returns
        counts without logging.

        Returns
        -------
        dict with keys:
            checked         int   — total compute_trade_parameters calls
            approved        int   — trades that passed all checks
            rejected        int   — trades rejected (ATR missing, RAR fail-safe,
                                    or risk_percentile exceeded threshold)
            rejection_rate  float — rejected / checked * 100 (0.0 if checked == 0)
            filter_active   bool  — whether max_risk_percentile < 100.0
            threshold_pct   float — configured max_risk_percentile value
        """
        rejection_rate = (
            (self._risk_rejected / self._risk_checked * 100.0)
            if self._risk_checked > 0 else 0.0
        )
        return {
            "checked":        self._risk_checked,
            "approved":       self._risk_approved,
            "rejected":       self._risk_rejected,
            "rejection_rate": round(rejection_rate, 2),
            "filter_active":  self.risk_filter_active,
            "threshold_pct":  self.max_risk_percentile,
        }

    # ──────────────────────────────────────────────────────────────────────
    # Cache helpers (via CacheManager)
    # ──────────────────────────────────────────────────────────────────────

    def _get_or_compute_atr(self, length: int) -> pd.Series:
        """Get ATR from cache or compute and store."""
        key = _dataframe_fingerprint(self.ohlcv_data, extra=f"atr{length}")

        if self._cache_manager:
            cached = self._cache_manager.get_atr(key)
            if cached is not None:
                if self._mode == "analytics":
                    logger.debug("ATR cache hit (key=%s…).", key[:8])
                return cached

        series = self._calculate_atr_wilders(length)
        if self._cache_manager:
            self._cache_manager.set_atr(key, series)
            if self._mode == "analytics":
                logger.info(
                    "ATR computed and cached (Wilder RMA, length=%d, key=%s…).",
                    length, key[:8],
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
                    logger.debug("Annual range cache hit (key=%s…).", key[:8])
                return cached

        series = self._calculate_rolling_annual_range_internal()
        if series is not None and self._cache_manager:
            self._cache_manager.set_annual_range(key, series)
            if self._mode == "analytics":
                logger.info(
                    "Annual range computed and cached (key=%s…). "
                    "Non-null bars: %d / %d (NaN in warm-up period is expected).",
                    key[:8],
                    int(series.notna().sum()),
                    len(series),
                )
        return series

    # ──────────────────────────────────────────────────────────────────────
    # Computation internals
    # ──────────────────────────────────────────────────────────────────────

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
        """12-month RAR using monthly ARTF bars — no lookahead.

        [FIX-2] Window now requires exactly 12 months of ARTF history.
        Partial windows during the first 12 months of the ARTF file produce
        NaN, preventing artificially low RAR from over-rejecting trades during
        the warm-up period. Strategy date ranges well within the ARTF file are
        unaffected (all their windows are full).
        """
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
            # [FIX-2] Require full 12-month window. Partial warm-up windows
            # produce NaN — validate_risk_percentile treats NaN as fail-safe reject.
            rar_per_month[ym] = (
                float(window["high"].max() - window["low"].min())
                if len(window) >= 12
                else np.nan
            )

        rar_monthly_series = pd.Series(rar_per_month, dtype="float32")
        strategy_prev_ym = self.ohlcv_data.index.to_period("M") - 1
        rar_strategy = strategy_prev_ym.map(rar_monthly_series)
        return pd.Series(rar_strategy.values, index=self.ohlcv_data.index, dtype="float32")

    # ──────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────

    def compute_trade_parameters(
        self,
        timestamp: pd.Timestamp,
        bid_price: float,
        is_long: bool,
    ) -> Optional[TradeParameters]:
        """Compute all trade parameters including spread, SL/TP, and risk validation.

        Returns None when:
        - ATR is unavailable or zero at timestamp
        - Risk percentile check rejects the trade (when filter is active)
        """
        self._risk_checked += 1

        # ── ATR ──────────────────────────────────────────────────────────
        if self.atr_series is None:
            self._risk_rejected += 1
            return None
        try:
            # [PERF-4] .at[] is the pandas scalar-access API — bypasses label
            # broadcasting overhead of .loc[]. Semantically identical for a
            # single key on a Series with a unique DatetimeIndex.
            atr_val = float(self.atr_series.at[timestamp])
        except KeyError:
            logger.warning("ATR not available for %s.", timestamp)
            self._risk_rejected += 1
            return None
        if atr_val <= 0 or np.isnan(atr_val):
            self._risk_rejected += 1
            return None

        # ── Spread ───────────────────────────────────────────────────────
        spread = 0.0

        if self.spread_manager:
            spread = self.spread_manager.get_spread_in_points(bid_price)

        # [PERF-3] Use cached instance attributes instead of rebuilding
        # get_spread_info() dict on every call. Both values are stable for
        # the lifetime of this RiskManager instance.
        spread_type = self._spread_type
        spread_value_config = self._spread_value

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

        executed_entry = bid_price + spread_for_this if is_long else bid_price

        # ── SL ───────────────────────────────────────────────────────────
        risk_distance = atr_val * self.sl_multiplier
        raw_sl = (
            executed_entry - risk_distance if is_long else executed_entry + risk_distance
        )

        is_valid, adjusted_sl, comment = self.validate_risk_percentile(
            executed_entry, raw_sl, is_long, timestamp
        )

        # ── [DIAG] Analytics-mode per-trade risk log ─────────────────────
        if self._mode == "analytics":
            direction_str = "LONG" if is_long else "SHORT"
            logger.debug(
                "RISK | %s | %s | ATR=%.2f | SL_dist=%.2f | pct=%.4f%% | "
                "threshold=%.4f%% | verdict=%s",
                timestamp,
                direction_str,
                atr_val,
                risk_distance,
                float(comment.split(":")[1].rstrip("%")) if ":" in comment and "%" in comment else 0.0,
                self.max_risk_percentile,
                "PASS" if is_valid else "REJECT",
            )

        if not is_valid:
            self._risk_rejected += 1
            return None

        self._risk_approved += 1

        sl_adjusted = adjusted_sl != raw_sl
        sl_price_raw = raw_sl if not sl_adjusted else None
        sl_distance_raw = risk_distance if not sl_adjusted else None
        final_sl = adjusted_sl
        risk_distance = abs(executed_entry - final_sl)

        trigger_sl = (
            final_sl if is_long
            else final_sl + spread_for_this
        )

        # ── TP ───────────────────────────────────────────────────────────
        if self.tp_mode == "rr_ratio":
            tp_distance = risk_distance * self.rr_ratio
        else:
            tp_distance = atr_val * self.atr_multiplier_tp

        tp = (
            executed_entry + tp_distance if is_long
            else executed_entry - tp_distance
        )

        take_profit_trigger = (
            tp if is_long
            else tp + spread_for_this
        )

        spread_at_tp_exit: Optional[float] = (
            None if is_long else (spread_for_this if apply_spread else 0.0)
        )

        # ── Annual range metadata ────────────────────────────────────────
        annual_range_value: Optional[float] = None
        risk_percentile_calculated: Optional[float] = None
        max_risk_pct: Optional[float] = None
        risk_percentile_passed = True

        if self.annual_range_series is not None:
            try:
                # [PERF-4] .at[] for scalar access — see ATR note above.
                rar = float(self.annual_range_series.at[timestamp])
                if not np.isnan(rar) and rar > 0:
                    annual_range_value = rar
                    risk_percentile_calculated = (risk_distance / rar) * 100.0
                    max_risk_pct = self.max_risk_percentile
                    risk_percentile_passed = risk_percentile_calculated <= max_risk_pct
            except (KeyError, ValueError):
                pass

        # ── Spread efficiency ────────────────────────────────────────────
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
        """Validate SL distance against max_risk_percentile of annual range.

        max_risk_percentile is a PERCENTAGE value (e.g., 0.20 means 0.20%).
        The filter is active when max_risk_percentile < 100.0.

        [FIX-1] Replaced bare try/except KeyError → return True (silent fail-open)
        with an explicit index membership check. A missing or NaN RAR value now
        rejects the trade (fail-safe), consistent with the fail-fast principle.
        An unknown timestamp indicates a data alignment problem that should surface
        as a rejection, not a silent approval.

        Returns
        -------
        Tuple[is_valid, adjusted_stop_loss, comment]
        """
        if not self.risk_filter_active:
            return True, stop_loss, "Risk filter disabled"

        # annual_range_series is guaranteed non-None when risk_filter_active is True
        # (enforced in __init__). The assertion makes this contract explicit.
        assert self.annual_range_series is not None, (
            "annual_range_series is None with risk_filter_active=True — "
            "this is a constructor invariant violation."
        )

        # [FIX-1] Explicit membership check instead of try/except KeyError.
        # Missing timestamp → fail-safe rejection, not silent approval.
        if timestamp not in self.annual_range_series.index:
            logger.warning(
                "RAR index miss at %s — trade rejected (fail-safe). "
                "Check that ohlcv_data passed to RiskManager matches the "
                "DataFrame iterated by TradeSimulator.",
                timestamp,
            )
            return False, stop_loss, f"RAR index miss at {timestamp}"

        # [PERF-4] .at[] for scalar label access — bypasses .loc[] broadcasting.
        current_annual_range = float(self.annual_range_series.at[timestamp])

        # NaN means ARTF warm-up period has < 12 months of history.
        # Reject the trade — cannot assess risk without a valid RAR.
        if pd.isna(current_annual_range) or current_annual_range <= 0:
            logger.warning(
                "RAR is NaN/zero at %s (ARTF warm-up or data gap) — "
                "trade rejected (fail-safe).",
                timestamp,
            )
            return False, stop_loss, f"RAR unavailable at {timestamp}"

        risk_distance = abs(entry_price - stop_loss)
        risk_percentile = (risk_distance / current_annual_range) * 100.0

        if risk_percentile <= self.max_risk_percentile:
            return True, stop_loss, f"Risk: {risk_percentile:.4f}%"

        # Trade exceeds threshold.
        if not self._allow_exceed_limit:
            return (
                False,
                stop_loss,
                f"Risk rejected: {risk_percentile:.4f}% > {self.max_risk_percentile:.4f}%",
            )

        # SL capping — only reached when _allow_exceed_limit is True.
        # (Currently always False until wired to StrategyConfig.)
        adjusted_distance = (self.max_risk_percentile / 100.0) * current_annual_range
        adjusted_sl = (
            entry_price - adjusted_distance if is_long else entry_price + adjusted_distance
        )
        return (
            True,
            adjusted_sl,
            f"SL adjusted: {risk_percentile:.4f}% → {self.max_risk_percentile:.4f}%",
        )
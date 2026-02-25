"""
Trade simulation with LTF OHLC execution

Version: 5.4.0
Session: LTF Coverage Guard

Changes from v5.3.0:
- [GUARD-1] _precompute_ltf_windows: LTF coverage validation added after the
  window build (both uniform fast path and non-uniform fallback).
  Zero windows built → ValueError with actionable diagnostics (LTF file date
  range vs strategy date range). This enforces the fail-fast principle for the
  case where the LTF file and strategy date_range do not overlap at all —
  previously every exit check would silently return early and every trade would
  close at end-of-data price with no indication of the problem.
  Partial coverage (n_windows < n_strategy) → WARNING logged with coverage
  percentage, missing bar count, and both date boundaries. Not aborted —
  accepted risk for backtester windows that extend slightly beyond the LTF
  file, consistent with architecture decision to allow end-of-data closes.

Changes from v5.2.0 (carried from v5.3.0):
- [PERF-1] _precompute_ltf_windows: vectorised index conversion and searchsorted.
  Two previously-hot items inside the 68,400-iteration loop have been eliminated:
    (a) np.datetime64(strategy_time) conversion — moved outside the loop as a
        single df_strategy.index.to_numpy() call (68,400 → 1 conversion).
    (b) Per-bar searchsorted — replaced with two vectorised searchsorted calls
        over the full strategy index array (136,800 → 2 calls).
  Additionally, a uniform-data fast path detects when the LTF file has exactly
  one tick per second per strategy bar (the normal case for clean backtest data).
  In that case numpy reshape + min/max(axis=1) computes all min_low/max_high
  values in two array operations instead of one per-bar Python call.
  The non-uniform fallback is preserved for data where bar widths vary.
  Expected saving: ~517ms on a 68,400-bar strategy run.
- [PERF-2] _check_exits_with_ltf_ohlc: removed per-bar list comprehension and
  np.array() allocation for the common case of few open trades. A named constant
  _ARRAY_THRESHOLD = 4 controls the crossover point below which direct attribute
  access is used instead of array construction. The np.array() path is fully
  preserved for ≥ _ARRAY_THRESHOLD trades — vectorisation only pays off there.
  Expected saving: ~190ms on a 68,400-bar strategy run.
"""

import time
import logging
from collections import defaultdict
from typing import Callable, Dict, List, Optional, Any

import numpy as np
import pandas as pd

from src.strategies.specific.modules.risk_manager import RiskManager
from src.strategies.specific.modules.spread_manager import SpreadManager
from src.strategies.specific.modules.trade_manager import TradeManager
from src.strategies.core.null_progressive_tracker import NullProgressiveTracker
from src.strategies.core.cache_manager import CacheManager

from src.strategies.contracts.trade_contracts import (
    Trade,
    TradeEntry,
    TradeExit,
    TradeDecision,
    DecisionType,
    TradeDirection,
    ExitReason,
    TradeParameters,
    RejectedSignal,
    TradeResult,
)
from src.strategies.contracts.signal_contracts import SignalFrame
from src.config.config_schema import StrategyConfig

logger = logging.getLogger(__name__)

try:
    from numba import njit
    NUMBA_AVAILABLE = True
except ImportError:
    NUMBA_AVAILABLE = False

_SIGNAL_CODE_TO_STR = {1: "BUY", 2: "SELL"}

# [PERF-2] Crossover point for direct-access vs np.array() in exit checks.
# For n_open_trades < _ARRAY_THRESHOLD, direct attribute access is faster than
# np.array() construction + vectorised comparison. Break-even measured at ~4.
_ARRAY_THRESHOLD: int = 4


class TradeSimulatorProfiler:
    """Simple profiler for performance monitoring in analytics mode"""

    def __init__(self) -> None:
        self.timings: Dict[str, list] = defaultdict(list)

    def profile(self, name: str) -> Callable:
        """Return a decorator that records execution time under ``name``."""
        def decorator(func: Callable) -> Callable:
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                start = time.perf_counter()
                result = func(*args, **kwargs)
                elapsed = time.perf_counter() - start
                self.timings[name].append(elapsed)
                return result
            return wrapper
        return decorator

    def print_report(self) -> None:
        logger.info("=" * 60)
        logger.info("TRADE SIMULATOR PROFILING REPORT")
        logger.info("=" * 60)
        for name, times in self.timings.items():
            total = sum(times)
            avg = total / len(times) if times else 0
            logger.info(
                f"{name:30s}: {total:.3f}s total, {avg:.3f}s avg, {len(times)} calls"
            )


if NUMBA_AVAILABLE:
    @njit
    def _numba_find_first_hit_long(low_np, high_np, sl_price, tp_price, is_sl):
        n = low_np.shape[0]
        for i in range(n):
            if is_sl:
                if low_np[i] <= sl_price:
                    return i
            else:
                if high_np[i] >= tp_price:
                    return i
        return -1

    @njit
    def _numba_find_first_hit_short(low_np, high_np, sl_price, tp_price, is_sl):
        n = low_np.shape[0]
        for i in range(n):
            if is_sl:
                if high_np[i] >= sl_price:
                    return i
            else:
                if low_np[i] <= tp_price:
                    return i
        return -1


class TradeSimulator:
    """
    Trade simulator with LTF OHLC execution for realistic SL/TP triggers.
    """

    def __init__(
        self,
        config: StrategyConfig,
        df_full: pd.DataFrame,
        df_artf: Optional[pd.DataFrame] = None,
        cache_manager: Optional[CacheManager] = None,
    ):
        """
        Initialize TradeSimulator with typed config.

        Args:
            config: StrategyConfig instance
            df_full: Full OHLCV DataFrame
            df_artf: Monthly ARTF DataFrame (required when risk filter enabled)
            cache_manager: Optional cache manager for multi-run state
        """
        self.config = config
        self.df_full = df_full
        self.df_artf = df_artf
        self._cache_manager = cache_manager or CacheManager()

        # [L3] Access analytics.profile_simulator via clean optional attribute
        # chain rather than getattr(config, 'analytics', {}).get(...) which
        # treated an optional typed object as a plain dict.
        _analytics = getattr(config, "analytics", None)
        self.profile_enabled: bool = bool(
            getattr(_analytics, "profile_simulator", False)
        )

        # ── Canonical trade store ──────────────────────────────────────── #
        self.all_trades: List[Trade] = []
        self.rejected_signals: List[RejectedSignal] = []
        self.trade_counter = 0
        self.rejection_counter = 0

        # ── O(1) lookup indices ──────────────────────────────────────── #
        self._open_trades: Dict[str, Trade] = {}
        self._tm_id_to_entry_id: Dict[int, str] = {}
        self._trade_list_index: Dict[str, int] = {}

        # ── Managers ─────────────────────────────────────────────────── #
        self.trade_manager: Optional[TradeManager] = None
        self.spread_manager: Optional[SpreadManager] = None
        self.progressive_tracker = None
        self._tracking_enabled = False
        self.df_ltf: Optional[pd.DataFrame] = None
        self._ltf_windows: Dict = {}

        # Phase 5.7/5.8: All managers now use StrategyConfig
        self.risk_manager = RiskManager(
            config=self.config,
            ohlcv_data=df_full,
            ohlcv_artf=df_artf,
            mode="core",
            cache_manager=self._cache_manager,
        )

        self.initialize_managers()

        if self.profile_enabled:
            self.profiler = TradeSimulatorProfiler()
            self._check_exits_with_ltf_ohlc = self.profiler.profile("check_exits_ltf")(
                self._check_exits_with_ltf_ohlc
            )
        else:
            self.profiler = None

    # ──────────────────────────────────────────────────────────────────── #
    # Initialization
    # ──────────────────────────────────────────────────────────────────── #

    def initialize_managers(self) -> None:
        """Initialize trade, spread, and risk managers."""
        # Phase 5.8: TradeManager now accepts StrategyConfig
        self.trade_manager = TradeManager(self.config)

        spread_config = self.config.trade_management.spread
        if spread_config.enabled:
            asset_symbol = self.config.asset.symbol
            self.spread_manager = SpreadManager(
                asset_symbol=asset_symbol,
                spread_config_path=str(spread_config.config_path) if spread_config.config_path else None,
                mode="core",
                cache_manager=self._cache_manager,
            )

    # ──────────────────────────────────────────────────────────────────── #
    # LTF window precomputation
    # ──────────────────────────────────────────────────────────────────── #

    def _precompute_ltf_windows(self, df_strategy: pd.DataFrame) -> None:
        """Pre-compute LTF windows and numpy views for each strategy bar.

        [PERF-1] Two vectorised improvements over v5.2.0:

        1. Index conversion: df_strategy.index.to_numpy() is called ONCE to
           produce strat_np and end_np. The previous implementation called
           np.datetime64(strategy_time) inside the loop — 68,400 conversions
           replaced by 1 vectorised call.

        2. searchsorted: both start_idx and end_idx arrays are computed with a
           single vectorised searchsorted call each on the full ltf index. The
           previous implementation called searchsorted twice per loop iteration —
           136,800 calls replaced by 2 vectorised calls.

        3. Uniform fast path: if the LTF file has exactly `ticks_per_bar` rows
           per strategy bar (the normal case for clean second-level data), numpy
           reshape + min/max(axis=1) computes all min_low/max_high values in two
           array operations. A size-mismatch guard falls back to the per-bar loop
           for non-uniform data so the method is safe for any input.

        [GUARD-1] LTF coverage validation added after window build:
        - Zero windows built → hard abort (LTF file does not overlap the
          strategy date range at all — wrong file or wrong date_range).
        - Partial coverage → WARNING logged with exact missing-bar count and
          the LTF file's actual date boundaries so the operator can act.
          Partial coverage is not aborted because it is an accepted risk for
          backtester runs where the last bars of a window may lack LTF data,
          but it must always be visible rather than silent.
        """
        if self.df_ltf is None or self.df_ltf.empty:
            raise ValueError(
                "LTF execution data missing. "
                "Verify config paths.ltf_ohlcv_file points to valid 1-second OHLCV data."
            )

        self._ltf_windows = {}
        ltf_index_np = self.df_ltf.index.to_numpy()   # datetime64[ns]
        low_np  = self.df_ltf["low"].to_numpy(np.float32)
        high_np = self.df_ltf["high"].to_numpy(np.float32)

        n_strategy = len(df_strategy)
        one_minute = np.timedelta64(1, "m")

        # [PERF-1a] Convert entire strategy index to numpy ONCE.
        # Previously: np.datetime64(strategy_time) inside the loop per bar.
        strat_np: np.ndarray = df_strategy.index.to_numpy()   # shape (n_strategy,)
        end_np:   np.ndarray = strat_np + one_minute           # shape (n_strategy,)

        # [PERF-1b] Vectorised searchsorted — 2 calls for all bars instead of
        # 2 × n_strategy calls inside the loop.
        start_idx_arr: np.ndarray = ltf_index_np.searchsorted(strat_np, side="left")
        end_idx_arr:   np.ndarray = ltf_index_np.searchsorted(end_np,   side="left")

        # [PERF-1c] Uniform fast path.
        # Detect whether every strategy bar maps to the same number of LTF ticks.
        # This is true for clean second-level data (60 ticks per 1-min bar).
        # When it holds, a single reshape + numpy min/max eliminates all per-bar
        # min/max Python calls.
        widths = end_idx_arr - start_idx_arr          # ticks per strategy bar
        ticks_per_bar = int(widths[0]) if n_strategy > 0 else 0
        uniform = (
            ticks_per_bar > 0
            and bool(np.all(widths == ticks_per_bar))
            and int(start_idx_arr[0]) + n_strategy * ticks_per_bar <= len(low_np)
        )

        if uniform:
            offset = int(start_idx_arr[0])
            total  = n_strategy * ticks_per_bar
            low_2d  = low_np[offset : offset + total].reshape(n_strategy, ticks_per_bar)
            high_2d = high_np[offset : offset + total].reshape(n_strategy, ticks_per_bar)
            min_low_arr:  np.ndarray = low_2d.min(axis=1)
            max_high_arr: np.ndarray = high_2d.max(axis=1)

            strategy_times = df_strategy.index
            for i in range(n_strategy):
                s = offset + i * ticks_per_bar
                e = s + ticks_per_bar
                self._ltf_windows[strategy_times[i]] = {
                    "min_low":  float(min_low_arr[i]),
                    "max_high": float(max_high_arr[i]),
                    "low_np":   low_np[s:e],
                    "high_np":  high_np[s:e],
                    "index_np": ltf_index_np[s:e],
                }
        else:
            # Non-uniform fallback — same logic as v5.2.0 but using the
            # pre-computed vectorised start/end index arrays.
            strategy_times = df_strategy.index
            for i in range(n_strategy):
                s = int(start_idx_arr[i])
                e = int(end_idx_arr[i])
                if e <= s:
                    continue
                sl = low_np[s:e]
                sh = high_np[s:e]
                si = ltf_index_np[s:e]
                if sl.size == 0:
                    continue
                self._ltf_windows[strategy_times[i]] = {
                    "min_low":  float(sl.min()),
                    "max_high": float(sh.max()),
                    "low_np":   sl,
                    "high_np":  sh,
                    "index_np": si,
                }

        # [GUARD-1] LTF coverage validation — enforces fail-fast on zero
        # coverage and makes partial coverage explicitly visible.
        #
        # Why this guard is placed here and not earlier:
        #   Both the uniform fast path and the non-uniform fallback must
        #   complete before we know the actual window count. The searchsorted
        #   results alone cannot predict coverage — a strategy bar may fall
        #   entirely outside the LTF index even if start_idx == end_idx == 0.
        #
        # Zero windows → hard abort.
        #   The LTF file and the strategy date range do not overlap at all.
        #   Every exit check would silently return early; every trade would
        #   close at end-of-data price. This is never acceptable — abort
        #   immediately with actionable diagnostics.
        #
        # Partial coverage → WARNING, not abort.
        #   Accepted risk for backtester runs where the LTF file covers most
        #   but not all of the strategy window (e.g. file ends mid-month while
        #   strategy window extends to month-end). Positions in uncovered bars
        #   close at end-of-data price via _close_remaining_positions.
        #   The warning makes this visible rather than silent.
        n_windows   = len(self._ltf_windows)
        n_missing   = n_strategy - n_windows
        ltf_first   = str(self.df_ltf.index[0])  if len(self.df_ltf) > 0 else "?"
        ltf_last    = str(self.df_ltf.index[-1]) if len(self.df_ltf) > 0 else "?"
        strat_first = str(df_strategy.index[0])  if n_strategy > 0 else "?"
        strat_last  = str(df_strategy.index[-1]) if n_strategy > 0 else "?"

        if n_windows == 0:
            raise ValueError(
                f"LTF coverage error: zero windows built for strategy period "
                f"[{strat_first} → {strat_last}]. "
                f"LTF file covers [{ltf_first} → {ltf_last}]. "
                f"The LTF file does not overlap the strategy date range. "
                f"Extend the LTF file or correct the date_range configuration."
            )

        if n_missing > 0:
            coverage_pct = n_windows / n_strategy * 100.0
            logger.warning(
                "LTF partial coverage: %d of %d strategy bars have LTF windows "
                "(%.1f%% coverage, %d bars missing). "
                "Strategy period: [%s → %s]. "
                "LTF file covers: [%s → %s]. "
                "Trades in uncovered bars will close at end-of-data price.",
                n_windows, n_strategy, coverage_pct, n_missing,
                strat_first, strat_last,
                ltf_first, ltf_last,
            )

    # ──────────────────────────────────────────────────────────────────── #
    # Numba-accelerated exact exit detection
    # ──────────────────────────────────────────────────────────────────── #

    def _find_exact_exit_bar_numba(
        self,
        trade: Trade,
        low_np: np.ndarray,
        high_np: np.ndarray,
        index_np: np.ndarray,
        exit_reason: str,
        is_long: bool,
    ) -> tuple:
        """Numba-accelerated first-hit search."""
        if low_np.size == 0:
            return None, None, None, None

        sl_price = trade.entry.stop_loss
        tp_price = trade.entry.take_profit

        if NUMBA_AVAILABLE:
            is_sl = exit_reason == "STOP_LOSS"
            if is_long:
                idx = _numba_find_first_hit_long(low_np, high_np, sl_price, tp_price, is_sl)
            else:
                idx = _numba_find_first_hit_short(low_np, high_np, sl_price, tp_price, is_sl)
            if idx < 0:
                return None, None, None, None
        else:
            is_sl = exit_reason == "STOP_LOSS"
            if is_long:
                hit_mask = low_np <= sl_price if is_sl else high_np >= tp_price
            else:
                hit_mask = high_np >= sl_price if is_sl else low_np <= tp_price
            if not hit_mask.any():
                return None, None, None, None
            idx = int(np.argmax(hit_mask))

        ts        = pd.Timestamp(index_np[idx])
        low_val   = float(low_np[idx])
        high_val  = float(high_np[idx])

        if is_long:
            exit_price = min(low_val,  sl_price) if exit_reason == "STOP_LOSS" else min(high_val, tp_price)
        else:
            exit_price = max(high_val, sl_price) if exit_reason == "STOP_LOSS" else max(low_val,  tp_price)

        return ts, exit_price, high_val, low_val

    # ──────────────────────────────────────────────────────────────────── #
    # Exit execution
    # ──────────────────────────────────────────────────────────────────── #

    def _execute_trade_exit(
        self,
        trade: Trade,
        exit_time: pd.Timestamp,
        exit_price: float,
        exit_reason: str,
        exit_stats: Dict,
        verbose: bool,
        exit_high: float,
        exit_low: float,
    ) -> None:
        """
        Execute trade exit and update tracking.

        TS-2: Fail-fast on unknown exit reason - no silent default.
        """
        # TS-2: Strict exit reason validation
        try:
            exit_reason_enum = ExitReason[exit_reason]
        except KeyError:
            raise ValueError(
                f"Unknown exit reason '{exit_reason}'. "
                f"Valid values: {[e.name for e in ExitReason]}. "
                f"This is a code defect — exit_reason must always be a valid ExitReason name."
            ) from None

        trade_exit = TradeExit.create(
            entry=trade.entry,
            exit_time=exit_time,
            exit_price=exit_price,
            exit_reason=exit_reason_enum,
        )
        updated_trade = Trade(entry=trade.entry, exit=trade_exit)

        # O(1) bookkeeping
        entry_id = trade.entry.entry_id
        list_idx = self._trade_list_index[entry_id]
        self.all_trades[list_idx] = updated_trade
        del self._open_trades[entry_id]
        if trade.entry.trade_manager_id is not None:
            self._tm_id_to_entry_id.pop(trade.entry.trade_manager_id, None)

        # Close in TradeManager
        if trade.entry.trade_manager_id:
            self.trade_manager.close_positions([trade.entry.trade_manager_id])

        exit_stats[exit_reason] = exit_stats.get(exit_reason, 0) + 1

        # Progressive tracking
        if self._tracking_enabled and trade.entry.signal_id:
            self.progressive_tracker.update_trade_execution_details(
                signal_id=trade.entry.signal_id,
                trade_id=int(trade.entry.entry_id.replace("E", "")),
                exit_time=exit_time,
                exit_price=exit_price,
                exit_reason=exit_reason,
                pnl_points=trade_exit.pnl_points,
                pnl_percent=trade_exit.pnl_percent,
                duration_minutes=trade_exit.duration_minutes,
                is_win=trade_exit.is_win,
                is_loss=trade_exit.is_loss,
                exit_check_high=exit_high,
                exit_check_low=exit_low,
                reason=f"CLOSED: {exit_reason}",
            )

        if verbose:
            theoretical = trade.entry.stop_loss if exit_reason == "STOP_LOSS" else trade.entry.take_profit
            diff = exit_price - theoretical
            sign = "+" if diff >= 0 else ""
            logger.debug(
                f"[EXIT-LTF] {exit_time} {trade.direction} {exit_reason} | "
                f"Actual: {exit_price:.5f} ({sign}{diff:.5f}) | P&L: {trade_exit.pnl_points:+.2f} pts"
            )

    # ──────────────────────────────────────────────────────────────────── #
    # LTF exit engine
    # ──────────────────────────────────────────────────────────────────── #

    def _check_exits_with_ltf_ohlc(
        self,
        strategy_timestamp: pd.Timestamp,
        exit_stats: Dict,
        verbose: bool,
    ) -> None:
        """Check for SL/TP exits using LTF OHLC data.

        [PERF-2] v5.2.0 allocated Python lists and np.array() on every call
        (all 68,400 strategy bars) regardless of the number of open trades.
        For the typical case of 0–3 simultaneous open trades, direct attribute
        access is faster than the allocation + vectorised comparison path.

        The crossover is controlled by _ARRAY_THRESHOLD (= 4). Below the
        threshold, each trade is evaluated directly via attribute access and
        a scalar comparison against the pre-computed window min/max. At or
        above the threshold, the original np.array() vectorised path is used.

        The window guard and entry_time filter are unchanged in both paths.
        """
        if not self._open_trades:
            return

        window = self._ltf_windows.get(strategy_timestamp)
        if window is None:
            return

        low_np   = window["low_np"]
        high_np  = window["high_np"]
        index_np = window["index_np"]
        if low_np.size == 0:
            return

        min_low  = window["min_low"]
        max_high = window["max_high"]

        # Separate open trades into long/short, filtering on entry_time.
        # The filter is applied here in both paths — no separate list comp.
        long_trades:  List[Trade] = []
        short_trades: List[Trade] = []
        for t in self._open_trades.values():
            if t.entry.entry_time >= strategy_timestamp:
                continue
            if t.entry.is_long:
                long_trades.append(t)
            else:
                short_trades.append(t)

        n_long  = len(long_trades)
        n_short = len(short_trades)

        # ── Long trades ──────────────────────────────────────────────────
        if n_long > 0:
            if n_long < _ARRAY_THRESHOLD:
                # [PERF-2] Direct attribute access path for small trade counts.
                # Avoids np.array() construction; uses scalar comparisons on the
                # pre-computed window min/max (no per-second scan needed here —
                # the full scan happens inside _find_exact_exit_bar_numba only
                # when a hit is confirmed by the cheap min/max guard).
                for trade in long_trades:
                    sl_hit = min_low  <= trade.entry.stop_loss
                    tp_hit = max_high >= trade.entry.take_profit
                    if not (sl_hit or tp_hit):
                        continue
                    reason = "STOP_LOSS" if sl_hit else "TAKE_PROFIT"
                    ts, price, h, l = self._find_exact_exit_bar_numba(
                        trade, low_np, high_np, index_np, reason, True
                    )
                    if ts is not None:
                        self._execute_trade_exit(
                            trade, ts, price, reason, exit_stats, verbose, h, l
                        )
            else:
                # Original np.array() vectorised path — kept for ≥ _ARRAY_THRESHOLD.
                sl_arr = np.array([t.entry.stop_loss  for t in long_trades], dtype=np.float32)
                tp_arr = np.array([t.entry.take_profit for t in long_trades], dtype=np.float32)
                sl_hit   = min_low  <= sl_arr
                tp_hit   = max_high >= tp_arr
                exit_mask = sl_hit | tp_hit
                if exit_mask.any():
                    reasons = np.where(sl_hit, "STOP_LOSS", np.where(tp_hit, "TAKE_PROFIT", None))
                    for idx in np.where(exit_mask)[0]:
                        trade  = long_trades[idx]
                        reason = reasons[idx]
                        if reason is None:
                            continue
                        ts, price, h, l = self._find_exact_exit_bar_numba(
                            trade, low_np, high_np, index_np, reason, True
                        )
                        if ts is not None:
                            self._execute_trade_exit(
                                trade, ts, price, reason, exit_stats, verbose, h, l
                            )

        # ── Short trades ─────────────────────────────────────────────────
        if n_short > 0:
            if n_short < _ARRAY_THRESHOLD:
                # [PERF-2] Direct attribute access path for small trade counts.
                for trade in short_trades:
                    sl_hit = max_high >= trade.entry.stop_loss
                    tp_hit = min_low  <= trade.entry.take_profit
                    if not (sl_hit or tp_hit):
                        continue
                    reason = "STOP_LOSS" if sl_hit else "TAKE_PROFIT"
                    ts, price, h, l = self._find_exact_exit_bar_numba(
                        trade, low_np, high_np, index_np, reason, False
                    )
                    if ts is not None:
                        self._execute_trade_exit(
                            trade, ts, price, reason, exit_stats, verbose, h, l
                        )
            else:
                # Original np.array() vectorised path — kept for ≥ _ARRAY_THRESHOLD.
                sl_arr = np.array([t.entry.stop_loss  for t in short_trades], dtype=np.float32)
                tp_arr = np.array([t.entry.take_profit for t in short_trades], dtype=np.float32)
                sl_hit   = max_high >= sl_arr
                tp_hit   = min_low  <= tp_arr
                exit_mask = sl_hit | tp_hit
                if exit_mask.any():
                    reasons = np.where(sl_hit, "STOP_LOSS", np.where(tp_hit, "TAKE_PROFIT", None))
                    for idx in np.where(exit_mask)[0]:
                        trade  = short_trades[idx]
                        reason = reasons[idx]
                        if reason is None:
                            continue
                        ts, price, h, l = self._find_exact_exit_bar_numba(
                            trade, low_np, high_np, index_np, reason, False
                        )
                        if ts is not None:
                            self._execute_trade_exit(
                                trade, ts, price, reason, exit_stats, verbose, h, l
                            )

    # ──────────────────────────────────────────────────────────────────── #
    # Main simulation loop
    # ──────────────────────────────────────────────────────────────────── #

    def simulate_trades(
        self,
        df_strategy: pd.DataFrame,
        signal_frame: SignalFrame,
        verbose: bool = False,
        progressive_tracker=None,
        signal_id_map: Optional[Dict] = None,   # [L3] was: Dict = None
        df_ltf: Optional[pd.DataFrame] = None,
    ) -> TradeResult:
        """
        Simulate trades with realistic LTF execution.

        Args:
            df_strategy: Strategy timeframe OHLCV
            signal_frame: SignalFrame with int8 signals (CF-6)
            verbose: Enable verbose logging
            progressive_tracker: Optional progressive tracker
            signal_id_map: Optional signal ID map
            df_ltf: LTF data for execution

        Returns:
            TradeResult contract
        """
        if df_ltf is None or df_ltf.empty:
            logger.error("EXECUTION ABORTED: LTF data missing")
            raise ValueError(
                "LTF execution data missing. "
                "Verify config paths.ltf_ohlcv_file points to valid 1-second OHLCV data."
            )

        # CF-6: Translate SignalFrame to string Series internally
        filtered_signals: pd.Series = (
            signal_frame.signals
            .map(_SIGNAL_CODE_TO_STR)
            .dropna()
        )

        self.progressive_tracker = progressive_tracker
        self._tracking_enabled = (
            progressive_tracker is not None
            and not isinstance(progressive_tracker, NullProgressiveTracker)
        )
        self.df_ltf = df_ltf

        # Reset O(1) lookup structures
        self._open_trades        = {}
        self._tm_id_to_entry_id  = {}
        self._trade_list_index: Dict[str, int] = {}

        # dtype optimization
        ohlc_cols = ["open", "high", "low", "close"]
        for df in (self.df_ltf, df_strategy):
            for col in ohlc_cols:
                if col in df.columns and df[col].dtype != np.float32:
                    df[col] = df[col].to_numpy(dtype=np.float32)
            if "volume" in df.columns and df["volume"].dtype == np.float64:
                df["volume"] = df["volume"].to_numpy(dtype=np.float32)

        if verbose:
            logger.info(f"LTF Execution: {len(df_ltf):,} bars (float32 optimized)")
            logger.info(f"Numba: {'ENABLED' if NUMBA_AVAILABLE else 'NOT AVAILABLE (numpy fallback)'}")

        self._precompute_ltf_windows(df_strategy)
        if verbose:
            logger.info(f"Pre-computed {len(self._ltf_windows):,} LTF windows")

        # Pre-build signals dict for O(1) lookup
        signals_dict: Dict[pd.Timestamp, Any] = {
            ts: val
            for ts, val in zip(filtered_signals.index, filtered_signals.values)
            if not pd.isna(val)
        }

        # Convert index to list for faster iteration
        strategy_index_list: List[pd.Timestamp] = list(df_strategy.index)
        close_np = df_strategy["close"].to_numpy(np.float32)

        check_exits = self._check_exits_with_ltf_ohlc
        tm           = self.trade_manager
        risk_mgr     = self.risk_manager
        tracker      = self.progressive_tracker
        tracking_enabled = self._tracking_enabled

        position_rejected_count = {"buy": 0, "sell": 0}
        exit_stats = {
            "STOP_LOSS": 0,
            "TAKE_PROFIT": 0,
            "OPPOSITE_SIGNAL": 0,
            "END_OF_DATA": 0,
        }
        risk_stats = {
            "approved":       {"buy": 0, "sell": 0},
            "rejected":       {"buy": 0, "sell": 0},
            "adjusted":       {"buy": 0, "sell": 0},
            "total_approved": 0,
            "total_rejected": 0,
            "total_adjusted": 0,
        }

        # ────────────────────────────────────────────────────────────────
        # Main simulation loop — Legacy-compatible order (position first)
        # ────────────────────────────────────────────────────────────────
        for i, timestamp in enumerate(strategy_index_list):
            # 1) Check exits on LTF (unchanged)
            check_exits(timestamp, exit_stats, verbose)

            # 2) Skip if no signal — O(1) dict lookup
            signal_type = signals_dict.get(timestamp)
            if signal_type is None:
                continue

            is_long = signal_type == "BUY"
            direction = "BUY" if is_long else "SELL"
            bid_price = float(close_np[i])
            signal_id = signal_id_map.get(timestamp) if signal_id_map else None

            # 3) POSITION CONTROL FIRST (Legacy logic) — cheap gatekeeper
            result = tm.handle_signal_position_only(
                timestamp=timestamp,
                signal_type=signal_type,
            )

            # 4) Progressive tracking: position management stage
            if tracking_enabled and signal_id:
                needs_open = result.is_open or result.decision_type == DecisionType.CLOSE_AND_REVERSE
                tracker.update_position_management_details(
                    signal_id=signal_id,
                    action=result.to_dict()["action"],
                    reason=result.reason,
                    current_direction=tm.current_direction.to_string() if tm.current_direction else None,
                    open_positions_count=len(tm.current_positions),
                    pyramiding_enabled=tm.pyramiding_enabled,
                    close_on_opposite=tm.close_on_opposite,
                    can_open_new_position=needs_open,
                )

            # 5) Handle position rejection
            if result.is_reject:
                self._reject_signal(timestamp, direction, signal_id, result.reason, verbose)
                position_rejected_count["buy" if is_long else "sell"] += 1
                continue

            # 6) Risk management ONLY if we are going to open/reverse
            needs_open = result.is_open or result.decision_type == DecisionType.CLOSE_AND_REVERSE
            params = None
            if needs_open:
                params = risk_mgr.compute_trade_parameters(timestamp, bid_price, is_long)
                if params is None:
                    self._handle_risk_rejection(
                        action=result.decision_type.name,
                        close_trade_ids=result.close_trade_ids if hasattr(result, "close_trade_ids") else [],
                        timestamp=timestamp,
                        direction=direction,
                        signal_id=signal_id,
                        is_long=is_long,
                        risk_stats=risk_stats,
                        position_rejected_count=position_rejected_count,
                        current_bid=bid_price,
                        verbose=verbose,
                    )
                    continue

                key = "buy" if is_long else "sell"
                risk_stats["approved"][key] += 1
                risk_stats["total_approved"] += 1
                if params.sl_adjusted:
                    risk_stats["adjusted"][key] += 1
                    risk_stats["total_adjusted"] += 1

                if tracking_enabled and signal_id:
                    tracker.update_risk_management_details(...)   # keep your existing call

            # 7) Execute trade manager actions
            if result.decision_type == DecisionType.CLOSE_AND_REVERSE:
                self._handle_close(timestamp, result.close_trade_ids, bid_price, exit_stats, verbose)
                tm.close_positions(result.close_trade_ids)
                if params:
                    self._handle_open(timestamp, direction, params, result.new_trade_id,
                                      verbose, comment_suffix=" (Reversal)", signal_id=signal_id)
            elif result.is_open:
                if params:
                    self._handle_open(timestamp, direction, params, result.new_trade_id,
                                      verbose, comment_suffix="", signal_id=signal_id)

        # 8) Close remaining positions at end of data
        self._close_remaining_positions(df_strategy, exit_stats, verbose)

        if verbose and self.profiler:
            self.profiler.print_report()

        execution_mode = (
            "LTF_OHLC_V5_NUMBA" if NUMBA_AVAILABLE else "LTF_OHLC_V5"
        )

        return TradeResult.from_trades(
            trades=self.all_trades,
            rejected_signals=self.rejected_signals,
            exit_stats=exit_stats,
            risk_stats=risk_stats,
            position_rejected=position_rejected_count,
            trade_manager_metrics=self.trade_manager.get_metrics(),
            execution_mode=execution_mode,
        )

    # ──────────────────────────────────────────────────────────────────── #
    # Rejected signals
    # ──────────────────────────────────────────────────────────────────── #

    def _reject_signal(
        self,
        timestamp: pd.Timestamp,
        direction: str,
        signal_id: Optional[int],
        reason: str,
        verbose: bool,
        rejection_stage: str = "POSITION",
    ) -> None:
        """Record a rejected signal as RejectedSignal contract."""
        self.rejection_counter += 1
        rejected = RejectedSignal(
            rejection_id=f"R{self.rejection_counter}",
            signal_id=signal_id,
            rejection_time=timestamp,
            direction=direction,
            rejection_stage=rejection_stage,
            rejection_reason=reason,
            current_price=None,
        )
        self.rejected_signals.append(rejected)
        if verbose:
            logger.debug(f"[REJECT] {timestamp} {direction} — {reason}")

    # ──────────────────────────────────────────────────────────────────── #
    # Risk rejection handling
    # ──────────────────────────────────────────────────────────────────── #

    def _handle_risk_rejection(
        self,
        action: str,
        close_trade_ids: List[int],
        timestamp: pd.Timestamp,
        direction: str,
        signal_id: Optional[int],
        is_long: bool,
        risk_stats: Dict,
        position_rejected_count: Dict,
        current_bid: float,
        verbose: bool,
    ) -> None:
        """Handle risk rejection."""
        key = "buy" if is_long else "sell"
        risk_stats["rejected"][key] += 1
        risk_stats["total_rejected"] += 1

        if self._tracking_enabled and signal_id:
            self.progressive_tracker.update_risk_management_details(
                signal_id=signal_id, approved=False, reason="Risk validation failed"
            )

        if action in ("OPEN", "REJECT"):
            self._reject_signal(
                timestamp, direction, signal_id, "Risk rejected", verbose, rejection_stage="RISK"
            )
            position_rejected_count[key] += 1
        elif action == "CLOSE_AND_REVERSE":
            self._handle_close(timestamp, close_trade_ids, current_bid, {}, verbose)
            self.trade_manager.close_positions(close_trade_ids)
            if verbose:
                logger.debug(f"[CLOSE ONLY] {timestamp} {direction} — Risk rejected new position")

    # ──────────────────────────────────────────────────────────────────── #
    # Close positions on opposite signal
    # ──────────────────────────────────────────────────────────────────── #

    def _handle_close(
        self,
        timestamp: pd.Timestamp,
        close_trade_ids: List[int],
        current_bid: float,
        exit_stats: Dict,
        verbose: bool,
    ) -> None:
        """Close positions due to opposite signal."""
        spread = (
            self.spread_manager.get_spread_in_points(current_bid)
            if self.spread_manager else 0.0
        )

        for tid in close_trade_ids:
            entry_id = self._tm_id_to_entry_id.get(tid)
            if entry_id is None:
                continue
            trade = self._open_trades.get(entry_id)
            if trade is None:
                continue

            exit_price = current_bid if trade.entry.is_long else current_bid + spread

            trade_exit = TradeExit.create(
                entry=trade.entry,
                exit_time=timestamp,
                exit_price=exit_price,
                exit_reason=ExitReason.OPPOSITE_SIGNAL,
            )
            updated_trade = Trade(entry=trade.entry, exit=trade_exit)

            list_idx = self._trade_list_index[entry_id]
            self.all_trades[list_idx] = updated_trade
            del self._open_trades[entry_id]
            del self._tm_id_to_entry_id[tid]

            exit_stats["OPPOSITE_SIGNAL"] = exit_stats.get("OPPOSITE_SIGNAL", 0) + 1

            if verbose:
                logger.debug(
                    f"[CLOSE] {timestamp} {trade.direction} OPPOSITE at {exit_price:.2f}"
                )

    # ──────────────────────────────────────────────────────────────────── #
    # Open new position
    # ──────────────────────────────────────────────────────────────────── #

    def _handle_open(
        self,
        timestamp: pd.Timestamp,
        direction: str,
        params: TradeParameters,
        new_trade_id: int,
        verbose: bool,
        comment_suffix: str = "",
        signal_id: Optional[int] = None,
    ) -> None:
        """Open new position."""
        self.trade_counter += 1
        entry_id = f"E{self.trade_counter}"

        entry = TradeEntry.from_trade_parameters(
            entry_id=entry_id,
            timestamp=timestamp,
            direction=TradeDirection.from_string(direction),
            params=params,
        )
        entry_with_metadata = TradeEntry(
            entry_id=entry.entry_id,
            trade_manager_id=new_trade_id,
            signal_id=signal_id,
            entry_time=entry.entry_time,
            direction=entry.direction,
            entry_price=entry.entry_price,
            stop_loss=entry.stop_loss,
            take_profit=entry.take_profit,
            position_size=entry.position_size,
            sl_distance=entry.sl_distance,
            tp_distance=entry.tp_distance,
            risk_reward_ratio=entry.risk_reward_ratio,
            atr_value=entry.atr_value,
            spread_enabled=entry.spread_enabled,
            spread_points=entry.spread_points,
            sl_adjusted=entry.sl_adjusted,
            comment=params.comment + comment_suffix,
        )

        trade = Trade(entry=entry_with_metadata, exit=None)

        # Register in O(1) index structures
        list_idx = len(self.all_trades)
        self.all_trades.append(trade)
        self._trade_list_index[entry_id]            = list_idx
        self._open_trades[entry_id]                  = trade
        if new_trade_id is not None:
            self._tm_id_to_entry_id[new_trade_id]   = entry_id

        self.trade_manager.open_position(
            trade_id=new_trade_id,
            timestamp=timestamp,
            direction=TradeDirection.from_string(direction),
            entry_price=params.entry_price_executed,
            stop_loss=params.stop_loss_trigger,
            take_profit=params.take_profit,
            position_size=params.position_size,
            meta={"signal_id": signal_id} if signal_id else None,
        )

        if self._tracking_enabled and signal_id:
            self.progressive_tracker.update_trade_execution_details(
                signal_id=signal_id,
                trade_id=self.trade_counter,
                entry_time=timestamp,
                entry_price_executed=params.entry_price_executed,
                sl_price_executed=params.stop_loss_trigger,
                tp_price_executed=params.take_profit,
                reason="OPENED" + comment_suffix,
            )

        if verbose:
            logger.debug(
                f"[OPEN] {timestamp} {direction} at {params.entry_price_executed:.2f}{comment_suffix}"
            )

    # ──────────────────────────────────────────────────────────────────── #
    # Close remaining positions at end of backtest
    # ──────────────────────────────────────────────────────────────────── #

    def _close_remaining_positions(
        self,
        df_strategy: pd.DataFrame,
        exit_stats: Dict,
        verbose: bool,
    ) -> None:
        """Close all remaining open positions at end of backtest."""
        if df_strategy.empty or not self._open_trades:
            return

        last_timestamp = df_strategy.index[-1]
        last_bid       = float(df_strategy.iloc[-1]["close"])
        spread = (
            self.spread_manager.get_spread_in_points(last_bid)
            if self.spread_manager else 0.0
        )

        for trade in list(self._open_trades.values()):
            exit_price = last_bid if trade.entry.is_long else last_bid + spread
            self._execute_trade_exit(
                trade,
                exit_time=last_timestamp,
                exit_price=exit_price,
                exit_reason="END_OF_DATA",
                exit_stats=exit_stats,
                verbose=verbose,
                exit_high=last_bid,
                exit_low=last_bid,
            )
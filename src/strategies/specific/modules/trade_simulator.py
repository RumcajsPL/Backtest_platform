"""
Trade simulation with LTF OHLC execution

Version: 5.1.0 (Phase 5 Final)
Session: 21 - Final Hardening

Changes from v5.0.0:
- Phase 5.8: TradeManager now initialized with StrategyConfig
- Phase 5.7: Spread settings read exclusively from SpreadManager
"""

import time
import logging
from collections import defaultdict
from typing import Dict, List, Optional, Any

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


class TradeSimulatorProfiler:
    """Simple profiler for performance monitoring in analytics mode"""

    def __init__(self):
        self.timings = defaultdict(list)

    def profile(self, name):
        def decorator(func):
            def wrapper(*args, **kwargs):
                start = time.perf_counter()
                result = func(*args, **kwargs)
                elapsed = time.perf_counter() - start
                self.timings[name].append(elapsed)
                return result
            return wrapper
        return decorator

    def print_report(self):
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
        cache_manager: Optional[CacheManager] = None,
    ):
        """
        Initialize TradeSimulator with typed config.

        Args:
            config: StrategyConfig instance
            df_full: Full OHLCV DataFrame
            cache_manager: Optional cache manager for multi-run state
        """
        self.config = config
        self.df_full = df_full
        self._cache_manager = cache_manager or CacheManager()

        analytics_cfg = getattr(config, "analytics", {})
        self.profile_enabled = analytics_cfg.get("profile_simulator", False)

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
            ohlcv_artf=None,
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
        """Pre-compute LTF windows and numpy views for each strategy bar."""
        if self.df_ltf is None or self.df_ltf.empty:
            raise ValueError(
                "LTF execution data missing. "
                "Verify config paths.ltf_ohlcv_file points to valid 1-second OHLCV data."
            )

        self._ltf_windows = {}
        ltf_index_np = self.df_ltf.index.to_numpy()  # datetime64[ns]
        low_np  = self.df_ltf["low"].to_numpy(np.float32)
        high_np = self.df_ltf["high"].to_numpy(np.float32)

        one_minute = np.timedelta64(1, "m")

        for strategy_time in df_strategy.index:
            strategy_ts  = np.datetime64(strategy_time)
            window_end_ts = strategy_ts + one_minute

            start_idx = ltf_index_np.searchsorted(strategy_ts,  side="left")
            end_idx   = ltf_index_np.searchsorted(window_end_ts, side="left")
            if end_idx <= start_idx:
                continue

            sl  = low_np[start_idx:end_idx]
            sh  = high_np[start_idx:end_idx]
            si  = ltf_index_np[start_idx:end_idx]
            if sl.size == 0:
                continue

            self._ltf_windows[strategy_time] = {
                "min_low":  float(sl.min()),
                "max_high": float(sh.max()),
                "low_np":   sl,
                "high_np":  sh,
                "index_np": si,
            }

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
        """Check for SL/TP exits using vectorized LTF OHLC."""
        window = self._ltf_windows.get(strategy_timestamp)
        if window is None:
            return

        low_np   = window["low_np"]
        high_np  = window["high_np"]
        index_np = window["index_np"]
        if low_np.size == 0:
            return

        # Filter to trades opened before this bar
        open_list = [
            t for t in self._open_trades.values()
            if t.entry.entry_time < strategy_timestamp
        ]
        if not open_list:
            return

        min_low  = window["min_low"]
        max_high = window["max_high"]

        long_trades  = [t for t in open_list if t.entry.is_long]
        short_trades = [t for t in open_list if t.entry.is_short]

        if long_trades:
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

        if short_trades:
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
        signal_frame: SignalFrame,  # Now accepts SignalFrame directly (CF-6)
        verbose: bool = False,
        progressive_tracker=None,
        signal_id_map: Dict = None,
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

        for i, timestamp in enumerate(strategy_index_list):
            # 1) Check exits on LTF
            check_exits(timestamp, exit_stats, verbose)

            # 2) Skip if no signal — O(1) dict lookup
            signal_type = signals_dict.get(timestamp)
            if signal_type is None:
                continue

            is_long   = signal_type == "BUY"
            direction = "BUY" if is_long else "SELL"
            bid_price = float(close_np[i])
            signal_id = signal_id_map.get(timestamp) if signal_id_map else None

            # 3) Risk management
            params = risk_mgr.compute_trade_parameters(timestamp, bid_price, is_long)

            if params is None:
                self._handle_risk_rejection(
                    action="REJECT",
                    close_trade_ids=[],
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
                tracker.update_risk_management_details(
                    signal_id=signal_id,
                    approved=True,
                    reason=params.comment,
                    entry_price=params.entry_price_executed,
                    sl_price=params.stop_loss_trigger,
                    tp_price=params.take_profit,
                    spread_cost=params.spread_points if params.spread_points else 0.0,
                    atr_value=params.atr_value,
                    atr_length=params.atr_length,
                    atr_multiplier=getattr(params, "atr_multiplier", None),
                    sl_distance_raw=params.sl_distance_raw,
                    sl_price_raw=params.sl_price_raw,
                    annual_range_value=params.annual_range_value,
                    risk_percentile_calculated=params.risk_percentile_calculated,
                    max_risk_percentile=params.max_risk_percentile,
                    risk_percentile_passed=params.risk_percentile_passed,
                    sl_price_final=params.stop_loss_trigger,
                    tp_price_final=params.take_profit,
                    rr_ratio=params.risk_reward_ratio,
                    spread_enabled=params.spread_enabled,
                    spread_type=params.spread_type,
                    spread_value=getattr(params, "spread_value", params.spread_points),
                    spread_points=params.spread_points,
                    entry_price_mid=params.entry_price_mid,
                    entry_price_adjusted=params.entry_price_executed,
                    spread_efficiency_percent=getattr(params, "spread_efficiency_percent", None),
                )

            # 4) Trade manager decision
            result = tm.handle_signal(
                timestamp=timestamp,
                signal_type=signal_type,
                entry_price=params.entry_price_executed,
                stop_loss=params.stop_loss_trigger,
                take_profit=params.take_profit,
                position_size=params.position_size,
                meta={"signal_id": signal_id} if signal_id else None,
            )

            # 5) Progressive tracking
            if tracking_enabled and signal_id:
                current_dir_str = tm.current_direction.to_string() if tm.current_direction else None
                tracker.update_position_management_details(
                    signal_id=signal_id,
                    action=result.to_dict()["action"],
                    reason=result.reason,
                    current_direction=current_dir_str,
                    open_positions_count=len(tm.current_positions),
                    pyramiding_enabled=tm.pyramiding_enabled,
                    close_on_opposite=tm.close_on_opposite,
                    can_open_new_position=(
                        result.is_open or result.decision_type == DecisionType.CLOSE_AND_REVERSE
                    ),
                )

            # 6) Position rejection
            if result.is_reject:
                self._reject_signal(timestamp, direction, signal_id, result.reason, verbose)
                position_rejected_count[key] += 1
                continue

            # 7) Execute trade manager actions
            if result.decision_type == DecisionType.CLOSE_AND_REVERSE:
                self._handle_close(timestamp, result.close_trade_ids, bid_price, exit_stats, verbose)
                tm.close_positions(result.close_trade_ids)
                self._handle_open(
                    timestamp, direction, params, result.new_trade_id,
                    verbose, comment_suffix=" (Reversal)", signal_id=signal_id,
                )
            elif result.is_open:
                self._handle_open(
                    timestamp, direction, params, result.new_trade_id,
                    verbose, comment_suffix="", signal_id=signal_id,
                )

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
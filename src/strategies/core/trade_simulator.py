"""Trade simulation with LTF OHLC execution - Optimized v4.3
v4.3: v4.1 + ProgressiveTracker v2 alignment (keyword-based risk updates),
      Numba-accelerated exit detection (when available), datetime-safe.
"""
import time
import logging
from collections import defaultdict
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from src.strategies.trade_management.risk_manager import RiskManager
from src.strategies.trade_management.spread_manager import SpreadManager
from src.strategies.core.null_progressive_tracker import NullProgressiveTracker

logger = logging.getLogger(__name__)

try:
    from numba import njit
    NUMBA_AVAILABLE = True
except ImportError:
    NUMBA_AVAILABLE = False


class TradeSimulatorProfiler:
    """Simple profiler for performance monitoring in debug mode"""

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
            logger.info(f"{name:30s}: {total:.3f}s total, {avg:.3f}s avg, {len(times)} calls")


# ---------------- Numba-accelerated helpers ---------------- #

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
    v4.3: v4.1 + ProgressiveTracker v2 alignment (keyword-based risk updates),
          Numba-accelerated exit detection (when numba is available),
          fully datetime-safe.
    """

    def __init__(self, config: Dict, df_full: pd.DataFrame):
        self.config = config
        self.df_full = df_full
        self.profile_enabled = config.get("debug", {}).get("profile_simulator", False)

        self.all_trades: List[Dict] = []
        self.trade_counter = 0

        self.trade_manager = None
        self.spread_manager = None
        self.progressive_tracker = None
        self._tracking_enabled = False
        self.df_ltf: Optional[pd.DataFrame] = None
        self._ltf_windows: Dict = {}
        self.risk_manager = RiskManager(self.config, df_full)

        self.initialize_managers()

        if self.profile_enabled:
            self.profiler = TradeSimulatorProfiler()
            self._check_exits_with_ltf_ohlc = self.profiler.profile("check_exits_ltf")(
                self._check_exits_with_ltf_ohlc
            )
        else:
            self.profiler = None

    # ------------------------------------------------------------------ #
    # Initialization
    # ------------------------------------------------------------------ #
    def initialize_managers(self):
        from src.strategies.trade_management.trade_manager import TradeManager

        self.trade_manager = TradeManager(self.config)

        tm_config = self.config.get("trade_management", {})
        spread_config = tm_config.get("spread", {})
        if spread_config.get("enabled", False):
            asset_symbol = self.config.get("asset", {}).get("symbol", "")
            config_path = spread_config.get("config_path")
            self.spread_manager = SpreadManager(asset_symbol, config_path)

    # ------------------------------------------------------------------ #
    # LTF window precomputation
    # ------------------------------------------------------------------ #
    def _precompute_ltf_windows(self, df_strategy: pd.DataFrame) -> None:
        """Pre-compute LTF windows and numpy views for each strategy bar"""
        if self.df_ltf is None or self.df_ltf.empty:
            logger.error("EXECUTION ABORTED: LTF data missing")
            logger.error("Required: 1-second OHLCV data for realistic SL/TP execution")
            logger.error("Check config: paths.ltf_ohlcv_file")
            raise ValueError(
                "LTF execution data missing. "
                "Verify config paths.ltf_ohlcv_file points to valid 1-second OHLCV data."
            )

        self._ltf_windows = {}
        ltf_index = self.df_ltf.index
        low_series = self.df_ltf["low"]
        high_series = self.df_ltf["high"]
        low_np = low_series.to_numpy(np.float32)
        high_np = high_series.to_numpy(np.float32)
        ltf_index_np = ltf_index.to_numpy()  # datetime64[ns]

        for strategy_time in df_strategy.index:
            window_end = strategy_time + pd.Timedelta(minutes=1)

            strategy_ts = np.datetime64(strategy_time)
            window_end_ts = np.datetime64(window_end)

            start_idx = ltf_index_np.searchsorted(strategy_ts, side="left")
            end_idx = ltf_index_np.searchsorted(window_end_ts, side="left")
            if end_idx <= start_idx:
                continue

            slice_low = low_np[start_idx:end_idx]
            slice_high = high_np[start_idx:end_idx]
            slice_index = ltf_index_np[start_idx:end_idx]
            if slice_low.size == 0:
                continue

            self._ltf_windows[strategy_time] = {
                "min_low": float(slice_low.min()),
                "max_high": float(slice_high.max()),
                "low_np": slice_low,
                "high_np": slice_high,
                "index_np": slice_index,
            }

    # ------------------------------------------------------------------ #
    # Numba-accelerated exact exit detection
    # ------------------------------------------------------------------ #
    def _find_exact_exit_bar_numba(
        self,
        trade: Dict,
        low_np: np.ndarray,
        high_np: np.ndarray,
        index_np: np.ndarray,
        exit_reason: str,
        is_long: bool,
    ) -> tuple[Optional[pd.Timestamp], Optional[float], Optional[float], Optional[float]]:
        """Numba-accelerated first-hit search (returns pandas Timestamp)"""
        if low_np.size == 0:
            return None, None, None, None

        if NUMBA_AVAILABLE:
            is_sl = exit_reason == "STOP_LOSS"
            if is_long:
                idx = _numba_find_first_hit_long(
                    low_np, high_np, trade["sl_price"], trade["tp_price"], is_sl
                )
            else:
                idx = _numba_find_first_hit_short(
                    low_np, high_np, trade["sl_price"], trade["tp_price"], is_sl
                )
            if idx < 0:
                return None, None, None, None
        else:
            # Fallback: simple numpy scan (same logic as v3.1)
            if is_long:
                if exit_reason == "STOP_LOSS":
                    hit_mask = low_np <= trade["sl_price"]
                else:
                    hit_mask = high_np >= trade["tp_price"]
            else:
                if exit_reason == "STOP_LOSS":
                    hit_mask = high_np >= trade["sl_price"]
                else:
                    hit_mask = low_np <= trade["tp_price"]
            if not hit_mask.any():
                return None, None, None, None
            idx = int(np.argmax(hit_mask))

        ts = pd.Timestamp(index_np[idx])
        low_val = float(low_np[idx])
        high_val = float(high_np[idx])

        if is_long:
            if exit_reason == "STOP_LOSS":
                exit_price = min(low_val, trade["sl_price"])
            else:
                exit_price = min(high_val, trade["tp_price"])
        else:
            if exit_reason == "STOP_LOSS":
                exit_price = max(high_val, trade["sl_price"])
            else:
                exit_price = max(low_val, trade["tp_price"])

        return ts, exit_price, high_val, low_val

    # ------------------------------------------------------------------ #
    # Exit execution
    # ------------------------------------------------------------------ #
    def _execute_trade_exit(
        self,
        trade: Dict,
        exit_time: pd.Timestamp,
        exit_price: float,
        exit_reason: str,
        exit_stats: Dict,
        verbose: bool,
        exit_high: float,
        exit_low: float,
    ):
        """Execute trade exit and update tracking"""
        if trade["direction"] == "BUY":
            pnl_points = exit_price - trade["entry_price"]
        else:
            pnl_points = trade["entry_price"] - exit_price

        pnl_percent = (
            (pnl_points / trade["entry_price"]) * 100 if trade["entry_price"] else 0
        )

        entry_time = trade.get("entry_time") or trade.get("timestamp")
        duration_minutes = (
            (exit_time - entry_time).total_seconds() / 60 if entry_time else None
        )

        trade["status"] = "CLOSED"
        trade["exit_time"] = exit_time
        trade["exit_price"] = exit_price
        trade["exit_reason"] = exit_reason
        trade["pnl_points"] = pnl_points
        trade["pnl_percent"] = pnl_percent
        trade["duration_minutes"] = duration_minutes
        trade["is_win"] = pnl_points > 0
        trade["is_loss"] = pnl_points < 0

        if trade.get("trade_manager_trade_id"):
            self.trade_manager.close_positions([trade["trade_manager_trade_id"]])

        exit_stats[exit_reason] = exit_stats.get(exit_reason, 0) + 1

        if self._tracking_enabled and trade.get("signal_id"):
            self.progressive_tracker.update_trade_execution_details(
                signal_id=trade["signal_id"],
                trade_id=trade["trade_id"],
                exit_time=exit_time,
                exit_price=exit_price,
                exit_reason=exit_reason,
                pnl_points=pnl_points,
                pnl_percent=pnl_percent,
                duration_minutes=duration_minutes,
                is_win=pnl_points > 0,
                is_loss=pnl_points < 0,
                exit_check_high=exit_high,
                exit_check_low=exit_low,
                reason=f"CLOSED: {exit_reason}",
            )

        if verbose:
            theoretical = (
                trade["sl_price"] if exit_reason == "STOP_LOSS" else trade["tp_price"]
            )
            diff = exit_price - theoretical
            sign = "+" if diff > 0 else ""
            logger.debug(
                f"[EXIT-LTF] {exit_time} {trade['direction']} {exit_reason} | "
                f"Actual: {exit_price:.5f} ({sign}{diff:.5f}) | P&L: {pnl_points:+.2f} pts"
            )

    # ------------------------------------------------------------------ #
    # LTF exit engine
    # ------------------------------------------------------------------ #
    def _check_exits_with_ltf_ohlc(
        self,
        strategy_timestamp: pd.Timestamp,
        exit_stats: Dict,
        verbose: bool,
    ):
        """Check for SL/TP exits using vectorized LTF OHLC (Numba-accelerated when available)"""
        window = self._ltf_windows.get(strategy_timestamp)
        if window is None:
            return

        low_np = window["low_np"]
        high_np = window["high_np"]
        index_np = window["index_np"]
        if low_np.size == 0:
            return

        open_trades = [
            t
            for t in self.all_trades
            if t["status"] == "OPEN"
            and (t.get("entry_time") or t.get("timestamp")) < strategy_timestamp
        ]
        if not open_trades:
            return

        long_trades = [t for t in open_trades if t["direction"] == "BUY"]
        short_trades = [t for t in open_trades if t["direction"] == "SELL"]

        if long_trades:
            sl_prices = np.array([t["sl_price"] for t in long_trades], dtype=np.float32)
            tp_prices = np.array([t["tp_price"] for t in long_trades], dtype=np.float32)
            sl_hit = window["min_low"] <= sl_prices
            tp_hit = window["max_high"] >= tp_prices
            exit_mask = sl_hit | tp_hit
            reasons = np.where(sl_hit, "STOP_LOSS", np.where(tp_hit, "TAKE_PROFIT", None))

            for idx in np.where(exit_mask)[0]:
                trade = long_trades[idx]
                reason = reasons[idx]
                if not reason:
                    continue
                ts, price, h, l = self._find_exact_exit_bar_numba(
                    trade, low_np, high_np, index_np, reason, True
                )
                if ts is not None:
                    self._execute_trade_exit(
                        trade, ts, price, reason, exit_stats, verbose, h, l
                    )

        if short_trades:
            sl_prices = np.array(
                [t["sl_price"] for t in short_trades], dtype=np.float32
            )
            tp_prices = np.array(
                [t["tp_price"] for t in short_trades], dtype=np.float32
            )
            sl_hit = window["max_high"] >= sl_prices
            tp_hit = window["min_low"] <= tp_prices
            exit_mask = sl_hit | tp_hit
            reasons = np.where(sl_hit, "STOP_LOSS", np.where(tp_hit, "TAKE_PROFIT", None))

            for idx in np.where(exit_mask)[0]:
                trade = short_trades[idx]
                reason = reasons[idx]
                if not reason:
                    continue
                ts, price, h, l = self._find_exact_exit_bar_numba(
                    trade, low_np, high_np, index_np, reason, False
                )
                if ts is not None:
                    self._execute_trade_exit(
                        trade, ts, price, reason, exit_stats, verbose, h, l
                    )

    # ------------------------------------------------------------------ #
    # Main simulation loop
    # ------------------------------------------------------------------ #
    def simulate_trades(
        self,
        df_strategy: pd.DataFrame,
        filtered_signals: pd.Series,
        verbose: bool = False,
        progressive_tracker=None,
        signal_id_map: Dict = None,
        df_ltf: Optional[pd.DataFrame] = None,
    ) -> Dict:
        """Simulate trades with realistic LTF execution"""
        if df_ltf is None or df_ltf.empty:
            logger.error("EXECUTION ABORTED: LTF data missing")
            logger.error("Required: 1-second OHLCV data for realistic SL/TP execution")
            logger.error("Check config: paths.ltf_ohlcv_file")
            raise ValueError(
                "LTF execution data missing. "
                "Verify config paths.ltf_ohlcv_file points to valid 1-second OHLCV data."
            )

        self.progressive_tracker = progressive_tracker
        self._tracking_enabled = (
            progressive_tracker is not None
            and not isinstance(progressive_tracker, NullProgressiveTracker)
        )
        self.df_ltf = df_ltf

        # dtype optimization
        ohlc_cols = ["open", "high", "low", "close"]
        for df in (self.df_ltf, df_strategy):
            available_cols = [c for c in ohlc_cols if c in df.columns]
            if available_cols:
                df[available_cols] = df[available_cols].astype("float32")
            if "volume" in df.columns and df["volume"].dtype == "float64":
                df["volume"] = df["volume"].astype("float32")

        if verbose:
            logger.info(f"LTF Execution: {len(df_ltf):,} bars (float32 optimized)")
            if NUMBA_AVAILABLE:
                logger.info("Numba acceleration: ENABLED for exit engine")
            else:
                logger.info("Numba acceleration: NOT AVAILABLE (using pure numpy fallback)")

        self._precompute_ltf_windows(df_strategy)
        if verbose:
            logger.info(f"Pre-computed {len(self._ltf_windows):,} LTF windows")

        strategy_index = df_strategy.index
        close_np = df_strategy["close"].to_numpy(np.float32)

        check_exits = self._check_exits_with_ltf_ohlc
        tm = self.trade_manager
        risk_mgr = self.risk_manager
        tracker = self.progressive_tracker
        tracking_enabled = self._tracking_enabled

        position_rejected_count = {"buy": 0, "sell": 0}
        exit_stats = {
            "STOP_LOSS": 0,
            "TAKE_PROFIT": 0,
            "OPPOSITE_SIGNAL": 0,
            "END_OF_DATA": 0,
        }
        risk_stats = {
            "approved": {"buy": 0, "sell": 0},
            "rejected": {"buy": 0, "sell": 0},
            "adjusted": {"buy": 0, "sell": 0},
            "total_approved": 0,
            "total_rejected": 0,
            "total_adjusted": 0,
        }

        for i, timestamp in enumerate(strategy_index):
            # 1) Check exits on LTF
            check_exits(timestamp, exit_stats, verbose)

            # 2) Skip if no signal
            if timestamp not in filtered_signals.index or pd.isna(filtered_signals[timestamp]):
                continue

            signal_type = filtered_signals[timestamp]
            is_long = signal_type == "BUY"
            direction = "BUY" if is_long else "SELL"
            bid_price = close_np[i]
            signal_id = signal_id_map.get(timestamp) if signal_id_map else None

            # 3) Trade manager decision
            result = tm.handle_signal(timestamp, signal_type)

            # 4) Progressive tracking: position management stage
            if tracking_enabled and signal_id:
                needs_open = result["action"] in ["OPEN", "CLOSE_AND_REVERSE"]
                tracker.update_position_management_details(
                    signal_id=signal_id,
                    action=result["action"],
                    reason=result["reason"],
                    current_direction=tm.current_direction,
                    open_positions_count=len(tm.current_positions),
                    pyramiding_enabled=tm.pyramiding_enabled,
                    close_on_opposite=tm.close_on_opposite,
                    can_open_new_position=needs_open,
                )

            # 5) Handle position rejection
            if result["action"] == "REJECT":
                self._reject_signal(
                    timestamp,
                    direction,
                    signal_id,
                    result.get("reason", "Unknown"),
                    verbose,
                )
                position_rejected_count["buy" if is_long else "sell"] += 1
                continue

            # 6) Risk management for new/ reversed positions
            needs_open = result["action"] in ["OPEN", "CLOSE_AND_REVERSE"]
            params = None
            if needs_open:
                params = risk_mgr.compute_trade_parameters(timestamp, bid_price, is_long)
                if params is None:
                    self._handle_risk_rejection(
                        action=result["action"],
                        close_trade_ids=result.get("close_trade_ids", []),
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
                if params.get("sl_adjusted", False):
                    risk_stats["adjusted"][key] += 1
                    risk_stats["total_adjusted"] += 1

                if tracking_enabled and signal_id:
                    tracker.update_risk_management_details(
                        signal_id=signal_id,
                        approved=True,
                        reason=params.get("comment", None),
                        entry_price=params.get("entry_price"),
                        sl_price=params.get("sl_price"),
                        tp_price=params.get("tp_price"),
                        spread_cost=params.get("spread_cost"),
                        atr_value=params.get("atr_value"),
                        atr_length=params.get("atr_length"),
                        atr_multiplier=params.get("atr_multiplier"),
                        sl_distance_raw=params.get("sl_distance_raw"),
                        sl_price_raw=params.get("sl_price_raw"),
                        annual_range_value=params.get("annual_range_value"),
                        risk_percentile_calculated=params.get("risk_percentile_calculated"),
                        max_risk_percentile=params.get("max_risk_percentile"),
                        risk_percentile_passed=params.get("risk_percentile_passed"),
                        sl_price_final=params.get("trigger_sl"),
                        tp_price_final=params.get("tp"),
                        rr_ratio=params.get("rr_ratio"),
                        spread_enabled=params.get("spread_enabled"),
                        spread_type=params.get("spread_type"),
                        spread_value=params.get("spread_value"),
                        spread_points=params.get("spread_points"),
                        entry_price_mid=params.get("entry_price_mid"),
                        entry_price_adjusted=params.get("executed_entry"),
                        spread_efficiency_percent=params.get("spread_efficiency_percent"),
                    )

            # 7) Execute trade manager actions
            if result["action"] == "CLOSE_AND_REVERSE":
                self._handle_close(
                    timestamp,
                    result.get("close_trade_ids", []),
                    bid_price,
                    exit_stats,
                    verbose,
                )
                tm.close_positions(result.get("close_trade_ids", []))
                if params:
                    self._handle_open(
                        timestamp,
                        direction,
                        params,
                        result["new_trade_id"],
                        verbose,
                        comment_suffix=" (Reversal)",
                        signal_id=signal_id,
                    )
            elif result["action"] == "OPEN":
                if params:
                    self._handle_open(
                        timestamp,
                        direction,
                        params,
                        result["new_trade_id"],
                        verbose,
                        comment_suffix="",
                        signal_id=signal_id,
                    )

        # 8) Close remaining positions at end of data
        self._close_remaining_positions(df_strategy, exit_stats, verbose)

        if verbose and self.profiler:
            self.profiler.print_report()

        closed_trades = [t for t in self.all_trades if t["status"] == "CLOSED"]
        open_trades = [t for t in self.all_trades if t["status"] == "OPEN"]
        rejected_trades = [t for t in self.all_trades if t["status"] == "REJECTED"]

        return {
            "all_trades": self.all_trades,
            "closed_trades": closed_trades,
            "open_trades": open_trades,
            "rejected_trades": rejected_trades,
            "exit_stats": exit_stats,
            "position_rejected_count": position_rejected_count,
            "risk_stats": risk_stats,
            "trade_manager_metrics": self.trade_manager.get_metrics(),
            "execution_mode": (
                "LTF_OHLC_VECTORIZED_V4_3_NUMBA"
                if NUMBA_AVAILABLE
                else "LTF_OHLC_VECTORIZED_V4_3"
            ),
        }

    # ------------------------------------------------------------------ #
    # Rejected signals
    # ------------------------------------------------------------------ #
    def _reject_signal(
        self,
        timestamp: pd.Timestamp,
        direction: str,
        signal_id: Optional[int],
        reason: str,
        verbose: bool,
    ):
        """Record rejected signal"""
        self.trade_counter += 1
        trade = {
            "trade_id": self.trade_counter,
            "trade_manager_trade_id": None,
            "position_id": None,
            "status": "REJECTED",
            "entry_time": timestamp,
            "exit_time": None,
            "direction": direction,
            "entry_price": None,
            "exit_price": None,
            "sl_price": None,
            "tp_price": None,
            "exit_reason": None,
            "pnl_points": 0,
            "pnl_percent": 0,
            "duration_bars": 0,
            "duration_minutes": 0,
            "sl_distance": 0,
            "tp_distance": 0,
            "risk_reward_ratio": 0,
            "is_win": False,
            "is_loss": False,
            "comment": f"Rejected: {reason}",
            "reject_reason": reason,
            "signal_id": signal_id,
        }
        self.all_trades.append(trade)

        if verbose:
            logger.debug(f"[REJECT] {timestamp} {direction} - {reason}")

    # ------------------------------------------------------------------ #
    # Risk rejection handling
    # ------------------------------------------------------------------ #
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
    ):
        """Handle risk rejection scenarios"""
        key = "buy" if is_long else "sell"
        risk_stats["rejected"][key] += 1
        risk_stats["total_rejected"] += 1

        if self._tracking_enabled and signal_id:
            self.progressive_tracker.update_risk_management_details(
                signal_id=signal_id,
                approved=False,
                reason="Risk validation failed",
            )

        if action == "OPEN":
            self._reject_signal(timestamp, direction, signal_id, "Risk rejected", verbose)
            position_rejected_count[key] += 1
        elif action == "CLOSE_AND_REVERSE":
            self._handle_close(
                timestamp,
                close_trade_ids,
                current_bid,
                exit_stats={},
                verbose=verbose,
            )
            self.trade_manager.close_positions(close_trade_ids)
            if verbose:
                logger.debug(
                    f"[CLOSE ONLY] {timestamp} {direction} - Risk rejected new position"
                )

    # ------------------------------------------------------------------ #
    # Close positions on opposite signal
    # ------------------------------------------------------------------ #
    def _handle_close(
        self,
        timestamp: pd.Timestamp,
        close_trade_ids: List[int],
        current_bid: float,
        exit_stats: Dict,
        verbose: bool,
    ):
        """Close positions due to opposite signal"""
        spread = (
            self.spread_manager.get_spread_in_points(current_bid)
            if self.spread_manager
            else 0.0
        )

        for tid in close_trade_ids:
            trade = next(
                (
                    t
                    for t in self.all_trades
                    if t["status"] == "OPEN"
                    and t.get("trade_manager_trade_id") == tid
                ),
                None,
            )
            if trade:
                exit_price = (
                    current_bid if trade["direction"] == "BUY" else current_bid + spread
                )

                if trade["direction"] == "BUY":
                    pnl_points = exit_price - trade["entry_price"]
                else:
                    pnl_points = trade["entry_price"] - exit_price

                trade["status"] = "CLOSED"
                trade["exit_time"] = timestamp
                trade["exit_price"] = exit_price
                trade["exit_reason"] = "OPPOSITE_SIGNAL"
                trade["pnl_points"] = pnl_points
                trade["pnl_percent"] = (
                    (pnl_points / trade["entry_price"]) * 100
                    if trade["entry_price"]
                    else 0
                )
                trade["is_win"] = pnl_points > 0
                trade["is_loss"] = pnl_points < 0

                exit_stats["OPPOSITE_SIGNAL"] = exit_stats.get("OPPOSITE_SIGNAL", 0) + 1

                if verbose:
                    logger.debug(
                        f"[CLOSE] {timestamp} {trade['direction']} OPPOSITE at {exit_price:.2f}"
                    )

    # ------------------------------------------------------------------ #
    # Open new position
    # ------------------------------------------------------------------ #
    def _handle_open(
        self,
        timestamp: pd.Timestamp,
        direction: str,
        params: Dict,
        new_trade_id: int,
        verbose: bool,
        comment_suffix: str = "",
        signal_id: int = None,
    ):
        """Open new position"""
        self.trade_counter += 1

        entry = params["executed_entry"]
        sl = params["trigger_sl"]
        tp = params["tp"]
        sl_dist = abs(entry - sl)
        tp_dist = abs(tp - entry)
        rr = tp_dist / sl_dist if sl_dist > 0 else 0

        trade = {
            "trade_id": self.trade_counter,
            "trade_manager_trade_id": new_trade_id,
            "position_id": new_trade_id,
            "status": "OPEN",
            "entry_time": timestamp,
            "exit_time": None,
            "direction": direction,
            "entry_price": entry,
            "exit_price": None,
            "sl_price": sl,
            "tp_price": tp,
            "exit_reason": None,
            "pnl_points": 0,
            "pnl_percent": 0,
            "duration_bars": 0,
            "duration_minutes": 0,
            "sl_distance": sl_dist,
            "tp_distance": tp_dist,
            "risk_reward_ratio": rr,
            "is_win": False,
            "is_loss": False,
            "comment": params["comment"] + comment_suffix,
            "reject_reason": None,
            "signal_id": signal_id,
        }

        self.all_trades.append(trade)
        self.trade_manager.open_position(new_trade_id, timestamp, direction)

        if self._tracking_enabled and signal_id:
            self.progressive_tracker.update_trade_execution_details(
                signal_id=signal_id,
                trade_id=self.trade_counter,
                entry_time=timestamp,
                entry_price_executed=entry,
                sl_price_executed=sl,
                tp_price_executed=tp,
                reason="OPENED" + comment_suffix,
            )

        if verbose:
            logger.debug(
                f"[OPEN] {timestamp} {direction} at {entry:.2f}{comment_suffix}"
            )

    # ------------------------------------------------------------------ #
    # Close remaining positions at end of backtest
    # ------------------------------------------------------------------ #
    def _close_remaining_positions(
        self,
        df_strategy: pd.DataFrame,
        exit_stats: Dict,
        verbose: bool,
    ):
        """Close all remaining open positions at end of backtest"""
        if df_strategy.empty:
            return

        last_timestamp = df_strategy.index[-1]
        last_bid = df_strategy.iloc[-1]["close"]
        spread = (
            self.spread_manager.get_spread_in_points(last_bid)
            if self.spread_manager
            else 0.0
        )

        for trade in [t for t in self.all_trades if t["status"] == "OPEN"]:
            exit_price = last_bid if trade["direction"] == "BUY" else last_bid + spread

            if trade["direction"] == "BUY":
                pnl_points = exit_price - trade["entry_price"]
            else:
                pnl_points = trade["entry_price"] - exit_price

            trade["status"] = "CLOSED"
            trade["exit_time"] = last_timestamp
            trade["exit_price"] = exit_price
            trade["exit_reason"] = "END_OF_DATA"
            trade["pnl_points"] = pnl_points
            trade["pnl_percent"] = (
                (pnl_points / trade["entry_price"]) * 100
                if trade["entry_price"]
                else 0
            )
            trade["is_win"] = pnl_points > 0
            trade["is_loss"] = pnl_points < 0

            exit_stats["END_OF_DATA"] = exit_stats.get("END_OF_DATA", 0) + 1

            if verbose:
                logger.debug(
                    f"[FORCE CLOSE] {last_timestamp} {trade['direction']} END_OF_DATA at {exit_price:.2f}"
                )
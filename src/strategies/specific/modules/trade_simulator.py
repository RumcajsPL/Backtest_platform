"""Trade simulation with LTF OHLC execution - v4.6 (Session 11)

v4.6: TradeResult contract output
      - Returns TradeResult directly (no dict conversion)
      - Complete contract-based architecture
      - Use result.to_dict() for legacy compatibility

v4.5.1: Rejected signals use RejectedSignal contract (not TradeEntry)
        - Fixes validation issue with entry_price=0.0
        - RejectedSignal is separate from Trade (cleaner design)
        - Rejected signals stored in self.rejected_signals list
        - Trade contracts only for actual trades
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

# Session 10: Expanded contract imports for Trade objects
from src.strategies.contracts.trade_contracts import (
    Trade,
    TradeEntry,
    TradeExit,
    TradeDecision,
    DecisionType,
    TradeDirection,
    ExitReason,
    TradeParameters,
    RejectedSignal,  # Session 10.1: For rejected signals
    TradeResult,  # Session 11: For contract output
)

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
    
    v4.5 (Session 10): Internal Trade contract usage
    - Creates TradeEntry/TradeExit contracts
    - Stores Trade contracts internally
    - Converts to dict on output (backward compatible)
    - Ready for TradeResult migration in Session 11
    
    v4.4 (Session 9): TradeManager contract integration
    - Uses TradeDecision contract (not dict)
    - Uses Position contract with full price data
    - RiskManager called before TradeManager (price parameters)
    - Type-safe decision handling with DecisionType enum
    """

    def __init__(self, config: Dict, df_full: pd.DataFrame):
        self.config = config
        self.df_full = df_full
        self.profile_enabled = config.get("debug", {}).get("profile_simulator", False)

        # SESSION 10: Store Trade contracts (not dicts)
        self.all_trades: List[Trade] = []
        # SESSION 10.1: Store rejected signals separately
        self.rejected_signals: List[RejectedSignal] = []
        self.trade_counter = 0
        self.rejection_counter = 0

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
        trade: Trade,
        low_np: np.ndarray,
        high_np: np.ndarray,
        index_np: np.ndarray,
        exit_reason: str,
        is_long: bool,
    ) -> tuple[Optional[pd.Timestamp], Optional[float], Optional[float], Optional[float]]:
        """Numba-accelerated first-hit search (returns pandas Timestamp)
        
        SESSION 10: Now accepts Trade contract instead of dict
        """
        if low_np.size == 0:
            return None, None, None, None

        # Extract from Trade contract
        sl_price = trade.entry.stop_loss
        tp_price = trade.entry.take_profit

        if NUMBA_AVAILABLE:
            is_sl = exit_reason == "STOP_LOSS"
            if is_long:
                idx = _numba_find_first_hit_long(
                    low_np, high_np, sl_price, tp_price, is_sl
                )
            else:
                idx = _numba_find_first_hit_short(
                    low_np, high_np, sl_price, tp_price, is_sl
                )
            if idx < 0:
                return None, None, None, None
        else:
            # Fallback: simple numpy scan
            if is_long:
                if exit_reason == "STOP_LOSS":
                    hit_mask = low_np <= sl_price
                else:
                    hit_mask = high_np >= tp_price
            else:
                if exit_reason == "STOP_LOSS":
                    hit_mask = high_np >= sl_price
                else:
                    hit_mask = low_np <= tp_price
            if not hit_mask.any():
                return None, None, None, None
            idx = int(np.argmax(hit_mask))

        ts = pd.Timestamp(index_np[idx])
        low_val = float(low_np[idx])
        high_val = float(high_np[idx])

        if is_long:
            if exit_reason == "STOP_LOSS":
                exit_price = min(low_val, sl_price)
            else:
                exit_price = min(high_val, tp_price)
        else:
            if exit_reason == "STOP_LOSS":
                exit_price = max(high_val, sl_price)
            else:
                exit_price = max(low_val, tp_price)

        return ts, exit_price, high_val, low_val

    # ------------------------------------------------------------------ #
    # Exit execution
    # ------------------------------------------------------------------ #
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
    ):
        """Execute trade exit and update tracking
        
        SESSION 10: Creates TradeExit contract and updates Trade object
        """
        # Convert exit_reason string to ExitReason enum
        try:
            exit_reason_enum = ExitReason[exit_reason]
        except KeyError:
            logger.warning(f"Unknown exit reason: {exit_reason}, defaulting to END_OF_DATA")
            exit_reason_enum = ExitReason.END_OF_DATA

        # Create TradeExit contract
        trade_exit = TradeExit.create(
            entry=trade.entry,
            exit_time=exit_time,
            exit_price=exit_price,
            exit_reason=exit_reason_enum,
        )

        # Create updated Trade with exit
        updated_trade = Trade(entry=trade.entry, exit=trade_exit)
        
        # Replace in all_trades list
        for i, t in enumerate(self.all_trades):
            if t.entry.entry_id == trade.entry.entry_id:
                self.all_trades[i] = updated_trade
                break

        # Close position in TradeManager
        if trade.entry.trade_manager_id:
            self.trade_manager.close_positions([trade.entry.trade_manager_id])

        exit_stats[exit_reason] = exit_stats.get(exit_reason, 0) + 1

        # Progressive tracking
        if self._tracking_enabled and trade.entry.signal_id:
            self.progressive_tracker.update_trade_execution_details(
                signal_id=trade.entry.signal_id,
                trade_id=int(trade.entry.entry_id.replace("E", "")),  # Extract numeric ID
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
            theoretical = (
                trade.entry.stop_loss if exit_reason == "STOP_LOSS" 
                else trade.entry.take_profit
            )
            diff = exit_price - theoretical
            sign = "+" if diff > 0 else ""
            logger.debug(
                f"[EXIT-LTF] {exit_time} {trade.direction} {exit_reason} | "
                f"Actual: {exit_price:.5f} ({sign}{diff:.5f}) | P&L: {trade_exit.pnl_points:+.2f} pts"
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
        """Check for SL/TP exits using vectorized LTF OHLC
        
        SESSION 10: Works with Trade contracts
        """
        window = self._ltf_windows.get(strategy_timestamp)
        if window is None:
            return

        low_np = window["low_np"]
        high_np = window["high_np"]
        index_np = window["index_np"]
        if low_np.size == 0:
            return

        # Get open trades (Trade contracts with no exit)
        open_trades = [
            t for t in self.all_trades
            if t.is_open and t.entry.entry_time < strategy_timestamp
        ]
        if not open_trades:
            return

        # Separate by direction
        long_trades = [t for t in open_trades if t.entry.is_long]
        short_trades = [t for t in open_trades if t.entry.is_short]

        # Process LONG trades
        if long_trades:
            sl_prices = np.array([t.entry.stop_loss for t in long_trades], dtype=np.float32)
            tp_prices = np.array([t.entry.take_profit for t in long_trades], dtype=np.float32)
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

        # Process SHORT trades
        if short_trades:
            sl_prices = np.array([t.entry.stop_loss for t in short_trades], dtype=np.float32)
            tp_prices = np.array([t.entry.take_profit for t in short_trades], dtype=np.float32)
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
    ) -> TradeResult:
        """
        Simulate trades with realistic LTF execution.
        
        Session 11 Changes:
        - Returns TradeResult contract (not dict)
        - Complete contract-based architecture
        - Use result.to_dict() for legacy compatibility
        
        Session 9 Changes:
        - RiskManager called FIRST to get prices
        - TradeManager receives price parameters
        - Uses TradeDecision contract (not dict)
        - Position contracts created with full data
        """
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
            for col in available_cols:
                df.loc[:, col] = df[col].astype("float32").to_numpy()
    
            if "volume" in df.columns and df["volume"].dtype == "float64":
                df.loc[:, "volume"] = df["volume"].astype("float32").to_numpy()

        if verbose:
            logger.info(f"LTF Execution: {len(df_ltf):,} bars (float32 optimized)")
            if NUMBA_AVAILABLE:
                logger.info("Numba acceleration: ENABLED for exit engine")
            else:
                logger.info("Numba acceleration: NOT AVAILABLE (using pure numpy fallback)")
            logger.info("Session 10: Internal Trade contract usage ACTIVE")

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

            # 3) Risk management - get trade parameters
            params = risk_mgr.compute_trade_parameters(timestamp, bid_price, is_long)
            
            # 3a) If risk rejected, handle early exit
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
            
            # 3b) Track risk approval
            key = "buy" if is_long else "sell"
            risk_stats["approved"][key] += 1
            risk_stats["total_approved"] += 1
            if params.sl_adjusted:
                risk_stats["adjusted"][key] += 1
                risk_stats["total_adjusted"] += 1
            
            # 3c) Progressive tracking: risk management stage
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
                    atr_multiplier=getattr(params, 'atr_multiplier', None),
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
                    spread_value=getattr(params, 'spread_value', params.spread_points),
                    spread_points=params.spread_points,
                    entry_price_mid=params.entry_price_mid,
                    entry_price_adjusted=params.entry_price_executed,
                    spread_efficiency_percent=getattr(params, 'spread_efficiency_percent', None),
                )

            # 4) Trade manager decision
            result = tm.handle_signal(
                timestamp=timestamp,
                signal_type=signal_type,
                entry_price=params.entry_price_executed,
                stop_loss=params.stop_loss_trigger,
                take_profit=params.take_profit,
                position_size=params.position_size,
                meta={'signal_id': signal_id} if signal_id else None
            )

            # 5) Progressive tracking: position management stage
            if tracking_enabled and signal_id:
                current_dir_str = tm.current_direction.to_string() if tm.current_direction else None
                
                tracker.update_position_management_details(
                    signal_id=signal_id,
                    action=result.to_dict()['action'],
                    reason=result.reason,
                    current_direction=current_dir_str,
                    open_positions_count=len(tm.current_positions),
                    pyramiding_enabled=tm.pyramiding_enabled,
                    close_on_opposite=tm.close_on_opposite,
                    can_open_new_position=result.is_open or result.decision_type == DecisionType.CLOSE_AND_REVERSE,
                )

            # 6) Handle position rejection
            if result.is_reject:
                self._reject_signal(
                    timestamp,
                    direction,
                    signal_id,
                    result.reason,
                    verbose,
                )
                position_rejected_count["buy" if is_long else "sell"] += 1
                continue

            # 7) Execute trade manager actions
            if result.decision_type == DecisionType.CLOSE_AND_REVERSE:
                self._handle_close(
                    timestamp,
                    result.close_trade_ids,
                    bid_price,
                    exit_stats,
                    verbose,
                )
                tm.close_positions(result.close_trade_ids)
                
                self._handle_open(
                    timestamp,
                    direction,
                    params,
                    result.new_trade_id,
                    verbose,
                    comment_suffix=" (Reversal)",
                    signal_id=signal_id,
                )
                
            elif result.is_open:
                self._handle_open(
                    timestamp,
                    direction,
                    params,
                    result.new_trade_id,
                    verbose,
                    comment_suffix="",
                    signal_id=signal_id,
                )

        # 8) Close remaining positions at end of data
        self._close_remaining_positions(df_strategy, exit_stats, verbose)

        if verbose and self.profiler:
            self.profiler.print_report()

        # ================================================================
        # SESSION 11: Return TradeResult contract (no dict conversion)
        # ================================================================
        execution_mode = (
            "LTF_OHLC_VECTORIZED_V4_6_SESSION11_NUMBA"
            if NUMBA_AVAILABLE
            else "LTF_OHLC_VECTORIZED_V4_6_SESSION11"
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
        rejection_stage: str = "POSITION",  # "RISK" or "POSITION"
    ):
        """Record rejected signal
        
        SESSION 10.1: Uses RejectedSignal contract (not TradeEntry)
        Rejected signals are NOT trades - they never reached execution
        """
        self.rejection_counter += 1
        
        # Create RejectedSignal contract
        rejected = RejectedSignal(
            rejection_id=f"R{self.rejection_counter}",
            signal_id=signal_id,
            rejection_time=timestamp,
            direction=direction,
            rejection_stage=rejection_stage,
            rejection_reason=reason,
            current_price=None,  # Could pass bid_price if available
        )
        
        self.rejected_signals.append(rejected)

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

        if action == "OPEN" or action == "REJECT":
            self._reject_signal(
                timestamp, 
                direction, 
                signal_id, 
                "Risk rejected", 
                verbose,
                rejection_stage="RISK"
            )
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
        """Close positions due to opposite signal
        
        SESSION 10: Works with Trade contracts
        """
        spread = (
            self.spread_manager.get_spread_in_points(current_bid)
            if self.spread_manager
            else 0.0
        )

        for tid in close_trade_ids:
            # Find Trade contract by trade_manager_id
            trade = next(
                (
                    t for t in self.all_trades
                    if t.is_open and t.entry.trade_manager_id == tid
                ),
                None,
            )
            if trade:
                exit_price = (
                    current_bid if trade.entry.is_long 
                    else current_bid + spread
                )

                # Create TradeExit for opposite signal
                trade_exit = TradeExit.create(
                    entry=trade.entry,
                    exit_time=timestamp,
                    exit_price=exit_price,
                    exit_reason=ExitReason.OPPOSITE_SIGNAL,
                )

                # Update Trade
                updated_trade = Trade(entry=trade.entry, exit=trade_exit)
                
                # Replace in list
                for i, t in enumerate(self.all_trades):
                    if t.entry.entry_id == trade.entry.entry_id:
                        self.all_trades[i] = updated_trade
                        break

                exit_stats["OPPOSITE_SIGNAL"] = exit_stats.get("OPPOSITE_SIGNAL", 0) + 1

                if verbose:
                    logger.debug(
                        f"[CLOSE] {timestamp} {trade.direction} OPPOSITE at {exit_price:.2f}"
                    )

    # ------------------------------------------------------------------ #
    # Open new position
    # ------------------------------------------------------------------ #
    def _handle_open(
        self,
        timestamp: pd.Timestamp,
        direction: str,
        params: TradeParameters,
        new_trade_id: int,
        verbose: bool,
        comment_suffix: str = "",
        signal_id: int = None,
    ):
        """
        Open new position.
        
        SESSION 10: Creates TradeEntry contract and Trade object
        """
        self.trade_counter += 1

        # Create TradeEntry contract from TradeParameters
        entry = TradeEntry.from_trade_parameters(
            entry_id=f"E{self.trade_counter}",
            timestamp=timestamp,
            direction=TradeDirection.from_string(direction),
            params=params,
        )
        
        # Update with additional metadata
        # Note: We need to create a new contract with updated comment
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

        # Create Trade contract (open, no exit yet)
        trade = Trade(entry=entry_with_metadata, exit=None)
        self.all_trades.append(trade)
        
        # Register position in TradeManager
        self.trade_manager.open_position(
            trade_id=new_trade_id,
            timestamp=timestamp,
            direction=TradeDirection.from_string(direction),
            entry_price=params.entry_price_executed,
            stop_loss=params.stop_loss_trigger,
            take_profit=params.take_profit,
            position_size=params.position_size,
            meta={'signal_id': signal_id} if signal_id else None
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

    # ------------------------------------------------------------------ #
    # Close remaining positions at end of backtest
    # ------------------------------------------------------------------ #
    def _close_remaining_positions(
        self,
        df_strategy: pd.DataFrame,
        exit_stats: Dict,
        verbose: bool,
    ):
        """Close all remaining open positions at end of backtest
        
        SESSION 10: Works with Trade contracts
        """
        if df_strategy.empty:
            return

        last_timestamp = df_strategy.index[-1]
        last_bid = df_strategy.iloc[-1]["close"]
        spread = (
            self.spread_manager.get_spread_in_points(last_bid)
            if self.spread_manager
            else 0.0
        )

        for trade in [t for t in self.all_trades if t.is_open]:
            exit_price = (
                last_bid if trade.entry.is_long 
                else last_bid + spread
            )

            # Create TradeExit for end of data
            trade_exit = TradeExit.create(
                entry=trade.entry,
                exit_time=last_timestamp,
                exit_price=exit_price,
                exit_reason=ExitReason.END_OF_DATA,
            )

            # Update Trade
            updated_trade = Trade(entry=trade.entry, exit=trade_exit)
            
            # Replace in list
            for i, t in enumerate(self.all_trades):
                if t.entry.entry_id == trade.entry.entry_id:
                    self.all_trades[i] = updated_trade
                    break

            exit_stats["END_OF_DATA"] = exit_stats.get("END_OF_DATA", 0) + 1

            if verbose:
                logger.debug(
                    f"[FORCE CLOSE] {last_timestamp} {trade.direction} END_OF_DATA at {exit_price:.2f}"
                )
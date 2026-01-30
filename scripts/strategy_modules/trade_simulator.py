"""Trade simulation with LTF OHLC execution"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from collections import defaultdict
import time
import logging

logger = logging.getLogger(__name__)

from .trade_tracker import TradeTracker
from src.strategies.trade_management.risk_manager import RiskManager
from src.strategies.trade_management.spread_manager import SpreadManager

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
        logger.info("="*60)
        logger.info("TRADE SIMULATOR PROFILING REPORT")
        logger.info("="*60)
        for name, times in self.timings.items():
            total = sum(times)
            avg = total / len(times) if times else 0
            logger.info(f"{name:30s}: {total:.3f}s total, {avg:.3f}s avg, {len(times)} calls")

class TradeSimulator:
    """
    Trade simulator with LTF (1-second) OHLC execution for realistic SL/TP triggers.
    ID System:
    - trade_id: TradeTracker internal sequential ID (for CSV export)
    - trade_manager_trade_id: TradeManager position ID (for position logic)
    """
    
    def __init__(self, config: Dict):
        self.config = config
        self.profile_enabled = config.get('debug', {}).get('profile_simulator', False)
        
        self.trade_tracker = TradeTracker()
        self.trade_manager = None
        self.spread_manager = None
        self.progressive_tracker = None
        self.df_ltf = None
        self._ltf_windows: Dict = {}
        
        self.initialize_managers()
        
        if self.profile_enabled:
            self.profiler = TradeSimulatorProfiler()
            self._check_exits_with_ltf_ohlc = self.profiler.profile("check_exits_ltf")(
                self._check_exits_with_ltf_ohlc
            )
        else:
            self.profiler = None

    def initialize_managers(self):
        """Initialize trade manager and spread manager"""
        from src.strategies.trade_management.trade_manager import TradeManager
        self.trade_manager = TradeManager(self.config)
        self.trade_tracker.set_trade_manager(self.trade_manager)
        
        tm_config = self.config.get('trade_management', {})
        spread_config = tm_config.get('spread', {})
        if spread_config.get('enabled', False):
            asset_symbol = self.config.get('asset', {}).get('symbol', '')
            config_path = spread_config.get('config_path')
            self.spread_manager = SpreadManager(asset_symbol, config_path)

    def _precompute_ltf_windows(self, df_strategy: pd.DataFrame) -> None:
        """Pre-compute LTF windows for each strategy bar for fast lookup"""
        if self.df_ltf is None or self.df_ltf.empty:
            raise ValueError("LTF data required for simulation but not provided")
        
        self._ltf_windows = {}
        for strategy_time in df_strategy.index:
            window_end = strategy_time + pd.Timedelta(minutes=1)
            mask = (self.df_ltf.index >= strategy_time) & (self.df_ltf.index < window_end)
            window_bars = self.df_ltf[mask]
            if not window_bars.empty:
                self._ltf_windows[strategy_time] = {
                    'min_low': window_bars['low'].min(),
                    'max_high': window_bars['high'].max(),
                    'bars': window_bars
                }

    def _find_exact_exit_bar(self, trade: Dict, window_bars: pd.DataFrame, 
                             exit_reason: str, is_long: bool) -> tuple[Optional[pd.Series], Optional[float]]:
        """Find exact LTF bar where SL/TP was hit and calculate exit price"""
        if window_bars.empty:
            return None, None
        
        # Determine which price level to check
        if is_long:
            hit_mask = window_bars['low'] <= trade['sl_price'] if exit_reason == 'STOP_LOSS' else window_bars['high'] >= trade['tp_price']
        else:
            hit_mask = window_bars['high'] >= trade['sl_price'] if exit_reason == 'STOP_LOSS' else window_bars['low'] <= trade['tp_price']
        
        if not hit_mask.any():
            return None, None
        
        # Find first bar where condition met
        exit_idx = hit_mask.idxmax()
        exit_bar = window_bars.loc[exit_idx]
        
        # Calculate actual exit price (best case within bar range)
        if is_long:
            exit_price = min(exit_bar['low'], trade['sl_price']) if exit_reason == 'STOP_LOSS' else min(exit_bar['high'], trade['tp_price'])
        else:
            exit_price = max(exit_bar['high'], trade['sl_price']) if exit_reason == 'STOP_LOSS' else max(exit_bar['low'], trade['tp_price'])
        
        return exit_bar, exit_price

    def _execute_trade_exit(self, trade: Dict, exit_bar: pd.Series, exit_price: float, 
                            exit_reason: str, exit_stats: Dict, verbose: bool):
        """Execute trade exit and update all tracking"""
        # Calculate P&L
        if trade['direction'] == 'BUY':
            pnl_points = exit_price - trade['entry_price']
        else:
            pnl_points = trade['entry_price'] - exit_price
        
        pnl_percent = (pnl_points / trade['entry_price']) * 100 if trade['entry_price'] else 0
        
        entry_time = trade.get('entry_time') or trade.get('timestamp')
        duration_minutes = (exit_bar.name - entry_time).total_seconds() / 60 if entry_time else None
        
        # Update trade tracker
        self.trade_tracker.close_position(
            trade['trade_id'], exit_bar.name, exit_price, exit_reason, None
        )
        
        # Update trade manager
        if trade.get('trade_manager_trade_id'):
            self.trade_manager.close_positions([trade['trade_manager_trade_id']])
        
        exit_stats[exit_reason] += 1
        
        # Update progressive tracker
        if self.progressive_tracker and 'signal_id' in trade and trade['signal_id']:
            self.progressive_tracker.update_trade_execution_details(
                trade['signal_id'],
                trade_id=trade['trade_id'],
                exit_time=exit_bar.name,
                exit_price=exit_price,
                exit_reason=exit_reason,
                pnl_points=pnl_points,
                pnl_percent=pnl_percent,
                duration_minutes=duration_minutes,
                is_win=pnl_points > 0,
                is_loss=pnl_points < 0,
                exit_check_high=exit_bar['high'],
                exit_check_low=exit_bar['low'],
                reason=f'Trade closed ({exit_reason}) - BID OHLC execution'
            )
        
        if verbose:
            theoretical = trade['sl_price'] if exit_reason == 'STOP_LOSS' else trade['tp_price']
            diff = exit_price - theoretical
            sign = '+' if diff > 0 else ''
            logger.debug(f"[EXIT-LTF] {exit_bar.name} {trade['direction']} {exit_reason} | "
                        f"Actual: {exit_price:.5f} ({sign}{diff:.5f}) | P&L: {pnl_points:+.2f} pts")

    def _check_exits_with_ltf_ohlc(self, strategy_timestamp: pd.Timestamp, 
                                   exit_stats: Dict, verbose: bool):
        """Check for SL/TP exits using vectorized LTF OHLC data"""
        if strategy_timestamp not in self._ltf_windows:
            return
        
        window = self._ltf_windows[strategy_timestamp]
        if window['bars'].empty or not self.trade_tracker.get_open_trades():
            return
        
        # Filter trades that entered before current bar
        open_trades = [t for t in self.trade_tracker.get_open_trades() 
                       if (t.get('entry_time') or t.get('timestamp')) < strategy_timestamp]
        
        if not open_trades:
            return
        
        # Separate LONG and SHORT trades for vectorized checking
        long_trades = [t for t in open_trades if t['direction'] == 'BUY']
        short_trades = [t for t in open_trades if t['direction'] == 'SELL']
        
        # Process LONG trades (vectorized)
        if long_trades:
            sl_prices = np.array([t['sl_price'] for t in long_trades])
            tp_prices = np.array([t['tp_price'] for t in long_trades])
            sl_hit = window['min_low'] <= sl_prices
            tp_hit = window['max_high'] >= tp_prices
            exit_mask = sl_hit | tp_hit
            reasons = np.where(sl_hit, 'STOP_LOSS', np.where(tp_hit, 'TAKE_PROFIT', None))
            
            for idx in np.where(exit_mask)[0]:
                trade = long_trades[idx]
                reason = reasons[idx]
                if reason:
                    bar, price = self._find_exact_exit_bar(trade, window['bars'], reason, True)
                    if bar is not None:
                        self._execute_trade_exit(trade, bar, price, reason, exit_stats, verbose)
        
        # Process SHORT trades (vectorized)
        if short_trades:
            sl_prices = np.array([t['sl_price'] for t in short_trades])
            tp_prices = np.array([t['tp_price'] for t in short_trades])
            sl_hit = window['max_high'] >= sl_prices
            tp_hit = window['min_low'] <= tp_prices
            exit_mask = sl_hit | tp_hit
            reasons = np.where(sl_hit, 'STOP_LOSS', np.where(tp_hit, 'TAKE_PROFIT', None))
            
            for idx in np.where(exit_mask)[0]:
                trade = short_trades[idx]
                reason = reasons[idx]
                if reason:
                    bar, price = self._find_exact_exit_bar(trade, window['bars'], reason, False)
                    if bar is not None:
                        self._execute_trade_exit(trade, bar, price, reason, exit_stats, verbose)
    
    def simulate_trades(self, df_strategy: pd.DataFrame, filtered_signals: pd.Series, 
                        verbose: bool = False, progressive_tracker=None, risk_manager: RiskManager = None,
                        signal_id_map: Dict = None, df_ltf: Optional[pd.DataFrame] = None) -> Dict:
        """
        Simulate trades with realistic LTF execution.        
        """
        if df_ltf is None or df_ltf.empty:
            raise ValueError("LTF (1-second) data is mandatory for realistic execution simulation")

        self.progressive_tracker = progressive_tracker
        self.df_ltf = df_ltf

        # Convert to float32 for memory efficiency in batch runs
        ohlc_cols = ['open', 'high', 'low', 'close']
        for df in [self.df_ltf, df_strategy]:
            available_cols = [c for c in ohlc_cols if c in df.columns]
            if available_cols:
                df[available_cols] = df[available_cols].astype('float32')
            if 'volume' in df.columns and df['volume'].dtype == 'float64':
                df['volume'] = df['volume'].astype('float32')

        if verbose:
            logger.info(f"LTF Execution: {len(df_ltf):,} bars (float32 optimized)")

        # Pre-compute LTF windows for fast lookup
        self._precompute_ltf_windows(df_strategy)
        if verbose:
            logger.info(f"Pre-computed {len(self._ltf_windows):,} LTF windows")
        
        # Initialize statistics
        position_rejected_count = {'buy': 0, 'sell': 0}
        exit_stats = {'STOP_LOSS': 0, 'TAKE_PROFIT': 0, 'OPPOSITE_SIGNAL': 0, 'END_OF_DATA': 0}
        risk_stats = {
            'approved': {'buy': 0, 'sell': 0}, 
            'rejected': {'buy': 0, 'sell': 0}, 
            'adjusted': {'buy': 0, 'sell': 0},
            'total_approved': 0, 
            'total_rejected': 0, 
            'total_adjusted': 0
        }
        
        # Main simulation loop
        for timestamp, row in zip(df_strategy.index, df_strategy.itertuples(index=False)):
            # Check for exits first
            self._check_exits_with_ltf_ohlc(timestamp, exit_stats, verbose)
            
            # Process new signals
            if timestamp in filtered_signals.index and pd.notna(filtered_signals[timestamp]):
                signal_type = filtered_signals[timestamp]
                is_long = (signal_type == 'BUY')
                direction = 'BUY' if is_long else 'SELL'
                bid_price = row.close
                signal_id = signal_id_map.get(timestamp) if signal_id_map else None
                
                # Ask trade manager what to do with this signal
                result = self.trade_manager.handle_signal(timestamp, signal_type)
                
                # Update progressive tracker with position management details
                if self.progressive_tracker and signal_id:
                    needs_open = result['action'] in ['OPEN', 'CLOSE_AND_REVERSE']
                    self.progressive_tracker.update_position_management_details(
                        signal_id, result['action'], result['reason'],
                        self.trade_manager.current_direction,
                        len(self.trade_manager.current_positions),
                        self.trade_manager.pyramiding_enabled,
                        self.trade_manager.close_on_opposite,
                        needs_open
                    )
                
                # Handle REJECT action
                if result['action'] == 'REJECT':
                    self.trade_tracker.reject_signal(timestamp, direction, None, None, None, 
                                                     result.get('reason', 'Unknown'), '')
                    position_rejected_count['buy' if is_long else 'sell'] += 1
                    if verbose:
                        logger.debug(f"[REJECT] {timestamp} {direction} - {result.get('reason', 'Unknown')}")
                    continue
                
                # Calculate trade parameters if opening position
                needs_open = result['action'] in ['OPEN', 'CLOSE_AND_REVERSE']
                params = None
                if needs_open:
                    params = risk_manager.compute_trade_parameters(timestamp, bid_price, is_long)
                    if params is None:
                        # Risk validation rejected
                        key = 'buy' if is_long else 'sell'
                        risk_stats['rejected'][key] += 1
                        risk_stats['total_rejected'] += 1
                        
                        if self.progressive_tracker and signal_id:
                            self.progressive_tracker.update_risk_management_details(
                                signal_id, False, 'Risk validation failed'
                            )
                        
                        if result['action'] == 'OPEN':
                            self.trade_tracker.reject_signal(timestamp, direction, None, None, None, 
                                                           'Risk rejected', '')
                            if verbose:
                                logger.debug(f"[REJECT] {timestamp} {direction} - Risk rejected")
                            continue
                        elif result['action'] == 'CLOSE_AND_REVERSE':
                            # Close existing but don't open new
                            self._handle_close(timestamp, result.get('close_trade_ids', []), row, verbose)
                            self.trade_manager.close_positions(result.get('close_trade_ids', []))
                            if verbose:
                                logger.debug(f"[CLOSE ONLY] {timestamp} {direction} - Risk rejected new position")
                            continue
                    
                    # Risk approved - update stats
                    key = 'buy' if is_long else 'sell'
                    risk_stats['approved'][key] += 1
                    risk_stats['total_approved'] += 1
                    if params['sl_adjusted']:
                        risk_stats['adjusted'][key] += 1
                        risk_stats['total_adjusted'] += 1
                    
                    if self.progressive_tracker and signal_id:
                        self.progressive_tracker.update_risk_management_details(
                            signal_id, True, params['comment']
                        )
                
                # Execute CLOSE_AND_REVERSE
                if result['action'] == 'CLOSE_AND_REVERSE':
                    self._handle_close(timestamp, result.get('close_trade_ids', []), row, verbose)
                    self.trade_manager.close_positions(result.get('close_trade_ids', []))
                    if params:
                        self._handle_open(timestamp, direction, params, result['new_trade_id'], 
                                        verbose, '(Reversal)', signal_id)
                
                # Execute OPEN
                elif result['action'] == 'OPEN':
                    if params:
                        self._handle_open(timestamp, direction, params, result['new_trade_id'], 
                                        verbose, '', signal_id)
        
        # Close remaining open positions at end of data
        self._close_remaining_positions(df_strategy, exit_stats, verbose)
        
        if verbose and self.profiler:
            self.profiler.print_report()
        
        return {
            'all_trades': self.trade_tracker.get_trades(),
            'closed_trades': self.trade_tracker.get_closed_trades(),
            'open_trades': self.trade_tracker.get_open_trades(),
            'rejected_trades': self.trade_tracker.get_rejected_trades(),
            'exit_stats': exit_stats,
            'position_rejected_count': position_rejected_count,
            'risk_stats': risk_stats,
            'trade_manager_metrics': self.trade_manager.get_metrics(),
            'execution_mode': 'LTF_OHLC_VECTORIZED'
        }

    def _handle_close(self, timestamp: pd.Timestamp, close_trade_ids: List[int], 
                     row: pd.Series, verbose: bool):
        """Close positions due to opposite signal"""
        current_bid = row['close']
        spread = self.spread_manager.get_spread_in_points(current_bid) if self.spread_manager else 0.0
        
        for tid in close_trade_ids:
            track_trade = next((t for t in self.trade_tracker.get_open_trades() 
                              if t['trade_manager_trade_id'] == tid), None)
            if track_trade:
                exit_price = current_bid if track_trade['direction'] == 'BUY' else current_bid + spread
                self.trade_tracker.close_position(track_trade['trade_id'], timestamp, 
                                                exit_price, 'OPPOSITE_SIGNAL', None)
                if verbose:
                    logger.debug(f"[CLOSE] {timestamp} {track_trade['direction']} OPPOSITE at {exit_price:.2f}")

    def _handle_open(self, timestamp: pd.Timestamp, direction: str, params: Dict, 
                    new_trade_id: int, verbose: bool, comment_suffix: str = '', signal_id: int = None):
        """Open new position"""
        self.trade_tracker.open_position(
            timestamp=timestamp, direction=direction, entry_price=params['executed_entry'],
            sl_price=params['trigger_sl'], tp_price=params['tp'],
            comment=params['comment'] + comment_suffix,
            trade_manager_action='OPEN', trade_manager_trade_id=new_trade_id, signal_id=signal_id
        )
        self.trade_manager.open_position(new_trade_id, timestamp, direction)
        
        if self.progressive_tracker and signal_id:
            self.progressive_tracker.update_trade_execution_details(
                signal_id, trade_id=new_trade_id, entry_time=timestamp,
                entry_price_executed=params['executed_entry'],
                sl_price_executed=params['trigger_sl'], tp_price_executed=params['tp'],
                reason='Trade opened' + comment_suffix
            )
        
        if verbose:
            logger.debug(f"[OPEN] {timestamp} {direction} at {params['executed_entry']:.2f}{comment_suffix}")

    def _close_remaining_positions(self, df_strategy: pd.DataFrame, exit_stats: Dict, verbose: bool):
        """Close all remaining open positions at end of backtest period"""
        if df_strategy.empty:
            return
            
        last_timestamp = df_strategy.index[-1]
        last_bid = df_strategy.iloc[-1]['close']
        spread = self.spread_manager.get_spread_in_points(last_bid) if self.spread_manager else 0.0
        
        for open_trade in list(self.trade_tracker.get_open_trades()):
            exit_price = last_bid if open_trade['direction'] == 'BUY' else last_bid + spread
            self.trade_tracker.close_position(open_trade['trade_id'], last_timestamp, 
                                            exit_price, 'END_OF_DATA', df_strategy)
            
            if 'trade_manager_trade_id' in open_trade:
                self.trade_manager.close_positions([open_trade['trade_manager_trade_id']])
            
            exit_stats['END_OF_DATA'] += 1
            
            if verbose:
                logger.debug(f"[CLOSE] End of data {open_trade['direction']} at {exit_price:.2f}")
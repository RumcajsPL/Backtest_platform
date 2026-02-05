"""Trade simulation with LTF OHLC execution - Production Optimized"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from collections import defaultdict
import time
import logging

logger = logging.getLogger(__name__)

from src.strategies.trade_management.risk_manager import RiskManager
from src.strategies.trade_management.spread_manager import SpreadManager
from src.strategies.core.null_progressive_tracker import NullProgressiveTracker

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
    Trade simulator with LTF OHLC execution for realistic SL/TP triggers.
    Direct trade tracking - no intermediate TradeTracker layer.
    """
    
    def __init__(self, config: Dict, df_full: pd.DataFrame):
        self.config = config
        self.df_full = df_full
        self.profile_enabled = config.get('debug', {}).get('profile_simulator', False)
        
        # Direct trade tracking (replaces TradeTracker)
        self.all_trades: List[Dict] = []
        self.trade_counter = 0
        
        self.trade_manager = None
        self.spread_manager = None
        self.progressive_tracker = None
        self._tracking_enabled = False
        self.df_ltf = None
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

    def initialize_managers(self):
        """Initialize trade manager and spread manager"""
        from src.strategies.trade_management.trade_manager import TradeManager
        self.trade_manager = TradeManager(self.config)
        
        tm_config = self.config.get('trade_management', {})
        spread_config = tm_config.get('spread', {})
        if spread_config.get('enabled', False):
            asset_symbol = self.config.get('asset', {}).get('symbol', '')
            config_path = spread_config.get('config_path')
            self.spread_manager = SpreadManager(asset_symbol, config_path)

    def _precompute_ltf_windows(self, df_strategy: pd.DataFrame) -> None:
        """Pre-compute LTF windows for each strategy bar"""
        if self.df_ltf is None or self.df_ltf.empty:
            logger.error("EXECUTION ABORTED: LTF data missing")
            logger.error("Required: 1-second OHLCV data for realistic SL/TP execution")
            logger.error("Check config: paths.ltf_ohlcv_file")
            raise ValueError(
                "LTF execution data missing. "
                "Verify config paths.ltf_ohlcv_file points to valid 1-second OHLCV data."
            )
        
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
        """Find exact LTF bar where SL/TP was hit"""
        if window_bars.empty:
            return None, None
        
        if is_long:
            hit_mask = window_bars['low'] <= trade['sl_price'] if exit_reason == 'STOP_LOSS' else window_bars['high'] >= trade['tp_price']
        else:
            hit_mask = window_bars['high'] >= trade['sl_price'] if exit_reason == 'STOP_LOSS' else window_bars['low'] <= trade['tp_price']
        
        if not hit_mask.any():
            return None, None
        
        exit_idx = hit_mask.idxmax()
        exit_bar = window_bars.loc[exit_idx]
        
        # Calculate actual exit price
        if is_long:
            exit_price = min(exit_bar['low'], trade['sl_price']) if exit_reason == 'STOP_LOSS' else min(exit_bar['high'], trade['tp_price'])
        else:
            exit_price = max(exit_bar['high'], trade['sl_price']) if exit_reason == 'STOP_LOSS' else max(exit_bar['low'], trade['tp_price'])
        
        return exit_bar, exit_price

    def _execute_trade_exit(self, trade: Dict, exit_bar: pd.Series, exit_price: float, 
                            exit_reason: str, exit_stats: Dict, verbose: bool):
        """Execute trade exit and update tracking"""
        # Calculate P&L
        if trade['direction'] == 'BUY':
            pnl_points = exit_price - trade['entry_price']
        else:
            pnl_points = trade['entry_price'] - exit_price
        
        pnl_percent = (pnl_points / trade['entry_price']) * 100 if trade['entry_price'] else 0
        
        entry_time = trade.get('entry_time') or trade.get('timestamp')
        duration_minutes = (exit_bar.name - entry_time).total_seconds() / 60 if entry_time else None
        
        # Update trade record directly
        trade['status'] = 'CLOSED'
        trade['exit_time'] = exit_bar.name
        trade['exit_price'] = exit_price
        trade['exit_reason'] = exit_reason
        trade['pnl_points'] = pnl_points
        trade['pnl_percent'] = pnl_percent
        trade['duration_minutes'] = duration_minutes
        trade['is_win'] = pnl_points > 0
        trade['is_loss'] = pnl_points < 0
        
        # Update trade manager state
        if trade.get('trade_manager_trade_id'):
            self.trade_manager.close_positions([trade['trade_manager_trade_id']])
        
        exit_stats[exit_reason] += 1
        
        # Update progressive tracker
        if self._tracking_enabled and trade.get('signal_id'):
            self.progressive_tracker.update_trade_execution_details(
                signal_id=trade['signal_id'],
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
                reason=f'CLOSED: {exit_reason}'
            )
        
        if verbose:
            theoretical = trade['sl_price'] if exit_reason == 'STOP_LOSS' else trade['tp_price']
            diff = exit_price - theoretical
            sign = '+' if diff > 0 else ''
            logger.debug(f"[EXIT-LTF] {exit_bar.name} {trade['direction']} {exit_reason} | "
                        f"Actual: {exit_price:.5f} ({sign}{diff:.5f}) | P&L: {pnl_points:+.2f} pts")

    def _check_exits_with_ltf_ohlc(self, strategy_timestamp: pd.Timestamp, 
                                   exit_stats: Dict, verbose: bool):
        """Check for SL/TP exits using vectorized LTF OHLC"""
        if strategy_timestamp not in self._ltf_windows:
            return
        
        window = self._ltf_windows[strategy_timestamp]
        open_trades = [t for t in self.all_trades if t['status'] == 'OPEN']
        
        if window['bars'].empty or not open_trades:
            return
        
        # Filter trades that entered before current bar
        open_trades = [t for t in open_trades if (t.get('entry_time') or t.get('timestamp')) < strategy_timestamp]
        
        if not open_trades:
            return
        
        # Separate LONG and SHORT for vectorized checking
        long_trades = [t for t in open_trades if t['direction'] == 'BUY']
        short_trades = [t for t in open_trades if t['direction'] == 'SELL']
        
        # Process LONG trades
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
        
        # Process SHORT trades
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
                        verbose: bool = False, progressive_tracker=None,
                        signal_id_map: Dict = None, df_ltf: Optional[pd.DataFrame] = None) -> Dict:
        """Simulate trades with realistic LTF execution"""
        
        # Mandatory LTF check
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
            progressive_tracker is not None and 
            not isinstance(progressive_tracker, NullProgressiveTracker)
        )
        self.df_ltf = df_ltf

        # Memory optimization: convert to float32
        ohlc_cols = ['open', 'high', 'low', 'close']
        for df in [self.df_ltf, df_strategy]:
            available_cols = [c for c in ohlc_cols if c in df.columns]
            if available_cols:
                df[available_cols] = df[available_cols].astype('float32')
            if 'volume' in df.columns and df['volume'].dtype == 'float64':
                df['volume'] = df['volume'].astype('float32')

        if verbose:
            logger.info(f"LTF Execution: {len(df_ltf):,} bars (float32 optimized)")

        # Pre-compute LTF windows
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
            # Check exits first
            self._check_exits_with_ltf_ohlc(timestamp, exit_stats, verbose)
            
            # Process new signals
            if timestamp not in filtered_signals.index or pd.isna(filtered_signals[timestamp]):
                continue
            
            signal_type = filtered_signals[timestamp]
            is_long = (signal_type == 'BUY')
            direction = 'BUY' if is_long else 'SELL'
            bid_price = row.close
            signal_id = signal_id_map.get(timestamp) if signal_id_map else None
            
            # Get position management decision
            result = self.trade_manager.handle_signal(timestamp, signal_type)
            
            # Update progressive tracker with position management
            if self._tracking_enabled and signal_id:
                needs_open = result['action'] in ['OPEN', 'CLOSE_AND_REVERSE']
                self.progressive_tracker.update_position_management_details(
                    signal_id, result['action'], result['reason'],
                    self.trade_manager.current_direction,
                    len(self.trade_manager.current_positions),
                    self.trade_manager.pyramiding_enabled,
                    self.trade_manager.close_on_opposite,
                    needs_open
                )
            
            # Handle REJECT
            if result['action'] == 'REJECT':
                self._reject_signal(timestamp, direction, signal_id, result.get('reason', 'Unknown'), verbose)
                position_rejected_count['buy' if is_long else 'sell'] += 1
                continue
            
            # Calculate trade parameters if opening
            needs_open = result['action'] in ['OPEN', 'CLOSE_AND_REVERSE']
            params = None
            if needs_open:
                params = self.risk_manager.compute_trade_parameters(timestamp, bid_price, is_long)
                if params is None:
                    # Risk rejected
                    self._handle_risk_rejection(
                        result['action'], result.get('close_trade_ids', []),
                        timestamp, direction, signal_id, is_long,
                        risk_stats, position_rejected_count, row, verbose
                    )
                    continue
                
                # Risk approved
                key = 'buy' if is_long else 'sell'
                risk_stats['approved'][key] += 1
                risk_stats['total_approved'] += 1
                if params['sl_adjusted']:
                    risk_stats['adjusted'][key] += 1
                    risk_stats['total_adjusted'] += 1
                
                if self._tracking_enabled and signal_id:
                    self.progressive_tracker.update_risk_management_details(
                        signal_id, True, params['comment']
                    )
            
            # Execute CLOSE_AND_REVERSE
            if result['action'] == 'CLOSE_AND_REVERSE':
                self._handle_close(timestamp, result.get('close_trade_ids', []), row, exit_stats, verbose)
                self.trade_manager.close_positions(result.get('close_trade_ids', []))
                if params:
                    self._handle_open(timestamp, direction, params, result['new_trade_id'], 
                                    verbose, '(Reversal)', signal_id)
            
            # Execute OPEN
            elif result['action'] == 'OPEN':
                if params:
                    self._handle_open(timestamp, direction, params, result['new_trade_id'], 
                                    verbose, '', signal_id)
        
        # Close remaining positions
        self._close_remaining_positions(df_strategy, exit_stats, verbose)
        
        if verbose and self.profiler:
            self.profiler.print_report()
        
        # Return results
        closed_trades = [t for t in self.all_trades if t['status'] == 'CLOSED']
        open_trades = [t for t in self.all_trades if t['status'] == 'OPEN']
        rejected_trades = [t for t in self.all_trades if t['status'] == 'REJECTED']
        
        return {
            'all_trades': self.all_trades,
            'closed_trades': closed_trades,
            'open_trades': open_trades,
            'rejected_trades': rejected_trades,
            'exit_stats': exit_stats,
            'position_rejected_count': position_rejected_count,
            'risk_stats': risk_stats,
            'trade_manager_metrics': self.trade_manager.get_metrics(),
            'execution_mode': 'LTF_OHLC_VECTORIZED'
        }

    def _reject_signal(self, timestamp: pd.Timestamp, direction: str, signal_id: Optional[int],
                      reason: str, verbose: bool):
        """Record rejected signal"""
        self.trade_counter += 1
        trade = {
            'trade_id': self.trade_counter,
            'trade_manager_trade_id': None,
            'position_id': None,
            'status': 'REJECTED',
            'entry_time': timestamp,
            'exit_time': None,
            'direction': direction,
            'entry_price': None,
            'exit_price': None,
            'sl_price': None,
            'tp_price': None,
            'exit_reason': None,
            'pnl_points': 0,
            'pnl_percent': 0,
            'duration_bars': 0,
            'duration_minutes': 0,
            'sl_distance': 0,
            'tp_distance': 0,
            'risk_reward_ratio': 0,
            'is_win': False,
            'is_loss': False,
            'comment': f'Rejected: {reason}',
            'reject_reason': reason,
            'signal_id': signal_id,
        }
        self.all_trades.append(trade)
        
        if verbose:
            logger.debug(f"[REJECT] {timestamp} {direction} - {reason}")

    def _handle_risk_rejection(self, action: str, close_trade_ids: List[int],
                               timestamp: pd.Timestamp, direction: str, signal_id: Optional[int],
                               is_long: bool, risk_stats: Dict, position_rejected_count: Dict,
                               row: pd.Series, verbose: bool):
        """Handle risk rejection scenarios"""
        key = 'buy' if is_long else 'sell'
        risk_stats['rejected'][key] += 1
        risk_stats['total_rejected'] += 1
        
        if self._tracking_enabled and signal_id:
            self.progressive_tracker.update_risk_management_details(
                signal_id, False, 'Risk validation failed'
            )
        
        if action == 'OPEN':
            self._reject_signal(timestamp, direction, signal_id, 'Risk rejected', verbose)
        elif action == 'CLOSE_AND_REVERSE':
            # Close existing but don't open new
            self._handle_close(timestamp, close_trade_ids, row, {}, verbose)
            self.trade_manager.close_positions(close_trade_ids)
            if verbose:
                logger.debug(f"[CLOSE ONLY] {timestamp} {direction} - Risk rejected new position")

    def _handle_close(self, timestamp: pd.Timestamp, close_trade_ids: List[int], 
                     row: pd.Series, exit_stats: Dict, verbose: bool):
        """Close positions due to opposite signal"""
        current_bid = row['close']
        spread = self.spread_manager.get_spread_in_points(current_bid) if self.spread_manager else 0.0
        
        for tid in close_trade_ids:
            trade = next((t for t in self.all_trades 
                         if t['status'] == 'OPEN' and t.get('trade_manager_trade_id') == tid), None)
            if trade:
                exit_price = current_bid if trade['direction'] == 'BUY' else current_bid + spread
                
                # Calculate P&L
                pnl_points = exit_price - trade['entry_price'] if trade['direction'] == 'BUY' else trade['entry_price'] - exit_price
                
                # Update trade
                trade['status'] = 'CLOSED'
                trade['exit_time'] = timestamp
                trade['exit_price'] = exit_price
                trade['exit_reason'] = 'OPPOSITE_SIGNAL'
                trade['pnl_points'] = pnl_points
                trade['pnl_percent'] = (pnl_points / trade['entry_price']) * 100 if trade['entry_price'] else 0
                trade['is_win'] = pnl_points > 0
                trade['is_loss'] = pnl_points < 0
                
                exit_stats['OPPOSITE_SIGNAL'] = exit_stats.get('OPPOSITE_SIGNAL', 0) + 1
                
                if verbose:
                    logger.debug(f"[CLOSE] {timestamp} {trade['direction']} OPPOSITE at {exit_price:.2f}")

    def _handle_open(self, timestamp: pd.Timestamp, direction: str, params: Dict, 
                    new_trade_id: int, verbose: bool, comment_suffix: str = '', signal_id: int = None):
        """Open new position"""
        self.trade_counter += 1
        
        trade = {
            'trade_id': self.trade_counter,
            'trade_manager_trade_id': new_trade_id,
            'position_id': new_trade_id,  # Simplified
            'status': 'OPEN',
            'entry_time': timestamp,
            'exit_time': None,
            'direction': direction,
            'entry_price': params['executed_entry'],
            'exit_price': None,
            'sl_price': params['trigger_sl'],
            'tp_price': params['tp'],
            'exit_reason': None,
            'pnl_points': 0,
            'pnl_percent': 0,
            'duration_bars': 0,
            'duration_minutes': 0,
            'sl_distance': abs(params['executed_entry'] - params['trigger_sl']),
            'tp_distance': abs(params['tp'] - params['executed_entry']),
            'risk_reward_ratio': abs(params['tp'] - params['executed_entry']) / abs(params['executed_entry'] - params['trigger_sl']) if abs(params['executed_entry'] - params['trigger_sl']) > 0 else 0,
            'is_win': False,
            'is_loss': False,
            'comment': params['comment'] + comment_suffix,
            'reject_reason': None,
            'signal_id': signal_id,
        }
        
        self.all_trades.append(trade)
        self.trade_manager.open_position(new_trade_id, timestamp, direction)
        
        if self._tracking_enabled and signal_id:
            self.progressive_tracker.update_trade_execution_details(
                signal_id, trade_id=self.trade_counter, entry_time=timestamp,
                entry_price_executed=params['executed_entry'],
                sl_price_executed=params['trigger_sl'], tp_price_executed=params['tp'],
                reason='OPENED' + comment_suffix
            )
        
        if verbose:
            logger.debug(f"[OPEN] {timestamp} {direction} at {params['executed_entry']:.2f}{comment_suffix}")

    def _close_remaining_positions(self, df_strategy: pd.DataFrame, exit_stats: Dict, verbose: bool):
        """Close all remaining open positions at end of backtest"""
        if df_strategy.empty:
            return
            
        last_timestamp = df_strategy.index[-1]
        last_bid = df_strategy.iloc[-1]['close']
        spread = self.spread_manager.get_spread_in_points(last_bid) if self.spread_manager else 0.0
        
        for trade in [t for t in self.all_trades if t['status'] == 'OPEN']:
            exit_price = last_bid if trade['direction'] == 'BUY' else last_bid + spread
            
            # Calculate P&L
            pnl_points = exit_price - trade['entry_price'] if trade['direction'] == 'BUY' else trade['entry_price'] - exit_price
            
            # Update trade
            trade['status'] = 'CLOSED'
            trade['exit_time'] = last_timestamp
            trade['exit_price'] = exit_price
            trade['exit_reason'] = 'END_OF_DATA'
            trade['pnl_points'] = pnl_points
            trade['pnl_percent'] = (pnl_points / trade['entry_price']) * 100 if trade['entry_price'] else 0
            trade['is_win'] = pnl_points > 0
            trade['is_loss'] = pnl_points < 0
            
            if trade.get('trade_manager_trade_id'):
                self.trade_manager.close_positions([trade['trade_manager_trade_id']])
            
            exit_stats['END_OF_DATA'] += 1
            
            if verbose:
                logger.debug(f"[CLOSE] End of data {trade['direction']} at {exit_price:.2f}")
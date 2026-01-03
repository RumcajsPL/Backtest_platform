# Updated: scripts/strategy_modules/trade_simulator.py
"""
Trade Simulation Module
Handles position management and trade simulation
"""
import pandas as pd
from typing import Dict, List
from .trade_tracker import TradeTracker
from src.strategies.trade_management.risk_manager import RiskManager
from src.strategies.trade_management.spread_manager import SpreadManager

class TradeSimulator:
    def __init__(self, config: Dict):
        self.config = config
        self.trade_tracker = TradeTracker()
        self.trade_manager = None
        self.spread_manager = None
        self.progressive_tracker = None  # Add this
        self.initialize_managers()
        
    def initialize_managers(self):
        """Initialize trade manager and spread manager"""
        from src.strategies.trade_management.trade_manager import TradeManager
        self.trade_manager = TradeManager(self.config)
        self.trade_tracker.set_trade_manager(self.trade_manager)
        # FIX: Drill down to trade_management for spread settings
        tm_config = self.config.get('trade_management', {})
        spread_config = tm_config.get('spread', {})
        if spread_config.get('enabled', False):
            asset_symbol = self.config.get('asset', {}).get('symbol', '')
            config_path = spread_config.get('config_path')
            self.spread_manager = SpreadManager(asset_symbol, config_path)
            
    def simulate_trades(self, df_strategy: pd.DataFrame, filtered_signals: pd.Series, 
                        verbose: bool = False, progressive_tracker=None, risk_manager: RiskManager = None,
                        signal_id_map: Dict = None) -> Dict:
        self.progressive_tracker = progressive_tracker  # Assign to self
        
        if risk_manager is None:
            raise ValueError("RiskManager required for simulation")
        
        position_rejected_count = {'buy': 0, 'sell': 0}
        exit_stats = {
            'STOP_LOSS': 0,
            'TAKE_PROFIT': 0,
            'OPPOSITE_SIGNAL': 0,
            'END_OF_DATA': 0
        }
        risk_stats = {
            'approved': {'buy': 0, 'sell': 0},
            'rejected': {'buy': 0, 'sell': 0},
            'adjusted': {'buy': 0, 'sell': 0},
            'total_approved': 0,
            'total_rejected': 0,
            'total_adjusted': 0
        }
        
        # Process each bar
        for i, (timestamp, row) in enumerate(df_strategy.iterrows()):
            # Check exits (SL/TP)
            for open_trade in list(self.trade_tracker.get_open_trades()):
                exit_price, exit_reason = self._check_exit_conditions(open_trade, row)
                
                if exit_reason:
                    # Calculate pnl etc
                    pnl_points = exit_price - open_trade['entry_price'] if open_trade['direction'] == 'BUY' else open_trade['entry_price'] - exit_price
                    pnl_percent = (pnl_points / open_trade['entry_price']) * 100 if open_trade['entry_price'] else 0
                    duration_bars = i - df_strategy.index.get_loc(open_trade['timestamp']) if 'timestamp' in open_trade else None
                    duration_minutes = (timestamp - open_trade['timestamp']).total_seconds() / 60 if 'timestamp' in open_trade else None
                    is_win = pnl_points > 0
                    is_loss = pnl_points < 0
                    
                    self.trade_tracker.close_position(open_trade['trade_id'], timestamp, 
                                                      exit_price, exit_reason, df_strategy)
                    # FIX: Close in TradeManager using correct ID
                    if open_trade.get('trade_manager_trade_id'):
                        self.trade_manager.close_positions([open_trade['trade_manager_trade_id']])  # ← FIXED
                    exit_stats[exit_reason] += 1
                    
                    if self.progressive_tracker and 'signal_id' in open_trade and open_trade['signal_id']:
                        self.progressive_tracker.update_trade_execution_details(
                            open_trade['signal_id'],
                            trade_id=open_trade['trade_id'],
                            exit_time=timestamp,
                            exit_price=exit_price,
                            exit_reason=exit_reason,
                            pnl_points=pnl_points,
                            pnl_percent=pnl_percent,
                            duration_bars=duration_bars,
                            duration_minutes=duration_minutes,
                            is_win=is_win,
                            is_loss=is_loss,
                            exit_check_high=row['high'],
                            exit_check_low=row['low'],
                            spread_adjusted_high=row['high'] + (self.spread_manager.get_spread_in_points(row['high']) if self.spread_manager else 0),
                            spread_adjusted_low=row['low'] - (self.spread_manager.get_spread_in_points(row['low']) if self.spread_manager else 0),
                            reason='Trade closed'
                        )
                    
                    if verbose:
                        print(f"  [EXIT] {timestamp} {open_trade['direction']} {exit_reason} at {exit_price:.2f}")
            
            # Process signal
            if timestamp in filtered_signals.index and pd.notna(filtered_signals[timestamp]):
                signal_type = filtered_signals[timestamp]
                is_long = (signal_type == 'BUY')
                direction = 'BUY' if is_long else 'SELL'
                bid_price = row['close']
                
                signal_id = signal_id_map.get(timestamp) if signal_id_map else None
                
                # STAGE 3
                result = self.trade_manager.handle_signal(timestamp, signal_type)
                
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
                
                if result['action'] == 'REJECT':
                    self.trade_tracker.reject_signal(
                        timestamp=timestamp,
                        direction=direction,
                        entry_price=None,
                        sl_price=None,
                        tp_price=None,
                        reason=result.get('reason', 'Unknown'),
                        comment=''
                    )
                    position_rejected_count['buy' if is_long else 'sell'] += 1
                    if verbose:
                        print(f"  [REJECT] {timestamp} {direction} - {result.get('reason', 'Unknown')}")
                    continue
                
                # STAGE 4
                needs_open = (result['action'] in ['OPEN', 'CLOSE_AND_REVERSE'])
                params = None
                if needs_open:
                    params = risk_manager.compute_trade_parameters(timestamp, bid_price, is_long)
                    
                    if params is None:
                        key = 'buy' if is_long else 'sell'
                        risk_stats['rejected'][key] += 1
                        risk_stats['total_rejected'] += 1
                        
                        reason = 'Risk validation failed'
                        
                        if self.progressive_tracker and signal_id:
                            self.progressive_tracker.update_risk_management_details(
                                signal_id, False, reason
                            )
                        
                        if result['action'] == 'OPEN':
                            self.trade_tracker.reject_signal(
                                timestamp=timestamp,
                                direction=direction,
                                entry_price=None,
                                sl_price=None,
                                tp_price=None,
                                reason='Risk rejected',
                                comment=''
                            )
                            if verbose:
                                print(f"  [REJECT] {timestamp} {direction} - Risk rejected")
                            continue
                        
                        elif result['action'] == 'CLOSE_AND_REVERSE':
                            self._handle_close(timestamp, result.get('close_trade_ids', []), row, verbose)
                            self.trade_manager.close_positions(result.get('close_trade_ids', []))
                            if verbose:
                                print(f"  [CLOSE ONLY] {timestamp} {direction} - Risk rejected new open")
                            continue
                    
                    key = 'buy' if is_long else 'sell'
                    risk_stats['approved'][key] += 1
                    risk_stats['total_approved'] += 1
                    if params['sl_adjusted']:
                        risk_stats['adjusted'][key] += 1
                        risk_stats['total_adjusted'] += 1
                    
                    # Update tracker for risk
                    if self.progressive_tracker and signal_id:
                        atr_length = self.config.get('trade_management', {}).get('sl_tp', {}).get('atr_length', 14)
                        atr_multiplier = self.config.get('trade_management', {}).get('sl_tp', {}).get('sl_multiplier', 1.4)
                        rr_ratio = self.config.get('trade_management', {}).get('sl_tp', {}).get('risk_to_reward_ratio', 3.0)
                        max_risk_percentile = self.config.get('trade_management', {}).get('risk_management', {}).get('max_risk_percentile', 0.003)
                        
                        atr_value = risk_manager.atr_series.loc[timestamp] if risk_manager.atr_series is not None else None
                        annual_range_value = risk_manager.annual_range_series.loc[timestamp] if risk_manager.annual_range_series is not None else None
                        
                        sl_distance_raw = atr_value * atr_multiplier if atr_value else None
                        risk_percentile_calculated = abs(params['executed_entry'] - params['raw_sl']) / annual_range_value if annual_range_value else None
                        risk_percentile_passed = True  # Since approved
                        
                        spread_enabled = self.config.get('spread', {}).get('enabled', False)
                        spread_type = risk_manager.spread_manager.asset_config.get('spread_type') if risk_manager.spread_manager else None
                        # Get actual spread values from params
                        spread_value = params.get('spread_value', 0.0)
                        spread_points = params.get('spread_value', 0.0)  # Same as spread_value
                        spread_applied = params.get('spread_applied', False)
                        
                        # Calculate spread efficiency
                        entry_price_mid = bid_price
                        entry_price_adjusted = params['executed_entry']
                        spread_efficiency_percent = (spread_value / bid_price * 100) if spread_value and bid_price else None
                        
                        self.progressive_tracker.update_risk_management_details(
                        signal_id, True, params['comment'],
                        entry_price=params['executed_entry'],
                        sl_price=params['trigger_sl'],
                        tp_price=params['tp'],
                        spread_cost=spread_value,
                        atr_value=atr_value,
                        atr_length=atr_length,
                        atr_multiplier=atr_multiplier,  # This is your sl_multiplier from config
                        sl_distance_raw=sl_distance_raw,
                        sl_price_raw=params['raw_sl'],
                        annual_range_value=annual_range_value,
                        risk_percentile_calculated=risk_percentile_calculated,
                        max_risk_percentile=max_risk_percentile,
                        risk_percentile_passed=risk_percentile_passed,
                        sl_price_final=params['trigger_sl'],
                        tp_price_final=params['tp'],
                        rr_ratio=rr_ratio,
                        spread_enabled=spread_enabled,
                        spread_type=spread_type,
                        spread_value=spread_value,
                        spread_points=spread_points,
                        entry_price_mid=entry_price_mid,
                        entry_price_adjusted=entry_price_adjusted,
                        spread_efficiency_percent=spread_efficiency_percent
                    )
                
                # STAGE 5
                if result['action'] == 'CLOSE_AND_REVERSE':
                    self._handle_close(timestamp, result.get('close_trade_ids', []), row, verbose)
                    self.trade_manager.close_positions(result.get('close_trade_ids', []))
                    if params:
                        self._handle_open(timestamp, direction, params, result['new_trade_id'], verbose, '(Reversal)', signal_id)
                
                elif result['action'] == 'OPEN':
                    if params:
                        self._handle_open(timestamp, direction, params, result['new_trade_id'], verbose, '', signal_id)
        
        # Close remaining
        self._close_remaining_positions(df_strategy, exit_stats, verbose)
        
        return {
            'all_trades': self.trade_tracker.get_trades(),
            'closed_trades': self.trade_tracker.get_closed_trades(),
            'open_trades': self.trade_tracker.get_open_trades(),
            'rejected_trades': self.trade_tracker.get_rejected_trades(),
            'exit_stats': exit_stats,
            'position_rejected_count': position_rejected_count,
            'risk_stats': risk_stats,
            'trade_manager_metrics': self.trade_manager.get_metrics()
        }
    
    def _check_exit_conditions(self, trade: Dict, bar: pd.Series) -> tuple:
        """Check if trade should exit based on current bar"""
        exit_price = None
        exit_reason = None
        
        if trade['direction'] == 'BUY':
            if bar['low'] <= trade['sl_price']:
                exit_price = trade['sl_price']
                exit_reason = 'STOP_LOSS'
            elif bar['high'] >= trade['tp_price']:
                exit_price = trade['tp_price']
                exit_reason = 'TAKE_PROFIT'
        else:  # SELL
            if bar['high'] >= trade['sl_price']:
                exit_price = trade['sl_price']
                exit_reason = 'STOP_LOSS'
            elif bar['low'] <= trade['tp_price']:
                exit_price = trade['tp_price']
                exit_reason = 'TAKE_PROFIT'
        
        return exit_price, exit_reason
    
    def _handle_close(self, timestamp: pd.Timestamp, close_trade_ids: List[int], row: pd.Series, verbose: bool):
        current_bid = row['close']
        spread = self.spread_manager.get_spread_in_points(current_bid) if self.spread_manager else 0.0
        
        for tid in close_trade_ids:
            track_trade = next((t for t in self.trade_tracker.get_open_trades() if t['trade_manager_trade_id'] == tid), None)
            if track_trade:
                exit_price = current_bid if track_trade['direction'] == 'BUY' else current_bid + spread
                self.trade_tracker.close_position(
                    trade_id=track_trade['trade_id'],
                    exit_time=timestamp,
                    exit_price=exit_price,
                    exit_reason='OPPOSITE_SIGNAL',
                    ohlcv_df=None
                )
                if verbose:
                    print(f"  [CLOSE] {timestamp} {track_trade['direction']} OPPOSITE at {exit_price:.2f}")
    
    def _handle_open(self, timestamp: pd.Timestamp, direction: str, params: Dict, new_trade_id: int, verbose: bool, comment_suffix: str = '', signal_id: int = None):
        self.trade_tracker.open_position(
            timestamp=timestamp,
            direction=direction,
            entry_price=params['executed_entry'],
            sl_price=params['trigger_sl'],
            tp_price=params['tp'],
            comment=params['comment'] + comment_suffix,
            trade_manager_action='OPEN',  # Or 'CLOSE_AND_REVERSE' if suffix
            trade_manager_trade_id=new_trade_id,
            signal_id=signal_id  # Add
        )
        self.trade_manager.open_position(new_trade_id, timestamp, direction)
        
        if self.progressive_tracker and signal_id:
            self.progressive_tracker.update_trade_execution_details(
                signal_id,
                trade_id=new_trade_id,
                entry_time=timestamp,
                entry_price_executed=params['executed_entry'],
                sl_price_executed=params['trigger_sl'],
                tp_price_executed=params['tp'],
                reason='Trade opened' + comment_suffix
            )
        
        if verbose:
            print(f"  [OPEN] {timestamp} {direction} at {params['executed_entry']:.2f}{comment_suffix}")
    
    def _close_remaining_positions(self, df_strategy: pd.DataFrame, 
                                  exit_stats: Dict, verbose: bool):
        if df_strategy.empty:
            return
        
        last_timestamp = df_strategy.index[-1]
        last_bid = df_strategy.iloc[-1]['close']
        spread = self.spread_manager.get_spread_in_points(last_bid) if self.spread_manager else 0.0
        
        for open_trade in list(self.trade_tracker.get_open_trades()):
            exit_price = last_bid if open_trade['direction'] == 'BUY' else last_bid + spread
            self.trade_tracker.close_position(
                trade_id=open_trade['trade_id'],
                exit_time=last_timestamp,
                exit_price=exit_price,
                exit_reason='END_OF_DATA',
                ohlcv_df=df_strategy
            )
            self.trade_manager.close_positions([open_trade['trade_id']])
            exit_stats['END_OF_DATA'] += 1
            if verbose:
                print(f"  [CLOSE] End of data {open_trade['direction']} at {exit_price:.2f}")
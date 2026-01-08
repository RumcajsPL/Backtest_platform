"""
Trade Simulation Module with LTF Execution Support
Uses actual OHLC prices from 1-second bars for realistic exit pricing
"""
import pandas as pd
from typing import Dict, List, Optional
from .trade_tracker import TradeTracker
from src.strategies.trade_management.risk_manager import RiskManager
from src.strategies.trade_management.spread_manager import SpreadManager

class TradeSimulator:
    def __init__(self, config: Dict):
        self.config = config
        self.trade_tracker = TradeTracker()
        self.trade_manager = None
        self.spread_manager = None
        self.progressive_tracker = None
        self.initialize_managers()
        
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
    
    def simulate_trades(self, df_strategy: pd.DataFrame, filtered_signals: pd.Series, 
                        verbose: bool = False, progressive_tracker=None, risk_manager: RiskManager = None,
                        signal_id_map: Dict = None, df_ltf: Optional[pd.DataFrame] = None) -> Dict:
        """Simulate trades with LTF execution using actual OHLC prices"""
        self.progressive_tracker = progressive_tracker
        
        if risk_manager is None:
            raise ValueError("RiskManager required for simulation")
        
        # Store LTF data if provided
        self.df_ltf = df_ltf
        
        # Debug output
        if verbose and df_ltf is not None:
            print(f"🔍 LTF Execution: Using {len(df_ltf):,} 1-second bars")
            print(f"🔍 Price source: Actual OHLC prices from LTF bars")
        
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
        
        # Process each strategy bar (1-minute intervals)
        for i, (timestamp, row) in enumerate(df_strategy.iterrows()):
            # Check exits using appropriate method
            if self.df_ltf is not None:
                # Use LTF for precise exit timing with actual OHLC prices
                self._check_exits_with_ltf_ohlc(timestamp, exit_stats, verbose)
            else:
                # Fallback to strategy timeframe with actual bar prices
                self._check_exits_with_strategy_tf_ohlc(i, timestamp, row, exit_stats, verbose, df_strategy)
            
            # Process signal if present at this timestamp (unchanged)
            if timestamp in filtered_signals.index and pd.notna(filtered_signals[timestamp]):
                signal_type = filtered_signals[timestamp]
                is_long = (signal_type == 'BUY')
                direction = 'BUY' if is_long else 'SELL'
                bid_price = row['close']
                
                signal_id = signal_id_map.get(timestamp) if signal_id_map else None
                
                # STAGE 3: Position Management
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
                
                # STAGE 4: Risk Management
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
                    
                    # Update progressive tracker with risk details
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
                        spread_value = params.get('spread_value', 0.0)
                        spread_points = params.get('spread_value', 0.0)
                        
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
                            atr_multiplier=atr_multiplier,
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
                
                # STAGE 5: Trade Execution
                if result['action'] == 'CLOSE_AND_REVERSE':
                    self._handle_close(timestamp, result.get('close_trade_ids', []), row, verbose)
                    self.trade_manager.close_positions(result.get('close_trade_ids', []))
                    if params:
                        self._handle_open(timestamp, direction, params, result['new_trade_id'], verbose, '(Reversal)', signal_id)
                
                elif result['action'] == 'OPEN':
                    if params:
                        self._handle_open(timestamp, direction, params, result['new_trade_id'], verbose, '', signal_id)
        
        # Close any remaining positions at end of data
        self._close_remaining_positions(df_strategy, exit_stats, verbose)
        
        return {
            'all_trades': self.trade_tracker.get_trades(),
            'closed_trades': self.trade_tracker.get_closed_trades(),
            'open_trades': self.trade_tracker.get_open_trades(),
            'rejected_trades': self.trade_tracker.get_rejected_trades(),
            'exit_stats': exit_stats,
            'position_rejected_count': position_rejected_count,
            'risk_stats': risk_stats,
            'trade_manager_metrics': self.trade_manager.get_metrics(),
            'execution_mode': 'LTF_OHLC' if self.df_ltf is not None else 'Strategy_TF_OHLC'
        }
    
    def _check_exits_with_ltf_ohlc(self, strategy_timestamp: pd.Timestamp, 
                                exit_stats: Dict, verbose: bool):
        """Check exits using LTF BID OHLC prices"""
        if self.df_ltf is None:
            return
        
        # Find LTF bars for this strategy minute
        next_timestamp = strategy_timestamp + pd.Timedelta(minutes=1)
        ltf_bars = self.df_ltf[
            (self.df_ltf.index >= strategy_timestamp) & 
            (self.df_ltf.index < next_timestamp)
        ]
        
        if ltf_bars.empty:
            return
        
        # Check each open trade
        for open_trade in list(self.trade_tracker.get_open_trades()):
            entry_time = open_trade.get('entry_time') or open_trade.get('timestamp')
            
            if entry_time is None or entry_time >= strategy_timestamp:
                continue
            
            # Check each LTF bar
            for ltf_index, (ltf_timestamp, ltf_row) in enumerate(ltf_bars.iterrows()):
                exit_info = self._check_exit_with_bid_ohlc(open_trade, ltf_row)
                
                if exit_info['exit_reason']:
                    # Calculate P&L with actual exit price
                    if open_trade['direction'] == 'BUY':
                        pnl_points = exit_info['exit_price'] - open_trade['entry_price']
                    else:
                        pnl_points = open_trade['entry_price'] - exit_info['exit_price']
                    
                    pnl_percent = (pnl_points / open_trade['entry_price']) * 100 if open_trade['entry_price'] else 0
                    
                    # Calculate duration
                    if entry_time:
                        duration_minutes = (ltf_timestamp - entry_time).total_seconds() / 60
                    else:
                        duration_minutes = None
                    
                    # Close trade
                    self.trade_tracker.close_position(
                        open_trade['trade_id'], 
                        ltf_timestamp, 
                        exit_info['exit_price'], 
                        exit_info['exit_reason'], 
                        None
                    )
                    
                    if open_trade.get('trade_manager_trade_id'):
                        self.trade_manager.close_positions([open_trade['trade_manager_trade_id']])
                    
                    exit_stats[exit_info['exit_reason']] += 1
                    
                    # Update progressive tracker
                    if self.progressive_tracker and 'signal_id' in open_trade and open_trade['signal_id']:
                        self.progressive_tracker.update_trade_execution_details(
                            open_trade['signal_id'],
                            trade_id=open_trade['trade_id'],
                            exit_time=ltf_timestamp,
                            exit_price=exit_info['exit_price'],
                            exit_reason=exit_info['exit_reason'],
                            pnl_points=pnl_points,
                            pnl_percent=pnl_percent,
                            duration_bars=None,
                            duration_minutes=duration_minutes,
                            is_win=pnl_points > 0,
                            is_loss=pnl_points < 0,
                            exit_check_high=ltf_row['high'],
                            exit_check_low=ltf_row['low'],
                            reason=f'Trade closed ({exit_info["exit_reason"]}) - BID OHLC execution'
                        )
                    
                    if verbose:
                        theoretical = open_trade['sl_price'] if exit_info['exit_reason'] == 'STOP_LOSS' else open_trade['tp_price']
                        diff = exit_info['exit_price'] - theoretical
                        sign = '+' if diff > 0 else ''
                        print(f"  [EXIT-LTF] {ltf_timestamp} {open_trade['direction']} {exit_info['exit_reason']}")
                        print(f"    Theoretical: {theoretical:.5f}")
                        print(f"    Actual BID:  {exit_info['exit_price']:.5f} ({sign}{diff:.5f})")
                        print(f"    Bar H/L:     {ltf_row['high']:.5f}/{ltf_row['low']:.5f}")
                        print(f"    P&L:         {pnl_points:+.2f} pts")
                    
                    break  # Exit this trade, move to next
    
    def _check_exit_with_bid_ohlc(self, trade: Dict, bar: pd.Series) -> Dict:
        """
        Check exit based on BID OHLC prices touching SL/TP levels
        
        Since SL/TP already include spread, we check if BID prices touch these levels
        
        Returns:
            Dict with 'exit_reason' and 'exit_price' or None for no exit
        """
        if trade['direction'] == 'BUY':
            # LONG position
            # SL hit: BID low touches or goes below SL (already spread-adjusted)
            if bar['low'] <= trade['sl_price']:
                # When SL is hit, fill at SL price or worse
                # Most conservative: use bar low (could be worse than SL)
                actual_price = min(bar['low'], trade['sl_price'])
                return {'exit_reason': 'STOP_LOSS', 'exit_price': actual_price}
                
            # TP hit: BID high touches or goes above TP (already spread-adjusted)
            elif bar['high'] >= trade['tp_price']:
                # When TP is hit, fill at TP price or better
                # Most favorable: use bar high (could be better than TP)
                actual_price = min(bar['high'], trade['tp_price'])
                return {'exit_reason': 'TAKE_PROFIT', 'exit_price': actual_price}
                
        else:  # SELL (SHORT)
            # SHORT position
            # SL hit: BID high touches or goes above SL (already spread-adjusted)
            if bar['high'] >= trade['sl_price']:
                actual_price = max(bar['high'], trade['sl_price'])
                return {'exit_reason': 'STOP_LOSS', 'exit_price': actual_price}
                
            # TP hit: BID low touches or goes below TP (already spread-adjusted)
            elif bar['low'] <= trade['tp_price']:
                actual_price = max(bar['low'], trade['tp_price'])
                return {'exit_reason': 'TAKE_PROFIT', 'exit_price': actual_price}
        
        return {'exit_reason': None, 'exit_price': None}
    
    def _check_exit_with_ohlc_prices(self, trade: Dict, bar: pd.Series) -> Dict:
        """
        Check if trade should exit based on actual OHLC prices
        
        Uses the actual high/low of the bar, not theoretical prices
        """
        if trade['direction'] == 'BUY':
            # LONG position
            # SL hit: price goes DOWN to or below stop loss
            if bar['low'] <= trade['sl_price']:
                return {'exit_reason': 'STOP_LOSS'}
            # TP hit: price goes UP to or above take profit
            elif bar['high'] >= trade['tp_price']:
                return {'exit_reason': 'TAKE_PROFIT'}
        else:
            # SHORT position  
            # SL hit: price goes UP to or above stop loss
            if bar['high'] >= trade['sl_price']:
                return {'exit_reason': 'STOP_LOSS'}
            # TP hit: price goes DOWN to or below take profit
            elif bar['low'] <= trade['tp_price']:
                return {'exit_reason': 'TAKE_PROFIT'}
        
        return {'exit_reason': None}
    
    def _calculate_actual_exit_price(self, trade: Dict, exit_reason: str, bar: pd.Series) -> float:
        """
        Calculate actual exit price from OHLC bar
        
        Professional approach:
        - SL hits: Worst price (low for long, high for short)
        - TP hits: Best possible fill (high for long, low for short)
        """
        # Get spread if available
        spread = 0.0
        if self.spread_manager:
            mid_price = bar['close']
            spread = self.spread_manager.get_spread_in_points(mid_price)
        
        if exit_reason == 'STOP_LOSS':
            if trade['direction'] == 'BUY':
                # Long position hitting SL: Market sell at BID price
                # Worst case: bar low minus spread
                exit_price = bar['low'] - spread
                # Add small slippage for SL (market moving against you)
                slippage = spread * 0.3  # 30% of spread as slippage
                exit_price -= slippage
            else:  # SELL
                # Short position hitting SL: Market buy at ASK price
                # Worst case: bar high plus spread
                exit_price = bar['high'] + spread
                slippage = spread * 0.3
                exit_price += slippage
                
        elif exit_reason == 'TAKE_PROFIT':
            if trade['direction'] == 'BUY':
                # Long position hitting TP: Limit sell at BID price
                # Best case: bar high (or TP if it's lower)
                exit_price = min(bar['high'], trade['tp_price'])
                # Limit orders can get filled at limit price or better
                if exit_price > trade['tp_price']:
                    exit_price = trade['tp_price']
            else:  # SELL
                # Short position hitting TP: Limit buy at ASK price
                # Best case: bar low (or TP if it's higher)
                exit_price = max(bar['low'], trade['tp_price'])
                if exit_price < trade['tp_price']:
                    exit_price = trade['tp_price']
        
        else:
            # Other exit reasons
            if trade['direction'] == 'BUY':
                exit_price = bar['close'] - spread  # Market sell
            else:
                exit_price = bar['close'] + spread  # Market buy
        
        return round(exit_price, 5)
    
    def _execute_trade_exit_ohlc(self, trade: Dict, exit_time: pd.Timestamp, 
                               exit_price: float, exit_reason: str, verbose: bool,
                               exit_bar: pd.Series = None):
        """Execute trade exit with actual OHLC-based prices"""
        entry_time = trade.get('entry_time') or trade.get('timestamp')
        
        # Calculate P&L with actual price
        if trade['direction'] == 'BUY':
            pnl_points = exit_price - trade['entry_price']
        else:  # SELL
            pnl_points = trade['entry_price'] - exit_price
        
        pnl_percent = (pnl_points / trade['entry_price']) * 100 if trade['entry_price'] else 0
        
        # Calculate duration
        if entry_time:
            duration_minutes = (exit_time - entry_time).total_seconds() / 60
        else:
            duration_minutes = None
        
        # Calculate how much worse/better than theoretical price
        theoretical_price = trade['sl_price'] if exit_reason == 'STOP_LOSS' else trade['tp_price']
        price_diff = exit_price - theoretical_price
        
        # Close in trade tracker
        self.trade_tracker.close_position(
            trade['trade_id'], 
            exit_time, 
            exit_price,  # Actual OHLC-based price
            exit_reason, 
            None
        )
        
        # Close in trade manager if applicable
        if trade.get('trade_manager_trade_id'):
            self.trade_manager.close_positions([trade['trade_manager_trade_id']])
        
        # Update progressive tracker
        if self.progressive_tracker and 'signal_id' in trade and trade['signal_id']:
            self.progressive_tracker.update_trade_execution_details(
                trade['signal_id'],
                trade_id=trade['trade_id'],
                exit_time=exit_time,
                exit_price=exit_price,
                exit_reason=exit_reason,
                pnl_points=pnl_points,
                pnl_percent=pnl_percent,
                duration_bars=None,
                duration_minutes=duration_minutes,
                is_win=pnl_points > 0,
                is_loss=pnl_points < 0,
                exit_check_high=exit_bar['high'] if exit_bar is not None else None,
                exit_check_low=exit_bar['low'] if exit_bar is not None else None,
                reason=f'Trade closed ({exit_reason}) - OHLC execution'
            )
        
        if verbose:
            sign = '+' if price_diff > 0 else ''
            print(f"  [EXIT-OHLC] {exit_time} {trade['direction']} {exit_reason}")
            print(f"    Theoretical: {theoretical_price:.5f}")
            print(f"    Actual OHLC: {exit_price:.5f} ({sign}{price_diff:.5f})")
            print(f"    P&L:         {pnl_points:+.2f} pts")
            if exit_bar is not None:
                print(f"    Bar H/L:     {exit_bar['high']:.5f}/{exit_bar['low']:.5f}")
    
    def _check_exits_with_strategy_tf_ohlc(self, bar_index: int, timestamp: pd.Timestamp, 
                                          bar: pd.Series, exit_stats: Dict, verbose: bool,
                                          df_strategy: pd.DataFrame):
        """Exit checking using actual OHLC prices from strategy timeframe"""
        for open_trade in list(self.trade_tracker.get_open_trades()):
            exit_info = self._check_exit_with_ohlc_prices(open_trade, bar)
            
            if exit_info['exit_reason']:
                # Calculate actual exit price from OHLC bar
                actual_exit_price = self._calculate_actual_exit_price(
                    open_trade, exit_info['exit_reason'], bar
                )
                
                # Calculate P&L
                if open_trade['direction'] == 'BUY':
                    pnl_points = actual_exit_price - open_trade['entry_price']
                else:
                    pnl_points = open_trade['entry_price'] - actual_exit_price
                
                pnl_percent = (pnl_points / open_trade['entry_price']) * 100 if open_trade['entry_price'] else 0
                
                # Calculate duration
                entry_time = open_trade.get('entry_time') or open_trade.get('timestamp')
                if entry_time and entry_time in df_strategy.index:
                    duration_bars = bar_index - df_strategy.index.get_loc(entry_time)
                else:
                    duration_bars = None
                
                if entry_time:
                    duration_minutes = (timestamp - entry_time).total_seconds() / 60
                else:
                    duration_minutes = None
                
                # Close position with actual price
                self.trade_tracker.close_position(
                    open_trade['trade_id'], 
                    timestamp, 
                    actual_exit_price, 
                    exit_info['exit_reason'], 
                    df_strategy
                )
                
                if open_trade.get('trade_manager_trade_id'):
                    self.trade_manager.close_positions([open_trade['trade_manager_trade_id']])
                
                exit_stats[exit_info['exit_reason']] += 1
                
                # Update progressive tracker
                if self.progressive_tracker and 'signal_id' in open_trade and open_trade['signal_id']:
                    self.progressive_tracker.update_trade_execution_details(
                        open_trade['signal_id'],
                        trade_id=open_trade['trade_id'],
                        exit_time=timestamp,
                        exit_price=actual_exit_price,
                        exit_reason=exit_info['exit_reason'],
                        pnl_points=pnl_points,
                        pnl_percent=pnl_percent,
                        duration_bars=duration_bars,
                        duration_minutes=duration_minutes,
                        is_win=pnl_points > 0,
                        is_loss=pnl_points < 0,
                        exit_check_high=bar['high'],
                        exit_check_low=bar['low'],
                        reason='Trade closed (Strategy TF OHLC)'
                    )
                
                if verbose:
                    theoretical_price = open_trade['sl_price'] if exit_info['exit_reason'] == 'STOP_LOSS' else open_trade['tp_price']
                    price_diff = actual_exit_price - theoretical_price
                    sign = '+' if price_diff > 0 else ''
                    print(f"  [EXIT] {timestamp} {open_trade['direction']} {exit_info['exit_reason']}")
                    print(f"    Actual: {actual_exit_price:.5f} ({sign}{price_diff:.5f})")
                    print(f"    P&L:    {pnl_points:+.2f} pts")
    
    def _get_next_strategy_timestamp(self, timestamp: pd.Timestamp) -> pd.Timestamp:
        """Get next strategy timeframe timestamp"""
        # Assuming 1-minute bars
        return timestamp + pd.Timedelta(minutes=1)
    
    def _handle_close(self, timestamp: pd.Timestamp, close_trade_ids: List[int], row: pd.Series, verbose: bool):
        """Handle closing positions due to opposite signals"""
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
        """Handle opening new positions"""
        self.trade_tracker.open_position(
            timestamp=timestamp,
            direction=direction,
            entry_price=params['executed_entry'],
            sl_price=params['trigger_sl'],
            tp_price=params['tp'],
            comment=params['comment'] + comment_suffix,
            trade_manager_action='OPEN',
            trade_manager_trade_id=new_trade_id,
            signal_id=signal_id
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
        """Close any remaining open positions at end of data"""
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
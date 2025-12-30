"""
Trade Simulation Module
Handles position management and trade simulation
"""
import pandas as pd
from typing import Dict, List
from .trade_tracker import TradeTracker

class TradeSimulator:
    def __init__(self, config: Dict):
        self.config = config
        self.trade_tracker = TradeTracker()
        self.trade_manager = None
        self.initialize_managers()
        
    def initialize_managers(self):
        """Initialize trade manager"""
        from src.strategies.trade_management.trade_manager import TradeManager
        self.trade_manager = TradeManager(self.config)
        self.trade_tracker.set_trade_manager(self.trade_manager)
    
    def simulate_trades(self, df_strategy: pd.DataFrame, potential_trades: Dict, 
                       verbose: bool = False) -> Dict:
        """
        Run complete trade simulation
        
        Args:
            df_strategy: OHLCV data for strategy period
            potential_trades: Dictionary of potential trades by timestamp
            verbose: Whether to print detailed logs
            
        Returns:
            Dictionary with simulation results
        """
        position_rejected_count = {'buy': 0, 'sell': 0}
        exit_stats = {
            'STOP_LOSS': 0,
            'TAKE_PROFIT': 0,
            'OPPOSITE_SIGNAL': 0,
            'END_OF_DATA': 0
        }
        
        # Process each bar in chronological order
        for i, (timestamp, row) in enumerate(df_strategy.iterrows()):
            # Check for exits on open positions (SL/TP) - FIRST check exits
            for open_trade in self.trade_tracker.get_open_trades():
                exit_price, exit_reason = self._check_exit_conditions(open_trade, row)
                
                if exit_reason:
                    self.trade_tracker.close_position(open_trade['trade_id'], timestamp, 
                                                     exit_price, exit_reason, df_strategy)
                    exit_stats[exit_reason] += 1
                    if verbose:
                        print(f"  [EXIT] {timestamp} {open_trade['direction']} {exit_reason} at {exit_price:.2f}")
            
            # Process new signal at this bar - AFTER checking exits
            if timestamp in potential_trades:
                self._process_signal(timestamp, potential_trades[timestamp], 
                                   position_rejected_count, verbose)
        
        # Close any remaining open positions at end of data
        self._close_remaining_positions(df_strategy, exit_stats, verbose)
        
        # Collect results
        all_trades = self.trade_tracker.get_trades()
        closed_trades = self.trade_tracker.get_closed_trades()
        open_trades = self.trade_tracker.get_open_trades()
        rejected_trades = self.trade_tracker.get_rejected_trades()
        
        return {
            'all_trades': all_trades,
            'closed_trades': closed_trades,
            'open_trades': open_trades,
            'rejected_trades': rejected_trades,
            'exit_stats': exit_stats,
            'position_rejected_count': position_rejected_count,
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
    
    def _process_signal(self, timestamp: pd.Timestamp, pot_trade: Dict, 
                       position_rejected_count: Dict, verbose: bool):
        """Process a single signal"""
        signal_row = pd.Series({
            'timestamp': timestamp,
            'signal': pot_trade['signal'],
            'entry': pot_trade['entry'],
            'sl': pot_trade['sl'],
            'tp': pot_trade['tp']
        })
        
        result = self.trade_manager.handle_signal(signal_row)
        
        if result['action'] == 'OPEN':
            trade_manager_trade_id = result.get('open_trade', {}).get('trade_id')
            
            self.trade_tracker.open_position(
                timestamp=timestamp,
                direction=pot_trade['signal'],
                entry_price=pot_trade['entry'],
                sl_price=pot_trade['sl'],
                tp_price=pot_trade['tp'],
                comment=pot_trade['comment'],
                trade_manager_action='OPEN',
                trade_manager_trade_id=trade_manager_trade_id
            )
            if verbose:
                print(f"  [OPEN] {timestamp} {pot_trade['signal']} at {pot_trade['entry']:.2f}")
            
        elif result['action'] == 'CLOSE_AND_REVERSE':
            self._handle_close_and_reverse(timestamp, pot_trade, result, verbose)
            
        elif result['action'] == 'REJECT':
            self.trade_tracker.reject_signal(
                timestamp=timestamp,
                direction=pot_trade['signal'],
                entry_price=pot_trade['entry'],
                sl_price=pot_trade['sl'],
                tp_price=pot_trade['tp'],
                reason=result.get('reason', 'Unknown'),
                comment=pot_trade['comment']
            )
            position_rejected_count['buy' if pot_trade['signal'] == 'BUY' else 'sell'] += 1
            if verbose:
                print(f"  [REJECT] {timestamp} {pot_trade['signal']} - {result.get('reason', 'Unknown')}")
    
    def _handle_close_and_reverse(self, timestamp: pd.Timestamp, pot_trade: Dict, 
                                result: Dict, verbose: bool):
        """Handle close and reverse action"""
        # Close existing trades
        for close_trade_info in result.get('close_trades', []):
            for track_trade in self.trade_tracker.get_open_trades():
                if track_trade.get('trade_manager_trade_id') == close_trade_info.get('trade_id'):
                    self.trade_tracker.close_position(
                        trade_id=track_trade['trade_id'],
                        exit_time=timestamp,
                        exit_price=close_trade_info.get('exit_price', pot_trade['entry']),
                        exit_reason='OPPOSITE_SIGNAL',
                        ohlcv_df=None
                    )
                    if verbose:
                        print(f"  [CLOSE] {timestamp} {track_trade['direction']} OPPOSITE at {close_trade_info.get('exit_price', pot_trade['entry']):.2f}")
                    break
        
        # Open reverse position
        trade_manager_trade_id = result.get('open_trade', {}).get('trade_id')
        
        self.trade_tracker.open_position(
            timestamp=timestamp,
            direction=pot_trade['signal'],
            entry_price=pot_trade['entry'],
            sl_price=pot_trade['sl'],
            tp_price=pot_trade['tp'],
            comment=pot_trade['comment'] + ' (Reversal)',
            trade_manager_action='CLOSE_AND_REVERSE',
            trade_manager_trade_id=trade_manager_trade_id
        )
        if verbose:
            print(f"  [OPEN] {timestamp} {pot_trade['signal']} REVERSE at {pot_trade['entry']:.2f}")
    
    def _close_remaining_positions(self, df_strategy: pd.DataFrame, 
                                 exit_stats: Dict, verbose: bool):
        """Close any remaining open positions at end of data"""
        for open_trade in self.trade_tracker.get_open_trades():
            exit_price = df_strategy.iloc[-1]['close']
            self.trade_tracker.close_position(
                trade_id=open_trade['trade_id'],
                exit_time=df_strategy.index[-1],
                exit_price=exit_price,
                exit_reason='END_OF_DATA',
                ohlcv_df=df_strategy
            )
            exit_stats['END_OF_DATA'] += 1
            if verbose:
                print(f"  [CLOSE] End of data {open_trade['direction']} at {exit_price:.2f}")
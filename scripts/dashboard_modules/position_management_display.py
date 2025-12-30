"""
Position Management Display Module
Displays position management statistics
"""
from typing import Dict
from .display_engine import DisplayEngine

class PositionManagementDisplay:
    def __init__(self, display_engine: DisplayEngine):
        self.display = display_engine
    
    def display_position_management(self, report_data: Dict):
        """Display position management statistics"""
        self.display.print_header("🎯 POSITION MANAGEMENT")
        
        signal_flow = report_data.get('signal_flow', {})
        pos_mgmt = signal_flow.get('step5_position_managed', {})
        config = report_data.get('config', {})
        
        # Display settings
        pos_config = config.get('position_control', {})
        close_opposite = pos_config.get('close_on_opposite', False)
        pyramiding = pos_config.get('pyramiding_enabled', False)
        
        print(f"{'Close on Opposite:':<30} "
              f"{self.display.get_emoji_indicator(close_opposite, '✅ ENABLED', '❌ DISABLED')}")
        print(f"{'Pyramiding:':<30} "
              f"{self.display.get_emoji_indicator(pyramiding, '✅ ENABLED', '❌ DISABLED')}")
        
        if pos_mgmt:
            self._display_signal_processing(pos_mgmt)
            self._display_exit_statistics(pos_mgmt)
            self._display_trade_manager_metrics(pos_mgmt)
    
    def _display_signal_processing(self, pos_mgmt: Dict):
        """Display signal processing statistics"""
        self.display.print_section("SIGNAL PROCESSING")
        
        stats = [
            ("Buy Opens", pos_mgmt.get('buy_opens', 0)),
            ("Sell Opens", pos_mgmt.get('sell_opens', 0)),
            ("Total Opens", pos_mgmt.get('total_opens', 0)),
            ("Rejected Signals", pos_mgmt.get('rejected_total', 0)),
        ]
        
        # Calculate rejection rate
        total_signals = pos_mgmt.get('total_opens', 0) + pos_mgmt.get('rejected_total', 0)
        rejection_rate = (pos_mgmt.get('rejected_total', 0) / total_signals * 100) if total_signals > 0 else 0
        
        stats.append(("Rejection Rate", f"{rejection_rate:.1f}%"))
        
        for label, value in stats:
            print(f"{label:<25} {value}")
    
    def _display_exit_statistics(self, pos_mgmt: Dict):
        """Display exit statistics"""
        exit_stats = pos_mgmt.get('exit_statistics', {})
        if not exit_stats:
            return
        
        self.display.print_section("EXIT STATISTICS")
        
        total_exits = sum(exit_stats.values())
        if total_exits > 0:
            for reason, count in exit_stats.items():
                if count > 0:
                    pct = count / total_exits * 100
                    print(f"{reason:<25} {count} ({pct:.1f}%)")
    
    def _display_trade_manager_metrics(self, pos_mgmt: Dict):
        """Display trade manager metrics"""
        tm_metrics = pos_mgmt.get('trade_manager_metrics', {})
        if not tm_metrics:
            return
        
        self.display.print_section("TRADE MANAGER METRICS")
        
        total_signals = tm_metrics.get('total_signals_received', 0)
        accepted = tm_metrics.get('signals_accepted', 0)
        rejected = tm_metrics.get('signals_rejected', 0)
        
        tm_stats = [
            ("Total Signals", total_signals),
            ("Signals Accepted", accepted),
            ("Signals Rejected", rejected),
        ]
        
        # Calculate acceptance rate
        if total_signals > 0:
            acceptance_rate = accepted / total_signals * 100
            tm_stats.append(("Acceptance Rate", f"{acceptance_rate:.1f}%"))
        
        tm_stats.append(("Positions Reversed", tm_metrics.get('positions_reversed', 0)))
        
        for label, value in tm_stats:
            print(f"{label:<25} {value}")
        
        # Display rejection reasons
        self._display_rejection_reasons(tm_metrics)
    
    def _display_rejection_reasons(self, tm_metrics: Dict):
        """Display rejection reasons breakdown"""
        reasons = tm_metrics.get('rejected_reasons', {})
        if not reasons:
            return
        
        self.display.print_section("REJECTION REASONS")
        
        total_reasons = sum(reasons.values())
        for reason, count in reasons.items():
            if count > 0:
                pct = count / total_reasons * 100
                print(f"{reason:<25} {count} ({pct:.1f}%)")
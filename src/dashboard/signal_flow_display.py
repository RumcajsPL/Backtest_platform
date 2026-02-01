"""
Signal Flow Display Module
Displays signal flow analysis with reliability notes
"""
from typing import Dict
from .display_engine import DisplayEngine

class SignalFlowDisplay:
    def __init__(self, display_engine: DisplayEngine):
        self.display = display_engine
    
    def display_signal_flow(self, report_data: Dict):
        """Display signal flow statistics with reliability warning"""
        self.display.print_header("📡 SIGNAL FLOW ANALYSIS")
        
        # Add reliability note
        print(f"{self.display.colors.YELLOW}⚠️  Note: This data comes from JSON report and may have known issues.")
        print(f"    Use 'Progressive Data Analysis' section for accurate signal progression.{self.display.colors.END}")
        print("-"*80)
        
        signal_flow = report_data.get('signal_flow', {})
        
        stages = [
            ("Raw Signals", "step1_raw_signals"),
            ("Time Filtered", "step2_time_filtered"),
            ("RSI Filtered", "step3_rsi_filtered"),
            ("Risk Managed", "step4_risk_managed"),
            ("Position Managed", "step5_position_managed"),
        ]
        
        headers = ["Stage", "BUY", "SELL", "TOTAL", "REJECTED", "% REJ"]
        rows = []
        
        for stage_name, stage_key in stages:
            if stage_key in signal_flow:
                row = self._get_stage_row(stage_name, stage_key, signal_flow, stages)
                rows.append(row)
        
        # Add overall rejection row
        overall = report_data.get('overall_rejection', {})
        total_rejected = overall.get('total_rejected', 0)
        rej_rate = overall.get('total_rejection_rate_pct', 0)
        
        rows.append([
            self.display.color_text('Overall Rejection:', self.display.colors.BOLD),
            '', '', '',
            str(total_rejected),
            f"{rej_rate:.1f}%"
        ])
        
        self.display.print_table(headers, rows, [20, 8, 8, 8, 10, 8])
    
    def _get_stage_row(self, stage_name: str, stage_key: str, signal_flow: Dict, stages: list) -> list:
        """Get row data for a stage"""
        stage = signal_flow[stage_key]
        buy = stage.get('buy', stage.get('buy_opens', 0))
        sell = stage.get('sell', stage.get('sell_opens', 0))
        total = stage.get('total', stage.get('total_opens', 0))
        rejected = stage.get('rejected_total', 0)
        
        # Calculate rejection rate
        rej_rate = self._calculate_rejection_rate(stage_key, signal_flow, stages, total, rejected)
        
        return [stage_name, str(buy), str(sell), str(total), str(rejected), f"{rej_rate:.1f}%"]
    
    def _calculate_rejection_rate(self, stage_key: str, signal_flow: Dict, stages: list, 
                                current_total: int, current_rejected: int) -> float:
        """Calculate rejection rate for a stage"""
        # Find previous stage
        prev_stage_key = None
        for prev_name, prev_key in stages:
            if prev_key == stage_key:
                break
            prev_stage_key = prev_key
        
        if prev_stage_key and prev_stage_key in signal_flow:
            prev_stage = signal_flow[prev_stage_key]
            prev_total = prev_stage.get('total', prev_stage.get('total_opens', current_total + current_rejected))
            return (current_rejected / prev_total * 100) if prev_total > 0 else 0
        
        return 0.0
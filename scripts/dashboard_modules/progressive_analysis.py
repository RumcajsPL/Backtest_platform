"""
Progressive Analysis Display Module
Analyzes signal progression data for deep insights
"""
import pandas as pd
from .display_engine import DisplayEngine

class ProgressiveAnalysisDisplay:
    def __init__(self, display_engine: DisplayEngine):
        self.display = display_engine
    
    def display_progressive_overview(self, progressive_df: pd.DataFrame):
        """Display comprehensive overview of progressive signal data"""
        if progressive_df is None or progressive_df.empty:
            print("⚠️  No progressive data available for analysis")
            return
        
        self.display.print_header("🔬 PROGRESSIVE SIGNAL ANALYSIS")
        
        # Basic statistics
        total_signals = len(progressive_df)
        executed_signals = len(progressive_df[
            (progressive_df['stage_trade'] == 'EXECUTED') | 
            (progressive_df['trade_id'].notna())
        ])
        executed_rate = (executed_signals / total_signals * 100) if total_signals > 0 else 0
        
        print(f"{'Total Signals Tracked:':<30} {total_signals:,}")
        print(f"{'Signals Executed:':<30} {executed_signals:,} ({executed_rate:.1f}%)")
        
        # Display signal flow through stages
        self._display_signal_funnel(progressive_df)
        
        # Display stage-by-stage rejection analysis
        self._display_stage_rejection_analysis(progressive_df)
        
        # Display detailed status breakdown
        self._display_final_status_breakdown(progressive_df)
    
    def _display_signal_funnel(self, progressive_df: pd.DataFrame):
        """Display signal progression through stages"""
        self.display.print_section("📊 SIGNAL PROGRESSION FUNNEL")
        
        # Define stages based on actual column names
        stages = [
            ('Stage 0: Raw Signals', None, None),
            ('Stage 1: Time Filter', 'stage_time', ['PASS']),
            ('Stage 2: RSI Filter', 'stage_rsi', ['PASS']),
            ('Stage 3: Position Mgmt', 'stage_position', ['OPEN', 'CLOSE_AND_REVERSE']),
            ('Stage 4: Risk Mgmt', 'stage_risk', ['APPROVED']),
            ('Stage 5: Trade Exec', 'stage_trade', ['EXECUTED']),
        ]
        
        headers = ["Stage", "Passed", "Rejected", "Total", "Pass Rate"]
        rows = []
        
        previous_total = len(progressive_df)
        
        for stage_name, stage_col, pass_values in stages:
            if stage_col is None:  # Raw signals stage
                passed = len(progressive_df)
                rejected = 0
                total = len(progressive_df)
                pass_rate = 100.0
            elif stage_col in progressive_df.columns:
                # Count passed signals for this stage
                if pass_values:
                    passed_mask = progressive_df[stage_col].isin(pass_values)
                else:
                    # If no pass values defined, count non-REJECT/non-NaN
                    passed_mask = ~progressive_df[stage_col].isin(['REJECT', 'REJECTED']) & progressive_df[stage_col].notna()
                
                passed = passed_mask.sum()
                rejected = previous_total - passed
                total = previous_total
                pass_rate = (passed / total * 100) if total > 0 else 0
                
                previous_total = passed  # For next stage
            else:
                # Stage column not found
                passed = 0
                rejected = previous_total
                total = previous_total
                pass_rate = 0
                previous_total = 0
            
            # Color code pass rate
            pass_rate_display = f"{pass_rate:.1f}%"
            if pass_rate > 80:
                pass_rate_display = self.display.color_text(pass_rate_display, self.display.colors.GREEN)
            elif pass_rate < 20:
                pass_rate_display = self.display.color_text(pass_rate_display, self.display.colors.RED)
            
            rows.append([
                stage_name,
                f"{passed:,}",
                f"{rejected:,}",
                f"{total:,}",
                pass_rate_display
            ])
        
        self.display.print_table(headers, rows, [25, 12, 12, 12, 15])
    
    def _display_stage_rejection_analysis(self, progressive_df: pd.DataFrame):
        """Display detailed rejection analysis by stage"""
        self.display.print_section("🎯 REJECTION REASON ANALYSIS")
        
        # Based on your CSV sample, here are the actual stage columns
        # Updated labels for clarity
        stage_configs = [
            ('TIME FILTER', 'stage_time', 'stage_time_reason', ['REJECT']),
            ('RSI FILTER', 'stage_rsi', 'stage_rsi_reason', ['REJECT']),
            ('POSITION MANAGEMENT', 'stage_position', 'stage_position_reason', ['REJECT']),
            ('RISK VALIDATION', 'stage_risk', 'stage_risk_reason', ['REJECTED']),
        ]
        
        for stage_name, stage_col, reason_col, reject_values in stage_configs:
            if stage_col in progressive_df.columns and reason_col in progressive_df.columns:
                # Create mask for rejected signals
                if reject_values:
                    rejected_mask = progressive_df[stage_col].isin(reject_values)
                else:
                    # Default: consider NaN as rejection if column exists but has no value
                    rejected_mask = progressive_df[stage_col].isna()
                
                rejected_df = progressive_df[rejected_mask]
                if not rejected_df.empty and len(rejected_df) > 0:
                    print(f"\n{stage_name} Rejections: {len(rejected_df):,}")
                    
                    # Get rejection reasons
                    if reason_col in rejected_df.columns:
                        reason_counts = rejected_df[reason_col].value_counts().head(5)
                        for reason, count in reason_counts.items():
                            if pd.notna(reason) and str(reason).strip():
                                pct = count / len(rejected_df) * 100
                                print(f"  • {str(reason)[:40]:<40} {count:>4} ({pct:4.1f}%)")
                    else:
                        print(f"  No reason column found: {reason_col}")
    
    def _display_final_status_breakdown(self, progressive_df: pd.DataFrame):
        """Display breakdown by final signal status"""
        if 'final_status' not in progressive_df.columns:
            return
        
        self.display.print_section("📈 FINAL STATUS BREAKDOWN")
        
        status_counts = progressive_df['final_status'].value_counts()
        
        headers = ["Status", "Count", "Percentage", "Description"]
        rows = []
        
        for status, count in status_counts.items():
            if pd.notna(status):
                pct = count / len(progressive_df) * 100
                
                # Add description based on status
                description = self._get_status_description(status)
                
                # Color code significant statuses
                count_display = f"{count:,}"
                if 'REJECTED' in str(status):
                    count_display = self.display.color_text(count_display, self.display.colors.YELLOW)
                elif 'TRADE_CLOSED' in str(status):
                    count_display = self.display.color_text(count_display, self.display.colors.GREEN)
                
                rows.append([
                    status,
                    count_display,
                    f"{pct:.1f}%",
                    description
                ])
        
        self.display.print_table(headers, rows, [20, 10, 12, 30])
    
    def _get_status_description(self, status: str) -> str:
        """Get human-readable description for status"""
        status_str = str(status)
        descriptions = {
            'RAW': 'Initial signal generated',
            'TIME_FILTERED': 'Passed time filter',
            'RSI_FILTERED': 'Passed RSI filter',
            'POSITION_APPROVED': 'Position management approved',
            'RISK_APPROVED': 'Risk management approved',
            'TRADE_OPEN': 'Trade opened (still active)',
            'TRADE_CLOSED': 'Trade closed',
            'REJECTED_TIME': 'Rejected by time filter',
            'REJECTED_RSI': 'Rejected by RSI filter',
            'REJECTED_POSITION': 'Rejected by position management',
            'REJECTED_RISK': 'Rejected by risk management',
            'NO_TRADE': 'No trade executed',
            'POSITION_REVERSAL': 'Position closed and reversed',
        }
        
        # Try exact match first
        if status_str in descriptions:
            return descriptions[status_str]
        
        # Try partial match
        for key, desc in descriptions.items():
            if key in status_str:
                return desc
        
        return 'Unknown status'
    
    def display_risk_analysis(self, progressive_df: pd.DataFrame):
        """Display risk analysis from progressive data - REMOVED MISLEADING METRICS"""
        if progressive_df is None or progressive_df.empty:
            return
        
        # Filter for signals that reached risk stage
        risk_signals = progressive_df[
            (progressive_df['stage_risk'].notna()) | 
            (progressive_df['risk_percentile_calculated'].notna()) |
            (progressive_df['atr_value'].notna())
        ].copy()
        
        if risk_signals.empty:
            print("⚠️  No risk management data available in progressive data")
            return
        
        self.display.print_header("🎲 RISK MANAGEMENT ANALYSIS")
        
        # Risk percentile analysis (RELIABLE - calculated from actual data)
        if 'risk_percentile_calculated' in risk_signals.columns:
            risk_pct = risk_signals['risk_percentile_calculated']
            valid_risk = risk_pct[risk_pct.notna()]
            if not valid_risk.empty:
                self.display.print_section("RISK PERCENTILE ANALYSIS")
                
                stats = [
                    ("Average Risk %", f"{valid_risk.mean() * 100:.3f}%"),
                    ("Median Risk %", f"{valid_risk.median() * 100:.3f}%"),
                    ("Max Risk %", f"{valid_risk.max() * 100:.3f}%"),
                    ("Min Risk %", f"{valid_risk.min() * 100:.3f}%"),
                ]
                
                if len(valid_risk) > 1:
                    stats.append(("Std Dev Risk %", f"{valid_risk.std() * 100:.3f}%"))
                
                for label, value in stats:
                    print(f"{label:<30} {value}")

        if 'atr_value' in risk_signals.columns:
            atr_values = risk_signals['atr_value']
            valid_atr = atr_values[atr_values.notna()]
            if not valid_atr.empty:
                self.display.print_section("ATR ANALYSIS (Market Volatility)")
                
                stats = [
                    ("Average ATR", f"{valid_atr.mean():.2f} pts"),
                    ("Median ATR", f"{valid_atr.median():.2f} pts"),
                    ("Max ATR", f"{valid_atr.max():.2f} pts"),
                    ("Min ATR", f"{valid_atr.min():.2f} pts"),
                ]
                
                for label, value in stats:
                    print(f"{label:<30} {value}")
        
        # Add actual spread cost analysis (if available)
        if 'spread_cost' in risk_signals.columns:
            spread_costs = risk_signals['spread_cost']
            valid_spread = spread_costs[spread_costs.notna()]
            if not valid_spread.empty:
                self.display.print_section("SPREAD COST ANALYSIS")
                
                stats = [
                    ("Avg Spread Cost", f"{valid_spread.mean():.2f} pts"),
                    ("Median Spread Cost", f"{valid_spread.median():.2f} pts"),
                    ("Max Spread Cost", f"{valid_spread.max():.2f} pts"),
                    ("Min Spread Cost", f"{valid_spread.min():.2f} pts"),
                ]
                
                for label, value in stats:
                    print(f"{label:<30} {value}")
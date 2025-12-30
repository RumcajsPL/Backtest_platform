"""
Enhanced Standalone Performance Dashboard - Modular Version
Main orchestrator for dashboard display
"""
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path for module imports
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

# ... [previous imports and setup]

def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/dashboard_standalone.py <report_json> [--visualize]")
        print("\nArguments:")
        print("  <report_json>    Path to the JSON report file")
        print("  --visualize      Create visualizations (optional)")
        sys.exit(1)
    
    report_path = sys.argv[1]
    create_visualizations = '--visualize' in sys.argv
    
    try:
        from scripts.dashboard_modules import (
            DashboardDataLoader, DisplayEngine, MetricsDisplay,
            SignalFlowDisplay, TradeAnalysisDisplay, DrawdownDisplay,
            PositionManagementDisplay, TimeBasedDisplay, DashboardVisualizations
        )
        
    except ImportError as e:
        print(f"❌ Error importing dashboard modules: {e}")
        sys.exit(1)
    
    # Initialize dashboard
    try:
        display = DisplayEngine()
        
        print(f"\n{display.color_text('📊 ENHANCED WBWS STRATEGY DASHBOARD', display.colors.BOLD + display.colors.CYAN)}")
        print("="*80)
        print(f"Report: {Path(report_path).name}")
        print("="*80)
        
        # 1. Load data
        data_loader = DashboardDataLoader(report_path)
        report_data, trades_df, config = data_loader.load_all_data()
        
        # Show data loading status
        summary = data_loader.get_data_summary()
        print(f"\n📁 Data loading status:")
        print(f"  • Report loaded: ✅")
        
        if summary['trades_loaded']:
            print(f"  • Trade data loaded: ✅ ({summary['closed_trades']} closed trades)")
        else:
            print(f"  • Trade data loaded: ❌ (CSV file not found or couldn't be loaded)")
        
        # 2. Initialize display modules
        metrics_display = MetricsDisplay(display)
        signal_display = SignalFlowDisplay(display)
        trade_display = TradeAnalysisDisplay(display)
        drawdown_display = DrawdownDisplay(display)
        position_display = PositionManagementDisplay(display)
        time_display = TimeBasedDisplay(display)
        
        # 3. Display all sections
        metrics_display.display_overview(report_data)
        signal_display.display_signal_flow(report_data)
        
        # Only display detailed analysis if we have the CSV data
        if trades_df is not None and not trades_df.empty:
            print(f"\n{display.color_text('📈 DETAILED ANALYSIS (from CSV trade data)', display.colors.GREEN)}")
            print("-"*80)
            
            metrics_display.display_advanced_metrics(trades_df, report_data)
            trade_display.display_trade_analysis(trades_df)
            drawdown_display.display_drawdown_analysis(trades_df)
            time_display.display_monthly_performance(trades_df)
            time_display.display_hourly_performance(trades_df)
            
            # Create visualizations if requested
            if create_visualizations:
                output_dir = Path(report_path).parent
                
                # Extract timestamp from report filename or use current time
                report_filename = Path(report_path).stem
                if '_' in report_filename:
                    # Try to extract timestamp from report filename (e.g., strategy_report_20251230_141721)
                    parts = report_filename.split('_')
                    if len(parts) >= 3:
                        timestamp = f"{parts[-2]}_{parts[-1]}"
                    else:
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                else:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                visualizer = DashboardVisualizations(output_dir, timestamp)
                visualizer.create_all_visualizations(trades_df)
        else:
            print(f"\n{display.color_text('📈 BASIC ANALYSIS (JSON report only)', display.colors.YELLOW)}")
            print("-"*80)
            print("Advanced metrics, trade analysis, and time-based metrics require the CSV trade file.")
        
        # Position management is available from JSON
        position_display.display_position_management(report_data)
        
        # Show file info
        display.print_header("📁 FILES")
        print(f"JSON Report: {Path(report_path).resolve()}")
        
        csv_path = data_loader._find_csv_file()
        if csv_path and csv_path.exists():
            print(f"CSV Trades: {csv_path.resolve()} ✅")
        else:
            print(f"CSV Trades: Not found ❌")
        
        print(f"\n{display.color_text('✅ Dashboard completed!', display.colors.GREEN)}")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
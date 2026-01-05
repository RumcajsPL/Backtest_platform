"""
Enhanced Standalone Performance Dashboard - Optimized for Progressive Data
"""
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

# Add project root to path for module imports
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

def create_visualizations(report_path: str, trades_df: pd.DataFrame):
    """Create visualizations"""
    output_dir = Path(report_path).parent
    
    # Extract timestamp from report filename or use current time
    report_filename = Path(report_path).stem
    if '_' in report_filename:
        parts = report_filename.split('_')
        if len(parts) >= 3:
            timestamp = f"{parts[-2]}_{parts[-1]}"
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    try:
        from scripts.dashboard_modules.visualizations import DashboardVisualizations
        visualizer = DashboardVisualizations(output_dir, timestamp)
        visualizer.create_all_visualizations(trades_df)
        
        print(f"\n🖼️  Visualizations created in: {output_dir / 'visualizations'}")
        
    except ImportError as e:
        print(f"⚠️  Could not create visualizations: {e}")

def display_file_info(display, report_path: str, data_loader):
    """Display file information"""
    display.print_header("📁 FILES & DATA SOURCES")
    print(f"JSON Report: {Path(report_path).resolve()}")
    
    # Progressive CSV info
    progressive_path = data_loader._find_progressive_csv()
    if progressive_path and progressive_path.exists():
        print(f"Progressive CSV: {progressive_path.resolve()} ✅")
    else:
        print(f"Progressive CSV: Not found")
    
    # Legacy CSV info (for reference)
    legacy_path = data_loader._find_legacy_csv()
    if legacy_path and legacy_path.exists():
        print(f"Legacy CSV: {legacy_path.resolve()} ✅")
    else:
        print(f"Legacy CSV: Not found")

def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/dashboard_standalone.py <report_json> [--visualize]")
        print("\nArguments:")
        print("  <report_json>    Path to the JSON report file")
        print("  --visualize      Create visualizations (optional)")
        sys.exit(1)
    
    report_path = sys.argv[1]
    create_visualizations_flag = '--visualize' in sys.argv
    
    try:
        from scripts.dashboard_modules import (
            DashboardDataLoader, DisplayEngine, MetricsDisplay,
            SignalFlowDisplay, TradeAnalysisDisplay, DrawdownDisplay,
            PositionManagementDisplay, TimeBasedDisplay,
            ProgressiveAnalysisDisplay
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
        
        # 1. Load all data with progressive priority
        data_loader = DashboardDataLoader(report_path)
        report_data, trades_df, _ = data_loader.load_all_data()
        progressive_df = data_loader.get_progressive_data()
        
        # Show data loading status
        summary = data_loader.get_data_summary()
        print(f"\n📁 Data loading status:")
        print(f"  • Report loaded: ✅")
        
        if summary['progressive_data_loaded']:
            print(f"  • Progressive data: ✅ ({summary['total_signals']:,} signals)")
            print(f"  • Data source: Progressive CSV")
        elif summary['trades_data_loaded']:
            print(f"  • Trade data loaded: ✅ ({summary['total_trade_records']:,} records)")
            print(f"  • Data source: Legacy CSV")
        else:
            print(f"  • Data loaded: ❌")
        
        # ============================================================
        # SECTION 1: JSON REPORT DATA (FROM STRATEGY - MAY HAVE BUGS)
        # ============================================================
        print(f"\n{display.color_text('📋 SECTION 1: STRATEGY REPORT DATA', display.colors.BOLD + display.colors.MAGENTA)}")
        print(f"{display.color_text('(From JSON report - strategy calculated)', display.colors.YELLOW)}")
        print("-"*80)
        
        # Initialize all display modules
        metrics_display = MetricsDisplay(display)
        signal_display = SignalFlowDisplay(display)
        position_display = PositionManagementDisplay(display)
        
        # Display JSON-based sections
        metrics_display.display_overview(report_data)
        signal_display.display_signal_flow(report_data)
        position_display.display_position_management(report_data)
        
        # ============================================================
        # SECTION 2: PROGRESSIVE CSV ANALYSIS (MORE RELIABLE)
        # ============================================================
        print(f"\n{display.color_text('🔬 SECTION 2: PROGRESSIVE DATA ANALYSIS', display.colors.BOLD + display.colors.GREEN)}")
        print(f"{display.color_text('(From progressive CSV - execution verified)', display.colors.CYAN)}")
        print("="*80)
        
        progressive_display = ProgressiveAnalysisDisplay(display)
        
        # 2.1 Progressive signal analysis
        if progressive_df is not None and not progressive_df.empty:
            progressive_display.display_progressive_overview(progressive_df)
            progressive_display.display_risk_analysis(progressive_df)
        else:
            print("⚠️  No progressive data available for analysis")
        
        # ============================================================
        # SECTION 3: DASHBOARD-CALCULATED METRICS (FROM PROGRESSIVE DATA)
        # ============================================================
        print(f"\n{display.color_text('📊 SECTION 3: DASHBOARD-CALCULATED METRICS', display.colors.BOLD + display.colors.BLUE)}")
        print(f"{display.color_text('(Calculated from progressive execution data)', display.colors.CYAN)}")
        print("="*80)
        
        # Initialize dashboard calculation modules
        trade_display = TradeAnalysisDisplay(display)
        drawdown_display = DrawdownDisplay(display)
        time_display = TimeBasedDisplay(display)
        
        # 3.1 Detailed performance analysis (from trade data)
        if trades_df is not None and not trades_df.empty:
            print(f"\n{display.color_text('🎯 ADVANCED PERFORMANCE METRICS', display.colors.GREEN)}")
            print("-"*60)
            metrics_display.display_advanced_metrics(trades_df, report_data)
            
            print(f"\n{display.color_text('🔍 DETAILED TRADE ANALYSIS', display.colors.GREEN)}")
            print("-"*60)
            trade_display.display_trade_analysis(trades_df)
            
            print(f"\n{display.color_text('📉 DRAWDOWN & RISK ANALYSIS', display.colors.GREEN)}")
            print("-"*60)
            drawdown_display.display_drawdown_analysis(trades_df)
            
            print(f"\n{display.color_text('📅 TIME-BASED PERFORMANCE', display.colors.GREEN)}")
            print("-"*60)
            time_display.display_monthly_performance(trades_df)
            time_display.display_hourly_performance(trades_df)
            
            # Create visualizations if requested
            if create_visualizations_flag:
                create_visualizations(report_path, trades_df)
        else:
            print(f"\n{display.color_text('📈 BASIC ANALYSIS ONLY', display.colors.YELLOW)}")
            print("-"*60)
            print("Dashboard-calculated metrics require trade execution data.")
        
        # ============================================================
        # SECTION 4: FILE INFORMATION
        # ============================================================
        print(f"\n{display.color_text('📁 SECTION 4: FILES & DATA SOURCES', display.colors.BOLD)}")
        print("="*80)
        display_file_info(display, report_path, data_loader)
        
        # ============================================================
        # DATA SOURCE NOTES
        # ============================================================
        print(f"\n{display.color_text('📝 DATA SOURCE NOTES:', display.colors.BOLD + display.colors.YELLOW)}")
        print("-"*80)
        print(f"{display.colors.YELLOW}• Section 1:{display.colors.END} From JSON report (strategy-calculated, may have known issues)")
        print(f"{display.colors.GREEN}• Section 2:{display.colors.END} From progressive CSV (signal progression tracking)")
        print(f"{display.colors.BLUE}• Section 3:{display.colors.END} Dashboard-calculated from progressive data (most reliable)")
        print(f"{display.colors.CYAN}Note:{display.colors.END} Progressive CSV is the single source of truth for execution data")
        
        print(f"\n{display.color_text('✅ Dashboard completed!', display.colors.GREEN)}")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
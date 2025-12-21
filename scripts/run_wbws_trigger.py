# scripts/run_wbws_trigger.py
"""
WBWS Trigger Runner - Main Orchestrator Script

Minimal terminal output - detailed results saved to reports.
Terminal shows: status, progress, errors/warnings only.
"""
import sys
import os
from pathlib import Path

# Get project root
project_root = Path(__file__).resolve().parent.parent

# Add to Python path
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
if str(project_root / 'src') not in sys.path:
    sys.path.insert(0, str(project_root / 'src'))
if str(project_root / 'scripts') not in sys.path:
    sys.path.insert(0, str(project_root / 'scripts'))

# Import modules
try:
    import src.config.config_loader as config_module
    import scripts.data_preprocessing.prepare_ohlcv as prep_module
    import src.indicators.wbws_trigger as trigger_module
    import src.utils.report_generator as report_module
    
    ConfigLoader = config_module.ConfigLoader
    OHLCVPreprocessor = prep_module.OHLCVPreprocessor
    WBWSTrigger = trigger_module.WBWSTrigger
    ReportGenerator = report_module.ReportGenerator
    
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)


def run_wbws_trigger(config_path: str, verbose: bool = False):
    """
    Run complete WBWS Trigger workflow with minimal terminal output.
    
    Args:
        config_path: Path to YAML config file
        verbose: If True, show detailed terminal output
        
    Returns:
        Tuple of (signals_df, execution_stats)
    """
    
    print("\n" + "="*70)
    print("🚀 WBWS TRIGGER WORKFLOW")
    print("="*70 + "\n")
    
    # ========================================================================
    # STEP 1: LOAD CONFIGURATION
    # ========================================================================
    print("📋 Loading configuration...")
    
    try:
        loader = ConfigLoader(project_root)
        config = loader.load(config_path, verbose=verbose)
        print(f"   ✅ Config loaded: {config.get('name', 'Unnamed')}")
        
        if verbose:
            print(f"\n{loader.get_config_info(config)}\n")
    
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return None, None
    
    # ========================================================================
    # STEP 2: PREPROCESS DATA
    # ========================================================================
    print("🔧 Preprocessing data...")
    
    try:
        preprocessor = OHLCVPreprocessor(config['data'])
        
        # Set verbose mode from config
        preprocessor.verbose = verbose
        
        df_clean = preprocessor.prepare(config['data']['file'])
        preprocessing_info = preprocessor.get_info()
        
        print(f"   ✅ Data ready: {len(df_clean):,} bars")
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return None, None
    
    # ========================================================================
    # STEP 3: RUN INDICATOR
    # ========================================================================
    print("🧮 Calculating signals...")
    
    try:
        htf_period = config['indicator']['htf_period']
        indicator = WBWSTrigger(htf_period=htf_period)
        
        signals = indicator.calculate_signals(df_clean, verbose=verbose)
        stats = indicator.get_execution_stats()
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None, None
    
    # ========================================================================
    # STEP 4: GENERATE REPORTS
    # ========================================================================
    print("📊 Generating reports...")
    
    try:
        # Create report generator
        output_config = config.get('output', {
            'save_execution_report': True,
            'reports_dir': 'outputs/reports/WBWS',
            'verbose': verbose
        })
        output_config['verbose'] = verbose
        
        reporter = ReportGenerator(output_config, project_root)
        
        # Save comprehensive report (contains all details)
        report_file = reporter.save_comprehensive_report(
            config=config,
            preprocessing_info=preprocessing_info,
            execution_stats=stats,
            signals_df=signals,
            report_type='WBWS'
        )
        
        # Print minimal summary (or verbose if requested)
        reporter.print_minimal_summary(stats)
        
        # Save signals CSV (if configured)
        asset_symbol = config['asset'].get('symbol', 'UNKNOWN')
        reporter.save_signals_csv(signals, asset_symbol)
        
    except Exception as e:
        print(f"   ⚠️  Warning: {e}")
    
    # ========================================================================
    # COMPLETION
    # ========================================================================
    print("\n" + "="*70)
    print("✅ WORKFLOW COMPLETED")
    print("="*70)
    
    # Final summary (always show)
    print(f"\n📊 Summary:")
    print(f"   Asset:  {config['asset'].get('symbol', 'N/A')}")
    print(f"   Bars:   {stats['total_bars']:,}")
    print(f"   Signals: {stats['signals']['total']:,} ({stats['signals']['buy']:,} buy, {stats['signals']['sell']:,} sell)")
    
    if report_file:
        print(f"   Report: {os.path.basename(report_file)}")
    
    print("\n" + "="*70 + "\n")
    
    return signals, stats


def main():
    """Main entry point."""
    
    # Parse command line arguments
    import argparse
    
    parser = argparse.ArgumentParser(description='Run WBWS Trigger workflow')
    parser.add_argument('config', help='Path to YAML config file')
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Show detailed terminal output')
    
    args = parser.parse_args()
    
    # Check if config file exists
    config_file = Path(args.config)
    if not config_file.is_absolute():
        config_file = project_root / args.config
    
    if not config_file.exists():
        print(f"❌ Config file not found: {config_file}")
        sys.exit(1)
    
    # Run workflow
    try:
        signals, stats = run_wbws_trigger(args.config, verbose=args.verbose)
        
        if signals is None:
            print("❌ Workflow failed")
            sys.exit(1)
        
        sys.exit(0)
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        sys.exit(1)
    
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
# src/main.py
import sys
import os
from pathlib import Path

# Add src to Python path
sys.path.append(os.path.join(os.path.dirname(__file__)))

from src.indicators.wbws_trigger import WBWSTrigger
from src.config.paths import RAW_DATA_DIR, PROCESSED_DATA_DIR


def main():
    """Main entry point for the backtesting platform"""
    print("DAX40 Scalping Backtest Platform")
    print("=" * 40)
    
    # Check if data directory exists
    if not RAW_DATA_DIR.exists():
        print(f"Creating data directory: {RAW_DATA_DIR}")
        RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # Initialize and run WBWS Trigger
    print("\n1. Loading and processing WBWS Trigger...")
    try:
        wbws = WBWSTrigger()
        df_with_signals = wbws.load_and_process()
        print(f"   Processed {len(df_with_signals)} rows")
        
        # Show sample of signals
        if len(df_with_signals) > 0:
            print(f"\n   Sample signals:")
            print(f"   First signal at index: {df_with_signals.index[0]}")
            print(f"   Columns available: {list(df_with_signals.columns)}")
            
    except Exception as e:
        print(f"   Error processing WBWS Trigger: {e}")
    
    print("\nBacktesting platform ready!")


if __name__ == "__main__":
    main()
"""
Test Script
"""

import sys
from pathlib import Path
import pandas as pd
import yaml

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Imports from your architecture
from src.strategies.specific.wbws_strategy import WBWSStrategy #new WBWSStrategy
from src.strategies.core.signal_generator import SignalGenerator #old SignalGenerator (to be replaced by new when ready)
from src.strategies.core.filter_pipeline import FilterPipeline #old FilterPipeline (to be replaced by new when ready)
#... (as above)


# ---------------------------------------------------------
# Load YAML config
# ---------------------------------------------------------
def load_config():
    cfg_path = PROJECT_ROOT / "configs/strategies/wbws/wbws_strategy.yaml"
    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------
# Load OHLCV + HTF using config date_range => if new Dataloader created the below not required or to modify
# ---------------------------------------------------------
def load_sample_data(config):
    data_cfg = config["data"]

    data_path = PROJECT_ROOT / data_cfg["file"]
    htf_path = PROJECT_ROOT / data_cfg["file_htf"]

    date_start = data_cfg["date_range"]["start"]
    date_end = data_cfg["date_range"]["end"]

    # Load full OHLCV (for RiskManager)
    df_full = pd.read_parquet(data_path)

    # RiskManager needs ALL history up to end of test window
    df_history = df_full.loc[:date_end]

    # Strategy only uses the test window
    df = df_full.loc[date_start:date_end]

    # Load HTF and align to LTF window
    df_htf_full = pd.read_parquet(htf_path)
    df_htf = df_htf_full.loc[df.index.min(): df.index.max()]

    return df, df_htf, df_history

# ---------------------------------------------------------
# Raw signals + Filtered signals
# ---------------------------------------------------------

# ---------------------------------------------------------
# Main test
# ---------------------------------------------------------
def main():
    print("\n=== WBWSStrategy ... migration Test ===\n")

    # 1) Generate raw signals
    
    # 2) Apply filters
    
    # 3) Trade simulation -> RiskManager
    
    # 4) Trade simulation -> Position/Spread/Trade Manager
    
    # 5) Iterate through test window
    rejected_count = 0

    #for ...
    
        # Print valid signals (BUY/SELL)
        
        # Print OPEN trades
       

        # Print CLOSE trades
       
        # Count rejections
   
if __name__ == "__main__":
    main()
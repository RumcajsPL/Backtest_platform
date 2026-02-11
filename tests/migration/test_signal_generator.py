import sys
from pathlib import Path
import pandas as pd

# Path resolution
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.strategies.specific.modules.signal_generator import SignalGenerator as NewSignalGenerator
from src.strategies.specific.modules.data_loader import DataLoader as NewDataLoader

# 1. Configuration & Data Loading
config_path = PROJECT_ROOT / "configs/strategies/wbws/wbws_strategy_debug.yaml"
HTF_PERIOD = "1H"

print(f"\n{'='*40}\nDATA LOADER METRICS\n{'='*40}")
loader = NewDataLoader(str(config_path))
loader.load_config()
data_bundle = loader.load_data()

print(f"✅ DataLoader v2 Active")
print(f"Strategy bars: {len(data_bundle.strategy):,}")
print(f"HTF bars:      {len(data_bundle.htf):,}")

# 2. New SignalGenerator Output
print(f"\n{'='*40}\nNEW SIGNAL GENERATOR RESULTS\n{'='*40}")

# Using 'core' mode for clean signal generation
new_gen = NewSignalGenerator(htf_period=HTF_PERIOD, mode="core")
signal_frame = new_gen.generate_signals(data_bundle)

# Extract counts using the SignalFrame helper
counts = signal_frame.count_by_type()

print(f"Dtype:      {signal_frame.signals.dtype}")
print(f"BUY Signals:  {counts['buy']:,}")
print(f"SELL Signals: {counts['sell']:,}")
print(f"Total Rows:   {len(signal_frame.signals):,}")
print(f"{'='*40}\n")
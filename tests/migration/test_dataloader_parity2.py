"""
DataLoader Performance Comparison
Quick check: Old vs New loader load times
"""

import sys
from pathlib import Path
import time

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.strategies.core.data_loader import DataLoader as OldDataLoader

try:
    from src.strategies.specific.modules.data_loader import DataLoader as NewDataLoader
except ImportError:
    print("❌ New DataLoader not found")
    sys.exit(1)

config_path = PROJECT_ROOT / "configs/strategies/wbws/wbws_strategy_debug.yaml"
if not config_path.exists():
    print(f"❌ Config not found: {config_path}")
    sys.exit(1)

print("\n" + "="*50)
print("DATALOADER PERFORMANCE CHECK")
print("="*50)

# Old loader
start = time.perf_counter()
old = OldDataLoader(str(config_path))
old.load_config()
old.load_data()
old_time = time.perf_counter() - start
print(f"\n📦 Old DataLoader: {old_time*1000:.2f} ms")

# New loader
start = time.perf_counter()
new = NewDataLoader(str(config_path))
new.load_config()
new.load_data()
new_time = time.perf_counter() - start
print(f"📦 New DataLoader: {new_time*1000:.2f} ms")

# Result
diff = ((new_time - old_time) / old_time) * 100
print(f"\n{'='*50}")
if new_time <= old_time * 1.10:
    print(f"✅ PASS: New loader is {diff:+.1f}% of old (within 10%)")
else:
    print(f"❌ FAIL: New loader is {diff:+.1f}% slower (exceeds 10%)")
print("="*50 + "\n")
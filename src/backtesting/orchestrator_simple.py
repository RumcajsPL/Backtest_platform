#!/usr/bin/env python3
"""
MINIMAL TEST VERSION - orchestrator_simple.py
This will definitely show output to diagnose the issue
"""
import sys
import os
from pathlib import Path

print("=" * 80)
print("MINIMAL ORCHESTRATOR TEST - STARTING")
print(f"Python: {sys.executable}")
print(f"PID: {os.getpid()}")
print(f"CWD: {os.getcwd()}")
print(f"Args: {sys.argv}")
print("=" * 80)

# Test basic functionality
if len(sys.argv) < 2:
    print("ERROR: Missing config file argument")
    print(f"Usage: {sys.argv[0]} <config_yaml>")
    sys.exit(1)

config_path = Path(sys.argv[1])
print(f"Config path provided: {config_path}")
print(f"Config exists: {config_path.exists()}")

if not config_path.exists():
    print(f"ERROR: Config file not found: {config_path}")
    print("Trying common locations...")
    
    # Try to find it
    possible_locations = [
        Path("src/config/WBWS/wbws_backtest.yaml"),
        Path("configs/WBWS/wbws_backtest.yaml"),
        Path.cwd() / "wbws_backtest.yaml",
        Path(__file__).parent.parent / "config" / "WBWS" / "wbws_backtest.yaml"
    ]
    
    for loc in possible_locations:
        print(f"  Checking: {loc}")
        if loc.exists():
            print(f"  FOUND: {loc}")
            config_path = loc
            break
    else:
        print("ERROR: Could not find config file anywhere!")
        sys.exit(1)

# Read the config
try:
    import yaml
    print("\n✅ yaml module available")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    print(f"✅ Config loaded successfully from: {config_path}")
    print(f"Config keys: {list(config.keys())}")
    
except ImportError:
    print("❌ ERROR: pyyaml not installed. Install with: pip install pyyaml")
    sys.exit(1)
except Exception as e:
    print(f"❌ ERROR loading config: {e}")
    sys.exit(1)

# Show basic config info
print("\n📋 CONFIG SUMMARY:")
if 'backtest' in config:
    print(f"  Mode: {config['backtest'].get('mode', 'N/A')}")
if 'constraints' in config:
    print(f"  Constraints: {len(config['constraints'])} items")
if 'zones' in config:
    zones = config['zones']
    print(f"  Zones: {list(zones.keys())}")
    for zone_name, zone_data in zones.items():
        desc = zone_data.get('description', 'No description')
        print(f"    - {zone_name}: {desc}")

# Test creating a simple output
from datetime import datetime
output_dir = Path("outputs/test_simple")
output_dir.mkdir(parents=True, exist_ok=True)

test_file = output_dir / "test_output.txt"
with open(test_file, 'w') as f:
    f.write(f"Test run at: {datetime.now()}\n")
    f.write(f"Config: {config_path}\n")
    f.write(f"Zones: {list(config.get('zones', {}).keys())}\n")

print(f"\n✅ Created test file: {test_file}")

print("\n" + "=" * 80)
print("MINIMAL TEST COMPLETED SUCCESSFULLY!")
print("=" * 80)

# Keep terminal open
input("\nPress Enter to exit...")
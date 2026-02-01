# clean_strategy_yaml.py
import yaml
import numpy as np
from pathlib import Path

def clean_numpy_types(obj):
    """Convert numpy types to Python native types"""
    if isinstance(obj, dict):
        return {k: clean_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_numpy_types(v) for v in obj]
    elif isinstance(obj, (np.integer, np.floating)):
        return obj.item()  # Convert to Python int/float
    elif hasattr(obj, 'dtype'):  # numpy array
        return obj.tolist()
    else:
        return obj

# Load the file
strategy_path = Path("src/config/WBWS/wbws_rsi_strategy.yaml")
with open(strategy_path, 'r') as f:
    content = f.read()

# Remove numpy YAML tags
content = content.replace('!!python/object/apply:numpy.core.multiarray.scalar', '')
content = content.replace('!!python/object/apply:numpy._core.multiarray.scalar', '')

# Parse and clean
data = yaml.safe_load(content)
cleaned_data = clean_numpy_types(data)

# Save cleaned version
cleaned_path = Path("src/config/WBWS/wbws_rsi_strategy_clean.yaml")
with open(cleaned_path, 'w') as f:
    yaml.dump(cleaned_data, f, default_flow_style=False, sort_keys=False)

print(f"✅ Cleaned YAML saved to: {cleaned_path}")
print(f"   Original: {strategy_path}")
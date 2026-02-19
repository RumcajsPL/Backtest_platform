# tests/migration/test_config_validation.py
"""
Pytest-based validation for strategy configuration.
Run with: pytest tests/migration/test_config_validation.py -v
"""

import pytest
import yaml
from pathlib import Path
from pprint import pprint

# Add project root to path
import sys
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.config_schema import StrategyConfig, SpreadConfig, RiskConfig


# Fixture to load raw YAML for inspection
@pytest.fixture
def raw_config():
    """Load raw YAML to see what's actually being parsed"""
    config_path = PROJECT_ROOT / "configs" / "strategy_template.yaml"
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


# Fixture to get the config section we care about
@pytest.fixture
def spread_section(raw_config):
    """Extract just the spread section for focused testing"""
    return raw_config.get('trade_management', {}).get('spread', {})


def test_raw_yaml_loads(raw_config):
    """Test 1: Can we even load the YAML?"""
    assert raw_config is not None
    print("\n✅ Raw YAML loaded successfully")
    print("Trade management section:")
    pprint(raw_config.get('trade_management', {}))


def test_spread_section_exists(spread_section):
    """Test 2: Does the spread section exist with correct keys?"""
    assert spread_section is not None
    print("\n✅ Spread section found")
    print(f"Spread section content: {spread_section}")
    
    # Show what keys are present
    print(f"Keys in spread section: {list(spread_section.keys())}")


def test_spread_config_creation():
    """Test 3: Can we create a SpreadConfig directly with our values?"""
    
    # Try with the exact keys from schema
    try:
        config = SpreadConfig(
            enabled=True,
            spread_type="points",
            spread_value=1.0
        )
        print("\n✅ SpreadConfig created successfully with schema keys")
        print(f"  enabled={config.enabled}, type={config.spread_type}, value={config.spread_value}")
    except Exception as e:
        print(f"\n❌ Failed with schema keys: {e}")
    
    # Try with our YAML keys to see the difference
    try:
        # This will fail because 'type' and 'value' aren't valid fields
        config = SpreadConfig(
            enabled=True,
            type="points",
            value=1.0
        )
        print("\n✅ SpreadConfig created with YAML keys (should fail)")
    except Exception as e:
        print(f"\n✅ Correctly failed with YAML keys: {e}")
        print("   This confirms the schema expects 'spread_type' and 'spread_value'")


def test_full_config_load():
    """Test 4: Try loading the full config and catch the specific validation error"""
    config_path = PROJECT_ROOT / "configs" / "strategy_template.yaml"
    
    with pytest.raises(ValueError) as excinfo:
        config = StrategyConfig.from_yaml(config_path)
    
    print(f"\n✅ Got expected validation error: {excinfo.value}")
    
    # Now let's fix it dynamically and retry
    print("\nAttempting to fix and retry...")
    
    # Load raw, fix keys, try again
    with open(config_path, 'r') as f:
        raw = yaml.safe_load(f)
    
    # Fix the spread section
    if 'trade_management' in raw and 'spread' in raw['trade_management']:
        spread = raw['trade_management']['spread']
        if 'type' in spread:
            spread['spread_type'] = spread.pop('type')
        if 'value' in spread:
            spread['spread_value'] = spread.pop('value')
    
    # Now try to create config from fixed dict
    try:
        config = StrategyConfig.from_dict(raw)
        print("✅ Success! Config loads after fixing keys")
        print(f"  Spread: {config.trade_management.spread.spread_type} = {config.trade_management.spread.spread_value}")
    except Exception as e:
        print(f"❌ Still failing: {e}")
        raise


def test_risk_section_exists(raw_config):
    """Test 5: Verify risk section has required fields"""
    risk = raw_config.get('trade_management', {}).get('risk', {})
    print("\nRisk section content:")
    pprint(risk)
    
    required_fields = ['enabled', 'atr_length', 'sl_multiplier', 
                      'tp_multiplier', 'max_risk_percentile', 'allow_exceed_limit']
    
    for field in required_fields:
        assert field in risk, f"Missing required field: {field}"
    
    print("✅ All required risk fields present")


# Run this test to see exactly what's in your YAML
if __name__ == "__main__":
    print("="*60)
    print("CONFIG DEBUGGING TOOL")
    print("="*60)
    
    config_path = PROJECT_ROOT / "configs" / "strategy_template.yaml"
    
    # Dump raw YAML
    with open(config_path, 'r') as f:
        raw = yaml.safe_load(f)
    
    print("\n📄 RAW YAML STRUCTURE:")
    print("="*40)
    pprint(raw)
    
    print("\n🔍 SPREAD SECTION DETAIL:")
    print("="*40)
    spread = raw.get('trade_management', {}).get('spread', {})
    print(f"Keys present: {list(spread.keys())}")
    print(f"Values: {spread}")
    
    print("\n🔍 RISK SECTION DETAIL:")
    print("="*40)
    risk = raw.get('trade_management', {}).get('risk', {})
    print(f"Keys present: {list(risk.keys())}")
    print(f"Values: {risk}")
    
    print("\n✅ Run with: pytest tests/migration/test_config_validation.py -v -s")
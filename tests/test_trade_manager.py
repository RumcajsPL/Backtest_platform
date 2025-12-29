"""
Basic test script for TradeManager
Usage: python tests/test_trade_manager.py path/to/wbws_rsi_strategy.yaml

This script loads the provided YAML config but tests all 4 possible combinations of
position_control settings (close_on_opposite and pyramiding_enabled) for comprehensiveness.
It runs sequences of signals through the manager and verifies expected behavior.

Now with support for multiple positions in pyramiding mode.
"""

import sys
import os

# Debug prints to diagnose path issues
print("Current working directory:", os.getcwd())
print("Script path:", os.path.abspath(__file__))
print("Script directory:", os.path.dirname(os.path.abspath(__file__)))

# Add project root to sys.path to fix ModuleNotFoundError for 'src'
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
print("Calculated project root:", project_root)

if project_root not in sys.path:
    sys.path.insert(0, project_root)
    print("Added project root to sys.path")

print("Full sys.path:", sys.path)

import yaml
import copy
import pandas as pd
from datetime import datetime

try:
    from src.strategies.trade_management.trade_manager import TradeManager
    print("Successfully imported TradeManager")
except ModuleNotFoundError as e:
    print(f"Import failed: {e}")
    sys.exit(1)

def create_signal(ts_str: str, signal: str, entry: float, sl: float, tp: float) -> pd.Series:
    """Helper to create a mock signal row"""
    return pd.Series({
        'timestamp': datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S'),
        'signal': signal,
        'entry': entry,
        'sl': sl,
        'tp': tp
    })

def run_test_scenario(manager: TradeManager, signals: list, expected_results: list):
    """Run a sequence of signals and check against expected results"""
    for i, (signal, expected) in enumerate(zip(signals, expected_results)):
        result = manager.handle_signal(signal)
        
        # Check action
        assert result['action'] == expected['action'], \
            f"Signal {i+1}: Expected action {expected['action']}, got {result['action']}"
        
        # Check reason contains expected substring
        if 'reason_substr' in expected:
            assert expected['reason_substr'] in result['reason'], \
                f"Signal {i+1}: Reason '{result['reason']}' does not contain '{expected['reason_substr']}'"
        
        # Check open_trade if expected
        if expected.get('open_trade_id'):
            assert result['open_trade']['trade_id'] == expected['open_trade_id'], \
                f"Signal {i+1}: Expected open_trade_id {expected['open_trade_id']}, got {result['open_trade']['trade_id']}"
        
        # Check close_trades if expected
        if expected.get('close_trade_ids'):
            assert [ct['trade_id'] for ct in result['close_trades']] == expected['close_trade_ids'], \
                f"Signal {i+1}: Expected close_trade_ids {expected['close_trade_ids']}, got {[ct['trade_id'] for ct in result['close_trades']]}"
            assert [ct['pnl_points'] for ct in result['close_trades']] == expected['pnl_points'], \
                f"Signal {i+1}: Expected pnl_points {expected['pnl_points']}, got {[ct['pnl_points'] for ct in result['close_trades']]}"
        
        # Check current_positions after
        if expected.get('after_current_ids'):
            current = manager.get_current_positions()
            assert [p.trade_id for p in current] == expected['after_current_ids'], \
                f"Signal {i+1}: Expected current_ids {expected['after_current_ids']}, got {[p.trade_id for p in current]}"
            if expected.get('after_current_direction'):
                assert manager.current_direction == expected['after_current_direction'], \
                    f"Signal {i+1}: Expected direction {expected['after_current_direction']}, got {manager.current_direction}"
        else:
            assert not manager.has_open_position(), f"Signal {i+1}: Expected no open positions, but found {len(manager.current_positions)}"

def test_combination(config_template: dict, close_on_opposite: bool, pyramiding_enabled: bool):
    """Test a specific configuration combination"""
    print(f"\n=== Testing combination: close_on_opposite={close_on_opposite}, pyramiding_enabled={pyramiding_enabled} ===")
    
    config = copy.deepcopy(config_template)
    config['trade_management'] = config.get('trade_management', {})
    config['trade_management']['position_control'] = {
        'close_on_opposite': close_on_opposite,
        'pyramiding_enabled': pyramiding_enabled
    }
    
    manager = TradeManager(config)
    
    # Define test signals with varying prices
    signals = [
        create_signal('2025-12-01 10:00:00', 'BUY', 100.0, 95.0, 110.0),  # Signal 1: BUY @100
        create_signal('2025-12-01 11:00:00', 'BUY', 102.0, 97.0, 112.0),  # Signal 2: BUY @102 (same)
        create_signal('2025-12-01 12:00:00', 'SELL', 105.0, 110.0, 95.0)  # Signal 3: SELL @105 (opposite)
    ]
    
    # Expected results depend on config
    if not pyramiding_enabled and not close_on_opposite:
        expected = [
            # Signal 1: Open BUY1
            {'action': 'OPEN', 'open_trade_id': 1, 'after_current_ids': [1], 'after_current_direction': 'BUY'},
            # Signal 2: Reject same (pyramiding disabled)
            {'action': 'REJECT', 'reason_substr': 'Pyramiding disabled', 'after_current_ids': [1], 'after_current_direction': 'BUY'},
            # Signal 3: Reject opposite (ignore)
            {'action': 'REJECT', 'reason_substr': 'Opposite signal ignored', 'after_current_ids': [1], 'after_current_direction': 'BUY'}
        ]
    
    elif pyramiding_enabled and not close_on_opposite:
        expected = [
            # Signal 1: Open BUY1
            {'action': 'OPEN', 'open_trade_id': 1, 'after_current_ids': [1], 'after_current_direction': 'BUY'},
            # Signal 2: Open BUY2 (add to list)
            {'action': 'OPEN', 'open_trade_id': 2, 'after_current_ids': [1, 2], 'after_current_direction': 'BUY'},
            # Signal 3: Reject opposite (ignore), both remain open
            {'action': 'REJECT', 'reason_substr': 'Opposite signal ignored', 'after_current_ids': [1, 2], 'after_current_direction': 'BUY'}
        ]
    
    elif not pyramiding_enabled and close_on_opposite:
        expected = [
            # Signal 1: Open BUY1
            {'action': 'OPEN', 'open_trade_id': 1, 'after_current_ids': [1], 'after_current_direction': 'BUY'},
            # Signal 2: Reject same (pyramiding disabled)
            {'action': 'REJECT', 'reason_substr': 'Pyramiding disabled', 'after_current_ids': [1], 'after_current_direction': 'BUY'},
            # Signal 3: Close BUY1 and reverse to SELL2 (pnl=5)
            {'action': 'CLOSE_AND_REVERSE', 'close_trade_ids': [1], 'pnl_points': [5.0], 'open_trade_id': 2, 'after_current_ids': [2], 'after_current_direction': 'SELL'}
        ]
    
    elif pyramiding_enabled and close_on_opposite:
        expected = [
            # Signal 1: Open BUY1
            {'action': 'OPEN', 'open_trade_id': 1, 'after_current_ids': [1], 'after_current_direction': 'BUY'},
            # Signal 2: Open BUY2 (add)
            {'action': 'OPEN', 'open_trade_id': 2, 'after_current_ids': [1, 2], 'after_current_direction': 'BUY'},
            # Signal 3: Close both BUY1 (pnl=5) and BUY2 (pnl=3), reverse to SELL3
            {'action': 'CLOSE_AND_REVERSE', 'close_trade_ids': [1, 2], 'pnl_points': [5.0, 3.0], 'open_trade_id': 3, 'after_current_ids': [3], 'after_current_direction': 'SELL'}
        ]
    
    try:
        run_test_scenario(manager, signals, expected)
        
        # After signals, test manual close if positions open
        if manager.has_open_position():
            current = manager.get_current_positions()
            for pos in current[:]:  # Copy to avoid modification during iteration
                exit_time = datetime.strptime('2025-12-01 13:00:00', '%Y-%m-%d %H:%M:%S')
                exit_price = 108.0 if pos.direction == 'BUY' else 98.0
                exit_reason = 'TP'
                close_trade = manager.close_position_on_exit(pos.trade_id, exit_time, exit_price, exit_reason)
                
                assert close_trade is not None, f"Expected close_trade for {pos.trade_id}, got None"
                assert close_trade['exit_reason'] == exit_reason, f"Expected reason {exit_reason}, got {close_trade['exit_reason']}"
                
                # Check pnl
                expected_pnl = exit_price - close_trade['entry_price'] if close_trade['direction'] == 'BUY' else close_trade['entry_price'] - exit_price
                assert close_trade['pnl_points'] == expected_pnl, f"Expected pnl {expected_pnl} for {pos.trade_id}, got {close_trade['pnl_points']}"
            
            assert not manager.has_open_position(), "All positions should be closed"
        
        # Check metrics
        metrics = manager.get_metrics()
        expected_accepted = sum(1 for e in expected if e['action'] in ['OPEN', 'CLOSE_AND_REVERSE'])
        assert metrics['signals_accepted'] == expected_accepted, f"Expected accepted {expected_accepted}, got {metrics['signals_accepted']}"
        assert metrics['total_signals_received'] == len(signals), f"Expected total {len(signals)}, got {metrics['total_signals_received']}"
        
        print("✅ All checks passed for this combination")
    
    except AssertionError as e:
        print(f"❌ Test failed: {str(e)}")

def main():
    if len(sys.argv) != 2:
        print("Usage: python test_trade_manager.py path/to/yaml")
        sys.exit(1)
    
    yaml_path = sys.argv[1]
    if not os.path.exists(yaml_path):
        print(f"Error: YAML file not found: {yaml_path}")
        sys.exit(1)
    
    with open(yaml_path, 'r') as f:
        config_template = yaml.safe_load(f)
    
    print(f"Loaded YAML from: {yaml_path}")
    print("Running comprehensive tests for all position_control combinations...")
    
    # Test all 4 combinations
    test_combination(config_template, False, False)
    test_combination(config_template, False, True)
    test_combination(config_template, True, False)
    test_combination(config_template, True, True)
    
    # Additional test: reset
    print("\n=== Testing reset ===")
    config = copy.deepcopy(config_template)
    manager = TradeManager(config)
    signal1 = create_signal('2025-12-01 10:00:00', 'BUY', 100.0, 95.0, 110.0)
    manager.handle_signal(signal1)
    assert manager.has_open_position(), "Position should be open before reset"
    manager.reset()
    assert not manager.has_open_position(), "Position should be closed after reset"
    assert manager.get_metrics()['total_signals_received'] == 0, "Metrics should be reset"
    print("✅ Reset test passed")

if __name__ == "__main__":
    main()
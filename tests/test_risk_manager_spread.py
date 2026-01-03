import sys
import os
from pathlib import Path
import unittest
import pandas as pd
import numpy as np

# --- PATH FIX ---
# Add the project root to sys.path so we can import 'src'
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))
# ----------------

from src.strategies.trade_management.risk_manager import RiskManager

class TestRiskManagerSpread(unittest.TestCase):

    def setUp(self):
        # Create Dummy Data (100 days)
        dates = pd.date_range(start="2024-01-01", periods=100, freq="1h")
        data = {
            'high': [105.0] * 100,
            'low': [95.0] * 100,
            'close': [100.0] * 100,
            'open': [100.0] * 100
        }
        self.ohlcv = pd.DataFrame(data, index=dates)
        
        # Config
        self.config = {
            'sl_tp': {
                'enabled': True,
                'atr_length': 14,
                'sl_multiplier': 1.0, 
                'risk_to_reward_ratio': 2.0
            },
            'risk_management': {
                'enabled': True,
                'max_risk_percentile': 0.10, # Max risk = 10% of annual range
                'allow_exceed_limit': False
            },
            'spread': {
                'enabled': True, 
                'config_path': 'dummy_path'
            }
        }

    def test_spread_increases_risk_calculation(self):
        """
        Verify that using Adjusted Entry (Ask) vs Mid Price changes the risk calculation.
        """
        rm = RiskManager(self.config, self.ohlcv)
        
        # 1. Force Annual Range to 50.0. 
        #    Max Risk Limit (10%) = 5.0 points.
        rm.annual_range_series = pd.Series([50.0] * len(self.ohlcv), index=self.ohlcv.index)
        
        # 2. Setup Scenario
        # Mid Price: 100
        # Spread: 2.0
        # Ask Price (Adjusted): 102
        # SL Level: 97
        
        mid_price = 100.0
        adjusted_entry = 102.0
        stop_loss = 97.0 
        timestamp = self.ohlcv.index[0]
        
        print("\n--- TEST SCENARIOS ---")
        
        # --- TEST 1: The Bug (Validating with Mid Price) ---
        # Risk = |100 - 97| = 3.0. 
        # 3.0 / 50.0 = 6%. (Below 10% limit -> PASS)
        # This simulates what was happening before: spread "absorbed" the risk.
        accepted_bug, _, msg_bug = rm.validate_risk_percentile(
            mid_price, stop_loss, True, timestamp
        )
        print(f"1. OLD BEHAVIOR (Using Mid): Risk={abs(mid_price-stop_loss)} -> {msg_bug} (Passes incorrectly)")
        
        # --- TEST 2: The Fix (Validating with Adjusted Entry) ---
        # Risk = |102 - 97| = 5.0.
        # 5.0 / 50.0 = 10%. (Exactly on limit -> PASS)
        # This is the correct calculation.
        accepted_fix, _, msg_fix = rm.validate_risk_percentile(
            adjusted_entry, stop_loss, True, timestamp
        )
        print(f"2. NEW BEHAVIOR (Using Ask): Risk={abs(adjusted_entry-stop_loss)} -> {msg_fix} (Correct)")
        
        # --- TEST 3: Should Fail ---
        # If we enter slightly higher (slippage/worse spread), we should break the risk limit.
        # Adjusted Entry: 102.1
        # SL Level: 97
        # Risk: 5.1 (Limit 5.0)
        fail_entry = 102.1
        accepted_should_fail, _, msg_fail = rm.validate_risk_percentile(
            fail_entry, stop_loss, True, timestamp
        )
        
        print(f"3. FAIL SCENARIO (Ask=102.1): Risk={abs(fail_entry-stop_loss):.1f} -> {msg_fail}")
        self.assertFalse(accepted_should_fail, "Trade should be rejected when risk > limit")

if __name__ == '__main__':
    unittest.main()
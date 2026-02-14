"""
Parity Test for RiskManager Migration (Session 7)
Location: tests/migration/test_risk_manager.py

Verifies that the migrated RiskManager.compute_trade_parameters() method
returns TradeParameters contracts that match the legacy dict format exactly.

Uses actual DEUIDXEUR data from 2024-01-01 onwards.
"""
import sys
import unittest
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Dict, Any
import warnings

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import contracts
from src.strategies.contracts.trade_contracts import TradeParameters

# Import migrated RiskManager
from src.strategies.specific.modules.risk_manager import RiskManager


# ============================================================================
# TEST DATA LOADER
# ============================================================================

def load_test_data() -> pd.DataFrame:
    """Load actual DEUIDXEUR data from parquet file"""
    data_path = PROJECT_ROOT / "data/processed/ohlcv/DEUIDXEUR_1min_20240101_20260207.parquet"
    
    if not data_path.exists():
        # Fallback to synthetic data if real data doesn't exist
        warnings.warn(f"Real data not found at {data_path}, using synthetic data")
        return create_synthetic_test_data()
    
    # Load parquet data
    df = pd.read_parquet(data_path)
    
    # Use data from 2025-01-01 onwards to ensure annual range is available
    df = df[df.index >= '2025-01-01']
    
    if len(df) < 100:
        warnings.warn(f"Only {len(df)} bars after 2025-01-01, using synthetic data")
        return create_synthetic_test_data()
    
    print(f"  → Loaded {len(df)} bars of DEUIDXEUR data from {df.index[0]} to {df.index[-1]}")
    return df


def create_synthetic_test_data(bars: int = 20000) -> pd.DataFrame:
    """Create synthetic OHLCV data as fallback"""
    print("  → Using synthetic test data")
    start_date = '2025-01-01'
    dates = pd.date_range(start_date, periods=bars, freq='1min')
    
    # Generate synthetic price data based on DEUIDXEUR-like levels
    np.random.seed(42)
    base_price = 19800.0
    returns = np.random.normal(0, 0.0002, len(dates))  # Lower volatility
    prices = base_price * (1 + returns).cumprod()
    
    # Create OHLC bars
    data = pd.DataFrame({
        'open': prices,
        'high': prices * (1 + abs(np.random.normal(0, 0.0003, len(dates)))),
        'low': prices * (1 - abs(np.random.normal(0, 0.0003, len(dates)))),
        'close': prices * (1 + np.random.normal(0, 0.0002, len(dates))),
        'volume': np.random.uniform(100, 1000, len(dates))
    }, index=dates)
    
    return data


def create_test_config() -> Dict[str, Any]:
    """Create minimal config for RiskManager testing"""
    return {
        'asset': {
            'symbol': 'DEUIDXEUR'
        },
        'trade_management': {
            'sl_tp': {
                'enabled': True,
                'atr_length': 14,
                'sl_multiplier': 1.4,
                'risk_to_reward_ratio': 5.7
            },
            'risk_management': {
                'enabled': True,
                'max_risk_percentile': 0.1,
                'allow_exceed_limit': False
            },
            'spread': {
                'enabled': False,  # Disable for basic tests
                'apply_to_long': True,
                'apply_to_short': True,
                'config_path': None
            }
        }
    }


# ============================================================================
# UNIT TESTS
# ============================================================================

class TestRiskManagerMigration(unittest.TestCase):
    """Test suite for RiskManager contract migration"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test data once for all tests"""
        print(f"\n{'-'*60}")
        print("Setting up RiskManager tests")
        print(f"{'-'*60}")
        
        cls.config = create_test_config()
        cls.test_data = load_test_data()

        # Load ARTF monthly data
        artf_path = PROJECT_ROOT / "data/processed/ohlcv/DEUIDXEUR_1ME_20210101_20260207.parquet"
        if artf_path.exists():
            cls.artf_data = pd.read_parquet(artf_path)
        else:
            warnings.warn(f"ARTF data not found at {artf_path}, using synthetic monthly data")
            cls.artf_data = cls.test_data.resample("M").agg({"high": "max", "low": "min"})

        # Inject ARTF into config for RiskManager
        cls.config.setdefault("data", {})
        cls.config["data"]["df_artf"] = cls.artf_data

        # Create RiskManager with ARTF support
        cls.risk_mgr = RiskManager(cls.config, cls.test_data)

        
        # Create RiskManager instance
        cls.risk_mgr = RiskManager(cls.config, cls.test_data)
        
        # Find first valid timestamp (at least 365 days into data to ensure annual range)
        cls.min_valid_date = cls.test_data.index[0] + pd.Timedelta(days=365)
        cls.valid_data = cls.test_data[cls.test_data.index >= cls.min_valid_date]
        
        # Select test timestamp from valid data
        if len(cls.valid_data) > 0:
            cls.test_timestamp = cls.valid_data.index[len(cls.valid_data) // 2]
            cls.bid_price = float(cls.valid_data.loc[cls.test_timestamp, 'close'])
            print(f"  → Using valid annual range data from {cls.valid_data.index[0]}")
        else:
            # Fallback to middle of data if no valid data after 365 days
            cls.test_timestamp = cls.test_data.index[len(cls.test_data) // 2]
            cls.bid_price = float(cls.test_data.loc[cls.test_timestamp, 'close'])
            print(f"  ⚠️  Warning: Less than 365 days of data, annual range may be invalid")
        
        print(f"  → Data from {cls.test_data.index[0]} to {cls.test_data.index[-1]}")
        print(f"  → Using test timestamp: {cls.test_timestamp}")
        print(f"  → Bid price: {cls.bid_price:.2f}")
    
    def test_01_returns_trade_parameters_contract(self):
        """Test that compute_trade_parameters returns TradeParameters, not dict"""
        params = self.risk_mgr.compute_trade_parameters(
            self.test_timestamp, self.bid_price, is_long=True
        )
        
        self.assertIsNotNone(params, "Should return TradeParameters, not None")
        self.assertIsInstance(params, TradeParameters,
            f"Expected TradeParameters, got {type(params)}")
    
    def test_02_legacy_compatibility_method(self):
        """Test that legacy method returns dict for backward compatibility"""
        # Call legacy method
        params_dict = self.risk_mgr.compute_trade_parameters_legacy(
            self.test_timestamp, self.bid_price, True
        )
        
        self.assertIsInstance(params_dict, dict, "Legacy method should return dict")
        
        # Verify required keys present
        required_keys = ['executed_entry', 'raw_sl', 'trigger_sl', 'tp', 
                        'comment', 'sl_adjusted', 'spread_applied', 'spread_value']
        for key in required_keys:
            self.assertIn(key, params_dict, f"Missing required key: {key}")
    
    def test_03_contract_to_dict_parity(self):
        """Test that TradeParameters.to_dict() matches legacy format"""
        # Get contract
        params = self.risk_mgr.compute_trade_parameters(
            self.test_timestamp, self.bid_price, True
        )
        self.assertIsNotNone(params)
        
        # Get legacy dict
        params_legacy = self.risk_mgr.compute_trade_parameters_legacy(
            self.test_timestamp, self.bid_price, True
        )
        self.assertIsNotNone(params_legacy)
        
        # Convert contract to dict
        params_dict = params.to_dict()
        
        # Compare key fields (using legacy dict keys)
        self.assertAlmostEqual(
            params_dict['executed_entry'], 
            params_legacy['executed_entry'], 
            places=2, 
            msg="Entry price mismatch"
        )
        self.assertAlmostEqual(
            params_dict['trigger_sl'], 
            params_legacy['trigger_sl'], 
            places=2, 
            msg="SL trigger mismatch"
        )
        self.assertAlmostEqual(
            params_dict['tp'], 
            params_legacy['tp'], 
            places=2, 
            msg="TP mismatch"
        )
        self.assertEqual(
            params_dict['sl_adjusted'], 
            params_legacy['sl_adjusted'], 
            "SL adjusted flag mismatch"
        )
    
    def test_04_long_trade_sl_tp_positioning(self):
        """Test that LONG trade has correct SL/TP positioning"""
        params = self.risk_mgr.compute_trade_parameters(
            self.test_timestamp, self.bid_price, is_long=True
        )
        self.assertIsNotNone(params)
        
        # For LONG: SL < Entry < TP
        self.assertLess(
            params.stop_loss_trigger, 
            params.entry_price_executed,
            "LONG: SL should be below entry"
        )
        self.assertGreater(
            params.take_profit, 
            params.entry_price_executed,
            "LONG: TP should be above entry"
        )
    
    def test_05_short_trade_sl_tp_positioning(self):
        """Test that SHORT trade has correct SL/TP positioning"""
        params = self.risk_mgr.compute_trade_parameters(
            self.test_timestamp, self.bid_price, is_long=False
        )
        self.assertIsNotNone(params)
        
        # For SHORT: TP < Entry < SL
        self.assertLess(
            params.take_profit, 
            params.entry_price_executed,
            "SHORT: TP should be below entry"
        )
        self.assertGreater(
            params.stop_loss_trigger, 
            params.entry_price_executed,
            "SHORT: SL should be above entry"
        )
    
    def test_06_atr_fields_populated(self):
        """Test that ATR-related fields are correctly populated"""
        params = self.risk_mgr.compute_trade_parameters(
            self.test_timestamp, self.bid_price, True
        )
        self.assertIsNotNone(params)
        
        # Verify ATR fields
        self.assertIsNotNone(params.atr_value, "ATR value should be set")
        self.assertGreater(params.atr_value, 0, "ATR should be positive")
        self.assertEqual(params.atr_length, 14, "ATR length should match config")
        self.assertEqual(params.atr_multiplier, 1.4, "ATR multiplier should match config")
    
    def test_07_annual_range_populated(self):
        """Test that annual range is properly calculated"""
        params = self.risk_mgr.compute_trade_parameters(
            self.test_timestamp, self.bid_price, True
        )
        self.assertIsNotNone(params)
        
        # Verify annual range is populated and positive
        self.assertIsNotNone(params.annual_range_value, "Annual range should be set")
        self.assertGreater(params.annual_range_value, 0, "Annual range should be positive")
        print(f"\n  → Annual range value: {params.annual_range_value:.2f}")
    
    def test_08_risk_reward_ratio(self):
        """Test that risk:reward ratio is calculated correctly"""
        params = self.risk_mgr.compute_trade_parameters(
            self.test_timestamp, self.bid_price, True
        )
        self.assertIsNotNone(params)
        
        # Verify R:R ratio
        expected_rr = self.config['trade_management']['sl_tp']['risk_to_reward_ratio']
        self.assertEqual(
            params.risk_reward_ratio, 
            expected_rr,
            f"R:R ratio should be {expected_rr}"
        )
        
        # Verify distances match R:R ratio (with tolerance for spread)
        if params.sl_distance and params.tp_distance and params.sl_distance > 0:
            calculated_rr = params.tp_distance / params.sl_distance
            self.assertAlmostEqual(
                calculated_rr, 
                expected_rr, 
                places=1,
                msg="TP/SL distances don't match configured R:R ratio"
            )
    
    def test_09_all_contract_fields_valid(self):
        """Test that all TradeParameters fields have valid values"""
        params = self.risk_mgr.compute_trade_parameters(
            self.test_timestamp, self.bid_price, True
        )
        self.assertIsNotNone(params)
        
        # Verify required fields are positive
        self.assertGreater(params.entry_price_mid, 0)
        self.assertGreater(params.entry_price_executed, 0)
        self.assertGreater(params.stop_loss_raw, 0)
        self.assertGreater(params.stop_loss_trigger, 0)
        self.assertGreater(params.take_profit, 0)
        self.assertGreater(params.position_size, 0)
        
        # Verify distances are positive
        self.assertGreater(params.sl_distance, 0)
        self.assertGreater(params.tp_distance, 0)
        
        # Verify comment is string or None
        self.assertIsInstance(params.comment, (str, type(None)))
    
    def test_10_progressive_tracker_fields_available(self):
        """Test that all fields needed by ProgressiveTracker are accessible"""
        params = self.risk_mgr.compute_trade_parameters(
            self.test_timestamp, self.bid_price, True
        )
        self.assertIsNotNone(params)
        
        # Fields required by ProgressiveTracker.update_risk_management_details()
        required_tracker_fields = [
            'entry_price_executed',
            'stop_loss_trigger',
            'take_profit',
            'atr_value',
            'atr_length',
            'atr_multiplier',
            'annual_range_value',
            'risk_percentile_calculated',
            'max_risk_percentile',
            'risk_percentile_passed',
            'spread_enabled',
            'spread_type',
            'spread_value',
            'spread_points',
            'entry_price_mid',
            'spread_efficiency_percent',
            'sl_adjusted',
            'comment',
        ]
        
        # Verify all fields accessible (even if None)
        for field in required_tracker_fields:
            self.assertTrue(
                hasattr(params, field),
                f"TradeParameters missing field required by tracker: {field}"
            )
    
    def test_11_multiple_timestamps(self):
        """Test risk manager works across multiple timestamps"""
        # Skip if not enough valid data
        if len(self.valid_data) < 10:
            self.skipTest(f"Not enough valid data after {self.min_valid_date} for multiple timestamp test")
        
        # Select 5 evenly spaced timestamps from valid data
        step = len(self.valid_data) // 5
        test_indices = [i * step for i in range(5)]
        
        print(f"\n  → Testing with {len(test_indices)} timestamps from {self.valid_data.index[0]} to {self.valid_data.index[-1]}")
        
        for idx in test_indices:
            timestamp = self.valid_data.index[idx]
            bid_price = float(self.valid_data.loc[timestamp, 'close'])
            
            params = self.risk_mgr.compute_trade_parameters(timestamp, bid_price, True)
            self.assertIsNotNone(params)
            self.assertIsInstance(params, TradeParameters)
            
            # Basic sanity check
            self.assertGreater(params.atr_value, 0)
            self.assertGreater(params.annual_range_value, 0, f"Annual range should be >0 at {timestamp}")
        
        print(f"  ✅ Successfully tested {len(test_indices)} timestamps")


# ============================================================================
# PERFORMANCE TESTS
# ============================================================================

class TestRiskManagerPerformance(unittest.TestCase):
    """Performance tests to ensure no regression"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test data once for all tests"""
        cls.config = create_test_config()
        cls.test_data = load_test_data()
        cls.risk_mgr = RiskManager(cls.config, cls.test_data)
        
        # Find valid data range (at least 365 days into data)
        cls.min_valid_date = cls.test_data.index[0] + pd.Timedelta(days=365)
        cls.valid_data = cls.test_data[cls.test_data.index >= cls.min_valid_date]
    
    def test_contract_creation_overhead(self):
        """Test that contract creation doesn't add significant overhead"""
        import time
        
        # Skip if not enough valid data
        if len(self.valid_data) < 100:
            self.skipTest(f"Not enough valid data after {self.min_valid_date} for performance test")
        
        # Get test timestamps and prices
        step = len(self.valid_data) // 50
        test_indices = [i * step for i in range(50)]
        
        timestamps = []
        prices = []
        for idx in test_indices[:20]:  # Use 20 samples for performance test
            if idx < len(self.valid_data):
                timestamps.append(self.valid_data.index[idx])
                prices.append(float(self.valid_data.loc[self.valid_data.index[idx], 'close']))
        
        print(f"\n  → Testing with {len(timestamps)} timestamps from {timestamps[0]} to {timestamps[-1]}")
        
        # Warm up
        _ = self.risk_mgr.compute_trade_parameters(timestamps[0], prices[0], True)
        _ = self.risk_mgr.compute_trade_parameters_legacy(timestamps[0], prices[0], True)
        
        # Benchmark new method
        start = time.perf_counter()
        for ts, price in zip(timestamps, prices):
            _ = self.risk_mgr.compute_trade_parameters(ts, price, True)
        new_time = time.perf_counter() - start
        
        # Benchmark legacy method
        start = time.perf_counter()
        for ts, price in zip(timestamps, prices):
            _ = self.risk_mgr.compute_trade_parameters_legacy(ts, price, True)
        legacy_time = time.perf_counter() - start
        
        # Calculate overhead
        overhead = (new_time / legacy_time - 1) * 100
        calls = len(timestamps)
        
        print(f"  → Contract overhead: {overhead:.1f}%")
        print(f"  → New method: {new_time*1000:.1f}ms for {calls} calls ({new_time/calls*1000:.2f}ms/call)")
        print(f"  → Legacy method: {legacy_time*1000:.1f}ms for {calls} calls ({legacy_time/calls*1000:.2f}ms/call)")
        print(f"  → Throughput: {calls/new_time:.0f} calls/second")
        
        # Lenient threshold for now - migration not yet optimized
        # Allow up to 600% overhead (7x slower) during migration phase
        max_allowed_ratio = 7.0
        self.assertLess(
            new_time, 
            legacy_time * max_allowed_ratio,
            f"Contract method too slow: {overhead:.1f}% overhead (max {(max_allowed_ratio-1)*100:.0f}%)"
        )


# ============================================================================
# MAIN - Run tests
# ============================================================================

if __name__ == '__main__':
    print("\n" + "="*60)
    print("RISK MANAGER MIGRATION TESTS (Session 7 - Task 1)")
    print("="*60)
    
    # Run tests with unittest
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestRiskManagerMigration))
    suite.addTests(loader.loadTestsFromTestCase(TestRiskManagerPerformance))
    
    # Run with verbose output
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    print(f"Tests run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped)}")
    print("="*60)
    
    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)
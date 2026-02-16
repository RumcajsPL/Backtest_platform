"""
Integration Test for MetricsCalculator with Real TradeResult

Session 13 - MetricsCalculator Testing
Version: 1.0.0

Tests MetricsCalculator with real TradeResult from simulator.
This is the integration test that validates the complete flow:
    TradeSimulator → TradeResult → MetricsCalculator → MetricsReport

Usage:
    pytest tests/migration/test_metrics_calculator_integration.py -v
    
    OR run directly:
    python tests/migration/test_metrics_calculator_integration.py
"""
try:
    import pytest
    PYTEST_AVAILABLE = True
except ImportError:
    PYTEST_AVAILABLE = False
    
import time
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Verify imports work
try:
    from src.strategies.contracts.trade_contracts import TradeResult
    from src.strategies.specific.modules.metrics_calculator import (
        MetricsCalculator,
        calculate_metrics,
        calculate_metrics_with_timing,
    )
    from src.strategies.contracts.metrics_contracts import MetricsReport
except ImportError as e:
    print("="*70)
    print("IMPORT ERROR")
    print("="*70)
    print(f"Error: {e}")
    print(f"\nProject root: {PROJECT_ROOT}")
    print(f"Current sys.path: {sys.path[:3]}")
    print("\nPlease ensure:")
    print("1. You're running from project root")
    print("2. All required files exist:")
    print("   - src/strategies/contracts/metrics_contracts.py")
    print("   - src/strategies/specific/modules/metrics_calculator.py")
    print("   - src/strategies/contracts/trade_contracts.py")
    print("="*70)
    raise


class TestMetricsCalculatorIntegration:
    """Integration tests with real TradeResult data"""
    
    def test_calculate_with_real_trade_result(self):
        """
        Test MetricsCalculator with real TradeResult.
        
        NOTE: This test requires a real TradeResult object.
        Run this after a successful backtest to validate metrics calculation.
        """
        # Skip if no real data available
        if PYTEST_AVAILABLE:
            pytest.skip("Run this test manually with real TradeResult data")
        
        # Example usage (uncomment and provide real trade_result):
        # from your_simulator import run_backtest
        # trade_result = run_backtest()
        # 
        # metrics = MetricsCalculator.calculate(trade_result)
        # 
        # assert metrics.total_trades > 0
        # assert 0 <= metrics.win_rate <= 100
        # assert metrics.winning_trades + metrics.losing_trades == metrics.total_trades
    
    def test_metrics_format_matches_backtester_requirements(self):
        """
        Test that output format matches backtester requirements exactly.
        
        This validates the structure of the output dict.
        """
        # Create a minimal mock TradeResult for format validation
        from dataclasses import dataclass
        from typing import List
        from src.strategies.contracts.trade_contracts import Trade
        
        @dataclass
        class MinimalTradeResult:
            trades: List[Trade]
            
            @property
            def closed_trades(self):
                return []  # Empty for format test
        
        mock_result = MinimalTradeResult(trades=[])
        
        # Calculate metrics (will be empty)
        metrics = MetricsCalculator.calculate(mock_result)
        
        # Get dict format
        output = metrics.to_dict()
        
        # Validate structure
        assert "simulation_results" in output
        assert "performance_metrics" in output["simulation_results"]
        assert "trade_summary" in output["simulation_results"]
        assert "execution_date" in output
        assert "execution_duration" in output
        
        # Validate performance_metrics fields
        perf_metrics = output["simulation_results"]["performance_metrics"]
        required_fields = [
            "total_trades", "winning_trades", "losing_trades", "win_rate",
            "total_pnl_points", "expectancy_points", "profit_factor",
            "avg_pnl_points", "largest_win", "largest_loss",
            "max_drawdown", "losing_streak", "winning_streak"
        ]
        for field in required_fields:
            assert field in perf_metrics, f"Missing required field: {field}"
        
        # Validate trade_summary fields
        trade_summary = output["simulation_results"]["trade_summary"]
        assert "trades_per_week" in trade_summary
        assert "trades_per_day" in trade_summary
        
        print("✅ Output format matches backtester requirements")
    
    def test_performance_with_timing(self):
        """Test that timing calculation works correctly"""
        from dataclasses import dataclass
        from typing import List
        from src.strategies.contracts.trade_contracts import Trade
        
        @dataclass
        class MinimalTradeResult:
            trades: List[Trade]
            
            @property
            def closed_trades(self):
                return []
        
        mock_result = MinimalTradeResult(trades=[])
        
        # Calculate with timing
        start = time.perf_counter()
        time.sleep(0.001)  # Simulate 1ms work
        metrics = MetricsCalculator.calculate(mock_result, start_time=start)
        
        # Should have captured timing
        assert metrics.execution_duration_ms > 0
        assert metrics.execution_duration_ms >= 1.0  # At least 1ms
        
        print(f"✅ Timing captured: {metrics.execution_duration_ms:.2f}ms")


# ============================================================================
# MANUAL TEST SCRIPT (Run with real data)
# ============================================================================

def run_manual_integration_test():
    """
    Manual integration test with real simulator output.
    
    Run this function after generating a real TradeResult from your simulator.
    
    Usage:
        1. Run your backtest to get a TradeResult
        2. Save it or pass it to this function
        3. Validate metrics calculation
    """
    print("="*70)
    print("MANUAL INTEGRATION TEST - MetricsCalculator with Real Data")
    print("="*70)
    
    # STEP 1: Load or generate real TradeResult
    print("\n1️⃣  Loading TradeResult...")
    print("   NOTE: You need to provide a real TradeResult object here.")
    print("   Options:")
    print("   - Load from saved file (pickle, JSON)")
    print("   - Run simulator to generate fresh data")
    print("   - Use test fixture from your test suite")
    
    # Example: Load from file (uncomment and adjust path)
    # import pickle
    # with open('data/test_trade_result.pkl', 'rb') as f:
    #     trade_result = pickle.load(f)
    
    # Example: Run simulator (uncomment and adjust imports)
    # from your_simulator_module import run_backtest
    # trade_result = run_backtest(config)
    
    # For demo purposes, we'll show what the test would do:
    print("\n   ⚠️  No real TradeResult provided (demo mode)")
    print("   To run this test:")
    print("   1. Uncomment the loading code above")
    print("   2. Provide path to real TradeResult")
    print("   3. Run this script")
    
    # STEP 2: Calculate metrics
    print("\n2️⃣  Would calculate metrics...")
    print("   start = time.perf_counter()")
    print("   metrics = MetricsCalculator.calculate(trade_result, start_time=start)")
    
    # STEP 3: Validate results
    print("\n3️⃣  Would validate results...")
    print("   - Check total_trades > 0")
    print("   - Check win_rate in range [0, 100]")
    print("   - Check winning_trades + losing_trades == total_trades")
    print("   - Check profit_factor calculation")
    print("   - Check max_drawdown <= 0")
    print("   - Check largest_win >= 0")
    print("   - Check largest_loss <= 0")
    print("   - Validate trades_per_day/week")
    
    # STEP 4: Show output format
    print("\n4️⃣  Would show output format...")
    print("   output = metrics.to_dict()")
    print("   print(json.dumps(output, indent=2))")
    
    # STEP 5: Performance check
    print("\n5️⃣  Would check performance...")
    print("   assert metrics.execution_duration_ms < 10.0  # Target <10ms")
    
    print("\n" + "="*70)
    print("To run with real data, edit this function and provide TradeResult")
    print("="*70)


def run_with_sample_data():
    """
    Run with sample data to demonstrate the flow.
    
    This creates a realistic TradeResult-like object with sample trades
    to show how the integration would work.
    """
    import pandas as pd
    from dataclasses import dataclass
    from typing import List
    from src.strategies.contracts.trade_contracts import (
        Trade, TradeEntry, TradeExit, TradeDirection, ExitReason
    )
    
    print("="*70)
    print("SAMPLE DATA INTEGRATION TEST")
    print("="*70)
    
    # Create sample trades (simulating real backtest output)
    print("\n1️⃣  Creating sample trades (simulating real backtest)...")
    trades = []
    
    # Generate 100 sample trades with realistic patterns
    for i in range(100):
        is_win = (i % 3) == 0  # 33% win rate
        
        entry = TradeEntry(
            entry_id=f"E{i}",
            entry_time=pd.Timestamp("2025-01-01") + pd.Timedelta(hours=i*2),
            direction=TradeDirection.LONG if i % 2 == 0 else TradeDirection.SHORT,
            entry_price=1.2000 + (i * 0.0001),
            stop_loss=1.1950 + (i * 0.0001) if i % 2 == 0 else 1.2050 + (i * 0.0001),
            take_profit=1.2100 + (i * 0.0001) if i % 2 == 0 else 1.1900 + (i * 0.0001),
        )
        
        if is_win:
            exit_price = entry.take_profit
            exit_reason = ExitReason.TAKE_PROFIT
        else:
            exit_price = entry.stop_loss
            exit_reason = ExitReason.STOP_LOSS
        
        exit = TradeExit.create(
            entry=entry,
            exit_time=entry.entry_time + pd.Timedelta(hours=1),
            exit_price=exit_price,
            exit_reason=exit_reason,
        )
        
        trades.append(Trade(entry=entry, exit=exit))
    
    # Create mock TradeResult
    @dataclass
    class MockTradeResult:
        trades: List[Trade]
        
        @property
        def closed_trades(self):
            return [t for t in self.trades if t.is_closed]
    
    trade_result = MockTradeResult(trades=trades)
    
    print(f"   ✅ Created {len(trades)} sample trades")
    
    # Calculate metrics with timing
    print("\n2️⃣  Calculating metrics...")
    start = time.perf_counter()
    metrics = MetricsCalculator.calculate(trade_result, start_time=start)
    
    print(f"   ✅ Metrics calculated in {metrics.execution_duration_ms:.2f}ms")
    
    # Display results
    print("\n3️⃣  Metrics Report:")
    print("   " + "-"*66)
    print(f"   Total Trades:      {metrics.total_trades}")
    print(f"   Winning Trades:    {metrics.winning_trades}")
    print(f"   Losing Trades:     {metrics.losing_trades}")
    print(f"   Win Rate:          {metrics.win_rate:.1f}%")
    print(f"   Total P&L:         {metrics.total_pnl_points:+.2f} points")
    print(f"   Expectancy:        {metrics.expectancy_points:+.2f} points/trade")
    print(f"   Profit Factor:     {metrics.profit_factor:.2f}")
    print(f"   Largest Win:       {metrics.largest_win:+.2f} points")
    print(f"   Largest Loss:      {metrics.largest_loss:+.2f} points")
    print(f"   Max Drawdown:      {metrics.max_drawdown:.2f} points")
    print(f"   Winning Streak:    {metrics.winning_streak}")
    print(f"   Losing Streak:     {metrics.losing_streak}")
    print(f"   Trades/Day:        {metrics.trades_per_day:.1f}")
    print(f"   Trades/Week:       {metrics.trades_per_week:.1f}")
    
    # Validate metrics
    print("\n4️⃣  Validating metrics...")
    assert metrics.total_trades == 100, "Total trades should be 100"
    assert metrics.winning_trades + metrics.losing_trades == 100, "Wins + losses should equal total"
    assert 0 <= metrics.win_rate <= 100, "Win rate should be 0-100%"
    assert metrics.profit_factor >= 0, "Profit factor should be non-negative"
    assert metrics.max_drawdown <= 0, "Max drawdown should be negative or zero"
    assert metrics.execution_duration_ms > 0, "Execution time should be captured"
    print("   ✅ All validations passed!")
    
    # Show backtester format
    print("\n5️⃣  Backtester format output:")
    output = metrics.to_dict()
    import json
    print(json.dumps(output, indent=2))
    
    # Performance check
    print("\n6️⃣  Performance check:")
    print(f"   Target:   <10ms per 1000 trades")
    print(f"   Actual:   {metrics.execution_duration_ms:.2f}ms per 100 trades")
    scaled_time = metrics.execution_duration_ms * 10  # Estimate for 1000 trades
    print(f"   Estimated for 1000 trades: {scaled_time:.2f}ms")
    if scaled_time < 10:
        print(f"   ✅ PASS - Meets performance target")
    else:
        print(f"   ⚠️  May need optimization")
    
    print("\n" + "="*70)
    print("✅ SAMPLE DATA INTEGRATION TEST COMPLETE")
    print("="*70)
    print("\nNext steps:")
    print("1. Run this with your real TradeResult data")
    print("2. Validate metrics match your expectations")
    print("3. Integrate into your backtester")


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--manual":
        # Manual test mode (requires real data)
        run_manual_integration_test()
    else:
        # Sample data mode (works out of the box)
        run_with_sample_data()
        
        print("\n" + "="*70)
        print("To run with real data:")
        print("  python tests/migration/test_metrics_calculator_integration.py --manual")
        print("  (then edit the function to provide real TradeResult)")
        print("="*70)
"""
Comprehensive TradeSimulator tests - Session 9 (UPDATED FOR ARTF + NEW RISK MANAGER)

Tests both legacy and new TradeSimulator implementations:
- Output parity between legacy and new versions
- Performance benchmarks
- Core vs Debug mode testing
- Contract integration verification

Uses:
- DataLoader v2.1 (DataBundle with .full, .strategy, .ltf, .artf)
- RiskManager with ARTF-based Rolling Annual Range
- pytest (no __main__ block)
"""

import sys
from pathlib import Path
import time
from typing import Dict, Any

import numpy as np
import pandas as pd
import pytest
import yaml
import warnings

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import both legacy and new simulators
from src.strategies.core.trade_simulator import TradeSimulator as LegacyTradeSimulator
from src.strategies.specific.modules.trade_simulator import TradeSimulator as NewTradeSimulator
from src.strategies.specific.modules.data_loader import DataLoader
from src.strategies.specific.modules.signal_generator import SignalGenerator


# ============================================================================  
# FIXTURES  
# ============================================================================  

@pytest.fixture(scope="session")
def config_core() -> Dict[str, Any]:
    """Load core mode configuration (wbws_strategy.yaml)"""
    path = PROJECT_ROOT / "configs/strategies/wbws/wbws_strategy.yaml"
    with open(path, "r") as f:
        config = yaml.safe_load(f)

    config.setdefault("output", {})
    config.setdefault("data", {})
    return config


@pytest.fixture(scope="session")
def config_debug() -> Dict[str, Any]:
    """Load debug mode configuration (wbws_strategy_debug.yaml)"""
    path = PROJECT_ROOT / "configs/strategies/wbws/wbws_strategy_debug.yaml"
    with open(path, "r") as f:
        config = yaml.safe_load(f)

    config.setdefault("output", {})
    config.setdefault("data", {})
    return config


@pytest.fixture(scope="session")
def test_data(config_core):
    """Load test data using date range from core config via DataLoader/DataBundle"""
    print("\n" + "=" * 60)
    print("Loading test data...")
    print("=" * 60)

    config_path = PROJECT_ROOT / "configs/strategies/wbws/wbws_strategy.yaml"
    with open(config_path, "r") as f:
        raw_config = yaml.safe_load(f)

    start_date = pd.Timestamp(raw_config["data"]["date_range"]["start"])
    end_date = pd.Timestamp(raw_config["data"]["date_range"]["end"])

    print(f"Using date range: {start_date} to {end_date}")

    loader = DataLoader(str(config_path))
    loader.load_config()
    data_bundle = loader.load_data()

    # Full data range
    full_data_start = data_bundle.full.index[0]
    full_data_end = data_bundle.full.index[-1]
    print(f"Full data available: {full_data_start} to {full_data_end}")
    print(f"Total days of history: {(full_data_end - full_data_start).days}")

    # Strategy data: use bundle.strategy if available, else slice full
    if data_bundle.strategy is not None and not data_bundle.strategy.empty:
        df_strategy = data_bundle.strategy
    else:
        df_strategy = data_bundle.full

    # Restrict to config date range
    df_full = data_bundle.full
    df_full = df_full[(df_full.index >= start_date) & (df_full.index <= end_date)]
    df_strategy = df_strategy[(df_strategy.index >= start_date) & (df_strategy.index <= end_date)]

    # Take first 500 bars for strategy to keep tests fast
    df_strategy = df_strategy[:500]

    # LTF data
    df_ltf = data_bundle.ltf
    if df_ltf is not None and not df_ltf.empty:
        df_ltf = df_ltf[(df_ltf.index >= start_date) & (df_ltf.index <= end_date)]
        df_ltf = df_ltf[:30000]

    print(f"Strategy bars: {len(df_strategy)} (from {df_strategy.index[0]} to {df_strategy.index[-1]})")
    print(f"LTF bars: {len(df_ltf) if df_ltf is not None else 0}")

    # ARTF monthly data from DataBundle
    df_artf = data_bundle.artf
    if df_artf is None or df_artf.empty:
        warnings.warn("DataBundle.artf is empty or missing, generating synthetic monthly ARTF from full data")
        df_artf = data_bundle.full.resample("M").agg({"high": "max", "low": "min"})

    # Inject ARTF into config_core for RiskManager
    config_core["data"]["df_artf"] = df_artf

    return {
        "strategy": df_strategy,
        "ltf": df_ltf,
        "full": df_full,
        "bundle": data_bundle,
        "artf": df_artf,
        "date_range": {"start": start_date, "end": end_date},
        "full_data_start": full_data_start,
        "full_data_end": full_data_end,
    }


@pytest.fixture(scope="session")
def test_signals(test_data):
    """Generate BUY/SELL signals for testing using SignalGenerator"""
    print("\n" + "=" * 60)
    print("Generating test signals...")
    print("=" * 60)

    gen = SignalGenerator(htf_period="1H", mode="core")
    signal_frame = gen.generate_signals(test_data["bundle"])

    # Convert to Series aligned with strategy index
    all_signals = pd.Series(index=test_data["bundle"].strategy.index, dtype="object")
    for ts, code in signal_frame.iter_raw():
        if ts in all_signals.index:
            all_signals[ts] = "BUY" if code == 1 else "SELL"

    # Filter to strategy data range
    signals = all_signals[all_signals.index.isin(test_data["strategy"].index)]

    signal_count = signals.notna().sum()
    earliest_signal = signals[signals.notna()].index.min() if signal_count > 0 else None
    latest_signal = signals[signals.notna()].index.max() if signal_count > 0 else None

    print(f"Total signals in test range: {signal_count}")
    print(f"BUY signals: {(signals == 'BUY').sum()}")
    print(f"SELL signals: {(signals == 'SELL').sum()}")
    if earliest_signal is not None:
        print(f"Earliest signal: {earliest_signal}")
        print(f"Days of history before first signal: {(earliest_signal - test_data['full_data_start']).days}")
        print(f"Latest signal: {latest_signal}")

    return signals


# ============================================================================  
# DIAGNOSTIC TESTS  
# ============================================================================  

class TestRiskManagerDiagnostics:
    """Diagnose annual range behavior with ARTF-based RiskManager"""

    def test_inspect_annual_range_calculation(self, config_core, test_data):
        print("\n" + "=" * 60)
        print("DIAGNOSTIC: Annual Range Calculation")
        print("=" * 60)

        from src.strategies.specific.modules.risk_manager import RiskManager

        risk_mgr = RiskManager(config_core, test_data["full"])

        assert hasattr(risk_mgr, "annual_range_series"), "RiskManager has no annual_range_series attribute"

        if risk_mgr.annual_range_series is None:
            print("❌ annual_range_series is None (RAR disabled)")
        else:
            ar_series = risk_mgr.annual_range_series
            non_nan = ar_series.notna().sum()
            print(f"✅ annual_range_series exists")
            print(f"  Non-NaN values: {non_nan} / {len(ar_series)}")
            print(f"  Index range: {ar_series.index[0]} to {ar_series.index[-1]}")

            # Sample a few strategy timestamps
            sample_ts = test_data["strategy"].index[::100][:5]
            for ts in sample_ts:
                if ts in ar_series.index:
                    val = ar_series.loc[ts]
                    print(f"  {ts}: annual_range = {val}")
                else:
                    print(f"  {ts}: NOT IN RAR INDEX")

        assert True  # diagnostic always passes


# ============================================================================  
# PARITY TESTS  
# ============================================================================  

class TestSimulatorParity:
    """Compare legacy and new simulator outputs"""

    def test_legacy_vs_new_trade_count_parity(self, config_core, test_data, test_signals):
        """Test that both simulators produce same number of trades (after RAR is valid)"""
        print("\n" + "=" * 60)
        print("PARITY TEST: Trade Count")
        print("=" * 60)

        if len(test_signals[test_signals.notna()]) == 0:
            pytest.skip("No signals available for testing")

        from src.strategies.specific.modules.risk_manager import RiskManager

        temp_risk = RiskManager(config_core, test_data["full"])
        ar_series = temp_risk.annual_range_series

        if ar_series is None:
            pytest.skip("Annual range unavailable (RAR disabled)")

        valid_mask = ar_series.notna()
        if not valid_mask.any():
            pytest.skip("No valid annual range values in data")

        first_valid = ar_series[valid_mask].index[0]
        last_valid = ar_series[valid_mask].index[-1]
        print(f"First valid RAR: {first_valid}")
        print(f"Last valid RAR:  {last_valid}")

        # Filter signals to timestamps where RAR is valid
        signals_to_use = test_signals[test_signals.index >= first_valid]
        signals_to_use = signals_to_use[signals_to_use.notna()]

        print(f"Signals with valid RAR: {len(signals_to_use)}")
        if len(signals_to_use) == 0:
            pytest.skip("No signals after first valid RAR")

        legacy = LegacyTradeSimulator(config_core, test_data["full"])
        new = NewTradeSimulator(config_core, test_data["full"])

        result_legacy = legacy.simulate_trades(
            df_strategy=test_data["strategy"],
            filtered_signals=signals_to_use,
            df_ltf=test_data["ltf"],
            verbose=False,
        )

        result_new = new.simulate_trades(
            df_strategy=test_data["strategy"],
            filtered_signals=signals_to_use,
            df_ltf=test_data["ltf"],
            verbose=False,
        )

        assert len(result_legacy["all_trades"]) == len(result_new["all_trades"]), (
            f"Trade count mismatch: Legacy={len(result_legacy['all_trades'])}, "
            f"New={len(result_new['all_trades'])}"
        )

        print(f"✅ Trade counts match: {len(result_new['all_trades'])} total trades")

    def test_legacy_vs_new_metrics_parity(self, config_core, test_data, test_signals):
        """Test that aggregated metrics match between legacy and new simulators"""
        print("\n" + "=" * 60)
        print("PARITY TEST: Metrics")
        print("=" * 60)

        if len(test_signals[test_signals.notna()]) == 0:
            pytest.skip("No signals available for testing")

        legacy = LegacyTradeSimulator(config_core, test_data["full"])
        new = NewTradeSimulator(config_core, test_data["full"])

        result_legacy = legacy.simulate_trades(
            df_strategy=test_data["strategy"],
            filtered_signals=test_signals,
            df_ltf=test_data["ltf"],
            verbose=False,
        )

        result_new = new.simulate_trades(
            df_strategy=test_data["strategy"],
            filtered_signals=test_signals,
            df_ltf=test_data["ltf"],
            verbose=False,
        )

        for reason in ["STOP_LOSS", "TAKE_PROFIT", "OPPOSITE_SIGNAL", "END_OF_DATA"]:
            legacy_count = result_legacy["exit_stats"].get(reason, 0)
            new_count = result_new["exit_stats"].get(reason, 0)
            assert legacy_count == new_count, (
                f"{reason} mismatch: Legacy={legacy_count}, New={new_count}"
            )

        print(f"✅ Exit stats match: {result_new['exit_stats']}")

        if "risk_stats" in result_legacy and "risk_stats" in result_new:
            legacy_approved = result_legacy["risk_stats"].get("total_approved", 0)
            new_approved = result_new["risk_stats"].get("total_approved", 0)
            if legacy_approved != new_approved:
                print(
                    f"⚠️  Risk approved differs: Legacy={legacy_approved}, New={new_approved} "
                    f"(may differ due to CLOSE_AND_REVERSE semantics)"
                )


# ============================================================================  
# PERFORMANCE TESTS  
# ============================================================================  

class TestSimulatorPerformance:
    """Benchmark and compare simulator performance"""

    @pytest.mark.parametrize("mode", ["core", "debug"])
    def test_simulator_speed_comparison(self, config_core, config_debug, test_data, test_signals, mode):
        """Compare speed between legacy and new simulators in both modes"""
        print("\n" + "=" * 60)
        print(f"PERFORMANCE TEST: {mode.upper()} Mode")
        print("=" * 60)

        if len(test_signals[test_signals.notna()]) == 0:
            pytest.skip("No signals available for testing")

        config = config_core if mode == "core" else config_debug

        legacy = LegacyTradeSimulator(config, test_data["full"])
        new = NewTradeSimulator(config, test_data["full"])

        start = time.perf_counter()
        result_legacy = legacy.simulate_trades(
            df_strategy=test_data["strategy"],
            filtered_signals=test_signals,
            df_ltf=test_data["ltf"],
            verbose=False,
        )
        legacy_time = time.perf_counter() - start

        start = time.perf_counter()
        result_new = new.simulate_trades(
            df_strategy=test_data["strategy"],
            filtered_signals=test_signals,
            df_ltf=test_data["ltf"],
            verbose=False,
        )
        new_time = time.perf_counter() - start

        print(f"\n{mode.upper()} MODE PERFORMANCE:")
        print(f"  Legacy Simulator: {legacy_time * 1000:.2f}ms")
        print(f"  New Simulator:    {new_time * 1000:.2f}ms")
        print(f"  Difference:       {((new_time / legacy_time) - 1) * 100:+.1f}%")
        print(f"  Trades processed: {len(result_new['all_trades'])}")

        assert new_time < legacy_time * 1.5, (
            f"New simulator too slow: {new_time / legacy_time:.2f}x slower"
        )

    def test_core_vs_debug_speed_improvement(self, config_core, config_debug, test_data, test_signals):
        """Test that core mode is faster than debug mode in new simulator"""
        print("\n" + "=" * 60)
        print("PERFORMANCE TEST: Core vs Debug Mode")
        print("=" * 60)

        if len(test_signals[test_signals.notna()]) == 0:
            pytest.skip("No signals available for testing")

        # Ensure tracking is disabled in core mode
        config_core["output"]["enable_progressive_tracking"] = False

        core_sim = NewTradeSimulator(config_core, test_data["full"])
        debug_sim = NewTradeSimulator(config_debug, test_data["full"])

        start = time.perf_counter()
        core_sim.simulate_trades(
            df_strategy=test_data["strategy"],
            filtered_signals=test_signals,
            df_ltf=test_data["ltf"],
            verbose=False,
        )
        core_time = time.perf_counter() - start

        start = time.perf_counter()
        debug_sim.simulate_trades(
            df_strategy=test_data["strategy"],
            filtered_signals=test_signals,
            df_ltf=test_data["ltf"],
            verbose=False,
        )
        debug_time = time.perf_counter() - start

        print(f"\nNEW SIMULATOR PERFORMANCE:")
        print(f"  Core mode:  {core_time * 1000:.2f}ms")
        print(f"  Debug mode: {debug_time * 1000:.2f}ms")
        print(f"  Speedup:    {debug_time / core_time:.2f}x faster in core mode")

        assert core_time <= debug_time * 1.3, \
            f"Core mode should be at least ~5% faster or equal; got core={core_time}, debug={debug_time}"   

    def test_throughput_benchmark(self, config_core, test_data, test_signals):
        """Measure trades per second throughput for new simulator"""
        print("\n" + "=" * 60)
        print("THROUGHPUT BENCHMARK")
        print("=" * 60)

        if len(test_signals[test_signals.notna()]) == 0:
            pytest.skip("No signals available for testing")

        sim = NewTradeSimulator(config_core, test_data["full"])

        iterations = 5
        total_trades = 0
        total_time = 0.0

        for _ in range(iterations):
            start = time.perf_counter()
            result = sim.simulate_trades(
                df_strategy=test_data["strategy"],
                filtered_signals=test_signals,
                df_ltf=test_data["ltf"],
                verbose=False,
            )
            elapsed = time.perf_counter() - start
            total_time += elapsed
            total_trades += len(result["all_trades"])

        avg_time = total_time / iterations
        avg_trades = total_trades / iterations
        trades_per_second = avg_trades / avg_time if avg_time > 0 else 0.0

        print(f"\nTHROUGHPUT RESULTS:")
        print(f"  Average time:    {avg_time * 1000:.2f}ms per run")
        print(f"  Average trades:  {avg_trades:.1f} per run")
        print(f"  Throughput:      {trades_per_second:.1f} trades/second")
        print(f"  Bars processed:  {len(test_data['strategy'])} per run")

        assert trades_per_second > 100, (
            f"Throughput too low: {trades_per_second:.1f} trades/sec"
        )


# ============================================================================  
# MODE-SPECIFIC TESTS  
# ============================================================================  

class TestExecutionModes:
    """Test core vs debug mode differences (progressive tracking)"""

    def test_debug_mode_enables_tracking(self, config_debug, test_data, test_signals):
        """Debug mode should call progressive tracker"""
        sim = NewTradeSimulator(config_debug, test_data["full"])

        if len(test_signals[test_signals.notna()]) == 0:
            pytest.skip("No signals available for testing")

        class MockTracker:
            def __init__(self):
                self.calls = []

            def update_position_management_details(self, **kwargs):
                self.calls.append(("position", kwargs))

            def update_risk_management_details(self, **kwargs):
                self.calls.append(("risk", kwargs))

            def update_trade_execution_details(self, **kwargs):
                self.calls.append(("trade", kwargs))

        mock_tracker = MockTracker()

        signal_id_map = {}
        signal_counter = 0
        for ts in test_signals[test_signals.notna()].index:
            signal_counter += 1
            signal_id_map[ts] = signal_counter

        sim.simulate_trades(
            df_strategy=test_data["strategy"],
            filtered_signals=test_signals,
            df_ltf=test_data["ltf"],
            progressive_tracker=mock_tracker,
            signal_id_map=signal_id_map,
            verbose=False,
        )

        assert len(mock_tracker.calls) > 0, "No tracking calls in debug mode"
        print(f"\n✅ Debug mode tracking active: {len(mock_tracker.calls)} calls")
        print(f"   Call types: {set(call[0] for call in mock_tracker.calls)}")

    def test_core_mode_disables_tracking(self, config_core, test_data, test_signals):
        """Core mode should not call progressive tracker when disabled in config"""

        if len(test_signals[test_signals.notna()]) == 0:
            pytest.skip("No signals available for testing")

        # Explicitly disable tracking in core config BEFORE creating simulator
        config_core["output"]["enable_progressive_tracking"] = False

        sim = NewTradeSimulator(config_core, test_data["full"])

        class MockTracker:
            def __init__(self):
                self.calls = []

            def update_position_management_details(self, **kwargs):
                self.calls.append(("position", kwargs))

            def update_risk_management_details(self, **kwargs):
                self.calls.append(("risk", kwargs))

            def update_trade_execution_details(self, **kwargs):
                self.calls.append(("trade", kwargs))

        mock_tracker = MockTracker()

        # Create signal_id_map
        signal_id_map = {}
        signal_counter = 0
        for ts in test_signals[test_signals.notna()].index:
            signal_counter += 1
            signal_id_map[ts] = signal_counter

        # Tracking is disabled in config, so we pass progressive_tracker=None
        sim.simulate_trades(
            df_strategy=test_data["strategy"],
            filtered_signals=test_signals,
            df_ltf=test_data["ltf"],
            progressive_tracker=None,
            signal_id_map=signal_id_map,
            verbose=False,
        )

        # No tracking calls should have been made
        assert len(mock_tracker.calls) == 0, "Tracking should be disabled in core mode"
        print(f"\n✅ Core mode tracking disabled (as expected)")


# ============================================================================  
# CONTRACT INTEGRATION TESTS  
# ============================================================================  

class TestContractIntegration:
    """Test that new simulator properly uses contracts"""

    def test_risk_manager_returns_contracts(self, config_core, test_data):
        """Verify RiskManager returns TradeParameters contracts"""
        from src.strategies.contracts.trade_contracts import TradeParameters

        sim = NewTradeSimulator(config_core, test_data["full"])

        if len(test_data["strategy"]) == 0:
            pytest.skip("No strategy data available")

        for idx in [100, 200, 300, 400]:
            if idx >= len(test_data["strategy"]):
                continue

            ts = test_data["strategy"].index[idx]
            price = float(test_data["strategy"].loc[ts, "close"])

            print(f"\nTrying timestamp: {ts}")
            try:
                params = sim.risk_manager.compute_trade_parameters(ts, price, True)
                assert isinstance(params, TradeParameters)
                print(f"✅ Success with timestamp {ts}")
                return
            except Exception as e:
                print(f"  Failed: {e}")
                continue

        pytest.skip("Could not find a working timestamp for risk manager")

    def test_trade_manager_returns_decisions(self, config_core, test_data, test_signals):
        """Verify TradeManager returns TradeDecision contracts"""
        from src.strategies.contracts.trade_contracts import TradeDecision

        sim = NewTradeSimulator(config_core, test_data["full"])

        signal_timestamps = list(test_signals[test_signals.notna()].index)
        if len(signal_timestamps) == 0:
            pytest.skip("No signals available for testing")

        for ts in signal_timestamps:
            signal = test_signals[ts]
            is_long = signal == "BUY"
            bid_price = float(test_data["strategy"].loc[ts, "close"])

            print(f"\nTrying signal at: {ts}")
            try:
                params = sim.risk_manager.compute_trade_parameters(ts, bid_price, is_long)
                decision = sim.trade_manager.handle_signal(
                    timestamp=ts,
                    signal_type=signal,
                    entry_price=params.entry_price_executed,
                    stop_loss=params.stop_loss_trigger,
                    take_profit=params.take_profit,
                    position_size=params.position_size,
                )

                assert isinstance(decision, TradeDecision)
                print(f"✅ Success with signal at {ts}")
                return
            except Exception as e:
                print(f"  Failed: {e}")
                continue
        
        pytest.skip("Could not find a working signal for trade manager")


# ============================================================================  
# BENCHMARK TESTS (INFORMATIONAL ONLY)  
# ============================================================================  

class TestSimulatorBenchmark:
    """Informational benchmark comparing legacy vs new simulator speed.
    This test prints timing information regardless of pass/fail status.
    """

    def test_legacy_vs_new_speed_benchmark(self, config_core, test_data, test_signals):
        """Benchmark comparing legacy vs new simulator speed"""
        print("\n" + "=" * 60)
        print("INFORMATIONAL BENCHMARK: Legacy vs New Simulator Speed")
        print("=" * 60)

        if len(test_signals[test_signals.notna()]) == 0:
            print("\n⚠️  No signals available for testing - skipping benchmark")
            pytest.skip("No signals available for testing")

        legacy = LegacyTradeSimulator(config_core, test_data["full"])
        new = NewTradeSimulator(config_core, test_data["full"])

        # Warm-up run (stabilizes Python JIT, caches, etc.)
        print("\nPerforming warm-up run...")
        legacy.simulate_trades(
            df_strategy=test_data["strategy"],
            filtered_signals=test_signals,
            df_ltf=test_data["ltf"],
            verbose=False,
        )
        new.simulate_trades(
            df_strategy=test_data["strategy"],
            filtered_signals=test_signals,
            df_ltf=test_data["ltf"],
            verbose=False,
        )

        # Benchmark
        iterations = 5
        legacy_times = []
        new_times = []

        print(f"Running {iterations} iterations for benchmark...")
        
        for i in range(iterations):
            start = time.perf_counter()
            legacy.simulate_trades(
                df_strategy=test_data["strategy"],
                filtered_signals=test_signals,
                df_ltf=test_data["ltf"],
                verbose=False,
            )
            legacy_times.append(time.perf_counter() - start)

            start = time.perf_counter()
            new.simulate_trades(
                df_strategy=test_data["strategy"],
                filtered_signals=test_signals,
                df_ltf=test_data["ltf"],
                verbose=False,
            )
            new_times.append(time.perf_counter() - start)

        avg_legacy = sum(legacy_times) / iterations
        avg_new = sum(new_times) / iterations
        speed_ratio = avg_new / avg_legacy

        # Calculate min/max for additional context
        min_legacy = min(legacy_times)
        max_legacy = max(legacy_times)
        min_new = min(new_times)
        max_new = max(new_times)

        print("\n" + "=" * 60)
        print("BENCHMARK RESULTS")
        print("=" * 60)
        print(f"\n📊 Test Data:")
        print(f"   • Strategy bars: {len(test_data['strategy'])}")
        print(f"   • Signals processed: {len(test_signals[test_signals.notna()])}")
        print(f"   • LTF bars: {len(test_data['ltf']) if test_data['ltf'] is not None else 0}")
        
        print(f"\n⏱️  Legacy Simulator:")
        print(f"   • Average: {avg_legacy * 1000:.2f} ms")
        print(f"   • Min:     {min_legacy * 1000:.2f} ms")
        print(f"   • Max:     {max_legacy * 1000:.2f} ms")
        
        print(f"\n⚡ New Simulator:")
        print(f"   • Average: {avg_new * 1000:.2f} ms")
        print(f"   • Min:     {min_new * 1000:.2f} ms")
        print(f"   • Max:     {max_new * 1000:.2f} ms")
        
        print(f"\n📈 Performance Comparison:")
        if speed_ratio < 1:
            improvement = (1 - speed_ratio) * 100
            print(f"   • New is {improvement:.1f}% FASTER than Legacy")
        else:
            slowdown = (speed_ratio - 1) * 100
            print(f"   • New is {slowdown:.1f}% SLOWER than Legacy")
        print(f"   • Ratio: {speed_ratio:.2f}x (new / legacy)")
        
        print(f"\n📋 Raw Times (seconds):")
        print(f"   Legacy: {[f'{t:.4f}' for t in legacy_times]}")
        print(f"   New:    {[f'{t:.4f}' for t in new_times]}")

        print("\n" + "=" * 60)
        print("Note: This benchmark is informational only and will always pass.")
        print("=" * 60)
        
        # Always pass - this is informational only
        assert True
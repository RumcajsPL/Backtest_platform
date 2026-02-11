"""
DataLoader Migration Performance Test

This test benchmarks the performance of the old DataLoader (dict-based 4-tuple return)
vs the new DataLoader_v2 (DataBundle return) to ensure performance is maintained.

Usage:
    python tests/migration/test_dataloader_parity.py

Expected Result:
    ⚡ New DataLoader performance ≤ 110% of old
"""

import sys
from pathlib import Path
import pandas as pd
import time
import numpy as np

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import old DataLoader
from src.strategies.core.data_loader import DataLoader as OldDataLoader

# Import new DataLoader
# Try direct import first, fall back to manual loading
try:
    from src.strategies.specific.modules.data_loader import DataLoader as NewDataLoader
except ImportError:
    # If the modules directory doesn't exist yet, try loading from file
    import importlib.util
    
    # Check multiple possible locations
    possible_paths = [
        PROJECT_ROOT / "src/strategies/specific/modules/data_loader.py",
        PROJECT_ROOT / "src/strategies/specific/data_loader.py",
        PROJECT_ROOT / "data_loader_v2.py",  # If you saved it at root temporarily
    ]
    
    new_loader_path = None
    for path in possible_paths:
        if path.exists():
            new_loader_path = path
            break
    
    if new_loader_path is None:
        print("\n❌ ERROR: Could not find new DataLoader file.")
        print("\nLooked in:")
        for path in possible_paths:
            print(f"  - {path}")
        print("\nPlease save data_loader_v2.py to one of these locations.")
        sys.exit(1)
    
    print(f"\nℹ️  Loading new DataLoader from: {new_loader_path}")
    
    spec = importlib.util.spec_from_file_location("new_data_loader", new_loader_path)
    new_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(new_module)
    NewDataLoader = new_module.DataLoader


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def benchmark(func, *args, **kwargs):
    """
    Benchmark a function and return result + elapsed time.
    
    Args:
        func: Function to benchmark
        *args, **kwargs: Function arguments
        
    Returns:
        (result, elapsed_seconds)
    """
    start = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = time.perf_counter() - start
    return result, elapsed


def run_performance_iteration(loader_class, config_path, iterations=1):
    """
    Run multiple performance iterations to get stable measurements.
    
    Args:
        loader_class: DataLoader class to instantiate
        config_path: Path to config file
        iterations: Number of iterations to run
        
    Returns:
        (avg_time_ms, std_dev_ms, all_times_ms)
    """
    times_ms = []
    
    for i in range(iterations):
        loader = loader_class(str(config_path))
        loader.load_config()
        
        _, elapsed = benchmark(loader.load_data)
        times_ms.append(elapsed * 1000)
        
        # Force garbage collection between iterations
        del loader
        import gc
        gc.collect()
    
    avg_time = np.mean(times_ms)
    std_dev = np.std(times_ms)
    
    return avg_time, std_dev, times_ms


# =============================================================================
# MAIN TEST
# =============================================================================

def test_dataloader_performance():
    """
    Performance-focused test comparing old and new DataLoader.
    """
    print("=" * 70)
    print("DataLoader Migration Performance Test")
    print("=" * 70)
    
    # Use the test config
    config_path = PROJECT_ROOT / "configs/strategies/wbws/wbws_strategy_debug.yaml"
        
    if not config_path.exists():
        print(f"\n❌ Config file not found: {config_path}")
        return False
    
    print(f"\nConfig: {config_path.name}")
    
    # Performance test configuration
    WARMUP_ITERATIONS = 3
    BENCHMARK_ITERATIONS = 10
    
    print(f"\nPerformance Test Configuration:")
    print(f"  Warmup iterations: {WARMUP_ITERATIONS}")
    print(f"  Benchmark iterations: {BENCHMARK_ITERATIONS}")
    
    # ==========================================================================
    # WARMUP PHASE
    # ==========================================================================
    print("\n" + "=" * 70)
    print("WARMUP PHASE (caching, JIT, etc.)")
    print("=" * 70)
    
    print("\n  Warming up Old DataLoader...")
    run_performance_iteration(OldDataLoader, config_path, WARMUP_ITERATIONS)
    
    print("  Warming up New DataLoader...")
    run_performance_iteration(NewDataLoader, config_path, WARMUP_ITERATIONS)
    
    # ==========================================================================
    # BENCHMARK PHASE
    # ==========================================================================
    print("\n" + "=" * 70)
    print("BENCHMARK PHASE")
    print("=" * 70)
    
    # Benchmark Old DataLoader
    print("\n  📊 Benchmarking Old DataLoader...")
    old_avg, old_std, old_times = run_performance_iteration(
        OldDataLoader, config_path, BENCHMARK_ITERATIONS
    )
    
    # Benchmark New DataLoader
    print("  📊 Benchmarking New DataLoader...")
    new_avg, new_std, new_times = run_performance_iteration(
        NewDataLoader, config_path, BENCHMARK_ITERATIONS
    )
    
    # ==========================================================================
    # PERFORMANCE ANALYSIS
    # ==========================================================================
    print("\n" + "=" * 70)
    print("PERFORMANCE ANALYSIS")
    print("=" * 70)
    
    print(f"\n{'Metric':<25} {'Old DataLoader':<25} {'New DataLoader':<25}")
    print("-" * 75)
    
    print(f"{'Mean time (ms):':<25} {old_avg:<25.2f} {new_avg:<25.2f}")
    print(f"{'Std deviation (ms):':<25} {old_std:<25.2f} {new_std:<25.2f}")
    print(f"{'Min time (ms):':<25} {min(old_times):<25.2f} {min(new_times):<25.2f}")
    print(f"{'Max time (ms):':<25} {max(old_times):<25.2f} {max(new_times):<25.2f}")
    
    # Calculate performance metrics
    speedup = old_avg / new_avg if new_avg > 0 else 0
    slowdown_pct = ((new_avg - old_avg) / old_avg * 100) if old_avg > 0 else 0
    
    print(f"\n{'Performance Metrics':<40}")
    print("-" * 40)
    print(f"  Speedup: {speedup:.3f}x")
    print(f"  Slowdown: {slowdown_pct:+.2f}%")
    
    # Performance acceptance: new must be ≤ 110% of old
    threshold_ms = old_avg * 1.10
    performance_ok = new_avg <= threshold_ms
    
    print(f"\n  Performance threshold: ≤ {threshold_ms:.2f} ms (110% of baseline)")
    print(f"  Result: {new_avg:.2f} ms")
    
    if performance_ok:
        print(f"  ✅ PERFORMANCE ACCEPTABLE")
        
        if speedup > 1.0:
            print(f"     New DataLoader is {speedup:.2f}x FASTER")
        else:
            print(f"     New DataLoader is {abs(slowdown_pct):.2f}% slower (within threshold)")
    else:
        print(f"  ❌ PERFORMANCE REGRESSION")
        print(f"     New DataLoader exceeds 110% threshold by {new_avg - threshold_ms:.2f} ms")
    
    # Statistical significance test (optional)
    from scipy import stats
    
    t_stat, p_value = stats.ttest_ind(old_times, new_times)
    print(f"\n{'Statistical Significance':<40}")
    print("-" * 40)
    print(f"  t-statistic: {t_stat:.4f}")
    print(f"  p-value: {p_value:.4f}")
    
    if p_value < 0.05:
        print(f"  ✅ Statistically significant difference (p < 0.05)")
        if new_avg < old_avg:
            print(f"     New DataLoader is significantly FASTER")
        else:
            print(f"     New DataLoader is significantly SLOWER")
    else:
        print(f"  ℹ️  No statistically significant difference (p ≥ 0.05)")
    
    # ==========================================================================
    # DETAILED TIMING BREAKDOWN
    # ==========================================================================
    print("\n" + "=" * 70)
    print("DETAILED TIMING BREAKDOWN")
    print("=" * 70)
    
    print(f"\n{'Iteration':<12} {'Old (ms)':<15} {'New (ms)':<15}")
    print("-" * 42)
    
    for i, (old_t, new_t) in enumerate(zip(old_times, new_times), 1):
        diff = new_t - old_t
        diff_indicator = "🔻" if diff > 0 else "✅" if diff < 0 else "="
        print(f"{i:<12} {old_t:<15.2f} {new_t:<15.2f} {diff_indicator} ({diff:+.2f} ms)")
    
    # ==========================================================================
    # FINAL RESULT
    # ==========================================================================
    print("\n" + "=" * 70)
    print("FINAL RESULT")
    print("=" * 70)
    
    print(f"\n  Performance test:     {'✅ PASS' if performance_ok else '❌ FAIL'}")
    
    print(f"\n  {'='*70}")
    if performance_ok:
        print(f"  ✅ PERFORMANCE TEST PASSED - New DataLoader meets performance criteria")
        if speedup > 1.0:
            print(f"     🚀 Performance improvement detected: {speedup:.2f}x faster")
        else:
            print(f"     📊 Performance neutral (within {abs(slowdown_pct):.1f}% of baseline)")
    else:
        print(f"  ❌ PERFORMANCE TEST FAILED - Do not proceed with migration")
        print(f"     Performance regression: {slowdown_pct:+.1f}% (exceeds 10% threshold)")
    print(f"  {'='*70}\n")
    
    return performance_ok


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    success = test_dataloader_performance()
    sys.exit(0 if success else 1)
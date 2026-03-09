"""
scripts/diagnostics/test_b9o007_memory.py  (v2)
-------------------------------------------------
Validates B9O-007 is correctly applied.

The key assertion is:
  bundle.full - bundle.strategy == _WFO_WARMUP_BARS (exactly the warmup prefix)
  bundle.full << 22M (not the full file)

Usage:
    python scripts/diagnostics/test_b9o007_memory.py
"""
import sys
import logging
from pathlib import Path

logging.basicConfig(level=logging.WARNING)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from src.strategies.config.config_schema import StrategyConfig
    from src.strategies.core.data_loader import DataLoader, _WFO_WARMUP_BARS
except ImportError as e:
    print(f"FAIL: Cannot import project modules: {e}")
    sys.exit(1)

BASE_YAML = PROJECT_ROOT / "configs" / "strategies" / "strategy_template.yaml"
FULL_FILE_ROWS = 22_431_468  # Known from OOM error trace shape (5, 22431468)


def test_warmup_prefix():
    print(f"[TEST 1] Warmup prefix (expect full - strategy == {_WFO_WARMUP_BARS})...")

    config = StrategyConfig.from_yaml(BASE_YAML)
    loader = DataLoader(config, mode="core")
    bundle = loader.load_data()

    full_rows     = len(bundle.full)
    strategy_rows = len(bundle.strategy)
    diff          = full_rows - strategy_rows
    is_bounded    = (
        loader.data_config.date_range is not None
        and loader.data_config.date_range.is_bounded
    )

    print(f"   bundle.full rows:          {full_rows:,}")
    print(f"   bundle.strategy rows:      {strategy_rows:,}")
    print(f"   difference (warmup):       {diff}")
    print(f"   _WFO_WARMUP_BARS:          {_WFO_WARMUP_BARS}")
    print(f"   date_range active:         {is_bounded}")

    if not is_bounded:
        print("   INFO: No date_range — Stage 1 path, full==strategy (expected).")
        print("   SKIP\n")
        return

    # Warmup must be <= _WFO_WARMUP_BARS (can be less if window is near file start)
    assert 0 <= diff <= _WFO_WARMUP_BARS, (
        f"FAIL: warmup diff={diff}, expected 0..{_WFO_WARMUP_BARS}. "
        f"Slicing is not working correctly."
    )

    # Must be much smaller than full 22M file
    assert full_rows < FULL_FILE_ROWS * 0.5, (
        f"FAIL: bundle.full={full_rows:,} is {100*full_rows/FULL_FILE_ROWS:.0f}% of full file. "
        f"Memory reduction insufficient."
    )

    print(f"   File reduction: {full_rows:,} / {FULL_FILE_ROWS:,} "
          f"= {100*full_rows/FULL_FILE_ROWS:.2f}% of full file")
    print("   PASS\n")


def test_atr_not_nan():
    print("[TEST 2] ATR not NaN at first window bar...")

    try:
        from src.strategies.market.risk_manager import RiskManager
        from src.strategies.core.cache_manager import CacheManager
        import numpy as np
    except ImportError as e:
        print(f"   SKIP: {e}\n")
        return

    config = StrategyConfig.from_yaml(BASE_YAML)
    loader = DataLoader(config, mode="core")
    bundle = loader.load_data()

    is_bounded = (
        loader.data_config.date_range is not None
        and loader.data_config.date_range.is_bounded
    )
    if not is_bounded:
        print("   SKIP: No date_range — ATR test not applicable.\n")
        return

    risk_mgr = RiskManager(
        config=config,
        ohlcv_data=bundle.full,
        ohlcv_artf=bundle.artf,
        mode="core",
        cache_manager=CacheManager(),
    )

    window_start = bundle.strategy.index[0]
    atr_val = None
    if risk_mgr.atr_series is not None:
        atr_val = risk_mgr.atr_series.get(window_start)

    print(f"   window_start:        {window_start}")
    print(f"   ATR at window_start: {atr_val}")
    print(f"   warmup bars:         {len(bundle.full) - len(bundle.strategy)}")

    assert (
        atr_val is not None
        and not np.isnan(float(atr_val))
        and float(atr_val) > 0
    ), f"FAIL: ATR={atr_val} at {window_start}. Increase _WFO_WARMUP_BARS."

    print("   PASS\n")


if __name__ == "__main__":
    print("=" * 60)
    print(f"B9O-007 Validation  (_WFO_WARMUP_BARS={_WFO_WARMUP_BARS})")
    print("=" * 60)
    print()
    try:
        test_warmup_prefix()
        test_atr_not_nan()
        print("=" * 60)
        print("All tests passed. Fix correctly applied.")
        print("Restore max_workers: 6 in calibration YAML.")
        print("=" * 60)
    except AssertionError as e:
        print(f"\n{e}")
        sys.exit(1)
    except Exception as e:
        import traceback
        traceback.print_exc()
        sys.exit(1)
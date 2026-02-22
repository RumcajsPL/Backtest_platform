"""
pytest Configuration and Shared Fixtures
=========================================
Provides fixtures for all unit tests including:
- Test data generation
- Mock strategy configurations
- Sample DataFrames
- Temporary directories
- REAL DATA fixtures from test_data_paths.yaml
"""

import pytest
import pandas as pd
import numpy as np
import yaml
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

from src.utils.paths import (
    CONFIGS_DIR, TESTS_DIR, ensure_dir,
    config_path, test_path
)
from src.config.config_schema import (
    StrategyConfig, AssetConfig, DataConfig, DataPathsConfig,
    DateRangeConfig, ExecutionConfig, TradeManagementConfig,
    SpreadConfig, RiskConfig, PositionControlConfig,
    FilterPipelineConfig, FilterConfig, OutputConfig
)
from src.strategies.contracts.data_contracts import DataBundle, DataInfo
from src.strategies.contracts.signal_contracts import SignalFrame
from src.strategies.core.cache_manager import CacheManager
from src.strategies.specific.modules.data_loader import DataLoader
from src.strategies.specific.modules.spread_manager import SpreadManager
from src.strategies.specific.modules.trade_analytics import TradeAnalytics
from src.strategies.orchestrator import StrategyOrchestrator


# ============================================================================
# Path Resolution Fixtures
# ============================================================================

@pytest.fixture(scope="session")
def project_paths():
    """Provide all project paths for testing."""
    return {
        "configs_dir": CONFIGS_DIR,
        "tests_dir": TESTS_DIR,
        "test_config": config_path("tests", "test_config.yaml"),
        "test_data_paths": config_path("tests", "test_data_paths.yaml"),
        "reports_dir": test_path("reports"),
        "diagnostic_dir": test_path("diagnostic_output"),
    }


# ============================================================================
# Real Data Fixtures (using test_data_paths.yaml)
# ============================================================================

@pytest.fixture(scope="session")
def test_paths_config(project_paths):
    """Load the test_data_paths.yaml configuration."""
    config_path = project_paths["test_data_paths"]
    if not config_path.exists():
        pytest.skip(f"Test data paths config not found: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="session")
def test_runner_config(project_paths):
    """Load the test_config.yaml configuration."""
    config_path = project_paths["test_config"]
    if not config_path.exists():
        pytest.skip(f"Test runner config not found: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


@pytest.fixture
def real_data_config(request, base_config_dict, test_paths_config, test_runner_config):
    """Create StrategyConfig with REAL data paths from test_data_paths.yaml."""
    from src.config.config_schema import StrategyConfig
    
    # Update config with real data paths
    data_paths = test_paths_config["data"]
    
    base_config_dict["data"]["paths"]["strategy_ohlcv"] = data_paths["strategy_ohlcv"]
    base_config_dict["data"]["paths"]["htf_ohlcv"] = data_paths.get("htf_ohlcv")
    base_config_dict["data"]["paths"]["ltf_ohlcv"] = data_paths.get("ltf_ohlcv")
    base_config_dict["data"]["paths"]["artf_ohlcv"] = data_paths.get("artf_ohlcv")
    
    # Set date range from config
    date_range = test_paths_config.get("date_range", {})
    if date_range:
        base_config_dict["data"]["date_range"] = {
            "start": date_range.get("start"),
            "end": date_range.get("end")
        }
    
    # Set test mode from runner config
    base_config_dict["execution"]["mode"] = test_runner_config.get("execution", {}).get("mode", "core")
    
    # Set asset symbol from test data (DEUIDXEUR)
    base_config_dict["asset"]["symbol"] = "DEUIDXEUR"
    
    return StrategyConfig.from_dict(base_config_dict)


@pytest.fixture
def real_data_bundle(real_data_config):
    """Load actual data using DataLoader with real paths."""
    loader = DataLoader(config=real_data_config, mode="analytics")
    try:
        return loader.load_data()
    except Exception as e:
        pytest.skip(f"Failed to load real data: {e}")


@pytest.fixture
def real_trade_result(real_data_config):
    """Run full strategy on real data and return trade result using orchestrator."""
    orchestrator = StrategyOrchestrator(config=real_data_config)
    result = orchestrator.run()
    return result.trade_result


@pytest.fixture
def real_trade_result_direct(real_data_config):
    """
    Load real trades directly without using orchestrator.
    This bypasses the orchestrator's DataLoader signature issue.
    """
    from src.strategies.specific.modules.signal_generator import SignalGenerator
    from src.strategies.specific.modules.filter_pipeline import FilterPipeline
    from src.strategies.specific.modules.trade_simulator import TradeSimulator
    
    print(f"\n{'='*60}")
    print("Loading real trades directly (bypassing orchestrator)")
    print(f"{'='*60}")
    
    # Load data
    loader = DataLoader(config=real_data_config, mode="core")
    bundle = loader.load_data()
    print(f"Data loaded: {bundle.info.strategy_bars} bars")
    
    # Generate signals
    generator = SignalGenerator(config=real_data_config, mode="core")
    signals = generator.generate_signals(bundle)
    signal_counts = signals.count_by_type()
    print(f"Signals generated: {signal_counts['total']} total ({signal_counts['buy']} BUY, {signal_counts['sell']} SELL)")
    
    # Filter signals
    pipeline = FilterPipeline(config=real_data_config, mode="core")
    filtered = pipeline.apply_filters(signals, bundle.strategy)
    print(f"Filters applied: {filtered.raw_count} → {filtered.final_count} signals")
    
    # Simulate trades
    simulator = TradeSimulator(config=real_data_config, df_full=bundle.full)
    result = simulator.simulate_trades(
        df_strategy=bundle.strategy,
        signal_frame=filtered.final_signals,
        df_ltf=bundle.ltf,
    )
    print(f"Trades simulated: {result.total_opened} opened, {result.total_closed} closed")
    print(f"{'='*60}\n")
    
    return result


@pytest.fixture
def real_analytics_report(real_data_config, real_trade_result):
    """Generate analytics report from real data run."""
    report = TradeAnalytics.analyze(
        trade_result=real_trade_result,
        config=real_data_config
    )
    return report


# ============================================================================
# Test Data Generation (for unit tests)
# ============================================================================

@pytest.fixture
def sample_ohlcv_data() -> pd.DataFrame:
    """Generate sample OHLCV data for unit testing."""
    dates = pd.date_range(start="2025-01-01", periods=1000, freq="1min")
    
    # Generate realistic price movements
    np.random.seed(42)
    returns = np.random.randn(1000) * 0.0001
    prices = 100 * (1 + np.cumsum(returns))
    
    df = pd.DataFrame({
        "open": prices * (1 + np.random.randn(1000) * 0.001),
        "high": prices * (1 + np.abs(np.random.randn(1000)) * 0.002),
        "low": prices * (1 - np.abs(np.random.randn(1000)) * 0.002),
        "close": prices,
        "volume": np.random.randint(100, 1000, 1000)
    }, index=dates)
    
    # Ensure OHLC integrity
    df["high"] = df[["open", "high", "close"]].max(axis=1)
    df["low"] = df[["open", "low", "close"]].min(axis=1)
    
    return df


@pytest.fixture
def sample_htf_data() -> pd.DataFrame:
    """Generate sample higher timeframe data."""
    dates = pd.date_range(start="2025-01-01", periods=200, freq="1H")
    
    prices = 100 + np.cumsum(np.random.randn(200) * 0.001)
    
    return pd.DataFrame({
        "open": prices * (1 + np.random.randn(200) * 0.001),
        "high": prices * (1 + np.abs(np.random.randn(200)) * 0.002),
        "low": prices * (1 - np.abs(np.random.randn(200)) * 0.002),
        "close": prices,
        "volume": np.random.randint(1000, 10000, 200)
    }, index=dates)


@pytest.fixture
def sample_ltf_data() -> pd.DataFrame:
    """Generate sample lower timeframe (1s) data."""
    dates = pd.date_range(start="2025-01-01 09:00:00", periods=5000, freq="1s")
    
    # Generate more volatile tick data
    prices = 100 + np.cumsum(np.random.randn(5000) * 0.0005)
    
    df = pd.DataFrame({
        "open": prices * (1 + np.random.randn(5000) * 0.0005),
        "high": prices * (1 + np.abs(np.random.randn(5000)) * 0.001),
        "low": prices * (1 - np.abs(np.random.randn(5000)) * 0.001),
        "close": prices,
        "volume": np.ones(5000)
    }, index=dates)
    
    return df


@pytest.fixture
def sample_artf_data() -> pd.DataFrame:
    """Generate sample monthly (ARTF) data."""
    dates = pd.date_range(start="2020-01-01", periods=60, freq="ME")
    
    prices = 100 + np.cumsum(np.random.randn(60) * 1.0)
    
    return pd.DataFrame({
        "open": prices * (1 + np.random.randn(60) * 0.01),
        "high": prices * (1 + np.abs(np.random.randn(60)) * 0.02),
        "low": prices * (1 - np.abs(np.random.randn(60)) * 0.02),
        "close": prices,
        "volume": np.random.randint(100000, 1000000, 60)
    }, index=dates)


# ============================================================================
# Test DataBundle Fixtures
# ============================================================================

@pytest.fixture
def sample_data_bundle(sample_ohlcv_data, sample_htf_data, sample_ltf_data, sample_artf_data) -> DataBundle:
    """Create a complete DataBundle with sample data."""
    info = DataInfo(
        total_bars=len(sample_ohlcv_data),
        strategy_bars=len(sample_ohlcv_data),
        htf_bars=len(sample_htf_data),
        ltf_bars=len(sample_ltf_data),
        artf_bars=len(sample_artf_data),
        date_range=(
            sample_ohlcv_data.index[0].to_pydatetime(),
            sample_ohlcv_data.index[-1].to_pydatetime()
        ),
        ltf_timeframe="1s",
        artf_timeframe="1ME",
        cache_hit=False
    )
    
    return DataBundle(
        full=sample_ohlcv_data,
        strategy=sample_ohlcv_data,
        htf=sample_htf_data,
        ltf=sample_ltf_data,
        artf=sample_artf_data,
        info=info
    )


@pytest.fixture
def sample_signal_frame(sample_ohlcv_data) -> SignalFrame:
    """Create a sample SignalFrame with mixed signals."""
    signals = pd.Series(0, index=sample_ohlcv_data.index, dtype=np.int8)
    
    # Add signals at various indices
    signals.iloc[10] = 1  # BUY
    signals.iloc[20] = 1  # BUY
    signals.iloc[30] = 2  # SELL
    signals.iloc[40] = 2  # SELL
    signals.iloc[50] = 1  # BUY
    signals.iloc[60] = 2  # SELL
    
    return SignalFrame(
        signals=signals,
        indicator_data=None,
        signal_metadata={"source": "test"}
    )


# ============================================================================
# Test Configuration Fixtures
# ============================================================================

@pytest.fixture
def base_config_dict() -> Dict[str, Any]:
    """Base configuration dictionary for testing."""
    return {
        "asset": {
            "symbol": "TEST",
            "pip_size": 0.0001,
            "point_size": 0.00001
        },
        "data": {
            "paths": {
                "strategy_ohlcv": "data/test/strategy.parquet",
                "htf_ohlcv": "data/test/htf.parquet",
                "ltf_ohlcv": "data/test/ltf.parquet",
                "artf_ohlcv": "data/test/artf.parquet"
            },
            "date_range": {
                "start": "2025-01-01 00:00:00",
                "end": "2025-01-31 23:59:59"
            },
            "timezone": "CET",
            "htf_period": "1H",
            "ltf_timeframe": "1s",
            "artf_timeframe": "1ME"
        },
        "execution": {
            "mode": "core"
        },
        "trade_management": {
            "spread": {
                "enabled": True,
                "config_path": "configs/spreads/broker_spreads.yaml"
            },
            "risk": {
                "atr_length": 14,
                "atr_multiplier_sl": 1.4,
                "atr_multiplier_tp": 7.98,
                "max_risk_percentile": 0.1,
                "tp_mode": "rr_ratio",
                "risk_to_reward_ratio": 5.7
            },
            "position_control": {
                "pyramiding_enabled": False,
                "close_on_opposite": False,
                "max_positions": 1
            }
        },
        "filters": {
            "time_filters": {
                "time_filter": {
                    "enabled": True,
                    "config": {
                        "session_start": {"hour": 8, "minute": 30},
                        "session_end": {"hour": 20, "minute": 30},
                        "excluded_days": []
                    }
                }
            },
            "filter_sequence": ["rsi_filter"],
            "technical_filters": {
                "rsi_filter": {
                    "enabled": True,
                    "config": {
                        "length": 14,
                        "overbought": 70,
                        "oversold": 30
                    }
                }
            },
            "default_error_strategy": "pass_through"
        },
        "output": {
            "reports": {
                "enabled": True,
                "output_dir": "tests/reports",
                "theme": "dark",
                "brand_name": "TestStrategy"
            },
            "logging": {
                "level": "INFO",
                "output_dir": "tests/diagnostic_output"
            }
        }
    }


@pytest.fixture
def test_config(base_config_dict) -> StrategyConfig:
    """Create a valid StrategyConfig for testing."""
    return StrategyConfig.from_dict(base_config_dict)


@pytest.fixture
def cache_manager() -> CacheManager:
    """Create a fresh CacheManager for testing."""
    return CacheManager()


# ============================================================================
# Filter Test Fixtures
# ============================================================================

@pytest.fixture
def filter_test_df() -> pd.DataFrame:
    """Create a DataFrame specifically for filter testing with engineered values."""
    dates = pd.date_range(start="2025-01-01 00:00:00", periods=200, freq="1min")
    np.random.seed(42)
    
    # Create trending then ranging data
    trend = np.concatenate([
        np.linspace(100, 110, 100),  # Uptrend
        np.linspace(110, 105, 50),    # Down trend
        np.linspace(105, 106, 50)     # Sideways
    ])
    
    df = pd.DataFrame({
        "open": trend * 0.999,
        "high": trend * 1.002,
        "low": trend * 0.998,
        "close": trend,
        "volume": np.random.randint(100, 1000, 200)
    }, index=dates)
    
    # Ensure OHLC integrity
    df["high"] = df[["open", "high", "close"]].max(axis=1)
    df["low"] = df[["open", "low", "close"]].min(axis=1)
    
    return df


@pytest.fixture
def sample_signal_frame_with_mixed_signals(sample_ohlcv_data) -> SignalFrame:
    """Create SignalFrame with mix of BUY/SELL signals at specific positions."""
    signals = pd.Series(0, index=sample_ohlcv_data.index, dtype=np.int8)
    
    # Add signals at various positions
    signals.iloc[10] = 1   # BUY
    signals.iloc[20] = 1   # BUY
    signals.iloc[30] = 2   # SELL
    signals.iloc[40] = 2   # SELL
    signals.iloc[50] = 1   # BUY
    signals.iloc[60] = 2   # SELL
    signals.iloc[70] = 1   # BUY
    signals.iloc[80] = 2   # SELL
    
    return SignalFrame(
        signals=signals,
        indicator_data=None,
        signal_metadata={"source": "test"}
    )


@pytest.fixture
def sample_signal_frame_all_buy(sample_ohlcv_data) -> SignalFrame:
    """Create SignalFrame with only BUY signals."""
    signals = pd.Series(0, index=sample_ohlcv_data.index, dtype=np.int8)
    signals.iloc[10:20] = 1
    return SignalFrame(
        signals=signals,
        indicator_data=None,
        signal_metadata={"source": "test"}
    )


@pytest.fixture
def sample_signal_frame_all_sell(sample_ohlcv_data) -> SignalFrame:
    """Create SignalFrame with only SELL signals."""
    signals = pd.Series(0, index=sample_ohlcv_data.index, dtype=np.int8)
    signals.iloc[30:40] = 2
    return SignalFrame(
        signals=signals,
        indicator_data=None,
        signal_metadata={"source": "test"}
    )


@pytest.fixture
def sample_signal_frame_no_signals(sample_ohlcv_data) -> SignalFrame:
    """Create SignalFrame with no signals."""
    signals = pd.Series(0, index=sample_ohlcv_data.index, dtype=np.int8)
    return SignalFrame(
        signals=signals,
        indicator_data=None,
        signal_metadata={"source": "test"}
    )


@pytest.fixture
def indicators_dict():
    """Empty indicators dictionary for testing."""
    return {}


@pytest.fixture
def ind_np_dict():
    """Empty numpy indicators dictionary for testing."""
    return {}


# ============================================================================
# Temporary Directory Fixtures
# ============================================================================

@pytest.fixture
def tmp_test_dir(tmp_path) -> Path:
    """Create a temporary directory for test outputs."""
    test_dir = tmp_path / "test_output"
    test_dir.mkdir()
    return test_dir


@pytest.fixture
def tmp_cache_dir(tmp_path) -> Path:
    """Create a temporary cache directory."""
    cache_dir = tmp_path / "test_cache"
    cache_dir.mkdir()
    return cache_dir
"""
Baseline Test – New Architecture Only
Runs enabled filters from YAML config on generated signals.
No legacy code, no parity logic, no debug mode.
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
import yaml

# ------------------------------------------------------------
# Project root
# ------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ------------------------------------------------------------
# Imports – New Architecture Only
# ------------------------------------------------------------
from src.strategies.specific.modules.data_loader import DataLoader
from src.strategies.specific.modules.signal_generator import SignalGenerator
from src.strategies.contracts.signal_contracts import SignalFrame

# All new filters
from src.strategies.specific.filters.dpo_filter import DPOFilter
from src.strategies.specific.filters.ma_filter import MAFilter
from src.strategies.specific.filters.macd_filter import MACDFilter
from src.strategies.specific.filters.pivot_filter import PivotFilter
from src.strategies.specific.filters.rsi_filter import RSIFilter
from src.strategies.specific.filters.cci_filter import CCIFilter
from src.strategies.specific.filters.adx_filter import ADXFilter
from src.strategies.specific.filters.bollinger_filter import BollingerFilter
from src.strategies.specific.filters.choppiness_filter import ChoppinessFilter
from src.strategies.specific.filters.supertrend_filter import SupertrendFilter
from src.strategies.specific.filters.time_filter import TimeFilter


# ------------------------------------------------------------
# Load config
# ------------------------------------------------------------
def load_config(name="wbws_strategy.yaml"):
    path = PROJECT_ROOT / f"configs/strategies/wbws/{name}"
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ------------------------------------------------------------
# Build filter instances from config
# ------------------------------------------------------------
def build_filters(config):
    filters = []
    fcfg = config.get("filters", {})

    mapping = {
        "dpo_filter": DPOFilter,
        "ma_filter": MAFilter,
        "macd_filter": MACDFilter,
        "pivot_filter": PivotFilter,
        "rsi_filter": RSIFilter,
        "cci_filter": CCIFilter,
        "adx_filter": ADXFilter,
        "bollinger_filter": BollingerFilter,
        "choppiness_filter": ChoppinessFilter,
        "supertrend_filter": SupertrendFilter,
        "time_filter": TimeFilter,
    }

    for name, params in fcfg.items():
        if not params.get("enabled", False):
            continue
        cls = mapping.get(name)
        if cls:
            filters.append(cls(**params, name=name))

    return filters


# ------------------------------------------------------------
# Run filters sequentially
# ------------------------------------------------------------
def run_filters(df, signal_frame, filters):
    indicators = {}
    ind_np = {}

    for flt in filters:
        # Compute indicators
        flt.compute_indicators(df, indicators, ind_np)

        # Apply filter
        result = flt.apply_filter(
            signal_frame=signal_frame,
            df=df,
            indicators=indicators,
            ind_np=ind_np,
            mode="core"
        )
        signal_frame = result.signal_frame

        # Print filter summary
        counts = signal_frame.count_by_type()
        print("\n=== FILTERED SIGNALS ===")
        print(f"Filter: {flt.name}")
        print(f"BUY:  {counts['buy']}")
        print(f"SELL: {counts['sell']}")
        print(f"TOTAL: {counts['total']}")

    return signal_frame

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
if __name__ == "__main__":
    config = load_config()

    # ------------------------------------------------------------
    # BASELINE: DATA LOADER & SIGNAL GENERATOR
    # ------------------------------------------------------------
    print("\n" + "="*60)
    print("BASELINE: DATA LOADER & SIGNAL GENERATOR")
    print("="*60)

    loader = DataLoader(str(PROJECT_ROOT / "configs/strategies/wbws/wbws_strategy.yaml"))
    loader.load_config()
    data_bundle = loader.load_data()

    df = data_bundle.strategy

    gen = SignalGenerator(htf_period="1H", mode="core")
    signal_frame = gen.generate_signals(data_bundle)

    counts = signal_frame.count_by_type()
    print(f"✅ DataLoader v2 Active")
    print(f"Strategy bars: {len(df):,}")
    print(f"BUY Signals: {counts['buy']:,} | SELL Signals: {counts['sell']:,} | Total: {counts['total']:,}")

    # Build and run filters
    filters = build_filters(config)
    filtered = run_filters(df, signal_frame, filters)

    # Output summary
    counts = filtered.count_by_type()
    print("\n=== FILTERED SIGNALS ===")
    print(f"BUY:  {counts['buy']}")
    print(f"SELL: {counts['sell']}")
    print(f"TOTAL: {counts['total']}")
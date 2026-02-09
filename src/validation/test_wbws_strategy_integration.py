"""
Test Script
"""

import sys
from pathlib import Path
import pandas as pd
import yaml

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Imports from your architecture
from src.strategies.specific.wbws_strategy import WBWSStrategy #new WBWSStrategy
from src.strategies.core.signal_generator import SignalGenerator
from src.strategies.core.filter_pipeline import FilterPipeline
from src.strategies.trade_management.signal_frame import SignalFrame
from src.strategies.trade_management.risk_manager import RiskManager
from src.strategies.trade_management.trade_manager import TradeManager


# ---------------------------------------------------------
# Load YAML config
# ---------------------------------------------------------
def load_config():
    cfg_path = PROJECT_ROOT / "configs/strategies/wbws/wbws_strategy.yaml"
    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------
# Load OHLCV + HTF using config date_range
# ---------------------------------------------------------
def load_sample_data(config):
    data_cfg = config["data"]

    data_path = PROJECT_ROOT / data_cfg["file"]
    htf_path = PROJECT_ROOT / data_cfg["file_htf"]

    date_start = data_cfg["date_range"]["start"]
    date_end = data_cfg["date_range"]["end"]

    # Load full OHLCV (for RiskManager)
    df_full = pd.read_parquet(data_path)

    # RiskManager needs ALL history up to end of test window
    df_history = df_full.loc[:date_end]

    # Strategy only uses the test window
    df = df_full.loc[date_start:date_end]

    # Load HTF and align to LTF window
    df_htf_full = pd.read_parquet(htf_path)
    df_htf = df_htf_full.loc[df.index.min(): df.index.max()]

    return df, df_htf, df_history


# ---------------------------------------------------------
# Build SignalFrame for each bar
# ---------------------------------------------------------
def build_signal_frame(row, raw_signal, filtered_signal):
    return SignalFrame(
        timestamp=row.name,
        open=row["open"],
        high=row["high"],
        low=row["low"],
        close=row["close"],
        volume=row["volume"],
        indicators={},
        state={
            "raw_signal": raw_signal,
            "filtered_signal": filtered_signal,
        },
    )


# ---------------------------------------------------------
# Main test
# ---------------------------------------------------------
def main():
    print("\n=== WBWSStrategy v1.4 + RiskManager Integration Test ===\n")

    config = load_config()
    df, df_htf, df_history = load_sample_data(config)

    # 1) Generate raw signals
    sg = SignalGenerator(htf_period=config["indicator"]["htf_period"])
    raw_signals, indicator_df = sg.generate_signals(df, df_htf)

    # 2) Apply filters
    pipeline = FilterPipeline(config)
    filtered_signals, _ = pipeline.apply_filters(df, raw_signals)

    # 3) RiskManager with full history
    risk_manager = RiskManager(config, ohlcv_data=df_history)

    # 4) Strategy with RiskManager injected
    trade_manager = TradeManager(config)
    strategy = WBWSStrategy(
        risk_manager=risk_manager,
        trade_manager=trade_manager
    )

    # 5) Iterate through test window
    rejected_count = 0

    for ts, row in df.iterrows():
        raw_sig = raw_signals.loc[ts]
        filt_sig = filtered_signals.loc[ts]

        sf = build_signal_frame(row, raw_sig, filt_sig)
        decision = strategy.on_bar(sf)

        # Print valid signals (BUY/SELL)
        if pd.notna(filt_sig) and filt_sig in ("BUY", "SELL"):
            print(f"{ts} | filtered={filt_sig}")

        # Print OPEN trades
        if decision.decision_type.name == "OPEN":
            tp = decision.trade_params
            print(
                f"  → OPEN {tp.direction.name} | entry={tp.entry:.2f} "
                f"sl={tp.stop_loss:.2f} tp={tp.take_profit:.2f} "
                f"(reason={decision.reason})"
            )
            continue

        # Print CLOSE trades
        if decision.decision_type.name == "CLOSE":
            print(f"  → CLOSE position (reason={decision.reason})")
            continue

        # Count rejections (NONE)
        if decision.decision_type.name == "NONE":
            rejected_count += 1

    print(f"\nTotal rejected decisions: {rejected_count}")


if __name__ == "__main__":
    main()
# tests/strategies/diagnostics/debug_pipeline_diff_fixed.py
"""Pipeline Comparison Debug Script - Using correct architecture imports"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
import logging
from datetime import datetime
from typing import Optional, Tuple, Dict, Any

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Add project root
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# =============================================================================
# Import correct modules based on ARCHITECTURE.md
# =============================================================================

# New architecture imports (from src.strategies.specific.modules)
from src.strategies.specific.modules.signal_generator import SignalGenerator as NewSignalGenerator
from src.strategies.specific.modules.filter_pipeline import FilterPipeline as NewFilterPipeline
from src.strategies.specific.modules.risk_manager import RiskManager as NewRiskManager
from src.strategies.specific.modules.spread_manager import SpreadManager
from src.strategies.contracts.signal_contracts import SignalFrame
from src.strategies.contracts.filter_contracts import FilterPipelineResult

# Config imports
from src.config.config_schema import (
    StrategyConfig, AssetConfig, DataConfig, DataPathsConfig,
    TradeManagementConfig, SpreadConfig, RiskConfig, PositionControlConfig,
    FilterPipelineConfig, FilterConfig, TimeFilterConfig,
    ExecutionConfig, OutputConfig, ReportConfig
)

# Legacy architecture imports (from src.strategies.core)
# Note: Legacy uses different import paths
from src.strategies.core.data_loader import DataLoader as LegacyDataLoader
from src.strategies.core.signal_generator import SignalGenerator as LegacySignalGenerator
from src.strategies.core.filter_pipeline import FilterPipeline as LegacyFilterPipeline
from src.strategies.core.trade_simulator import TradeSimulator as LegacyTradeSimulator

# Technical indicator - from src.indicators
from src.indicators import rsi

# =============================================================================
# Load test data
# =============================================================================

def load_test_data():
    """Load the exact data slice from 2025-12-12 18:00:00 to 21:00:00"""
    
    # Load main OHLCV data
    data_path = PROJECT_ROOT / "data" / "processed" / "ohlcv" / "DEUIDXEUR_1min_20240101_20260207.parquet"
    df = pd.read_parquet(data_path)
    
    # Standardize columns
    df.columns = df.columns.str.lower()
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
    
    df.sort_index(inplace=True)
    
    # Slice to test period
    start = "2025-12-12 18:00:00"
    end = "2025-12-12 21:00:00"
    df_test = df.loc[start:end].copy()
    
    # Load ARTF data
    artf_path = PROJECT_ROOT / "data" / "processed" / "ohlcv" / "DEUIDXEUR_1ME_20210101_20260207.parquet"
    df_artf = pd.read_parquet(artf_path)
    df_artf.columns = df_artf.columns.str.lower()
    if 'timestamp' in df_artf.columns:
        df_artf['timestamp'] = pd.to_datetime(df_artf['timestamp'])
        df_artf.set_index('timestamp', inplace=True)
    df_artf.sort_index(inplace=True)
    
    # Load HTF data
    htf_path = PROJECT_ROOT / "data" / "processed" / "ohlcv" / "DEUIDXEUR_1H_20240101_20260207.parquet"
    df_htf = pd.read_parquet(htf_path)
    df_htf.columns = df_htf.columns.str.lower()
    if 'timestamp' in df_htf.columns:
        df_htf['timestamp'] = pd.to_datetime(df_htf['timestamp'])
        df_htf.set_index('timestamp', inplace=True)
    df_htf = df_htf.loc[start:end].copy()
    df_htf.sort_index(inplace=True)
    
    logger.info(f"Loaded test data:")
    logger.info(f"  Main OHLCV: {len(df_test)} bars from {df_test.index[0]} to {df_test.index[-1]}")
    logger.info(f"  ARTF: {len(df_artf)} bars")
    logger.info(f"  HTF: {len(df_htf)} bars")
    
    return df_test, df_artf, df_htf

# =============================================================================
# Create proper config for New architecture
# =============================================================================

def create_new_config() -> StrategyConfig:
    """Create a proper StrategyConfig for New architecture with all required fields"""
    
    # Data paths
    paths = DataPathsConfig(
        strategy_ohlcv=PROJECT_ROOT / "data" / "processed" / "ohlcv" / "DEUIDXEUR_1min_20240101_20260207.parquet",
        htf_ohlcv=PROJECT_ROOT / "data" / "processed" / "ohlcv" / "DEUIDXEUR_1H_20240101_20260207.parquet",
        ltf_ohlcv=PROJECT_ROOT / "data" / "processed" / "ohlcv" / "DEUIDXEUR_1s_20240101_20260207.parquet",
        artf_ohlcv=PROJECT_ROOT / "data" / "processed" / "ohlcv" / "DEUIDXEUR_1ME_20210101_20260207.parquet"
    )
    
    # Data config
    data = DataConfig(
        paths=paths,
        date_range=None,
        timezone="CET",
        htf_period="1H",
        ltf_timeframe="1s",
        artf_timeframe="1ME"
    )
    
    # Asset config
    asset = AssetConfig(
        symbol="DEUIDXEUR",
        pip_size=0.1,
        point_size=1.0
    )
    
    # Execution config
    execution = ExecutionConfig(mode="analytics")
    
    # Spread config (from broker file)
    spread = SpreadConfig(
        enabled=False,  # Disable for signal comparison
        config_path=PROJECT_ROOT / "configs" / "spreads" / "broker_spreads.yaml"
    )
    
    # Risk config - WITH ALL REQUIRED FIELDS
    risk = RiskConfig(
        atr_length=14,
        atr_multiplier_sl=1.4,
        atr_multiplier_tp=7.98,
        max_risk_percentile=0.1,
        tp_mode="rr_ratio",
        risk_to_reward_ratio=5.7
    )
    
    # Position control
    position_control = PositionControlConfig(
        pyramiding_enabled=False,
        close_on_opposite=False,
        max_positions=1
    )
    
    # Trade management
    trade_mgmt = TradeManagementConfig(
        spread=spread,
        risk=risk,
        position_control=position_control
    )
    
    # Filters - with RSI enabled
    rsi_filter_cfg = FilterConfig(
        enabled=True,
        config={
            "length": 14,
            "overbought": 70,
            "oversold": 30
        }
    )
    
    time_filter_cfg = FilterConfig(
        enabled=False,
        config={}
    )
    
    filters = FilterPipelineConfig(
        time_filters={"time_filter": time_filter_cfg},
        technical_filters={"rsi_filter": rsi_filter_cfg},
        filter_sequence=["rsi_filter"]
    )
    
    # Output config
    output = OutputConfig(
        reports=ReportConfig(
            enabled=True,
            output_dir=PROJECT_ROOT / "outputs" / "strategies" / "reports" / "wbws",
            theme="dark",
            chart_height_px=300,
            brand_name="WBWSStrategy",
            include_raw_data=True
        ),
        logging_config={"level": "INFO", "output_dir": PROJECT_ROOT / "outputs" / "strategies" / "logs" / "wbws"}
    )
    
    return StrategyConfig(
        asset=asset,
        data=data,
        execution=execution,
        trade_management=trade_mgmt,
        filters=filters,
        output=output
    )

# =============================================================================
# Legacy Signal Generation
# =============================================================================

def run_legacy_signal_generation(df_test, df_htf):
    """Run legacy signal generator"""
    
    try:
        # Legacy signal generator takes htf_period as string
        sg = LegacySignalGenerator("1H")
        
        # Generate signals
        raw_signals, indicator_df = sg.generate_signals(df_test, df_htf)
        
        # Convert to list
        signals_list = []
        for ts, sig in raw_signals.dropna().items():
            signals_list.append({
                'timestamp': ts,
                'signal': sig,
                'price': df_test.loc[ts, 'close']
            })
        
        signals_df = pd.DataFrame(signals_list)
        if not signals_df.empty:
            signals_df.set_index('timestamp', inplace=True)
        
        logger.info(f"\n--- LEGACY SIGNAL GENERATION ---")
        logger.info(f"Raw signals: {len(signals_df)}")
        for ts, row in signals_df.iterrows():
            logger.info(f"  {ts}: {row['signal']} @ {row['price']:.2f}")
        
        return signals_df
        
    except Exception as e:
        logger.error(f"Legacy signal generation failed: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

# =============================================================================
# New Signal Generation
# =============================================================================

def run_new_signal_generation(df_test, df_htf):
    """Run new signal generator"""
    
    try:
        # Create proper config
        config = create_new_config()
        
        # Initialize signal generator
        sg = NewSignalGenerator(config, mode="analytics")
        
        # Generate signals
        signal_frame = sg.generate(df_test, df_htf)
        
        # Convert to comparable format
        signals_list = []
        for idx in signal_frame.signal_indices:
            ts = signal_frame.timestamps[idx]
            signal_val = signal_frame.signals[idx]
            signal_str = 'BUY' if signal_val == 1 else 'SELL' if signal_val == -1 else None
            if signal_str:
                signals_list.append({
                    'timestamp': ts,
                    'signal': signal_str,
                    'price': df_test.loc[ts, 'close']
                })
        
        signals_df = pd.DataFrame(signals_list)
        if not signals_df.empty:
            signals_df.set_index('timestamp', inplace=True)
        
        logger.info(f"\n--- NEW SIGNAL GENERATION ---")
        logger.info(f"Raw signals: {len(signals_df)}")
        for ts, row in signals_df.iterrows():
            logger.info(f"  {ts}: {row['signal']} @ {row['price']:.2f}")
        
        return signals_df, signal_frame
        
    except Exception as e:
        logger.error(f"New signal generation failed: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(), None

# =============================================================================
# Compare RSI Filter
# =============================================================================

def compare_rsi_filter(legacy_signals, new_signals, df_test):
    """Compare RSI filter results"""
    
    logger.info("\n" + "="*70)
    logger.info("RSI FILTER COMPARISON")
    logger.info("="*70)
    
    # Calculate RSI
    rsi_values = rsi(df_test['close'], length=14)
    
    # Legacy filter (manual)
    logger.info("\n--- LEGACY RSI FILTER ---")
    legacy_accepted = []
    for ts, row in legacy_signals.iterrows():
        rsi_val = rsi_values.loc[ts]
        signal = row['signal']
        
        if signal == 'BUY' and rsi_val < 70:
            legacy_accepted.append({'timestamp': ts, 'signal': signal, 'rsi': rsi_val})
            logger.info(f"  {ts}: {signal} ACCEPTED (RSI={rsi_val:.2f})")
        elif signal == 'SELL' and rsi_val > 30:
            legacy_accepted.append({'timestamp': ts, 'signal': signal, 'rsi': rsi_val})
            logger.info(f"  {ts}: {signal} ACCEPTED (RSI={rsi_val:.2f})")
        else:
            logger.info(f"  {ts}: {signal} REJECTED (RSI={rsi_val:.2f})")
    
    legacy_accepted_df = pd.DataFrame(legacy_accepted)
    if not legacy_accepted_df.empty:
        legacy_accepted_df.set_index('timestamp', inplace=True)
    
    # New filter (if we have signals)
    if new_signals.empty:
        logger.info("\n--- NEW RSI FILTER ---")
        logger.info("No signals to filter")
        new_accepted_df = pd.DataFrame()
    else:
        logger.info("\n--- NEW RSI FILTER ---")
        new_accepted = []
        for ts, row in new_signals.iterrows():
            rsi_val = rsi_values.loc[ts]
            signal = row['signal']
            
            if signal == 'BUY' and rsi_val < 70:
                new_accepted.append({'timestamp': ts, 'signal': signal, 'rsi': rsi_val})
                logger.info(f"  {ts}: {signal} ACCEPTED (RSI={rsi_val:.2f})")
            elif signal == 'SELL' and rsi_val > 30:
                new_accepted.append({'timestamp': ts, 'signal': signal, 'rsi': rsi_val})
                logger.info(f"  {ts}: {signal} ACCEPTED (RSI={rsi_val:.2f})")
            else:
                logger.info(f"  {ts}: {signal} REJECTED (RSI={rsi_val:.2f})")
        
        new_accepted_df = pd.DataFrame(new_accepted)
        if not new_accepted_df.empty:
            new_accepted_df.set_index('timestamp', inplace=True)
    
    # Compare
    logger.info("\n--- COMPARISON ---")
    legacy_set = set(legacy_accepted_df.index) if not legacy_accepted_df.empty else set()
    new_set = set(new_accepted_df.index) if not new_accepted_df.empty else set()
    
    logger.info(f"Legacy accepted: {len(legacy_set)}")
    logger.info(f"New accepted: {len(new_set)}")
    
    only_legacy = legacy_set - new_set
    only_new = new_set - legacy_set
    
    if only_legacy:
        logger.info(f"Signals ONLY in legacy: {sorted(only_legacy)}")
    if only_new:
        logger.info(f"Signals ONLY in new: {sorted(only_new)}")
    
    if not only_legacy and not only_new:
        logger.info("✓ RSI filter results are IDENTICAL!")
    
    return legacy_accepted_df, new_accepted_df, only_legacy, only_new

# =============================================================================
# Main
# =============================================================================

def main():
    logger.info("="*70)
    logger.info("PIPELINE DIAGNOSTIC - STEP BY STEP COMPARISON")
    logger.info("="*70)
    
    # Load data
    logger.info("\n1. Loading test data...")
    df_test, df_artf, df_htf = load_test_data()
    
    # Step 1: Compare raw signal generation
    logger.info("\n" + "="*70)
    logger.info("STEP 1: RAW SIGNAL GENERATION")
    logger.info("="*70)
    
    legacy_raw = run_legacy_signal_generation(df_test, df_htf)
    new_raw, new_signal_frame = run_new_signal_generation(df_test, df_htf)
    
    # Compare raw signals
    logger.info("\n--- RAW SIGNAL COMPARISON ---")
    legacy_times = set(legacy_raw.index) if not legacy_raw.empty else set()
    new_times = set(new_raw.index) if not new_raw.empty else set()
    
    logger.info(f"Legacy raw signals: {len(legacy_times)}")
    logger.info(f"New raw signals: {len(new_times)}")
    
    only_legacy_raw = legacy_times - new_times
    only_new_raw = new_times - legacy_times
    
    if only_legacy_raw:
        logger.info(f"Raw signals ONLY in legacy: {sorted(only_legacy_raw)}")
    if only_new_raw:
        logger.info(f"Raw signals ONLY in new: {sorted(only_new_raw)}")
    
    if not only_legacy_raw and not only_new_raw:
        logger.info("✓ Raw signals are IDENTICAL!")
    
    # Step 2: Compare RSI filter
    logger.info("\n" + "="*70)
    logger.info("STEP 2: RSI FILTER APPLICATION")
    logger.info("="*70)
    
    legacy_filtered, new_filtered, only_legacy_filt, only_new_filt = compare_rsi_filter(
        legacy_raw, new_raw, df_test
    )
    
    # Step 3: Check specific timestamps
    logger.info("\n" + "="*70)
    logger.info("STEP 3: CHECKING SPECIFIC TIMESTAMPS")
    logger.info("="*70)
    
    target_times = [
        pd.Timestamp('2025-12-12 19:53:00'),
        pd.Timestamp('2025-12-12 20:00:00')
    ]
    
    for ts in target_times:
        logger.info(f"\nTimestamp: {ts}")
        
        # Check in raw signals
        in_legacy_raw = ts in legacy_times
        in_new_raw = ts in new_times
        logger.info(f"  Raw signals: Legacy={'✓' if in_legacy_raw else '✗'}, New={'✓' if in_new_raw else '✗'}")
        
        # Check in filtered signals
        in_legacy_filt = ts in (legacy_filtered.index if not legacy_filtered.empty else set())
        in_new_filt = ts in (new_filtered.index if not new_filtered.empty else set())
        logger.info(f"  Filtered: Legacy={'✓' if in_legacy_filt else '✗'}, New={'✓' if in_new_filt else '✗'}")
        
        # Show RSI value
        if ts in df_test.index:
            rsi_val = rsi(df_test['close'], length=14).loc[ts]
            logger.info(f"  RSI at {ts}: {rsi_val:.2f}")
    
    # Summary
    logger.info("\n" + "="*70)
    logger.info("DIAGNOSTIC SUMMARY")
    logger.info("="*70)
    
    if only_legacy_raw or only_new_raw:
        logger.info("❌ DIFFERENCE FOUND AT RAW SIGNAL GENERATION STAGE")
        if only_new_raw:
            logger.info(f"   New generates signals that legacy doesn't at: {sorted(only_new_raw)}")
        if only_legacy_raw:
            logger.info(f"   Legacy generates signals that new doesn't at: {sorted(only_legacy_raw)}")
    elif only_legacy_filt or only_new_filt:
        logger.info("❌ DIFFERENCE FOUND AT FILTER STAGE")
        if only_new_filt:
            logger.info(f"   New accepts signals that legacy rejects at: {sorted(only_new_filt)}")
        if only_legacy_filt:
            logger.info(f"   Legacy accepts signals that new rejects at: {sorted(only_legacy_filt)}")
    else:
        logger.info("✓ ALL STAGES IDENTICAL - Difference must be in trade simulation or risk calculation")
    
    logger.info("\n" + "="*70)

if __name__ == "__main__":
    main()
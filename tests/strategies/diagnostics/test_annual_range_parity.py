"""
Annual Range Calculation Parity Test

This test directly compares how Legacy and New systems calculate the annual range
from the same monthly ARTF data.

Run with: python tests/strategies/diagnostics/test_annual_range_parity.py
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import New modules
from src.strategies.specific.modules.risk_manager import RiskManager as NewRiskManager
from src.config.config_schema import StrategyConfig, AssetConfig, DataConfig, DataPathsConfig
from src.config.config_schema import TradeManagementConfig, SpreadConfig, RiskConfig, PositionControlConfig
from src.config.config_schema import ExecutionConfig, OutputConfig, FilterPipelineConfig


def load_real_data():
    """Load the actual data files used in your backtest."""
    
    # Paths to your real data
    strategy_path = PROJECT_ROOT / "data" / "processed" / "ohlcv" / "DEUIDXEUR_1min_20240101_20260207.parquet"
    artf_path = PROJECT_ROOT / "data" / "processed" / "ohlcv" / "DEUIDXEUR_1ME_20210101_20260207.parquet"
    
    logger.info(f"Loading strategy data from: {strategy_path}")
    logger.info(f"Loading ARTF data from: {artf_path}")
    
    # Load data
    df_strategy = pd.read_parquet(strategy_path)
    df_artf = pd.read_parquet(artf_path)
    
    # Standardize columns
    df_strategy.columns = df_strategy.columns.str.lower()
    df_artf.columns = df_artf.columns.str.lower()
    
    # Ensure datetime index
    if 'timestamp' in df_strategy.columns:
        df_strategy['timestamp'] = pd.to_datetime(df_strategy['timestamp'])
        df_strategy.set_index('timestamp', inplace=True)
    
    if 'timestamp' in df_artf.columns:
        df_artf['timestamp'] = pd.to_datetime(df_artf['timestamp'])
        df_artf.set_index('timestamp', inplace=True)
    
    # Sort indexes
    df_strategy.sort_index(inplace=True)
    df_artf.sort_index(inplace=True)
    
    # Use the same date range as your backtest
    start_date = "2025-09-14"
    end_date = "2025-12-17"
    df_strategy = df_strategy.loc[start_date:end_date]
    
    logger.info(f"Strategy data: {len(df_strategy):,} bars from {df_strategy.index.min()} to {df_strategy.index.max()}")
    logger.info(f"ARTF data: {len(df_artf):,} bars from {df_artf.index.min()} to {df_artf.index.max()}")
    
    return df_strategy, df_artf


def create_new_config() -> StrategyConfig:
    """Create New-style typed StrategyConfig."""
    
    paths = DataPathsConfig(
        strategy_ohlcv=Path("dummy.parquet"),
        artf_ohlcv=Path("dummy_artf.parquet"),
    )
    
    data = DataConfig(
        paths=paths,
        date_range=None,
    )
    
    asset = AssetConfig(symbol="DEUIDXEUR")
    
    spread = SpreadConfig(
        enabled=False,  # Disable spread for this test
        config_path=None,
    )
    
    risk = RiskConfig(
        atr_length=14,
        atr_multiplier_sl=1.4,
        atr_multiplier_tp=7.98,
        max_risk_percentile=0.1,  # Value doesn't matter
        tp_mode="rr_ratio",
        risk_to_reward_ratio=5.7,
    )
    
    position_control = PositionControlConfig(
        pyramiding_enabled=False,
        close_on_opposite=False,
        max_positions=1,
    )
    
    trade_mgmt = TradeManagementConfig(
        spread=spread,
        risk=risk,
        position_control=position_control,
    )
    
    execution = ExecutionConfig(mode="analytics")
    filters = FilterPipelineConfig(time_filters={}, technical_filters={}, filter_sequence=[])
    output = OutputConfig()
    
    return StrategyConfig(
        asset=asset,
        data=data,
        execution=execution,
        trade_management=trade_mgmt,
        filters=filters,
        output=output,
    )


class LegacyAnnualRangeCalculator:
    """
    Replicates Legacy RiskManager's annual range calculation logic.
    """
    
    def __init__(self, df_strategy, df_artf):
        self.df_strategy = df_strategy
        self.df_artf = df_artf
        self.annual_range_series = None
        self._calculate()
    
    def _calculate(self):
        """Calculate annual range using Legacy methodology."""
        
        if self.df_artf is None or self.df_artf.empty:
            logger.warning("No ARTF data provided")
            return
        
        # Prepare monthly data
        monthly = self.df_artf.copy()
        monthly = monthly.sort_index()
        monthly.index = monthly.index.normalize()
        
        # Create year-month period index
        monthly['ym'] = monthly.index.to_period('M')
        monthly_by_ym = monthly.set_index('ym')[['high', 'low']]
        
        # Get all year-month periods in order
        yms = monthly_by_ym.index.unique().sort_values()
        
        # Calculate annual range for each month (using previous 12 months)
        rar_per_month = {}
        
        for i, ym in enumerate(yms):
            if i < 12:
                # Not enough history - use all available data
                window = monthly_by_ym.loc[:ym]
            else:
                # Use previous 12 months (excluding current month)
                start_ym = yms[i-12]
                end_ym = yms[i-1]
                window = monthly_by_ym.loc[start_ym:end_ym]
            
            if len(window) > 0:
                rar_per_month[ym] = float(window['high'].max() - window['low'].min())
            else:
                rar_per_month[ym] = np.nan
        
        # Create Series with monthly values
        rar_monthly = pd.Series(rar_per_month, dtype='float32')
        
        # Map to strategy timestamps (use previous month's range)
        # Convert strategy timestamps to year-month periods
        strategy_ym = self.df_strategy.index.to_period('M')
        
        # For each strategy timestamp, we want the annual range from the PREVIOUS month
        # This avoids lookahead bias
        prev_ym = pd.Series(strategy_ym - 1, index=self.df_strategy.index)
        
        # Map to values
        rar_values = prev_ym.map(rar_monthly)
        
        # Convert to Series with proper index
        self.annual_range_series = pd.Series(
            rar_values.values,
            index=self.df_strategy.index,
            dtype='float32'
        )
        
        # Forward fill any missing values (for early dates before we have 12 months of history)
        self.annual_range_series = self.annual_range_series.ffill()


class NewAnnualRangeCalculator:
    """Extracts annual range from New RiskManager."""
    
    def __init__(self, df_strategy, df_artf):
        config = create_new_config()
        self.rm = NewRiskManager(
            config=config,
            ohlcv_data=df_strategy,
            ohlcv_artf=df_artf,
            mode="analytics",
        )
        self.annual_range_series = self.rm.annual_range_series


def analyze_artf_data(df_artf):
    """Analyze the raw ARTF data to understand what annual range should be."""
    
    logger.info("\n--- ARTF Data Analysis ---")
    logger.info(f"ARTF date range: {df_artf.index.min()} to {df_artf.index.max()}")
    logger.info(f"ARTF bars: {len(df_artf)}")
    
    # Calculate true 12-month rolling ranges from ARTF data
    monthly_high = df_artf['high']
    monthly_low = df_artf['low']
    
    # 12-month rolling range (using previous 12 months, not including current)
    rolling_ranges = []
    for i in range(12, len(df_artf)):
        prev_12_high = monthly_high.iloc[i-12:i].max()
        prev_12_low = monthly_low.iloc[i-12:i].min()
        rolling_ranges.append(prev_12_high - prev_12_low)
    
    if rolling_ranges:
        logger.info(f"\nTrue 12-month rolling ranges (from ARTF only):")
        logger.info(f"  Mean: {np.mean(rolling_ranges):.2f}")
        logger.info(f"  Min: {np.min(rolling_ranges):.2f}")
        logger.info(f"  Max: {np.max(rolling_ranges):.2f}")
        logger.info(f"  Std: {np.std(rolling_ranges):.2f}")
    
    # Calculate annual range for each month (using previous 12 months)
    monthly_ranges = {}
    for i in range(12, len(df_artf)):
        month = df_artf.index[i]
        prev_12_high = monthly_high.iloc[i-12:i].max()
        prev_12_low = monthly_low.iloc[i-12:i].min()
        monthly_ranges[month] = prev_12_high - prev_12_low
    
    return monthly_ranges


def compare_annual_ranges(legacy_calc, new_calc, df_strategy, sample_size=100):
    """Compare annual range values at sample timestamps."""
    
    if legacy_calc.annual_range_series is None or new_calc.annual_range_series is None:
        logger.error("One or both annual range series are None")
        return None
    
    # Sample timestamps evenly spaced
    total_bars = len(df_strategy)
    step = max(1, total_bars // sample_size)
    sample_indices = list(range(0, total_bars, step))[:sample_size]
    sample_timestamps = df_strategy.index[sample_indices]
    
    comparisons = []
    
    for ts in sample_timestamps:
        try:
            legacy_val = legacy_calc.annual_range_series.loc[ts]
            new_val = new_calc.annual_range_series.loc[ts]
            
            if (not pd.isna(legacy_val) and not pd.isna(new_val) and 
                legacy_val > 0 and new_val > 0):
                ratio = legacy_val / new_val
                comparisons.append({
                    'timestamp': ts,
                    'legacy_range': legacy_val,
                    'new_range': new_val,
                    'ratio': ratio,
                    'diff_pct': (legacy_val - new_val) / new_val * 100,
                })
        except KeyError:
            continue
    
    return pd.DataFrame(comparisons) if comparisons else None

# Add to the comparison
class LegacyATRCalculator:
    def __init__(self, df_strategy):
        self.df_strategy = df_strategy
        self.atr_series = self._calculate_atr()
    
    def _calculate_atr(self, length=14):
        high = self.df_strategy['high']
        low = self.df_strategy['low']
        close = self.df_strategy['close']
        
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        return tr.ewm(alpha=1/length, adjust=False).mean().astype('float32')


class NewATRCalculator:
    def __init__(self, df_strategy):
        config = create_new_config()
        self.rm = NewRiskManager(
            config=config,
            ohlcv_data=df_strategy,
            ohlcv_artf=None,
            mode="analytics",
        )
        self.atr_series = self.rm.atr_series


def main():
    """Main diagnostic test."""
    logger.info("=" * 70)
    logger.info("ANNUAL RANGE PARITY DIAGNOSTIC")
    logger.info("=" * 70)
    
    # Load real data
    logger.info("\n1. Loading real data...")
    df_strategy, df_artf = load_real_data()
    
    # Analyze ARTF data
    true_monthly_ranges = analyze_artf_data(df_artf)
    
    # Initialize calculators
    logger.info("\n2. Calculating annual ranges...")
    
    logger.info("   Calculating Legacy-style annual range...")
    legacy_calc = LegacyAnnualRangeCalculator(
        df_strategy=df_strategy.copy(),
        df_artf=df_artf.copy(),
    )
    
    logger.info("   Calculating New-style annual range...")
    new_calc = NewAnnualRangeCalculator(
        df_strategy=df_strategy.copy(),
        df_artf=df_artf.copy(),
    )
    
    # Compare annual ranges
    logger.info("\n3. Comparing annual range calculations...")
    df_comp = compare_annual_ranges(legacy_calc, new_calc, df_strategy, sample_size=50)
    
    if df_comp is not None and len(df_comp) > 0:
        logger.info(f"\nAnnual Range Comparison Statistics (over {len(df_comp)} samples):")
        logger.info(f"  Legacy mean: {df_comp['legacy_range'].mean():.2f}")
        logger.info(f"  New mean:    {df_comp['new_range'].mean():.2f}")
        logger.info(f"  Mean ratio (Legacy/New): {df_comp['ratio'].mean():.2f}x")
        logger.info(f"  Median ratio: {df_comp['ratio'].median():.2f}x")
        logger.info(f"  Ratio std: {df_comp['ratio'].std():.2f}")
        logger.info(f"  Min ratio: {df_comp['ratio'].min():.2f}x")
        logger.info(f"  Max ratio: {df_comp['ratio'].max():.2f}x")
        
        # Show sample comparisons
        logger.info(f"\nSample comparisons (first 10):")
        for i, row in df_comp.head(10).iterrows():
            logger.info(
                f"  {row['timestamp'].strftime('%Y-%m-%d %H:%M')}: "
                f"Legacy={row['legacy_range']:8.0f}, "
                f"New={row['new_range']:8.0f}, "
                f"Ratio={row['ratio']:5.1f}x"
            )
        
        # Compare with true monthly ranges
        logger.info(f"\n4. Comparison with true ARTF-based ranges:")
        
        # Get a sample month from strategy period
        sample_date = df_strategy.index[len(df_strategy)//2]
        sample_month = sample_date.replace(day=1)
        
        # Find the closest month in true ranges
        closest_month = min(true_monthly_ranges.keys(), key=lambda x: abs((x - sample_month).days))
        true_range = true_monthly_ranges[closest_month]
        
        # Find corresponding values from both systems around that time
        legacy_at_month = legacy_calc.annual_range_series.loc[sample_date]
        new_at_month = new_calc.annual_range_series.loc[sample_date]
        
        logger.info(f"  For date {sample_date.strftime('%Y-%m-%d')} (month {closest_month.strftime('%Y-%m')}):")
        logger.info(f"    True ARTF 12-month range: {true_range:.0f}")
        logger.info(f"    Legacy range: {legacy_at_month:.0f} (ratio to true: {legacy_at_month/true_range:.2f}x)")
        logger.info(f"    New range:    {new_at_month:.0f} (ratio to true: {new_at_month/true_range:.2f}x)")
        
        # Calculate correlation
        if len(df_comp) > 1:
            corr = df_comp['legacy_range'].corr(df_comp['new_range'])
            logger.info(f"\n5. Correlation between Legacy and New ranges: {corr:.3f}")
    
    else:
        logger.error("No valid comparisons found")
    
    logger.info("\n" + "=" * 70)


if __name__ == "__main__":
    main()
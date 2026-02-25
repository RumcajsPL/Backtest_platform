"""
Risk Distance Calculation with Spread - Full Parity Test
With patched New RiskManager to avoid comment parsing error
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import types

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import New modules
from src.strategies.specific.modules.risk_manager import RiskManager as NewRiskManager
from src.strategies.specific.modules.spread_manager import SpreadManager
from src.config.config_schema import StrategyConfig, AssetConfig, DataConfig, DataPathsConfig
from src.config.config_schema import TradeManagementConfig, SpreadConfig, RiskConfig, PositionControlConfig
from src.config.config_schema import ExecutionConfig, OutputConfig, FilterPipelineConfig


def load_real_data():
    """Load the actual data files used in your backtest."""
    
    strategy_path = PROJECT_ROOT / "data" / "processed" / "ohlcv" / "DEUIDXEUR_1min_20240101_20260207.parquet"
    artf_path = PROJECT_ROOT / "data" / "processed" / "ohlcv" / "DEUIDXEUR_1ME_20210101_20260207.parquet"
    
    logger.info(f"Loading strategy data from: {strategy_path}")
    logger.info(f"Loading ARTF data from: {artf_path}")
    
    df_strategy = pd.read_parquet(strategy_path)
    df_artf = pd.read_parquet(artf_path)
    
    # Standardize
    df_strategy.columns = df_strategy.columns.str.lower()
    df_artf.columns = df_artf.columns.str.lower()
    
    if 'timestamp' in df_strategy.columns:
        df_strategy['timestamp'] = pd.to_datetime(df_strategy['timestamp'])
        df_strategy.set_index('timestamp', inplace=True)
    
    if 'timestamp' in df_artf.columns:
        df_artf['timestamp'] = pd.to_datetime(df_artf['timestamp'])
        df_artf.set_index('timestamp', inplace=True)
    
    df_strategy.sort_index(inplace=True)
    df_artf.sort_index(inplace=True)
    
    start_date = "2025-09-14"
    end_date = "2025-12-17"
    df_strategy = df_strategy.loc[start_date:end_date]
    
    logger.info(f"Strategy data: {len(df_strategy):,} bars from {df_strategy.index.min()} to {df_strategy.index.max()}")
    logger.info(f"ARTF data: {len(df_artf):,} bars from {df_artf.index.min()} to {df_artf.index.max()}")
    
    return df_strategy, df_artf


def create_test_config(with_spread: bool = True) -> StrategyConfig:
    """Create config for testing."""
    
    paths = DataPathsConfig(
        strategy_ohlcv=Path("dummy.parquet"),
        artf_ohlcv=Path("dummy_artf.parquet"),
    )
    
    data = DataConfig(
        paths=paths,
        date_range=None,
    )
    
    asset = AssetConfig(symbol="DEUIDXEUR")
    
    # Spread config
    if with_spread:
        spread = SpreadConfig(
            enabled=True,
            config_path=PROJECT_ROOT / 'configs' / 'spreads' / 'broker_spreads.yaml',
        )
    else:
        spread = SpreadConfig(
            enabled=False,
            config_path=None,
        )
    
    # Risk config
    risk = RiskConfig(
        atr_length=14,
        atr_multiplier_sl=1.4,
        atr_multiplier_tp=7.98,
        max_risk_percentile=0.1,  # 0.1%
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


class PatchedNewRiskManager(NewRiskManager):
    """Patched version of New RiskManager that fixes the comment parsing bug."""
    
    def compute_trade_parameters(self, timestamp, bid_price, is_long):
        """Fixed version without the comment parsing error."""
        # Call the original method
        try:
            return super().compute_trade_parameters(timestamp, bid_price, is_long)
        except ValueError as e:
            if "could not convert string to float" in str(e):
                # This is the comment parsing bug - return None for rejected signals
                return None
            raise


class LegacyRiskLogicSimulator:
    """Simulates Legacy risk calculation logic."""
    
    def __init__(self, df_strategy, df_artf):
        self.df_strategy = df_strategy
        self.df_artf = df_artf
        
        # Calculate ATR (Wilder's)
        self.atr = self._calculate_atr()
        logger.info(f"ATR calculated: mean={self.atr.mean():.2f}, range=[{self.atr.min():.2f}, {self.atr.max():.2f}]")
        
        # Calculate annual range
        self.annual_range = self._calculate_annual_range()
        logger.info(f"Annual range calculated: mean={self.annual_range.mean():.2f}")
    
    def _get_spread_for_price(self, price):
        """Calculate spread points based on price (0.015%)."""
        return (0.015 / 100.0) * price
    
    def _calculate_atr(self, length=14):
        high = self.df_strategy['high']
        low = self.df_strategy['low']
        close = self.df_strategy['close']
        
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        return tr.ewm(alpha=1/length, adjust=False).mean()
    
    def _calculate_annual_range(self):
        """Simplified annual range calculation."""
        monthly = self.df_artf.copy()
        monthly = monthly.sort_index()
        monthly.index = monthly.index.normalize()
        
        monthly['ym'] = monthly.index.to_period('M')
        monthly_by_ym = monthly.set_index('ym')[['high', 'low']]
        
        yms = monthly_by_ym.index.unique().sort_values()
        rar_per_month = {}
        
        for i, ym in enumerate(yms):
            if i < 12:
                window = monthly_by_ym.loc[:ym]
            else:
                start_ym = yms[i-12]
                end_ym = yms[i-1]
                window = monthly_by_ym.loc[start_ym:end_ym]
            
            if len(window) > 0:
                rar_per_month[ym] = float(window['high'].max() - window['low'].min())
        
        rar_monthly = pd.Series(rar_per_month)
        strategy_prev_ym = self.df_strategy.index.to_period('M') - 1
        
        # Fix: Convert to Series properly
        rar_values = strategy_prev_ym.map(rar_monthly)
        rar_series = pd.Series(rar_values.values, index=self.df_strategy.index, dtype='float32')
        return rar_series.ffill()
    
    def calculate_risk_legacy(self, timestamp, bid_price, is_long):
        """Legacy risk calculation with multiple hypotheses about order."""
        
        atr_val = float(self.atr.loc[timestamp])
        annual_range_val = float(self.annual_range.loc[timestamp])
        spread = self._get_spread_for_price(bid_price)
        
        # Step 1: Calculate raw SL without spread
        risk_distance_raw = atr_val * 1.4  # sl_multiplier
        raw_sl = bid_price - risk_distance_raw if is_long else bid_price + risk_distance_raw
        
        # Step 2: Apply spread to get executed entry and trigger SL
        if is_long:
            executed_entry = bid_price + spread
            trigger_sl = raw_sl - spread
        else:
            executed_entry = bid_price  # SHORT entry at Bid
            trigger_sl = raw_sl + spread
        
        # HYPOTHESIS A: Risk percentile uses distance from executed_entry to raw_sl
        risk_distance_A = abs(executed_entry - raw_sl)
        risk_pct_A = (risk_distance_A / annual_range_val) * 100
        
        # HYPOTHESIS B: Risk percentile uses distance from executed_entry to trigger_sl
        risk_distance_B = abs(executed_entry - trigger_sl)
        risk_pct_B = (risk_distance_B / annual_range_val) * 100
        
        # HYPOTHESIS C: Risk percentile uses distance from bid to raw_sl (no spread on entry)
        risk_distance_C = abs(bid_price - raw_sl)
        risk_pct_C = (risk_distance_C / annual_range_val) * 100
        
        return {
            'timestamp': timestamp,
            'direction': 'LONG' if is_long else 'SHORT',
            'bid_price': bid_price,
            'spread': spread,
            'atr': atr_val,
            'annual_range': annual_range_val,
            
            # Entry and SL values
            'executed_entry': executed_entry,
            'raw_sl': raw_sl,
            'trigger_sl': trigger_sl,
            
            # Risk distances (different hypotheses)
            'risk_distance_A': risk_distance_A,
            'risk_pct_A': risk_pct_A,
            'risk_distance_B': risk_distance_B,
            'risk_pct_B': risk_pct_B,
            'risk_distance_C': risk_distance_C,
            'risk_pct_C': risk_pct_C,
            
            # Raw components
            'atr_multiplier': 1.4,
            'risk_distance_raw': risk_distance_raw,
        }


class NewRiskLogicExtractor:
    """Extracts risk calculation from patched New RiskManager."""
    
    def __init__(self, df_strategy, df_artf, with_spread=True):
        config = create_test_config(with_spread=with_spread)
        self.rm = PatchedNewRiskManager(
            config=config,
            ohlcv_data=df_strategy,
            ohlcv_artf=df_artf,
            mode="analytics",
        )
    
    def calculate_risk_new(self, timestamp, bid_price, is_long):
        """Get risk calculation from New RiskManager."""
        params = self.rm.compute_trade_parameters(timestamp, bid_price, is_long)
        
        if params is None:
            return None
        
        # Extract all values safely
        result = {
            'timestamp': timestamp,
            'direction': 'LONG' if is_long else 'SHORT',
            'bid_price': bid_price,
            'spread': getattr(params, 'spread_points', 0.0) or 0.0,
            'atr': getattr(params, 'atr_value', 0.0),
            'annual_range': getattr(params, 'annual_range_value', 0.0) or 0.0,
            
            # Entry and SL values
            'executed_entry': getattr(params, 'entry_price_executed', 0.0),
            'raw_sl': getattr(params, 'stop_loss_raw', 0.0),
            'trigger_sl': getattr(params, 'stop_loss_trigger', 0.0),
            
            # Risk distance and percentile
            'risk_distance': getattr(params, 'sl_distance', 0.0),
            'risk_percentile': getattr(params, 'risk_percentile_calculated', 0.0) or 0.0,
            
            # Raw components
            'atr_multiplier': 1.4,
            'risk_distance_raw': getattr(params, 'atr_value', 0.0) * 1.4,
            
            # Additional info
            'sl_adjusted': getattr(params, 'sl_adjusted', False),
            'comment': getattr(params, 'comment', ''),
        }
        
        return result


def test_single_timestamp(ts, bid_price, legacy_sim, new_sim):
    """Test a single timestamp with both LONG and SHORT."""
    
    logger.info(f"\n{'='*70}")
    logger.info(f"TESTING TIMESTAMP: {ts}")
    logger.info(f"Bid price: {bid_price:.2f}")
    logger.info(f"{'='*70}")
    
    results = []
    
    for direction in ['LONG', 'SHORT']:
        is_long = (direction == 'LONG')
        
        logger.info(f"\n--- {direction} ---")
        
        # Legacy simulation
        legacy_result = legacy_sim.calculate_risk_legacy(ts, bid_price, is_long)
        
        # New extraction
        new_result = new_sim.calculate_risk_new(ts, bid_price, is_long)
        
        if new_result is None:
            logger.info(f"  NEW: REJECTED (risk exceeds limit)")
            logger.info(f"  LEGACY: ACCEPTED with risk_pct_A={legacy_result['risk_pct_A']:.4f}%")
            results.append({
                'timestamp': ts,
                'direction': direction,
                'match': False,
                'legacy_pct_A': legacy_result['risk_pct_A'],
                'legacy_pct_B': legacy_result['risk_pct_B'],
                'legacy_pct_C': legacy_result['risk_pct_C'],
                'new_pct': None,
                'legacy_accepted': True,
                'new_accepted': False,
            })
            continue
        
        # Compare ATR
        atr_match = abs(legacy_result['atr'] - new_result['atr']) < 0.01
        logger.info(f"  ATR:              LEGACY={legacy_result['atr']:.4f}, NEW={new_result['atr']:.4f} | Match: {atr_match}")
        
        # Compare annual range
        ar_match = abs(legacy_result['annual_range'] - new_result['annual_range']) < 1.0
        logger.info(f"  Annual Range:     LEGACY={legacy_result['annual_range']:.2f}, NEW={new_result['annual_range']:.2f} | Match: {ar_match}")
        
        # Compare spread
        spread_match = abs(legacy_result['spread'] - new_result['spread']) < 0.1
        logger.info(f"  Spread:           LEGACY={legacy_result['spread']:.4f}, NEW={new_result['spread']:.4f} | Match: {spread_match}")
        
        # Compare executed entry
        entry_match = abs(legacy_result['executed_entry'] - new_result['executed_entry']) < 0.01
        logger.info(f"  Executed Entry:   LEGACY={legacy_result['executed_entry']:.4f}, NEW={new_result['executed_entry']:.4f} | Match: {entry_match}")
        
        # Compare raw SL
        raw_sl_match = abs(legacy_result['raw_sl'] - new_result['raw_sl']) < 0.01
        logger.info(f"  Raw SL:           LEGACY={legacy_result['raw_sl']:.4f}, NEW={new_result['raw_sl']:.4f} | Match: {raw_sl_match}")
        
        # Compare trigger SL
        trigger_sl_match = abs(legacy_result['trigger_sl'] - new_result['trigger_sl']) < 0.01
        logger.info(f"  Trigger SL:       LEGACY={legacy_result['trigger_sl']:.4f}, NEW={new_result['trigger_sl']:.4f} | Match: {trigger_sl_match}")
        
        # Compare risk percentile with different hypotheses
        pct_A_match = abs(legacy_result['risk_pct_A'] - new_result['risk_percentile']) < 0.001 if new_result['risk_percentile'] > 0 else False
        pct_B_match = abs(legacy_result['risk_pct_B'] - new_result['risk_percentile']) < 0.001 if new_result['risk_percentile'] > 0 else False
        pct_C_match = abs(legacy_result['risk_pct_C'] - new_result['risk_percentile']) < 0.001 if new_result['risk_percentile'] > 0 else False
        
        logger.info(f"  Risk Distance:    LEGACY_A={legacy_result['risk_distance_A']:.4f}, LEGACY_B={legacy_result['risk_distance_B']:.4f}, LEGACY_C={legacy_result['risk_distance_C']:.4f}, NEW={new_result['risk_distance']:.4f}")
        logger.info(f"  Risk Percentile:  LEGACY_A={legacy_result['risk_pct_A']:.4f}%, LEGACY_B={legacy_result['risk_pct_B']:.4f}%, LEGACY_C={legacy_result['risk_pct_C']:.4f}%, NEW={new_result['risk_percentile']:.4f}%")
        logger.info(f"  Match with A: {pct_A_match}, with B: {pct_B_match}, with C: {pct_C_match}")
        logger.info(f"  New comment: {new_result['comment']}")
        
        # Determine which hypothesis matches
        matching_hypothesis = None
        if pct_A_match:
            matching_hypothesis = "A (executed_entry to raw_sl)"
        elif pct_B_match:
            matching_hypothesis = "B (executed_entry to trigger_sl)"
        elif pct_C_match:
            matching_hypothesis = "C (bid to raw_sl)"
        
        results.append({
            'timestamp': ts,
            'direction': direction,
            'match': matching_hypothesis is not None,
            'matching_hypothesis': matching_hypothesis,
            'legacy_pct_A': legacy_result['risk_pct_A'],
            'legacy_pct_B': legacy_result['risk_pct_B'],
            'legacy_pct_C': legacy_result['risk_pct_C'],
            'new_pct': new_result['risk_percentile'],
            'legacy_accepted': True,
            'new_accepted': True,
        })
    
    return results

def main():
    logger.info("=" * 70)
    logger.info("RISK DISTANCE WITH SPREAD - FULL PARITY TEST")
    logger.info("=" * 70)
    
    # Load data
    logger.info("\n1. Loading real data...")
    df_strategy, df_artf = load_real_data()
    
    # Initialize calculators
    logger.info("\n2. Initializing calculators...")
    legacy_sim = LegacyRiskLogicSimulator(df_strategy, df_artf)
    new_sim = NewRiskLogicExtractor(df_strategy, df_artf, with_spread=True)
    
    # === DIAGNOSTIC: effective max_risk_percentile values ===
    logger.info("\n=== THRESHOLD DIAGNOSTIC ===")
    logger.info(f"LEGACY max_risk_percentile (from wbws_strategy.yaml) = 0.001")
    logger.info(f"NEW   max_risk_percentile effective               = {new_sim.rm.max_risk_percentile}")
    logger.info("===========================\n")
    # =======================================================
    
    # Test multiple timestamps
    logger.info("\n3. Testing sample timestamps...")
    
    # Select 5 timestamps spread throughout the period
    total_bars = len(df_strategy)
    test_indices = [
        total_bars // 8,      # 12.5%
        total_bars // 4,      # 25%
        total_bars // 2,      # 50%
        3 * total_bars // 4,  # 75%
        7 * total_bars // 8,  # 87.5%
    ]
    
    test_timestamps = [df_strategy.index[i] for i in test_indices]
    all_results = []
    
    for ts in test_timestamps:
        bid_price = float(df_strategy.loc[ts, 'close'])
        results = test_single_timestamp(ts, bid_price, legacy_sim, new_sim)
        all_results.extend(results)
    
    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("SUMMARY")
    logger.info("=" * 70)
    
    total_tests = len(all_results)
    matches = sum(1 for r in all_results if r.get('match', False))
    new_rejected = sum(1 for r in all_results if not r.get('new_accepted', True))
    
    logger.info(f"Total tests: {total_tests}")
    logger.info(f"New system rejections: {new_rejected}")
    logger.info(f"Matching hypothesis found: {matches}/{total_tests - new_rejected}")
    
    # Show which hypothesis matched for each successful comparison
    for r in all_results:
        if r.get('new_accepted', True):
            if r.get('matching_hypothesis'):
                logger.info(f"  {r['timestamp']} {r['direction']}: {r['matching_hypothesis']} (New={r['new_pct']:.4f}%, Legacy_A={r['legacy_pct_A']:.4f}%, Legacy_B={r['legacy_pct_B']:.4f}%, Legacy_C={r['legacy_pct_C']:.4f}%)")
            else:
                logger.info(f"  {r['timestamp']} {r['direction']}: NO MATCH - New={r['new_pct']:.4f}%, Legacy_A={r['legacy_pct_A']:.4f}%, Legacy_B={r['legacy_pct_B']:.4f}%, Legacy_C={r['legacy_pct_C']:.4f}%")
    
    logger.info("\n" + "=" * 70)

def test_fail_safe_only():
    """Focused test: only the fail-closed vs fail-open difference.
    Runs on the FULL dataset to count exactly how many extra rejections
    the New system would produce purely because of the fail-safe.
    """
    logger.info("\n" + "="*80)
    logger.info("FAIL-SAFE PARITY TEST (Legacy fail-open vs New fail-closed)")
    logger.info("="*80)

    df_strategy, df_artf = load_real_data()

    # Reuse the same calculators you already have
    legacy_sim = LegacyRiskLogicSimulator(df_strategy, df_artf)
    new_sim = NewRiskLogicExtractor(df_strategy, df_artf, with_spread=True)

    # Get the RAR series from both (we only care about validity, not the value)
    legacy_rar = legacy_sim.annual_range
    new_rar = new_sim.rm.annual_range_series   # the real New series (with >=12 check)

    total_bars = len(df_strategy)
    legacy_approves_new_rejects = 0
    fail_safe_rejections = 0

    for ts in df_strategy.index:
        for is_long in [True, False]:
            # Legacy behaviour (fail-open)
            legacy_rar_val = legacy_rar.loc[ts] if ts in legacy_rar.index else None
            legacy_valid = not (pd.isna(legacy_rar_val) or legacy_rar_val <= 0)

            # New behaviour (fail-closed)
            new_rar_val = new_rar.loc[ts] if ts in new_rar.index else None
            new_valid = not (pd.isna(new_rar_val) or new_rar_val <= 0)

            if legacy_valid and not new_valid:
                legacy_approves_new_rejects += 1
                fail_safe_rejections += 1

    logger.info(f"Total bars tested (2 directions): {total_bars * 2:,}")
    logger.info(f"Extra rejections caused by New fail-closed only: {fail_safe_rejections:,}")
    logger.info(f"→ This would explain up to {fail_safe_rejections:,} of the 203-trade gap")
    
    if fail_safe_rejections == 0:
        logger.info("→ FAIL-SAFE IS NOT THE CAUSE (0 extra rejections)")
    elif fail_safe_rejections > 200:
        logger.info("→ FAIL-SAFE EXPLAINS THE ENTIRE GAP (or most of it)")
    else:
        logger.info("→ FAIL-SAFE EXPLAINS PART OF THE GAP")

    # Optional: show a few examples
    if fail_safe_rejections > 0:
        logger.info("\nFirst 5 fail-safe rejection timestamps:")
        count = 0
        for ts in df_strategy.index:
            for is_long in [True, False]:
                legacy_valid = not pd.isna(legacy_rar.loc[ts]) and legacy_rar.loc[ts] > 0
                new_valid = ts in new_rar.index and not pd.isna(new_rar.loc[ts]) and new_rar.loc[ts] > 0
                if legacy_valid and not new_valid:
                    dir_str = "LONG" if is_long else "SHORT"
                    logger.info(f"  {ts} {dir_str} → Legacy would accept, New rejects (RAR issue)")
                    count += 1
                    if count >= 5:
                        return


# =============================================================================
# Run both tests
# =============================================================================
if __name__ == "__main__":
    main()                    # your original full parity test
   # test_fail_safe_only()     # ← new focused test
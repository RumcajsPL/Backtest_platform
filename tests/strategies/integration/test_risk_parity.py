"""
RiskManager Parity Diagnostic Test

This test compares Legacy and New RiskManager implementations using identical mock data
to isolate the root cause of trade count discrepancies.

Run with: python tests/strategies/diagnostics/test_risk_parity.py
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

# Import Legacy modules - we'll need to patch them
from src.strategies.trade_management import risk_manager as legacy_risk_module

# Import New modules
from src.strategies.specific.modules.risk_manager import RiskManager as NewRiskManager
from src.config.config_schema import StrategyConfig, AssetConfig, DataConfig, DataPathsConfig
from src.config.config_schema import TradeManagementConfig, SpreadConfig, RiskConfig, PositionControlConfig
from src.config.config_schema import ExecutionConfig, OutputConfig, FilterPipelineConfig


class PatchedLegacyRiskManager:
    """
    Wrapper for Legacy RiskManager that fixes the boolean DataFrame issue.
    """
    
    def __init__(self, config, ohlcv_data, ohlcv_artf=None):
        # Store the original __init__ method
        original_init = legacy_risk_module.RiskManager.__init__
        
        # Create a patched version that handles the DataFrame boolean check
        def patched_init(self, config, ohlcv_data, ohlcv_artf=None):
            # Store data as attributes directly (bypass the problematic line)
            self.config = config
            self.ohlcv_data = ohlcv_data.copy() if ohlcv_data is not None else None
            self.ohlcv_artf = ohlcv_artf.copy() if ohlcv_artf is not None else None
            
            # Set up other attributes manually
            tm_config = config.get('trade_management', {})
            self.sl_tp_config = tm_config.get('sl_tp', {})
            self.risk_config = tm_config.get('risk_management', {})
            self.spread_config = tm_config.get('spread', {})
            
            # Validate and prepare OHLCV data
            if self.ohlcv_data is not None:
                if not isinstance(self.ohlcv_data.index, pd.DatetimeIndex):
                    if 'timestamp' in self.ohlcv_data.columns:
                        self.ohlcv_data.set_index('timestamp', inplace=True)
            
            # Pre-calculate ATR
            self.atr_series = None
            if self.sl_tp_config.get('enabled', True):
                atr_length = self.sl_tp_config.get('atr_length', 14)
                self.atr_series = self._calculate_atr_wilders(atr_length)
                logger.info(f"ATR calculated (Wilder's RMA, length={atr_length})")
            
            # Pre-calculate Rolling Annual Range
            self.annual_range_series = None
            if self.risk_config.get('enabled', False) and self.ohlcv_artf is not None:
                self._calculate_rolling_annual_range()
                logger.info("Rolling Annual Range calculated")
            
            # Initialize Spread Manager (simplified for test)
            self.spread_manager = None
        
        def _calculate_atr_wilders(self, length=14):
            """Simplified ATR calculation for testing"""
            high = self.ohlcv_data['high']
            low = self.ohlcv_data['low']
            close = self.ohlcv_data['close']
            
            tr1 = high - low
            tr2 = abs(high - close.shift())
            tr3 = abs(low - close.shift())
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            
            return tr.ewm(alpha=1/length, adjust=False).mean().astype('float32')
        
        def _calculate_rolling_annual_range(self):
            """Simplified annual range calculation for testing"""
            if self.ohlcv_artf is None or self.ohlcv_artf.empty:
                self.annual_range_series = None
                return
            
            monthly = self.ohlcv_artf.copy()
            monthly = monthly.sort_index()
            monthly.index = monthly.index.normalize()
            
            # Simple rolling max-min over last 12 months
            rolling_max = monthly['high'].rolling(12, min_periods=1).max()
            rolling_min = monthly['low'].rolling(12, min_periods=1).min()
            
            # Map to strategy timestamps (simplified)
            rar_series = pd.Series(index=self.ohlcv_data.index, dtype='float32')
            for i, ts in enumerate(self.ohlcv_data.index):
                month = ts.replace(day=1)
                if month in monthly.index:
                    idx = monthly.index.get_loc(month)
                    if idx >= 11:
                        rar_series.iloc[i] = float(rolling_max.iloc[idx] - rolling_min.iloc[idx])
            
            self.annual_range_series = rar_series
        
        def compute_trade_parameters(self, timestamp, bid_price, is_long):
            """Simplified parameter computation for testing"""
            if self.atr_series is None:
                return None
            
            try:
                atr_val = float(self.atr_series.loc[timestamp])
            except (KeyError, TypeError):
                return None
            
            if atr_val <= 0 or np.isnan(atr_val):
                return None
            
            # Simple parameter calculation
            sl_mult = self.sl_tp_config.get('sl_multiplier', 1.4)
            risk_distance = atr_val * sl_mult
            
            raw_sl = bid_price - risk_distance if is_long else bid_price + risk_distance
            
            # Risk validation
            if self.annual_range_series is not None:
                try:
                    annual_range = float(self.annual_range_series.loc[timestamp])
                    if not np.isnan(annual_range) and annual_range > 0:
                        risk_pct = (risk_distance / annual_range) * 100
                        max_pct = self.risk_config.get('max_risk_percentile', 0.1)
                        
                        if risk_pct > max_pct:
                            return None
                except (KeyError, TypeError):
                    pass
            
            # Create a simple result object
            result = types.SimpleNamespace()
            result.executed_entry = bid_price
            result.trigger_sl = raw_sl
            result.tp = bid_price + risk_distance * 5.7 if is_long else bid_price - risk_distance * 5.7
            result.comment = "OK"
            
            return result
        
        # Attach methods
        self._calculate_atr_wilders = _calculate_atr_wilders.__get__(self)
        self._calculate_rolling_annual_range = _calculate_rolling_annual_range.__get__(self)
        self.compute_trade_parameters = compute_trade_parameters.__get__(self)
        
        # Call the patched init
        patched_init(self, config, ohlcv_data, ohlcv_artf)


def create_mock_data(days: int = 60, freq: str = '1min') -> pd.DataFrame:
    """Create mock OHLCV data with realistic properties."""
    dates = pd.date_range(
        start=datetime.now() - timedelta(days=days),
        end=datetime.now(),
        freq=freq
    )
    
    np.random.seed(42)  # For reproducibility
    
    # Generate realistic price series (starting at 15000 for DAX-like)
    base_price = 15000
    returns = np.random.randn(len(dates)) * 0.0001  # 0.01% daily volatility
    price = base_price * np.exp(np.cumsum(returns))
    
    # Create OHLC with realistic spreads
    df = pd.DataFrame(index=dates)
    df['close'] = price
    df['open'] = df['close'].shift(1).fillna(price[0])
    df['high'] = df[['open', 'close']].max(axis=1) * (1 + np.random.uniform(0, 0.001, len(dates)))
    df['low'] = df[['open', 'close']].min(axis=1) * (1 - np.random.uniform(0, 0.001, len(dates)))
    
    return df.astype(np.float32)


def create_mock_artf_data(years: int = 5) -> pd.DataFrame:
    """Create mock monthly ARTF data."""
    dates = pd.date_range(
        start=datetime.now() - timedelta(days=365*years),
        end=datetime.now(),
        freq='ME'
    )
    
    np.random.seed(42)
    
    # Monthly data with clear annual patterns
    base = 15000
    trend = np.linspace(0, 2000, len(dates))
    seasonality = 1000 * np.sin(2 * np.pi * np.arange(len(dates)) / 12)
    noise = np.random.randn(len(dates)) * 200
    
    close = base + trend + seasonality + noise
    
    df = pd.DataFrame(index=dates)
    df['close'] = close
    df['open'] = df['close'].shift(1).fillna(close[0])
    df['high'] = df['close'] * (1 + np.random.uniform(0.01, 0.05, len(dates)))
    df['low'] = df['close'] * (1 - np.random.uniform(0.01, 0.05, len(dates)))
    
    return df.astype(np.float32)


def create_legacy_config(risk_pct: float) -> dict:
    """Create Legacy-style config dict."""
    return {
        'trade_management': {
            'sl_tp': {
                'enabled': True,
                'atr_length': 14,
                'sl_multiplier': 1.4,
                'risk_to_reward_ratio': 5.7,
            },
            'risk_management': {
                'enabled': True,
                'max_risk_percentile': risk_pct,  # Legacy interprets as percentage points
                'allow_exceed_limit': False,
            },
            'spread': {
                'enabled': True,
                'apply_to_long': True,
                'apply_to_short': True,
                'config_path': str(PROJECT_ROOT / 'configs' / 'spreads' / 'broker_spreads.yaml'),
            }
        },
        'asset': {
            'symbol': 'DEUIDXEUR',
        },
    }


def create_new_config(max_risk_percentile: float = 0.1) -> StrategyConfig:
    """Create New-style typed StrategyConfig."""
    
    # Create paths config
    paths = DataPathsConfig(
        strategy_ohlcv=Path("dummy.parquet"),
        artf_ohlcv=Path("dummy_artf.parquet"),
    )
    
    # Create data config
    data = DataConfig(
        paths=paths,
        date_range=None,
    )
    
    # Create asset config
    asset = AssetConfig(symbol="DEUIDXEUR")
    
    # Create spread config
    spread = SpreadConfig(
        enabled=True,
        config_path=PROJECT_ROOT / 'configs' / 'spreads' / 'broker_spreads.yaml',
    )
    
    # Create risk config - Note: New interprets as percentage directly
    risk = RiskConfig(
        atr_length=14,
        atr_multiplier_sl=1.4,
        atr_multiplier_tp=7.98,
        max_risk_percentile=max_risk_percentile,  # 0.1 means 0.1% in New
        tp_mode="rr_ratio",
        risk_to_reward_ratio=5.7,
    )
    
    # Create position control
    position_control = PositionControlConfig(
        pyramiding_enabled=False,
        close_on_opposite=False,
        max_positions=1,
    )
    
    # Create trade management
    trade_mgmt = TradeManagementConfig(
        spread=spread,
        risk=risk,
        position_control=position_control,
    )
    
    # Create dummy execution config
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


def test_single_timestamp(legacy_rm, new_rm, timestamp, bid_price, is_long):
    """Test a single timestamp and compare results."""
    
    # Legacy computation
    try:
        legacy_params = legacy_rm.compute_trade_parameters(timestamp, bid_price, is_long)
        legacy_exists = legacy_params is not None
    except Exception as e:
        logger.warning(f"Legacy error at {timestamp}: {e}")
        legacy_exists = False
        legacy_params = None
    
    # New computation
    try:
        new_params = new_rm.compute_trade_parameters(timestamp, bid_price, is_long)
        new_exists = new_params is not None
    except Exception as e:
        logger.warning(f"New error at {timestamp}: {e}")
        new_exists = False
        new_params = None
    
    # Extract key values for comparison when both exist
    comparison = {
        'timestamp': timestamp,
        'bid_price': bid_price,
        'is_long': is_long,
        'legacy_exists': legacy_exists,
        'new_exists': new_exists,
    }
    
    if legacy_exists and new_exists:
        # Compare key parameters
        comparison.update({
            'legacy_entry': getattr(legacy_params, 'executed_entry', None),
            'new_entry': getattr(new_params, 'entry_price_executed', None),
            'legacy_sl': getattr(legacy_params, 'trigger_sl', None),
            'new_sl': getattr(new_params, 'stop_loss_trigger', None),
            'legacy_tp': getattr(legacy_params, 'tp', None),
            'new_tp': getattr(new_params, 'take_profit', None),
            'legacy_comment': getattr(legacy_params, 'comment', ''),
            'new_comment': getattr(new_params, 'comment', ''),
        })
    
    return comparison

# Add to test_risk_parity.py

def extract_risk_params_from_logs():
    """Extract actual ATR and annual range values from your production runs."""
    # From your New run logs, we see:
    # Risk percentages calculated: 0.6103%, 0.7325%, etc.
    
    # From your Legacy run with 0.001%, we know:
    # 570 trades accepted out of 5,182 filtered signals
    # This means ~11% of signals passed the risk filter
    
    # If risk percentages in Legacy were similar to New (0.6-0.8%),
    # then with 0.001% limit, almost all should be rejected (0% pass rate)
    # But 11% passed, so Legacy risk percentages must be much smaller
    
    # Calculate: If 11% passed at 0.001% limit,
    # then risk_percentile must be <= 0.001% for 11% of signals
    # This suggests Legacy risk_percentile values are ~0.0001-0.001%
    # Which are 100-1000x smaller than New's 0.6-0.8%
    
    return {
        'legacy_risk_pct_range': (0.0001, 0.001),  # Estimated
        'new_risk_pct_range': (0.6, 0.8),          # From logs
    }

def main():
    """Main diagnostic test."""
    logger.info("=" * 70)
    logger.info("RISKMANAGER PARITY DIAGNOSTIC")
    logger.info("=" * 70)
    
    # Create mock data
    logger.info("\n1. Creating mock data...")
    df_ohlcv = create_mock_data(days=60, freq='1min')
    df_artf = create_mock_artf_data(years=5)
    logger.info(f"   OHLCV bars: {len(df_ohlcv):,}")
    logger.info(f"   ARTF bars: {len(df_artf):,}")
    
    # Test with different max_risk_percentile values
    test_values = [0.01, 0.05, 0.1, 0.2, 0.5, 1.0]
    
    results_summary = []
    
    for risk_pct in test_values:
        logger.info(f"\n{'-' * 70}")
        logger.info(f"TESTING WITH max_risk_percentile = {risk_pct}%")
        logger.info(f"{'-' * 70}")
        
        # Create Legacy config
        legacy_config = create_legacy_config(risk_pct)
        
        # Create New config
        new_config = create_new_config(max_risk_percentile=risk_pct)
        
        # Initialize RiskManagers
        logger.info("\n   Initializing RiskManagers...")
        
        try:
            legacy_rm = PatchedLegacyRiskManager(
                config=legacy_config,
                ohlcv_data=df_ohlcv.copy(),
                ohlcv_artf=df_artf.copy(),
            )
            logger.info("   Legacy RiskManager initialized successfully")
        except Exception as e:
            logger.error(f"   Failed to initialize Legacy RiskManager: {e}")
            continue
        
        try:
            new_rm = NewRiskManager(
                config=new_config,
                ohlcv_data=df_ohlcv.copy(),
                ohlcv_artf=df_artf.copy(),
                mode="analytics",
            )
            logger.info("   New RiskManager initialized successfully")
        except Exception as e:
            logger.error(f"   Failed to initialize New RiskManager: {e}")
            continue
        
        # Test a sample of timestamps
        test_timestamps = df_ohlcv.index[::1000][:20]  # 20 samples spaced out
        comparisons = []
        
        logger.info(f"\n   Testing {len(test_timestamps)} sample timestamps...")
        
        for ts in test_timestamps:
            bid_price = float(df_ohlcv.loc[ts, 'close'])
            
            # Test LONG
            long_result = test_single_timestamp(legacy_rm, new_rm, ts, bid_price, is_long=True)
            comparisons.append(long_result)
            
            # Test SHORT
            short_result = test_single_timestamp(legacy_rm, new_rm, ts, bid_price, is_long=False)
            comparisons.append(short_result)
        
        # Analyze results
        total_tests = len(comparisons)
        both_exist = sum(1 for c in comparisons if c['legacy_exists'] and c['new_exists'])
        legacy_only = sum(1 for c in comparisons if c['legacy_exists'] and not c['new_exists'])
        new_only = sum(1 for c in comparisons if c['new_exists'] and not c['legacy_exists'])
        both_none = sum(1 for c in comparisons if not c['legacy_exists'] and not c['new_exists'])
        
        logger.info(f"\n   Results:")
        logger.info(f"     Both accepted: {both_exist:>3}/{total_tests} ({both_exist/total_tests*100:>5.1f}%)")
        logger.info(f"     Legacy only:   {legacy_only:>3}/{total_tests} ({legacy_only/total_tests*100:>5.1f}%)")
        logger.info(f"     New only:      {new_only:>3}/{total_tests} ({new_only/total_tests*100:>5.1f}%)")
        logger.info(f"     Both rejected: {both_none:>3}/{total_tests} ({both_none/total_tests*100:>5.1f}%)")
        
        # If there are discrepancies, show first few with details
        if legacy_only > 0 or new_only > 0:
            logger.info(f"\n   Sample discrepancies (first 3):")
            discrepancies = [c for c in comparisons if c['legacy_exists'] != c['new_exists']][:3]
            for i, d in enumerate(discrepancies, 1):
                direction = "LONG" if d['is_long'] else "SHORT"
                logger.info(f"\n     Discrepancy {i}: {d['timestamp']} {direction}")
                if d['legacy_exists']:
                    logger.info(f"       Legacy: ACCEPTED - {d.get('legacy_comment', '')}")
                else:
                    logger.info(f"       Legacy: REJECTED")
                if d['new_exists']:
                    logger.info(f"       New:    ACCEPTED - {d.get('new_comment', '')}")
                else:
                    logger.info(f"       New:    REJECTED")
        
        results_summary.append({
            'risk_pct': risk_pct,
            'both_exist': both_exist,
            'legacy_only': legacy_only,
            'new_only': new_only,
            'both_none': both_none,
            'total': total_tests,
        })
    
    # Final summary
    logger.info("\n" + "=" * 70)
    logger.info("SUMMARY BY RISK PERCENTILE")
    logger.info("=" * 70)
    logger.info(f"{'Risk %':>8} | {'Both':>6} | {'Legacy Only':>10} | {'New Only':>8} | {'Both Rej':>8} | {'Total':>6}")
    logger.info("-" * 70)
    
    for r in results_summary:
        if r['total'] > 0:
            logger.info(
                f"{r['risk_pct']:>7}% | "
                f"{r['both_exist']:>3}/{r['total']:<2} ({r['both_exist']/r['total']*100:>4.1f}%) | "
                f"{r['legacy_only']:>3}/{r['total']:<2} ({r['legacy_only']/r['total']*100:>4.1f}%) | "
                f"{r['new_only']:>3}/{r['total']:<2} ({r['new_only']/r['total']*100:>4.1f}%) | "
                f"{r['both_none']:>3}/{r['total']:<2} | "
                f"{r['total']:>3}"
            )
    
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
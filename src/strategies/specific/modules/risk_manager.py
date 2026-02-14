"""Risk management: SL/TP with R:R ratio, spread adjustments, annual range validation
MIGRATED: Session 7 - Returns TradeParameters contract instead of dict
Location: src/strategies/specific/modules/risk_manager.py
"""
import pandas as pd
import numpy as np
import logging
from typing import Tuple, Optional, Dict, Any

# NEW: Import TradeParameters contract from contracts directory
from src.strategies.contracts.trade_contracts import TradeParameters

# Import SpreadManager (will be in same modules directory)
from src.strategies.specific.modules.spread_manager import SpreadManager

logger = logging.getLogger(__name__)

class RiskManager:
    """Manages SL/TP calculations with R:R ratio and risk validation using rolling annual range
    
    MIGRATION NOTE (Session 7):
    - compute_trade_parameters() now returns TradeParameters contract
    - All calculation logic unchanged (exact parity maintained)
    - Added additional fields for complete contract population
    """
    
    def __init__(self, config: Dict[str, Any], ohlcv_data: pd.DataFrame, ohlcv_artf: Optional[pd.DataFrame] = None):
        """
        Initialize RiskManager with configuration and data.       
        """
        self.config = config
        tm_config = config.get('trade_management', {})
        self.sl_tp_config = tm_config.get('sl_tp', {})
        self.risk_config = tm_config.get('risk_management', {})
        self.spread_config = tm_config.get('spread', {})
        
        # Validate and prepare OHLCV data
        self.ohlcv_data = ohlcv_data.copy()
        if not isinstance(self.ohlcv_data.index, pd.DatetimeIndex):
            if 'timestamp' in self.ohlcv_data.columns:
                self.ohlcv_data.set_index('timestamp', inplace=True)
            else:
                raise ValueError("RiskManager requires OHLCV data with DatetimeIndex")
        
        # Monthly ARTF data (prefer explicit arg, fallback to config injection)
        self.ohlcv_artf = ohlcv_artf or self.config.get("data", {}).get("df_artf")
        
        # Pre-calculate ATR (Wilder's Smoothing)
        self.atr_series = None
        if self.sl_tp_config.get('enabled', True):
            atr_length = self.sl_tp_config.get('atr_length', 14)
            self.atr_series = self._calculate_atr_wilders(atr_length)
            logger.info(f"ATR calculated (Wilder's RMA, length={atr_length})")
        
        # Pre-calculate Rolling Annual Range (now ARTF-based, 12-month lookback)
        self.annual_range_series = None
        if self.risk_config.get('enabled', False):
            self._calculate_rolling_annual_range()
            logger.info("Rolling Annual Range calculated (12-month ARTF, year-month based)")
        
        # Initialize Spread Manager if enabled
        self.spread_manager = None
        if self.spread_config.get('enabled', False):
            asset_symbol = self.config.get('asset', {}).get('symbol', '')
            config_path = self.spread_config.get('config_path')
            self.spread_manager = SpreadManager(asset_symbol, config_path)
            logger.info(f"SpreadManager initialized for {asset_symbol}")

    def _calculate_atr_wilders(self, length: int = 14) -> pd.Series:
        """Calculate ATR using Wilder's Smoothing (RMA) - matches TradingView"""
        high = self.ohlcv_data['high']
        low = self.ohlcv_data['low']
        close = self.ohlcv_data['close']
        
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        atr = tr.ewm(alpha=1/length, adjust=False).mean()
        
        # Convert to float32 for memory efficiency
        return atr.astype('float32')
    
    # -------------------------------------------------------------------------
    # FAST ARTF-BASED RAR (12-month, year-month based, no lookahead)
    # -------------------------------------------------------------------------
    def _calculate_rolling_annual_range(self):
        """
        Fast 12‑month RAR using monthly ARTF bars.
        Precompute RAR per month, then map to strategy timestamps.
        """
        if self.ohlcv_artf is None or self.ohlcv_artf.empty:
            logger.warning("Monthly ARTF data missing — annual range disabled")
            self.annual_range_series = None
            return

        monthly = self.ohlcv_artf.copy()
        if not isinstance(monthly.index, pd.DatetimeIndex):
            raise ValueError("ARTF monthly data must have DatetimeIndex")

        monthly = monthly.sort_index()
        monthly.index = monthly.index.normalize()

        # Year‑month key
        monthly["ym"] = monthly.index.to_period("M")
        monthly_by_ym = monthly.set_index("ym")[["high", "low"]]

        # Compute RAR per month (vectorized over months, not strategy bars)
        yms = monthly_by_ym.index.unique().sort_values()
        rar_per_month: Dict[pd.Period, float] = {}

        for ym in yms:
            prev_ym = ym - 1
            start_ym = prev_ym - 11
            window = monthly_by_ym.loc[start_ym:prev_ym]

            if len(window) == 0:
                rar_per_month[ym] = np.nan
            else:
                rar_per_month[ym] = float(window["high"].max() - window["low"].min())

        rar_monthly_series = pd.Series(rar_per_month, dtype="float32")

        # Map each strategy timestamp to RAR of previous month
        strategy_ym = self.ohlcv_data.index.to_period("M")
        strategy_prev_ym = strategy_ym - 1
        rar_strategy = strategy_prev_ym.map(rar_monthly_series)

        self.annual_range_series = pd.Series(
            rar_strategy.values,
            index=self.ohlcv_data.index,
            dtype="float32",
        )

    def compute_trade_parameters(self, 
                                 timestamp: pd.Timestamp,
                                 bid_price: float,
                                 is_long: bool) -> Optional[TradeParameters]:
        """
        Compute all trade parameters including spread adjustments, SL/TP, and risk validation.
        
        MIGRATED (Session 7): Now returns TradeParameters contract instead of dict.
        """
        if not self.sl_tp_config.get('enabled', True):
            return None
        
        # Get ATR value
        try:
            atr_val = self.atr_series.loc[timestamp]
        except KeyError:
            logger.warning(f"ATR not available for {timestamp}")
            return None
        
        if atr_val <= 0 or np.isnan(atr_val):
            return None
        
        # Get spread if enabled
        spread = 0.0
        spread_type = None
        spread_value_config = None
        if self.spread_manager:
            spread = self.spread_manager.get_spread_in_points(bid_price)
            if self.spread_manager.asset_config:
                spread_type = self.spread_manager.asset_config.get('spread_type')
                spread_value_config = self.spread_manager.asset_config.get('spread_value')
        
        # Check if spread applies to this trade direction
        apply_spread = False
        if self.spread_config.get('enabled', False):
            if (is_long and self.spread_config.get('apply_to_long', True)) or \
               (not is_long and self.spread_config.get('apply_to_short', True)):
                apply_spread = True
        
        spread_for_this = spread if apply_spread else 0.0
        
        # Calculate executed entry price
        executed_entry = bid_price + spread_for_this if is_long else bid_price
        
        # Get multipliers from config
        sl_mult = self.sl_tp_config.get('sl_multiplier', 1.4)
        rr_ratio = self.sl_tp_config.get('risk_to_reward_ratio', 2.0)
        
        # Calculate risk distance and raw SL
        risk_distance = atr_val * sl_mult
        raw_sl = executed_entry - risk_distance if is_long else executed_entry + risk_distance
        
        # Validate risk percentile (may adjust SL)
        is_valid, adjusted_sl, comment = self.validate_risk_percentile(
            executed_entry, raw_sl, is_long, timestamp
        )
        
        if not is_valid:
            return None
        
        # Apply adjustment if any
        sl_adjusted = (adjusted_sl != raw_sl)
        sl_price_raw = raw_sl if not sl_adjusted else None
        sl_distance_raw = risk_distance if not sl_adjusted else None
        final_sl = adjusted_sl
        risk_distance = abs(executed_entry - final_sl)
        
        # Calculate TP based on R:R ratio
        tp = executed_entry + (risk_distance * rr_ratio) if is_long else executed_entry - (risk_distance * rr_ratio)
        
        # Calculate trigger SL (chart price that triggers exit)
        trigger_sl = final_sl - spread_for_this if is_long else final_sl + spread_for_this
        
        # Get annual range data (if risk management enabled)
        annual_range_value = None
        risk_percentile_calculated = None
        max_risk_percentile = None
        risk_percentile_passed = True
        
        if self.risk_config.get('enabled', False) and self.annual_range_series is not None:
            try:
                annual_range_value = float(self.annual_range_series.loc[timestamp])
                if not np.isnan(annual_range_value) and annual_range_value > 0:
                    risk_percentile_calculated = risk_distance / annual_range_value
                    max_risk_percentile = self.risk_config.get('max_risk_percentile', 1.0)
                    risk_percentile_passed = (risk_percentile_calculated <= max_risk_percentile)
            except (KeyError, ValueError):
                pass
        
        # Calculate spread efficiency (if applicable)
        spread_efficiency_percent = None
        if apply_spread and spread > 0:
            spread_cost = spread
            spread_efficiency_percent = (spread_cost / executed_entry) * 100
        
        return TradeParameters(
            # Core execution prices
            entry_price_mid=bid_price,
            entry_price_executed=executed_entry,
            stop_loss_raw=final_sl,
            stop_loss_trigger=trigger_sl,
            take_profit=tp,
            position_size=1.0,
            
            # Risk metrics
            atr_value=float(atr_val),
            atr_length=self.sl_tp_config.get('atr_length', 14),
            atr_multiplier=sl_mult,
            sl_distance=risk_distance,
            tp_distance=abs(tp - executed_entry),
            risk_reward_ratio=rr_ratio,
            
            # Annual range validation
            annual_range_value=annual_range_value,
            risk_percentile_calculated=risk_percentile_calculated,
            max_risk_percentile=max_risk_percentile,
            risk_percentile_passed=risk_percentile_passed,
            
            # Spread details
            spread_enabled=self.spread_config.get('enabled', False),
            spread_applied=apply_spread,
            spread_type=spread_type,
            spread_value=spread_value_config,
            spread_points=spread_for_this,
            spread_cost=spread if apply_spread else None,
            spread_efficiency_percent=spread_efficiency_percent,
            
            # Adjustments
            sl_adjusted=sl_adjusted,
            sl_distance_raw=sl_distance_raw,
            sl_price_raw=sl_price_raw,
            
            # Metadata
            comment=comment,
        )

    def validate_risk_percentile(self,
                                 entry_price: float,
                                 stop_loss: float,
                                 is_long: bool,
                                 timestamp: pd.Timestamp) -> Tuple[bool, float, str]:
        """
        Validate stop loss against max risk percentile of annual range.
        
        Returns:
            Tuple of (is_valid, adjusted_sl, comment)
        """
        # Early return if risk management disabled
        if not self.risk_config.get('enabled', False):
            return True, stop_loss, "Risk mgmt disabled"
        
        # If RAR not initialized, behave as "no limit"
        if self.annual_range_series is None:
            return True, stop_loss, "RAR not initialized"
        
        try:
            current_annual_range = self.annual_range_series.loc[timestamp]
        except KeyError:
            return True, stop_loss, "RAR missing for timestamp"
        
        # If RAR invalid, do not block trade
        if pd.isna(current_annual_range) or current_annual_range <= 0:
            return True, stop_loss, f"RAR unavailable or invalid ({current_annual_range})"
        
        max_percentile = self.risk_config.get('max_risk_percentile', 1.0)
        
        if max_percentile >= 1.0:
            return True, stop_loss, "No risk limit"
        
        risk_distance = abs(entry_price - stop_loss)
        risk_percentile = risk_distance / current_annual_range
        
        if risk_percentile <= max_percentile:
            return True, stop_loss, f"Risk: {risk_percentile*100:.2f}%"
        
        allow_exceed = self.risk_config.get('allow_exceed_limit', False)
        if not allow_exceed:
            return False, stop_loss, f"Risk Rejected: {risk_percentile*100:.2f}% > {max_percentile*100:.2f}%"
        
        adjusted_distance = max_percentile * current_annual_range
        adjusted_sl = entry_price - adjusted_distance if is_long else entry_price + adjusted_distance
        return True, adjusted_sl, f"SL Adjusted: {risk_percentile*100:.2f}% -> {max_percentile*100:.2f}%"

    # ========================================================================
    # LEGACY COMPATIBILITY METHOD (for gradual migration)
    # ========================================================================
    def compute_trade_parameters_legacy(self, 
                                       timestamp: pd.Timestamp,
                                       bid_price: float,
                                       is_long: bool) -> Optional[Dict]:
        """
        Legacy method that returns dict instead of TradeParameters.
        
        DEPRECATED: Use compute_trade_parameters() which returns TradeParameters contract.
        This method is kept for backward compatibility during migration.
        """
        params = self.compute_trade_parameters(timestamp, bid_price, is_long)
        if params is None:
            return None
        
        return {
            'executed_entry': params.entry_price_executed,
            'raw_sl': params.stop_loss_raw,
            'trigger_sl': params.stop_loss_trigger,
            'tp': params.take_profit,
            'comment': params.comment or '',
            'sl_adjusted': params.sl_adjusted,
            'spread_applied': params.spread_applied,
            'spread_value': params.spread_points,
        }
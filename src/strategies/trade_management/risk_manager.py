"""Risk management: SL/TP with R:R ratio, spread adjustments, annual range validation"""
import pandas as pd
import numpy as np
import logging
from typing import Tuple, Optional, Dict, Any
from .spread_manager import SpreadManager

logger = logging.getLogger(__name__)

class RiskManager:
    """Manages SL/TP calculations with R:R ratio and risk validation using rolling annual range"""
    
    def __init__(self, config: Dict[str, Any], ohlcv_data: pd.DataFrame):
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
        
        # Pre-calculate ATR (Wilder's Smoothing)
        self.atr_series = None
        if self.sl_tp_config.get('enabled', True):
            atr_length = self.sl_tp_config.get('atr_length', 14)
            self.atr_series = self._calculate_atr_wilders(atr_length)
            logger.info(f"ATR calculated (Wilder's RMA, length={atr_length})")
        
        # Pre-calculate Rolling Annual Range
        self.annual_range_series = None
        if self.risk_config.get('enabled', False):
            self._calculate_rolling_annual_range()
            logger.info("Rolling Annual Range calculated (252-day lookback)")
        
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
    
    def _calculate_rolling_annual_range(self):
        """Calculate 1-year range based on past data only (no lookahead bias)"""
        if self.ohlcv_data.empty:
            return

        daily_df = self.ohlcv_data.resample('D').agg({
            'high': 'max', 
            'low': 'min'
        }).dropna()
        
        rolling_high = daily_df['high'].rolling(window=252, min_periods=20).max().shift(1)
        rolling_low = daily_df['low'].rolling(window=252, min_periods=20).min().shift(1)
        daily_range = rolling_high - rolling_low
        
        self.annual_range_series = daily_range.reindex(self.ohlcv_data.index, method='ffill')
        
        # Convert to float32 for memory efficiency
        self.annual_range_series = self.annual_range_series.astype('float32')

    def compute_trade_parameters(self, 
                                 timestamp: pd.Timestamp,
                                 bid_price: float,
                                 is_long: bool) -> Optional[Dict]:
        """
        Compute all trade parameters including spread adjustments, SL/TP, and risk validation.        
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
        if self.spread_manager:
            spread = self.spread_manager.get_spread_in_points(bid_price)
        
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
        raw_sl = adjusted_sl if sl_adjusted else raw_sl
        risk_distance = abs(executed_entry - raw_sl)
        
        # Calculate TP based on R:R ratio
        tp = executed_entry + (risk_distance * rr_ratio) if is_long else executed_entry - (risk_distance * rr_ratio)
        
        # Calculate trigger SL (chart price that triggers exit)
        trigger_sl = raw_sl - spread_for_this if is_long else raw_sl + spread_for_this
        
        return {
            'executed_entry': executed_entry,
            'raw_sl': raw_sl,
            'trigger_sl': trigger_sl,
            'tp': tp,
            'comment': comment,
            'sl_adjusted': sl_adjusted,
            'spread_applied': apply_spread,
            'spread_value': spread_for_this
        }

    def validate_risk_percentile(self,
                                 entry_price: float,
                                 stop_loss: float,
                                 is_long: bool,
                                 timestamp: pd.Timestamp) -> Tuple[bool, float, str]:
        """
        Validate stop loss against max risk percentile of annual range.        
        """
        # Early return if risk management disabled
        if not self.risk_config.get('enabled', False):
            return True, stop_loss, "Risk mgmt disabled"
        
        # Fail-fast: annual range must be initialized
        if self.annual_range_series is None:
            raise RuntimeError("Annual range not initialized - check RiskManager setup")
        
        # Fail-fast: timestamp must exist in range data
        try:
            current_annual_range = self.annual_range_series.loc[timestamp]
        except KeyError:
            raise ValueError(f"Annual range data missing for {timestamp}")
        
        # Fail-fast: range value must be valid
        if pd.isna(current_annual_range) or current_annual_range <= 0:
            raise ValueError(f"Invalid annual range at {timestamp}: {current_annual_range}")
        
        # Get risk limit from config
        max_percentile = self.risk_config.get('max_risk_percentile', 1.0)
        
        # No limit if >= 1.0
        if max_percentile >= 1.0:
            return True, stop_loss, "No risk limit"
        
        # Calculate risk percentile
        risk_distance = abs(entry_price - stop_loss)
        risk_percentile = risk_distance / current_annual_range
        
        # Within limit - approve
        if risk_percentile <= max_percentile:
            return True, stop_loss, f"Risk: {risk_percentile*100:.2f}%"
        
        # Exceeds limit - check if adjustment allowed
        allow_exceed = self.risk_config.get('allow_exceed_limit', False)
        if not allow_exceed:
            return False, stop_loss, f"Risk Rejected: {risk_percentile*100:.2f}% > {max_percentile*100:.2f}%"
        
        # Adjust SL to meet limit
        adjusted_distance = max_percentile * current_annual_range
        adjusted_sl = entry_price - adjusted_distance if is_long else entry_price + adjusted_distance
        return True, adjusted_sl, f"SL Adjusted: {risk_percentile*100:.2f}% -> {max_percentile*100:.2f}%"
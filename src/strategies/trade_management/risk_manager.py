# Updated: src/strategies/trade_management/risk_manager.py
"""
Risk Management Module for Backtesting Platform.
Handles SL/TP calculation with R:R ratio and risk validation.
Integrates SpreadManager for bid-based adjustments.
"""

import pandas as pd
import numpy as np
import logging
from typing import Tuple, Optional, Dict, Any
from .spread_manager import SpreadManager

logger = logging.getLogger(__name__)

class RiskManager:
    """
    Manages risk calculations including SL/TP with R:R ratio.
    Implements 'Rolling Annual Range' to prevent lookahead bias.
    Integrates spread adjustments assuming OHLCV is Bid-based.
    """
    
    def __init__(self, config: Dict[str, Any], ohlcv_data: pd.DataFrame):
        """
        Initialize RiskManager with configuration and data.
        Pre-calculates indicators to ensure vectorization speed.
        
        Args:
            config: Dictionary containing risk management configuration
            ohlcv_data: OHLCV DataFrame (must have DatetimeIndex)
        """
        self.config = config
        # If 'trade_management' is missing, default to empty dict to avoid crash, 
        # but internal defaults will then take over.
        tm_config = config.get('trade_management', {})
        # Now access the sub-sections from tm_config, NOT self.config
        self.sl_tp_config = tm_config.get('sl_tp', {}) 
        self.risk_config = tm_config.get('risk_management', {})
        self.spread_config = tm_config.get('spread', {})
                
        # Ensure we work with a copy and valid index
        self.ohlcv_data = ohlcv_data.copy()
        if not isinstance(self.ohlcv_data.index, pd.DatetimeIndex):
            if 'timestamp' in self.ohlcv_data.columns:
                self.ohlcv_data.set_index('timestamp', inplace=True)
            else:
                raise ValueError("RiskManager requires OHLCV data with DatetimeIndex")
        
        # --- 1. Pre-calculate ATR (Wilder's Smoothing) ---
        self.atr_series = None
        if self.sl_tp_config.get('enabled', True):
            atr_length = self.sl_tp_config.get('atr_length', 14)
            self.atr_series = self._calculate_atr_wilders(atr_length)
            logger.info(f"ATR calculated (Wilder's RMA) with length={atr_length}")
        
        # --- 2. Pre-calculate Rolling Annual Range ---
        self.annual_range_series = None
        if self.risk_config.get('enabled', False):
            self._calculate_rolling_annual_range()
            logger.info("Rolling Annual Range calculated (Resampled Daily -> 252 Period)")
        
        # --- 3. Initialize Spread Manager if enabled ---
        self.spread_manager = None
        if self.spread_config.get('enabled', False):
            asset_symbol = self.config.get('asset', {}).get('symbol', '')
            config_path = self.spread_config.get('config_path')
            self.spread_manager = SpreadManager(asset_symbol, config_path)
            logger.info(f"SpreadManager initialized for {asset_symbol}")

    def _calculate_atr_wilders(self, length: int = 14) -> pd.Series:
        """
        Calculate ATR using Wilder's Smoothing (RMA) to match TradingView.
        """
        high = self.ohlcv_data['high']
        low = self.ohlcv_data['low']
        close = self.ohlcv_data['close']
        
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        atr = tr.ewm(alpha=1/length, adjust=False).mean()
        
        return atr
    
    def _calculate_rolling_annual_range(self):
        """
        Calculate 1-Year Range based on Past Data only (No Lookahead).
        """
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

    def compute_trade_parameters(self, 
                                 timestamp: pd.Timestamp,
                                 bid_price: float,
                                 is_long: bool) -> Optional[Dict]:
        """
        Compute all trade parameters including spread adjustments, SL/TP, and risk validation.
        
        Args:
            timestamp: Current candle timestamp
            bid_price: Current close price (Bid)
            is_long: Trade direction
            
        Returns:
            Dict with trade parameters or None if invalid/rejected
        """
        if not self.sl_tp_config.get('enabled', True):
            return None
        
        # Get ATR
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
        
        # Calculate executed entry
        executed_entry = bid_price + spread_for_this if is_long else bid_price
        
        # Get multipliers
        sl_mult = self.sl_tp_config.get('sl_multiplier', 1.4)
        rr_ratio = self.sl_tp_config.get('risk_to_reward_ratio', 2.0)
        
        # Calculate risk distance and raw SL
        risk_distance = atr_val * sl_mult
        raw_sl = executed_entry - risk_distance if is_long else executed_entry + risk_distance
        
        # Validate risk percentile (using raw_sl)
        is_valid, adjusted_sl, comment = self.validate_risk_percentile(
            executed_entry, raw_sl, is_long, timestamp
        )
        
        if not is_valid:
            return None
        
        # Apply adjustment if any
        sl_adjusted = (adjusted_sl != raw_sl)
        raw_sl = adjusted_sl if sl_adjusted else raw_sl
        risk_distance = abs(executed_entry - raw_sl)
        
        # Calculate TP
        tp = executed_entry + (risk_distance * rr_ratio) if is_long else executed_entry - (risk_distance * rr_ratio)
        
        # Calculate trigger SL
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
        Validate stop loss against MAX risk percentile of the Annual Range.
        Uses raw SL for risk calculation.
        """
        if not self.risk_config.get('enabled', False):
            return True, stop_loss, "Risk mgmt disabled"

        if self.annual_range_series is None:
            return True, stop_loss, "Annual range data missing"
            
        try:
            current_annual_range = self.annual_range_series.loc[timestamp]
        except KeyError:
            return True, stop_loss, "Range data unavailable for date"

        if pd.isna(current_annual_range) or current_annual_range <= 0:
            return True, stop_loss, "History insufficient for range calc"

        max_percentile = self.risk_config.get('max_risk_percentile', 1.0)
        allow_exceed = self.risk_config.get('allow_exceed_limit', False)
        
        risk_distance = abs(entry_price - stop_loss)
        risk_percentile = risk_distance / current_annual_range
        
        if max_percentile >= 1.0 or risk_percentile <= max_percentile:
            return True, stop_loss, f"Risk: {risk_percentile*100:.2f}% (Limit: {max_percentile*100:.2f}%)"
            
        if allow_exceed:
            adjusted_distance = max_percentile * current_annual_range
            if is_long:
                adjusted_sl = entry_price - adjusted_distance
            else:
                adjusted_sl = entry_price + adjusted_distance
            
            comment = f"SL Adjusted: {risk_percentile*100:.2f}% -> {max_percentile*100:.2f}%"
            return True, adjusted_sl, comment
        else:
            comment = f"Risk Rejected: {risk_percentile*100:.2f}% > {max_percentile*100:.2f}%"
            return False, stop_loss, comment
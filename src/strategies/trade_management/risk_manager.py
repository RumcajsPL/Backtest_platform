"""
Risk Management Module for Backtesting Platform.
Handles SL/TP calculation with R:R ratio and risk validation.
"""

import pandas as pd
import numpy as np
import logging
from typing import Tuple, Optional, Dict, Any

logger = logging.getLogger(__name__)

class RiskManager:
    """
    Manages risk calculations including SL/TP with R:R ratio.
    """
    
    def __init__(self, config: Dict[str, Any], ohlcv_data: pd.DataFrame):
        """
        Initialize RiskManager with configuration and data.
        
        Args:
            config: Dictionary containing risk management configuration
            ohlcv_data: OHLCV DataFrame for calculating indicators
        """
        self.config = config
        self.sl_tp_config = config.get('sl_tp', {})
        self.risk_config = config.get('risk_management', {})
        self.ohlcv_data = ohlcv_data.copy()
        
        # Calculate ATR if SL/TP is enabled
        self.atr_values = None
        if self.sl_tp_config.get('enabled', True):
            atr_length = self.sl_tp_config.get('atr_length', 14)
            self.atr_values = self._calculate_atr(atr_length)
            logger.info(f"ATR calculated with length={atr_length}")
        
        # Annual range calculation
        self.annual_high = None
        self.annual_low = None
        self.annual_range = None
        
        if self.risk_config.get('enabled', False):
            self._calculate_annual_range()
            if self.annual_range is not None:
                logger.info(f"Annual range calculated: {self.annual_range:.2f}")
    
    def _calculate_atr(self, length: int = 14) -> pd.Series:
        """Calculate Average True Range."""
        high = self.ohlcv_data['high']
        low = self.ohlcv_data['low']
        close = self.ohlcv_data['close']
        
        # Calculate True Range
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        # Calculate ATR
        atr = tr.rolling(window=length).mean()
        
        return atr
    
    def _calculate_annual_range(self):
        """Calculate annual high/low range from available data."""
        if self.ohlcv_data.empty:
            logger.warning("No OHLCV data for annual range calculation")
            return
        
        # Use last 252 trading days (~1 year) of data
        days_back = min(252, len(self.ohlcv_data))
        recent_data = self.ohlcv_data.iloc[-days_back:]
        
        self.annual_high = recent_data['high'].max()
        self.annual_low = recent_data['low'].min()
        self.annual_range = self.annual_high - self.annual_low
        
        if self.annual_range <= 0:
            logger.warning("Invalid annual range (<= 0)")
            self.annual_range = None
    
    def calculate_sl_tp(self, 
                       entry_price: float,
                       is_long: bool,
                       atr_value: Optional[float] = None) -> Tuple[Optional[float], Optional[float]]:
        """
        Calculate Stop Loss and Take Profit levels.
        Uses ATR for SL, and R:R ratio for TP.
        
        Args:
            entry_price: Entry price for the trade
            is_long: True for long position, False for short
            atr_value: ATR value (if None, uses current)
            
        Returns:
            Tuple of (stop_loss, take_profit) or (None, None) if disabled
        """
        if not self.sl_tp_config.get('enabled', True):
            logger.debug("SL/TP calculation skipped (disabled)")
            return None, None
        
        # Get ATR value
        if atr_value is None:
            if self.atr_values is not None and not self.atr_values.empty:
                atr_value = self.atr_values.iloc[-1]
            else:
                logger.error("Cannot calculate SL/TP - ATR not available")
                return None, None
        
        if atr_value <= 0:
            logger.error("Invalid ATR value for SL/TP calculation")
            return None, None
        
        # Get multipliers
        sl_mult = self.sl_tp_config.get('sl_multiplier', 1.4)
        rr_ratio = self.sl_tp_config.get('risk_to_reward_ratio', 2.0)
        
        # Calculate distances
        sl_distance = atr_value * sl_mult
        tp_distance = sl_distance * rr_ratio  # TP is R:R ratio times SL distance
        
        # Calculate prices
        if is_long:
            stop_loss = entry_price - sl_distance
            take_profit = entry_price + tp_distance
        else:
            stop_loss = entry_price + sl_distance
            take_profit = entry_price - tp_distance
        
        logger.debug(f"SL/TP calculated: Entry={entry_price:.2f}, "
                    f"SL={stop_loss:.2f}, TP={take_profit:.2f}, "
                    f"R:R=1:{rr_ratio}")
        
        return stop_loss, take_profit
    
    def validate_risk_percentile(self,
                               entry_price: float,
                               stop_loss: float,
                               is_long: bool) -> Tuple[bool, Optional[float], str]:
        """
        Validate stop loss against maximum risk percentile.
        
        Args:
            entry_price: Entry price
            stop_loss: Proposed stop loss price
            is_long: True for long position
            
        Returns:
            Tuple of (can_trade, adjusted_sl, comment)
        """
        if not self.risk_config.get('enabled', False):
            logger.debug("Risk percentile validation skipped (disabled)")
            return True, stop_loss, "Risk management disabled"
        
        if self.annual_range is None or self.annual_range <= 0:
            logger.warning("Annual range not available for risk validation")
            return True, stop_loss, "Annual range not available"
        
        max_percentile = self.risk_config.get('max_risk_percentile', 1.0)
        allow_exceed = self.risk_config.get('allow_exceed_limit', False)
        
        # Calculate current risk as percentile of annual range
        risk_distance = abs(entry_price - stop_loss)
        risk_percentile = risk_distance / self.annual_range
        
        if max_percentile >= 1.0 or risk_percentile <= max_percentile:
            # Risk within limits or feature disabled
            comment = f"SL: {risk_percentile*100:.2f}% of annual range"
            logger.debug(f"Risk within limits: {risk_percentile*100:.2f}% <= {max_percentile*100:.2f}%")
            return True, stop_loss, comment
        
        if allow_exceed:
            # Adjust SL to max percentile
            adjusted_distance = max_percentile * self.annual_range
            if is_long:
                adjusted_sl = entry_price - adjusted_distance
            else:
                adjusted_sl = entry_price + adjusted_distance
            
            comment = (f"SL adjusted from {risk_percentile*100:.2f}% to {max_percentile*100:.2f}% "
                      f"of annual range")
            logger.warning(f"Risk adjusted: {risk_percentile*100:.2f}% > {max_percentile*100:.2f}%, "
                         f"new SL: {adjusted_sl:.2f}")
            return True, adjusted_sl, comment
        else:
            # Reject trade
            comment = (f"Risk too high: {risk_percentile*100:.2f}% > "
                      f"max {max_percentile*100:.2f}% of annual range")
            logger.warning(f"Trade rejected: {risk_percentile*100:.2f}% > {max_percentile*100:.2f}%")
            return False, None, comment
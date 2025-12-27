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
    Implements 'Rolling Annual Range' to prevent lookahead bias.
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
        self.sl_tp_config = config.get('sl_tp', {})
        self.risk_config = config.get('risk_management', {})
        
        # Ensure we work with a copy and valid index
        self.ohlcv_data = ohlcv_data.copy()
        if not isinstance(self.ohlcv_data.index, pd.DatetimeIndex):
            # Attempt to convert if not already DatetimeIndex
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
        # Instead of a single static value, we generate a Series matching the index
        self.annual_range_series = None
        if self.risk_config.get('enabled', False):
            self._calculate_rolling_annual_range()
            logger.info("Rolling Annual Range calculated (Resampled Daily -> 252 Period)")

    def _calculate_atr_wilders(self, length: int = 14) -> pd.Series:
        """
        Calculate ATR using Wilder's Smoothing (RMA) to match TradingView.
        Standard Pandas .rolling().mean() is a simple SMA, which deviates from TV.
        """
        high = self.ohlcv_data['high']
        low = self.ohlcv_data['low']
        close = self.ohlcv_data['close']
        
        # Calculate True Range
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        # Wilder's Smoothing (RMA) is equivalent to EWM with alpha=1/length
        atr = tr.ewm(alpha=1/length, adjust=False).mean()
        
        return atr
    
    def _calculate_rolling_annual_range(self):
        """
        Calculate 1-Year Range based on Past Data only (No Lookahead).
        Method: Resample to Daily -> Roll 252 -> Shift 1 -> Broadcast back to Intraday.
        """
        if self.ohlcv_data.empty:
            return

        # 1. Resample to Daily (D) to mimic "HTF" logic and reduce computation
        daily_df = self.ohlcv_data.resample('D').agg({
            'high': 'max', 
            'low': 'min'
        }).dropna()
        
        # 2. Calculate Rolling 252-day (approx 1 trading year) High/Low
        # shift(1) is CRITICAL: We only use data up to Yesterday. 
        # Today's volatility should be judged against Past Year, not including Today.
        rolling_high = daily_df['high'].rolling(window=252, min_periods=20).max().shift(1)
        rolling_low = daily_df['low'].rolling(window=252, min_periods=20).min().shift(1)
        
        daily_range = rolling_high - rolling_low
        
        # 3. Broadcast (Forward Fill) back to the original 1-minute timestamps
        # This aligns the "Yesterday's Range" to "Today's Intraday Candles"
        self.annual_range_series = daily_range.reindex(self.ohlcv_data.index, method='ffill')

    def calculate_sl_tp(self, 
                       entry_price: float,
                       is_long: bool,
                       timestamp: pd.Timestamp = None,
                       manual_atr: Optional[float] = None) -> Tuple[Optional[float], Optional[float]]:
        """
        Calculate Stop Loss and Take Profit.
        
        Args:
            entry_price: Trade entry price
            is_long: Direction
            timestamp: Current candle timestamp (REQUIRED for correct historical ATR lookup)
            manual_atr: Optional override if not using historical lookup
            
        Returns:
            (stop_loss, take_profit)
        """
        if not self.sl_tp_config.get('enabled', True):
            return None, None
        
        # Determine ATR value
        atr_val = 0.0
        
        if manual_atr is not None:
            atr_val = manual_atr
        elif timestamp is not None and self.atr_series is not None:
            try:
                atr_val = self.atr_series.loc[timestamp]
            except KeyError:
                # Fallback if timestamp slightly off, or just warn
                logger.warning(f"ATR lookup failed for {timestamp}, checking nearest")
                # method='nearest' not supported in simple .loc, handle carefully in production
                return None, None
        else:
            logger.error("calculate_sl_tp requires either 'timestamp' or 'manual_atr'")
            return None, None

        if atr_val <= 0 or np.isnan(atr_val):
            return None, None # Cannot calc without valid ATR
            
        # Get multipliers
        sl_mult = self.sl_tp_config.get('sl_multiplier', 1.4)
        rr_ratio = self.sl_tp_config.get('risk_to_reward_ratio', 2.0)
        
        sl_distance = atr_val * sl_mult
        tp_distance = sl_distance * rr_ratio
        
        if is_long:
            stop_loss = entry_price - sl_distance
            take_profit = entry_price + tp_distance
        else:
            stop_loss = entry_price + sl_distance
            take_profit = entry_price - tp_distance
            
        return stop_loss, take_profit

    def validate_risk_percentile(self,
                               entry_price: float,
                               stop_loss: float,
                               is_long: bool,
                               timestamp: pd.Timestamp) -> Tuple[bool, Optional[float], str]:
        """
        Validate stop loss against MAX risk percentile of the Annual Range.
        Requires 'timestamp' to look up the valid range for that specific date.
        """
        if not self.risk_config.get('enabled', False):
            return True, stop_loss, "Risk mgmt disabled"

        if self.annual_range_series is None:
            return True, stop_loss, "Annual range data missing"
            
        # Look up the Annual Range for this specific moment
        try:
            current_annual_range = self.annual_range_series.loc[timestamp]
        except KeyError:
             return True, stop_loss, "Range data unavailable for date"

        if pd.isna(current_annual_range) or current_annual_range <= 0:
             # If we are at start of dataset and don't have 1 year history yet
             # Default to allowing trade or blocking (user preference). allowing for now.
             return True, stop_loss, "History insufficient for range calc"

        max_percentile = self.risk_config.get('max_risk_percentile', 1.0)
        allow_exceed = self.risk_config.get('allow_exceed_limit', False)
        
        # Calculate Risk Percentile
        risk_distance = abs(entry_price - stop_loss)
        risk_percentile = risk_distance / current_annual_range
        
        # 1. Check if within limits
        if max_percentile >= 1.0 or risk_percentile <= max_percentile:
            return True, stop_loss, f"Risk: {risk_percentile*100:.2f}% (Limit: {max_percentile*100:.2f}%)"
            
        # 2. Handle Exceeding Limits
        if allow_exceed:
            # Adjust SL
            adjusted_distance = max_percentile * current_annual_range
            if is_long:
                adjusted_sl = entry_price - adjusted_distance
            else:
                adjusted_sl = entry_price + adjusted_distance
            
            comment = (f"SL Adjusted: {risk_percentile*100:.2f}% -> {max_percentile*100:.2f}%")
            return True, adjusted_sl, comment
        else:
            # Reject
            comment = (f"Risk Rejected: {risk_percentile*100:.2f}% > {max_percentile*100:.2f}%")
            return False, None, comment
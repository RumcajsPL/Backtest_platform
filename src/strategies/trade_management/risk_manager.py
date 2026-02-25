"""Risk management: SL/TP with R:R ratio, spread adjustments, annual range validation

[FIX-L4-SL] 2026-02-25
Corrected long SL trigger price calculation to match eToro CFD BID price model.

eToro execution model (all OHLCV data is BID price):
  LONG  entry:      Ask = Bid + spread  → add spread                (unchanged)
  LONG  SL exit:    Bid                 → NO spread adjustment      (FIXED)
  LONG  TP exit:    Bid                 → no spread adjustment      (unchanged)
  SHORT entry:      Bid                 → no spread adjustment      (unchanged)
  SHORT SL exit:    Ask = Bid + spread  → add spread                (unchanged)
  SHORT TP exit:    Ask = Bid + spread  → add spread                (unchanged)

Previous (wrong):
    trigger_sl = raw_sl - spread_for_this if is_long else raw_sl + spread_for_this

Fixed:
    trigger_sl = raw_sl if is_long else raw_sl + spread_for_this

Root cause of discrepancy: the incorrect subtraction of spread from the long SL
trigger caused long positions to be held open longer than correct broker execution
requires (SL needed to fall an extra ~3.6 pts beyond the intended level). This
systematically blocked subsequent signals from opening new positions, producing
~52% fewer trades than the New pipeline over a 3-month window.
"""
import pandas as pd
import numpy as np
import logging
from typing import Tuple, Optional, Dict, Any
from .spread_manager import SpreadManager

logger = logging.getLogger(__name__)

class RiskManager:
    """Manages SL/TP calculations with R:R ratio and risk validation using rolling annual range"""
    
    def __init__(self, config: Dict[str, Any], ohlcv_data: pd.DataFrame, ohlcv_artf: Optional[pd.DataFrame] = None):
        """
        Initialize RiskManager with configuration and data.       
        """
        self.config = config
        tm_config = config.get('trade_management', {})
        self.sl_tp_config = tm_config.get('sl_tp', {})
        self.risk_config = tm_config.get('risk_management', {})
        self.spread_config = tm_config.get('spread', {})
        
        # Validate and prepare OHLCV data (strategy timeframe)
        self.ohlcv_data = ohlcv_data.copy()
        if not isinstance(self.ohlcv_data.index, pd.DatetimeIndex):
            if 'timestamp' in self.ohlcv_data.columns:
                self.ohlcv_data.set_index('timestamp', inplace=True)
            else:
                raise ValueError("RiskManager requires OHLCV data with DatetimeIndex")

        # Store monthly ARTF data (prefer explicit arg, fallback to config)
        self.ohlcv_artf = ohlcv_artf or self.config.get("data", {}).get("df_artf")

        # Pre-calculate ATR (Wilder's Smoothing)
        self.atr_series = None
        if self.sl_tp_config.get('enabled', True):
            atr_length = self.sl_tp_config.get('atr_length', 14)
            self.atr_series = self._calculate_atr_wilders(atr_length)
            logger.info(f"ATR calculated (Wilder's RMA, length={atr_length})")
        
        # Pre-calculate Rolling Annual Range (using monthly ARTF, year-month logic)
        self.annual_range_series = None
        if self.risk_config.get('enabled', False):
            self._calculate_rolling_annual_range()
            logger.info("Rolling Annual Range calculated (12-month ARTF lookback, year-month based)")
        
        # Initialize Spread Manager if enabled
        self.spread_manager = None
        if self.spread_config.get('enabled', False):
            asset_symbol = self.config.get('asset', {}).get('symbol', '')
            config_path = self.spread_config.get('config_path')
            self.spread_manager = SpreadManager(asset_symbol, config_path)
            logger.info(f"SpreadManager initialized for {asset_symbol}")

    # -------------------------------------------------------------------------
    # ATR (unchanged)
    # -------------------------------------------------------------------------
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
        
        return atr.astype('float32')

    # -------------------------------------------------------------------------
    # Rolling Annual Range using 12 monthly bars (year-month based, no lookahead)
    # -------------------------------------------------------------------------
    def _calculate_rolling_annual_range(self):
        """
        Fast 12-month RAR using monthly ARTF bars.
        Precompute RAR per month, then map to strategy timestamps.
        """
        if self.ohlcv_artf is None or self.ohlcv_artf.empty:
            logger.warning("Monthly ARTF data missing — annual range disabled")
            self.annual_range_series = None
            return

        # Copy and ensure DatetimeIndex
        monthly = self.ohlcv_artf.copy()
        if not isinstance(monthly.index, pd.DatetimeIndex):
            raise ValueError("ARTF monthly data must have DatetimeIndex")

        monthly = monthly.sort_index()
        monthly.index = monthly.index.normalize()

        # Year-month key
        monthly["ym"] = monthly.index.to_period("M")
        monthly_by_ym = monthly.set_index("ym")[["high", "low"]]

        # Compute RAR per month (vectorized)
        yms = monthly_by_ym.index.unique().sort_values()

        rar_per_month = {}

        for i, ym in enumerate(yms):
            prev_ym = ym - 1
            start_ym = prev_ym - 11

            # Slice once per month (fast)
            window = monthly_by_ym.loc[start_ym:prev_ym]
            if len(window) == 0:
                rar_per_month[ym] = np.nan
            else:
                rar_per_month[ym] = float(window["high"].max() - window["low"].min())

        # Convert to Series indexed by month
        rar_monthly_series = pd.Series(rar_per_month, dtype="float32")

        # Map each strategy timestamp to its month
        strategy_ym = self.ohlcv_data.index.to_period("M")

        # RAR for strategy timestamps = RAR of previous month
        strategy_prev_ym = strategy_ym - 1

        # Map using vectorized lookup
        rar_strategy = strategy_prev_ym.map(rar_monthly_series)

        self.annual_range_series = pd.Series(
            rar_strategy.values,
            index=self.ohlcv_data.index,
            dtype="float32"
        )

    # -------------------------------------------------------------------------
    # Trade parameter computation
    # -------------------------------------------------------------------------
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
        
        # [FIX-L4-SL] Correct SL trigger per eToro BID price model:
        #   LONG  SL: exit when Bid falls to SL level — data IS Bid, no adjustment needed
        #   SHORT SL: exit when Bid rises to SL level, but execution at Ask = Bid + spread
        #
        # BEFORE (wrong): trigger_sl = raw_sl - spread_for_this if is_long else raw_sl + spread_for_this
        # AFTER  (fixed): trigger_sl = raw_sl if is_long else raw_sl + spread_for_this
        trigger_sl = raw_sl if is_long else raw_sl + spread_for_this
        
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

    # -------------------------------------------------------------------------
    # Risk percentile validation (unchanged)
    # -------------------------------------------------------------------------
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
            return True, stop_loss, "RAR not initialized"

        # Get current RAR
        try:
            current_annual_range = self.annual_range_series.loc[timestamp]
        except KeyError:
            return True, stop_loss, "RAR missing for timestamp"

        # If RAR is invalid/NaN or non-positive, do NOT block the trade
        if pd.isna(current_annual_range) or current_annual_range <= 0:
            return True, stop_loss, f"RAR unavailable or invalid ({current_annual_range})"

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
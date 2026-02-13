"""Spread management: handles broker spread calculations based on BID price data
MIGRATED: Session 7 - Task 2
Location: src/strategies/specific/modules/spread_manager.py

Minimal changes - pure utility class with no contract dependencies.
"""
import yaml
import logging
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class SpreadManager:
    """
    Manages broker spread application assuming input data is BID PRICE.
    
    Spread Application Rules:
    - LONG Entry: Bid + Spread (buy at Ask)
    - LONG SL Trigger: Bid_SL - Spread (sell at Bid when hit)
    - SHORT Entry: Bid (sell at Bid)
    - SHORT SL Trigger: Bid_SL + Spread (buy at Ask when hit)
    
    Supported Spread Types:
    - 'percentage': Spread as % of price (e.g., 0.05 = 0.05%)
    - 'points': Spread in absolute price points (e.g., 1.0 = 1 point)
    - 'pips': Spread in forex pips (e.g., 2 pips with pip_position=4)
    
    MIGRATION NOTE (Session 7):
    - No contract changes needed (utility class)
    - Updated config path resolution for project structure
    - Enhanced type hints and documentation
    """
    
    def __init__(self, asset_symbol: str, spread_config_path: Optional[str] = None):
        """
        Initialize SpreadManager with asset and config.
        
        Args:
            asset_symbol: Asset symbol (e.g., 'DEUIDXEUR')
            spread_config_path: Optional path to spread config YAML
                               If None, uses default: configs/spreads/broker_spreads.yaml
        """
        self.asset_symbol = asset_symbol.upper()
        self.spread_config = None
        self.asset_config = None
        
        # Determine config path
        if spread_config_path is None:
            # Use project root resolution
            # Navigate from src/strategies/specific/modules/ to project root
            project_root = Path(__file__).resolve().parents[4]
            spread_config_path = project_root / "configs" / "spreads" / "broker_spreads.yaml"
        else:
            spread_config_path = Path(spread_config_path)
        
        self._load_config(spread_config_path)

    def _load_config(self, config_path: Path):
        """
        Load spread configuration from YAML file.
        
        Args:
            config_path: Path to broker_spreads.yaml
            
        Raises:
            FileNotFoundError: If config file not found
            Exception: If config parsing fails
        """
        try:
            with open(config_path, 'r') as f:
                self.spread_config = yaml.safe_load(f)
            
            spreads = self.spread_config.get('spreads', {})
            if self.asset_symbol not in spreads:
                logger.warning(f"Asset {self.asset_symbol} not found in spread config")
                logger.warning(f"Available assets: {list(spreads.keys())}")
                return
            
            self.asset_config = spreads[self.asset_symbol]
            logger.info(
                f"Spread config loaded for {self.asset_symbol}: "
                f"{self.asset_config['spread_value']} {self.asset_config['spread_type']}"
            )
        except FileNotFoundError:
            logger.error(f"Spread config file not found: {config_path}")
            raise
        except Exception as e:
            logger.error(f"Error loading spread config: {e}")
            raise

    def get_spread_in_points(self, bid_price: float) -> float:
        """
        Calculate spread in price points for given bid price.
        
        Args:
            bid_price: Current bid price
            
        Returns:
            Spread in price points (absolute value)
            Returns 0.0 if no config or spread disabled
            
        Examples:
            # Percentage spread (0.05% of 19800 = 9.9 points)
            spread = mgr.get_spread_in_points(19800.0)  # → 9.9
            
            # Fixed points spread
            spread = mgr.get_spread_in_points(19800.0)  # → 1.0
            
            # Pips spread (2 pips with pip_position=4 → 0.0002)
            spread = mgr.get_spread_in_points(1.1850)  # → 0.0002
        """
        if self.asset_config is None:
            return 0.0
        
        spread_type = self.asset_config['spread_type']
        spread_value = self.asset_config['spread_value']
        
        if spread_type == 'percentage':
            # Percentage of price
            return (spread_value / 100.0) * bid_price
        
        elif spread_type == 'points':
            # Fixed points
            return spread_value
        
        elif spread_type == 'pips':
            # Forex pips (depends on pip position)
            pip_position = self.asset_config.get('pip_position', 4)
            return spread_value * (10 ** (-pip_position))
        
        return 0.0

    def calculate_entry_cost(self, bid_price: float, is_long: bool) -> float:
        """
        Calculate actual entry price including spread.
        
        Args:
            bid_price: Current bid/mid price
            is_long: True for LONG position, False for SHORT
            
        Returns:
            Actual entry price after spread adjustment
            
        Logic:
            - LONG: Buy at Ask = Bid + Spread (pay spread to enter)
            - SHORT: Sell at Bid (no spread on entry)
            
        Examples:
            # LONG with 1.0 point spread
            entry = mgr.calculate_entry_cost(19800.0, is_long=True)
            # → 19801.0 (buy at Ask)
            
            # SHORT (no spread on entry)
            entry = mgr.calculate_entry_cost(19800.0, is_long=False)
            # → 19800.0 (sell at Bid)
        """
        spread = self.get_spread_in_points(bid_price)
        
        if is_long:
            # LONG: Buy at Ask (Bid + Spread)
            return bid_price + spread
        else:
            # SHORT: Sell at Bid (no spread adjustment)
            return bid_price

    def get_sl_trigger_level(self, raw_sl_price: float, spread: float, is_long: bool) -> float:
        """
        Calculate SL trigger level accounting for spread.
        
        Args:
            raw_sl_price: Desired SL price (chart level)
            spread: Spread in points
            is_long: True for LONG position, False for SHORT
            
        Returns:
            Adjusted SL trigger level
            
        Logic:
            - LONG SL: Sell at Bid when hit → trigger at (SL - Spread)
            - SHORT SL: Buy at Ask when hit → trigger at (SL + Spread)
            
        Examples:
            # LONG: SL at 19750, spread 1.0
            trigger = mgr.get_sl_trigger_level(19750.0, 1.0, is_long=True)
            # → 19749.0 (trigger before chart SL to account for selling at Bid)
            
            # SHORT: SL at 19850, spread 1.0
            trigger = mgr.get_sl_trigger_level(19850.0, 1.0, is_long=False)
            # → 19851.0 (trigger before chart SL to account for buying at Ask)
        """
        if is_long:
            # LONG: Exit at Bid (subtract spread from trigger)
            return raw_sl_price - spread
        else:
            # SHORT: Exit at Ask (add spread to trigger)
            return raw_sl_price + spread

    def get_spread_info(self) -> Dict:
        """
        Get spread configuration information.
        
        Returns:
            Dict with spread config details or {'enabled': False} if not configured
            
        Example:
            info = mgr.get_spread_info()
            # → {
            #     'enabled': True,
            #     'asset': 'DEUIDXEUR',
            #     'spread_value': 1.0,
            #     'spread_type': 'points'
            # }
        """
        if self.asset_config is None:
            return {'enabled': False}
        
        return {
            'enabled': True,
            'asset': self.asset_symbol,
            'spread_value': self.asset_config['spread_value'],
            'spread_type': self.asset_config['spread_type']
        }
    
    def is_enabled(self) -> bool:
        """
        Check if spread is enabled for this asset.
        
        Returns:
            True if spread config loaded, False otherwise
        """
        return self.asset_config is not None
    
    def __repr__(self) -> str:
        """String representation for debugging"""
        if self.asset_config is None:
            return f"SpreadManager({self.asset_symbol}, disabled)"
        
        return (
            f"SpreadManager({self.asset_symbol}, "
            f"{self.asset_config['spread_value']} {self.asset_config['spread_type']})"
        )


# ============================================================================
# UTILITY FUNCTIONS (for testing and debugging)
# ============================================================================

def calculate_spread_impact(
    entry_price: float,
    sl_price: float,
    tp_price: float,
    spread: float,
    is_long: bool
) -> Dict[str, float]:
    """
    Calculate spread impact on a trade.
    
    Utility function to analyze how spread affects entry, SL, and potential P&L.
    
    Args:
        entry_price: Entry price (before spread)
        sl_price: Stop loss price
        tp_price: Take profit price
        spread: Spread in points
        is_long: True for LONG, False for SHORT
        
    Returns:
        Dict with spread impact analysis
        
    Example:
        impact = calculate_spread_impact(
            entry_price=19800.0,
            sl_price=19750.0,
            tp_price=19900.0,
            spread=1.0,
            is_long=True
        )
        # → {
        #     'entry_cost': 1.0,
        #     'sl_slippage': 1.0,
        #     'total_cost': 2.0,
        #     'cost_as_percent_of_risk': 4.0,
        #     'effective_rr_ratio': 1.96
        # }
    """
    # Entry cost
    entry_cost = spread if is_long else 0.0
    
    # SL slippage (we trigger earlier to account for exit spread)
    sl_slippage = spread
    
    # Total spread cost
    total_cost = entry_cost + sl_slippage
    
    # Original risk and reward
    if is_long:
        risk = entry_price - sl_price
        reward = tp_price - entry_price
    else:
        risk = sl_price - entry_price
        reward = entry_price - tp_price
    
    # Cost as % of risk
    cost_pct = (total_cost / risk * 100) if risk > 0 else 0
    
    # Effective R:R after spread
    adjusted_reward = reward - entry_cost
    adjusted_risk = risk + sl_slippage
    effective_rr = adjusted_reward / adjusted_risk if adjusted_risk > 0 else 0
    
    return {
        'entry_cost': entry_cost,
        'sl_slippage': sl_slippage,
        'total_cost': total_cost,
        'cost_as_percent_of_risk': cost_pct,
        'effective_rr_ratio': effective_rr,
        'original_rr_ratio': reward / risk if risk > 0 else 0
    }
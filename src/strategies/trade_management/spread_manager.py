"""Spread management: handles broker spread calculations based on BID price data"""
import yaml
import logging
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)

class SpreadManager:
    """
    Manages broker spread application assuming input data is BID PRICE.
    Formulas:
    - LONG Entry: Bid + Spread (buy at Ask)
    - LONG SL Trigger: Bid_SL - Spread
    - SHORT Entry: Bid (sell at Bid)
    - SHORT SL Trigger: Bid_SL + Spread
    """
    
    def __init__(self, asset_symbol: str, spread_config_path: Optional[str] = None):
        self.asset_symbol = asset_symbol.upper()
        self.spread_config = None
        self.asset_config = None
        
        # Determine config path
        if spread_config_path is None:
            project_root = Path(__file__).resolve().parent.parent.parent.parent
            spread_config_path = project_root / "configs" / "spreads" / "broker_spreads.yaml"
        
        self._load_config(spread_config_path)

    def _load_config(self, config_path: Path):
        """Load spread configuration from YAML file"""
        try:
            with open(config_path, 'r') as f:
                self.spread_config = yaml.safe_load(f)
            
            spreads = self.spread_config.get('spreads', {})
            if self.asset_symbol not in spreads:
                logger.warning(f"Asset {self.asset_symbol} not found in spread config")
                return
            
            self.asset_config = spreads[self.asset_symbol]
            logger.info(f"Spread config loaded for {self.asset_symbol}: "
                       f"{self.asset_config['spread_value']} {self.asset_config['spread_type']}")
        except FileNotFoundError:
            logger.error(f"Spread config file not found: {config_path}")
            raise
        except Exception as e:
            logger.error(f"Error loading spread config: {e}")
            raise

    def get_spread_in_points(self, bid_price: float) -> float:
        
        if self.asset_config is None:
            return 0.0
        
        spread_type = self.asset_config['spread_type']
        spread_value = self.asset_config['spread_value']
        
        if spread_type == 'percentage':
            return (spread_value / 100.0) * bid_price
        elif spread_type == 'points':
            return spread_value
        elif spread_type == 'pips':
            pip_position = self.asset_config.get('pip_position', 4)
            return spread_value * (10 ** (-pip_position))
        
        return 0.0

    def calculate_entry_cost(self, bid_price: float, is_long: bool) -> float:
        
        spread = self.get_spread_in_points(bid_price)
        if is_long:
            return bid_price + spread  # Buy at Ask
        else:
            return bid_price  # Sell at Bid

    def get_sl_trigger_level(self, raw_sl_price: float, spread: float, is_long: bool) -> float:
        
        if is_long:
            return raw_sl_price - spread
        else:
            return raw_sl_price + spread

    def get_spread_info(self) -> Dict:
        """Get spread configuration information"""
        if self.asset_config is None:
            return {'enabled': False}
        
        return {
            'enabled': True,
            'asset': self.asset_symbol,
            'spread_value': self.asset_config['spread_value'],
            'spread_type': self.asset_config['spread_type']
        }
"""
Data loading and validation module
"""
import pandas as pd
import yaml
from pathlib import Path
from typing import Dict, Tuple

class DataLoader:
    def __init__(self, config_path: str):
        self.config_path = Path(config_path).resolve()
        self.project_root = self.config_path.parent.parent.parent.parent  # Adjusted to correctly reach project root
        self.config = None
        self.df_full = None
        self.df_strategy = None
        
    def load_config(self) -> Dict:
        """Load YAML configuration file"""
        with open(self.config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        return self.config
    
    def load_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Load OHLCV data and apply date range"""
        data_cfg = self.config.get('data', {})
        
        # Construct file path
        data_file = data_cfg['file']
        if not Path(data_file).is_absolute():
            data_file = self.project_root / data_file
        
        # Load full dataset
        self.df_full = pd.read_csv(data_file, parse_dates=['timestamp'])
        self.df_full.columns = self.df_full.columns.str.lower()
        self.df_full = self.df_full.set_index('timestamp').sort_index()
        
        # Apply date range for strategy
        dr = data_cfg.get('date_range', {})
        if dr:
            self.df_strategy = self.df_full[
                (self.df_full.index >= pd.to_datetime(dr['start'])) & 
                (self.df_full.index <= pd.to_datetime(dr['end']))
            ].copy()
        else:
            self.df_strategy = self.df_full.copy()
        
        return self.df_full, self.df_strategy
    
    def get_data_info(self) -> Dict:
        """Get information about loaded data"""
        if self.df_strategy is None or self.df_full is None:
            return {}
        
        return {
            'full_bars': len(self.df_full),
            'strategy_bars': len(self.df_strategy),
            'date_range': [
                self.df_strategy.index[0].strftime('%Y-%m-%d %H:%M:%S'),
                self.df_strategy.index[-1].strftime('%Y-%m-%d %H:%M:%S')
            ] if len(self.df_strategy) > 0 else [],
            'full_range': [
                self.df_full.index[0].strftime('%Y-%m-%d %H:%M:%S'),
                self.df_full.index[-1].strftime('%Y-%m-%d %H:%M:%S')
            ] if len(self.df_full) > 0 else []
        }
    
    def validate_data(self) -> Dict:
        """Perform basic data validation"""
        validation = {
            'has_data': self.df_strategy is not None and len(self.df_strategy) > 0,
            'ohlc_columns': all(col in self.df_strategy.columns for col in ['open', 'high', 'low', 'close']),
            'no_nan': not self.df_strategy[['open', 'high', 'low', 'close']].isnull().any().any(),
            'positive_prices': (self.df_strategy[['open', 'high', 'low', 'close']] > 0).all().all(),
            'high_low_valid': (self.df_strategy['high'] >= self.df_strategy['low']).all(),
            'open_close_valid': (
                (self.df_strategy['open'] >= self.df_strategy['low']) & 
                (self.df_strategy['open'] <= self.df_strategy['high']) &
                (self.df_strategy['close'] >= self.df_strategy['low']) & 
                (self.df_strategy['close'] <= self.df_strategy['high'])
            ).all()
        }
        
        validation['is_valid'] = all(validation.values())
        return validation
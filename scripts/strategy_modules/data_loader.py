"""
Data loading and validation module
"""
import pandas as pd
import yaml
from pathlib import Path
from typing import Dict, Tuple, Optional

class DataLoader:
    def __init__(self, config_path: str):
        self.config_path = Path(config_path).resolve()
        self.project_root = self.config_path.parent.parent.parent.parent  # Adjusted to correctly reach project root
        self.config = None
        self.df_full = None
        self.df_strategy = None
        self.df_htf = None  # New: Dedicated HTF DataFrame
        
    def load_config(self) -> Dict:
        """Load YAML configuration file"""
        with open(self.config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        return self.config
    
    def load_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, Optional[pd.DataFrame]]:
        """Load OHLCV data and apply date range. Now supports optional HTF file."""
        data_cfg = self.config.get('data', {})
        
        # Construct base file path
        data_file = data_cfg['file']
        if not Path(data_file).is_absolute():
            data_file = self.project_root / data_file
        
        # Load full base dataset
        self.df_full = pd.read_csv(data_file, parse_dates=['timestamp'])
        self.df_full.columns = self.df_full.columns.str.lower()
        self.df_full = self.df_full.set_index('timestamp').sort_index()
        
        # Apply date range for strategy
        dr = data_cfg.get('date_range', {})
        start = pd.to_datetime(dr['start']) if dr else self.df_full.index.min()
        end = pd.to_datetime(dr['end']) if dr else self.df_full.index.max()
        self.df_strategy = self.df_full[(self.df_full.index >= start) & (self.df_full.index <= end)].copy()
        
        # New: Load dedicated HTF if specified
        if 'file_htf' in data_cfg:
            htf_file = data_cfg['file_htf']
            if not Path(htf_file).is_absolute():
                htf_file = self.project_root / htf_file
            
            self.df_htf = pd.read_csv(htf_file, parse_dates=['timestamp'])
            self.df_htf.columns = self.df_htf.columns.str.lower()
            self.df_htf = self.df_htf.set_index('timestamp').sort_index()
            
            # Apply same date range to HTF
            self.df_htf = self.df_htf[(self.df_htf.index >= start) & (self.df_htf.index <= end)].copy()
        
        return self.df_full, self.df_strategy, self.df_htf
    
    def get_data_info(self) -> Dict:
        """Get information about loaded data"""
        if self.df_strategy is None or self.df_full is None:
            return {}
        
        info = {
            'full_bars': len(self.df_full),
            'strategy_bars': len(self.df_strategy),
            'date_range': [
                self.df_strategy.index[0].isoformat() if len(self.df_strategy) > 0 else None,
                self.df_strategy.index[-1].isoformat() if len(self.df_strategy) > 0 else None
            ],
            'full_range': [
                self.df_full.index[0].isoformat() if len(self.df_full) > 0 else None,
                self.df_full.index[-1].isoformat() if len(self.df_full) > 0 else None
            ]
        }
        
        if self.df_htf is not None:
            info['htf_bars'] = len(self.df_htf)
            info['htf_range'] = [
                self.df_htf.index[0].isoformat() if len(self.df_htf) > 0 else None,
                self.df_htf.index[-1].isoformat() if len(self.df_htf) > 0 else None
            ]
        
        return info
    
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
        
        # New: Validate HTF if loaded
        if self.df_htf is not None:
            validation.update({
                'htf_has_data': len(self.df_htf) > 0,
                'htf_ohlc_columns': all(col in self.df_htf.columns for col in ['open', 'high', 'low', 'close']),
                'htf_no_nan': not self.df_htf[['open', 'high', 'low', 'close']].isnull().any().any(),
                'htf_positive_prices': (self.df_htf[['open', 'high', 'low', 'close']] > 0).all().all(),
                'htf_high_low_valid': (self.df_htf['high'] >= self.df_htf['low']).all(),
                'htf_open_close_valid': (
                    (self.df_htf['open'] >= self.df_htf['low']) & 
                    (self.df_htf['open'] <= self.df_htf['high']) &
                    (self.df_htf['close'] >= self.df_htf['low']) & 
                    (self.df_htf['close'] <= self.df_htf['high'])
                ).all()
            })
        
        validation['is_valid'] = all(validation.values())
        return validation
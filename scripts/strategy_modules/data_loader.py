"""Orchestrator source data loading model"""
import pandas as pd
import yaml
from pathlib import Path
from typing import Dict, Tuple, Optional
import hashlib
import pickle

class DataLoader:
    def __init__(self, config_path: str):
        self.config_path = Path(config_path).resolve()
        self.project_root = self.config_path.parent.parent.parent.parent
        self.config = None
        self.df_full = None
        self.df_strategy = None
        self.df_htf = None
        self.df_ltf = None  # New: LTF DataFrame for execution
        
        # CACHE INITIALIZATION - NEW
        self.cache_dir = Path.home() / ".wbws_data_cache"
        self.cache_dir.mkdir(exist_ok=True)
        self.cache_hits = 0
        self.cache_misses = 0
        
    def load_config(self) -> Dict:
        """Load YAML configuration file"""
        with open(self.config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        return self.config
    
    # NEW: CACHE HELPER METHODS
    def _get_cache_key(self, file_path: Path, start_date=None, end_date=None):
        """Generate unique cache key for file and date range"""
        if not file_path.exists():
            return None
            
        # Use file stats for change detection
        stat = file_path.stat()
        key_parts = [
            str(file_path.resolve()),
            f"size:{stat.st_size}",
            f"mtime:{stat.st_mtime}"
        ]
        
        if start_date:
            key_parts.append(f"start:{start_date}")
        if end_date:
            key_parts.append(f"end:{end_date}")
        
        key_str = "|".join(key_parts)
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def _load_cached_data(self, cache_key: str):
        """Load data from cache if exists"""
        if not cache_key:
            return None
            
        cache_file = self.cache_dir / f"{cache_key}.pkl"
        if cache_file.exists():
            try:
                with open(cache_file, 'rb') as f:
                    return pickle.load(f)
            except (pickle.UnpicklingError, EOFError, AttributeError) as e:
                # Cache corrupted, delete it
                cache_file.unlink()
                print(f"⚠️  Cache corrupted, deleted: {e}")
                return None
        return None
    
    def _save_to_cache(self, cache_key: str, df: pd.DataFrame):
        """Save data to cache"""
        if not cache_key:
            return
            
        cache_file = self.cache_dir / f"{cache_key}.pkl"
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump(df.copy(deep=True), f)  # Save deep copy
            print(f"💾 Saved to cache: {cache_file.name}")
        except Exception as e:
            print(f"⚠️  Could not save to cache: {e}")
    
    def _load_csv_with_cache(self, file_path: Path, data_type: str, start_date=None, end_date=None) -> pd.DataFrame:
        """Load CSV with caching, return deep copy"""
        cache_key = self._get_cache_key(file_path, start_date, end_date)
        
        cached_df = self._load_cached_data(cache_key)
        if cached_df is not None:
            self.cache_hits += 1
            print(f"   🔄 Cache hit for {data_type}: {file_path.name}")
            return cached_df.copy(deep=True)  # Return deep copy
        
        self.cache_misses += 1
        print(f"   📥 Loading fresh {data_type}: {file_path.name}")
        
        df = pd.read_csv(file_path, parse_dates=['timestamp'])
        df.columns = df.columns.str.lower()
        df = df.set_index('timestamp').sort_index()
        
        # Apply date range if specified
        if start_date or end_date:
            mask = True
            if start_date:
                mask &= (df.index >= pd.to_datetime(start_date))
            if end_date:
                mask &= (df.index <= pd.to_datetime(end_date))
            df = df[mask]
        
        self._save_to_cache(cache_key, df)
        return df.copy(deep=True)  # Return deep copy
    
    def load_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """Load OHLCV data and apply date range with caching. Supports optional HTF and LTF files."""
        data_cfg = self.config.get('data', {})
        
        # Construct and ensure base file path is Path
        data_file = data_cfg['file']
        data_file = Path(data_file)  # Zawsze konwertuj na Path
        if not data_file.is_absolute():
            data_file = self.project_root / data_file
        
        # Load full base dataset (1min TF) - no date range for full
        self.df_full = self._load_csv_with_cache(data_file, 'full')
        
        # Apply date range for strategy
        dr = data_cfg.get('date_range', {})
        start = pd.to_datetime(dr.get('start')) if dr.get('start') else self.df_full.index.min()
        end = pd.to_datetime(dr.get('end')) if dr.get('end') else self.df_full.index.max()
        self.df_strategy = self.df_full[(self.df_full.index >= start) & (self.df_full.index <= end)].copy(deep=True)
        
        # Load dedicated HTF if specified
        self.df_htf = None
        if 'file_htf' in data_cfg:
            htf_file = data_cfg['file_htf']
            htf_file = Path(htf_file)  # Zawsze konwertuj na Path
            if not htf_file.is_absolute():
                htf_file = self.project_root / htf_file
            
            self.df_htf = self._load_csv_with_cache(htf_file, 'htf', start, end)
        
        # Load dedicated LTF if specified
        self.df_ltf = None
        if 'file_ltf' in data_cfg:
            ltf_file = data_cfg['file_ltf']
            ltf_file = Path(ltf_file)  # Zawsze konwertuj na Path
            if not ltf_file.is_absolute():
                ltf_file = self.project_root / ltf_file
            
            self.df_ltf = self._load_csv_with_cache(ltf_file, 'ltf', start, end)
        
        return self.df_full, self.df_strategy, self.df_htf, self.df_ltf
    
    def get_data_info(self) -> Dict:
        """Get data statistics including HTF and LTF info"""
        info = {
            'full_bars': len(self.df_full) if self.df_full is not None else 0,
            'strategy_bars': len(self.df_strategy) if self.df_strategy is not None else 0,
            'htf_bars': len(self.df_htf) if self.df_htf is not None else 0,
            'ltf_bars': len(self.df_ltf) if self.df_ltf is not None else 0,
            'date_range': (
                self.df_strategy.index.min().strftime('%Y-%m-%d') if self.df_strategy is not None else None,
                self.df_strategy.index.max().strftime('%Y-%m-%d') if self.df_strategy is not None else None
            )
        }
        if self.df_ltf is not None:
            info['ltf_tf'] = self.config.get('data', {}).get('ltf_timeframe', '1s')
        return info
    
    def validate_data(self) -> Dict:
        """Validate loaded data including HTF and LTF"""
        validation = {
            'has_data': len(self.df_strategy) > 0,
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
        
        # Validate HTF if loaded
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
        
        # Validate LTF if loaded
        if self.df_ltf is not None:
            validation.update({
                'ltf_has_data': len(self.df_ltf) > 0,
                'ltf_ohlc_columns': all(col in self.df_ltf.columns for col in ['open', 'high', 'low', 'close']),
                'ltf_no_nan': not self.df_ltf[['open', 'high', 'low', 'close']].isnull().any().any(),
                'ltf_positive_prices': (self.df_ltf[['open', 'high', 'low', 'close']] > 0).all().all(),
                'ltf_high_low_valid': (self.df_ltf['high'] >= self.df_ltf['low']).all(),
                'ltf_open_close_valid': (
                    (self.df_ltf['open'] >= self.df_ltf['low']) & 
                    (self.df_ltf['open'] <= self.df_ltf['high']) &
                    (self.df_ltf['close'] >= self.df_ltf['low']) & 
                    (self.df_ltf['close'] <= self.df_ltf['high'])
                ).all()
            })
        
        validation['is_valid'] = all(validation.values())
        return validation
    
    # NEW: Cache management methods
    def get_cache_stats(self) -> Dict:
        """Get cache statistics"""
        total = self.cache_hits + self.cache_misses
        hit_rate = (self.cache_hits / total * 100) if total > 0 else 0
        
        cache_files = list(self.cache_dir.glob("*.pkl"))
        total_size = sum(f.stat().st_size for f in cache_files)
        
        return {
            'hits': self.cache_hits,
            'misses': self.cache_misses,
            'hit_rate': f"{hit_rate:.1f}%",
            'cache_files': len(cache_files),
            'cache_size_mb': total_size / (1024 * 1024),
            'cache_dir': str(self.cache_dir)
        }
    
    def clear_cache(self, pattern: str = "*"):
        """Clear cache files matching pattern"""
        cache_files = list(self.cache_dir.glob(f"{pattern}.pkl"))
        for cache_file in cache_files:
            try:
                cache_file.unlink()
            except Exception as e:
                print(f"⚠️  Could not delete {cache_file.name}: {e}")
        
        print(f"🧹 Cleared {len(cache_files)} cache files")
        return len(cache_files)
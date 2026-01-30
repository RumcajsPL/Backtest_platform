"""Orchestrator source data loading model"""
import pandas as pd
import yaml
from pathlib import Path
from typing import Dict, Tuple, Optional
import hashlib
import pickle
import logging

logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, config_path: str):
        self.config_path = Path(config_path).resolve()
        self.project_root = self.config_path.parent.parent.parent.parent
        self.config = None
        self.df_full = None
        self.df_strategy = None
        self.df_htf = None
        self.df_ltf = None
        
        self.cache_dir = Path.home() / ".wbws_data_cache"
        self.cache_dir.mkdir(exist_ok=True)
        self.cache_hits = 0
        self.cache_misses = 0

    def load_config(self) -> Dict:
        with open(self.config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        return self.config

    def _get_cache_key(self, file_path: Path, start_date=None, end_date=None) -> Optional[str]:
        if not file_path.exists():
            return None

        stat = file_path.stat()
        key_parts = [str(file_path.resolve()), f"size:{stat.st_size}", f"mtime:{stat.st_mtime}"]

        try:
            with open(file_path, 'rb') as f:
                content = f.read(1024 * 1024)  # First 1MB
            key_parts.append(f"content:{hashlib.md5(content).hexdigest()}")
        except Exception as e:
            logger.warning(f"Failed to compute content hash for {file_path.name}: {e}")

        if start_date:
            key_parts.append(f"start:{start_date}")
        if end_date:
            key_parts.append(f"end:{end_date}")

        return hashlib.md5("|".join(key_parts).encode()).hexdigest()

    def _load_cached_data(self, cache_key: str) -> Optional[pd.DataFrame]:
        if not cache_key:
            return None
        cache_file = self.cache_dir / f"{cache_key}.pkl"
        if not cache_file.exists():
            return None
        try:
            with open(cache_file, 'rb') as f:
                return pickle.load(f)
        except (pickle.UnpicklingError, EOFError, AttributeError) as e:
            cache_file.unlink(missing_ok=True)
            logger.warning(f"Cache corrupted/deleted: {cache_file.name} ({e})")
            return None

    def _save_to_cache(self, cache_key: str, df: pd.DataFrame):
        if not cache_key:
            return
        cache_file = self.cache_dir / f"{cache_key}.pkl"
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump(df.copy(deep=True), f)
            logger.debug(f"Saved cache: {cache_file.name}")
        except Exception as e:
            logger.warning(f"Cache save failed for {cache_file.name}: {e}")

    def _validate_date_format(self, date_str: str, date_type: str):
        """Validate that date string includes time component"""
        if date_str and ' ' not in str(date_str):
            raise ValueError(
                f"{date_type} '{date_str}' must include time component (e.g., '2025-12-15 08:00:00'). "
                f"Please check your config file: {self.config_path}"
            )

    def _load_csv_with_cache(self, file_path: Path, data_type: str, start_date=None, end_date=None) -> pd.DataFrame:
        if start_date:
            self._validate_date_format(start_date, "start_date")
        if end_date:
            self._validate_date_format(end_date, "end_date")
        
        cache_key = self._get_cache_key(file_path, start_date, end_date)
        cached = self._load_cached_data(cache_key)
        if cached is not None:
            self.cache_hits += 1
            logger.info(f"Cache hit for {data_type}: {file_path.name}")
            return cached.copy(deep=True)

        self.cache_misses += 1
        logger.info(f"Loading fresh {data_type}: {file_path.name}")

        df = pd.read_csv(file_path, parse_dates=['timestamp'])
        df.columns = df.columns.str.lower()
        df = df.set_index('timestamp').sort_index()

        price_cols = ['open', 'high', 'low', 'close']
        available_prices = [col for col in price_cols if col in df.columns]
        if available_prices:
            df[available_prices] = df[available_prices].astype('float32')

        if start_date or end_date:
            mask = pd.Series(True, index=df.index)
            if start_date:
                start_dt = pd.to_datetime(start_date)
                mask &= (df.index >= start_dt)
            if end_date:
                end_dt = pd.to_datetime(end_date)
                mask &= (df.index <= end_dt)
            df = df[mask]

        self._save_to_cache(cache_key, df)
        return df.copy(deep=True)

    def load_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        data_cfg = self.config.get('data', {})
        
        dr = data_cfg.get('date_range', {})
        start_date_str = dr.get('start')
        end_date_str = dr.get('end')
        
        if start_date_str:
            self._validate_date_format(start_date_str, "start_date in config")
        if end_date_str:
            self._validate_date_format(end_date_str, "end_date in config")

        start_dt = pd.to_datetime(start_date_str) if start_date_str else None
        end_dt = pd.to_datetime(end_date_str) if end_date_str else None

        data_file = Path(data_cfg['file'])
        if not data_file.is_absolute():
            data_file = self.project_root / data_file
        
        self.df_full = self._load_csv_with_cache(data_file, 'full', None, None)

        if start_dt or end_dt:
            mask = pd.Series(True, index=self.df_full.index)
            if start_dt:
                mask &= (self.df_full.index >= start_dt)
            if end_dt:
                mask &= (self.df_full.index <= end_dt)
            self.df_strategy = self.df_full[mask].copy(deep=True)
        else:
            self.df_strategy = self.df_full.copy(deep=True)

        self.df_htf = None
        if 'file_htf' in data_cfg:
            htf_file = Path(data_cfg['file_htf'])
            if not htf_file.is_absolute():
                htf_file = self.project_root / htf_file
            # Cache with date range (smaller files, date filtering saves memory)
            self.df_htf = self._load_csv_with_cache(htf_file, 'htf', start_dt, end_dt)
        
        self.df_ltf = None
        if 'file_ltf' in data_cfg:
            ltf_file = Path(data_cfg['file_ltf'])
            if not ltf_file.is_absolute():
                ltf_file = self.project_root / ltf_file
            # Cache with date range (smaller files, date filtering saves memory)
            self.df_ltf = self._load_csv_with_cache(ltf_file, 'ltf', start_dt, end_dt)
        
        return self.df_full, self.df_strategy, self.df_htf, self.df_ltf

    def get_data_info(self) -> Dict:
        info = {
            'full_bars': len(self.df_full) if self.df_full is not None else 0,
            'strategy_bars': len(self.df_strategy) if self.df_strategy is not None else 0,
            'htf_bars': len(self.df_htf) if self.df_htf is not None else 0,
            'ltf_bars': len(self.df_ltf) if self.df_ltf is not None else 0,
            'date_range': (
                self.df_strategy.index.min().strftime('%Y-%m-%d %H:%M:%S') if self.df_strategy is not None else None,
                self.df_strategy.index.max().strftime('%Y-%m-%d %H:%M:%S') if self.df_strategy is not None else None
            )
        }
        if self.df_ltf is not None:
            info['ltf_tf'] = self.config.get('data', {}).get('ltf_timeframe', '1s')
        return info

    def validate_data(self) -> Dict:
        validation = {
            'has_data': len(self.df_strategy) > 0 if self.df_strategy is not None else False,
            'ohlc_columns': all(col in self.df_strategy.columns for col in ['open', 'high', 'low', 'close']) if self.df_strategy is not None else False,
            'no_nan': not self.df_strategy[['open', 'high', 'low', 'close']].isnull().any().any() if self.df_strategy is not None else False,
            'positive_prices': (self.df_strategy[['open', 'high', 'low', 'close']] > 0).all().all() if self.df_strategy is not None else False,
            'high_low_valid': (self.df_strategy['high'] >= self.df_strategy['low']).all() if self.df_strategy is not None else False,
            'open_close_valid': (
                (self.df_strategy['open'] >= self.df_strategy['low']) & 
                (self.df_strategy['open'] <= self.df_strategy['high']) &
                (self.df_strategy['close'] >= self.df_strategy['low']) & 
                (self.df_strategy['close'] <= self.df_strategy['high'])
            ).all() if self.df_strategy is not None else False
        }

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

        validation['is_valid'] = all(v for k, v in validation.items() if k != 'is_valid')
        return validation

    def get_cache_stats(self) -> Dict:
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
        cache_files = list(self.cache_dir.glob(f"{pattern}.pkl"))
        deleted = 0
        for cache_file in cache_files:
            try:
                cache_file.unlink()
                deleted += 1
            except Exception as e:
                logger.warning(f"Could not delete {cache_file.name}: {e}")
        
        logger.info(f"Cleared {deleted} cache files")
        return deleted
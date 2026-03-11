"""
CSV-based storage for trading journal with deduplication.
"""
import pandas as pd
from pathlib import Path
from typing import List, Optional
from loguru import logger

# Fix this import - remove 'src'
from broker_support.models.trade import Trade


class CSVJournal:
    """Manages trade journal storage in CSV format."""
    
    def __init__(self, filepath: Path):
        self.filepath = Path(filepath)
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        
    def _load_existing(self) -> pd.DataFrame:
        """Load existing journal entries."""
        if self.filepath.exists():
            return pd.read_csv(self.filepath)
        return pd.DataFrame()
    
    def _get_existing_ids(self) -> set:
        """Get set of existing trade IDs."""
        df = self._load_existing()
        if not df.empty and 'trade_id' in df.columns:
            return set(df['trade_id'].astype(str))
        return set()
    
    def append_trades(self, trades: List[Trade]) -> int:
        """
        Append new trades to journal, avoiding duplicates.
        
        Returns:
            Number of new trades added
        """
        if not trades:
            logger.info("No trades to append")
            return 0
        
        # Convert trades to DataFrame
        new_df = pd.DataFrame([t.model_dump() for t in trades])
        
        # Load existing and check for duplicates
        existing_ids = self._get_existing_ids()
        new_df = new_df[~new_df['trade_id'].astype(str).isin(existing_ids)]
        
        if new_df.empty:
            logger.info("No new unique trades to add")
            return 0
        
        # Append to CSV
        new_df.to_csv(
            self.filepath,
            mode='a',
            header=not self.filepath.exists(),
            index=False
        )
        
        logger.success(f"Added {len(new_df)} new trades to journal")
        return len(new_df)
    
    def load_all(self) -> pd.DataFrame:
        """Load all journal entries."""
        return self._load_existing()
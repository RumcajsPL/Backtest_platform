"""
CSV-based storage for the trading journal with deduplication.

Append-only. trade_id is the deduplication key.
Thread-safety: single-process use only (no file locking).
"""
from pathlib import Path
from typing import List

import pandas as pd
from loguru import logger

from src.broker_support.models.trade import Trade


class CSVJournal:
    """Manages closed-trade journal storage in CSV format."""

    def __init__(self, filepath: Path) -> None:
        self.filepath = Path(filepath)
        self.filepath.parent.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _file_has_content(self) -> bool:
        """Return True if the file exists and has at least one byte."""
        return self.filepath.exists() and self.filepath.stat().st_size > 0

    def _load_existing(self) -> pd.DataFrame:
        """Load journal from disk. Returns empty DataFrame if file absent."""
        if self._file_has_content():
            return pd.read_csv(self.filepath)
        return pd.DataFrame()

    def _get_existing_ids(self) -> set:
        """Return set of trade_id strings already in the journal."""
        df = self._load_existing()
        if not df.empty and 'trade_id' in df.columns:
            return set(df['trade_id'].astype(str))
        return set()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def append_trades(self, trades: List[Trade]) -> int:
        """
        Append new trades to the journal, skipping duplicates.

        Returns:
            Number of new unique trades written.
        """
        if not trades:
            logger.info("append_trades: nothing to write.")
            return 0

        new_df = pd.DataFrame([t.model_dump() for t in trades])
        existing_ids = self._get_existing_ids()
        new_df = new_df[~new_df['trade_id'].astype(str).isin(existing_ids)]

        if new_df.empty:
            logger.info("append_trades: all trades already present — nothing written.")
            return 0

        # Write header only when the file has no content yet.
        new_df.to_csv(
            self.filepath,
            mode='a',
            header=not self._file_has_content(),
            index=False,
        )
        logger.info(f"append_trades: wrote {len(new_df)} new trades to {self.filepath}.")
        return len(new_df)

    def load_all(self) -> pd.DataFrame:
        """Return all journal entries as a DataFrame."""
        return self._load_existing()
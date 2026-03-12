"""
Unit tests for CSVJournal.

Run:  pytest tests/broker_support/test_csv_journal.py -v
"""
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.broker_support.models.trade import Trade
from src.broker_support.tracking.csv_journal import CSVJournal

# Import path utilities
from src.utils.paths import journal_path, ensure_dir


def _make_trade(position_id: str, profit: float = 10.0) -> Trade:
    return Trade(
        trade_id=position_id,
        instrument_id=1002,
        is_buy=True,
        open_time=datetime(2026, 1, 15, 9, 0, tzinfo=timezone.utc),
        close_time=datetime(2026, 1, 15, 16, 0, tzinfo=timezone.utc),
        entry_price=21800.0,
        exit_price=22100.0,
        volume=1000.0,
        profit_loss=profit,
    )


class TestCSVJournal:

    def test_append_to_empty_journal(self, tmp_path):
        # Use tmp_path for tests, but in real code you'd use: journal_path("trades.csv")
        journal = CSVJournal(tmp_path / "trades.csv")
        trades = [_make_trade('1001'), _make_trade('1002')]
        written = journal.append_trades(trades)
        assert written == 2

    # ... rest of the tests remain the same, using tmp_path

    def test_deduplication(self, tmp_path):
        journal = CSVJournal(tmp_path / "trades.csv")
        journal.append_trades([_make_trade('1001')])
        written = journal.append_trades([_make_trade('1001')])
        assert written == 0

    def test_partial_deduplication(self, tmp_path):
        journal = CSVJournal(tmp_path / "trades.csv")
        journal.append_trades([_make_trade('1001')])
        written = journal.append_trades([_make_trade('1001'), _make_trade('1002')])
        assert written == 1

    def test_load_all_returns_all_rows(self, tmp_path):
        journal = CSVJournal(tmp_path / "trades.csv")
        journal.append_trades([_make_trade('1001'), _make_trade('1002'), _make_trade('1003')])
        df = journal.load_all()
        assert len(df) == 3

    def test_empty_list_is_noop(self, tmp_path):
        journal = CSVJournal(tmp_path / "trades.csv")
        written = journal.append_trades([])
        assert written == 0
        assert not journal.filepath.exists() or journal.filepath.stat().st_size == 0

    def test_header_written_once(self, tmp_path):
        """Appending in two batches must not write the header twice."""
        journal = CSVJournal(tmp_path / "trades.csv")
        journal.append_trades([_make_trade('1001')])
        journal.append_trades([_make_trade('1002')])
        df = journal.load_all()
        assert len(df) == 2
        # Verify no duplicate header rows
        assert 'trade_id' not in df['trade_id'].values

    def test_load_all_on_missing_file(self, tmp_path):
        journal = CSVJournal(tmp_path / "nonexistent.csv")
        df = journal.load_all()
        assert df.empty
"""
Unit tests for PositionTracker — no real API calls.

Run:  pytest tests/broker_support/test_position_tracker.py -v
"""
import shutil
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from src.broker_support.tracking.position_tracker import PositionTracker

# Import path utilities
from src.utils.paths import journal_path, snapshots_path, ensure_dir


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tracker(tmp_path):
    """PositionTracker with isolated temp directories."""
    # Use tmp_path for tests, but in real code you'd use the path utilities
    journal_path_tmp = tmp_path / "journal.csv"
    snapshots_dir_tmp = tmp_path / "snapshots"
    snapshots_dir_tmp.mkdir()
    # Avoid hitting the real API — we override fetch_current_positions in tests
    return PositionTracker(journal_path=journal_path_tmp, snapshots_dir=snapshots_dir_tmp)


# ---------------------------------------------------------------------------
# detect_closed_positions
# ---------------------------------------------------------------------------

class TestDetectClosedPositions:

    def test_empty_old_snapshot_returns_empty(self, tracker):
        closed = tracker.detect_closed_positions(pd.DataFrame(), [])
        assert closed == []

    def test_no_change_returns_empty(self, tracker):
        old = pd.DataFrame([
            {'positionId': '123', 'instrumentId': 1002, 'amount': 100},
            {'positionId': '456', 'instrumentId': 1003, 'amount': 200},
        ])
        new = [
            {'positionId': '123', 'instrumentId': 1002},
            {'positionId': '456', 'instrumentId': 1003},
        ]
        assert tracker.detect_closed_positions(old, new) == []

    def test_one_position_closed(self, tracker):
        old = pd.DataFrame([
            {'positionId': '123', 'instrumentId': 1002, 'amount': 100},
            {'positionId': '456', 'instrumentId': 1003, 'amount': 200},
        ])
        new = [{'positionId': '456', 'instrumentId': 1003}]
        closed = tracker.detect_closed_positions(old, new)
        assert len(closed) == 1
        assert str(closed[0]['positionId']) == '123'

    def test_all_positions_closed(self, tracker):
        old = pd.DataFrame([
            {'positionId': '111', 'instrumentId': 1002, 'amount': 100},
            {'positionId': '222', 'instrumentId': 1003, 'amount': 200},
            {'positionId': '333', 'instrumentId': 1004, 'amount': 300},
        ])
        closed = tracker.detect_closed_positions(old, [])
        assert len(closed) == 3
        closed_ids = {str(c['positionId']) for c in closed}
        assert closed_ids == {'111', '222', '333'}

    def test_integer_position_ids_handled(self, tracker):
        """positionId may arrive as int from API — must not cause set-diff failure."""
        old = pd.DataFrame([{'positionId': 123, 'instrumentId': 1002, 'amount': 100}])
        new = []
        closed = tracker.detect_closed_positions(old, new)
        assert len(closed) == 1


# ---------------------------------------------------------------------------
# convert_to_trade
# ---------------------------------------------------------------------------

class TestConvertToTrade:

    def test_buy_position(self, tracker):
        pos = {
            'positionId': 9001,
            'instrumentId': 1002,
            'isBuy': True,
            'openDateTime': '2026-03-01T09:00:00Z',
            'openRate': 22100.5,
            'amount': 500.0,
            'units': 0.022,
        }
        trade = tracker.convert_to_trade(pos)
        assert trade is not None
        assert trade.trade_id == '9001'
        assert trade.direction == 'BUY'
        assert trade.instrument_id == 1002
        assert trade.entry_price == 22100.5
        assert trade.volume == 500.0

    def test_sell_position(self, tracker):
        pos = {
            'positionId': 9002,
            'instrumentId': 1002,
            'isBuy': False,
            'openDateTime': '2026-03-01T09:00:00Z',
            'openRate': 22200.0,
            'amount': 300.0,
            'units': 0.013,
        }
        trade = tracker.convert_to_trade(pos)
        assert trade is not None
        assert trade.direction == 'SELL'

    def test_placeholder_exit_fields(self, tracker):
        """exit_price and profit_loss must be 0.0 until Step 3 enrichment."""
        pos = {
            'positionId': 9003,
            'instrumentId': 1002,
            'isBuy': True,
            'openDateTime': '2026-03-01T09:00:00Z',
            'openRate': 22000.0,
            'amount': 100.0,
            'units': 0.004,
        }
        trade = tracker.convert_to_trade(pos)
        assert trade.exit_price == 0.0
        assert trade.profit_loss == 0.0
        assert trade.instrument is None  # populated by InstrumentResolver (Step 2)

    def test_malformed_position_returns_none(self, tracker):
        """Missing required fields must not crash — return None."""
        trade = tracker.convert_to_trade({'positionId': 'bad'})
        assert trade is None


# ---------------------------------------------------------------------------
# Snapshot persistence
# ---------------------------------------------------------------------------

class TestSnapshotPersistence:

    def test_save_and_reload(self, tracker):
        positions = [
            {'positionId': '10', 'instrumentId': 1002, 'isBuy': True,
             'openRate': 100.0, 'amount': 500.0, 'openDateTime': '2026-01-01T00:00:00Z',
             'units': 0.005, 'stopLossRate': 0.0, 'takeProfitRate': 0.0},
        ]
        tracker.save_snapshot(positions)
        loaded = tracker.load_last_snapshot()
        assert len(loaded) == 1
        assert loaded['positionId'].astype(str).iloc[0] == '10'

    def test_empty_snapshot_roundtrip(self, tracker):
        tracker.save_snapshot([])
        loaded = tracker.load_last_snapshot()
        assert loaded.empty

    def test_no_snapshot_returns_empty(self, tracker):
        """Before first cycle there is no snapshot file."""
        loaded = tracker.load_last_snapshot()
        assert loaded.empty
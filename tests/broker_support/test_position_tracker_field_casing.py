"""
Unit tests for PositionTracker field casing (PascalCase vs camelCase ID fields).

These tests verify that the tracker correctly handles:
- Portfolio API responses with PascalCase positionID/instrumentID
- Snapshot-reloaded row dicts with camelCase positionId/instrumentId

Run:  pytest tests/broker_support/test_position_tracker_field_casing.py -v
"""
import pandas as pd
import pytest

from src.broker_support.tracking.position_tracker import PositionTracker


@pytest.fixture
def tracker(tmp_path):
    journal_path_tmp = tmp_path / "journal.csv"
    snapshots_dir_tmp = tmp_path / "snapshots"
    snapshots_dir_tmp.mkdir()
    return PositionTracker(journal_path=journal_path_tmp, snapshots_dir=snapshots_dir_tmp)


class TestSaveSnapshotFieldCasing:
    """Tests for save_snapshot() handling PascalCase vs camelCase position IDs."""

    def test_save_snapshot_with_portfolio_pascal_case(self, tracker):
        """Portfolio API returns positionID (capital ID) — snapshot must store correctly."""
        positions = [
            {
                'positionID': '5001',
                'instrumentID': 2001,
                'isBuy': True,
                'openRate': 150.0,
                'amount': 1000.0,
                'openDateTime': '2026-04-01T10:00:00Z',
                'units': 0.01,
                'stopLossRate': 0.0,
                'takeProfitRate': 0.0,
            },
        ]
        tracker.save_snapshot(positions)
        loaded = tracker.load_last_snapshot()

        assert len(loaded) == 1
        assert loaded['positionId'].astype(str).iloc[0] == '5001'

    def test_save_snapshot_with_snapshot_camel_case(self, tracker):
        """Snapshot-reloaded rows use positionId (lowercase d) — backward compat."""
        positions = [
            {
                'positionId': '6002',
                'instrumentId': 2002,
                'isBuy': False,
                'openRate': 160.0,
                'amount': 800.0,
                'openDateTime': '2026-04-02T11:00:00Z',
                'units': 0.008,
                'stopLossRate': 0.0,
                'takeProfitRate': 0.0,
            },
        ]
        tracker.save_snapshot(positions)
        loaded = tracker.load_last_snapshot()

        assert len(loaded) == 1
        assert loaded['positionId'].astype(str).iloc[0] == '6002'

    def test_save_snapshot_integer_position_id_from_portfolio(self, tracker):
        """positionID may be int from API — must convert to string."""
        positions = [
            {
                'positionID': 7003,
                'instrumentID': 2003,
                'isBuy': True,
                'openRate': 170.0,
                'amount': 1200.0,
                'openDateTime': '2026-04-03T12:00:00Z',
                'units': 0.012,
            },
        ]
        tracker.save_snapshot(positions)
        loaded = tracker.load_last_snapshot()

        assert loaded['positionId'].astype(str).iloc[0] == '7003'


class TestConvertToTradeFieldCasing:
    """Tests for convert_to_trade() handling PascalCase vs camelCase ID fields."""

    def test_convert_to_trade_with_portfolio_pascal_case(self, tracker):
        """Portfolio API returns positionID/instrumentID — Trade must get correct values."""
        position = {
            'positionID': '8001',
            'instrumentID': 3001,
            'isBuy': True,
            'openDateTime': '2026-05-01T08:00:00Z',
            'openRate': 25000.0,
            'amount': 500.0,
            'units': 0.02,
        }
        trade = tracker.convert_to_trade(position)

        assert trade is not None
        assert trade.trade_id == '8001'
        assert trade.instrument_id == 3001

    def test_convert_to_trade_with_snapshot_camel_case(self, tracker):
        """Snapshot-reloaded row uses positionId/instrumentId — Trade must get correct values."""
        position = {
            'positionId': '8002',
            'instrumentId': 3002,
            'isBuy': False,
            'openDateTime': '2026-05-02T09:00:00Z',
            'openRate': 26000.0,
            'amount': 400.0,
            'units': 0.015,
        }
        trade = tracker.convert_to_trade(position)

        assert trade is not None
        assert trade.trade_id == '8002'
        assert trade.instrument_id == 3002

    def test_convert_to_trade_integer_position_id_from_portfolio(self, tracker):
        """positionID may be int from API — must convert to string correctly."""
        position = {
            'positionID': 9005,
            'instrumentID': 4001,
            'isBuy': True,
            'openDateTime': '2026-05-03T10:00:00Z',
            'openRate': 27000.0,
            'amount': 600.0,
            'units': 0.022,
        }
        trade = tracker.convert_to_trade(position)

        assert trade is not None
        assert trade.trade_id == '9005'
        assert trade.instrument_id == 4001

    def test_convert_to_trade_integer_instrument_id_from_portfolio(self, tracker):
        """instrumentID may be int from API — must convert correctly."""
        position = {
            'positionID': '9006',
            'instrumentID': 5001,
            'isBuy': True,
            'openDateTime': '2026-05-04T11:00:00Z',
            'openRate': 28000.0,
            'amount': 700.0,
            'units': 0.025,
        }
        trade = tracker.convert_to_trade(position)

        assert trade is not None
        assert trade.instrument_id == 5001


class TestEndToEndFieldCasing:
    """Integration tests spanning detect_closed_positions with mixed casing."""

    def test_detect_closed_with_portfolio_pascal_case(self, tracker):
        """New positions from portfolio use positionID — detection must work."""
        old = pd.DataFrame([
            {'positionId': '1001', 'instrumentId': 5001, 'amount': 100},
        ])
        new = [
            {'positionID': '1001', 'instrumentID': 5001},
            {'positionID': '1002', 'instrumentID': 5002},
        ]
        closed = tracker.detect_closed_positions(old, new)

        assert len(closed) == 1
        assert str(closed[0]['positionId']) == '1001'
"""
Unit tests for PositionTracker without hitting real API.
"""
import pytest
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import shutil

from broker_support.tracker.position_tracker import PositionTracker
from broker_support.models.trade import Trade


class TestPositionTracker:
    """Test suite for PositionTracker logic."""
    
    @pytest.fixture
    def setup(self):
        """Create temporary directories for testing."""
        self.temp_dir = tempfile.mkdtemp()
        self.journal_path = Path(self.temp_dir) / "test_journal.csv"
        self.snapshots_dir = Path(self.temp_dir) / "snapshots"
        self.snapshots_dir.mkdir()
        
        # We'll mock the client later
        yield
        
        # Cleanup
        shutil.rmtree(self.temp_dir)
    
    def test_detect_closed_positions_empty(self, setup):
        """Test with no positions."""
        tracker = PositionTracker(self.journal_path, self.snapshots_dir)
        
        old = pd.DataFrame()
        new = []
        
        closed = tracker.detect_closed_positions(old, new)
        assert len(closed) == 0
    
    def test_detect_closed_positions_no_change(self, setup):
        """Test when positions haven't changed."""
        tracker = PositionTracker(self.journal_path, self.snapshots_dir)
        
        old = pd.DataFrame([
            {'positionId': '123', 'instrumentId': 1002, 'amount': 100},
            {'positionId': '456', 'instrumentId': 1003, 'amount': 200}
        ])
        
        new = [
            {'positionId': '123', 'instrumentId': 1002, 'amount': 100},
            {'positionId': '456', 'instrumentId': 1003, 'amount': 200}
        ]
        
        closed = tracker.detect_closed_positions(old, new)
        assert len(closed) == 0
    
    def test_detect_closed_positions_one_closed(self, setup):
        """Test when one position closes."""
        tracker = PositionTracker(self.journal_path, self.snapshots_dir)
        
        old = pd.DataFrame([
            {'positionId': '123', 'instrumentId': 1002, 'amount': 100},
            {'positionId': '456', 'instrumentId': 1003, 'amount': 200}
        ])
        
        new = [
            {'positionId': '456', 'instrumentId': 1003, 'amount': 200}
        ]
        
        closed = tracker.detect_closed_positions(old, new)
        assert len(closed) == 1
        assert closed[0]['positionId'] == '123'
    
    def test_detect_closed_positions_multiple_closed(self, setup):
        """Test when multiple positions close."""
        tracker = PositionTracker(self.journal_path, self.snapshots_dir)
        
        old = pd.DataFrame([
            {'positionId': '123', 'instrumentId': 1002, 'amount': 100},
            {'positionId': '456', 'instrumentId': 1003, 'amount': 200},
            {'positionId': '789', 'instrumentId': 1004, 'amount': 300}
        ])
        
        new = [
            {'positionId': '456', 'instrumentId': 1003, 'amount': 200}
        ]
        
        closed = tracker.detect_closed_positions(old, new)
        assert len(closed) == 2
        closed_ids = [p['positionId'] for p in closed]
        assert '123' in closed_ids
        assert '789' in closed_ids
    
    def test_convert_to_trade_buy(self, setup):
        """Test converting a BUY position to Trade object."""
        tracker = PositionTracker(self.journal_path, self.snapshots_dir)
        
        position = {
            'positionId': 123456,
            'instrumentId': 1002,
            'isBuy': True,
            'openDateTime': '2024-02-15T10:30:00Z',
            'openRate': 1.2345,
            'amount': 1000
        }
        
        trade = tracker.convert_to_trade(position)
        assert trade is not None
        assert trade.trade_id == '123456'
        assert trade.direction == 'BUY'
        assert trade.entry_price == 1.2345
        assert trade.volume == 1000
    
    def test_convert_to_trade_sell(self, setup):
        """Test converting a SELL position to Trade object."""
        tracker = PositionTracker(self.journal_path, self.snapshots_dir)
        
        position = {
            'positionId': 123456,
            'instrumentId': 1002,
            'isBuy': False,
            'openDateTime': '2024-02-15T10:30:00Z',
            'openRate': 1.2345,
            'amount': 1000
        }
        
        trade = tracker.convert_to_trade(position)
        assert trade is not None
        assert trade.trade_id == '123456'
        assert trade.direction == 'SELL'
        assert trade.entry_price == 1.2345
    
    def test_save_and_load_snapshot(self, setup):
        """Test saving and loading snapshots."""
        tracker = PositionTracker(self.journal_path, self.snapshots_dir)
        
        positions = [
            {'positionId': '123', 'instrumentId': 1002, 'amount': 100},
            {'positionId': '456', 'instrumentId': 1003, 'amount': 200}
        ]
        
        # Save snapshot
        tracker.save_snapshot(positions)
        assert tracker.last_snapshot_file.exists()
        
        # Load snapshot
        loaded = tracker.load_last_snapshot()
        assert len(loaded) == 2
        
        # Convert to string for comparison (fix the type issue)
        loaded_ids = loaded['positionId'].astype(str).values
        assert '123' in loaded_ids
        assert '456' in loaded_ids
    
    def test_empty_snapshot_handling(self, setup):
        """Test handling empty snapshots."""
        tracker = PositionTracker(self.journal_path, self.snapshots_dir)
        
        # Save empty snapshot
        tracker.save_snapshot([])
        assert tracker.last_snapshot_file.exists()
        
        # Load should return empty DataFrame, not None
        loaded = tracker.load_last_snapshot()
        assert loaded.empty
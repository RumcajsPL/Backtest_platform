"""
Real-time position tracker for eToro demo account.
Detects when positions open and close by comparing snapshots.
"""
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Optional
import pandas as pd
from loguru import logger

from broker_support.api.client import EToroClient
from broker_support.models.trade import Trade
from broker_support.storage.csv_journal import CSVJournal


class PositionTracker:
    """Tracks open positions and detects when they close."""
    
    def __init__(self, journal_path: Path, snapshots_dir: Path):
        """
        Initialize tracker.
        
        Args:
            journal_path: Path to CSV journal file for closed trades
            snapshots_dir: Directory to store position snapshots
        """
        self.client = EToroClient()
        self.journal = CSVJournal(journal_path)
        self.snapshots_dir = Path(snapshots_dir)
        self.snapshots_dir.mkdir(parents=True, exist_ok=True)
        
        # File to store last known open positions
        self.last_snapshot_file = self.snapshots_dir / "last_positions.csv"
        
    def fetch_current_positions(self) -> List[Dict]:
        """
        Fetch current open positions from portfolio.
        
        Returns:
            List of open positions with all details
        """
        portfolio = self.client.get_portfolio()
        positions = portfolio.get('positions', [])
        logger.info(f"Fetched {len(positions)} open positions")
        return positions
    
    def save_snapshot(self, positions: List[Dict]):
        """Save current positions as snapshot."""
        if not positions:
            df = pd.DataFrame()
        else:
            # Ensure positionId is string
            for pos in positions:
                if 'positionId' in pos:
                    pos['positionId'] = str(pos['positionId'])
            df = pd.DataFrame(positions)
        
        # Add timestamp
        df['snapshot_timestamp'] = datetime.now().isoformat()
        df.to_csv(self.last_snapshot_file, index=False)
        logger.debug(f"Saved snapshot with {len(positions)} positions")

    def load_last_snapshot(self) -> pd.DataFrame:
        """Load the last known positions snapshot."""
        if self.last_snapshot_file.exists():
            df = pd.read_csv(self.last_snapshot_file)
            # Ensure positionId is string
            if 'positionId' in df.columns:
                df['positionId'] = df['positionId'].astype(str)
            return df
        return pd.DataFrame()

    def detect_closed_positions(self, old_positions: pd.DataFrame, 
                            new_positions: List[Dict]) -> List[Dict]:
        """
        Detect which positions closed between snapshots.
        """
        if old_positions.empty:
            return []
        
        # Ensure all IDs are strings
        old_ids = set(str(pid) for pid in old_positions['positionId'].values)
        new_ids = set(str(p.get('positionId')) for p in new_positions)
        
        # Closed positions are those in old but not in new
        closed_ids = old_ids - new_ids
        
        if not closed_ids:
            return []
        
        # Get full details of closed positions from old snapshot
        closed_positions = []
        for pos_id in closed_ids:
            # Convert both to string for safe comparison
            mask = old_positions['positionId'].astype(str) == str(pos_id)
            pos_data = old_positions[mask]
            if not pos_data.empty:
                closed_positions.append(pos_data.iloc[0].to_dict())
        
        logger.info(f"Detected {len(closed_positions)} closed positions")
        return closed_positions
    
    def convert_to_trade(self, position: Dict) -> Optional[Trade]:
        """
        Convert a closed position to a Trade object.
        
        Note: When a position closes, we need to fetch its final details.
        The position dict from snapshot has open details, but we need close info.
        """
        try:
            # For now, we'll create a trade with available data
            # In a real implementation, you might need to fetch trade details
            # from a history endpoint or calculate from position data
            
            # Determine direction
            direction = "BUY" if position.get('isBuy') else "SELL"
            
            # Parse timestamps
            open_time = datetime.fromisoformat(
                position.get('openDateTime', '').replace('Z', '+00:00')
            )
            
            # Note: We don't have close info yet
            # This will be populated when we detect closure
            trade = Trade(
                trade_id=str(position.get('positionId')),
                instrument=str(position.get('instrumentId')),  # Need mapping to symbol
                direction=direction,
                open_time=open_time,
                close_time=datetime.now(),  # Approximate close time
                entry_price=float(position.get('openRate', 0)),
                exit_price=0.0,  # Unknown yet
                volume=float(position.get('amount', 0)),
                profit_loss=0.0,  # Unknown yet
                profit_loss_currency='USD'
            )
            return trade
        except Exception as e:
            logger.error(f"Failed to convert position to trade: {e}")
            return None
    
    def track(self) -> int:
        """
        Main tracking function: fetch current positions, detect changes,
        and record closed trades.
        
        Returns:
            Number of new closed trades recorded
        """
        logger.info("Starting position tracking cycle")
        
        # Load previous snapshot
        old_positions = self.load_last_snapshot()
        
        # Fetch current positions
        current_positions = self.fetch_current_positions()
        
        # Detect closed positions
        closed = self.detect_closed_positions(old_positions, current_positions)
        
        # Convert closed positions to trades and save
        new_trades = []
        for pos_data in closed:
            trade = self.convert_to_trade(pos_data)
            if trade:
                new_trades.append(trade)
        
        if new_trades:
            added = self.journal.append_trades(new_trades)
            logger.success(f"Recorded {added} new closed trades")
        else:
            logger.info("No new closed trades detected")
        
        # Save current snapshot for next time
        self.save_snapshot(current_positions)
        
        return len(new_trades)
"""
PositionTracker — detects closed positions by comparing portfolio snapshots.

Workflow per cycle:
  1. load_last_snapshot()   — load previous open positions from disk
  2. fetch_current_positions() — call GET /trading/info/demo/pnl
  3. detect_closed_positions() — set-difference on positionId
  4. convert_to_trade()     — build Trade objects (exit_price/profit_loss incomplete
                              until Step 3 trade enricher is implemented)
  5. journal.append_trades() — deduplicated write to CSV
  6. save_snapshot()        — persist current state for next cycle

NOTE: exit_price and profit_loss are set to 0.0 at detection time.
      They will be populated by TradeEnricher (Step 3) once the empirical
      demo history test confirms which enrichment path is available.
"""
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from loguru import logger

from src.broker_support.client.client import EToroClient
from src.broker_support.models.trade import Trade
from src.broker_support.tracking.csv_journal import CSVJournal
from src.broker_support.enrichment.trade_enricher import TradeEnricher


class PositionTracker:
    """Snapshot-comparison tracker for eToro demo open positions."""

    def __init__(
        self,
        journal_path: Path,
        snapshots_dir: Path,
        instrument_map_path: Optional[Path] = None,
    ) -> None:
        """
        Args:
            journal_path:        Path to closed-trades CSV journal.
            snapshots_dir:       Directory for position snapshots.
            instrument_map_path: Path to instrument_map.yaml for enrichment.
                                 Defaults to configs/broker_support/instrument_map.yaml.
        """
        self.client = EToroClient()
        self.journal = CSVJournal(journal_path)
        self.snapshots_dir = Path(snapshots_dir)
        self.snapshots_dir.mkdir(parents=True, exist_ok=True)
        self._snapshot_file = self.snapshots_dir / "last_positions.csv"

        if instrument_map_path is None:
            instrument_map_path = Path("configs/broker_support/instrument_map.yaml")
        self._enricher = TradeEnricher(instrument_map_path)

    # ------------------------------------------------------------------
    # Portfolio fetch
    # ------------------------------------------------------------------

    def fetch_current_positions(self) -> List[Dict]:
        """
        Return current open positions as a list of raw dicts.
        Delegates to EToroClient.get_portfolio().
        """
        portfolio = self.client.get_portfolio()
        positions = portfolio.get('positions', [])
        logger.info(f"Fetched {len(positions)} open positions.")
        return positions

    # ------------------------------------------------------------------
    # Snapshot persistence
    # ------------------------------------------------------------------

    def save_snapshot(self, positions: List[Dict]) -> None:
        """Persist current positions to disk for comparison on next cycle."""
        if positions:
            for p in positions:
                p['positionId'] = str(p.get('positionId', ''))
            df = pd.DataFrame(positions)
        else:
            df = pd.DataFrame()

        df['snapshot_timestamp'] = datetime.now(tz=timezone.utc).isoformat()
        df.to_csv(self._snapshot_file, index=False)
        logger.debug(f"Snapshot saved: {len(positions)} positions.")

    def load_last_snapshot(self) -> pd.DataFrame:
        """Load the most recent position snapshot. Returns empty DataFrame if none."""
        if self._snapshot_file.exists() and self._snapshot_file.stat().st_size > 0:
            df = pd.read_csv(self._snapshot_file)
            if 'positionId' in df.columns:
                df['positionId'] = df['positionId'].astype(str)
            return df
        return pd.DataFrame()

    # ------------------------------------------------------------------
    # Closure detection — core logic, do not modify
    # ------------------------------------------------------------------

    def detect_closed_positions(
        self,
        old_positions: pd.DataFrame,
        new_positions: List[Dict],
    ) -> List[Dict]:
        """
        Return positions present in old_positions but absent from new_positions.

        Args:
            old_positions: DataFrame loaded from last snapshot.
            new_positions: List of raw position dicts from current portfolio fetch.

        Returns:
            List of position dicts that have closed since last snapshot.
        """
        if old_positions.empty:
            return []

        old_ids: set = set(old_positions['positionId'].astype(str).values)
        new_ids: set = {str(p.get('positionId', '')) for p in new_positions}
        closed_ids = old_ids - new_ids

        if not closed_ids:
            return []

        closed: List[Dict] = []
        for pid in closed_ids:
            mask = old_positions['positionId'].astype(str) == pid
            row = old_positions[mask]
            if not row.empty:
                closed.append(row.iloc[0].to_dict())

        logger.info(f"Detected {len(closed)} closed position(s).")
        return closed

    # ------------------------------------------------------------------
    # Trade construction
    # ------------------------------------------------------------------

    def convert_to_trade(self, position: Dict) -> Optional[Trade]:
        """
        Construct a Trade from a closed-position snapshot dict.

        exit_price and profit_loss are placeholder 0.0 values — they will be
        populated by TradeEnricher once Step 3 enrichment path is confirmed.
        instrument is set to None — populated by InstrumentResolver (Step 2).
        """
        try:
            direction = 'BUY' if position.get('isBuy') else 'SELL'
            raw_dt = position.get('openDateTime', '')
            open_time = datetime.fromisoformat(raw_dt.replace('Z', '+00:00'))

            trade = Trade.model_validate(
                {
                    'positionId': str(position.get('positionId', '')),
                    'instrumentId': int(position.get('instrumentId', 0)),
                    'isBuy': position.get('isBuy', True),
                    'openTimestamp': open_time.isoformat(),
                    'closeTimestamp': datetime.now(tz=timezone.utc).isoformat(),
                    'openRate': float(position.get('openRate', 0.0)),
                    'closeRate': 0.0,       # unknown at snapshot — Step 3 enrichment
                    'investment': float(position.get('amount', 0.0)),
                    'units': float(position.get('units', 0.0)),
                    'netProfit': 0.0,       # unknown at snapshot — Step 3 enrichment
                }
            )
            return trade
        except Exception as exc:
            logger.error(
                f"convert_to_trade failed for positionId="
                f"{position.get('positionId')}: {exc}"
            )
            return None

    # ------------------------------------------------------------------
    # Main tracking cycle
    # ------------------------------------------------------------------

    def track(self) -> int:
        """
        Execute one complete tracking cycle.

        Returns:
            Number of new closed trades written to the journal.
        """
        logger.info("=== PositionTracker cycle start ===")

        old_positions = self.load_last_snapshot()
        current_positions = self.fetch_current_positions()
        closed = self.detect_closed_positions(old_positions, current_positions)

        new_trades: List[Trade] = []
        for pos in closed:
            trade = self.convert_to_trade(pos)
            if trade:
                trade = self._enricher.enrich(trade)
                new_trades.append(trade)

        written = 0
        if new_trades:
            written = self.journal.append_trades(new_trades)

        self.save_snapshot(current_positions)
        logger.info(f"=== PositionTracker cycle complete — {written} new trade(s) written ===")
        return written
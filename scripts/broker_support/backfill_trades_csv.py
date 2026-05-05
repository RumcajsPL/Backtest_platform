"""
One-time backfill script to reconstruct trades.csv for CTP instances.

Queries eToro closed trade history API and writes authoritative trade records
for all positionIDs that belong to each CTP instance (as recorded in
open_positions.json), replacing the corrupted records that resulted from
the PascalCase field mismatch bug.

Usage:
    python scripts/broker_support/backfill_trades_csv.py --dry-run
    python scripts/broker_support/backfill_trades_csv.py --instance c424 --instance 240166
    python scripts/broker_support/backfill_trades_csv.py  # live run for all instances
"""
import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Set

import pandas as pd
from loguru import logger

from src.broker_support.client.client import EToroClient
from src.broker_support.enrichment.instrument_resolver import InstrumentResolver
from src.broker_support.models.trade import Trade
from src.broker_support.tracking.csv_journal import CSVJournal


INSTANCE_PATHS = {
    'c424': Path('outputs/broker_support/journal/c424'),
    '240166': Path('outputs/broker_support/journal/240166'),
    '7ffbc5': Path('outputs/broker_support/journal/7ffbc5'),
    '61875': Path('outputs/broker_support/journal/61875'),
}

DEFAULT_DAYS_BACK = 29
MAX_PAGES = 10


def load_position_ids(instance_path: Path) -> Set[int]:
    """Load CTP positionIDs from open_positions.json."""
    pos_file = instance_path / 'open_positions.json'
    if not pos_file.exists():
        logger.warning(f"open_positions.json not found at {pos_file} — skipping instance.")
        return set()

    with open(pos_file, encoding='utf-8') as f:
        data = json.load(f)

    ids = data.get('position_ids', [])
    if not ids:
        logger.warning(f"open_positions.json is empty at {pos_file} — skipping instance.")
        return set()

    return set(ids)


def fetch_all_closed_trades(client: EToroClient, days_back: int = DEFAULT_DAYS_BACK) -> List[dict]:
    """Paginate through trade history API, collecting all trades within the window."""
    from_date = datetime.now(timezone.utc) - timedelta(days=days_back)
    all_trades = []

    try:
        for page in range(1, MAX_PAGES + 1):
            trades = client.fetch_closed_trades(from_date=from_date, page=page)
            if not trades:
                break
            all_trades.extend(trades)
            logger.debug(f"Page {page}: fetched {len(trades)} trades.")
    except Exception as exc:
        logger.error(f"Failed to fetch closed trades from API: {exc}")
        logger.error("Check that minDate is within the 29-day exclusive window (30 days → 403).")
        logger.error("Endpoint: GET /api/v1/trading/info/trade/history")
        return []

    logger.info(f"Fetched {len(all_trades)} total closed trades from API.")
    return all_trades


def filter_ctp_trades(api_trades: List[dict], ctp_position_ids: Set[int]) -> List[dict]:
    """Filter API trades to only those with positionId in the CTP set."""
    filtered = []
    for trade in api_trades:
        pos_id = trade.get('positionId')
        if pos_id is not None and pos_id in ctp_position_ids:
            filtered.append(trade)

    logger.info(f"Filtered to {len(filtered)} CTP trades (of {len(api_trades)} total).")
    return filtered


def build_trade_from_history(api_trade: dict, instrument_resolver: InstrumentResolver) -> Trade:
    """
    Construct a Trade from an API history record.

    Maps history fields to Trade model aliases:
      positionId → trade_id (alias)
      instrumentId → instrument_id
      openDate → openTimestamp
      closeDate → closeTimestamp
      isBuy → is_buy
      closeRate → exit_price
      netProfit → profit_loss
    """
    open_date = api_trade.get('openDate', '')
    close_date = api_trade.get('closeDate', '')

    open_timestamp = _parse_datetime(open_date)
    close_timestamp = _parse_datetime(close_date)

    trade_data = {
        'positionId': str(api_trade.get('positionId', '')),
        'instrumentId': int(api_trade.get('instrumentId', 0)),
        'isBuy': api_trade.get('isBuy', True),
        'openTimestamp': open_timestamp.isoformat() if open_timestamp else '',
        'closeTimestamp': close_timestamp.isoformat() if close_timestamp else '',
        'openRate': float(api_trade.get('openRate', 0.0) or 0.0),
        'closeRate': float(api_trade.get('closeRate', 0.0) or 0.0),
        'investment': float(api_trade.get('amount', 0.0) or 0.0),
        'units': float(api_trade.get('units', 0.0) or 0.0),
        'netProfit': float(api_trade.get('netProfit', 0.0) or 0.0),
    }

    if 'fees' in api_trade and api_trade['fees'] is not None:
        trade_data['fees'] = float(api_trade['fees'])
    if 'leverage' in api_trade and api_trade['leverage'] is not None:
        trade_data['leverage'] = int(api_trade['leverage'])
    if 'stopLossRate' in api_trade and api_trade['stopLossRate'] is not None:
        trade_data['stopLossRate'] = float(api_trade['stopLossRate'])
    if 'takeProfitRate' in api_trade and api_trade['takeProfitRate'] is not None:
        trade_data['takeProfitRate'] = float(api_trade['takeProfitRate'])

    trade = Trade.model_validate(trade_data)

    instrument_id = trade.instrument_id
    symbol = instrument_resolver.symbol(instrument_id)
    trade = trade.model_copy(update={'instrument': symbol})

    return trade


def _parse_datetime(date_str: str) -> datetime:
    """Parse various datetime formats from eToro API."""
    if not date_str:
        return datetime.now(tz=timezone.utc)

    date_str = date_str.replace('Z', '+00:00')

    try:
        return datetime.fromisoformat(date_str)
    except ValueError:
        pass

    for fmt in ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%d']:
        try:
            return datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    return datetime.now(tz=timezone.utc)


def has_corrupted_records(journal_path: Path) -> bool:
    """Check if trades.csv contains corrupted records (trade_id='nan')."""
    if not journal_path.exists():
        return False

    df = pd.read_csv(journal_path)
    if df.empty:
        return False

    if 'trade_id' not in df.columns:
        return False

    trade_ids = df['trade_id'].astype(str)
    return trade_ids.isin(['nan', 'NaN', '']).any() or trade_ids.str.lower().eq('nan').any()


def process_instance(
    instance_id: str,
    instance_path: Path,
    client: EToroClient,
    instrument_resolver: InstrumentResolver,
    dry_run: bool,
    all_api_trades: List[dict],
) -> dict:
    """Process a single instance: load position IDs, filter trades, write to CSV."""
    result = {
        'instance': instance_id,
        'position_ids_in_config': 0,
        'found_in_api': 0,
        'not_found_in_api': [],
        'written': 0,
        'corrupted_removed': False,
    }

    ctp_position_ids = load_position_ids(instance_path)
    result['position_ids_in_config'] = len(ctp_position_ids)

    if not ctp_position_ids:
        logger.warning(f"[{instance_id}] No CTP positionIDs to process — skipping.")
        return result

    ctp_trades = filter_ctp_trades(all_api_trades, ctp_position_ids)
    result['found_in_api'] = len(ctp_trades)

    found_ids = set()
    for trade in ctp_trades:
        pid = trade.get('positionId')
        if pid is not None:
            found_ids.add(pid)

    not_found = ctp_position_ids - found_ids
    result['not_found_in_api'] = sorted(not_found)

    for pid in not_found:
        logger.warning(f"[{instance_id}] positionID={pid} not found in API — may be outside 30-day window or still open")

    trades_to_write: List[Trade] = []
    for api_trade in ctp_trades:
        try:
            trade = build_trade_from_history(api_trade, instrument_resolver)

            if trade.trade_id in ('nan', 'NaN', '') or str(trade.trade_id).lower() == 'nan':
                raise ValueError(f"Refusing to write trade with trade_id='nan'")
            if trade.instrument and trade.instrument.startswith('UNKNOWN_'):
                raise ValueError(f"Refusing to write trade with instrument='UNKNOWN_*'")

            trades_to_write.append(trade)
        except Exception as exc:
            logger.error(f"[{instance_id}] Failed to build trade for positionId={api_trade.get('positionId')}: {exc}")

    trades_csv = instance_path / 'trades.csv'

    if has_corrupted_records(trades_csv):
        logger.info(f"[{instance_id}] Removing corrupted trades.csv")
        if not dry_run:
            trades_csv.unlink()
        result['corrupted_removed'] = True
    else:
        logger.info(f"[{instance_id}] No corrupted records found in trades.csv")

    if not trades_to_write:
        logger.info(f"[{instance_id}] No valid trades to write.")
        return result

    if dry_run:
        logger.info(f"[DRY-RUN] [{instance_id}] Would write {len(trades_to_write)} trades:")
        for t in trades_to_write:
            print(f"  - trade_id={t.trade_id}, instrument={t.instrument}, "
                  f"entry={t.entry_price}, exit={t.exit_price}, profit={t.profit_loss}")
    else:
        journal = CSVJournal(trades_csv)
        written = journal.append_trades(trades_to_write)
        result['written'] = written
        logger.info(f"[{instance_id}] Wrote {written} trades to trades.csv")

    return result


def main() -> None:
    parser = argparse.ArgumentParser(description='Backfill trades.csv for CTP instances.')
    parser.add_argument(
        '--instance',
        action='append',
        dest='instances',
        help='Instance ID to process (default: all four)',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Print what would be done without writing anything',
    )
    parser.add_argument(
        '--days-back',
        type=int,
        default=DEFAULT_DAYS_BACK,
        help=f'Days of history to fetch from API (default: {DEFAULT_DAYS_BACK})',
    )

    args = parser.parse_args()

    instances = args.instances if args.instances else list(INSTANCE_PATHS.keys())

    logger.info(f"Starting backfill for instances: {instances}")
    if args.dry_run:
        logger.info("Running in DRY-RUN mode — no files will be modified.")

    client = EToroClient()
    instrument_map_path = Path('configs/broker_support/instrument_map.yaml')
    instrument_resolver = InstrumentResolver(instrument_map_path)

    logger.info(f"Fetching closed trades from API (last {args.days_back} days)...")
    all_api_trades = fetch_all_closed_trades(client, days_back=args.days_back)

    results = []
    for inst_id in instances:
        if inst_id not in INSTANCE_PATHS:
            logger.error(f"Unknown instance: {inst_id}")
            continue

        inst_path = INSTANCE_PATHS[inst_id]
        result = process_instance(
            inst_id,
            inst_path,
            client,
            instrument_resolver,
            args.dry_run,
            all_api_trades,
        )
        results.append(result)

    print("\n" + "=" * 60)
    print("BACKFILL SUMMARY")
    print("=" * 60)
    for r in results:
        print(f"\nInstance: {r['instance']}")
        print(f"  PositionIDs in config:     {r['position_ids_in_config']}")
        print(f"  Found in API:              {r['found_in_api']}")
        print(f"  Not found in API:          {len(r['not_found_in_api'])}")
        if r['not_found_in_api']:
            print(f"    Missing IDs: {r['not_found_in_api']}")
        print(f"  Corrupted records removed: {r['corrupted_removed']}")
        print(f"  Trades written:            {r['written']}")

    print("\n" + "=" * 60)

    if args.dry_run:
        print("DRY-RUN complete — no files were modified.")
    else:
        print("Backfill complete.")


if __name__ == '__main__':
    main()
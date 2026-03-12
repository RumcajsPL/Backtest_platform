"""
broker-support CLI entry point.
Registered as console_scripts: broker-support = broker_support.cli:main
"""
from datetime import datetime, timedelta
from pathlib import Path

import click
from loguru import logger

from src.broker_support.config.settings import settings
from src.broker_support.client.client import EToroClient
from src.broker_support.models.trade import Trade
from src.broker_support.tracking.csv_journal import CSVJournal


def _configure_logging(log_dir: str) -> None:
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    logger.add(
        str(log_path / "broker_support_{time}.log"),
        rotation="10 MB",
        retention="30 days",
    )


@click.group()
def main() -> None:
    """CTP broker-support — eToro demo paper trading journal."""
    _configure_logging(settings.log_dir)


@main.command()
def test_connection() -> None:
    """Test API connectivity."""
    client = EToroClient()
    if client.test_connection():
        click.echo(click.style("Connection OK.", fg="green"))
    else:
        click.echo(click.style("Connection failed. Check logs.", fg="red"))


@main.command()
@click.option('--days', default=settings.default_days_back, show_default=True,
              help='Days of history to fetch.')
@click.option('--output', default='outputs/broker_support/journal/trades.csv',
              show_default=True, help='Output CSV path.')
def fetch_trades(days: int, output: str) -> None:
    """Fetch closed trades from the real-account history endpoint."""
    client = EToroClient()
    journal = CSVJournal(Path(output))

    from_date = datetime.now() - timedelta(days=days)
    raw_trades = client.fetch_closed_trades(from_date=from_date)

    if not raw_trades:
        click.echo("No trades returned. Check the empirical demo history test result.")
        return

    trades = []
    for raw in raw_trades:
        try:
            trades.append(Trade.model_validate(raw))
        except Exception as exc:
            logger.warning(f"Skipping invalid trade {raw.get('positionId', '?')}: {exc}")

    new_count = journal.append_trades(trades)
    click.echo(click.style(f"Wrote {new_count} new trades to {output}.", fg="green"))


@main.command()
@click.option('--journal', default='outputs/broker_support/journal/trades.csv',
              show_default=True)
def show_metrics(journal: str) -> None:
    """Display basic performance metrics from the journal."""
    df = CSVJournal(Path(journal)).load_all()

    if df.empty:
        click.echo("Journal is empty.")
        return

    total = len(df)
    wins = (df['profit_loss'] > 0).sum()
    win_rate = wins / total * 100
    total_pl = df['profit_loss'].sum()
    avg_pl = df['profit_loss'].mean()

    click.echo("\n=== Trading Performance ===")
    click.echo(f"Total trades:   {total}")
    click.echo(f"Win rate:       {win_rate:.1f}%  ({wins}/{total})")
    click.echo(f"Total P/L:      ${total_pl:.2f}")
    click.echo(f"Average P/L:    ${avg_pl:.2f}")

    best = df.loc[df['profit_loss'].idxmax()]
    worst = df.loc[df['profit_loss'].idxmin()]
    click.echo(f"Best trade:     ${best['profit_loss']:.2f} (instrumentId={best.get('instrument_id', '?')})")
    click.echo(f"Worst trade:    ${worst['profit_loss']:.2f} (instrumentId={worst.get('instrument_id', '?')})")


if __name__ == '__main__':
    main()
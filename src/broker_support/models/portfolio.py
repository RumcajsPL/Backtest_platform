"""
Command-line interface for broker-support.
"""
import click
from datetime import datetime, timedelta
from pathlib import Path
from loguru import logger

# Fix these imports - use broker_support prefix
from broker_support.config.settings import settings
from broker_support.api.client import EToroClient
from broker_support.models.trade import Trade
from broker_support.storage.csv_journal import CSVJournal


@click.group()
def main():
    """Automated Trading Journal for eToro Demo Account."""
    # Create log directory if it doesn't exist
    log_path = Path(settings.log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    
    # Configure logging
    logger.add(
        str(log_path / "broker_support_{time}.log"),
        rotation="10 MB",
        retention="30 days"
    )


@main.command()
def test_connection():
    """Test API connectivity."""
    try:
        client = EToroClient()
        if client.test_connection():
            click.echo(click.style("✅ Connection successful!", fg="green"))
        else:
            click.echo(click.style("❌ Connection failed. Check logs.", fg="red"))
    except Exception as e:
        click.echo(click.style(f"❌ Error: {e}", fg="red"))
        logger.error(f"Connection test failed: {e}")


@main.command()
@click.option('--days', default=30, help='Number of days back to fetch')
@click.option('--output', default='data/trading_journal.csv', help='Output CSV path')
def fetch_trades(days, output):
    """Fetch closed trades from demo account."""
    try:
        client = EToroClient()
        journal = CSVJournal(Path(output))
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Fetch trades from API
        raw_trades = client.fetch_closed_trades(start_date, end_date)
        
        if not raw_trades:
            click.echo("No trades found in the specified period.")
            return
        
        # Validate and transform
        trades = []
        for raw_trade in raw_trades:
            try:
                trade = Trade.model_validate(raw_trade)
                trades.append(trade)
            except Exception as e:
                logger.warning(f"Failed to validate trade: {raw_trade.get('id', 'unknown')} - {e}")
        
        # Store in journal
        new_count = journal.append_trades(trades)
        
        click.echo(click.style(f"✅ Added {new_count} new trades to {output}", fg="green"))
        
    except Exception as e:
        logger.error(f"Failed to fetch trades: {e}")
        click.echo(click.style(f"❌ Error: {e}", fg="red"))


@main.command()
@click.option('--journal', default='data/trading_journal.csv', help='Journal CSV path')
def show_metrics(journal):
    """Display basic performance metrics."""
    try:
        journal_mgr = CSVJournal(Path(journal))
        df = journal_mgr.load_all()
        
        if df.empty:
            click.echo("No trades in journal.")
            return
        
        # Calculate basic metrics
        total_trades = len(df)
        winning_trades = len(df[df['profit_loss'] > 0])
        win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0
        total_pl = df['profit_loss'].sum()
        avg_pl = df['profit_loss'].mean()
        
        # Display metrics
        click.echo("\n📊 Trading Performance Metrics")
        click.echo("=" * 40)
        click.echo(f"Total Trades:      {total_trades}")
        click.echo(f"Winning Trades:    {winning_trades}")
        click.echo(f"Win Rate:          {win_rate:.1f}%")
        click.echo(f"Total P/L:         ${total_pl:.2f}")
        click.echo(f"Average P/L:       ${avg_pl:.2f}")
        
        # Top performers
        if total_trades > 0:
            best_trade = df.loc[df['profit_loss'].idxmax()]
            worst_trade = df.loc[df['profit_loss'].idxmin()]
            click.echo(f"\nBest Trade:        ${best_trade['profit_loss']:.2f} ({best_trade['instrument']})")
            click.echo(f"Worst Trade:       ${worst_trade['profit_loss']:.2f} ({worst_trade['instrument']})")
            
    except Exception as e:
        click.echo(click.style(f"❌ Error calculating metrics: {e}", fg="red"))
        logger.error(f"Metrics calculation failed: {e}")


if __name__ == '__main__':
    main()
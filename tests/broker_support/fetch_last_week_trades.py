# fetch_last_week_trades.py
from broker_support.api.client import EToroClient
from datetime import datetime, timedelta
import json
from pathlib import Path

def main():
    # Initialize client
    client = EToroClient()
    
    # Calculate date range: last 7 days
    to_date = datetime.now()
    from_date = to_date - timedelta(days=7)
    
    print(f"📅 Fetching closed trades from {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}")
    
    # Fetch trades
    trades = client.fetch_closed_trades(from_date, to_date)
    
    if not trades:
        print("❌ No closed trades found in the last week")
        print("\nThis could mean:")
        print("  • You haven't closed any trades in this period")
        print("  • The endpoint needs adjustment")
        print("  • Your demo account has no trading history")
        return
    
    print(f"\n✅ Found {len(trades)} closed trades")
    
    # Save raw response for inspection
    output_file = Path("data/last_week_trades.json")
    output_file.parent.mkdir(exist_ok=True)
    
    with open(output_file, 'w') as f:
        json.dump(trades, f, indent=2, default=str)
    print(f"💾 Saved raw data to {output_file}")
    
    # Display first trade as sample
    if trades:
        print("\n📊 Sample trade:")
        sample = trades[0]
        for key, value in sample.items():
            print(f"  {key}: {value}")
    
    # Basic stats
    if isinstance(trades, list):
        print(f"\n📈 Summary:")
        print(f"  Total trades: {len(trades)}")
        
        # Try to calculate some stats if fields are present
        profits = []
        for t in trades:
            if isinstance(t, dict):
                # Try different possible field names for profit
                profit = (t.get('netProfit') or 
                         t.get('profit') or 
                         t.get('pnl') or 
                         t.get('gain'))
                if profit is not None:
                    profits.append(float(profit))
        
        if profits:
            winning = sum(1 for p in profits if p > 0)
            losing = sum(1 for p in profits if p < 0)
            total_pnl = sum(profits)
            
            print(f"  Winning trades: {winning}")
            print(f"  Losing trades: {losing}")
            print(f"  Total P&L: ${total_pnl:.2f}")
            if winning > 0:
                win_rate = (winning / len(profits)) * 100
                print(f"  Win rate: {win_rate:.1f}%")

if __name__ == "__main__":
    main()
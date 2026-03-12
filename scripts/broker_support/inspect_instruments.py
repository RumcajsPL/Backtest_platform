#!/usr/bin/env python
"""
Instrument ID lookup — searches for instruments by symbol.
Run: python scripts/broker_support/inspect_instruments.py

Confirms DAX instrumentId and any other instruments you trade.
Results feed into configs/broker_support/instrument_map.yaml (Step 2).
"""
import json
from src.broker_support.client.client import EToroClient

# Add any other symbols you trade on eToro demo
SYMBOLS_TO_CHECK = [
    'GER40',    # DAX — likely instrumentId=32
    'DE30',     # Alternative DAX symbol on eToro
    'DAX',      # Another possible name
    'SPX500',   # S&P 500
    'NAS100',   # Nasdaq
    'US30',     # Dow Jones
    'UK100',    # FTSE
    'EUR/USD',  # EUR/USD forex
    'Gold',     # Gold
    'Oil',      # Oil
]

def main():
    client = EToroClient()

    print("\n=== Instrument ID Lookup ===\n")

    # Also directly check instrumentId=32 from the sample trade
    print("--- Checking instrumentId=32 directly (from sample trade) ---")
    try:
        result = client._make_request(
            'GET',
            'api/v1/market-data/instruments',
            params={'instrumentIds': '32'}
        )
        instruments = result if isinstance(result, list) else result.get('instruments', result)
        if instruments:
            print(json.dumps(instruments[0] if isinstance(instruments, list) else instruments,
                           indent=2, default=str))
        else:
            print(f"Raw response: {json.dumps(result, indent=2, default=str)}")
    except Exception as e:
        print(f"Failed: {e}")

    print("\n--- Symbol search results ---")
    for symbol in SYMBOLS_TO_CHECK:
        try:
            results = client.search_instrument(symbol)
            if results:
                for r in results[:2]:  # show top 2 matches
                    iid = r.get('instrumentId') or r.get('InstrumentID', '?')
                    name = r.get('displayname') or r.get('internalSymbolFull', '?')
                    print(f"  {symbol:12s} → id={iid:6}  name={name}")
            else:
                print(f"  {symbol:12s} → no results")
        except Exception as e:
            print(f"  {symbol:12s} → error: {e}")

if __name__ == "__main__":
    main()
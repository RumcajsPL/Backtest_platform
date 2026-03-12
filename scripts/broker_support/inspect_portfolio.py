#!/usr/bin/env python
"""
Quick portfolio inspection — shows raw field names from live API response.
Run: python scripts/broker_support/inspect_portfolio.py
"""
import json
from src.broker_support.client.client import EToroClient

def main():
    client = EToroClient()
    portfolio = client.get_portfolio()

    credit = portfolio.get('credit')
    positions = portfolio.get('positions', [])
    orders = portfolio.get('orders', [])

    print(f"\nCredit:     {credit}")
    print(f"Positions:  {len(positions)}")
    print(f"Orders:     {len(orders)}")

    if positions:
        print("\n--- First open position (raw fields) ---")
        print(json.dumps(positions[0], indent=2, default=str))
        print("\n--- All field names ---")
        for k, v in sorted(positions[0].items()):
            print(f"  {k}: {type(v).__name__}")
    else:
        print("\nNo open positions currently.")
        print("(Open a demo trade on eToro and re-run to see position fields.)")

if __name__ == "__main__":
    main()
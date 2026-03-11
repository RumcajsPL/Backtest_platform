# test_portfolio.py
from broker_support.api.client import EToroClient
import json

client = EToroClient()
portfolio = client.get_portfolio()

print(f"Credit: {portfolio.get('credit')}")
print(f"Open positions: {len(portfolio.get('positions', []))}")
print(f"Orders: {len(portfolio.get('orders', []))}")
print(f"Mirrors: {len(portfolio.get('mirrors', []))}")

# Show first position as example
if portfolio.get('positions'):
    print("\nSample position:")
    print(json.dumps(portfolio['positions'][0], indent=2))
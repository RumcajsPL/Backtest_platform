#!/usr/bin/env python
"""
Demo portfolio endpoint probe — finds the actual working endpoint.
Run: python scripts/broker_support/probe_demo_endpoints.py
"""
import json
from src.broker_support.client.client import EToroClient

ENDPOINTS_TO_TRY = [
    # PnL variants
    'api/v1/trading/info/demo/pnl',
    'api/v1/trading/info/real/pnl',
    # Portfolio variants
    'api/v1/trading/info/demo/portfolio',
    'api/v1/trading/info/portfolio',
    # Other plausible patterns
    'api/v1/trading/info/demo/account',
    'api/v1/trading/info/demo/balance',
    'api/v1/trading/info/demo/positions',
    'api/v1/trading/info/demo/open-positions',
]

def main():
    client = EToroClient()
    print("\n=== Demo Portfolio Endpoint Probe ===\n")

    for endpoint in ENDPOINTS_TO_TRY:
        try:
            result = client._make_request('GET', endpoint)
            print(f"  ✅ {endpoint}")
            if isinstance(result, dict):
                print(f"     Keys: {list(result.keys())}")
                # If it looks like a portfolio, show more
                if 'clientPortfolio' in result:
                    cp = result['clientPortfolio']
                    print(f"     credit={cp.get('credit')}  positions={len(cp.get('positions',[]))}")
                elif 'positions' in result:
                    print(f"     positions={len(result.get('positions', []))}")
            elif isinstance(result, list):
                print(f"     List of {len(result)} items")
            print()
        except Exception as e:
            status = '403' if '403' in str(e) else ('404' if '404' in str(e) else str(e)[:40])
            print(f"  ❌ {endpoint}  ({status})")

if __name__ == "__main__":
    main()
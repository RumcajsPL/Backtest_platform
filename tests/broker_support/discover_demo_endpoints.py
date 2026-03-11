from broker_support.api.client import EToroClient
from datetime import datetime

client = EToroClient()

# Base path that works
base = 'api/v1/trading/info/demo'

# Resources to try
resources = [
    'history',
    'trades',
    'trade-history',
    'trade/history',
    'transactions',
    'portfolio/history',
    'positions/history',
    'positions/closed',
    'account/history',
    'statement',
]

print("🔍 Discovering demo endpoints...\n")

for resource in resources:
    endpoint = f"{base}/{resource}"
    print(f"Trying: {endpoint}")
    
    try:
        # Try without params first
        result = client._make_request('GET', endpoint)
        print(f"  ✅ SUCCESS!")
        if isinstance(result, list):
            print(f"  Found {len(result)} items")
            if result:
                print(f"  Sample keys: {list(result[0].keys()) if isinstance(result[0], dict) else 'not dict'}")
        elif isinstance(result, dict):
            print(f"  Response keys: {list(result.keys())}")
        break
    except Exception as e:
        print(f"  ❌ Failed: {type(e).__name__}")
        continue

print("\n" + "="*50)
print("\nIf none worked, try with date parameters:")
print("client._make_request('GET', endpoint, params={'fromDate': '2024-01-01T00:00:00Z'})")
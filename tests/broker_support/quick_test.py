# Quick test
from broker_support.api.client import EToroClient
client = EToroClient()

# Try the real account endpoint (maybe it works for demo too?)
try:
    result = client._make_request('GET', 'api/v1/trading/info/trade/history')
    print("✅ Real account endpoint worked for demo!")
except Exception as e:
    print(f"❌ Failed: {e}")
# tests/test_demo_history.py
from broker_support.api.client import EToroClient
from datetime import datetime, timedelta
import json

def main():
    client = EToroClient()
    
    # Try a longer period (last 90 days)
    to_date = datetime.now()
    from_date = to_date - timedelta(days=90)
    
    print(f"📅 Testing with date range: {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}")
    
    # Test the most promising endpoint first
    endpoint = 'api/v1/trading/info/demo/trade/history'
    
    try:
        print(f"\n🔍 Testing endpoint: {endpoint}")
        result = client._make_request('GET', endpoint, params={
            'fromDate': from_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            'toDate': to_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            'pageSize': 100
        })
        
        print(f"✅ Success! Response type: {type(result)}")
        
        if isinstance(result, list):
            print(f"Found {len(result)} trades")
            if result:
                print("\n📊 Sample trade:")
                print(json.dumps(result[0], indent=2))
        elif isinstance(result, dict):
            print(f"Response keys: {list(result.keys())}")
            if 'data' in result:
                print(f"Found {len(result['data'])} trades in data field")
            elif 'trades' in result:
                print(f"Found {len(result['trades'])} trades in trades field")
        
    except Exception as e:
        print(f"❌ Failed: {e}")
        
        # Try without date params
        try:
            print("\n🔍 Testing without date parameters...")
            result = client._make_request('GET', endpoint)
            print(f"✅ Success without dates! Response: {type(result)}")
        except Exception as e2:
            print(f"❌ Also failed: {e2}")

if __name__ == "__main__":
    main()
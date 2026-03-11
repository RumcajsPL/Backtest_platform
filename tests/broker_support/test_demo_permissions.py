from broker_support.api.client import EToroClient

client = EToroClient()

# These should all return 403 if they exist (good) or 404 if they don't
endpoints = [
    'api/v1/trading/info/demo/trades',
    'api/v1/trading/info/demo/transactions',
    'api/v1/trading/info/demo/history',
    'api/v1/trading/info/demo/positions/closed',
    'api/v1/trading/info/demo/portfolio/history',
    'api/v1/trading/info/demo/account/history',
]

print("🔍 Testing for endpoints that exist (expect 403 if found):\n")

for endpoint in endpoints:
    try:
        client._make_request('GET', endpoint)
        print(f"✅ {endpoint} - SUCCESS (unexpected!)")
    except Exception as e:
        # Check if it's a 403 (good - endpoint exists) or 404 (bad - doesn't exist)
        error_str = str(e)
        if "403" in error_str:
            print(f"🔐 {endpoint} - EXISTS! (403 Forbidden)")
        elif "404" in error_str:
            print(f"❌ {endpoint} - DOES NOT EXIST (404)")
        else:
            print(f"⚠️ {endpoint} - {error_str[:50]}")
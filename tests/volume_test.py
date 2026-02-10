import pandas as pd

# Load the same CSV file with both methods
csv_path = "data/processed/ohlcv/DEUIDXEUR_1min_20240101_20260207.csv"

# Method 1: Simple read (like old loader might do)
df1 = pd.read_csv(csv_path)
print(f"Method 1 (simple read):")
print(f"  Volume dtype: {df1['volume'].dtype}")
print(f"  First 5 volume values: {df1['volume'].head().tolist()}")

# Method 2: With lowercasing and timestamp parsing (like new loader)
df2 = pd.read_csv(csv_path)
df2.columns = df2.columns.str.lower()
df2["timestamp"] = pd.to_datetime(df2["timestamp"], format="%Y-%m-%d %H:%M:%S")
df2 = df2.set_index("timestamp")
print(f"\nMethod 2 (with processing):")
print(f"  Volume dtype: {df2['volume'].dtype}")
print(f"  First 5 volume values: {df2['volume'].head().tolist()}")

# Compare
print(f"\nComparison:")
print(f"  Values equal? {(df1['volume'].values == df2['volume'].values).all()}")
print(f"  Dtypes equal? {df1['volume'].dtype == df2['volume'].dtype}")
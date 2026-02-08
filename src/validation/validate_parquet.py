import pandas as pd
from pathlib import Path

def validate_parquet(path):
    path = Path(path)
    print(f"\n=== VALIDATING PARQUET FILE ===")
    print(f"File: {path}")
    print(f"Exists: {path.exists()}")
    print(f"Size: {path.stat().st_size / 1024 / 1024:.2f} MB")

    print("\n--- Reading raw Parquet metadata ---")
    try:
        import pyarrow.parquet as pq
        table = pq.read_table(path)
        print(table.schema)
    except Exception as e:
        print("Could not read metadata:", e)

    print("\n--- Loading with Pandas ---")
    try:
        df = pd.read_parquet(path)
        print("Columns:", df.columns.tolist())
        print("Index:", df.index.name)
        print("Shape:", df.shape)
    except Exception as e:
        print("Could not load with Pandas:", e)
        return

    print("\n--- HEAD (first 5 rows) ---")
    print(df.head())

    print("\n--- MIDDLE (5 rows) ---")
    mid = len(df) // 2
    print(df.iloc[mid:mid+5])

    print("\n--- TAIL (last 5 rows) ---")
    print(df.tail())

    print("\n--- TIMESTAMP CHECK ---")
    if "timestamp" in df.columns:
        print("timestamp column dtype:", df["timestamp"].dtype)
    else:
        print("No 'timestamp' column found.")

    if df.index.name:
        print("Index name:", df.index.name)
        print("Index dtype:", df.index.dtype)
    else:
        print("Index has no name.")

if __name__ == "__main__":
    validate_parquet(r"E:\Trading\Backtest_platform\data\processed\ohlcv\DEUIDXEUR_1min_20240101_20260207.parquet")
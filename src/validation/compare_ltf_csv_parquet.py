import pandas as pd
from pathlib import Path

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def load_csv(path):
    df = pd.read_csv(path)
    df.columns = df.columns.str.lower()
    df["timestamp"] = pd.to_datetime(df["timestamp"], format=DATE_FORMAT)
    df = df.set_index("timestamp").sort_index()
    return df

def load_parquet(path):
    df = pd.read_parquet(path)
    df.columns = df.columns.str.lower()

    if df.index.name == "timestamp":
        df = df.sort_index()
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        df.index = df.index.floor("s")
        df = df[~df.index.duplicated(keep="last")]
    elif "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["timestamp"])
        df["timestamp"] = df["timestamp"].dt.floor("s")
        df = df.set_index("timestamp").sort_index()
        df = df[~df.index.duplicated(keep="last")]
    else:
        raise ValueError("No timestamp in parquet")

    return df

if __name__ == "__main__":
    base = Path(r"E:\Trading\Backtest_platform\data\processed\ohlcv")

    csv_ltf = base / "DEUIDXEUR_1s_20240101_20260207.csv"
    pq_ltf  = base / "DEUIDXEUR_1s_20240101_20260207.parquet"

    df_csv = load_csv(csv_ltf)
    df_pq  = load_parquet(pq_ltf)

    print("CSV LTF bars:", len(df_csv))
    print("PQ  LTF bars:", len(df_pq))

    # Align indexes
    only_in_csv = df_csv.index.difference(df_pq.index)
    only_in_pq  = df_pq.index.difference(df_csv.index)

    print("\nOnly in CSV (first 10):")
    print(only_in_csv[:10])

    print("\nOnly in Parquet (first 10):")
    print(only_in_pq[:10])

    # Focus on strategy window
    start = "2025-12-15 08:00:00"
    end   = "2025-12-17 21:00:00"

    c_win = df_csv.loc[start:end]
    p_win = df_pq.loc[start:end]

    print("\nCSV window bars:", len(c_win))
    print("PQ  window bars:", len(p_win))

    only_in_csv_win = c_win.index.difference(p_win.index)
    only_in_pq_win  = p_win.index.difference(c_win.index)

    print("\nOnly in CSV within window:")
    print(only_in_csv_win[:20])

    print("\nOnly in Parquet within window:")
    print(only_in_pq_win[:20])
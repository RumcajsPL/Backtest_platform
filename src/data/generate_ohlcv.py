import os
import lzma
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import pytz
import yaml
import argparse
import gc
from concurrent.futures import ProcessPoolExecutor

from numba import njit

# --- CONSTANTS ---
INSTRUMENT_DIVISOR_MAP = {
    "DEUIDXEUR": 1000.0, "XAUUSD": 1000.0, "USA500IDXUSD": 1000.0,
    "USA30IDXUSD": 1000.0, "USATECHIDXUSD": 1000.0, "FRAIDXEUR": 1000.0,
    "GBRIDXGBP": 1000.0, "EURJPY": 1000.0, "USDJPY": 1000.0,
    "AUDUSD": 100000.0, "EURUSD": 100000.0, "GBPUSD": 100000.0,
    "USDCAD": 100000.0, "USDCHF": 100000.0,
}
DEFAULT_PRICE_DIVISOR = 100000.0
RAW_DATA_ROOT = "data/raw/dukascopy_bi5"
INDEX_NAME = "timestamp"
DEFAULT_CONFIG_PATH = "configs/data/data_aggregator.yaml"
OUTPUT_TIMEZONE = "Europe/Berlin"

# ------------------------------------------------------------
# Optimized Numba Decoder (Returns Arrays, not Lists)
# ------------------------------------------------------------
@njit(cache=True)
def decode_bi5_numba(raw_data, base_ts, divisor):
    n_bytes = len(raw_data)
    n_ticks = n_bytes // 20
    
    # Pre-allocate arrays
    out_ts = np.empty(n_ticks, dtype=np.float64)
    out_price = np.empty(n_ticks, dtype=np.float64)
    out_vol = np.empty(n_ticks, dtype=np.float64)
    
    count = 0
    i = 0
    div = float(divisor)
    
    while i + 20 <= n_bytes:
        # Decode big-endian integers manually
        b0, b1, b2, b3 = raw_data[i], raw_data[i+1], raw_data[i+2], raw_data[i+3]
        ms = (np.uint32(b0) << 24) | (np.uint32(b1) << 16) | (np.uint32(b2) << 8) | np.uint32(b3)

        b8, b9, b10, b11 = raw_data[i+8], raw_data[i+9], raw_data[i+10], raw_data[i+11]
        bid_int = np.int32((np.uint32(b8) << 24) | (np.uint32(b9) << 16) | (np.uint32(b10) << 8) | np.uint32(b11))

        # Ask/Bid Volumes
        v0, v1, v2, v3 = raw_data[i+12], raw_data[i+13], raw_data[i+14], raw_data[i+15]
        ask_vol = np.int32((np.uint32(v0) << 24) | (np.uint32(v1) << 16) | (np.uint32(v2) << 8) | np.uint32(v3))

        w0, w1, w2, w3 = raw_data[i+16], raw_data[i+17], raw_data[i+18], raw_data[i+19]
        bid_vol = np.int32((np.uint32(w0) << 24) | (np.uint32(w1) << 16) | (np.uint32(w2) << 8) | np.uint32(w3))

        if bid_int > 0:
            out_ts[count] = base_ts + (ms / 1000.0)
            out_price[count] = bid_int / div
            out_vol[count] = abs(ask_vol) + abs(bid_vol)
            count += 1
            
        i += 20

    return out_ts[:count], out_price[:count], out_vol[:count]

# ------------------------------------------------------------
# Worker Function (Runs in parallel process)
# ------------------------------------------------------------
def process_single_day(args):
    """
    Processes all 24 hourly files for a single day.
    Returns a resampled DataFrame for that day (or None).
    """
    instrument, day_dt, raw_root, divisor, target_tz_str, timeframe_pd = args
    
    # Storage for the whole day (list of arrays is faster than appending to DF)
    day_ts, day_prices, day_vols = [], [], []
    
    # Iterate 0-23 hours
    for hour in range(24):
        hour_dt = day_dt + timedelta(hours=hour)
        
        # Path construction
        file_path = os.path.join(
            raw_root, instrument, str(hour_dt.year),
            f"{hour_dt.month:02d}", f"{hour_dt.day:02d}",
            f"{hour_dt.hour:02d}h_ticks.bi5"
        )
        
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            try:
                with open(file_path, "rb") as f:
                    compressed = f.read()
                raw = lzma.decompress(compressed)
                raw_arr = np.frombuffer(raw, dtype=np.uint8)
                
                # Numba decode
                ts, p, v = decode_bi5_numba(raw_arr, hour_dt.timestamp(), divisor)
                
                if len(ts) > 0:
                    day_ts.append(ts)
                    day_prices.append(p)
                    day_vols.append(v)
            except Exception:
                pass # Skip corrupt files silently or log if needed

    # If no data for the whole day, return None
    if not day_ts:
        return None

    # Fast Concatenation
    full_ts = np.concatenate(day_ts)
    full_price = np.concatenate(day_prices)
    full_vol = np.concatenate(day_vols)

    # Vectorized Timezone Conversion
    # 1. Convert unix float to UTC Timestamp
    dt_index = pd.to_datetime(full_ts, unit="s", utc=True)
    # 2. Convert to Target TZ
    dt_index = dt_index.tz_convert(target_tz_str).tz_localize(None)

    # Create DataFrame ONCE per day
    df = pd.DataFrame({
        "price": full_price,
        "volume": full_vol
    }, index=dt_index)
    
    df.index.name = INDEX_NAME
    df.sort_index(inplace=True)

    # Resample ONCE per day
    ohlc = df["price"].resample(timeframe_pd).ohlc()
    vol = df["volume"].resample(timeframe_pd).sum()
    ohlc["volume"] = vol
    ohlc.dropna(inplace=True)

    return ohlc

# ------------------------------------------------------------
# Main Controller
# ------------------------------------------------------------
def generate_ohlcv_multicore(config_path: str):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    data_cfg = config["data_source"]
    out_cfg = config["output"]

    instrument = data_cfg["instrument"].upper()
    timeframe = data_cfg["timeframe"]
    target_tz_str = OUTPUT_TIMEZONE
    
    divisor = INSTRUMENT_DIVISOR_MAP.get(instrument, DEFAULT_PRICE_DIVISOR)
    
    start_date = datetime.strptime(str(data_cfg["start_date"]), "%Y-%m-%d")
    end_date = datetime.strptime(str(data_cfg["end_date"]), "%Y-%m-%d")

    # Resolve pandas timeframe
    tf_pd = timeframe.lower() if timeframe.lower().endswith("h") else timeframe

    print(f"--- Parallel OHLCV Gen: {instrument} ({timeframe}) ---")
    
    # Generate list of Days
    days_to_process = []
    curr = start_date
    while curr <= end_date:
        days_to_process.append(curr)
        curr += timedelta(days=1)

    total_days = len(days_to_process)
    print(f"Processing {total_days} days on {os.cpu_count()} cores...")

    # Prepare arguments for workers
    # (instrument, day_dt, raw_root, divisor, target_tz_str, timeframe_pd)
    worker_args = [
        (instrument, d, RAW_DATA_ROOT, divisor, target_tz_str, tf_pd) 
        for d in days_to_process
    ]

    results = []
    
    # Execute in Parallel
    with ProcessPoolExecutor() as executor:
        # Use simple counter for progress
        for i, res in enumerate(executor.map(process_single_day, worker_args)):
            if res is not None and not res.empty:
                results.append(res)
            
            # Progress print
            if i % 5 == 0 or i == total_days - 1:
                print(f"Progress: {100*(i+1)/total_days:.1f}%", end="\r")

    print("\nMerging data...")
    if not results:
        print(f"❌ No data found for {instrument}.")
        return

    final_ohlc = pd.concat(results)
    final_ohlc.sort_index(inplace=True)

    # Save
    out_dir = out_cfg["directory"]
    os.makedirs(out_dir, exist_ok=True)
    
    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")
    fmt = out_cfg["format"].lower()
    
    filename = f"{instrument}_{timeframe}_{start_str}_{end_str}.{fmt}"
    path = os.path.join(out_dir, filename)

    if fmt == "csv":
        final_ohlc.to_csv(path, float_format="%.6f")
    elif fmt == "parquet":
        final_ohlc.to_parquet(path)

    print(f"--- DONE ---")
    print(f"Total Bars: {len(final_ohlc):,}")
    print(f"Saved: {path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config_file", nargs="?", default=DEFAULT_CONFIG_PATH)
    args = parser.parse_args()
    
    # Windows/MacOS safety for multiprocessing
    try:
        generate_ohlcv_multicore(args.config_file)
    except Exception as e:
        print(f"Error: {e}")
import os
import lzma
import struct
import pandas as pd
from datetime import datetime, timedelta, timezone
import pytz
import yaml
import argparse
import gc # Garbage collection to free RAM explicitly

# --- CONSTANTS ---
# Map instruments to their correct price divisor for scaling raw integer prices.
INSTRUMENT_DIVISOR_MAP = {
    # ------------------------------------------------------------------
    # 1. Indices, Commodities & JPY Pairs (Require 1,000.0 Divisor)
    # ------------------------------------------------------------------
    "DEUIDXEUR": 1000.0,       # DAX40 (GER40)
    "XAUUSD": 1000.0,          # GOLD
    "USA500IDXUSD": 1000.0,    # SPTRD (SPX500)
    "USA30IDXUSD": 1000.0,     # DOW (DJ30)
    "USATECHIDXUSD": 1000.0,   # NASDAQ (NS100)
    "FRAIDXEUR": 1000.0,       # CAC (FR40)
    "GBRIDXGBP": 1000.0,       # UK100 (FTSE 100)
    "EURJPY": 1000.0,
    "USDJPY": 1000.0,

    # ------------------------------------------------------------------
    # 2. Standard Forex Pairs (Require 100,000.0 Divisor)
    # ------------------------------------------------------------------
    "AUDUSD": 100000.0,
    "EURUSD": 100000.0,
    "GBPUSD": 100000.0,
    "USDCAD": 100000.0,
    "USDCHF": 100000.0,
}
DEFAULT_PRICE_DIVISOR = 100000.0 

TICK_RECORD_SIZE = 20 
RAW_DATA_ROOT = "data/raw/dukascopy_bi5"
INDEX_NAME = 'timestamp'
DEFAULT_CONFIG_PATH = "configs/data_aggregator.yaml"

# --- FIXED TIMEZONE SETTINGS ---
# We output all data in CET/CEST (Europe/Berlin) as naive timestamps
# This matches TradingView environment
OUTPUT_TIMEZONE = 'Europe/Berlin'  # CET/CEST

def get_local_filepath(instrument: str, dt: datetime, base_dir: str) -> str:
    dir_path = os.path.join(
        base_dir,
        instrument.upper(),
        str(dt.year),
        f"{dt.month:02d}",
        f"{dt.day:02d}"
    )
    filename = f"{dt.hour:02d}h_ticks.bi5"
    return os.path.join(dir_path, filename)

def decode_bi5(data, hour_start_dt: datetime, divisor: float):
    ticks = []
    for i in range(0, len(data), TICK_RECORD_SIZE):
        block = data[i:i+TICK_RECORD_SIZE]
        if len(block) < TICK_RECORD_SIZE:
            continue
        try:
            ms, ask_int, bid_int, ask_vol_raw, bid_vol_raw = struct.unpack(">Iiiii", block)
        except struct.error:
            continue

        price = bid_int / divisor
        volume = abs(float(ask_vol_raw) + float(bid_vol_raw))
        t_sec = hour_start_dt.timestamp() + (ms / 1000.0)
        timestamp_utc = datetime.fromtimestamp(t_sec, tz=timezone.utc)
        
        ticks.append((timestamp_utc, price, volume))
    return ticks

def process_chunk(tick_buffer, tf_pd):
    if not tick_buffer:
        return None

    df = pd.DataFrame(tick_buffer, columns=[INDEX_NAME, "price", "volume"])
    df.drop_duplicates(subset=[INDEX_NAME, 'price'], keep='last', inplace=True)
    df.set_index(INDEX_NAME, inplace=True)
    
    ohlc = df["price"].resample(tf_pd).ohlc()
    vol = df["volume"].resample(tf_pd).sum()
    ohlc["volume"] = vol
    ohlc.dropna(inplace=True)
    
    return ohlc

def generate_ohlcv_from_bi5(config_path: str):
    # 1. Load Configuration
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"❌ ERROR: Configuration file not found at: {config_path}")
        return
    except yaml.YAMLError as e:
        print(f"❌ ERROR: YAML Error: {e}")
        return

    data_config = config['data_source']
    output_config = config['output']

    instrument = data_config['instrument'].upper()
    timeframe_str = data_config['timeframe']
    
    # Fixed: Use CET/CEST (Europe/Berlin) for output
    target_tz = pytz.timezone(OUTPUT_TIMEZONE)
    
    price_divisor = INSTRUMENT_DIVISOR_MAP.get(instrument, DEFAULT_PRICE_DIVISOR)
    
    # Ensure dates are UTC
    start_date_utc = datetime.strptime(str(data_config['start_date']), '%Y-%m-%d').replace(tzinfo=timezone.utc)
    end_date_utc = datetime.strptime(str(data_config['end_date']), '%Y-%m-%d').replace(tzinfo=timezone.utc) + timedelta(days=1)
    
    # --- FLEXIBLE TIMEFRAME SETUP ---
    if timeframe_str.endswith('min'):
        tf_pd = timeframe_str
    elif timeframe_str.lower().endswith('h'):
        tf_pd = timeframe_str.lower()
    else:
        tf_pd = timeframe_str

    print(f"--- 📊 Starting OHLCV Generation for {instrument} ({timeframe_str}) ---")
    print(f"  Period: {start_date_utc.date()} to {(end_date_utc - timedelta(days=1)).date()}")
    print(f"  Price Divisor: {price_divisor}")
    print(f"  Output Timezone: {OUTPUT_TIMEZONE} (CET/CEST, naive timestamps)")
    print(f"  Resampling Frequency: {tf_pd}")

    # 2. Iteration & Memory Management
    hours_to_process = []
    current_dt = start_date_utc
    while current_dt < end_date_utc:
        hours_to_process.append(current_dt)
        current_dt += timedelta(hours=1)
    
    total_hours = len(hours_to_process)
    
    all_ohlcv_chunks = []
    current_tick_buffer = []
    
    print(f"Total {total_hours} hourly files to process.")

    for i, hour_dt in enumerate(hours_to_process):
        file_path = get_local_filepath(instrument, hour_dt, RAW_DATA_ROOT)
        
        if i % 24 == 0:
            progress = (i + 1) / total_hours * 100
            print(f"Processing: {hour_dt.strftime('%Y-%m-%d')} | Progress: {progress:.1f}%", end='\r')
        
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            try:
                with open(file_path, 'rb') as f:
                    compressed_data = f.read()
                raw_data = lzma.decompress(compressed_data)
                ticks = decode_bi5(raw_data, hour_dt, price_divisor)
                current_tick_buffer.extend(ticks)
            except Exception:
                pass
        
        # Flush at end of day or last file
        is_end_of_day = (hour_dt.hour == 23)
        is_last_file = (i == total_hours - 1)

        if (is_end_of_day or is_last_file) and current_tick_buffer:
            chunk_ohlc = process_chunk(current_tick_buffer, tf_pd)
            
            if chunk_ohlc is not None:
                all_ohlcv_chunks.append(chunk_ohlc)
            
            current_tick_buffer.clear()
        
    print("\n")
        
    if not all_ohlcv_chunks:
        print(f"❌ No data found for {instrument}.")
        return

    # 3. Final Concatenation
    print("Combining chunks...")
    final_ohlc = pd.concat(all_ohlcv_chunks)
    final_ohlc.sort_index(inplace=True)
    
    # 4. CRITICAL: Convert to CET/CEST and remove timezone info
    print(f"Converting timestamps to {OUTPUT_TIMEZONE} (CET/CEST)...")
    
    # Convert from UTC to Europe/Berlin (handles DST automatically)
    final_ohlc.index = final_ohlc.index.tz_convert(target_tz)
    
    # Remove timezone info to get naive timestamps (in local CET/CEST time)
    final_ohlc.index = final_ohlc.index.tz_localize(None)
    
    final_ohlc.index.name = INDEX_NAME
    
    # 5. Save Output
    output_dir = output_config['directory']
    os.makedirs(output_dir, exist_ok=True)
    
    start_str = start_date_utc.strftime('%Y%m%d')
    end_str = (end_date_utc - timedelta(days=1)).strftime('%Y%m%d')
    output_format = output_config['format'].lower()
    
    filename_base = f"{instrument}_{timeframe_str}_{start_str}_{end_str}"
    path = os.path.join(output_dir, f"{filename_base}.{output_format}")
    
    if output_format == 'csv':
        # Save with naive timestamps
        final_ohlc.to_csv(path, float_format='%.6f', lineterminator='\n')
    elif output_format == 'parquet':
        final_ohlc.to_parquet(path)
    
    print(f"--- ✅ Aggregation Complete ---")
    print(f"  Bars Generated: {len(final_ohlc):,}")
    print(f"  Timezone: {OUTPUT_TIMEZONE} (CET/CEST, naive timestamps)")
    print(f"  Date Range: {final_ohlc.index.min()} to {final_ohlc.index.max()}")
    print(f"  Saved to: {path}")

if __name__ == "__main__":
    config_dir = os.path.dirname(DEFAULT_CONFIG_PATH)
    if not os.path.exists(config_dir):
        os.makedirs(config_dir, exist_ok=True)
    
    parser = argparse.ArgumentParser()
    parser.add_argument("config_file", nargs='?', default=DEFAULT_CONFIG_PATH)
    args = parser.parse_args()
    
    try:
        generate_ohlcv_from_bi5(args.config_file)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
import os
import re
from datetime import datetime, timedelta, timezone
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional 

# --- CONFIGURATION ---
BASE_URL = "https://datafeed.dukascopy.com/datafeed"
OUTPUT_DIRECTORY = "data/raw/dukascopy_bi5"
MAX_CONCURRENT_WORKERS = 50
MAX_DELTA_DAYS = 365 

# --- UTILITY: SESSION WITH RETRIES & POOLING ---

def get_requests_session(pool_size=50):
    """Sets up a requests session with retries and robust pooling."""
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=pool_size,
        pool_maxsize=pool_size
    )
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    })
    return session

# --- PATH AND URL FUNCTIONS ---

def get_bi5_url(instrument: str, dt: datetime) -> str:
    month_index = dt.month - 1
    url_path = f"{instrument.upper()}/{dt.year}/{month_index:02d}/{dt.day:02d}/{dt.hour:02d}h_ticks.bi5"
    return f"{BASE_URL}/{url_path}"

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

# --- CORE DOWNLOAD FUNCTION ---

def download_single_hour(instrument: str, dt: datetime, base_dir: str, session: requests.Session) -> str:
    local_path = get_local_filepath(instrument, dt, base_dir)
    url = get_bi5_url(instrument, dt)
    
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status() 

        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        with open(local_path, 'wb') as f:
            f.write(response.content)
            
        return f"✅ Downloaded: {local_path}"
        
    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
             return f"⚠️ Missing (404): {url}"
        return f"❌ HTTP Error {response.status_code}: {e}"
    except requests.exceptions.RequestException as e:
        return f"❌ Network Error: {e}"
    except Exception as e:
        return f"❌ Unexpected Error: {e}"

# --- OPTIMIZED DELTA FINDER LOGIC ---

def find_last_downloaded_hour_optimized(instrument: str, base_dir: str) -> Optional[datetime]:
    """
    Efficiently finds the last downloaded hour by traversing the latest 
    directory path only (Heuristic Search).
    Avoids scanning the entire disk (O(1) vs O(N)).
    """
    instrument_path = os.path.join(base_dir, instrument.upper())
    if not os.path.isdir(instrument_path):
        return None

    # 1. Find max Year
    try:
        years = [d for d in os.listdir(instrument_path) if d.isdigit() and os.path.isdir(os.path.join(instrument_path, d))]
        if not years: return None
        latest_year = max(years, key=int)
        
        # 2. Find max Month in that Year
        year_path = os.path.join(instrument_path, latest_year)
        months = [d for d in os.listdir(year_path) if d.isdigit() and os.path.isdir(os.path.join(year_path, d))]
        if not months: return None
        latest_month = max(months, key=int)
        
        # 3. Find max Day in that Month
        month_path = os.path.join(year_path, latest_month)
        days = [d for d in os.listdir(month_path) if d.isdigit() and os.path.isdir(os.path.join(month_path, d))]
        if not days: return None
        latest_day = max(days, key=int)
        
        # 4. Find max Hour file in that Day
        day_path = os.path.join(month_path, latest_day)
        files = [f for f in os.listdir(day_path) if f.endswith("h_ticks.bi5")]
        if not files: return None
        
        # Extract hours from filenames (e.g., "01h_ticks.bi5" -> 1)
        latest_hour_str = max(files, key=lambda x: int(x.split('h')[0]))
        hour = int(latest_hour_str.split('h')[0])
        
        return datetime(int(latest_year), int(latest_month), int(latest_day), hour, 0, tzinfo=timezone.utc)
    
    except Exception as e:
        print(f"⚠️ Error scanning directories: {e}")
        return None

# --- MAIN DELTA UPDATE FUNCTION ---

def update_raw_bi5_files(instrument: str, base_dir: str, max_workers: int = 50):
    """
    Finds the delta and concurrently downloads missing hourly .bi5 files.
    """
    instrument = instrument.upper()
    print(f"--- 🔄 Starting Delta Update for {instrument} ---")
    
    # Use the optimized O(1) finder
    last_dt = find_last_downloaded_hour_optimized(instrument, base_dir)
    
    if last_dt is None:
        print(f"⚠️ No existing data found for {instrument}. Use download_raw_ticks.py for a full download.")
        return

    start_date = last_dt + timedelta(hours=1)
    
    now_utc = datetime.now(timezone.utc)
    end_date = now_utc.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    
    if start_date >= end_date:
        print(f"✅ Data is up-to-date. Last file: {last_dt.strftime('%Y-%m-%d %H:%M UTC')}. Nothing to download.")
        return

    print(f"Data Found Up To: {last_dt.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Starting Download From: {start_date.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Ending Download At: {end_date.strftime('%Y-%m-%d %H:%M UTC')}")
    
    if start_date < end_date - timedelta(days=MAX_DELTA_DAYS):
        print(f"\n🛑 WARNING: The calculated delta period exceeds the {MAX_DELTA_DAYS}-day limit.")
        print("Please check your input directory structure or increase MAX_DELTA_DAYS.")
        return

    hours_to_download = []
    current_dt = start_date
    while current_dt <= end_date:
        hours_to_download.append(current_dt)
        current_dt += timedelta(hours=1)
        
    session = get_requests_session(pool_size=max_workers)
    futures_list = {}
    skipped_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for hour in hours_to_download:
            # OPTIMIZATION: Pre-check existence
            local_path = get_local_filepath(instrument, hour, base_dir)
            if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                skipped_count += 1
                continue
                
            future = executor.submit(download_single_hour, instrument, hour, base_dir, session)
            futures_list[future] = hour
        
        if skipped_count > 0:
             print(f"Skipped {skipped_count} existing files.")
             
        files_to_download = len(futures_list)
        print(f"Downloading {files_to_download} new files...")
        
        count = 0
        for future in as_completed(futures_list):
            count += 1
            try:
                # Progress update
                if count % 20 == 0 or count == files_to_download:
                    print(f"Progress: {count}/{files_to_download} ({count/files_to_download:.1%})", end='\r')
            except Exception as exc:
                print(f"\nError processing hour: {futures_list[future].strftime('%Y-%m-%d %H:%M')}, Exception: {exc}")

    print("\n--- Delta Update Complete ---")


if __name__ == "__main__":
    # --- EXAMPLE USAGE ---
    INSTRUMENT_TO_UPDATE = "USATECHIDXUSD"
    
    update_raw_bi5_files(
        instrument=INSTRUMENT_TO_UPDATE,
        base_dir=OUTPUT_DIRECTORY,
        max_workers=MAX_CONCURRENT_WORKERS
    )
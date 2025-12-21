import os
import re
from datetime import datetime, timedelta, timezone
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional # FIX: Added missing import

# --- CONFIGURATION ---
BASE_URL = "https://datafeed.dukascopy.com/datafeed"
OUTPUT_DIRECTORY = "data/raw/dukascopy_bi5"
MAX_CONCURRENT_WORKERS = 15
MAX_DELTA_DAYS = 365 # Cap the maximum delta download to prevent accidental full redownload

# --- UTILITY: SESSION WITH RETRIES (Copied from download_raw_ticks.py) ---

def get_requests_session():
    """Sets up a requests session with retries for robust downloading."""
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    })
    return session

# --- PATH AND URL FUNCTIONS ---

def get_bi5_url(instrument: str, dt: datetime) -> str:
    """Generates the URL for a specific hourly .bi5 file."""
    month_index = dt.month - 1
    url_path = f"{instrument.upper()}/{dt.year}/{month_index:02d}/{dt.day:02d}/{dt.hour:02d}h_ticks.bi5"
    return f"{BASE_URL}/{url_path}"

def get_local_filepath(instrument: str, dt: datetime, base_dir: str) -> str:
    """Generates the local file path for a specific hourly .bi5 file."""
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
    """Downloads a single hourly .bi5 file and saves it locally."""
    local_path = get_local_filepath(instrument, dt, base_dir)
    url = get_bi5_url(instrument, dt)
    
    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        return f"✅ Exists: {local_path}"

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

# --- DELTA FINDER LOGIC ---

def find_last_downloaded_hour(instrument: str, base_dir: str) -> Optional[datetime]:
    """
    Scans the local directory structure to find the datetime of the last successfully
    downloaded .bi5 file for the given instrument.
    """
    instrument_path = os.path.join(base_dir, instrument.upper())
    if not os.path.isdir(instrument_path):
        print(f"❌ Instrument directory not found: {instrument_path}")
        return None

    latest_dt = None
    
    for root, dirs, files in os.walk(instrument_path):
        for filename in files:
            if filename.endswith("h_ticks.bi5"):
                path_parts = root.split(os.sep)
                
                try:
                    hour = int(filename[:2])
                    day = int(path_parts[-1])
                    month = int(path_parts[-2])
                    year = int(path_parts[-3])
                    
                    current_dt = datetime(year, month, day, hour, 0, tzinfo=timezone.utc)
                    
                    if latest_dt is None or current_dt > latest_dt:
                        latest_dt = current_dt
                except (ValueError, IndexError):
                    continue
                    
    return latest_dt

# --- MAIN DELTA UPDATE FUNCTION ---

def update_raw_bi5_files(instrument: str, base_dir: str, max_workers: int = 15):
    """
    Finds the delta and concurrently downloads missing hourly .bi5 files.
    """
    instrument = instrument.upper()
    print(f"--- 🔄 Starting Delta Update for {instrument} ---")
    
    last_dt = find_last_downloaded_hour(instrument, base_dir)
    
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
        
    total_files = len(hours_to_download)
    print(f"Total {total_files} hourly files to download.")

    session = get_requests_session()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_hour = {
            executor.submit(download_single_hour, instrument, hour, base_dir, session): hour 
            for hour in hours_to_download
        }
        
        count = 0
        for future in as_completed(future_to_hour):
            count += 1
            try:
                # Simple progress update
                if count % (total_files // 100 + 1) == 0 or count == total_files:
                    print(f"Progress: {count}/{total_files} ({count/total_files:.1%})", end='\r')
            except Exception as exc:
                print(f"\nError processing hour: {future_to_hour[future].strftime('%Y-%m-%d %H:%M')}, Exception: {exc}")

    print("\n--- Delta Update Complete ---")


if __name__ == "__main__":
    # --- EXAMPLE USAGE ---
    INSTRUMENT_TO_UPDATE = "deuidxeur" 
    
    update_raw_bi5_files(
        instrument=INSTRUMENT_TO_UPDATE,
        base_dir=OUTPUT_DIRECTORY,
        max_workers=MAX_CONCURRENT_WORKERS
    )
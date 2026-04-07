import os
from datetime import datetime, timedelta, timezone
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURATION ---

BASE_URL = "https://datafeed.dukascopy.com/datafeed"

# --- UTILITY: SESSION WITH RETRIES & POOLING ---

def get_requests_session(pool_size=50):
    """
    Sets up a requests session with retries and increased connection pool size.
    This ensures that 50 threads can actually have 50 open connections.
    """
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    
    # OPTIMIZATION: Match pool_connections and pool_maxsize to your max_workers
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
    # We construct the path again here to ensure safety, though it's slightly redundant
    local_path = get_local_filepath(instrument, dt, base_dir)
    url = get_bi5_url(instrument, dt)
    
    # Redundant check removed here because we do it in the main loop now for speed

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

def download_and_save_bi5_files(instrument: str, start_date: datetime, end_date: datetime, output_dir: str, max_workers: int = 50):
    """
    Concurrently downloads raw Dukascopy .bi5 tick files for a date range.
    """
    print(f"--- Starting Concurrent Download for {instrument} ---")
    print(f"Period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    # Ensure start/end dates are hour-aligned UTC
    start = start_date.replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    end = end_date.replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    
    # Generate list of all hourly datetimes to download
    hours_to_download = []
    current_dt = start
    while current_dt <= end:
        hours_to_download.append(current_dt)
        current_dt += timedelta(hours=1)
        
    total_files = len(hours_to_download)
    print(f"Total range covers {total_files} hours.")

    # Pass max_workers to session to size the pool correctly
    session = get_requests_session(pool_size=max_workers)
    
    futures_list = {}
    skipped_count = 0
    
    print("Checking existing files (this might take a moment)...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for hour in hours_to_download:
            # OPTIMIZATION: Check existence BEFORE submitting the thread.
            # This saves massive overhead on "resume" runs.
            local_path = get_local_filepath(instrument, hour, output_dir)
            if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                skipped_count += 1
                continue
            
            # Only submit if file doesn't exist
            future = executor.submit(download_single_hour, instrument, hour, output_dir, session)
            futures_list[future] = hour

        if skipped_count > 0:
            print(f"Skipped {skipped_count} existing files.")
        
        files_to_download = len(futures_list)
        print(f"Starting download for {files_to_download} new files with {max_workers} workers...")
        
        count = 0
        for future in as_completed(futures_list):
            count += 1
            try:
                result = future.result()
                # Progress update
                if count % 100 == 0 or count == files_to_download:
                    print(f"Progress: {count}/{files_to_download} ({count/files_to_download:.1%})", end='\r')
            except Exception as exc:
                print(f"\nError processing hour: {futures_list[future].strftime('%Y-%m-%d %H:%M')}, Exception: {exc}")

    print("\n--- Download Complete ---")
    
if __name__ == "__main__":
    # --- Set up PARAMETERS ---
    INSTRUMENT = "USATECHIDXUSD"
    START_DATE = datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc)
    END_DATE = datetime(2023, 12, 31, 23, 0, tzinfo=timezone.utc) 
    OUTPUT_DIRECTORY = "data/raw/dukascopy_bi5" 
    MAX_CONCURRENT_WORKERS = 50 # Increased default
    
    download_and_save_bi5_files(
        instrument=INSTRUMENT,
        start_date=START_DATE,
        end_date=END_DATE,
        output_dir=OUTPUT_DIRECTORY,
        max_workers=MAX_CONCURRENT_WORKERS
    )
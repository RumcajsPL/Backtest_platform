import os
from datetime import datetime, timedelta, timezone
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURATION ---

BASE_URL = "https://datafeed.dukascopy.com/datafeed"

# --- UTILITY: SESSION WITH RETRIES ---

def get_requests_session():
    """Sets up a requests session with retries for robust downloading."""
    session = requests.Session()
    # Retry on specific status codes (5xx)
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    })
    return session

# --- PATH AND URL FUNCTIONS ---

def get_bi5_url(instrument: str, dt: datetime) -> str:
    """Generates the URL for a specific hourly .bi5 file."""
    # Dukascopy uses zero-based month index in the URL (00 for January)
    month_index = dt.month - 1
    
    # URL pattern: BASE_URL / INSTRUMENT / YEAR / MONTH(00-11) / DAY / HOUR(00-23)h_ticks.bi5
    url_path = f"{instrument.upper()}/{dt.year}/{month_index:02d}/{dt.day:02d}/{dt.hour:02d}h_ticks.bi5"
    return f"{BASE_URL}/{url_path}"

def get_local_filepath(instrument: str, dt: datetime, base_dir: str) -> str:
    """Generates the local file path for a specific hourly .bi5 file."""
    # Structure: base_dir/<instrument>/2023/12/01/01h_ticks.bi5
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
    url = get_bi5_url(instrument, dt)
    local_path = get_local_filepath(instrument, dt, base_dir)
    
    # Check if file already exists to enable resume capability
    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        return f"✅ Exists: {local_path}"

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

        # Ensure the directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        # Save the raw compressed binary content
        with open(local_path, 'wb') as f:
            f.write(response.content)
            
        return f"✅ Downloaded: {local_path}"
        
    except requests.exceptions.HTTPError as e:
        # 404 Not Found is common for non-trading hours or holidays
        if response.status_code == 404:
             return f"⚠️ Missing (404): {url}"
        return f"❌ HTTP Error {response.status_code}: {e}"
    except requests.exceptions.RequestException as e:
        return f"❌ Network Error: {e}"
    except Exception as e:
        return f"❌ Unexpected Error: {e}"

def download_and_save_bi5_files(instrument: str, start_date: datetime, end_date: datetime, output_dir: str, max_workers: int = 15):
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
    print(f"Total {total_files} hourly files to process.")

    session = get_requests_session()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_hour = {
            executor.submit(download_single_hour, instrument, hour, output_dir, session): hour 
            for hour in hours_to_download
        }
        
        count = 0
        for future in as_completed(future_to_hour):
            count += 1
            try:
                result = future.result()
                # Simple progress update
                if count % (total_files // 100 + 1) == 0 or count == total_files:
                    print(f"Progress: {count}/{total_files} ({count/total_files:.1%})", end='\r')
            except Exception as exc:
                print(f"\nError processing hour: {future_to_hour[future].strftime('%Y-%m-%d %H:%M')}, Exception: {exc}")

    print("\n--- Download Complete ---")
    
if __name__ == "__main__":
    # --- Set up PARAMETERS: instrument (ducascopy names), date range, output directory ---
    INSTRUMENT = "fraidxeur"
    START_DATE = datetime(2023, 12, 1, 0, 0, tzinfo=timezone.utc)
    END_DATE = datetime(2025, 11, 30, 23, 0, tzinfo=timezone.utc) 
    OUTPUT_DIRECTORY = "data/raw/dukascopy_bi5" 
    MAX_CONCURRENT_WORKERS = 15
    
    download_and_save_bi5_files(
        instrument=INSTRUMENT,
        start_date=START_DATE,
        end_date=END_DATE,
        output_dir=OUTPUT_DIRECTORY,
        max_workers=MAX_CONCURRENT_WORKERS
    )
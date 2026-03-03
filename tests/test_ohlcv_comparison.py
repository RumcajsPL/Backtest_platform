# tests/test_ohlcv_comparison.py
import pytest
import pandas as pd
from pathlib import Path
import sys
import random
from typing import Optional, Tuple, List

# Add project root to path to allow paths module import
project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

# Import but exclude test_path from being treated as a test function
from src.utils.paths import PROCESSED_DATA_DIR, ensure_dir

# ============================================================================
# CONFIGURATION - Easily modifiable parameters
# ============================================================================

# Files to compare (relative to PROCESSED_DATA_DIR/ohlcv/)
FILE1 = "DEUIDXEUR_1min_20240101_20260207.parquet"
FILE2 = "DEUIDXEUR_1min_20221201_20260301.parquet"

# Number of bars to compare
N_BARS = 100

# Number of random samples to display
N_RANDOM_SAMPLES = 5

# ============================================================================
# Helper functions
# ============================================================================

def load_ohlcv_data(file_path: Path, n_bars: Optional[int] = None) -> pd.DataFrame:
    """
    Load OHLCV parquet file and return as DataFrame.
    
    Args:
        file_path: Path to parquet file
        n_bars: Number of rows to load (None = all rows)
    
    Returns:
        DataFrame with timestamp as index
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    # Read parquet file
    df = pd.read_parquet(file_path)
    
    # Ensure timestamp is datetime
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
    
    # Sort by timestamp to ensure correct order
    df.sort_index(inplace=True)
    
    # Take first n_bars if specified
    if n_bars:
        df = df.iloc[:n_bars]
    
    return df

def get_common_timestamps(df1: pd.DataFrame, df2: pd.DataFrame, n_bars: int) -> pd.DatetimeIndex:
    """
    Find common timestamps between two DataFrames and return first n_bars.
    """
    common_timestamps = df1.index.intersection(df2.index)
    
    if len(common_timestamps) == 0:
        raise ValueError("No common timestamps found between the two files")
    
    if len(common_timestamps) < n_bars:
        print(f"Warning: Only {len(common_timestamps)} common timestamps available, using all of them")
        return common_timestamps
    
    return common_timestamps[:n_bars]

def compare_bars(df1: pd.DataFrame, df2: pd.DataFrame, timestamps: pd.DatetimeIndex, 
                 debug: bool = False) -> Tuple[bool, List[str]]:
    """
    Compare bars at given timestamps between two DataFrames.
    
    Returns:
        Tuple of (all_match, list_of_differences)
    """
    all_match = True
    differences = []
    
    for ts in timestamps:
        bar1 = df1.loc[ts]
        bar2 = df2.loc[ts]
        
        # Compare each column
        for col in ['open', 'high', 'low', 'close', 'volume']:
            val1 = bar1[col]
            val2 = bar2[col]
            
            # Handle potential floating point differences
            if col == 'volume':
                # Volume might be float, compare with tolerance
                if not abs(val1 - val2) < 1e-10:
                    all_match = False
                    if debug:
                        differences.append(f"Timestamp {ts}: {col} differs - {val1} vs {val2}")
            else:
                # Price columns
                if not abs(val1 - val2) < 1e-8:
                    all_match = False
                    if debug:
                        differences.append(f"Timestamp {ts}: {col} differs - {val1} vs {val2}")
    
    return all_match, differences

def display_random_samples(df1: pd.DataFrame, df2: pd.DataFrame, timestamps: pd.DatetimeIndex, 
                          n_samples: int = 5):
    """
    Display random samples from both data sources for visual comparison.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        timestamps: List of timestamps to sample from
        n_samples: Number of random samples to display
    """
    if len(timestamps) < n_samples:
        sample_timestamps = timestamps
    else:
        sample_timestamps = random.sample(list(timestamps), n_samples)
    
    print(f"\n{'-'*60}")
    print(f"RANDOM SAMPLE - {len(sample_timestamps)} BARS (for visual comparison)")
    print(f"{'-'*60}")
    
    for ts in sorted(sample_timestamps):
        bar1 = df1.loc[ts]
        bar2 = df2.loc[ts]
        
        # Format the output for better readability
        ts_str = ts.strftime('%Y-%m-%d %H:%M:%S')
        
        # Source 1
        print(f"Source 1: {ts_str}  {bar1['open']:>10.3f}  {bar1['high']:>10.3f}  "
              f"{bar1['low']:>10.3f}  {bar1['close']:>10.3f}  {bar1['volume']:>15.3f}")
        
        # Source 2
        print(f"Source 2: {ts_str}  {bar2['open']:>10.3f}  {bar2['high']:>10.3f}  "
              f"{bar2['low']:>10.3f}  {bar2['close']:>10.3f}  {bar2['volume']:>15.3f}")
        
        # Add a blank line between samples for better readability
        print()

# ============================================================================
# Pytest fixtures
# ============================================================================

@pytest.fixture(scope="module")
def ohlcv_files() -> Tuple[Path, Path]:
    """Fixture providing paths to the two parquet files."""
    ohlcv_dir = PROCESSED_DATA_DIR / "ohlcv"
    
    file1_path = ohlcv_dir / FILE1
    file2_path = ohlcv_dir / FILE2
    
    # Verify files exist
    assert file1_path.exists(), f"File not found: {file1_path}"
    assert file2_path.exists(), f"File not found: {file2_path}"
    
    return file1_path, file2_path

@pytest.fixture(scope="module")
def ohlcv_data(ohlcv_files) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Fixture loading the OHLCV data."""
    file1_path, file2_path = ohlcv_files
    
    # Load full data
    df1 = load_ohlcv_data(file1_path)
    df2 = load_ohlcv_data(file2_path)
    
    return df1, df2

# ============================================================================
# Test functions
# ============================================================================

def test_files_exist(ohlcv_files):
    """Test that both files exist."""
    file1_path, file2_path = ohlcv_files
    assert file1_path.exists(), f"File not found: {file1_path}"
    assert file2_path.exists(), f"File not found: {file2_path}"
    print(f"\n✓ Files exist:\n  {file1_path.name}\n  {file2_path.name}")

def test_data_structure(ohlcv_data):
    """Test that both files have the expected structure."""
    df1, df2 = ohlcv_data
    
    # Check columns
    expected_columns = ['open', 'high', 'low', 'close', 'volume']
    assert all(col in df1.columns for col in expected_columns), f"File1 missing columns: {expected_columns}"
    assert all(col in df2.columns for col in expected_columns), f"File2 missing columns: {expected_columns}"
    
    # Check index is datetime
    assert isinstance(df1.index, pd.DatetimeIndex), "File1 index is not DatetimeIndex"
    assert isinstance(df2.index, pd.DatetimeIndex), "File2 index is not DatetimeIndex"
    
    print(f"\n✓ Data structure OK for both files")

@pytest.mark.parametrize("n_bars", [N_BARS])
def test_ohlcv_comparison(ohlcv_data, n_bars, pytestconfig):
    """
    Main test comparing N bars from both files.
    
    Run with: pytest tests/test_ohlcv_comparison.py -v
    For debug mode: pytest tests/test_ohlcv_comparison.py -v --debug
    """
    df1, df2 = ohlcv_data
    
    # Get debug mode from pytest config
    debug = pytestconfig.getoption("--debug", False)
    
    # Find common timestamps
    common_timestamps = get_common_timestamps(df1, df2, n_bars)
    
    print(f"\n{'='*60}")
    print(f"COMPARING {len(common_timestamps)} BARS")
    print(f"{'='*60}")
    print(f"Time range: {common_timestamps[0]} to {common_timestamps[-1]}")
    
    # Display random samples for visual comparison (always show in normal mode)
    display_random_samples(df1, df2, common_timestamps, N_RANDOM_SAMPLES)
    
    # Compare bars
    all_match, differences = compare_bars(df1, df2, common_timestamps, debug)
    
    if all_match:
        print(f"\n✓ SUCCESS: All {len(common_timestamps)} bars match perfectly!")
    else:
        print(f"\n✗ FAILURE: Found mismatches in {len(differences)} bars")
        if debug:
            print("\nDetails:")
            for diff in differences[:20]:  # Show first 20 differences
                print(f"  {diff}")
            if len(differences) > 20:
                print(f"  ... and {len(differences) - 20} more differences")
    
    assert all_match, f"Data mismatch between files for {len(differences)} bars"

# ============================================================================
# Additional utility tests (optional)
# ============================================================================

def test_summary_statistics(ohlcv_data):
    """Optional test comparing summary statistics."""
    df1, df2 = ohlcv_data
    
    # Get common timestamps for full overlap period
    common_timestamps = get_common_timestamps(df1, df2, min(len(df1), len(df2)))
    df1_common = df1.loc[common_timestamps]
    df2_common = df2.loc[common_timestamps]
    
    print(f"\n{'='*60}")
    print("SUMMARY STATISTICS (Common Period)")
    print(f"{'='*60}")
    
    for col in ['open', 'high', 'low', 'close', 'volume']:
        print(f"\n{col.upper()}:")
        print(f"  File1 - mean: {df1_common[col].mean():.4f}, std: {df1_common[col].std():.4f}")
        print(f"  File2 - mean: {df2_common[col].mean():.4f}, std: {df2_common[col].std():.4f}")
        print(f"  Difference: {(df1_common[col].mean() - df2_common[col].mean()):.4f}")
    
    # Also show random samples in summary statistics
    display_random_samples(df1, df2, common_timestamps, N_RANDOM_SAMPLES)

# ============================================================================
# Pytest configuration for debug mode
# ============================================================================

def pytest_addoption(parser):
    """Add custom command line options."""
    parser.addoption(
        "--debug",
        action="store_true",
        default=False,
        help="Enable debug mode with detailed output"
    )

# ============================================================================
# Standalone execution (for testing without pytest)
# ============================================================================

if __name__ == "__main__":
    """Run test standalone for quick checking."""
    print("\n" + "="*60)
    print("RUNNING STANDALONE COMPARISON")
    print("="*60)
    
    # Load files
    ohlcv_dir = PROCESSED_DATA_DIR / "ohlcv"
    file1_path = ohlcv_dir / FILE1
    file2_path = ohlcv_dir / FILE2
    
    try:
        df1 = load_ohlcv_data(file1_path)
        df2 = load_ohlcv_data(file2_path)
        
        print(f"\nFile1: {file1_path.name}")
        print(f"  - Shape: {df1.shape}")
        print(f"  - Date range: {df1.index[0]} to {df1.index[-1]}")
        
        print(f"\nFile2: {file2_path.name}")
        print(f"  - Shape: {df2.shape}")
        print(f"  - Date range: {df2.index[0]} to {df2.index[-1]}")
        
        # Find common timestamps
        common_timestamps = get_common_timestamps(df1, df2, N_BARS)
        
        print(f"\nComparing first {len(common_timestamps)} common bars...")
        
        # Display random samples
        display_random_samples(df1, df2, common_timestamps, N_RANDOM_SAMPLES)
        
        # Compare with debug=True
        all_match, differences = compare_bars(df1, df2, common_timestamps, debug=True)
        
        if all_match:
            print(f"\n✓ SUCCESS: All {len(common_timestamps)} bars match perfectly!")
        else:
            print(f"\n✗ FAILURE: Found {len(differences)} mismatches")
            for diff in differences:
                print(f"  {diff}")
                
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
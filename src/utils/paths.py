# src/utils/paths.py
from pathlib import Path

# ---------------------------------------------------------
# PROJECT ROOT RESOLUTION
# ---------------------------------------------------------
# This resolves the project root no matter where the code is executed from:
# - scripts/
# - notebooks/
# - tests/
# - src/
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# ---------------------------------------------------------
# TOP-LEVEL DIRECTORIES
# ---------------------------------------------------------
CONFIGS_DIR = PROJECT_ROOT / "configs"
DATA_DIR = PROJECT_ROOT / "data"
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
SRC_DIR = PROJECT_ROOT / "src"

# ---------------------------------------------------------
# DATA SUBDIRECTORIES
# ---------------------------------------------------------
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
FEATURES_DATA_DIR = DATA_DIR / "features"
EXPORTS_DATA_DIR = DATA_DIR / "exports"

# ---------------------------------------------------------
# OUTPUT SUBDIRECTORIES
# ---------------------------------------------------------
BACKTEST_OUTPUT_DIR = OUTPUTS_DIR / "backtests"
LOGS_DIR = OUTPUTS_DIR / "logs"
REPORTS_DIR = OUTPUTS_DIR / "reports"
SIGNALS_DIR = OUTPUTS_DIR / "signals"

# ---------------------------------------------------------
# SCRIPT RUNNERS
# ---------------------------------------------------------
RUNNERS_DIR = SCRIPTS_DIR / "runners"
DATA_SCRIPTS_DIR = SCRIPTS_DIR / "data"
VALIDATION_SCRIPTS_DIR = SCRIPTS_DIR / "validation"

# ---------------------------------------------------------
# CONFIG HELPERS
# ---------------------------------------------------------
def config_path(*parts) -> Path:
    """Return a path inside configs/."""
    return CONFIGS_DIR.joinpath(*parts)

# ---------------------------------------------------------
# DATA HELPERS
# ---------------------------------------------------------
def data_path(*parts) -> Path:
    """Return a path inside data/."""
    return DATA_DIR.joinpath(*parts)

# ---------------------------------------------------------
# OUTPUT HELPERS
# ---------------------------------------------------------
def output_path(*parts) -> Path:
    """Return a path inside outputs/."""
    return OUTPUTS_DIR.joinpath(*parts)

# ---------------------------------------------------------
# SAFE FILE CREATION
# ---------------------------------------------------------
def ensure_dir(path: Path):
    """Create directory if it does not exist."""
    path.mkdir(parents=True, exist_ok=True)
    return path
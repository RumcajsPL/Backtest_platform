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
BACKTEST_OUTPUT_DIR = OUTPUTS_DIR / "backtests" #Future backtester
LOGS_DIR = OUTPUTS_DIR / "logs"
REPORTS_DIR = OUTPUTS_DIR / "reports"
SIGNALS_DIR = OUTPUTS_DIR / "signals"
STRATEGIES_OUTPUTS_DIR = OUTPUTS_DIR / "strategies" #New architecture strategy-specific outputs (logs, reports, etc.)
STRATEGIES_LOGS_DIR = STRATEGIES_OUTPUTS_DIR / "logs" #New architecture strategy-specific logs
STRATEGIES_REPORTS_DIR = STRATEGIES_OUTPUTS_DIR / "reports" #New architecture strategy-specific reports

# ---------------------------------------------------------
# SCRIPT RUNNERS
# ---------------------------------------------------------
RUNNERS_DIR = SCRIPTS_DIR / "runners"
DATA_SCRIPTS_DIR = SCRIPTS_DIR / "data"
VALIDATION_SCRIPTS_DIR = SCRIPTS_DIR / "validation"

# ---------------------------------------------------------
# STRATEGY SUBDIRECTORIES (NEW MIGRATION STRUCTURE)
# ---------------------------------------------------------
STRATEGIES_DIR = SRC_DIR / "strategies"
CONTRACTS_DIR = STRATEGIES_DIR / "contracts"
SPECIFIC_STRATEGIES_DIR = STRATEGIES_DIR / "specific"
MODULES_DIR = SPECIFIC_STRATEGIES_DIR / "modules"
FILTERS_DIR = SPECIFIC_STRATEGIES_DIR / "filters"

# ---------------------------------------------------------
# TEST SUBDIRECTORIES (NEW MIGRATION STRUCTURE)
# ---------------------------------------------------------
TESTS_DIR = PROJECT_ROOT / "tests"
MIGRATION_TESTS_DIR = TESTS_DIR / "migration"

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
# STRATEGY HELPERS (NEW)
# ---------------------------------------------------------
def strategy_path(*parts) -> Path:
    """Return a path inside src/strategies/."""
    return STRATEGIES_DIR.joinpath(*parts)

def contract_path(*parts) -> Path:
    """Return a path inside src/strategies/contracts/."""
    return CONTRACTS_DIR.joinpath(*parts)

def specific_strategy_path(*parts) -> Path:
    """Return a path inside src/strategies/specific/."""
    return SPECIFIC_STRATEGIES_DIR.joinpath(*parts)

def module_path(*parts) -> Path:
    """Return a path inside src/strategies/specific/modules/."""
    return MODULES_DIR.joinpath(*parts)

def filter_path(*parts) -> Path:
    """Return a path inside src/strategies/specific/filters/."""
    return FILTERS_DIR.joinpath(*parts)

# ---------------------------------------------------------
# TEST HELPERS (NEW)
# ---------------------------------------------------------
def test_path(*parts) -> Path:
    """Return a path inside tests/."""
    return TESTS_DIR.joinpath(*parts)

def migration_test_path(*parts) -> Path:
    """Return a path inside tests/migration/."""
    return MIGRATION_TESTS_DIR.joinpath(*parts)

# ---------------------------------------------------------
# SAFE FILE CREATION
# ---------------------------------------------------------
def ensure_dir(path: Path):
    """Create directory if it does not exist."""
    path.mkdir(parents=True, exist_ok=True)
    return path
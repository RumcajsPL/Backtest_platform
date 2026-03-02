# src/utils/paths.py
from pathlib import Path
# ---------------------------------------------------------
# ROOT RESOLUTION
# ---------------------------------------------------------
# This resolves the project root no matter where the code is executed from:
# - configs/
# - data/
# - outputs/
# - scripts/
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
DB_DATA_DIR = DATA_DIR / "db"
# ---------------------------------------------------------
# OUTPUT SUBDIRECTORIES
# ---------------------------------------------------------
BACKTEST_OUTPUT_DIR = OUTPUTS_DIR / "backtests"
LOGS_DIR = OUTPUTS_DIR / "logs"
REPORTS_DIR = OUTPUTS_DIR / "reports"
STRATEGIES_OUTPUTS_DIR = OUTPUTS_DIR / "strategies" 
STRATEGIES_LOGS_DIR = STRATEGIES_OUTPUTS_DIR / "logs"
STRATEGIES_REPORTS_DIR = STRATEGIES_OUTPUTS_DIR / "reports"
# ---------------------------------------------------------
# SCRIPT RUNNERS
# ---------------------------------------------------------
RUNNERS_DIR = SCRIPTS_DIR / "runners"
# ---------------------------------------------------------
# STRATEGY SUBDIRECTORIES
# ---------------------------------------------------------
STRATEGIES_DIR = SRC_DIR / "strategies"
CONTRACTS_DIR = STRATEGIES_DIR / "contracts"
CORE_STRATEGIES_ = STRATEGIES_DIR / "core"
FILTERS_DIR = STRATEGIES_DIR / "filters"
RUNNERS_DIR = SCRIPTS_DIR / "runners"
# ---------------------------------------------------------
# BACKTESTER SUBDIRECTORIES
# ---------------------------------------------------------
BACKTEST_DIR = SRC_DIR / "backtesting"
# ---------------------------------------------------------
# UTILS SUBDIRECTORIES
# ---------------------------------------------------------
UTILS_DIR = SRC_DIR / "utils"
# ---------------------------------------------------------
# TEST SUBDIRECTORIES 
# ---------------------------------------------------------
TESTS_DIR = PROJECT_ROOT / "tests"
STRATEGIES_TESTS_DIR = TESTS_DIR / "strategies"
BACKTESTING_TESTS_DIR = TESTS_DIR / "backtesting"
UNIT_TESTS_DIR = STRATEGIES_TESTS_DIR / "unit"
CONTRACT_TEST_DIR = UNIT_TESTS_DIR / "contracts"
FILTERS_TEST_DIR = UNIT_TESTS_DIR / "filters"
RUNNER_TESTS_DIR = STRATEGIES_TESTS_DIR / "runners"
REPORT_TESTS_DIR = STRATEGIES_TESTS_DIR / "reports"
DIAG_TESTS_DIR = STRATEGIES_TESTS_DIR / "diagnostic"
BCST_BENCH_TEST_DIR = BACKTESTING_TESTS_DIR / "benchmarks"
BCST_INEGR_TEST_DIR = BACKTESTING_TESTS_DIR / "integration"
BCST_UNIT_TEST_DIR = BACKTESTING_TESTS_DIR / "unit"
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
# STRATEGY HELPERS
# ---------------------------------------------------------
def strategy_path(*parts) -> Path:
    """Return a path inside src/strategies/."""
    return STRATEGIES_DIR.joinpath(*parts)
def config_path(*parts) -> Path:
    """Return a path inside src/strategies/config/."""
    return CONFIGS_DIR.joinpath(*parts)

def contract_path(*parts) -> Path:
    """Return a path inside src/strategies/contracts/."""
    return CONTRACTS_DIR.joinpath(*parts)

def core_strategy_path(*parts) -> Path:
    """Return a path inside src/strategies/core/."""
    return CORE_STRATEGIES_.joinpath(*parts)

def filter_path(*parts) -> Path:
    """Return a path inside src/strategies/filters/."""
    return FILTERS_DIR.joinpath(*parts)
# ---------------------------------------------------------
# TEST HELPERS (NEW)
# ---------------------------------------------------------
def test_path(*parts) -> Path:
    """Return a path inside tests/."""
    return TESTS_DIR.joinpath(*parts)
# ---------------------------------------------------------
# SAFE FILE CREATION
# ---------------------------------------------------------
def ensure_dir(path: Path):
    """Create directory if it does not exist."""
    path.mkdir(parents=True, exist_ok=True)
    return path
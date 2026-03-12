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
DOCS_DIR = PROJECT_ROOT / "docs"

# ---------------------------------------------------------
# DATA SUBDIRECTORIES
# ---------------------------------------------------------
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
DB_DATA_DIR = DATA_DIR / "db"

# ---------------------------------------------------------
# CONFIGS SUBDIRECTORIES
# ---------------------------------------------------------
BACKTESTING_CONFIGS_DIR = CONFIGS_DIR / "backtesting"
BROKER_SUPPORT_CONFIGS_DIR = CONFIGS_DIR / "broker_support"
BROKER_SETTINGS_ENV = BROKER_SUPPORT_CONFIGS_DIR / "broker_settings.env"
INSTRUMENT_MAP_YAML = BROKER_SUPPORT_CONFIGS_DIR / "instrument_map.yaml"

# ---------------------------------------------------------
# DOCS SUBDIRECTORIES
# ---------------------------------------------------------
CTP_DOCS_DIR = DOCS_DIR / "ctp"
BROKER_INTEGRATION_DOC = CTP_DOCS_DIR / "BROKER_INTEGRATION.md"
CTP_ROADMAP_DOC = CTP_DOCS_DIR / "CTP_ROADMAP.md"

# ---------------------------------------------------------
# OUTPUT SUBDIRECTORIES
# ---------------------------------------------------------
BACKTEST_OUTPUT_DIR = OUTPUTS_DIR / "backtesting"
LOGS_DIR = OUTPUTS_DIR / "logs"
REPORTS_DIR = OUTPUTS_DIR / "reports"
STRATEGIES_OUTPUTS_DIR = OUTPUTS_DIR / "strategies"
STRATEGIES_LOGS_DIR = STRATEGIES_OUTPUTS_DIR / "logs"
STRATEGIES_REPORTS_DIR = STRATEGIES_OUTPUTS_DIR / "reports"

# Broker support output directories
BROKER_SUPPORT_OUTPUTS_DIR = OUTPUTS_DIR / "broker_support"
JOURNAL_DIR = BROKER_SUPPORT_OUTPUTS_DIR / "journal"
TRADES_CSV = JOURNAL_DIR / "trades.csv"
SNAPSHOTS_DIR = BROKER_SUPPORT_OUTPUTS_DIR / "snapshots"
BROKER_LOGS_DIR = BROKER_SUPPORT_OUTPUTS_DIR / "logs"
TRACKER_LOG = BROKER_LOGS_DIR / "tracker.log"

# ---------------------------------------------------------
# SCRIPT RUNNERS
# ---------------------------------------------------------
RUNNERS_DIR = SCRIPTS_DIR / "runners"
BROKER_SUPPORT_SCRIPTS_DIR = SCRIPTS_DIR / "broker_support"
RUN_TRACKER_SCRIPT = BROKER_SUPPORT_SCRIPTS_DIR / "run_tracker.py"
RUN_TRACKER_LOOP_SCRIPT = BROKER_SUPPORT_SCRIPTS_DIR / "run_tracker_loop.py"
RUN_INSTRUMENT_LOOKUP_SCRIPT = BROKER_SUPPORT_SCRIPTS_DIR / "run_instrument_lookup.py"
RUN_SIGNAL_BRIDGE_SCRIPT = BROKER_SUPPORT_SCRIPTS_DIR / "run_signal_bridge.py"

# ---------------------------------------------------------
# STRATEGY SUBDIRECTORIES
# ---------------------------------------------------------
STRATEGIES_DIR = SRC_DIR / "strategies"
CONTRACTS_DIR = STRATEGIES_DIR / "contracts"
CORE_STRATEGIES_ = STRATEGIES_DIR / "core"
FILTERS_DIR = STRATEGIES_DIR / "filters"

# ---------------------------------------------------------
# BACKTESTER SUBDIRECTORIES
# ---------------------------------------------------------
BACKTEST_DIR = SRC_DIR / "backtesting"

# ---------------------------------------------------------
# BROKER SUPPORT SUBDIRECTORIES
# ---------------------------------------------------------
BROKER_SUPPORT_DIR = SRC_DIR / "broker_support"
BROKER_SETTINGS_MODULE = BROKER_SUPPORT_DIR / "settings.py"

# Client
BROKER_CLIENT_DIR = BROKER_SUPPORT_DIR / "client"
BROKER_CLIENT_MODULE = BROKER_CLIENT_DIR / "client.py"
BROKER_EXCEPTIONS_MODULE = BROKER_CLIENT_DIR / "exceptions.py"

# Models
BROKER_MODELS_DIR = BROKER_SUPPORT_DIR / "models"
TRADE_MODEL_MODULE = BROKER_MODELS_DIR / "trade.py"
PORTFOLIO_MODEL_MODULE = BROKER_MODELS_DIR / "portfolio.py"
ORDER_MODEL_MODULE = BROKER_MODELS_DIR / "order.py"

# Tracking
BROKER_TRACKING_DIR = BROKER_SUPPORT_DIR / "tracking"
POSITION_TRACKER_MODULE = BROKER_TRACKING_DIR / "position_tracker.py"
CSV_JOURNAL_MODULE = BROKER_TRACKING_DIR / "csv_journal.py"

# Enrichment
BROKER_ENRICHMENT_DIR = BROKER_SUPPORT_DIR / "enrichment"
TRADE_ENRICHER_MODULE = BROKER_ENRICHMENT_DIR / "trade_enricher.py"
INSTRUMENT_RESOLVER_MODULE = BROKER_ENRICHMENT_DIR / "instrument_resolver.py"

# Execution
BROKER_EXECUTION_DIR = BROKER_SUPPORT_DIR / "execution"
ORDER_ROUTER_MODULE = BROKER_EXECUTION_DIR / "order_router.py"
SIGNAL_MAPPER_MODULE = BROKER_EXECUTION_DIR / "signal_mapper.py"

# Broker Utils
BROKER_UTILS_DIR = BROKER_SUPPORT_DIR / "utils"
TIME_UTILS_MODULE = BROKER_UTILS_DIR / "time_utils.py"
RATE_LIMITER_MODULE = BROKER_UTILS_DIR / "rate_limiter.py"

# ---------------------------------------------------------
# UTILS SUBDIRECTORIES
# ---------------------------------------------------------
UTILS_DIR = SRC_DIR / "utils"

# ---------------------------------------------------------
# TEST SUBDIRECTORIES
# ---------------------------------------------------------
TESTS_DIR = PROJECT_ROOT / "tests"
BACKTESTING_TESTS_DIR = TESTS_DIR / "backtesting"

# Broker support tests
BROKER_SUPPORT_TESTS_DIR = TESTS_DIR / "broker_support"
TEST_CLIENT_MODULE = BROKER_SUPPORT_TESTS_DIR / "test_client.py"
TEST_MODELS_MODULE = BROKER_SUPPORT_TESTS_DIR / "test_models.py"
TEST_POSITION_TRACKER_MODULE = BROKER_SUPPORT_TESTS_DIR / "test_position_tracker.py"
TEST_CSV_JOURNAL_MODULE = BROKER_SUPPORT_TESTS_DIR / "test_csv_journal.py"
TEST_TRADE_ENRICHER_MODULE = BROKER_SUPPORT_TESTS_DIR / "test_trade_enricher.py"
TEST_SIGNAL_BRIDGE_MODULE = BROKER_SUPPORT_TESTS_DIR / "test_signal_bridge.py"

# Strategy tests
STRATEGIES_TESTS_DIR = TESTS_DIR / "strategies"
UNIT_TESTS_DIR = STRATEGIES_TESTS_DIR / "unit"
CONTRACT_TEST_DIR = UNIT_TESTS_DIR / "contracts"
FILTERS_TEST_DIR = UNIT_TESTS_DIR / "filters"
RUNNER_TESTS_DIR = STRATEGIES_TESTS_DIR / "runners"
REPORT_TESTS_DIR = STRATEGIES_TESTS_DIR / "reports"
DIAG_TESTS_DIR = STRATEGIES_TESTS_DIR / "diagnostic"

# Backtesting tests
BCST_BENCH_TEST_DIR = BACKTESTING_TESTS_DIR / "benchmarks"
BCST_INTEGR_TEST_DIR = BACKTESTING_TESTS_DIR / "integration"
BCST_UNIT_TEST_DIR = BACKTESTING_TESTS_DIR / "unit"

# ---------------------------------------------------------
# CONFIG HELPERS
# ---------------------------------------------------------
def config_path(*parts) -> Path:
    """Return a path inside configs/."""
    return CONFIGS_DIR.joinpath(*parts)

def backtesting_config_path(*parts) -> Path:
    """Return a path inside configs/backtesting/."""
    return BACKTESTING_CONFIGS_DIR.joinpath(*parts)

def broker_config_path(*parts) -> Path:
    """Return a path inside configs/broker_support/."""
    return BROKER_SUPPORT_CONFIGS_DIR.joinpath(*parts)

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

def broker_output_path(*parts) -> Path:
    """Return a path inside outputs/broker_support/."""
    return BROKER_SUPPORT_OUTPUTS_DIR.joinpath(*parts)

def journal_path(*parts) -> Path:
    """Return a path inside outputs/broker_support/journal/."""
    return JOURNAL_DIR.joinpath(*parts)

def snapshots_path(*parts) -> Path:
    """Return a path inside outputs/broker_support/snapshots/."""
    return SNAPSHOTS_DIR.joinpath(*parts)

def broker_logs_path(*parts) -> Path:
    """Return a path inside outputs/broker_support/logs/."""
    return BROKER_LOGS_DIR.joinpath(*parts)

# ---------------------------------------------------------
# DOCS HELPERS
# ---------------------------------------------------------
def docs_path(*parts) -> Path:
    """Return a path inside docs/."""
    return DOCS_DIR.joinpath(*parts)

def ctp_docs_path(*parts) -> Path:
    """Return a path inside docs/ctp/."""
    return CTP_DOCS_DIR.joinpath(*parts)

# ---------------------------------------------------------
# SCRIPT HELPERS
# ---------------------------------------------------------
def scripts_path(*parts) -> Path:
    """Return a path inside scripts/."""
    return SCRIPTS_DIR.joinpath(*parts)

def broker_scripts_path(*parts) -> Path:
    """Return a path inside scripts/broker_support/."""
    return BROKER_SUPPORT_SCRIPTS_DIR.joinpath(*parts)

# ---------------------------------------------------------
# STRATEGY HELPERS
# ---------------------------------------------------------
def strategy_path(*parts) -> Path:
    """Return a path inside src/strategies/."""
    return STRATEGIES_DIR.joinpath(*parts)

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
# BROKER SUPPORT HELPERS
# ---------------------------------------------------------
def broker_support_path(*parts) -> Path:
    """Return a path inside src/broker_support/."""
    return BROKER_SUPPORT_DIR.joinpath(*parts)

def broker_client_path(*parts) -> Path:
    """Return a path inside src/broker_support/client/."""
    return BROKER_CLIENT_DIR.joinpath(*parts)

def broker_models_path(*parts) -> Path:
    """Return a path inside src/broker_support/models/."""
    return BROKER_MODELS_DIR.joinpath(*parts)

def broker_tracking_path(*parts) -> Path:
    """Return a path inside src/broker_support/tracking/."""
    return BROKER_TRACKING_DIR.joinpath(*parts)

def broker_enrichment_path(*parts) -> Path:
    """Return a path inside src/broker_support/enrichment/."""
    return BROKER_ENRICHMENT_DIR.joinpath(*parts)

def broker_execution_path(*parts) -> Path:
    """Return a path inside src/broker_support/execution/."""
    return BROKER_EXECUTION_DIR.joinpath(*parts)

def broker_utils_path(*parts) -> Path:
    """Return a path inside src/broker_support/utils/."""
    return BROKER_UTILS_DIR.joinpath(*parts)

# ---------------------------------------------------------
# TEST HELPERS
# ---------------------------------------------------------
def test_path(*parts) -> Path:
    """Return a path inside tests/."""
    return TESTS_DIR.joinpath(*parts)

def broker_test_path(*parts) -> Path:
    """Return a path inside tests/broker_support/."""
    return BROKER_SUPPORT_TESTS_DIR.joinpath(*parts)

# ---------------------------------------------------------
# SAFE FILE CREATION
# ---------------------------------------------------------
def ensure_dir(path: Path):
    """Create directory if it does not exist."""
    path.mkdir(parents=True, exist_ok=True)
    return path
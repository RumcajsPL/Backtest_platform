# WBWSStrategy Migration Project Charter
## Project Overview
**Objective**: Migrate existing WBWSStrategy from dict-based architecture to typed domain contracts while maintaining 100% functional parity and performance. Parity is same important as Perf. If no parity and no technical bug then analysis and decision to be made if accepted (exemple bug in legacy discovered) but globaly we expext parity matches.   
**Strategic Goal**: Create reusable platform foundation for future strategies and automated backtesting orchestrator.
**Timeline**: Multi-session project (estimated 18-28 sessions)
**Success Criteria**:
- ✅ Where relevant modules use typed contracts (no dict-based trade/signal communication)
- ✅ Performance maintained or improved (≤110% of baseline)
- ✅ Old system continues working in parallel
- ✅ Stage-by-stage validation against old outputs
- ✅ Ready for orchestrator integration
---
## Constraints & Principles
### Non-Negotiable
1. **No performance regression** - New ≤ 110% of old execution time
2. **Backward compatibility** - Old system untouched and functional, migration happens paralelly to existing
3. **Incremental validation** - Test after each module migration
4. **Session continuity** - Handoff protocol for chat window limits
### Design Principles
- Single Responsibility (one module = one concern)
- Performance-driven (vectorization, caching and more advanced)
- Explicit contracts (no hidden assumptions)
- Type safety (dataclasses over dicts)
- Performance-aware (vectorization, caching, etc.)
- Test-driven (validate each step)
---
## Current System Baseline
**Performance** (3-month sample):
- Original: <2 minutes end-to-end>
- **Target**: No regression/improvement
---
**Test Dataset**: 3 months of data
- Sufficient for metrics validation
- Fast iteration cycles
----
**Output Artifacts**:
- Raw signals (pre-filter)
- Time-filtered signals
- Technical-filtered signals
- Trade entries/exits
- Performance metrics
- Progressive tracking (debug mode)
---
## Migration Strategy
**Approach**: Hybrid Big Bang + Thin Slice
- **Big Bang**: Simple modules (DataLoader, SignalGenerator)
- **Thin Slice**: Complex modules (FilterPipeline, TradeSimulator)
**Parallel Execution**:
```
src/strategies/
├── core/              # OLD (frozen, working)
├── specific/          # NEW (migration target)
└── contracts/         # SHARED (domain types)
```
**Validation Framework**:
- Module-by-module output comparison
- Performance benchmarking
- Metrics parity testing
---
## Risk Management
| Risk | Mitigation |
|------|------------|
| Chat window limit | Progressive checkpoints/logs every 3-5 exchanges |
| Performance regression | Benchmark at each step |
| Breaking old system | Parallel architecture, no modifications to `core/` |
| Incomplete migration | Phase-based approach with rollback points |
---
## Path resolution orgenized by src\utils\paths.py
**Important sript parts**:
```python
# ---------------------------------------------------------
# PROJECT ROOT RESOLUTION
# ---------------------------------------------------------
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
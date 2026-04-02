# CHANGELOG.md — CTP V2 Backtester
# Scope: Architecture decisions and confirmed code changes — immutable audit trail
# Project planning: see PLAN.md | Session history: see SESSION_LOG.md
# Owner: Claude.ai | Version: 1.0 | Date: 2026-04-02
---

## FORMAT

Each entry records:
- **Date**: when the decision was made or change was confirmed
- **Type**: ARCHITECTURE / CODE / RULE / REVERT
- **ID**: matches backlog ID if applicable
- **Description**: what changed and why
- **Files affected**: exact file paths
- **Approved by**: Owner confirmation
- **Breaks**: any V1 behaviour or contract that no longer holds

Entries are immutable once written. Corrections are new entries (REVERT or SUPERSEDE).

---

## ENTRIES

### 2026-04-02 — Project initialisation

**Type**: ARCHITECTURE
**Description**: CTP V2 Backtester project initialised. The following architectural
decisions are confirmed from V1 analysis and carry into V2 from project start.
These are not new decisions — they are inherited constraints that shape V2 design.

---

#### V2-ARCH-001 — RawDataStore + WindowSlicer replace DataLoader

**Rationale**: V1 `DataLoader` violates Single Responsibility: it both loads raw files
and slices windows. For 33 candidates × 7 windows = 231 evaluations, raw files are
reloaded repeatedly, causing peak RAM of ~897MB per worker and forcing `max_workers: 2`
to avoid OOM on 8GB systems.

**Decision**: Replace `DataLoader` with two dedicated modules:
- `RawDataStore`: load responsibility only — called once per pipeline run
- `WindowSlicer`: slice responsibility only — called once per pipeline run, stores
  all slices in named `SharedMemory` blocks (Windows spawn-safe, zero-copy)

**Files to be created**: `src/backtesting/data/raw_data_store.py`,
`src/backtesting/data/window_slicer.py`
**Files to be retired**: `src/backtesting/data/data_loader.py` (or equivalent V1 path)
**Breaks**: Any test importing `DataLoader` directly — classify as RETIRE in TEST-001

---

#### V2-ARCH-002 — SignalCache introduced

**Rationale**: V1 `StrategyOrchestrator` re-runs RSI, ATR, Bollinger signal generation
on every candidate evaluation. These signals are deterministic functions of OHLCV data
and indicator-period parameters (`rsi_period`, `bollinger_length`, `atr_length`). They
do not vary per candidate threshold parameter. For 60 candidates sharing the same
indicator periods, signals are recomputed up to 231 times on identical data.

**Decision**: Introduce `SignalCache` module. Cache key: `(window_id, rsi_period,
bollinger_length, atr_length)`. Generated signals stored in shared memory alongside OHLCV
slices. Workers read signals — no recomputation. Cache eviction strategy: TBD (DEC-001).

**Files to be created**: `src/backtesting/data/signal_cache.py`
**Breaks**: None immediately — `TradeSimulator` interface unchanged; receives shm handle
instead of computed DataFrame. Requires `TradeSimulator` adaptation (IMPL-004).

---

#### V2-ARCH-003 — max_workers constraint under investigation

**Rationale**: V1 ran stably at `max_workers=2` and `max_workers=4`. `max_workers=6`
was tested without OOM issues but no measurable performance difference was observed
between 2 and 4 under the V1 architecture. The shared memory architecture in V2 changes
the memory profile substantially (~20MB per worker vs ~897MB), which may enable higher
parallelism with actual throughput benefit. This is to be verified by profiling.

**Decision**: The hard `max_workers=2` constraint is removed as an OOM guard. The
target value for V2 is subject to profiling under the shared memory architecture
(see DEC-008). Default config will be set after profiling confirms the optimal value.

**Files affected**: `backtest_template.yaml` default config
**Breaks**: Config entries relying on `max_workers=2` as a safety cap — these become
advisory rather than required.

---

#### V2-ARCH-004 — RSI removed from search space

**Rationale**: 6 consecutive sensitivity runs produced zero-delta results for
`rsi_period`, `rsi_overbought`, `rsi_oversold`. RSI carries no optimisation signal
in the current strategy configuration.

**Decision**: Remove RSI parameters from `parameter_space.py`, `backtest_template.yaml`
search space, and `_PARAM_KEY_MAP` / `_PARAM_MAP` twin files. RSI may remain in the
strategy's signal generation as a fixed-value filter if needed; it is removed from the
optimisation search space only.

**Files affected**: `parameter_space.py`, `backtest_template.yaml`, `strategy_runner.py`,
`yaml_generator.py` (twin key maps)
**Breaks**: Any V1 config YAML with RSI search space entries — these entries are ignored
or must be removed before V2 runs.

---

#### V2-ARCH-005 — Hardcoded normalisation constants replaced by V2-RAR

**Rationale**: `_SIGMOID_SCALE`, `_MAX_EXPECTED_DRAWDOWN`, `_MAX_EXPECTED_VARIANCE` in
`consistency_scorer.py` are instrument-specific constants that required repeated manual
recalibration across V1 runs — each recalibration was a direct source edit to that file.
`_SIGMOID_SCALE` never stabilised at a single value across configurations; 310.0 appeared
in one configuration only. This pattern of hardcoded constants requiring source edits is
the root cause blocking multi-asset support without per-instrument recalibration.

**Decision**: Replace all hardcoded instrument-specific constants with Rolling Annual Range
(RAR) fractions — dimensionless, instrument-agnostic. No calibration pass required per
instrument.

**Files affected**: `src/backtesting/wfo/consistency_scorer.py` (primary), `fitness.py`,
`monte_carlo/mc_metrics.py`
**Breaks**: All existing hardcoded constant values become obsolete. V1 config YAMLs
referencing these constants are superseded.

---

#### V2-ARCH-006 — run_backtest() callable interface required

**Rationale**: V3 meta-optimiser calls V2 backtester as a library, not a subprocess.
CLI-only entry point is insufficient for V3. Decided as V3-readiness requirement.

**Decision**: Implement `run_backtest(config: BacktestConfig) → BacktestResult` as the
primary programmatic interface. CLI `run_backtester.py` wraps this function. Each call
is fully stateless — no shared mutable state between calls.

**Files to be created**: `src/backtesting/api.py` (or equivalent)
**New contracts**: `BacktestConfig` dataclass, `BacktestResult` dataclass (ARCH-004)

---

#### V2-ARCH-007 — Dynamic WFO window generation

**Rationale**: Hardcoded window list in `backtest_template.yaml` prevents multi-asset use
(different instruments have different data ranges) and V3 meta-optimiser (varies window
size as a search dimension).

**Decision**: Replace hardcoded window list with `data_range + window_size` parameters.
`window_generator.py` derives windows programmatically from data range boundaries.

**Files affected**: `wfo/window_generator.py`, `backtest_template.yaml`
**Breaks**: Existing `backtest_template.yaml` files with explicit `walk_forward.windows`
lists — these must be migrated to the new `data_range + window_size` format.

---

#### V2-RULE-001 — Architecture principles formalised for V2

**Type**: RULE
**Description**: The following principles are formalised as V2 project rules (see SKILL.md §4
for full specification). These carry forward from V1 with explicit additions:

Additions vs V1:
- `CacheManager.clear_all_caches()` required between backtester runs (new in V2)
- `SharedMemory` blocks must be released in `finally` after pool closes (new in V2)
- `BacktestConfig` must be constructable programmatically — not YAML-only (V3-readiness)
- No Python loops over hot paths that can be vectorised (explicit rule)

Unchanged from V1: frozen dataclasses, fail-fast, single source of truth, logger over print,
pathlib.Path, datetime.now(UTC), config via .get() + defaults.

---

*Next entry will be added at Session 1 close.*
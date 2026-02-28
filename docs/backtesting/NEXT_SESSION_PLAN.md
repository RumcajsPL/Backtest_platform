# NEXT_SESSION_PLAN.md — Phase 2: Core Infrastructure
**Prepared**: 2026-02-27
**Session goal**: Implement all core infrastructure modules. Complete both Phase 2 benchmarks. Deliver a working single-candidate round-trip end-to-end.

---

## How to Start the Session

1. Open a new chat
2. Paste the **entire content of `CONTEXT.md`** as your first message
3. Add: *"We are starting Phase 2 — Core Infrastructure. Follow the breakdown in NEXT_SESSION_PLAN.md."*
4. Claude reads the `backtester-project` skill automatically (it is in your account)
5. Ask Claude to confirm it has read CONTEXT.md, the skill, and TECHNICAL_SPEC.md before writing any code

**Critical pre-coding reads for Claude:**
- `docs/backtesting/TECHNICAL_SPEC.md` — all contracts and module signatures
- `docs/backtesting/SQLITE_SCHEMA.md` — full schema before any store code
- `docs/strategies/architecture/ARCHITECTURE.md` — strategy architecture integration points

---

## Session Objective

At the end of this session we will have:
- Two benchmarks completed (D-01 and D-02) with results logged
- All 8 core infrastructure modules implemented and unit-tested
- A working end-to-end integration test: one candidate → evaluated → stored in SQLite

---

## Non-Negotiable Before Writing Any Code

The session **must not begin coding** until Claude has read and confirmed:
1. `TECHNICAL_SPEC.md` contract definitions (exact field names, types, validation rules)
2. `SQLITE_SCHEMA.md` table structures (exact column names and types)
3. D-01 resolution: direct Python call in worker process
4. D-02 resolution: WAL mode + single-writer queue pattern

Any code that deviates from TECHNICAL_SPEC.md contracts is a defect, not a style difference.

---

## Work Breakdown

### Block 0 — Benchmarks First (~30 min)
*These must be done before full module implementation. Results gate the architecture.*

**D-02 Benchmark: SQLite WAL + Writer Queue**
Write a standalone benchmark script (not production code):
- Spawn 6 `ProcessPoolExecutor` workers
- Each worker submits 500/6 ≈ 83 fake `CandidateRecord` objects to a `multiprocessing.Queue`
- One writer thread drains the queue and writes to SQLite in WAL mode
- Measure: total time, any write errors, final row count vs. expected
- **Pass criterion**: 500 rows, zero errors, no corruption on repeated runs

**D-01 Benchmark: Strategy Integration Speed**
Write a standalone benchmark script:
- Import the strategy orchestrator directly (no subprocess)
- Evaluate 50 candidates sequentially in a single process (no parallelism yet)
- Measure: time per candidate in `core` mode
- **Pass criterion**: Average ≤ 20 seconds per candidate (conservative — 6 workers × 20s = 120 candidates/min → 600 Random candidates ≈ 5 min)
- If average > 20s: log the result, flag for operator review, but do not change the D-01 decision. The 4-hour budget has slack; profiling in Phase 3 will confirm.

Log both benchmark results in `CHANGE_LOG.md` Phase 2 section before proceeding.

---

### Block 1 — `candidate_store.py` (~60 min)
*Build first. Everything else depends on it.*

**Implement:**
- `CandidateStore` class wrapping SQLite connection
- `__init__`: opens connection, sets `PRAGMA journal_mode = WAL`, `PRAGMA foreign_keys = ON`, `PRAGMA synchronous = NORMAL`
- Schema creation: all 9 tables from `SQLITE_SCHEMA.md` created if not existing
- Writer queue: `threading.Thread` running a drain loop; workers call `store.write_candidate(record)` which puts to the queue; the writer thread calls `_do_write(record)`
- `initialise_run(run_metadata: RunMetadata) -> None`: inserts the `runs` row
- `write_candidate(record: CandidateRecord) -> None`: non-blocking queue submit
- `get_checkpoint(run_id: str) -> Checkpoint`
- `set_checkpoint(run_id: str, checkpoint: Checkpoint) -> None`
- `query_candidates(run_id, stage, min_fitness, limit, order_by) -> List[CandidateRecord]`
- `get_run_metadata(run_id: str) -> Optional[RunMetadata]`
- `close() -> None`: flush queue, close connection

**Tests to write:**
- `test_store_creates_schema`: fresh DB has all 9 tables
- `test_write_and_read_candidate`: write one record, read it back, all fields match
- `test_concurrent_writes`: 6 threads, 100 records each, 600 total — no missing rows
- `test_checkpoint_round_trip`: set checkpoint, get checkpoint, matches
- `test_resume_detection`: run metadata present → get_checkpoint returns correct state

---

### Block 2 — `parameter_space.py` + `sampler.py` (~30 min)

**`parameter_space.py`:**
- `expand_zones(config: dict) -> Dict[str, List[Dict[str, object]]]`
  - Reads `zones:` from config dict
  - For each enabled zone, expands all `int`/`float` range+step params into discrete value lists
  - For `choice` params, uses the choices list directly
  - Returns Cartesian product of all parameter values per zone (as list of dicts)
- `validate_combination(params: dict, zone_def: dict) -> bool`

**`sampler.py`:**
- `sample_lhs(expanded_space, n_per_zone, seed) -> List[CandidateParameterSet]`
  - Latin Hypercube Sampling: divide each parameter's range into N equal strata, sample one from each stratum, shuffle
  - Use `scipy.stats.qmc.LatinHypercube` or implement manually if scipy unavailable
  - Returns `CandidateParameterSet.create(zone_name, params)` for each sample
- `sample_random(expanded_space, n_per_zone, seed) -> List[CandidateParameterSet]`

**Tests to write:**
- `test_expand_zones_safe`: expands safe zone, all values within bounds
- `test_expand_zones_disabled`: disabled zones produce empty output
- `test_lhs_no_duplicates`: 200 samples from safe zone — no duplicate candidate_ids
- `test_lhs_covers_range`: sample min/max values across parameter ranges are within zone bounds

---

### Block 3 — `scenario.py` (~20 min)

**`scenario.py`:**
- `load_scenario(config: dict) -> ScenarioProfile`
  - Reads `scenario: name` from config
  - Looks up the name in `config['scenarios']`
  - Constructs `ScenarioProfile` from the matched definition
  - `ScenarioProfile.__post_init__` does all validation — this function just maps YAML keys to fields
- Fail fast: unknown scenario name → `ValueError` with message listing available scenarios

**Tests to write:**
- `test_load_capital_accumulation`: fitness weights sum to 1.0, all thresholds present
- `test_load_conservative`: WFO temporal weights sum to 1.0
- `test_unknown_scenario_raises`: unknown name raises `ValueError` immediately
- `test_weights_validated`: manually tampered weights (sum ≠ 1.0) raise in `ScenarioProfile.__post_init__`

---

### Block 4 — `strategy_runner.py` (~45 min)
*Most safety-critical module. Never raises. Always returns CandidateResult.*

**`strategy_runner.py`:**
- `evaluate(candidate: CandidateParameterSet, base_yaml_path: Path, temp_dir: Path, min_significant_trades: int) -> CandidateResult`
  - Builds temp YAML path: `temp_dir / f"candidate_{candidate.candidate_id[:12]}.yaml"`
  - Merges candidate parameters into base YAML (deep copy of base, overwrite matching keys)
  - Parameter name mapping happens here — the only place strategy config field names appear
  - Calls `StrategyConfig.from_yaml(temp_yaml_path)` — fail fast on invalid config
  - Calls strategy in core mode, captures `MetricsReport` and `TradeResult`
  - Checks `total_trades >= min_significant_trades` — returns `REJECTED_INSUFFICIENT_TRADES` if not
  - Returns `CandidateResult` with metrics and trades on success
  - On any exception: logs with `exc_info=True`, returns `CandidateResult` with error set
  - `finally`: calls `CacheManager.clear_all_caches()`, deletes temp YAML (unless `retain_temp_yamls`)

**Tests to write:**
- `test_evaluate_returns_result_not_raises`: force a strategy exception — must return CandidateResult with error, not raise
- `test_significance_guard`: mock strategy returning 5 trades — returns REJECTED_INSUFFICIENT_TRADES
- `test_cache_cleared_on_success`: CacheManager.clear_all_caches called even on success
- `test_cache_cleared_on_failure`: CacheManager.clear_all_caches called even on exception
- `test_temp_yaml_deleted`: temp YAML file does not exist after evaluate returns

---

### Block 5 — `fitness.py` (~25 min)

**`fitness.py`:**
- `evaluate_fitness(result: CandidateResult, scenario: ScenarioProfile) -> FitnessResult`
  - Guard: if `not result.is_valid` → return FitnessResult with rejection (pass the error through)
  - Constraint evaluation order (fail fast, cheapest first):
    1. `actual_max_drawdown > scenario.max_drawdown`
    2. `actual_win_rate < scenario.min_win_rate`
    3. `actual_losing_streak > scenario.max_losing_streak`
    4. `actual_trades_per_week < scenario.min_trades_per_week`
    5. `actual_expectancy < scenario.min_expectancy`
    6. `actual_profit_factor < scenario.min_profit_factor`
  - On any constraint failure: return `FitnessResult` with `passed_constraints=False`, `failing_constraint` and `failing_value` set
  - On all pass: compute weighted score from scenario weights × normalised metric values → fitness in [0, 1]
  - Return `FitnessResult` with `passed_constraints=True`, `fitness_score` set

**Tests to write:**
- `test_constraint_order`: a candidate failing drawdown first — `failing_constraint == "max_drawdown"`
- `test_fitness_range`: valid candidate → fitness_score in [0.0, 1.0]
- `test_stateless`: calling twice with same inputs returns identical result
- `test_invalid_result_rejected`: `CandidateResult` with error set → FitnessResult with rejection

---

### Block 6 — `ranker.py` (~15 min)

**`ranker.py`:**
- `rank(store, run_id, stage, top_n) -> List[CandidateRecord]`
  - Calls `store.query_candidates(run_id=run_id, stage=stage, limit=top_n, order_by="fitness_score DESC")`
  - Filters: `passed_constraints = 1` only
- `rank_by_wfo(store, run_id, top_n) -> List[CandidateRecord]`
  - Queries `wfo_consistency_scores` joined to `candidates` for the run, orders by `wfo_consistency_score DESC`
- `rank_combined(store, run_id, stages, top_n) -> List[CandidateRecord]`
  - Used for Stage 4 input: top N from combined RANDOM + GA pool

**Tests to write:**
- `test_rank_returns_top_n`: 50 candidates stored, rank(top_n=10) returns exactly 10
- `test_rank_excludes_failed`: candidates with `passed_constraints=0` excluded
- `test_rank_ordering`: returned candidates are ordered by fitness descending

---

### Block 7 — `orchestrator.py` Skeleton (~30 min)

**`orchestrator.py`:**
- `run(config_path: Path) -> None` — entry point
- `_load_and_validate_config(config_path) -> dict` — YAML load + schema validation
- `_resume_or_start(store, config_path) -> RunMetadata` — hash check, resume or new run
- Eight stage methods, all as stubs that log "Stage N: [name] — stub, not yet implemented":
  - `_run_stage_0_init(config, store) -> RunMetadata`
  - `_run_stage_1_random_search(config, store, run_metadata) -> None`
  - `_run_stage_2_mc_prefilter(config, store, run_metadata) -> None`
  - `_run_stage_3_ga(config, store, run_metadata) -> None`
  - `_run_stage_4_wfo(config, store, run_metadata) -> None`
  - `_run_stage_5_mc_deep(config, store, run_metadata) -> None`
  - `_run_stage_6_sensitivity(config, store, run_metadata) -> None`
  - `_run_stage_7_report(config, store, run_metadata) -> None`
- Checkpoint skip logic implemented for all 8 stages (the real logic, not a stub)
- Stage 0 fully implemented: config validation, data file validation, WFO window validation (min 3), RunMetadata construction, store initialisation

**Tests to write:**
- `test_checkpoint_skip`: set checkpoint to RANDOM_SEARCH_COMPLETE, run orchestrator — Stage 1 stub is skipped
- `test_stage_0_validates_wfo_windows`: config with 2 WFO windows → raises ValueError
- `test_resume_rejects_changed_config`: prior run with different config hash → resume refused

---

### Block 8 — Integration Test (~20 min)

Write `tests/integration/test_single_candidate_roundtrip.py`:

```
Given: a valid backtest_template.yaml (test fixture, capital_accumulation scenario)
       with 3 WFO windows and the safe zone enabled

When:  orchestrator runs Stage 0 (full) + Stage 1 (single candidate, not parallelised)

Then:  - RunMetadata row exists in SQLite runs table with correct config_hash
       - CandidateRecord row exists in SQLite evaluations table with stage="RANDOM"
       - candidate_parameters row exists with correct parameter values
       - If candidate passed constraints: fitness_score is in [0, 1]
       - If candidate failed constraints: rejection_reason is set
       - CacheManager.clear_all_caches() was called exactly once
       - Temp YAML file does not exist (was cleaned up)
```

This test is the Phase 2 milestone. It must pass before Phase 2 is declared complete.

---

## Output Documents

By the end of this session, produce or update:

| Document | Action | Location |
|---|---|---|
| `src/backtesting/candidate_store.py` | Create | `src/backtesting/` |
| `src/backtesting/parameter_space.py` | Create | `src/backtesting/` |
| `src/backtesting/sampler.py` | Create | `src/backtesting/` |
| `src/backtesting/scenario.py` | Create | `src/backtesting/` |
| `src/backtesting/strategy_runner.py` | Create | `src/backtesting/` |
| `src/backtesting/fitness.py` | Create | `src/backtesting/` |
| `src/backtesting/ranker.py` | Create | `src/backtesting/` |
| `src/backtesting/orchestrator.py` | Create | `src/backtesting/` (skeleton) |
| `tests/backtesting/` | Create | Unit tests for all 8 modules |
| `tests/integration/test_single_candidate_roundtrip.py` | Create | Integration test |
| `CHANGE_LOG.md` | Append SESSION 3 block | Benchmark results, decisions confirmed/revised |
| `PROJECT_REPORT.md` | Update Phase 2 status | Mark deliverables complete |
| `CONTEXT.md` | Update current phase block | Phase 2 in-progress or complete |

---

## If the Session Runs Long

Priority order if forced to cut short:

1. D-02 benchmark (SQLite) — must complete; blocks store implementation
2. `candidate_store.py` — must complete; blocks everything else
3. `strategy_runner.py` — must complete; blocks integration test
4. `scenario.py` + `fitness.py` — needed for integration test
5. `parameter_space.py` + `sampler.py` + `ranker.py` + `orchestrator.py` — defer to next session if needed

Always write CHANGE_LOG.md session block and update CONTEXT.md before ending the session, even if cut short.

---

## Acceptance Criteria for Phase 2 Complete

- [ ] D-02 benchmark: 500 writes, 6 workers, zero errors — result logged in CHANGE_LOG.md
- [ ] D-01 benchmark: 50 candidates, direct-call mode — time per candidate logged
- [ ] `candidate_store.py`: all 5 unit tests pass, concurrent write test passes
- [ ] `parameter_space.py` + `sampler.py`: LHS produces no duplicates, values within zone bounds
- [ ] `scenario.py`: all 3 built-in scenarios load without error
- [ ] `strategy_runner.py`: never raises on any input — returns CandidateResult with error instead
- [ ] `fitness.py`: stateless, constraint order correct, fitness in [0, 1]
- [ ] `ranker.py`: returns top-N excluding failed candidates, ordered by fitness
- [ ] `orchestrator.py`: checkpoint skip logic works; Stage 0 validates WFO window count
- [ ] Integration test passes: single candidate full round-trip, all rows in SQLite correct
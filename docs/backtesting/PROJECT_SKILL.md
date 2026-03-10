---
name: backtester-project
description: >
  Use this skill whenever working on the Backtesting & Optimization Framework project
  OR the broker_support / eToro API integration project. Triggers: any mention of
  backtester, backtest pipeline, CandidateStore, GA engine, WFO evaluator, Monte Carlo
  engine, fitness evaluator, scenario profile, backtest_template.yaml, sensitivity
  evaluator, verdict engine, report generator, any module from src/backtesting/,
  broker_support, EToroClient, PositionTracker, CSVJournal, paper trading automation,
  eToro API, signal bridge, or CTP roadmap.
  Read this SKILL.md before writing any code, creating any file, or making any design
  decision for this project.
---
# CTP Project Skill — Backtesting + Broker Integration

## Project Status (2026-03-10, Block 9O end / Block 9P start)
```
BACKTESTING ENGINE:    V1 PRODUCTION DECLARED (f545f0f2, 2026-03-08)
                       Full-history calibration COMPLETE — Block 9O
                       Overnight production run IN PROGRESS — backtest_V1_01.yaml v3.0.0
                       _SIGMOID_SCALE = 310.0 CONFIRMED (N=231, stdev=620.09)
                       Next session: analyse overnight run → V1 closure if verdicts exist

BROKER INTEGRATION:    broker_support package — connection confirmed, 4 bugs to fix
                       5-step plan scoped. Empirical demo history test PENDING (P1).

CTP ROADMAP:           6-phase plan (v1.2). Phase 0 (broker fixes) + Phase 1 (full-history)
                       running in parallel NOW. Phase 2 = automated paper trading.
                       V2 architecture blueprint complete (RawDataStore+WindowSlicer+SignalCache).
                       V3 = Strategy Setup Builder (meta-optimiser over configuration space).
```

---
## DUAL-TRACK CONTEXT

### Track A — Backtesting Full-History
Overnight production run in progress: `backtest_V1_01.yaml` v3.0.0 (13 windows, all stages).

**Calibration constants — CONFIRMED FINAL (full-history track):**
```python
_SIGMOID_SCALE: float = 310.0           # N=231, stdev=620.09 (runs 2912e028, 519f84e2) CONFIRMED
_MAX_EXPECTED_DRAWDOWN: float = 2_500.0 # full-history track — correct
```
Do NOT use 221.1 — that was computed from GA 2-window partial samples in run 9f73d667,
not from full 7-window WFO. _SIGMOID_SCALE calibration requires Stage 1+4-only runs.

**Stage 1 distributions (38-month — stable across 4 runs):**
```
win_rate:      min=0.0944  avg=0.1477  max=0.2336
expectancy:    min=-3.28   avg=-1.87   max=-0.92
profit_factor: min=0.59    avg=0.813   max=0.93
trades/week:   min=1.89    avg=37.38   max=78.93
losing_streak: min=24      avg=49.6    max=94
```

**Confirmed constraint values for full-history runs:**
```yaml
min_win_rate: 0.11         # removes bottom ~5% only
min_expectancy: -2.0       # targets top ~60-65%
min_profit_factor: 0.75    # no failures observed
max_losing_streak: 200     # 38-month max observed=94
min_trades_per_week: 3.0   # unchanged
# max_drawdown: REMOVED    — accumulates over 38 months
mc_prefilter: false        # compounds 38-month perturbation into false ruin — DISABLE
go_wfo_floor: 0.40         # structural W02/W04/W05 suppress scores — 0.65 = zero verdicts
borderline_wfo_floor: 0.25 # maintains separation from go floor
max_workers: 2             # MANDATORY — OOM confirmed at 6 on cold cache (8GB RAM)
```

**RSI confirmed dead — RSI-SENS-2 CLOSED:**
Zero delta on rsi_period, rsi_overbought, rsi_oversold across ALL 5 sensitivity candidates
in run 9f73d667. Confirmed across 6+ calibration runs. Remove RSI from V2 search space.

### Track B — Broker Integration (pending fixes)
**Project path**: `E:\Trading\Broker_support`
**Confirmed working**: `_make_request()`, `test_connection()`, portfolio fetch, `CSVJournal`,
`PositionTracker` snapshot logic.

**Four bugs to fix before any new development**:
1. `get_portfolio()` endpoint: `/demo/portfolio` → `/demo/pnl` (official spec)
2. Orphaned function: second `fetch_closed_trades` in client.py is a free function — indent
   as class method, delete stub
3. Date param: `from`/`fromDate` → `minDate` (confirmed official param name)
4. Trade alias: `Field(alias='id')` → `Field(alias='positionId')`; add `fees`, `leverage`,
   `sl_rate`, `tp_rate`

---
## Backtesting Pipeline (in order — do not reorder)
```
Stage 0: Validation & Init     (min 3 WFO windows; param name validation vs _PARAM_KEY_MAP) ✅
Stage 1: Random Search         (LHS/random, significance guard, constraint filter) ✅
Stage 2: MC Pre-Filter         (re-evaluates candidates; cheap ruin screen) ✅ — DISABLED full-history
Stage 3: GA                    (WFO-aware: random 2 windows/generation + diversity penalty) ✅
Stage 4: Full WFO              (all windows, 4-metric composite consistency score) ✅
Stage 5: MC Deep               (full iterations, all perturbation types, WFO survivors only) ✅
Stage 6: Parameter Sensitivity (±1 step only [OPT-01], fitness delta map, spike = borderline) ✅
Stage 7: Report & Output       (HTML + checklist + JSON/Parquet + SQLite + YAML) ✅
```
All stages fully implemented. OOS gate: implemented but off by default (enforce_oos_gate: false).
MC Pre-Filter: disable for full-history (38-month) runs — see L-54.

**Stage input count relationships (CRITICAL — misreading causes false bug reports):**
```
Stage 5 MC Deep input:    monte_carlo.deep.input_count  (default 10) — top N by WFO score
Stage 6 Sensitivity input: sensitivity.input_count       (default 5)  — top N by WFO score
Stage 7 Verdict input:    sensitivity.input_count        (default 5)  — top N by WFO score (SAME)
```
Stage 7 uses `sens_config.get("input_count", 5)` — this is NOT a bug. Sensitivity and Verdict
are always paired on the same candidate set. MC Deep may evaluate MORE candidates than get
verdicts. A candidate appearing in Stage 5 MC results but NOT in Stage 7 verdicts is expected
behaviour when that candidate ranks outside top-N by WFO for sensitivity/verdict.

---
## Architecture Rules (non-negotiable)
```python
# Contracts: always frozen dataclasses, never raw dicts crossing module boundaries
# Fail fast: invalid config raises at construction, no silent fallbacks
# Datetime: datetime.now(timezone.utc) — NEVER datetime.utcnow() (deprecated Python 3.12+)
# Paths: pathlib.Path + src/utils/paths.py — never hardcoded separators
# Concurrency: ProcessPoolExecutor spawn mode — never multiprocessing fork
# Candidate ID: always CandidateParameterSet.create() — deterministic SHA-256 of params dict
# "Candidate" is NOT a contract type — use CandidateParameterSet
# LIVE_APPROVED: never set in code — operator-only manual action
# strategy_runner.run(): mode_override="core" — NOT mode="core"
# Timing: logger.info only — never print(), never debug flags
# store.close(): always in finally block
# Mutation: snap-then-clamp order — never clamp-then-snap
# Temp YAML filenames: full candidate_id (64 chars) — NEVER truncate (B9H-003)
# expand_zones() returns Dict[str, Dict[str, List]] — per-param lists, NOT Cartesian product (B9F-001)
# _lhs_sample() always returns exactly n candidates — no cap on n (B9I-001)
# actual_net_pnl / actual_total_trades do NOT exist in evaluations table — never query (B9I-002)
# net_pnl for calibration: wfo_window_results.net_pnl only (Stage 4)
# wfo_collapse_drawdown_threshold: default 400.0 pts (DAX). Must be pts, not fraction (COLLAPSE-UNIT)
# scenario.py wires via s.get("wfo_collapse_drawdown_threshold", 400.0) — YAML field optional
# contracts.py validates threshold > 0.0 only — no upper bound (any pts value valid)
# All normalisation constants are DAX-specific. V2-RAR will make them dimensionless.
# run_mc() Stage 2 MUST pass ruin_threshold=scenario.mc_prefilter_ruin_threshold (B8B-013)
# crossover(): zone guard first — if parent zones differ, return parent_a unchanged (B9B-001)
# bollinger_width_ma: step must be 1 (int type — float step causes silent errors) (L-41)
# Do NOT open raw sqlite3.connect() to backtester DB — always use CandidateStore API (L-40, B8-009)
# exploration zone: all filters enabled=true — no toggle params in zone definition
# Stage 0 validates all zone param names against _PARAM_KEY_MAP before any evaluation
# scenario.py constraint loader: use ct.get(key, default) NOT ct[key] — hard lookup = Stage 0 KeyError (B9N-001)
# max_drawdown constraint: DO NOT use for Stage 1 date ranges > 3 months (accumulates across full range)
# max_losing_streak: DO NOT set ≤50 for Stage 1 date ranges > 3 months (observed max 94 over 38 months)
# _MAX_EXPECTED_DRAWDOWN is dataset-range-specific — 3-month: 1_000.0, 38-month: 2_500.0 — do NOT mix tracks
# config["key"] hard lookup fails for any optional stage config block when that stage is
#   disabled and the YAML omits the block. Pattern: config.get("key", {}) + defaults dict.
#   Confirmed affected: mc_prefilter (B9O-003), genetic (B9O-004).
# mc_prefilter: DISABLE for full-history runs — 38-month MC perturbation compounds into false ruin (L-54)
# max_workers: HARD LIMIT 2 for full-history WFO — cold-cache pd.read_parquet() 897MB × workers = OOM (L-53)
# _SIGMOID_SCALE calibration: use Stage 1+4 only runs (pure full-window WFO net_pnl distribution).
#   GA partial-window (2-window) net_pnl produces different stdev — do NOT use for calibration.
# RSI parameters (rsi_period, rsi_overbought, rsi_oversold): zero sensitivity delta confirmed
#   across 6+ full-history runs. Remove from V2 search space. RSI-SENS-2 CLOSED.
#
# verdict.py — CONFIRMED CORRECT (reviewed 2026-03-10, orchestrator.py + candidate_store.py verified):
#   - Uses >= on go_wfo_floor. Float comparison correct.
#   - NO_GO fires only if wfo_pillar_no_go OR mc_pillar_no_go.
#   - window_collapse_flag=True → BORDERLINE (modifier only), never NO_GO.
#   - get_mc_result(candidate_id, MCMode.DEEP): uses mode.value="deep" in SQL — correct.
#   - _write_mc_result: writes mode=result.mode.value="deep" — consistent with read path.
#   - Stage 7 uses sens_config["input_count"] (default 5) for verdict candidates.
#   - Stage 5 uses mc_config["input_count"] (default 10) — may evaluate MORE than get verdicts.
#   - Candidate in Stage 5 MC table but not Stage 7 verdicts = expected — outside top-5 WFO.
#   - VERDICT-BUG is CLOSED — was a misread of query output. No bug exists.
#
# Stage 7 input count: sens_config.get("input_count", 5) — NOT a bug.
#   This is correct by design: sensitivity and verdict are always paired on the same candidate set.
#   If you want more verdicts, raise sensitivity.input_count in YAML (not monte_carlo.deep.input_count).
#   To give verdicts to ALL MC Deep candidates: set sensitivity.input_count >= monte_carlo.deep.input_count.
```

---
## Calibration Constants — TWO TRACKS (NEVER MIX)

### 3-Month Production Track (V1 — FROZEN)
```python
_SIGMOID_SCALE: float = 131.0           # stdev(net_pnl)=261.98 × 0.5 (run 87712cab)
_MAX_EXPECTED_VARIANCE: float = 100_000.0
_MAX_EXPECTED_DRAWDOWN: float = 1_000.0
# Restore these after full-history work. V2-RAR will eliminate this two-track complexity.
```

### Full-History Track (38-month — CONFIRMED)
```python
_SIGMOID_SCALE: float = 310.0           # N=231, stdev=620.09 (runs 2912e028, 519f84e2) CONFIRMED
_MAX_EXPECTED_DRAWDOWN: float = 2_500.0 # currently applied in code
```

### Shared (both tracks)
```
wfo_collapse_drawdown_threshold        = 400.0 pts  (per-window — never changes)
normalisation_expectancy_ref_pts       = 3.0 pts
normalisation_freq_ref_trades_per_week = 20.0 (CAL-01: raise to 50.0 before V2 only)
mc_prefilter_ruin_threshold            = 0.25 (capital_accumulation — Stage 2 disabled full-history)
```

---
## OOM Architecture — max_workers Constraint
```
Root cause (confirmed): pd.read_parquet() on 897MB LTF Parquet × N workers simultaneously
on cold cache (run_cleaner wipes cache before every run) = N × 897MB peak → PyArrow OOM.

Cold cache peak:
  6 workers: 6 × 897MB = 5.38GB → system crash (confirmed)
  2 workers: 2 × 897MB = 1.79GB → within 8GB RAM budget (confirmed stable)

Warm cache (subsequent workers): 20MB pkl slice per worker — no constraint.

max_workers: 2 is a HARD LIMIT for full-history runs until B9O-009 (V2 shared memory).
B9O-008 (slice-before-sort) reduces sort_index peak but cannot reduce read_parquet peak.

V2 fix: RawDataStore loads files once in parent process → WindowSlicer places slices in
named SharedMemory blocks → workers map shm (zero-copy, ~20MB per worker).
With shared memory: 6 workers × 20MB = 120MB total. max_workers constraint removed.
```

---
## Patches Applied (Block 9O — complete)
| Patch | File | Status | Description |
|-------|------|--------|-------------|
| B9O-001 | data_loader.py | ✅ | Sliced strategy cache (apply_date_range=False path) |
| B9O-002 | run_cleaner.py | ✅ | Pre-run cache + temp YAML cleaner |
| B9O-003 | mc_engine.py | ✅ | config.get() for mc_prefilter block |
| B9O-004 | ga_engine.py | ✅ | config.get() + _GA_DEFAULTS dict |
| B9O-005 | orchestrator.py | ✅ | stages: toggle enforcement |
| B9O-006 | data_loader.py | ✅ | Slice-before-cache for LTF/HTF |
| B9O-007 | data_loader.py | ✅ | Warmup-buffered df_full for WFO windows |
| B9O-008 | data_loader.py | ✅ | Slice-before-sort for LTF loading peak |

**data_loader.py current version: 3.5.0**

---
## Module Map (current state — all 9K + 9O patches applied)
```
orchestrator.py          — All stages. B8-009 store API. B8B-013 ruin_threshold.
                           B9O-005: stages: toggle guards in _execute_pipeline().
                           _promote_random_to_mc_pass() helper added.
                           Stage 7 uses sens_config["input_count"] for verdict set —
                           by design (sensitivity and verdict always paired). CONFIRMED CORRECT.
fitness.py               — B8B-003: normalisation_expectancy_ref_pts.
contracts.py             — B8B-003: new field. All invariants current.
candidate_store.py       — B8-009: get_incomplete_run/get_any_incomplete_run.
                           get_mc_result(candidate_id, MCMode.DEEP) uses mode.value="deep" — correct.
                           query_mc_results(run_id, mode: str) — plain string, not enum. For
                           diagnostic queries only (query_run.py). Not used in verdict path.
strategy_runner.py       — B9F-005/B9H-003.
parameter_space.py       — B9F-001.
sampler.py               — B9I-001.
scenario.py              — COLLAPSE-UNIT + B8B-003 wired + B9N-001 ct.get() fix (partial).
ga/crossover.py          — B9B-001: zone guard.
ga/ga_engine.py          — B9O-004: config.get("genetic", {}) + _GA_DEFAULTS dict.
monte_carlo/mc_engine.py — B8B-013: ruin_threshold param. B9O-003: config.get() fixes.
wfo/consistency_scorer.py — _SIGMOID_SCALE=310.0 (full-history). _MAX_EXPECTED_DRAWDOWN=2_500.0.
evaluation/verdict.py    — CONFIRMED CORRECT 2026-03-10 (full review). >= on go_wfo_floor.
                           M-01 applied. Two-pillar logic verified against orchestrator +
                           candidate_store. No bugs found anywhere in the verdict pipeline.
strategies/core/data_loader.py — v3.5.0: B9O-001/006/007/008 all applied.
report_generator.py      — B8C-002/003 open (deferred).
src/utils/run_cleaner.py — B9O-002: new utility. Pre-run cache + temp cleaner.
scripts/runners/run_backtester.py — B9O-002: calls clean_environment() before every run.
```

---
## CandidateStore — Two MC Query Methods (IMPORTANT DISTINCTION)
```python
# For pipeline verdict computation (Stage 7):
store.get_mc_result(candidate_id: str, mode: MCMode) -> Optional[MCResult]
  # Uses MCMode enum: MCMode.DEEP, MCMode.PRE_FILTER
  # mode.value used in SQL WHERE clause: "deep" or "pre_filter"
  # Returns single MCResult object or None

# For diagnostic/reporting queries (query_run.py):
store.query_mc_results(run_id: str, mode: str) -> List[Dict]
  # Uses plain string: "deep" or "pre_filter"
  # Returns list of plain dicts for all candidates in a run
  # NOT used in verdict computation path

# These are DIFFERENT methods. query_mc_results is never called by orchestrator.
# get_mc_result is what Stage 7 uses. Both correctly use mode="deep" string in SQL.
```

---
## CandidateStore Write API (verified)
```python
store.write_candidate(record: CandidateRecord)
store.write_candidate_stub(candidate: CandidateParameterSet, run_id, stage, generation)
store.write_wfo_window_result(result: WFOWindowResult, run_id: str)
store.flag_candidate_wfo_insufficient(candidate_id: str, run_id: str)
store.write_wfo_consistency_score(score: WFOConsistencyScore, run_id: str)
store.write_mc_result(result: MCResult, run_id: str)
store.write_sensitivity_profile(profile: SensitivityProfile, run_id: str)
store.write_verdict(verdict: VerdictResult, run_id: str)
store.initialise_run(run_metadata: RunMetadata)
store.set_checkpoint(run_id: str, checkpoint: Checkpoint)  # orchestrator ONLY
store.flush()
store.close()
# There is NO write_fitness_result() method
# get_candidate_result() returns trades=None / metrics=None ALWAYS — do NOT use for MC input
# write_candidate_stub() MUST be called before any FK-referencing write (B9G-001)
# _wfo_result_id() is deterministic SHA-256[:32] of run_id+candidate_id+window_id (B9H-002)
# INSERT OR REPLACE on wfo_window_results deduplicates correctly (B9H-002)
```

---
## Evaluate / Strategy Signatures
```python
def evaluate_window(
    candidate: CandidateParameterSet,
    window: WFOWindow,
    base_yaml_path: Path,
    temp_dir: Path,
    scenario: ScenarioProfile,
    min_significant_trades: int = 30,
    oos_gate_enabled: bool = False,
) -> WFOWindowResult: ...

def evaluate(
    candidate: CandidateParameterSet,
    base_yaml_path: Path,
    temp_dir: Path,
    min_significant_trades: int = 30,
    retain_temp_yamls: bool = False,
    date_start: Optional[Union[date, datetime]] = None,
    date_end: Optional[Union[date, datetime]] = None,
) -> CandidateResult: ...
```

---
## Lessons Learned (L-01 through L-56)
```
L-01 through L-48: see Block 9K/9O CONTEXT.md and prior SKILL.md versions

L-49: df_full in DataBundle is consumed by TradeSimulator → RiskManager ONLY.
      WFO window evaluations need only a warmup-buffered slice (window_start − 200 bars).
      DataLoader is the correct fix location — not frozen TradeSimulator/RiskManager.

L-50: Diagnostic test false positive: assertion accidentally matched _WFO_WARMUP_BARS
      via a different code path. Always verify the constant exists in target file
      before trusting a test that checks for it.

L-51: run_cleaner.py clears cache before every run. OOM on cache miss (first worker
      per window) is the actual failure mode — not stale cache.

L-52: B9O-006 fixed what gets cached (the slice) but not the sort_index() peak
      DURING loading. sort_index() calls .copy() on the full DataFrame internally —
      856MB for the 22.4M-row LTF file — before the B9O-006 slice runs.
      Fix: check is_monotonic_increasing and slice before sort for sorted Parquet files.
      This is B9O-008.

L-53: max_workers=6 OOM root cause: pd.read_parquet() on 897MB LTF file × 6 workers
      simultaneously on cold cache = 5.38GB peak → PyArrow C++ allocator failure inside
      Scanner.to_table(). B9O-008 cannot help — slicing happens AFTER read_parquet() returns.
      Fix: max_workers=2 (1.79GB peak). Permanent fix: V2 shared memory architecture (B9O-009).
      Stage 1 works at max_workers=6 because evaluations are staggered — workers do not
      all hit read_parquet simultaneously. Stage 4 WFO dispatches ALL tasks at once → cold
      cache race condition. B9O-009 eliminates this entirely.

L-54: MC pre-filter is NOT safe for 38-month continuous evaluation. MC perturbation
      (slippage, spread noise, shuffle) compounds over 38-month equity curve producing
      false ruin signals. Stage 2 passed 1/18 candidates in run 9f73d667, eliminating
      WFO-0.92 and WFO-0.80 candidates before GA could build on them.
      Fix: disable mc_prefilter for all full-history runs. Stage 4 WFO is the primary gate.
      Stage 5 MC Deep on top WFO survivors is the correct ruin screen.
      V2 fix: MC Deep should evaluate on per-window equity curves (3-month slices),
      not the full 38-month dataset.

L-55: Stage 7 verdict input_count comes from sensitivity.input_count, not
      monte_carlo.deep.input_count. Stage 5 MC Deep may evaluate more candidates than
      receive verdicts. A candidate in Stage 5 MC table but absent from Stage 7 verdicts
      is expected behaviour — not a bug. It ranked outside top-N for sensitivity/verdict.
      VERDICT-BUG (f86f7e6c491a) was a misread of query output: the candidate was in
      Stage 5 MC results (top 10) but never in Stage 7 verdicts (top 5) — correct design.
      verdict.py, orchestrator.py, and candidate_store.py all confirmed correct.
      To give verdicts to all MC-evaluated candidates: raise sensitivity.input_count
      to match or exceed monte_carlo.deep.input_count in the YAML.

L-56: Before declaring a verdict logic bug, cross-check the Stage 5 and Stage 7 input
      counts in the YAML. They use DIFFERENT config keys. Mismatched counts produce
      a query output where MC results exist for candidates with no verdict — this is
      not a bug, it is the pipeline's designed tiered selection behaviour.
```

---
## Open Issues (carry forward to Block 9P)
```
B9O-009  V2 shared memory architecture (RawDataStore + WindowSlicer + SignalCache).
         Eliminates max_workers constraint. Required before raising to 6 workers.
         See CTP_ROADMAP.md Phase 3 (V2) for full blueprint.
         STATUS: OPEN — deferred to Phase 3.

MC-DEEP-FULLHIST  MC Deep ruin compounds over 38-month equity curve — architectural issue.
                  All top WFO candidates show ruin > 0.80 in full-history runs.
                  Candidates evaluated on 38-month continuous equity curve; MC perturbation
                  (slippage, spread noise, shuffle) accumulates over 38 months producing
                  ruin that would not appear in 3-month window evaluation.
                  V2 fix: evaluate MC on per-window 3-month slices, not full 38-month range.
                  STATUS: OPEN — will manifest again in overnight production run.
                  Watch for: if overnight run also shows all ruin > 0.40, this is confirmed
                  and the V2 MC fix must be prioritised before Phase 2 (paper trading).

WINZIP-32  WinError 32 temp YAML file lock during GA stage worker teardown on Windows.
           Cosmetic — pipeline completes. V2 fix: per-worker temp dirs, clean at worker exit.
           STATUS: KNOWN / DEFERRED to V2.

RSI-SENS-2  RSI confirmed dead across 6+ runs. Remove from V2 search space.
            STATUS: CONFIRMED CLOSED — action deferred to V2 implementation.

VERDICT-BUG  CLOSED — was a misread of query output (L-55, L-56).
             f86f7e6c491a appeared in Stage 5 MC (top 10) but not Stage 7 verdicts (top 5).
             This is correct — it ranked outside top-5 WFO for sensitivity/verdict.
             verdict.py, orchestrator.py, candidate_store.py all confirmed correct.
```

---
## What NOT to Do
- Do not guess or reconstruct code not available — ask for the file
- Do not modify `src/strategies/` — strategy architecture is frozen
- Do not use `analytics` mode — `core` mode only (`mode_override="core"`)
- Do not add `print()` — use `logger.info`
- Do not set `deployment_status = LIVE_APPROVED` in code
- Do not use `datetime.utcnow()`
- Do not use `Candidate` type — use `CandidateParameterSet`
- Do not patch inside ProcessPoolExecutor workers (spawn boundary)
- Do not use `e2e_test` scenario for production runs
- Do not clamp-before-snap in mutation
- Do not use trade.pnl — use trade.pnl_points (B9F-004)
- Do not call store.get_candidate_result() for MC input (L-15)
- Do not truncate candidate_id in temp YAML filenames (B9H-003)
- Do not use uuid4() as result_id in wfo_window_results (B9H-002)
- Do not call expand_zones() expecting List[Dict] (B9F-001)
- Do not cap _lhs_sample() at min_universe_size (B9I-001)
- Do not query actual_net_pnl or actual_total_trades from evaluations (B9I-002)
- Do not set wfo_collapse_drawdown_threshold as fraction for DAX — must be pts (COLLAPSE-UNIT)
- Do not enable exploration zone before B9B-001 patch (crossover zone guard) is applied
- Do not open raw sqlite3.connect() to backtester DB (L-40)
- Do not call run_mc() in Stage 2 without passing ruin_threshold (L-38, B8B-013)
- Do not use float step on int-type zone parameters (L-41)
- Do not add filter toggle params to exploration zone — fix filters enabled=true
- Do not use max_drawdown as Stage 1 constraint when Stage 1 date range > 3 months (L-43)
- Do not use max_losing_streak ≤ 50 when Stage 1 date range > 3 months (L-43)
- Do not use ct["key"] for optional constraint fields in scenario.py — use ct.get() (L-44, B9N-001)
- Do not mix 3-month and full-history calibration constants — they are separate tracks
- Do not change _SIGMOID_SCALE = 131.0 without restoring it for 3-month production runs
- Do not call eToro GET /demo/portfolio — correct endpoint is /demo/pnl
- Do not use 'from'/'fromDate' for eToro trade history — correct param is 'minDate'
- Do not architecture broker close-price enrichment before running empirical demo history test
- Do not use config["key"] hard access for any optional stage config block (L-45, B9O-003/004)
- Do not set max_workers > 2 for full-history WFO runs (L-53) — OOM confirmed at 6
- Do not run full-history YAML without pre-clearing cache — use run_cleaner.py (L-47)
- Do not assume stages: YAML toggles are enforced without checking orchestrator guard (L-48)
- Do not enable mc_prefilter for full-history (38-month) runs (L-54) — false ruin confirmed
- Do not use _SIGMOID_SCALE=221.1 — computed from GA 2-window samples, wrong distribution
- Do not calibrate _SIGMOID_SCALE from runs with GA enabled — Stage 1+4 only runs only
- Do not assume a candidate in Stage 5 MC but absent from Stage 7 verdicts is a bug (L-55)
  Check Stage 5 vs Stage 7 input_count in YAML first — they use different config keys
- Do not raise monte_carlo.deep.input_count expecting more verdicts — raise sensitivity.input_count
- Do not re-examine verdict.py, orchestrator.py, or candidate_store.py for VERDICT-BUG — CLOSED

---
## Platform
- **OS**: Windows 10, Python 3.13.12
- **Timezone**: OHLCV/signals CET/CEST; pipeline timestamps UTC
- **Paths**: always `src/utils/paths.py`
- **DB**: `outputs/backtesting/backtester.db`
- **Production YAML**: `configs/backtesting/backtest_V1_01.yaml` (v3.0.0) — overnight run active
- **Calibration YAML**: `configs/backtesting/backtest_V3_calibration_fullpipeline.yaml` (v5.0.0)
- **Broker project**: `E:\Trading\Broker_support` (broker-support v0.1.0)

---
## Session Deliverables (end of every session)
- Updated `outputs/CONTEXT_<block>.md`
- `outputs/RUN_ANALYSIS_<run_id>.md` (if production run analysed)
- `outputs/ARCHITECTURE_<block>_DELTA.md` (if structural changes)
- `outputs/OPERATOR_RUNBOOK_<block>_DELTA.md` (if operator-visible changes)
- Updated `SKILL.md` in outputs/ (replace user skill)
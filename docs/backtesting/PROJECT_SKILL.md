---
name: backtester-project
description: >
  Use this skill whenever working on the Backtesting & Optimization Framework project.
  Triggers: any mention of backtester, backtest pipeline, CandidateStore, GA engine,
  WFO evaluator, Monte Carlo engine, fitness evaluator, scenario profile, backtest_template.yaml,
  sensitivity evaluator, verdict engine, report generator, or any module from src/backtesting/.
  Read this SKILL.md before writing any code, creating any file, or making any design
  decision for this project.
---
# Backtesting Framework — Project Skill
## What This Project Is
A fully automated 8-stage optimization pipeline for the WBWSStrategy. Given a parameter
space definition and a strategy base config, it searches for robust parameter combinations
and produces a verdict (auto_go / borderline / no_go) per candidate.
**Current status (2026-03-05)**: Block 9E complete.
- Tests: ~345 green, 0 skipped, 0 failed (Block 9C baseline; 9D/9E add no new tests)
- Next: Block 9F — first real pipeline run + calibration (B8B-012, B8B-003)
- Pre-production blocker: B8B-012 (sigmoid scale — needs first real run data)
---
## Pipeline (in order — do not reorder)
```
Stage 0: Validation & Init     (min 3 WFO windows; param name validation vs _PARAM_KEY_MAP)
Stage 1: Random Search         (LHS/random, significance guard, constraint filter) ✅ IMPLEMENTED
Stage 2: MC Pre-Filter         (cheap — ruin screen vs scenario.mc_prefilter_ruin_threshold) ✅ IMPLEMENTED
Stage 3: GA                    (WFO-aware: random 2 windows/generation + diversity penalty) ✅ IMPLEMENTED
Stage 4: Full WFO              (all windows, 4-metric composite consistency score) [STUB]
Stage 5: MC Deep               (full iterations, all perturbation types, WFO survivors only)
Stage 6: Parameter Sensitivity (±1/±2 step, fitness delta map, spike = borderline)
Stage 7: Report & Output       (HTML + checklist + JSON/Parquet + SQLite + YAML)
```
Stages 0–3, 5–7: fully implemented. Stage 4: stub that logs and advances checkpoint.
OOS gate: implemented but off by default (enforce_oos_gate: false in config).
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
```
---
## CandidateStore Write API (verified)
```python
store.write_candidate(record: CandidateRecord)
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
# query_mc_results: mode is plain string "deep"/"pre_filter" — NOT MCMode enum
```
---
## evaluate_window() Signature (critical — positional args must match pool.submit)
```python
def evaluate_window(
    candidate: CandidateParameterSet,
    window: WFOWindow,
    base_yaml_path: Path,
    temp_dir: Path,
    scenario: ScenarioProfile,
    min_significant_trades: int = 30,
    oos_gate_enabled: bool = False,      # 7th arg — B8B-005
) -> WFOWindowResult: ...

# pool.submit call in wfo_engine.py must pass all 7 positional args in order:
pool.submit(evaluate_window, candidate, window, base_yaml_path, temp_dir,
            scenario, min_significant_trades, oos_gate_enabled)
```
---
## OOS Gate Details (B8B-005 — implemented Block 9E)
```python
# IS/OOS split: _IS_FRACTION = 0.70 (70% IS, 30% OOS by calendar days)
# is_end_date = window.start_date + timedelta(days=int(total_days * 0.70))
# oos_delta = oos_fitness - is_fitness  (both [0,1]; negative = OOS underperforms IS)
# oos_delta = None when:
#   - oos_gate_enabled=False (GA lightweight mode, default)
#   - window total_days < 2 (too short to split)
#   - IS evaluation fails (error, significance guard, missing metrics)
#   - OOS evaluation fails (error, missing metrics) — NOT when OOS fails constraints
# OOS constraint-fail: oos_fitness = 0.0 (floor) — large negative delta, not None
# consistency_scorer._check_oos_gate: abs(median_delta) > oos_degradation_threshold
#   default threshold = 0.50 (50% fitness point drop — very lenient, calibrate after run)
# enforce_oos_gate: false by default — activate only after calibrating threshold
```
---
## Stage 1–3 Implementation Details
```python
# Stage 1 (_run_stage_1_random_search):
#   expand_zones(config) → sample_lhs() or sample_random()
#   → strategy_runner.evaluate() → fitness.evaluate_fitness()
#   → _build_candidate_record(..., CandidateStage.RANDOM) → store.write_candidate()
#   All candidates written (pass AND fail)
#   CandidateRecord.stage = CandidateStage.RANDOM.value  (string "RANDOM")

# Stage 2 (_run_stage_2_mc_prefilter):
#   ranker.rank(store, run_id, CandidateStage.RANDOM, top_n)  # RANDOM-pass only
#   → run_mc(PRE_FILTER) → store.write_mc_result()
#   → ruin > scenario.mc_prefilter_ruin_threshold → MC_PREFILTER_FAIL else PASS
#   → _build_candidate_record_from_existing(record, run_id, new_stage)
#   ruin threshold: scenario.mc_prefilter_ruin_threshold  (NOT config dict)

# Stage 3 (_run_stage_3_ga):
#   Build List[WFOWindow] from config["walk_forward"]["windows"] (date.fromisoformat)
#   ga_config = dict(config)  # shallow copy
#   ga_config["_base_yaml_path"] = str(base_yaml_path)  # B9B-003 injection
#   run_ga(store, run_id, scenario, wfo_windows, ga_config, seed=run_metadata.ga_seed)
```
---
## Known Confirmed Bugs / Fixed (do not re-open)
```
H-01: FALSE POSITIVE — strategy_runner.evaluate() DOES accept date_start/date_end.
H-02: FIXED (7A) — write_wfo_window_result + flag_candidate_wfo_insufficient were absent.
H-03: FALSE POSITIVE — wfo_evaluator passes window dates correctly.
I-07: FIXED (7A) — datetime.utcnow() → datetime.now(UTC) in wfo_evaluator.py.
B8B-001: FIXED — NaN guard in fitness.py.
B8B-018: FIXED — wfo_evaluator.py: total_pnl_points, expectancy_points.
B8C-001: FIXED — contracts.py: report_emphasis validation.
B9A-002: FIXED — Stage 1 stub advances RANDOM_SEARCH_COMPLETE checkpoint.
B9A-001: FIXED (9D) — orchestrator Stages 5–7: ranker.rank_by_wfo() (typed).
B9A-003: FIXED (9D) — Stage 6 spike_threshold → scenario.verdict_sensitivity_spike_threshold.
B9C-007: FIXED (9D) — sampler._lhs_sample() sort key → float(x).
B9C-006: FIXED (9D) — sampler.sample_random() docstring.
B9C-004: FIXED (9D) — wfo_engine.run_wfo() empty candidates guard.
B9C-005: FIXED (9D) — parameter_space._range_values() Decimal(str(step)).
B8-006:  FIXED (9D) — twin key map comments in strategy_runner + yaml_generator.
B8B-005: FIXED (9E) — IS/OOS split in wfo_evaluator + oos_gate_enabled pass-through in wfo_engine.
```
---
## Critical Open Findings
```
B9F-001 [P1 BLOCKER] parameter_space.py
  expand_zones() calls itertools.product() which ENUMERATES the full Cartesian product.
  exploration zone: ~387 trillion combinations → OOM / process hangs forever.
  safe zone: ~2 million combinations → ~520MB RAM, feasible on 64-bit / ≥4GB free RAM.
  Workaround for first run: exploration.enabled: false in YAML (enabled guard confirmed
  present at line 34 of parameter_space.py — setting enabled: false is sufficient).
  Fix: refactor expand_zones() to return Dict[str, Dict[str, List]] (per-param value
  lists, not full product); refactor sampler._lhs_sample() to accept per-param lists
  directly and sample via per-dimension stratified draws without enumeration.

B8B-012 [PRE-PROD BLOCKER] consistency_scorer.py
  _sigmoid_normalise scale=0.10 calibrated for unit fractions, not points.
  Fix after first real run: measure net_pnl distribution → set scale ≈ stdev * 0.5.
  Metrics are in pips/points — not currency.
B8B-003 [P3] fitness.py
  expectancy_norm hardcoded at / 3.0 pts. Calibrate after first real run.
B8-009 [P3] orchestrator.py
  Raw sqlite3 in _resume_or_start bypasses CandidateStore contract.
B9B-001 [P3] crossover.py
  No zone-name guard for cross-zone parents.
B8B-013 [P3] mc_engine.py
  ruin_threshold dual-source: config dict + ScenarioProfile.mc_prefilter_ruin_threshold.
B8B-011 [P3] consistency_scorer.py
  fraction_positive_windows uses fixed 0.0 floor.
B8C-002, B8C-003 [P3] report_generator.py — deferred
B9C-008 [P3] sampler.py — deferred
OPT-01 target (not achieved): Stage 6 ≤ 200s. Pool reuse applied but ~300s still.
```
---
## Test Import Convention (CRITICAL — violating causes circular import at collection)
```python
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
from src.utils.paths import PROJECT_ROOT
from src.backtesting.contracts import (...)   # BEFORE candidate_store
from src.backtesting.candidate_store import CandidateStore
```
---
## Critical Patch Targets (Windows spawn mode)
```python
patch("src.backtesting.orchestrator.evaluate_sensitivity", ...)   # Stage 6
patch("src.backtesting.monte_carlo.mc_engine.run_mc", ...)        # Stage 5
patch("src.backtesting.wfo.wfo_engine.evaluate_window", ...)      # wfo_engine tests
patch("src.backtesting.wfo.wfo_engine.compute_consistency", ...)  # wfo_engine tests
patch("src.backtesting.orchestrator.evaluate", ...)               # Stage 1
patch("src.backtesting.orchestrator.run_mc", ...)                 # Stage 2
patch("src.backtesting.orchestrator.run_ga", ...)                 # Stage 3
# DO NOT patch inside ProcessPoolExecutor workers (spawn boundary)
```
---
## Module Map (current state)
```
orchestrator.py         — Stages 0–3, 5–7 implemented. Stage 4 stub.
                          B9A-001/B9A-003 fixed. New helpers in 9D.
fitness.py              — Stateless. NaN guard. B8B-003 open (expectancy /3.0).
contracts.py            — All frozen dataclasses. B8C-001 fixed.
candidate_store.py      — SQLite WAL + single-writer queue. Thread-safe.
strategy_runner.py      — Single candidate eval. B8-006 comment added.
parameter_space.py      — Expands zones. B9C-005 fixed. AUDITED.
sampler.py              — LHS/random. B9C-006/B9C-007 fixed. AUDITED.
scenario.py             — Loads ScenarioProfile. AUDITED. Clean.
ranker.py               — List[CandidateRecord]. AUDITED. Clean.
yaml_generator.py       — Merges params. B8-006 comment added.
wfo/wfo_evaluator.py    — B8B-018 fixed. B8B-005 FIXED (9E): IS/OOS split.
                          _compute_oos_delta(): 70/30 split, oos_delta=fitness delta.
                          oos_gate_enabled param added (7th positional).
wfo/wfo_engine.py       — B9C-004 fixed. B8B-005 FIXED (9E): passes oos_gate_enabled
                          to evaluate_window in pool.submit(). Log updated.
wfo/consistency_scorer.py — 4-metric composite. B8B-012 open (sigmoid scale).
                            Already correct — no changes in 9E.
wfo/window_generator.py — YAML → sorted WFOWindow list.
ga/ga_engine.py         — Full evolution. _base_yaml_path injection from orchestrator.
ga/population.py        — Init from MC_PREFILTER_PASS. Typed API.
ga/selection.py         — Tournament selection.
ga/crossover.py         — Uniform crossover. B9B-001 open.
ga/mutation.py          — Snap-then-clamp.
ga/diversity.py         — Hybrid Euclidean/Hamming.
monte_carlo/mc_engine.py — Never raises. B8B-013 open.
evaluation/sensitivity.py — ±1/±2 steps. OPT-01 pool reuse.
evaluation/verdict.py   — Two-pillar + modifiers. Never sets LIVE_APPROVED.
report_generator.py     — HTML + JSON + Parquet. B8C-002/003 open.
```
---
## Lessons Learned (locked)
```
L-01: Windows spawn mode — patch at orchestrator level for integration tests.
L-02: Verdict boundary operators must be >= / <= (inclusive).
L-03: Stage 6 is the dominant runtime (98.7% of total).
L-04: Config fixture shape: config["scenarios"][name]["fitness_weights"][...].
L-05: Always verify store API completeness against all call sites.
L-06: CandidateParameterSet.candidate_id is SHA-256 — deterministic.
L-07: ScenarioProfile.__post_init__ must validate sequence fields.
L-08: ranker.rank_by_wfo() returns List[CandidateRecord] — not List[Dict].
L-09: _lhs_sample() must sort by float() — string sort breaks for values ≥ 10.
L-10: Stage 2 ruin threshold from scenario.mc_prefilter_ruin_threshold only.
L-11: Stage 3 injects _base_yaml_path via shallow copy — never mutate original config.
L-12: evaluate_window() takes oos_gate_enabled as 7th positional arg.
      pool.submit() must pass it explicitly — keyword args don't cross spawn boundary safely.
L-13: oos_delta=None is the safe default for any IS/OOS failure.
      Never force a numeric delta when sub-evaluation is incomplete.
L-14: OOS constraint-fail → oos_fitness=0.0 floor (not None) to preserve the
      signal that OOS degraded severely, while keeping delta numeric.
```
---
## What NOT To Do
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
- Do not re-open B8B-005 — fixed (9E)
- Do not change `_IS_FRACTION` without updating CONTEXT.md
- Do not enable `enforce_oos_gate: true` before calibrating `oos_degradation_threshold`
- Do not use the old `_record_to_candidate(Dict)` — use `_record_to_candidate_from_record(CandidateRecord)`
---
## Platform
- **OS**: Windows 10, Python 3.13.12
- **Timezone**: OHLCV/signals CET/CEST; pipeline timestamps UTC
- **Paths**: always `src/utils/paths.py`
- **DB**: `data/db/backtest.db` (prod); `tmp_path` in tests
---
## Session Deliverables (end of every session)
- Updated `outputs/CONTEXT.md` (handoff to next session)
- `outputs/ARCHITECTURE_<block>_DELTA.md` (append to ARCHITECTURE.md — only if structural changes)
- `outputs/OPERATOR_RUNBOOK_<block>_DELTA.md` (append to OPERATOR_RUNBOOK.md — only if operator-visible changes)
- Updated `SKILL.md` in outputs/ (this file — replace user skill at end of session)
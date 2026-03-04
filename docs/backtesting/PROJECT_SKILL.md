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
**Current status (2026-03-04)**: Block 9B complete.
- Tests: ~71 green, 0 skipped, 0 failed (8A×12 + 8B×14 + 8C×11 + 9A×7 + 9B×27 + prior 235)
- Next: Block 9C — supporting modules (wfo_engine, parameter_space, sampler, scenario, ranker, yaml_generator)
- Pre-production blocker: B8B-012 (sigmoid scale calibration — needs first real run)
---
## Pipeline (in order — do not reorder)
```
Stage 0: Validation & Init     (min 3 WFO windows; param name validation vs _PARAM_KEY_MAP)
Stage 1: Random Search         (LHS/random, significance guard, constraint filter) [STUB]
Stage 2: MC Pre-Filter         (cheap — 2 perturbation types, ruin screen) [STUB]
Stage 3: GA                    (WFO-aware: random 2 windows/generation + diversity penalty) [STUB]
Stage 4: Full WFO              (all windows, 4-metric composite consistency score) [STUB]
Stage 5: MC Deep               (full iterations, all perturbation types, WFO survivors only)
Stage 6: Parameter Sensitivity (±1/±2 step, fitness delta map, spike = borderline)
Stage 7: Report & Output       (HTML + checklist + JSON/Parquet + SQLite + YAML)
```
Stages 0, 5, 6, 7: fully implemented. Stages 1–4: stubs that log and advance checkpoint.
ALL stub checkpoints are now properly advanced (B9A-002 fixed).
---
## Architecture Rules (non-negotiable)
```python
# Contracts: always frozen dataclasses, never raw dicts crossing module boundaries
# Fail fast: invalid config raises at construction, no silent fallbacks
# Datetime: datetime.now(timezone.utc) — NEVER datetime.utcnow() (deprecated Python 3.12+)
# Paths: pathlib.Path + src/utils/paths.py — never hardcoded separators
# Concurrency: ProcessPoolExecutor spawn mode — never multiprocessing fork
# Candidate ID: always CandidateParameterSet.create() — deterministic SHA-256 of params dict
#   → reconstructing from same params always gives same ID (B9A-006 confirmed)
# "Candidate" is NOT a contract type — use CandidateParameterSet
# LIVE_APPROVED: never set in code — operator-only manual action
# strategy_runner.run(): mode_override="core" — NOT mode="core"
# Timing: logger.info only — never print(), never debug flags
# store.close(): always in finally block
# Mutation: snap-then-clamp order — never clamp-then-snap (would push off-grid back out of range)
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
# write_wfo_window_result: non-blocking (enqueue). is_ga_fitness_window=0, ga_generation=None.
# flag_candidate_wfo_insufficient: non-blocking. INSERT OR IGNORE — idempotent.
```
---
## Known Confirmed Bugs / Fixed (do not re-open)
```
H-01: FALSE POSITIVE — strategy_runner.evaluate() DOES accept date_start/date_end.
H-02: FIXED (7A) — write_wfo_window_result + flag_candidate_wfo_insufficient were absent.
H-03: FALSE POSITIVE — wfo_evaluator passes window dates correctly.
I-07: FIXED (7A) — datetime.utcnow() → datetime.now(UTC) in wfo_evaluator.py.
B8B-001: FIXED — NaN guard in fitness.py (NaN metrics → rejection, not silent pass).
B8B-018: FIXED — wfo_evaluator.py field names: "net_pnl"→"total_pnl_points", "expectancy"→"expectancy_points".
B8C-001: FIXED — contracts.py: report_emphasis scalar string now raises ValueError at construction.
B9A-002: FIXED — orchestrator.py Stage 1 stub now advances RANDOM_SEARCH_COMPLETE checkpoint.
B8C-007: CLOSED — Stage 7 guards None wfo_score/mc_result before compute_verdict(). No bug in verdict.py.
B9A-006: FALSE ALARM — CandidateParameterSet.candidate_id is SHA-256 of params. Deterministic. Safe.
```
---
## Critical Open Findings (fix before or when touching the file)
```
B8B-012 [PRE-PROD BLOCKER] consistency_scorer.py
  WFO sigmoid scale=0.10 calibrated for unit fractions, not currency points.
  Fix after first real run: measure net_pnl distribution → set wfo_sigmoid_scale.
  Attention! Currency are not used in this project. Strategy metrics are in pips and points. Currency is not required.
B8B-005 [P2] wfo_evaluator.py / wfo_engine.py
  oos_delta always None. enforce_oos_gate=True has no effect. OOS gate not implemented.
B9A-001 [P3] orchestrator.py
  rank_by_wfo() returns List[Dict] — raw dicts, not List[CandidateRecord].
B9A-003 [P3] orchestrator.py Stage 6
  spike_threshold dual-source: config["sensitivity"]["spike_threshold"] vs
  ScenarioProfile.verdict_sensitivity_spike_threshold. Must be kept in sync manually.
  Fix: Stage 6 should read scenario.verdict_sensitivity_spike_threshold directly.
B9B-003 [P3] ga_engine.py
  config['_base_yaml_path'] is a private injected key — not in backtest_template.yaml.
  When Stage 3 is implemented, orchestrator must inject:
  config['_base_yaml_path'] = str(_resolve_base_yaml(config))
B8-009 [P3] orchestrator.py
  Raw sqlite3 in _resume_or_start bypasses CandidateStore contract.
B9B-001 [P3] crossover.py
  No zone-name guard for cross-zone parents. Silent mixed-zone child.
  Downstream mutation clamping catches boundary violations but no warning logged.
B8B-013 [P3] mc_engine.py
  ruin_threshold dual-source: config dict + ScenarioProfile.mc_prefilter_ruin_threshold.
OPT-01 target (not achived): Stage 6 ≤ 200s (40% reduction via pool reuse across candidates). Pool 
  implemented but Stage 6 still ~300s - to reanalize if possible to improve below target
```
---
## Test Import Convention (CRITICAL — violating causes circular import at collection)
```python
# 1. sys.path FIRST — before any project imports
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
# 2. path anchor
from src.utils.paths import PROJECT_ROOT
# 3. contracts BEFORE candidate_store
from src.backtesting.contracts import (...)
# 4. candidate_store AFTER contracts
from src.backtesting.candidate_store import CandidateStore
```
---
## Critical Patch Targets (Windows spawn mode)
```python
# Stage 6 integration tests: patch at orchestrator level, NOT at worker function
patch("src.backtesting.orchestrator.evaluate_sensitivity", ...)   # CORRECT
# Stage 5: run_mc is a LOCAL import inside _run_stage_5_mc_deep
patch("src.backtesting.monte_carlo.mc_engine.run_mc", ...)        # CORRECT
# DO NOT patch inside ProcessPoolExecutor workers — mock doesn't cross spawn boundary (ROB-09)
```
---
## Module Map (current state)
```
orchestrator.py         — Stage sequencer. 0/5/6/7 implemented. 1-4 stubs with checkpoints.
                          B9A-002 fixed: all stubs now advance checkpoint.
                          B9A-003 open: Stage 6 spike_threshold from config, not ScenarioProfile.
fitness.py              — Stateless. MetricsReport + ScenarioProfile → FitnessResult. NaN guard added.
contracts.py            — All frozen dataclasses. report_emphasis validation added (B8C-001).
candidate_store.py      — SQLite WAL + single-writer queue. Thread-safe.
strategy_runner.py      — Single candidate eval. _PARAM_KEY_MAP maps zone params → YAML paths.
parameter_space.py      — Expands YAML zones. [NOT YET AUDITED — Block 9C]
sampler.py              — LHS or random. [NOT YET AUDITED — Block 9C]
scenario.py             — Loads ScenarioProfile from YAML. [NOT YET AUDITED — Block 9C]
ranker.py               — Returns ranked list. [NOT YET AUDITED — Block 9C — may resolve B9A-001]
yaml_generator.py       — Merges params into base YAML. [NOT YET AUDITED — Block 9C]

wfo/wfo_evaluator.py    — One candidate × one window → WFOWindowResult. Never raises.
                          B8B-018 FIXED: total_pnl_points, expectancy_points.
wfo/wfo_engine.py       — Lightweight + full modes. [NOT YET FULLY AUDITED — Block 9C]
wfo/consistency_scorer.py — 4-metric composite. sigmoid scale=0.10 (B8B-012 open).
wfo/window_generator.py — YAML → sorted WFOWindow list.

ga/ga_engine.py         — Full evolution. rng.sample(windows, k=2) per generation.
                          B9B-003 open: config['_base_yaml_path'] injection contract.
ga/population.py        — Init from MC_PREFILTER_PASS. Elite extraction. Typed API (CandidateRecord).
ga/selection.py         — Tournament selection. Raises on empty pop.
ga/crossover.py         — Uniform crossover. zone_name from parent_a. B9B-001 open (no zone guard).
ga/mutation.py          — Gaussian on step grid. Snap-then-clamp. choice edge cases handled.
ga/diversity.py         — Hybrid Euclidean/Hamming penalty.

monte_carlo/mc_engine.py         — Pre-filter + deep dispatch. Never raises.
monte_carlo/perturbation.py      — Named profiles from YAML.
monte_carlo/equity_simulator.py  — Vectorised np.cumsum.
monte_carlo/mc_metrics.py        — avg_equity, worst_dd, ruin_prob, p5_equity. Vectorised.

evaluation/sensitivity.py — ±1/±2 steps. Parallel via ProcessPoolExecutor. OPT-01 pool reuse applied.
evaluation/verdict.py     — Two-pillar + modifier flags. Never sets LIVE_APPROVED.
report_generator.py       — Self-contained HTML. Inline charts. JSON + Parquet.
```
---
## Performance Baseline (locked Block 3)
```
Stage 5 (MC Deep):    <3s   — vectorised, never the bottleneck
Stage 6 (Sensitivity): ~333–446s — structural bottleneck (Windows spawn overhead)
Stage 7 (Report):     4–8s
Budget: 14,400s → 2.3% consumed ✅
OPT-01 (not achieved) target: Stage 6 ≤ 200s (pool reuse across candidates — applied in evaluation/sensitivity.py)
```
---
## Lessons Learned (locked)
```
L-01: Windows spawn mode — mock patches don't cross worker boundary.
      Patch at orchestrator level for integration tests.
L-02: Verdict boundary operators must be >= / <= (inclusive) at go thresholds.
L-03: Stage 6 is the dominant runtime (98.7% of total).
L-04: Config fixture shape must match load_scenario() nested structure:
      config["scenarios"][name]["fitness_weights"][...] NOT config["fitness_weights"][...]
L-05: Silent write loss from missing store methods (H-02). Always verify store API
      completeness against all call sites before trusting DB output.
L-06: CandidateParameterSet.candidate_id is SHA-256 of params dict — deterministic.
      Reconstructing from same params in _record_to_candidate() always gives same ID.
      No need to carry candidate_id through rank_by_wfo() records (B9A-006 confirmed).
L-07: ScenarioProfile.__post_init__ must validate sequence fields (not scalar strings).
      report_emphasis="balanced" passes type hint but iterates as characters downstream.
      Always add isinstance(..., (list, tuple)) + len > 0 guard for sequence fields.
```
---
## What NOT To Do
- Do not guess or reconstruct code not available — ask for the file
- Do not modify `src/strategies/` — strategy architecture is frozen
- Do not use `analytics` mode — `core` mode only (`mode_override="core"`)
- Do not add `print()` — use `logger.info`
- Do not implement ML/AI, eToro API, regime-aware MC, global sensitivity random-walk (v2+)
- Do not re-open D-01 through D-12, H-01 through H-03 (all resolved)
- Do not set `deployment_status = LIVE_APPROVED` in code
- Do not use `datetime.utcnow()` in any new or modified code
- Do not use `Candidate` type — use `CandidateParameterSet`
- Do not patch functions called inside ProcessPoolExecutor workers (spawn boundary)
- Do not use `e2e_test` scenario for production optimization runs
- Do not clamp-before-snap in mutation — snap-then-clamp is correct
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
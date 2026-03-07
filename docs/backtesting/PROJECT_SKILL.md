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
**Current status (2026-03-07)**: Block 9J complete. All P1 blockers resolved. auto_go verdicts
confirmed (3 auto_go in run 1fcc6398). Pipeline production-ready for DAX paper trading.
- Tests: ~345 green, 0 skipped, 0 failed (Block 9C baseline)
- Next: Paper trade 1bfa417dc8bb / f57ade9c9e75. RSI-SENS decision (remove or tighten).
- Pre-production blockers: NONE
---
## Pipeline (in order — do not reorder)
```
Stage 0: Validation & Init     (min 3 WFO windows; param name validation vs _PARAM_KEY_MAP) ✅
Stage 1: Random Search         (LHS/random, significance guard, constraint filter) ✅
Stage 2: MC Pre-Filter         (re-evaluates candidates; cheap ruin screen) ✅
Stage 3: GA                    (WFO-aware: random 2 windows/generation + diversity penalty) ✅
Stage 4: Full WFO              (all windows, 4-metric composite consistency score) ✅
Stage 5: MC Deep               (full iterations, all perturbation types, WFO survivors only) ✅
Stage 6: Parameter Sensitivity (±1/±2 step, fitness delta map, spike = borderline) ✅
Stage 7: Report & Output       (HTML + checklist + JSON/Parquet + SQLite + YAML) ✅
```
All stages fully implemented. OOS gate: implemented but off by default (enforce_oas_gate: false).
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
```
---
## CandidateStore Write API (verified)
```python
store.write_candidate(record: CandidateRecord)
store.write_candidate_stub(candidate: CandidateParameterSet)  # INSERT OR IGNORE — safe always
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
# get_candidate_result() returns trades=None / metrics=None ALWAYS — do NOT use for MC input
# write_candidate_stub() MUST be called before any FK-referencing write (B9G-001)
# _wfo_result_id() is deterministic SHA-256[:32] of run_id+candidate_id+window_id (B9H-002)
# INSERT OR REPLACE on wfo_window_results deduplicates correctly (B9H-002)
```
---
## FitnessResult — stored actuals (evaluations table)
```python
# FitnessResult stores EXACTLY these six constraint actuals.
# Do NOT query any other "actual_*" columns — they do not exist (B9I-002).
actual_win_rate         # 0-1 fraction (divided by 100)
actual_max_drawdown     # 0-1 fraction (abs(pts) / ref_pts)
actual_losing_streak    # raw int
actual_trades_per_week  # raw float
actual_expectancy       # raw pts (expectancy_points)
actual_profit_factor    # raw float
# NOT in evaluations: actual_net_pnl, actual_total_trades
# net_pnl: wfo_window_results.net_pnl (Stage 4 only)
```
---
## consistency_scorer.py — Calibration State (B8B-012, DAX pts)
```python
_SIGMOID_SCALE: float = 131.0          # stdev(net_pnl)=261.98 × 0.5 (run 87712cab)
_MAX_EXPECTED_VARIANCE: float = 100_000.0   # pts² ceiling
_MAX_EXPECTED_DRAWDOWN: float = 1_000.0     # pts ceiling; abs() applied before use

# Recalibrate when instrument or data range changes materially:
#   python calibrate_sigmoid.py   (saved in outputs/)
#   scale = stdev × 0.5

# UNIT CONTRACT: all three constants are in DAX raw points.
# V2-RAR will normalise to Rolling Annual Range fractions.
```
---
## ScenarioProfile — wfo_collapse_drawdown_threshold
```python
# contracts.py: default = 400.0 pts (DAX). Validation: > 0.0 only (no upper bound).
# scenario.py: wired via s.get("wfo_collapse_drawdown_threshold", 400.0)
# backtest_1st_run.yaml: wfo_collapse_drawdown_threshold: 400.0

# HISTORY: was 0.40 (fraction), never wired from YAML, validator rejected any pts value.
# All three errors fixed in Block 9J (COLLAPSE-UNIT).

# For other instruments: set proportionally to instrument's typical drawdown magnitude.
# V2-RAR: will become a dimensionless RAR fraction (e.g. 0.133 for DAX at RAR=3000pts).
```
---
## Open Issues (prioritised)
```
RSI-SENS [P2] — RSI delta=0.0000 across 3 runs. Filter active but ineffective.
  Option A (recommended): remove rsi_period/rsi_overbought/rsi_oversold from search space.
  Option B: tighten overbought: 65, oversold: 40 to force filter activation.

RR-CEILING [P3] — rr_target spike suggests optimum above current max 7.0.
  Consider extending to 8.5. Low priority.

B8B-003 [P3] fitness.py: expectancy /3.0 — acceptable, low priority
B8-009  [P3] orchestrator.py: raw sqlite3 in _resume_or_start
B9B-001 [P3] crossover.py: no zone-name guard
B8B-013 [P3] mc_engine.py: ruin_threshold dual-source
B8B-011 [P3] consistency_scorer.py: fraction_positive_windows fixed 0.0 floor
B8C-002/003 [P3] report_generator.py: deferred
B9C-008 [P3] sampler.py: deferred
OPT-01  [P3] Stage 6 ≤200s
[WinError 32] cosmetic, pre-existing
```
---
## V2 Backlog
```
V2-RAR: Normalise all instrument-specific constants via Rolling Annual Range.
  Affected: _SIGMOID_SCALE, _MAX_EXPECTED_VARIANCE, _MAX_EXPECTED_DRAWDOWN,
  wfo_collapse_drawdown_threshold. Enables multi-asset without recalibration.
  Do not implement until DAX pipeline validated in paper trading.

Dynamic WFO window generation: data_range as single param, windows auto-derived.
  See OPERATOR_RUNBOOK_9I_DELTA.md for full spec.
```
---
## evaluate_window() Signature
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
```
---
## strategy_runner.evaluate() Signature
```python
def evaluate(
    candidate: CandidateParameterSet,
    base_yaml_path: Path,
    temp_dir: Path,
    min_significant_trades: int = 30,
    retain_temp_yamls: bool = False,
    date_start: Optional[Union[date, datetime]] = None,
    date_end: Optional[Union[date, datetime]] = None,
) -> CandidateResult: ...
# None date → base YAML date_range unchanged (Stage 1)
# Temp YAML uses full candidate_id (64 chars) — B9H-003
```
---
## Module Map (current state)
```
orchestrator.py         — All stages (B9G complete). Stage 5 re-evaluates (B9G-002).
fitness.py              — Stateless. NaN guard. B8B-003 low priority.
contracts.py            — All frozen dataclasses. COLLAPSE-UNIT: threshold default 400.0, validator > 0.0 only.
candidate_store.py      — SQLite WAL. _wfo_result_id() deterministic (B9H-002).
strategy_runner.py      — B9F-005: date params. B9H-003: full candidate_id.
parameter_space.py      — B9F-001: per-param lists. get_param_values() added.
sampler.py              — B9I-001: cycling strata, no cap on n.
scenario.py             — COLLAPSE-UNIT: wfo_collapse_drawdown_threshold wired from YAML.
ranker.py               — rank_by_wfo() deduplicates (B9G-003).
yaml_generator.py       — B9G-004: _PARAM_MAP corrected.
query_run.py            — B9H-001/002: GA health, GROUP BY. B9I-002: phantom cols removed.
wfo/wfo_evaluator.py    — B8B-005/B8B-018 fixed.
wfo/wfo_engine.py       — B8B-005/B9C-004 fixed.
wfo/consistency_scorer.py — B8B-012 FIXED: scale=131.0, var=100k, dd=1000 (DAX pts).
monte_carlo/mc_engine.py  — Never raises. B8B-013 open.
monte_carlo/equity_simulator.py — B9F-004: pnl_points.
ga/ga_engine.py         — write_candidate_stub() + flush() before pool (B9G-001).
ga/population.py        — Raises on empty seeds.
evaluation/sensitivity.py — OPT-01 pool reuse.
evaluation/verdict.py   — Two-pillar + modifiers.
report_generator.py     — B8C-002/003 open.
```
---
## Lessons Learned (L-01 through L-37)
```
L-01 through L-33: see Block 9I CONTEXT.md

L-34: Normalisation constants in consistency_scorer.py are instrument-specific.
      Calibrate from actual run data. Recalibrate after instrument or range change.

L-35: Threshold fields must use same units as stored metric values. Document unit
      (fraction vs pts) for every ScenarioProfile threshold field. Verify after
      any normalisation change.

L-36: A YAML field with a dataclass default is silently ignored unless the loader
      (scenario.py) explicitly reads and passes it. Always verify loader wire-up
      when adding new ScenarioProfile fields. Two-layer failures (validator rejects
      the correct value AND loader ignores the YAML) will not be caught by unit
      tests — requires integration test checking actual verdict output.

L-37: Zero sensitivity delta across all perturbation steps and all candidates over
      multiple runs = filter is either disabled or produces no overlap with active
      trade flow. Zero-delta params waste search dimensions. Verify filter
      effectiveness independently before including its params in search space.
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
- Do not use trade.pnl — use trade.pnl_points (B9F-004)
- Do not call store.get_candidate_result() for MC input (L-15)
- Do not truncate candidate_id in temp YAML filenames (B9H-003)
- Do not use uuid4() as result_id in wfo_window_results (B9H-002)
- Do not call expand_zones() expecting List[Dict] (B9F-001)
- Do not cap _lhs_sample() at min_universe_size (B9I-001)
- Do not query actual_net_pnl or actual_total_trades from evaluations (B9I-002)
- Do not use evaluations for net_pnl — use wfo_window_results
- Do not set wfo_collapse_drawdown_threshold as fraction for DAX — must be pts (COLLAPSE-UNIT)
- Do not assume identical WFO scores = equal candidates — check sigmoid scale (L-31)
- Do not add RSI params to search space without verifying filter is producing signals (L-37)
- Do not add new ScenarioProfile fields without wiring them in scenario.py (L-36)
- Do not run on non-DAX instruments without recalibrating all normalisation constants
- Do not enable exploration zone before verifying collapse rate is reasonable
---
## Platform
- **OS**: Windows 10, Python 3.13.12
- **Timezone**: OHLCV/signals CET/CEST; pipeline timestamps UTC
- **Paths**: always `src/utils/paths.py`
- **DB**: `outputs/backtesting/backtester.db`
---
## Session Deliverables (end of every session)
- Updated `outputs/CONTEXT.md`
- `outputs/ARCHITECTURE_<block>_DELTA.md` (if structural changes)
- `outputs/OPERATOR_RUNBOOK_<block>_DELTA.md` (if operator-visible changes)
- Updated `SKILL.md` in outputs/ (replace user skill)
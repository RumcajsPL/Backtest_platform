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
**Current status (2026-03-06)**: Block 9F complete.
- Tests: ~345 green, 0 skipped, 0 failed (Block 9C baseline; 9D/9E/9F add no new tests)
- Next: Block 9G — first clean pipeline run (e2e_test) + calibration (B8B-012, B8B-003)
- Pre-production blocker: B8B-012 (sigmoid scale — needs first real run data)
---
## Pipeline (in order — do not reorder)
```
Stage 0: Validation & Init     (min 3 WFO windows; param name validation vs _PARAM_KEY_MAP)
Stage 1: Random Search         (LHS/random, significance guard, constraint filter) ✅ IMPLEMENTED
Stage 2: MC Pre-Filter         (re-evaluates candidates; cheap ruin screen) ✅ IMPLEMENTED
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
# get_candidate_result() returns trades=None / metrics=None ALWAYS — do NOT use for MC input
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
## strategy_runner.evaluate() Full Signature (B9F-005)
```python
def evaluate(
    candidate: CandidateParameterSet,
    base_yaml_path: Path,
    temp_dir: Path,
    min_significant_trades: int = 30,
    retain_temp_yamls: bool = False,
    date_start: Optional[Union[date, datetime]] = None,  # B9F-005
    date_end: Optional[Union[date, datetime]] = None,    # B9F-005
) -> CandidateResult: ...
# date_start/date_end override data.date_range.start/end in temp YAML
# date object → "YYYY-MM-DD 00:00:00" (start) / "YYYY-MM-DD 23:59:59" (end)
# datetime object → formatted as-is
# None → base YAML date_range used unchanged (Stage 1 behaviour)
```
---
## Trade Contract (verified Block 9F — B9F-004)
```python
# Source: src/strategies/contracts/trade_contracts.py
TradeResult.trades          # List[Trade]  — unwrap if needed: trades.trades
Trade.entry                 # TradeEntry
Trade.exit                  # Optional[TradeExit]  — None = open trade
Trade.pnl_points            # Optional[float]      — None if open (no exit)
Trade.is_closed             # bool
TradeExit.pnl_points        # float  — the correct P&L field
# extract_trade_returns: use trade.pnl_points, skip None values (open trades)
# DO NOT use trade.pnl — this attribute does not exist on Trade
```
---
## Stage 2 Design (B9F-003)
```python
# Stage 2 MUST re-evaluate candidates via strategy_runner.evaluate() to get
# a live CandidateResult with real trades and metrics objects.
# store.get_candidate_result() always returns trades=None / metrics=None
# because these objects are never persisted to SQLite.
# CandidateResult.is_valid requires metrics is not None AND trades is not None.
# Re-evaluation is consistent with WFO stage behaviour (Stage 4 also re-evaluates).
```
---
## OOS Gate Details (B8B-005 — implemented Block 9E)
```python
# IS/OOS split: _IS_FRACTION = 0.70 (70% IS, 30% OOS by calendar days)
# is_end_date = window.start_date + timedelta(days=int(total_days * 0.70))
# oos_delta = oos_fitness - is_fitness  (both [0,1]; negative = OOS underperforms IS)
# oos_delta = None when gate disabled or sub-evaluation fails
# enforce_oos_gate: false until threshold calibrated
```
---
## Known Confirmed Bugs / Fixed (do not re-open)
```
H-01: FIXED (B9F-005) — strategy_runner.evaluate() NOW accepts date_start/date_end.
      Previous skill entry "FALSE POSITIVE" was incorrect.
      wfo_evaluator.py was always correct — the runner was missing the params.
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
B8B-005: FIXED (9E) — IS/OOS split in wfo_evaluator + oos_gate_enabled pass-through.
B9F-002: FIXED (9F) — Stage 3 graceful skip when no MC_PREFILTER_PASS candidates.
B9F-003: FIXED (9F) — Stage 2 re-evaluates via evaluate() instead of store reconstruct.
B9F-004: FIXED (9F) — extract_trade_returns uses trade.pnl_points not trade.pnl.
B9F-005: FIXED (9F) — strategy_runner.evaluate() accepts date_start/date_end.
```
---
## Critical Open Findings
```
B9F-001 [P1 BLOCKER] parameter_space.py
  expand_zones() calls itertools.product() which ENUMERATES the full Cartesian product.
  exploration zone: ~387 trillion combinations → OOM / process hangs forever.
  safe zone: ~2 million combinations → ~520MB RAM, feasible on 64-bit / ≥4GB free RAM.
  Workaround: exploration.enabled: false in YAML.
  Fix: refactor expand_zones() to return Dict[str, Dict[str, List]] (per-param value
  lists, not full product); refactor sampler._lhs_sample() to accept per-param lists.
B8B-012 [PRE-PROD BLOCKER] consistency_scorer.py
  _sigmoid_normalise scale=0.10 calibrated for unit fractions, not points.
  Fix after first real run: measure net_pnl distribution → set scale ≈ stdev * 0.5.
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
OPT-01 target (not achieved): Stage 6 ≤ 200s.
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
                          B9F-002: Stage 3 graceful skip guard added.
                          B9F-003: Stage 2 re-evaluates via evaluate().
fitness.py              — Stateless. NaN guard. B8B-003 open (expectancy /3.0).
contracts.py            — All frozen dataclasses. B8C-001 fixed.
candidate_store.py      — SQLite WAL + single-writer queue. Thread-safe.
                          get_candidate_result() returns trades=None — do not use for MC.
strategy_runner.py      — B9F-005: date_start/date_end params added.
                          Injects data.date_range.start/end override into temp YAML.
parameter_space.py      — B9C-005 fixed. B9F-001 open (Cartesian product OOM).
sampler.py              — B9C-006/B9C-007 fixed.
scenario.py             — Clean.
ranker.py               — Clean.
yaml_generator.py       — B8-006 comment added.
wfo/wfo_evaluator.py    — B8B-005/B8B-018 fixed. Passes date_start/date_end correctly.
wfo/wfo_engine.py       — B8B-005/B9C-004 fixed.
wfo/consistency_scorer.py — B8B-012 open (sigmoid scale).
monte_carlo/mc_engine.py  — Never raises. B8B-013 open.
monte_carlo/equity_simulator.py — B9F-004: extract_trade_returns uses pnl_points.
ga/ga_engine.py         — Full evolution loop.
ga/population.py        — Raises ValueError on empty seeds (correct — guard is in orchestrator).
evaluation/sensitivity.py — OPT-01 pool reuse.
evaluation/verdict.py   — Two-pillar + modifiers.
report_generator.py     — B8C-002/003 open.
```
---
## Lessons Learned (locked — additions from Block 9F)
```
L-01 through L-14: unchanged — see Block 9E CONTEXT.md
L-15: store.get_candidate_result() is structurally incomplete — trades/metrics are
      never persisted. Stage 2 and any stage needing live trade data must re-evaluate
      via strategy_runner.evaluate(). Never rely on reconstructed CandidateResult
      for MC or any trade-level operation.
L-16: Trade.pnl_points is the correct P&L attribute (not Trade.pnl which does not exist).
      Open trades return pnl_points=None — always skip None values in MC extraction.
L-17: strategy_runner.evaluate() date_start/date_end override data.date_range in the
      temp YAML. date objects use "00:00:00"/"23:59:59" boundary defaults.
L-18: H-01 "FALSE POSITIVE" in the skill was wrong. Always verify contract claims
      against actual function signatures before marking as false positive.
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
- Do not use trade.pnl — use trade.pnl_points (B9F-004)
- Do not call store.get_candidate_result() for MC input — re-evaluate instead (B9F-003)
- Do not mark H-01 as false positive — it is FIXED (B9F-005)
- Do not change `_IS_FRACTION` without updating CONTEXT.md
- Do not enable `enforce_oos_gate: true` before calibrating `oos_degradation_threshold`
---
## Platform
- **OS**: Windows 10, Python 3.13.12
- **Timezone**: OHLCV/signals CET/CEST; pipeline timestamps UTC
- **Paths**: always `src/utils/paths.py`
- **DB**: `outputs/backtesting/backtester.db` (confirmed path from run logs)
---
## Session Deliverables (end of every session)
- Updated `outputs/CONTEXT.md` (handoff to next session)
- `outputs/ARCHITECTURE_<block>_DELTA.md` (append to ARCHITECTURE.md — only if structural changes)
- `outputs/OPERATOR_RUNBOOK_<block>_DELTA.md` (append to OPERATOR_RUNBOOK.md — only if operator-visible changes)
- Updated `SKILL.md` in outputs/ (this file — replace user skill at end of session)
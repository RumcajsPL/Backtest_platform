---
name: backtester-project
description: >
  Use this skill whenever working on the Backtesting & Optimization Framework project.
  Triggers: any mention of backtester, backtest pipeline, CandidateStore, GA engine,
  WFO evaluator, Monte Carlo engine, fitness evaluator, scenario profile, backtest_template.yaml,
  or any module from src/backtesting/. Read this SKILL.md before writing any code,
  creating any file, or making any design decision for this project.
---
# Backtesting Framework — Project Skill
## What This Project Is
A fully automated 8-stage optimization pipeline for the WBWSStrategy trading strategy.
It answers: does this strategy have real trading potential, and if yes, what is the optimal setup?
The pipeline produces go/borderline/no-go verdicts based on two mandatory evidence pillars.
## Pipeline (in order — do not reorder without explicit instruction)
```
Stage 0: Validation & Init     (min 3 WFO windows required — validated here for GA random sampling)
Stage 1: Random Search         (LHS, significance guard, constraint filter, single-run fitness)
                               [post-stage: log statistical adequacy warning if MC/WFO config weak]
Stage 2: MC Pre-Filter         (cheap — 2 perturbation types, ruin probability screen)
Stage 3: GA                    (WFO-aware fitness: randomly sample 2 windows per generation from
                               full window list + diversity penalty — NOT fixed window pair)
Stage 4: Full WFO              (all configured windows, 4-metric composite consistency score)
Stage 5: MC Deep               (full iterations, all perturbation types, WFO survivors only)
Stage 6: Parameter Sensitivity (±1/±2 step per parameter, fitness delta map, spike = borderline)
Stage 7: Report & Output       (HTML + borderline checklist + JSON/Parquet + SQLite + YAML)
```
## Verdict Model
**Two mandatory pillars** (not four, not one):
1. WFO temporal consistency score — composite of four metrics:
   - median window return
   - window-to-window return variance
   - worst-window drawdown
   - fraction of positive windows
2. MC deep ruin probability
**Three outcomes**: auto_go | borderline (human review) | no_go
Sensitivity spike → borderline flag even if both pillars pass.
IS/OOS delta → informational by default. Optional: `enforce_oos_gate: true` in YAML makes
  IS/OOS degradation > 50% a borderline flag (never auto-reject).
Parameter region width → informational only, never a verdict gate.
**VerdictResult.deployment_status**: always `PAPER_TRADE_REQUIRED` for go/borderline verdicts.
  Operator manually sets `LIVE_APPROVED` after completing paper trading period.
  Embedded in trading-ready YAML metadata.
## Scenario System
One active scenario per run. Defined in `backtest_template.yaml` under `scenario:`.
Scenario controls: fitness weights, constraint thresholds, WFO temporal weights, verdict thresholds, report framing.
Built-in: `capital_accumulation`, `swing_trading`, `conservative`.
Custom scenarios via YAML only — never hardcode scenario logic.
Scenario profiles with concrete values are in `docs/backtesting/TECHNICAL_SPEC.md` Section 5.
## Architecture Rules (non-negotiable)
```python
# CORRECT — typed frozen contract between modules
@dataclass(frozen=True)
class CandidateResult:
    candidate_id: str
    evaluated_at: datetime
    metrics: Optional[MetricsReport]
    trades: Optional[TradeResult]
    total_trades: Optional[int]
    error: Optional[str] = None

# WRONG — raw dict
result = {"metrics": ..., "trades": ...}  # never

# CORRECT — fail fast at construction
def __post_init__(self):
    if self.fitness_score is not None and not (0.0 <= self.fitness_score <= 1.0):
        raise ValueError(f"fitness_score must be in [0, 1]; got {self.fitness_score}")

# WRONG — silent fallback
fitness = max(0, self.fitness_score)  # never

# CORRECT — Windows-safe path
output_dir = Path(config.output_dir) / "reports"

# WRONG — hardcoded separator
output_dir = config.output_dir + "/reports"  # never

# CORRECT — ProcessPoolExecutor (Windows spawn-safe)
with ProcessPoolExecutor(max_workers=config.max_workers) as pool:
    futures = [pool.submit(evaluate_candidate, c) for c in candidates]

# WRONG — multiprocessing with fork
mp.Pool(processes=6)  # fork mode breaks on Windows
```
## Module Map (one module, one concern)
```
orchestrator.py           — sequences stages, checkpoints, resume. Writes immutable run
                            artifacts (config hash, seeds) at start. Nothing else.
parameter_space.py        — expands YAML zones to parameter sets. No strategy knowledge.
sampler.py                — LHS or random selection from expanded space. No evaluation.
scenario.py               — loads ScenarioProfile from YAML. One profile per run.
strategy_runner.py        — single candidate evaluation. Builds temp YAML, calls core mode.
                            Returns failed CandidateResult on any error — never raises.
fitness.py                — stateless. Receives MetricsReport + ScenarioProfile. Returns FitnessResult.
candidate_store.py        — SQLite. WAL mode + single-writer queue. Thread-safe writes.
                            Immutable run artifact storage (config hash, seeds, perturbation profile name).
ranker.py                 — stateless. Query spec in → ranked list out.
ga/ga_engine.py           — GA loop. Randomly samples 2 windows per generation from full
                            WFO window list. Applies diversity penalty in fitness.
ga/diversity.py           — computes diversity penalty scalar for a candidate vs. elite
                            population. Hybrid Euclidean/Hamming. Configurable threshold and weight.
ga/population.py          — population initialisation from seed candidates.
ga/selection.py           — tournament selection. Zone-boundary-aware.
ga/crossover.py           — crossover operator. Produces valid CandidateParameterSets only.
ga/mutation.py            — mutation operator. Respects zone boundaries.
wfo/wfo_engine.py         — orchestrates WFO. Lightweight mode (GA) and full mode (Stage 4).
wfo/window_generator.py   — reads window definitions from YAML. Returns WFOWindow list.
wfo/wfo_evaluator.py      — one candidate, one window. Returns WFOWindowResult.
wfo/consistency_scorer.py — aggregates WFOWindowResults → four temporal metrics →
                            composite WFO consistency score. Returns WFOConsistencyScore.
monte_carlo/mc_engine.py  — two modes: pre-filter (Stage 2) and deep (Stage 5).
monte_carlo/equity_simulator.py — simulates equity paths. Applies perturbations.
monte_carlo/perturbation.py — named, versioned perturbation profiles from YAML.
monte_carlo/mc_metrics.py — computes avg equity, worst drawdown, ruin probability, p5 equity.
evaluation/sensitivity.py — perturbs each parameter ±1/±2 steps. Returns SensitivityProfile.
evaluation/verdict.py     — two-pillar logic + sensitivity modifier + optional IS/OOS gate.
                            Sets deployment_status: PAPER_TRADE_REQUIRED on all go/borderline.
report_generator.py       — scenario-framed. HTML + borderline checklist + JSON/Parquet.
yaml_generator.py         — top candidate → validated YAML with deployment_status embedded.
```
## Contracts Checklist
Before writing any inter-module call, verify the contract exists in `TECHNICAL_SPEC.md`:
- `RunMetadata` — run_id (UUID), config_hash (SHA-256), scenario, 5 seeds, window_ids (min 3), checkpoint
- `ScenarioProfile` — 6 fitness weights (sum=1.0), 6 constraints, MC threshold, 4 WFO weights (sum=1.0), 5 verdict thresholds
- `CandidateParameterSet` — zone_name, parameters dict, candidate_id (SHA-256). Use `.create()` factory always.
- `CandidateResult` — candidate_id, evaluated_at, metrics (Optional), trades (Optional), total_trades, error
- `FitnessResult` — candidate_id, scenario_name, fitness_score (Optional), passed_constraints, failing details, 6 constraint actuals
- `WFOWindow` — window_id, start_date, end_date
- `WFOWindowResult` — candidate_id, window_id, evaluated_at, fitness_score, key metrics, oos_delta, error
- `WFOConsistencyScore` — 4 sub-metrics + composite_score [0,1] + windows_evaluated + flags
- `MCResult` — candidate_id, mode (MCMode enum), profile_name, iterations, avg_final_equity, worst_drawdown, ruin_probability, p5_final_equity
- `SensitivityProfile` — candidate_id, baseline_fitness, tuple of ParameterSensitivity, spike_detected, spike_parameters, profile_complete
- `VerdictResult` — verdict (Verdict enum), deployment_status (PAPER_TRADE_REQUIRED), both pillar scores, 4 modifier flags, evidence_summary
- `CandidateRecord` — flattened SQLite row, all fields as primitives, parameters_json audit backup
- `ParameterSensitivity` — parameter_name, step, perturbed_value, fitness_delta, evaluation_error
## All Decisions Resolved — Quick Reference
ALL 12 decisions are RESOLVED. Do not re-open without explicit operator instruction.
| D | Decision | Resolution |
|---|---|---|
| D-01 | Integration mode | Direct Python call in worker process. Benchmark 50 candidates Phase 2. |
| D-02 | SQLite concurrency | WAL mode + single-writer queue. Benchmark 500 writes Phase 2. |
| D-03 | Temp YAML lifecycle | Per-candidate, named by param hash. Deleted in `finally`. `retain_temp_yamls: true` for debug. |
| D-04 | GA seeding | Top-N by fitness from MC_PREFILTER_PASS. Diversity handled by penalty during evolution. |
| D-05 | GA WFO windows | Randomly sample 2 per generation from full list. Min 3 windows required (Stage 0 validates). |
| D-06 | Stage counts | 200/zone→120→pop60/30gen→30→10→5. All YAML-configurable. |
| D-07 | Verdict thresholds | WFO: go≥0.65 / borderline 0.40–0.65 / no_go<0.40. MC: go≤5% / borderline 5–15% / no_go>15%. Scenario-specific. Calibrate Phase 6. |
| D-08 | Sensitivity scope | All parameters (~300 evals, ~200s at 6 workers). |
| D-09 | Output formats | Both JSON + Parquet, both on by default. Independently disableable. |
| D-10 | Report generator | Build new. Structurally too different from single-run strategy report to extend. |
| D-11 | Diversity distance | Hybrid: normalised Euclidean (continuous params) + Hamming (discrete params), weighted avg. |
| D-12 | IS/OOS gate default | Off by default. Opt-in via `enforce_oos_gate: true`. >50% degradation = borderline (never auto-reject). |
## SQLite Schema — 9 Tables (full schema in SQLITE_SCHEMA.md)
```
runs                  — one row per run. Immutable: config_hash, 5 seeds, perturbation_profile_name.
candidates            — one row per unique candidate_id.
candidate_parameters  — all parameter values as individual columns + parameters_json backup.
evaluations           — one row per candidate per stage. All constraint actuals + fitness.
wfo_window_results    — one row per candidate per window (GA lightweight + full WFO, flagged separately).
wfo_consistency_scores — four sub-metrics + composite score per candidate.
mc_results            — pre-filter and deep as separate rows per candidate.
sensitivity_results   — one row per candidate per parameter per step.
sensitivity_profiles  — summary: spike_detected, spike_parameters, profile_complete per candidate.
verdicts              — final verdict + full evidence + deployment_status per candidate.
```
**Schema rules:**
- One row per candidate per stage — never denormalised blobs
- All numeric metrics as individual columns — no JSON-serialised metric blobs
- All parameter values as individual columns — enables `WHERE rsi_period > 14 AND atr_multiplier < 2.0`
- All rows have timestamps
- No information destroyed — every MetricsReport field is a column
- ML-ready: `SELECT * FROM candidates JOIN verdicts ... WHERE verdict != 'no_go'` = feature matrix
## Adversarial Challenge Suite (Phase 6 — required for delivery)
- **AV-01** Random-signal baseline: replace signals with coin flips → pipeline must return no_go for all candidates
- **AV-02** Overfit-injection test: curve-fit strategy tuned to one window → must be flagged borderline or auto-rejected
- **AV-03** Meta-config stability: >80% verdict stability under seed/iteration perturbation (known-robust candidates)
- **AV-04** Borderline escalation: checklist template generated for every borderline candidate — required for sign-off
- **Run AV-01 as smoke test at end of Phase 4** (before output layer) to detect structural pipeline flaws early
## What NOT To Do
- Do not modify `src/strategies/` — strategy architecture is frozen input
- Do not use `analytics` mode inside the backtester loop — `core` mode only
- Do not add `print()` statements — use `structured_logger.py` from strategy architecture
- Do not hardcode parameter names anywhere except `strategy_runner.py`
- Do not implement ML/AI layer — schema design only in v1
- Do not implement eToro API — future project
- Do not implement regime-aware MC perturbation profiles — v2 scope
- Do not implement true global parameter sensitivity random-walk — v2 scope
- Do not re-open any of D-01 through D-12 without explicit operator instruction
## Key Files to Read Before Coding
1. `docs/backtesting/TECHNICAL_SPEC.md` — all contracts + all decisions + YAML schema + scenario profiles
2. `docs/backtesting/SQLITE_SCHEMA.md` — full database schema + 10 query examples
3. `docs/backtesting/FUNCTIONAL_SPEC.md` — plain-language description of all 8 stages
4. `docs/backtesting/BACKTESTER_PLAN.md` — master requirements v1.2
5. `docs/backtesting/CONTEXT.md` — current phase status and next task
6. `docs/architecture/ARCHITECTURE.md` — strategy architecture (frozen input)
## Phase 2 Implementation Order
Build in this order — each module depends on the ones before it:
1. **`candidate_store.py`** — SQLite WAL + writer queue. Run D-02 benchmark (500 writes, 6 workers) before full implementation.
2. **`parameter_space.py`** + **`sampler.py`** — zone expansion + LHS sampling
3. **`scenario.py`** — ScenarioProfile loader and validator
4. **`strategy_runner.py`** — single candidate evaluation. Run D-01 benchmark (50 candidates, direct-call) to confirm speed. Never raises.
5. **`fitness.py`** — stateless constraint check (ordered cheapest first) + weighted score
6. **`ranker.py`** — stateless query → ranked list
7. **`orchestrator.py`** — skeleton: 8 stage stubs + checkpoint/resume logic + Stage 0 fully implemented
8. **Integration test** — single candidate full round-trip → stored in SQLite (Phase 2 milestone)
## Common Patterns
```python
# ── strategy_runner.py — never raises, always returns CandidateResult ──────
def evaluate(candidate: CandidateParameterSet,
             base_yaml_path: Path,
             temp_dir: Path,
             min_significant_trades: int) -> CandidateResult:
    yaml_path = temp_dir / f"candidate_{candidate.candidate_id[:12]}.yaml"
    try:
        _write_temp_yaml(candidate, base_yaml_path, yaml_path)
        result = run_strategy_core_mode(yaml_path)
        if result.total_trades < min_significant_trades:
            return CandidateResult(
                candidate_id=candidate.candidate_id,
                evaluated_at=datetime.utcnow(),
                metrics=None, trades=None,
                total_trades=result.total_trades,
                error=RejectionReason.REJECTED_INSUFFICIENT_TRADES.value
            )
        return CandidateResult(
            candidate_id=candidate.candidate_id,
            evaluated_at=datetime.utcnow(),
            metrics=result.metrics,
            trades=result.trades,
            total_trades=result.total_trades
        )
    except Exception as e:
        logger.error(f"Candidate {candidate.candidate_id} failed: {e}", exc_info=True)
        return CandidateResult(
            candidate_id=candidate.candidate_id,
            evaluated_at=datetime.utcnow(),
            metrics=None, trades=None, total_trades=None, error=str(e)
        )
    finally:
        CacheManager.clear_all_caches()
        yaml_path.unlink(missing_ok=True)  # always clean up temp YAML

# ── orchestrator.py — checkpoint skip pattern ────────────────────────────────
if store.get_checkpoint(run_id) >= Checkpoint.RANDOM_SEARCH_COMPLETE:
    logger.info("Stage 1 already complete — skipping")
else:
    _run_random_search(config, store, run_metadata)
    store.set_checkpoint(run_id, Checkpoint.RANDOM_SEARCH_COMPLETE)

# ── candidate_store.py — single-writer queue pattern ────────────────────────
import queue, threading

class CandidateStore:
    def __init__(self, db_path: Path):
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA synchronous = NORMAL")
        self._queue: queue.Queue = queue.Queue()
        self._writer = threading.Thread(target=self._drain_queue, daemon=True)
        self._writer.start()

    def write_candidate(self, record: CandidateRecord) -> None:
        self._queue.put(record)          # non-blocking for worker processes

    def _drain_queue(self) -> None:
        while True:
            record = self._queue.get()
            if record is None:           # sentinel — time to shut down
                break
            self._do_write(record)
            self._queue.task_done()

    def close(self) -> None:
        self._queue.join()               # flush all pending writes first
        self._queue.put(None)            # send sentinel to stop drain loop
        self._writer.join()
        self._conn.close()

# ── CandidateParameterSet — always use the factory, never the constructor ───
candidate = CandidateParameterSet.create(
    zone_name="safe",
    parameters={"rsi_period": 14, "atr_multiplier": 2.0, "session_filter": "london"},
    generation=None    # None = Random Search; int = GA generation number
)

# ── fitness.py — constraint evaluation order (cheapest rejection first) ─────
import operator as op

CONSTRAINT_CHECKS = [
    # (field_name,        actual_attr,           threshold_attr,          comparator)
    ("max_drawdown",      "max_drawdown",         "max_drawdown",          op.gt),
    ("win_rate",          "win_rate",             "min_win_rate",          op.lt),
    ("losing_streak",     "max_losing_streak",    "max_losing_streak",     op.gt),
    ("trades_per_week",   "trades_per_week",      "min_trades_per_week",   op.lt),
    ("expectancy",        "expectancy",           "min_expectancy",        op.lt),
    ("profit_factor",     "profit_factor",        "min_profit_factor",     op.lt),
]

def evaluate_fitness(result: CandidateResult, scenario: ScenarioProfile) -> FitnessResult:
    if not result.is_valid:
        return FitnessResult(candidate_id=result.candidate_id,
                             scenario_name=scenario.name,
                             fitness_score=None,
                             passed_constraints=False,
                             rejection_reason=result.error, ...)
    m = result.metrics
    for name, metric_attr, threshold_attr, comparator in CONSTRAINT_CHECKS:
        actual = getattr(m, metric_attr)
        threshold = getattr(scenario, threshold_attr)
        if comparator(actual, threshold):
            return FitnessResult(candidate_id=result.candidate_id,
                                 passed_constraints=False,
                                 failing_constraint=name,
                                 failing_value=actual, ...)
    score = _compute_weighted_score(m, scenario)   # returns float in [0, 1]
    return FitnessResult(candidate_id=result.candidate_id,
                         passed_constraints=True,
                         fitness_score=score, ...)

# ── ga/ga_engine.py — random window sampling per generation ─────────────────
import random

def _sample_ga_windows(all_windows: List[WFOWindow], rng: random.Random) -> List[WFOWindow]:
    """Sample 2 windows without replacement. Called once per generation."""
    return rng.sample(all_windows, k=2)   # requires len(all_windows) >= 3 (validated Stage 0)
```
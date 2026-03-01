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
A fully automated 8-stage optimization pipeline for the WBWSStrategy trading strategy.
It answers: does this strategy have real trading potential, and if yes, what is the optimal setup?
The pipeline produces go/borderline/no-go verdicts based on two mandatory evidence pillars.

---

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

---

## Verdict Model
**Two mandatory pillars** (not four, not one):
1. WFO temporal consistency score — composite of four metrics:
   - median window return
   - window-to-window return variance (inverted — lower = better)
   - worst-window drawdown (inverted — lower = better)
   - fraction of positive windows
2. MC deep ruin probability

**Three outcomes**: auto_go | borderline (human review) | no_go

Verdict logic (exact — do not approximate):
- `AUTO_GO`: both pillars pass go thresholds AND no modifier flags set
- `BORDERLINE`: either pillar in borderline zone OR any modifier flag (spike, oos_gate, window_collapse, profile_incomplete)
- `NO_GO`: either pillar in no_go zone — no modifier flags can override this

Modifier flags (any one → borderline, never no_go on their own):
- `sensitivity_spike` — |fitness_delta| > spike_threshold for any parameter
- `oos_gate_triggered` — only when `enforce_oos_gate: true` AND IS/OOS degradation > 50%
- `window_collapse_flag` — any WFO window showed severe drawdown (≥ 40%)
- `sensitivity_profile_incomplete` — >50% of perturbation evaluations failed

`VerdictResult.deployment_status`: always `PAPER_TRADE_REQUIRED` for go/borderline.
Operator manually sets `LIVE_APPROVED` after paper trading period. Never set in code.

---

## Scenario System
One active scenario per run. Defined in `backtest_template.yaml` under `scenario:`.
Scenario controls: fitness weights, constraint thresholds, WFO temporal weights, verdict thresholds, report framing.
Built-in: `capital_accumulation`, `swing_trading`, `conservative`.
Custom scenarios via YAML only — never hardcode scenario logic.
Concrete values for all three built-in scenarios: `docs/backtesting/TECHNICAL_SPEC.md` Section 5.

---

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

# CORRECT — datetime (Python 3.12+ compatible)
from datetime import datetime, UTC
evaluated_at = datetime.now(UTC)

# WRONG — deprecated in Python 3.12+
evaluated_at = datetime.utcnow()  # emits DeprecationWarning — do not use in new code
```

---

## Module Map (one module, one concern)

### Phase 2 — Core Infrastructure (complete ✓)
```
orchestrator.py           — sequences stages, checkpoints, resume. Writes immutable run
                            artifacts (config hash, seeds) at start. Stages 5/6/7 stubs
                            to be fully wired in Phase 4.
parameter_space.py        — expands YAML zones to parameter sets. No strategy knowledge.
sampler.py                — LHS or random selection from expanded space. No evaluation.
scenario.py               — loads ScenarioProfile from YAML. One profile per run.
strategy_runner.py        — single candidate evaluation. Builds temp YAML, calls core mode.
                            Accepts date_start / date_end kwargs to scope to a WFO window.
                            Returns failed CandidateResult on any error — never raises.
fitness.py                — stateless. Receives MetricsReport + ScenarioProfile. Returns FitnessResult.
candidate_store.py        — SQLite. WAL mode + single-writer queue. Thread-safe writes.
                            Public methods: write_candidate(), write_wfo_window_result(),
                            write_wfo_consistency_score(), flag_candidate_wfo_insufficient(),
                            get_checkpoint(), set_checkpoint(), query_candidates(),
                            rank_by_wfo(), close().
ranker.py                 — stateless. Query spec in → ranked list out.
```

### Phase 3 — Optimization Engines (complete ✓)
```
wfo/window_generator.py   — reads window definitions from YAML. Returns sorted WFOWindow list.
                            Validates: min 3 windows, no overlaps, valid date order.
wfo/wfo_evaluator.py      — one candidate, one window. Returns WFOWindowResult. Never raises.
                            Calls strategy_runner.evaluate() with date_start/date_end.
wfo/wfo_engine.py         — orchestrates WFO. "lightweight" mode for GA (pre-selected windows),
                            "full" mode for Stage 4 (all windows). Flags WFO_INSUFFICIENT_WINDOWS
                            when >50% of windows fail per candidate.
wfo/consistency_scorer.py — aggregates WFOWindowResults → four temporal metrics →
                            composite WFO consistency score [0,1]. Returns WFOConsistencyScore.
                            Median return: sigmoid-normalised. Variance + worst_dd: inverted.
ga/population.py          — population init from MC_PREFILTER_PASS records. Elite extraction.
ga/selection.py           — tournament selection. Configurable tournament size.
ga/crossover.py           — uniform crossover. zone_name inherited from parent_a.
ga/mutation.py            — Gaussian on step grid (int/float), random flip (choice).
                            All params strictly clamped to zone min/max.
ga/diversity.py           — hybrid Euclidean/Hamming distance (D-11). RMS normalised per
                            continuous param. Linear penalty scalar in [0, penalty_weight].
ga/ga_engine.py           — full evolution loop. rng.sample(wfo_windows, k=2) per generation.
                            Diversity penalty applied pre-selection. Elite preservation.
                            Stagnation early stop. All candidates written to store with stage=GA.
monte_carlo/perturbation.py    — named profiles from YAML. Pre-filter: shuffle + spread noise.
                                 Deep: all 5 perturbation types. Vectorised numpy for deep mode.
monte_carlo/equity_simulator.py — simulate_paths() → shape (n_iterations, n_trades+1).
                                   Fully vectorised via np.cumsum. No Python loops over paths.
                                   Deterministic for same seed. Per-path derived seeds.
monte_carlo/mc_metrics.py      — compute_metrics() → (avg_final_equity, worst_drawdown,
                                  ruin_probability, p5_final_equity). All vectorised numpy.
                                  Drawdown via np.maximum.accumulate. Ruin via np.min per path.
monte_carlo/mc_engine.py       — pre-filter and deep mode dispatch. Never raises.
                                  All failures → MCResult(error=str(exc)).
```

### Phase 4 — Evaluation Layer (to build)
```
evaluation/sensitivity.py — perturbs each parameter ±1/±2 steps. Parallel via ProcessPoolExecutor.
                            profile_complete=False if >50% perturbations failed (auto-borderline).
                            Returns SensitivityProfile.
evaluation/verdict.py     — two-pillar logic + modifier flags. Exact verdict logic per spec.
                            Never sets deployment_status=LIVE_APPROVED. Returns VerdictResult.
yaml_generator.py         — top candidate → base strategy YAML merged with candidate params.
                            Embeds metadata: scenario, run_id, config_hash, deployment_status.
                            Validates output as StrategyConfig before writing.
report_generator.py       — reads entirely from CandidateStore. Jinja2 HTML, scenario-framed.
                            Inline base64 charts (matplotlib Agg backend).
                            Borderline checklist as separate HTML file per borderline candidate.
                            JSON/Parquet output configurable via output.formats in YAML.
```

---

## Contracts Checklist
Before writing any inter-module call, verify the contract in `TECHNICAL_SPEC.md`:
- `RunMetadata` — run_id (UUID4), config_hash (SHA-256 64-char hex), scenario_name, 5 seeds, wfo_window_ids (tuple, min 3), checkpoint (Checkpoint enum), backtester_version
- `ScenarioProfile` — 6 fitness weights (must sum=1.0), 6 constraint thresholds, mc_prefilter_ruin_threshold, 4 WFO temporal weights (must sum=1.0), 5 verdict thresholds, report_emphasis tuple
- `CandidateParameterSet` — zone_name, parameters dict, candidate_id (SHA-256 of params). **Always use `.create()` factory — never call constructor directly.**
- `CandidateResult` — candidate_id, evaluated_at, metrics (Optional), trades (Optional), total_trades (Optional), error (Optional). `is_valid` property.
- `FitnessResult` — candidate_id, scenario_name, fitness_score (Optional, must be in [0,1]), passed_constraints, rejection_reason, failing_constraint, failing_value, 6 constraint actuals
- `WFOWindow` — window_id, start_date, end_date (start must be before end)
- `WFOWindowResult` — candidate_id, window_id, evaluated_at, fitness_score (Optional), total_trades, net_pnl, max_drawdown, win_rate, expectancy, profit_factor, oos_delta, error. `is_valid` property.
- `WFOConsistencyScore` — candidate_id, windows_evaluated, windows_total, median_window_return (raw float), window_return_variance (raw float), worst_window_drawdown (raw float), fraction_positive_windows [0,1], composite_score [0,1], oos_gate_triggered, window_collapse_flag
- `MCResult` — candidate_id, mode (MCMode enum), perturbation_profile_name, iterations, evaluated_at, avg_final_equity, worst_drawdown_across_paths, ruin_probability [0,1], p5_final_equity, error. `is_valid` property.
- `ParameterSensitivity` — parameter_name, step (int: -2/-1/+1/+2), perturbed_value, fitness_delta (Optional), evaluation_error (Optional)
- `SensitivityProfile` — candidate_id, baseline_fitness [0,1], parameter_sensitivities (tuple of ParameterSensitivity), spike_detected, spike_parameters (tuple of str, non-empty when spike_detected=True), profile_complete
- `VerdictResult` — candidate_id, scenario_name, verdict (Verdict enum), deployment_status (DeploymentStatus enum — always PAPER_TRADE_REQUIRED for go/borderline), wfo_consistency_score (Optional), mc_deep_ruin_probability (Optional), sensitivity_spike, oos_gate_triggered, window_collapse_flag, sensitivity_profile_incomplete, median_oos_delta, parameter_region_width, yaml_output_path, evidence_summary (must not be empty)
- `CandidateRecord` — flattened SQLite row. All fields as primitives. parameters_json is JSON backup. Individual parameter columns are primary storage.

---

## All Decisions Resolved — Quick Reference
ALL 12 decisions RESOLVED. Do not re-open without explicit operator instruction.
| D | Decision | Resolution |
|---|---|---|
| D-01 | Integration mode | Direct Python call in worker process. Benchmark passed Phase 2. |
| D-02 | SQLite concurrency | WAL mode + single-writer queue. Benchmark passed Phase 2. |
| D-03 | Temp YAML lifecycle | Per-candidate, named by param hash. Deleted in `finally`. `retain_temp_yamls: true` for debug. |
| D-04 | GA seeding | Top-N by fitness from MC_PREFILTER_PASS. Diversity handled by penalty during evolution. |
| D-05 | GA WFO windows | Randomly sample 2 per generation from full list. Min 3 windows required (Stage 0 validates). Validated Phase 3. |
| D-06 | Stage counts | 200/zone → 120 → pop 60 / 30 gen → 30 → 10 → 5. All YAML-configurable. |
| D-07 | Verdict thresholds | WFO: go≥0.65 / borderline 0.40–0.65 / no_go<0.40. MC: go≤5% / borderline 5–15% / no_go>15%. Scenario-specific. Calibrate Phase 6. |
| D-08 | Sensitivity scope | All parameters (~300 evals, ~200s at 6 workers). |
| D-09 | Output formats | Both JSON + Parquet, both on by default. Independently disableable. |
| D-10 | Report generator | Build new. Jinja2 HTML. Too different from single-run strategy report to extend. |
| D-11 | Diversity distance | Hybrid: normalised Euclidean (continuous params) + Hamming (discrete params), weighted avg by type fraction. |
| D-12 | IS/OOS gate default | Off by default. Opt-in via `enforce_oos_gate: true`. >50% degradation = borderline flag (never auto-reject). |

---

## SQLite Schema — 9 Tables (full schema in SQLITE_SCHEMA.md)
```
runs                   — one row per run. Immutable: config_hash, 5 seeds, perturbation_profile_name.
candidates             — one row per unique candidate_id.
candidate_parameters   — all parameter values as individual columns + parameters_json backup.
evaluations            — one row per candidate per stage. All constraint actuals + fitness.
wfo_window_results     — one row per candidate per window (GA lightweight + full WFO, flagged separately).
wfo_consistency_scores — four sub-metrics (raw floats) + composite score per candidate.
mc_results             — pre-filter and deep as separate rows per candidate.
sensitivity_results    — one row per candidate per parameter per step.
sensitivity_profiles   — summary: spike_detected, spike_parameters, profile_complete per candidate.
verdicts               — final verdict + full evidence + deployment_status per candidate.
```
**Schema rules:**
- One row per candidate per stage — never denormalised blobs
- All numeric metrics as individual columns — no JSON-serialised metric blobs in primary columns
- All parameter values as individual columns — enables `WHERE rsi_period > 14 AND atr_multiplier < 2.0`
- All rows have timestamps
- No information destroyed — every MetricsReport field is a column
- ML-ready: `SELECT * FROM candidates JOIN verdicts WHERE verdict != 'no_go'` = feature matrix

---

## Adversarial Challenge Suite
- **AV-01** Random-signal baseline: replace signals with coin flips → all candidates must receive `no_go`. **Run as smoke test at end of Phase 4.**
- **AV-02** Overfit-injection test: curve-fit strategy tuned to one window → must be flagged borderline or no_go at WFO stage.
- **AV-03** Meta-config stability: >80% verdict stability under seed/iteration perturbation on known-robust candidates.
- **AV-04** Borderline escalation: adversarial checklist HTML generated for every borderline candidate — operator sign-off required before deployment.

---

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
- Do not set `deployment_status = LIVE_APPROVED` anywhere in code — operator-only manual action
- Do not use `datetime.utcnow()` in new code — use `datetime.now(UTC)` (Python 3.12+ compatible)

---

## Platform / Environment Notes
- **OS**: Windows 10. Always use `pathlib.Path`. Always use `ProcessPoolExecutor` with spawn mode.
- **Broker**: eToro. No API integration — future project.
- **Data timezone**: All OHLCV data and strategy signals operate in **CET/CEST**. Internal pipeline timestamps use UTC. Report wall-clock displays should note this distinction.
- **Path resolution**: Always use `src/utils/paths.py`. Never hardcode path separators or project roots.

---

## Common Patterns
```python
# ── strategy_runner.py — never raises, always returns CandidateResult ──────
from datetime import datetime, UTC

def evaluate(
    candidate: CandidateParameterSet,
    base_yaml_path: Path,
    temp_dir: Path,
    min_significant_trades: int,
    date_start=None,   # Optional[date] — scopes evaluation to WFO window
    date_end=None,     # Optional[date] — scopes evaluation to WFO window
) -> CandidateResult:
    yaml_path = temp_dir / f"candidate_{candidate.candidate_id[:12]}.yaml"
    try:
        _write_temp_yaml(candidate, base_yaml_path, yaml_path, date_start, date_end)
        result = run_strategy_core_mode(yaml_path)
        if result.total_trades < min_significant_trades:
            return CandidateResult(
                candidate_id=candidate.candidate_id,
                evaluated_at=datetime.now(UTC),
                metrics=None, trades=None,
                total_trades=result.total_trades,
                error=RejectionReason.REJECTED_INSUFFICIENT_TRADES.value
            )
        return CandidateResult(
            candidate_id=candidate.candidate_id,
            evaluated_at=datetime.now(UTC),
            metrics=result.metrics,
            trades=result.trades,
            total_trades=result.total_trades
        )
    except Exception as e:
        logger.error(f"Candidate {candidate.candidate_id} failed: {e}", exc_info=True)
        return CandidateResult(
            candidate_id=candidate.candidate_id,
            evaluated_at=datetime.now(UTC),
            metrics=None, trades=None, total_trades=None, error=str(e)
        )
    finally:
        CacheManager.clear_all_caches()
        yaml_path.unlink(missing_ok=True)

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
        self._queue.put(record)          # non-blocking for callers

    def write_wfo_window_result(self, result: WFOWindowResult, run_id: str) -> None:
        self._queue.put(("wfo_window", result, run_id))

    def write_wfo_consistency_score(self, score: WFOConsistencyScore, run_id: str) -> None:
        self._queue.put(("wfo_consistency", score, run_id))

    def flag_candidate_wfo_insufficient(self, candidate_id: str, run_id: str) -> None:
        self._queue.put(("wfo_insufficient", candidate_id, run_id))

    def _drain_queue(self) -> None:
        while True:
            item = self._queue.get()
            if item is None:
                break
            self._do_write(item)
            self._queue.task_done()

    def close(self) -> None:
        self._queue.join()
        self._queue.put(None)
        self._writer.join()
        self._conn.close()

# ── CandidateParameterSet — always use the factory ───────────────────────────
candidate = CandidateParameterSet.create(
    zone_name="safe",
    parameters={"rsi_period": 14, "atr_multiplier": 2.0, "session_filter": "london"},
    generation=None    # None = Random Search; int = GA generation number
)

# ── fitness.py — constraint evaluation order (cheapest rejection first) ─────
import operator as op

CONSTRAINT_CHECKS = [
    ("max_drawdown",    "max_drawdown",      "max_drawdown",        op.gt),
    ("win_rate",        "win_rate",          "min_win_rate",        op.lt),
    ("losing_streak",   "max_losing_streak", "max_losing_streak",   op.gt),
    ("trades_per_week", "trades_per_week",   "min_trades_per_week", op.lt),
    ("expectancy",      "expectancy",        "min_expectancy",      op.lt),
    ("profit_factor",   "profit_factor",     "min_profit_factor",   op.lt),
]

# ── ga/ga_engine.py — random window sampling per generation ─────────────────
def _sample_ga_windows(all_windows: List[WFOWindow], rng: random.Random) -> List[WFOWindow]:
    """Sample 2 windows without replacement. Called once per generation."""
    return rng.sample(all_windows, k=2)   # requires len(all_windows) >= 3 (validated Stage 0)

# ── monte_carlo/equity_simulator.py — vectorised path simulation ─────────────
# All N paths computed in one call. No Python loops over paths.
equity_paths = np.hstack([
    np.full((n_iterations, 1), starting_equity),
    starting_equity + np.cumsum(all_returns, axis=1)
])  # shape: (n_iterations, n_trades + 1)

# ── evaluation/verdict.py — deployment_status guard ─────────────────────────
# CORRECT — always PAPER_TRADE_REQUIRED in code
VerdictResult(
    verdict=Verdict.AUTO_GO,
    deployment_status=DeploymentStatus.PAPER_TRADE_REQUIRED,  # always
    ...
)
# WRONG — never set LIVE_APPROVED in code
deployment_status=DeploymentStatus.LIVE_APPROVED  # never — operator-only
```

---

## Key Files to Read Before Coding
1. `docs/backtesting/TECHNICAL_SPEC.md` — all contracts + all decisions + YAML schema + scenario profiles
2. `docs/backtesting/SQLITE_SCHEMA.md` — full database schema + query examples
3. `docs/backtesting/FUNCTIONAL_SPEC.md` — plain-language description of all 8 stages
4. `docs/backtesting/CONTEXT.md` — current phase status, next task, and integration bridge items
5. `docs/strategies/architecture/ARCHITECTURE.md` — strategy architecture (frozen input)
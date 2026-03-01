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
- `window_collapse_flag` — any WFO window showed severe drawdown
- `sensitivity_profile_incomplete` — >50% of perturbation evaluations failed

`VerdictResult.deployment_status`: always `PAPER_TRADE_REQUIRED` for go/borderline.
`VerdictResult.__post_init__` raises `ValueError` if `LIVE_APPROVED` is set for go/borderline — this is enforced at contract construction, not just convention.
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
evaluated_at = datetime.utcnow()  # do not use in any new code
```

---

## Module Map (one module, one concern)

### Phase 2 — Core Infrastructure (complete ✓)
```
orchestrator.py           — sequences stages, checkpoints, resume. Writes immutable run
                            artifacts (config hash, seeds) at start. Stages 5/6/7 fully wired in Phase 4.
parameter_space.py        — expands YAML zones to parameter sets. No strategy knowledge.
sampler.py                — LHS or random selection from expanded space. No evaluation.
scenario.py               — loads ScenarioProfile from YAML. One profile per run.
strategy_runner.py        — single candidate evaluation. Builds temp YAML, calls core mode.
                            Accepts date_start / date_end kwargs to scope to a WFO window.
                            Returns failed CandidateResult on any error — never raises.
fitness.py                — stateless. Receives MetricsReport + ScenarioProfile. Returns FitnessResult.
candidate_store.py        — SQLite. WAL mode + single-writer queue. Thread-safe writes.
                            Public write methods: write_candidate(), write_wfo_window_result(),
                            write_wfo_consistency_score(), flag_candidate_wfo_insufficient(),
                            write_mc_result(), write_sensitivity_profile(), write_verdict().
                            Public query methods: get_checkpoint(), set_checkpoint(),
                            query_candidates(), query_verdicts(), query_wfo_consistency_scores(),
                            query_mc_results(), query_sensitivity_profiles(),
                            query_wfo_window_results(), query_sensitivity_results(),
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
ga/population.py          — population init from MC_PREFILTER_PASS records. Elite extraction.
ga/selection.py           — tournament selection. Configurable tournament size.
ga/crossover.py           — uniform crossover. zone_name inherited from parent_a.
ga/mutation.py            — Gaussian on step grid (int/float), random flip (choice).
                            All params strictly clamped to zone min/max.
ga/diversity.py           — hybrid Euclidean/Hamming distance (D-11). Linear penalty scalar.
ga/ga_engine.py           — full evolution loop. rng.sample(wfo_windows, k=2) per generation.
                            Diversity penalty applied pre-selection. Elite preservation.
                            Stagnation early stop. All candidates written to store with stage=GA.
monte_carlo/perturbation.py    — named profiles from YAML. Pre-filter: shuffle + spread noise.
                                 Deep: all 5 perturbation types. Vectorised numpy for deep mode.
monte_carlo/equity_simulator.py — simulate_paths() → shape (n_iterations, n_trades+1).
                                   Fully vectorised via np.cumsum. No Python loops over paths.
monte_carlo/mc_metrics.py      — compute_metrics() → (avg_final_equity, worst_drawdown,
                                  ruin_probability, p5_final_equity). All vectorised numpy.
monte_carlo/mc_engine.py       — pre-filter and deep mode dispatch. Never raises.
```

### Phase 4 — Evaluation Layer (complete ✓)
```
evaluation/sensitivity.py — perturbs each parameter ±1/±2 steps. Parallel via ProcessPoolExecutor.
                            Worker function: _evaluate_perturbation() — patch THIS in tests, not
                            the functions it calls internally (patches don't cross process boundaries).
                            profile_complete=False if >50% perturbations failed.
                            spike_detected=True if any |fitness_delta| > spike_threshold.
                            Returns SensitivityProfile.
evaluation/verdict.py     — two-pillar logic + modifier flags. Exact verdict logic (see above).
                            oos_gate_triggered only fires when oos_gate_enabled=True AND
                            WFOConsistencyScore.oos_gate_triggered=True (two-condition guard).
                            Never sets deployment_status=LIVE_APPROVED. Returns VerdictResult.
yaml_generator.py         — merges candidate params into base strategy YAML via _STRATEGY_PARAM_KEY_MAP.
                            Embeds backtester_metadata: scenario, run_id, config_hash, all 5 seeds,
                            deployment_status=PAPER_TRADE_REQUIRED, verdict, wfo/mc scores.
                            Validates via StrategyConfig.from_yaml() if importable, else structural check.
                            build_output_path(): {output_dir}/trading_yamls/{run_id[:8]}_{cid[:12]}_strategy.yaml
report_generator.py       — reads entirely from CandidateStore via duck-typed query interface.
                            Self-contained HTML: no Jinja2 dep, no external CSS/JS. f-string rendering.
                            Scenario-framed: report_emphasis controls metric cell order.
                            Charts: matplotlib Agg → base64 PNG inline. Gracefully skipped on error.
                            Adversarial checklist: separate HTML per borderline candidate in checklists/.
                            JSON: per-candidate flat record → json/. Parquet: → parquet/ (pandas).
```

### Phase 5 — Orchestrator Audit
```
orchestrator.py           — audit completness.
                            confirm CandidateStore.close() in finally block.
```
---

## Contracts Checklist
Before writing any inter-module call, verify the contract in `TECHNICAL_SPEC.md`:
- `RunMetadata` — run_id (UUID4), config_hash (SHA-256 64-char hex), scenario_name, 5 seeds, wfo_window_ids (tuple, min 3), checkpoint (Checkpoint enum), backtester_version
- `ScenarioProfile` — 6 fitness weights (must sum=1.0), 6 constraint thresholds, mc_prefilter_ruin_threshold, 4 WFO temporal weights (must sum=1.0), 5 verdict thresholds, report_emphasis tuple
- `CandidateParameterSet` — zone_name, parameters dict, candidate_id (SHA-256 of params). **Always use `.create()` factory.** `Candidate` is NOT a defined contract — do not import or use it.
- `CandidateResult` — candidate_id, evaluated_at, metrics (Optional), trades (Optional), total_trades (Optional), error (Optional). `is_valid` property.
- `FitnessResult` — candidate_id, scenario_name, fitness_score (Optional [0,1]), passed_constraints, rejection_reason, failing_constraint, failing_value, 6 constraint actuals
- `WFOWindow` — window_id, start_date, end_date (start must be before end)
- `WFOWindowResult` — candidate_id, window_id, evaluated_at, fitness_score, total_trades, net_pnl, max_drawdown, win_rate, expectancy, profit_factor, oos_delta, error. `is_valid` property.
- `WFOConsistencyScore` — candidate_id, windows_evaluated, windows_total, 4 sub-metrics (raw floats), composite_score [0,1], oos_gate_triggered, window_collapse_flag
- `MCResult` — candidate_id, mode (MCMode enum), perturbation_profile_name, iterations, evaluated_at, avg_final_equity, worst_drawdown_across_paths, ruin_probability [0,1], p5_final_equity, error. `is_valid` property.
- `ParameterSensitivity` — parameter_name, step (int: -2/-1/+1/+2), perturbed_value, fitness_delta (Optional), evaluation_error (Optional)
- `SensitivityProfile` — candidate_id, baseline_fitness [0,1], parameter_sensitivities (Tuple[ParameterSensitivity,...]), spike_detected, spike_parameters (Tuple[str,...], non-empty when spike_detected=True), profile_complete
- `VerdictResult` — candidate_id, scenario_name, verdict (Verdict enum), deployment_status (always PAPER_TRADE_REQUIRED for go/borderline — enforced by __post_init__), wfo_consistency_score, mc_deep_ruin_probability, 4 modifier flags, median_oos_delta, parameter_region_width, yaml_output_path, evidence_summary (non-empty)
- `CandidateRecord` — flattened SQLite row. All fields as primitives. parameters_json is JSON backup.
---
## All Decisions Resolved — Quick Reference
ALL 12 decisions RESOLVED. Do not re-open without explicit operator instruction.
| D | Decision | Resolution |
|---|---|---|
| D-01 | Integration mode | Direct Python call in worker process. Benchmark passed Phase 2. |
| D-02 | SQLite concurrency | WAL mode + single-writer queue. Benchmark passed Phase 2. |
| D-03 | Temp YAML lifecycle | Per-candidate, named by param hash. Deleted in `finally`. `retain_temp_yamls: true` for debug. |
| D-04 | GA seeding | Top-N by fitness from MC_PREFILTER_PASS. Diversity handled by penalty during evolution. |
| D-05 | GA WFO windows | Randomly sample 2 per generation from full list. Min 3 windows required (Stage 0 validates). |
| D-06 | Stage counts | 200/zone → 120 → pop 60 / 30 gen → 30 → 10 → 5. All YAML-configurable. |
| D-07 | Verdict thresholds | WFO: go≥0.65 / borderline 0.40–0.65 / no_go<0.40. MC: go≤5% / borderline 5–15% / no_go>15%. Scenario-specific. |
| D-08 | Sensitivity scope | All parameters (~300 evals, ~200s at 6 workers). |
| D-09 | Output formats | Both JSON + Parquet, both on by default. Independently disableable. |
| D-10 | Report generator | Build new. Self-contained HTML, f-string rendering (no Jinja2 file dep). |
| D-11 | Diversity distance | Hybrid: normalised Euclidean (continuous) + Hamming (discrete), weighted avg by type fraction. |
| D-12 | IS/OOS gate default | Off by default. Opt-in via `enforce_oos_gate: true`. >50% degradation = borderline flag. |
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
---
## Adversarial Challenge Suite
- **AV-01** ✓ PASSED (Phase 4): Random-signal baseline → 0 AUTO_GO verdicts on 100 candidates. Pipeline thresholds validated.
- **AV-02** Overfit-injection test: curve-fit strategy tuned to one window → must be flagged borderline or no_go at WFO stage. Run in Phase 6 calibration.
- **AV-03** Meta-config stability: >80% verdict stability under seed/iteration perturbation on known-robust candidates. Phase 6.
- **AV-04** Borderline escalation: adversarial checklist HTML generated for every borderline candidate — operator sign-off required. Implemented in report_generator.py ✓.

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

# ── orchestrator.py — always close store ─────────────────────────────────────
try:
    _run_all_stages(...)
finally:
    store.close()   # drains write queue, joins writer thread, closes connection

# ── evaluation/verdict.py — oos_gate two-condition guard ────────────────────
# CORRECT — both conditions required
oos_gate_triggered: bool = oos_gate_enabled and wfo_score.oos_gate_triggered

# WRONG — ignores whether gate is enabled in config
oos_gate_triggered: bool = wfo_score.oos_gate_triggered  # never

# ── evaluation/verdict.py — deployment_status guard ─────────────────────────
# CORRECT — always PAPER_TRADE_REQUIRED in code
VerdictResult(
    verdict=Verdict.AUTO_GO,
    deployment_status=DeploymentStatus.PAPER_TRADE_REQUIRED,  # always
    ...
)
# WRONG — never set LIVE_APPROVED in code; contract __post_init__ raises ValueError
deployment_status=DeploymentStatus.LIVE_APPROVED  # never — operator-only

# ── yaml_generator.py — build canonical output path ─────────────────────────
from src.backtesting.yaml_generator import build_output_path
out_path = build_output_path(output_dir, run_id, candidate_id)
# → {output_dir}/trading_yamls/{run_id[:8]}_{candidate_id[:12]}_strategy.yaml

# ── Testing ProcessPoolExecutor-based code ───────────────────────────────────
# CORRECT — patch the worker function itself
with patch("src.backtesting.evaluation.sensitivity._evaluate_perturbation", side_effect=fake_fn):
    ...

# WRONG — patches in parent process don't propagate to spawned workers
with patch("src.backtesting.evaluation.sensitivity.runner_evaluate", fake_fn):
    ...  # runner_evaluate is called INSIDE _evaluate_perturbation in worker — patch won't apply

# ── CandidateParameterSet — always use the factory ───────────────────────────
candidate = CandidateParameterSet.create(
    zone_name="safe",
    parameters={"rsi_period": 14, "atr_multiplier": 2.0, "session_filter": "london"},
    generation=None    # None = Random Search; int = GA generation number
)
# NOTE: "Candidate" is NOT a defined contract. CandidateParameterSet is the correct type.
```

---

## Test Counts by Phase
| Phase | Unit Tests | Integration/Smoke | Total |
|---|---|---|---|
| Phase 2 | 3 | 1 | 4 (benchmarks) |
| Phase 3 | 50 | 1 | 51 |
| Phase 4 | 61 | 7 (4 AV-01 + 3 e2e) | 68 |
| **Cumulative** | **114** | **9** | **123** |

All 123 tests green as of end of Phase 4.

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
- Do not use `datetime.utcnow()` in any new code — use `datetime.now(UTC)` (Python 3.12+ compatible)
- Do not import or use `Candidate` — not a defined contract. Use `CandidateParameterSet`.
- Do not patch functions called inside ProcessPoolExecutor workers — patch the worker function itself.

---

## Platform / Environment Notes
- **OS**: Windows 10. Always use `pathlib.Path`. Always use `ProcessPoolExecutor` with spawn mode.
- **Broker**: eToro. No API integration — future project.
- **Data timezone**: All OHLCV data and strategy signals operate in **CET/CEST**. Internal pipeline timestamps use UTC.
- **Path resolution**: Always use `src/utils/paths.py`. Never hardcode path separators or project roots.
- **`datetime.utcnow()` status**: Phase 2/3 modules still use it (cleanup deferred to Phase 5). Phase 4 modules all use `datetime.now(UTC)`. Do not introduce any new `utcnow()` calls.
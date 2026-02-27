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
Stage 0: Validation & Init
Stage 1: Random Search         (LHS, significance guard, constraint filter, single-run fitness)
Stage 2: MC Pre-Filter         (cheap — 2 perturbation types, ruin probability screen)
Stage 3: GA                    (WFO-aware fitness: 2 lightweight windows per candidate per generation)
Stage 4: Full WFO              (all configured windows, temporal consistency evidence)
Stage 5: MC Deep               (full iterations, all perturbation types, WFO survivors only)
Stage 6: Parameter Sensitivity (±1/±2 step per parameter, fitness delta map, spike = borderline)
Stage 7: Report & Output       (HTML + JSON/Parquet + SQLite + trading-ready YAML)
```

## Verdict Model

**Two mandatory pillars** (not four, not one):
1. WFO temporal consistency score (multi-window performance variance)
2. MC deep ruin probability

**Three outcomes**: auto-go | borderline (human review) | auto-reject
Sensitivity spike → borderline flag even if both pillars pass.
IS/OOS delta and parameter region width → informational only, never verdict gates.

## Scenario System

One active scenario per run. Defined in `backtest_template.yaml` under `scenario:`.
Scenario controls: fitness weights, constraint thresholds, report framing.
Built-in: `capital_accumulation`, `swing_trading`, `conservative`.
Custom scenarios via YAML only — never hardcode scenario logic.

## Architecture Rules (non-negotiable)

```python
# CORRECT — typed frozen contract between modules
@dataclass(frozen=True)
class CandidateResult:
    candidate_id: str
    metrics: MetricsReport
    trades: TradeResult
    error: Optional[str] = None

# WRONG — raw dict
result = {"metrics": ..., "trades": ...}  # never

# CORRECT — fail fast at construction
def __post_init__(self):
    if self.fitness_score < 0:
        raise ValueError(f"fitness_score cannot be negative: {self.fitness_score}")

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
orchestrator.py          — sequences stages, checkpoints, resume. Nothing else.
parameter_space.py       — expands YAML zones to parameter sets. No strategy knowledge.
sampler.py               — LHS or random selection from expanded space. No evaluation.
scenario.py              — loads ScenarioProfile from YAML. One profile per run.
strategy_runner.py       — single candidate evaluation. Builds temp YAML, calls core mode.
                           Returns failed CandidateResult on any error — never raises.
fitness.py               — stateless. Receives MetricsReport + ScenarioProfile. Returns FitnessResult.
candidate_store.py       — SQLite. WAL mode. Thread-safe writes. All pipeline state lives here.
ranker.py                — stateless. Query spec in → ranked list out.
ga/ga_engine.py          — GA loop only. Per-generation fitness via wfo_evaluator (2 windows).
wfo/wfo_engine.py        — orchestrates WFO. Used in two modes: lightweight (GA) and full (Stage 4).
wfo/wfo_evaluator.py     — one candidate, one window. Returns WFOWindowResult.
monte_carlo/mc_engine.py — two modes: pre-filter (Stage 2) and deep (Stage 5).
evaluation/sensitivity.py — perturbs each parameter ±1/±2 steps. Returns SensitivityProfile.
evaluation/verdict.py    — two-pillar logic + sensitivity modifier. Returns VerdictResult.
report_generator.py      — scenario-framed. HTML + JSON/Parquet. Reads CandidateStore only.
yaml_generator.py        — top candidate → validated trading-ready strategy YAML.
```

## Contracts Checklist

Before writing any inter-module call, verify the contract exists:
- `CandidateParameterSet` — parameter set for one candidate
- `CandidateResult` — strategy run output (metrics + trades + error)
- `FitnessResult` — score + constraint pass/fail
- `ScenarioProfile` — active scenario weights and thresholds
- `WFOWindow` — train/test date range
- `WFOWindowResult` — one candidate × one window evaluation
- `MCResult` — MC summary (avg equity, worst drawdown, ruin probability)
- `SensitivityProfile` — per-parameter fitness delta map
- `VerdictResult` — final go/borderline/no-go with evidence summary
- `CandidateRecord` — SQLite row representation (all stages)
- `RunMetadata` — run identity, config hash, checkpoint state

## SQLite Schema Rules

- One row per candidate per stage (not blobs)
- All numeric metrics as individual columns (not JSON-serialised)
- All parameter values as individual columns (enables direct WHERE queries)
- All rows have timestamps
- No information destroyed — every MetricsReport field is a column
- Schema is ML-ready: `SELECT * FROM candidates WHERE stage='MC_DEEP'` = feature matrix

## What NOT To Do

- Do not modify `src/strategies/` — strategy architecture is frozen input
- Do not use `analytics` mode inside the backtester loop — `core` mode only
- Do not add `print()` statements — use `structured_logger.py` from strategy architecture
- Do not hardcode parameter names outside `strategy_runner.py`
- Do not implement ML/AI layer — schema design only in v1
- Do not implement eToro API — future project
- Do not resolve open decisions D-01 or D-02 without benchmark data

## Key Files to Read Before Coding

1. `docs/backtesting/BACKTESTER_PLAN.md` — full requirements and pipeline design
2. `docs/backtesting/CONTEXT.md` — current phase status and open decisions
3. `docs/architecture/ARCHITECTURE.md` — strategy architecture (what you're integrating with)
4. `configs/backtesting/backtest_template.yaml` — the config this system reads

## Common Patterns

```python
# Candidate evaluation with isolation (strategy_runner.py pattern)
def evaluate(candidate: CandidateParameterSet) -> CandidateResult:
    try:
        yaml_path = _build_temp_yaml(candidate)
        result = run_strategy_core_mode(yaml_path)
        if result.total_trades < MIN_TRADES:
            return CandidateResult(
                candidate_id=candidate.id,
                metrics=None,
                trades=None,
                error=f"REJECTED_INSUFFICIENT_TRADES: {result.total_trades}"
            )
        return CandidateResult(candidate_id=candidate.id, metrics=result.metrics, trades=result.trades)
    except Exception as e:
        logger.error(f"Candidate {candidate.id} failed: {e}", exc_info=True)
        return CandidateResult(candidate_id=candidate.id, metrics=None, trades=None, error=str(e))
    finally:
        CacheManager.clear_all_caches()

# Stage checkpoint pattern (orchestrator.py)
if store.get_checkpoint() >= Checkpoint.RANDOM_SEARCH_COMPLETE:
    logger.info("Stage 1 already complete — skipping")
else:
    _run_random_search(config, store)
    store.set_checkpoint(Checkpoint.RANDOM_SEARCH_COMPLETE)
```
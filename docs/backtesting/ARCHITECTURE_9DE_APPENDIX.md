# ARCHITECTURE.md — Appendix Block 9D/9E
**Append this section to ARCHITECTURE.md after final implementation.**
**Generated**: 2026-03-05

---
## Section: Pipeline Stage Implementations (Blocks 9D–9E)

### Stages 1–3 Implementation Overview

Blocks 9D completed the implementation of Stages 1, 2, and 3. Stage 4 remains a
stub. All stage logic lives in `orchestrator.py` as private `_run_stage_N_*()` functions.
The orchestrator is the only module that calls `store.set_checkpoint()`.

#### Stage 1 — Random Search

**Entry**: `_run_stage_1_random_search(config, store, run_metadata)`

The parameter space is expanded from `config["zones"]` via `parameter_space.expand_zones()`,
which performs a Cartesian product of all enabled parameter ranges. Sampling uses
Latin Hypercube Sampling (LHS) by default (`config["random_search"]["method"] = "lhs"`)
or uniform random without replacement (`"random"`). LHS is preferred because it
guarantees space-filling coverage — each parameter value stratum appears exactly once.

Each sampled `CandidateParameterSet` is evaluated by `strategy_runner.evaluate()`,
which writes a temp YAML, runs the strategy in `core` mode, and applies the significance
guard (`min_significant_trades`). The result is passed to `fitness.evaluate_fitness()`,
which applies constraint checks in fail-fast order (cheapest first: drawdown, win_rate,
losing_streak, trades_per_week, expectancy, profit_factor) and computes a weighted
composite score in [0, 1] for passing candidates.

Both passing and failing candidates are written to the store as `CandidateRecord`
with `stage = CandidateStage.RANDOM.value`. All candidates are preserved for audit
queries regardless of outcome.

#### Stage 2 — Monte Carlo Pre-Filter

**Entry**: `_run_stage_2_mc_prefilter(config, store, run_metadata)`

Queries the top-N RANDOM-stage constraint-passing candidates by fitness score via
`ranker.rank()`. For each, retrieves the `CandidateResult` from the store and runs
a cheap MC simulation (`MCMode.PRE_FILTER`: low iteration count, 2 perturbation types)
via `monte_carlo.mc_engine.run_mc()`.

The ruin probability threshold is read from `scenario.mc_prefilter_ruin_threshold`
(not the raw config dict) so that scenario-specific thresholds are respected. Candidates
with `ruin_probability > threshold` are written with `stage = MC_PREFILTER_FAIL`;
passing candidates with `MC_PREFILTER_PASS`. The fitness and constraint actuals from
the RANDOM stage are preserved — Stage 2 does not re-evaluate fitness.

#### Stage 3 — Genetic Algorithm

**Entry**: `_run_stage_3_ga(config, store, run_metadata)`

Builds a typed `List[WFOWindow]` from `config["walk_forward"]["windows"]` using
`datetime.date.fromisoformat()`. The base YAML path is injected as a private key
(`config["_base_yaml_path"]`) into a shallow copy of the config before passing to
`ga_engine.run_ga()`. This is the B9B-003 contract: `ga_engine` reads this key but
never writes it back to the caller's config.

`ga_engine.run_ga()` is the full WFO-aware evolution loop. It seeds the initial
population from `MC_PREFILTER_PASS` candidates, samples 2 random WFO windows per
generation for lightweight fitness evaluation, applies a diversity penalty, and
preserves top elites unchanged. All GA candidate evaluations are written to the
store directly by `ga_engine` with `stage = CandidateStage.GA.value` and a
`generation` number. No additional store writes are needed in the orchestrator.

---
### IS/OOS Gate Architecture (Block 9E — B8B-005)

#### Design

The IS/OOS gate screens candidates for temporal consistency between in-sample and
out-of-sample periods within each WFO window. It is implemented as an optional
sub-evaluation triggered only when `enforce_oos_gate: true` is set in config.

The gate is **disabled by default** and intentionally never fires in GA lightweight
mode. The rationale: GA uses 2-window random sampling for speed; requiring IS/OOS
fitness on those already-cheap evaluations would triple the per-generation cost and
produce noisy deltas from very short sub-periods.

#### IS/OOS Split

Each `WFOWindow` is split into an in-sample period (IS) and out-of-sample period (OOS)
by calendar days using a fixed 70/30 ratio (`_IS_FRACTION = 0.70` in `wfo_evaluator.py`).
The WFO window contract (`WFOWindow`: `window_id`, `start_date`, `end_date`) carries no
IS/OOS boundary fields — the split is computed inside `evaluate_window()` and is not
part of the public contract. This avoids proliferating IS/OOS fields across the data model.

```
Window:   [start_date ──────────────────────────────── end_date]
IS:       [start_date ─────────────────── is_end_date)
OOS:                  [is_end_date ───────────────── end_date]
Split:     70%                             30%
```

`is_end_date = start_date + timedelta(days=int(total_days * 0.70))`

Guards:
- `total_days < 2`: returns `None` (window too short to split meaningfully)
- `is_days >= total_days`: clamped to `total_days - 1` (OOS minimum 1 day)

#### Delta Computation

`oos_delta = oos_fitness - is_fitness` where both fitness scores are in [0, 1].
A negative delta means OOS underperforms IS.

Special cases:
- **Either sub-evaluation fails** (strategy error, significance guard, missing metrics):
  `oos_delta = None`. None is the safe default — it cannot trigger a false gate.
- **OOS fails constraints** (fitness_score is None): `oos_fitness = 0.0` (floor value).
  This preserves the signal that OOS degraded severely enough to fail hard constraints,
  producing a large negative delta rather than silently dropping to None.

#### Gate Trigger

In `consistency_scorer.py`, `_check_oos_gate()` returns `True` when
`abs(statistics.median(oos_deltas)) > oos_degradation_threshold`.
`oos_gate_triggered=True` in `WFOConsistencyScore` causes the verdict engine to
apply the `oos_gate_triggered` modifier flag, which downgrades a potential
`AUTO_GO` to `BORDERLINE` or `NO_GO` depending on the full evidence picture.

The default `oos_degradation_threshold = 0.50` (50 fitness point drop across [0,1])
is intentionally lenient — it catches only severe degradation. The threshold should
be calibrated after the first real pipeline run by examining the distribution of
`median_oos_delta` values in the `wfo_consistency_scores` table.

#### Data Flow (full mode, gate enabled)

```
wfo_engine.run_wfo(oos_gate_enabled=True)
  → pool.submit(evaluate_window, ..., oos_gate_enabled=True)   # 7th positional arg
    → _evaluate_candidate(date_start, date_end)                 # full window
    → _compute_oos_delta()
        → _evaluate_candidate(date_start, is_end_date)          # IS period
        → _evaluate_candidate(is_end_date, date_end)            # OOS period
        → return oos_fitness - is_fitness
    → WFOWindowResult(oos_delta=<float or None>)
  → consistency_scorer.compute_consistency()
    → _check_oos_gate(valid_results, threshold)
    → median_oos_delta = median([r.oos_delta for r where not None])
    → WFOConsistencyScore(oos_gate_triggered=<bool>, median_oos_delta=<float>)
```

**Important**: `fitness_score` and all metrics fields in `WFOWindowResult` are taken
from the **full window** evaluation (not IS or OOS alone). This ensures GA lightweight
mode and full WFO mode use the same metric basis, and that WFO consistency scoring
reflects the full window's performance, not a split sub-period.

---
### Normalisation Architecture (pending calibration)

Two normalisation constants are currently hardcoded and require calibration after
the first real pipeline run:

**B8B-012 — WFO sigmoid scale** (`consistency_scorer.py`)
```python
# Current (uncalibrated):
median_return_norm = _sigmoid_normalise(median_return_raw, scale=0.10)
# 0.10 was intended for unit-fraction returns. Strategy metrics are in points/pips.
# Fix: scale ≈ stdev(net_pnl_across_windows) * 0.50
# Target: net_pnl of 1 stdev → sigmoid ≈ 0.67 (good-but-not-exceptional)
```

**B8B-003 — Expectancy normalisation ceiling** (`fitness.py`)
```python
# Current (uncalibrated):
expectancy_norm = _clamp(expectancy_points / 3.0, 0.0, 1.0)
# 3.0 pts is arbitrary. If real expectancy ranges [0, 10], most candidates score ~0.3
# Fix: ceiling ≈ 90th percentile of observed expectancy across passing candidates
```

Both constants should be set as `ScenarioProfile` fields in a future block (B8B-003
already has a TECHNICAL_SPEC note deferring this to Block 9 calibration).

---
### Key Architectural Constraints Introduced in Blocks 9D–9E

1. **`evaluate_window()` positional argument contract**: The function takes `oos_gate_enabled`
   as its 7th positional argument. The `pool.submit()` call in `wfo_engine.py` must
   pass it positionally — keyword arguments are not reliably serialised across
   `ProcessPoolExecutor` spawn boundaries on Windows.

2. **`_base_yaml_path` injection contract (B9B-003)**: `ga_engine.run_ga()` reads
   `config["_base_yaml_path"]` as a private orchestrator-injected key. This key is
   never present in `backtest_template.yaml`. The orchestrator shallow-copies the
   config before injection to prevent the key from leaking to callers.

3. **Stage 2 ruin threshold ownership**: `scenario.mc_prefilter_ruin_threshold` is
   the single source of truth. The raw config dict value is never read directly in
   `_run_stage_2_mc_prefilter()`.

4. **Stage 6 spike threshold ownership**: `scenario.verdict_sensitivity_spike_threshold`
   is the single source of truth. The former `config["sensitivity"]["spike_threshold"]`
   read path has been removed.
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

   # ARCHITECTURE Block 9F Delta
**Append to**: docs/backtesting/ARCHITECTURE.md
**Date**: 2026-03-06

---
## Block 9F — Integration Bug Fixes

### B9F-002: Stage 3 Graceful Skip (orchestrator.py)
`_run_stage_3_ga()` now queries the store for MC_PREFILTER_PASS candidates
before calling `run_ga()`. If none exist it logs a WARNING and returns early.
The checkpoint is still advanced to GA_COMPLETE by the caller so the pipeline
continues to Stages 4–7. Previously the function called `run_ga()` unconditionally
which caused `initialise_population()` to raise ValueError, crashing the pipeline.

### B9F-003: Stage 2 Live CandidateResult (orchestrator.py)
`_run_stage_2_mc_prefilter()` now calls `strategy_runner.evaluate()` directly
to obtain a live `CandidateResult` instead of calling `store.get_candidate_result()`.

**Root cause**: `CandidateResult.trades` and `.metrics` are live Python objects
from the strategy architecture — they are never persisted to SQLite.
`store.get_candidate_result()` reconstructs from the `evaluations` table with
`trades=None` and `metrics=None` hardcoded, so `CandidateResult.is_valid` is
always `False` on reconstructed objects, causing MC to fail for every candidate.

**Design consequence**: Stage 2 performs N additional strategy evaluations
(one per top-N RANDOM-pass candidate). This is architecturally correct and
consistent with Stage 4 (WFO) which also re-evaluates per window. The trade
history cannot be meaningfully persisted in column form — only aggregate metrics
are stored. `store.get_candidate_result()` remains in the codebase for potential
future use but must not be relied upon for MC input.

### B9F-004: Trade P&L Attribute (equity_simulator.py)
`extract_trade_returns()` now uses `trade.pnl_points` (the correct attribute
on the `Trade` dataclass from `src/strategies/contracts/trade_contracts.py`)
instead of `trade.pnl` (which does not exist). Open trades (`trade.exit is None`)
return `pnl_points = None` and are skipped — only closed trades contribute to
MC simulation. Fallback chain: `pnl_points` → `pnl` → `dict["pnl_points"]`
→ `dict["pnl"]`.

### B9F-005: WFO Date Range Scoping (strategy_runner.py)
`strategy_runner.evaluate()` now accepts optional `date_start` and `date_end`
parameters. When provided, `_write_temp_yaml()` injects them as overrides to
`data.date_range.start` / `data.date_range.end` in the temp candidate YAML.

**Format**: `date` objects are formatted as `"YYYY-MM-DD 00:00:00"` (start) and
`"YYYY-MM-DD 23:59:59"` (end) to match the strategy YAML's datetime string format.
`datetime` objects are formatted as-is.

**Impact**: Enables WFO window-scoped evaluation — each window evaluates the
strategy only over its date range. Without this fix, all WFO windows evaluated
over the full dataset date range, producing identical results across windows.

**Note**: `wfo_evaluator.py` was already passing `date_start`/`date_end` to
`evaluate()` correctly. The bug was in `strategy_runner.py` which silently ignored
the kwargs (TypeError in Python — unexpected keyword argument). The skill entry
H-01 ("FALSE POSITIVE — strategy_runner.evaluate() DOES accept date_start/date_end")
was incorrect and has been corrected.

---
## Trade Contract Reference (verified Block 9F)
```
TradeResult.trades          List[Trade]
Trade.entry                 TradeEntry
Trade.exit                  Optional[TradeExit]   ← None = open trade
Trade.pnl_points            Optional[float]       ← None if open
Trade.is_closed             bool
TradeExit.pnl_points        float
TradeExit.pnl_percent       float
TradeExit.exit_reason       ExitReason
```
Source: `src/strategies/contracts/trade_contracts.py`

# ARCHITECTURE_9G_DELTA.md — Block 9G Appendix
**Append to**: `docs/backtesting/ARCHITECTURE.md`
**Date**: 2026-03-06
**Block**: 9G

---

## §1 — New CandidateStore Write Method: `write_candidate_stub()`

### Motivation
GA offspring candidates (from crossover + mutation) are produced inside `ga_engine`
and submitted directly to the WFO window pool. They were never written to the
`candidates` table before `write_wfo_window_result()` was called, causing a FOREIGN
KEY constraint failure on the `wfo_window_results.candidate_id` column.

### Design
```
write_candidate_stub(candidate: CandidateParameterSet) → None
  └─ _write_candidate_stub()
       ├─ INSERT OR IGNORE INTO candidates (candidate_id, zone_name, ...)
       └─ INSERT OR IGNORE INTO candidate_parameters (candidate_id, param_name, value)
       # NO evaluations row — stub only
```

**INSERT OR IGNORE** makes this safe for all callers:
- Seed candidates already in DB: no-op
- GA offspring not yet in DB: inserted
- Repeated calls for the same candidate: no-op

### Contract
`write_candidate_stub()` must be called (and `store.flush()` called after) before
any FK-referencing write (`write_wfo_window_result`, `write_mc_result`, etc.).

This invariant is now enforced in:
- `ga_engine._evaluate_generation()` — before pool.submit()
- `orchestrator._run_stage_4_wfo()` — before `wfo_engine.run_wfo()`
- `orchestrator._run_stage_5_mc_deep()` — before `run_mc()` (via re-evaluation pattern)

---

## §2 — Stage 5 Re-Evaluation Pattern

Stage 5 (MC Deep) now follows the same re-evaluation pattern as Stage 2 (MC Pre-Filter).

**Root cause**: `store.get_candidate_result()` reconstructs from SQLite but returns
`trades=None / metrics=None` because these objects are never persisted (L-15).
`MCEngine.run_mc()` raises immediately if `CandidateResult.is_valid` is False.

**Pattern (now consistent across Stage 2 and Stage 5)**:
```
for candidate in top_candidates:
    result = strategy_runner.evaluate(candidate, ...)   # live evaluation
    mc_result = run_mc(candidate, result, mode, ...)    # run_mc never raises
    store.write_mc_result(mc_result, run_id)
```

If `evaluate()` returns an invalid result (e.g. evaluation failed), `run_mc()` is
still called — it returns `MCResult(error=..., ruin_probability=None)`, which the
verdict engine treats as NO_GO. This is the correct conservative treatment.

---

## §3 — Ranker Deduplication Invariant

**Problem**: `query_candidates()` JOINs `evaluations`. A candidate that has been
evaluated in multiple stages (e.g. both `RANDOM` and `MC_PREFILTER_PASS`) produces
multiple rows in the result set with the same `candidate_id` but different stage values.

**Fix**: `rank_by_wfo()` now deduplicates by `candidate_id` after ORDER BY, keeping
the first (highest-scoring) occurrence. This was already done in `rank_combined()`.

**Invariant**: Any ranker function that calls `query_candidates()` with an ORDER BY
must deduplicate by `candidate_id` before applying `top_n`. Violating this causes
duplicate entries in every downstream stage (MC Deep, Sensitivity, Stage 7 verdicts,
trading YAMLs).

---

## §4 — yaml_generator Parameter Map Architecture

### Problem (B9G-004)
The original `_STRATEGY_PARAM_KEY_MAP` used a flat two-tuple `(section, key)` and
pointed all search-space parameters to `("strategy", ...)` or `("parameters", ...)`.
Neither `strategy` nor `parameters` are top-level keys in `strategy_template.yaml`.
The template has: `asset`, `data`, `execution`, `filters`, `trade_management`, `output`.

### New Map Format
```python
_PARAM_MAP: dict[str, tuple[str, tuple[str, ...], str]] = {
    # param_name → (top_section, nested_path_tuple, leaf_key)
    "rsi_period": ("filters", ("technical_filters", "rsi_filter"), "length"),
    "atr_length": ("trade_management", ("risk",), "atr_length"),
    ...
}
```

`_set_nested(d, top, path, key, value)` navigates `d[top][path[0]][path[1]]...[key]`,
creating intermediate dicts as needed.

### Invariant
`yaml_generator._PARAM_MAP` and `strategy_runner._PARAM_KEY_MAP` must be updated
together whenever a parameter is added, renamed, or remapped. Both files contain
a comment: `# WARNING: Twin key map exists in [other file]. Both MUST be updated together.`

### Validation Backstop
`_structural_validate()` now checks `["filters", "trade_management"]` (real template
sections) and spot-checks `filters.technical_filters` and `trade_management.risk` are
dicts. It runs as a hard backstop regardless of whether `StrategyConfig.from_yaml()`
passes — because `StrategyConfig.from_yaml()` silently accepted an invalid config in
the run that exposed B9G-004.

---

## §5 — WFO Consistency Scorer: WFO_INSUFFICIENT_WINDOWS Behaviour

Candidate `7bf2f892d683` produced 0 valid window results across 5 WFO windows.
The system behaved correctly:

```
consistency_scorer  → WARN: No valid window results — returning zero consistency score
wfo_engine          → WARN: Candidate failed >50% of WFO windows — flagging WFO_INSUFFICIENT_WINDOWS
candidate_store     → WARN: Candidate flagged WFO_INSUFFICIENT_WINDOWS
```

The candidate received `wfo_consistency_score = 0.0` and was not selected for
Stages 5–7 (rank_by_wfo top-10 excluded it). This is the expected path for
parameter combinations that produce no tradeable signals in any window.

**These three WARNINGs together indicate correct system behaviour, not a bug.**
Document this pattern so future operators do not file spurious bug reports.
# ARCHITECTURE_9I_DELTA.md
**Block**: 9I
**Date**: 2026-03-07
**Append to**: docs/backtesting/ARCHITECTURE.md
---

## Changes This Block

### sampler.py — LHS cycling strata (B9I-001)
`_lhs_sample()` previously hard-capped `actual_n = min(n, min_universe_size)`,
truncating every run to 4 candidates. Cap removed. `actual_n = n` always.

Stratum loop branches per parameter:
- `n <= n_vals`: standard LHS, one pick per stratum window
- `n > n_vals`: `cycled_idx = stratum_idx % n_vals` — cycles through universe

**Invariant**: `_lhs_sample()` always returns exactly `n` candidates.

---

### query_run.py — Phantom column removal (B9I-002)
`actual_net_pnl` and `actual_total_trades` removed from `evaluations` queries.
Neither column exists in `FitnessResult` or `CandidateRecord`.

`FitnessResult` stores exactly six constraint actuals:
`actual_win_rate`, `actual_max_drawdown`, `actual_losing_streak`,
`actual_trades_per_week`, `actual_expectancy`, `actual_profit_factor`.

Net PnL is available only at Stage 4 via `wfo_window_results.net_pnl`.

Secondary bug fixed: `q_constraint_margins` print loop was printing min twice.

---

### consistency_scorer.py — B8B-012 calibrated for DAX points
Three module-level constants recalibrated. Prior values were for fractional
returns (0–1 range) and produced degenerate output for DAX point values.

| Constant | Before | After | Derivation |
|---|---|---|---|
| `_SIGMOID_SCALE` | 0.10 | 131.0 | stdev(net_pnl) × 0.5 = 261.98 × 0.5 |
| `_MAX_EXPECTED_VARIANCE` | 0.10 | 100_000.0 | stdev≈262 → var≈68k → conservative ceiling |
| `_MAX_EXPECTED_DRAWDOWN` | 0.50 | 1_000.0 | observed range 282–899 pts → conservative ceiling |

`abs()` added to worst_drawdown_raw before normalisation (raw values are negative pts).
`abs()` added to collapse flag comparison for same reason.

**Calibration source**: run 87712cab, `wfo_window_results`, n=1006, stdev=261.98.
**Recalibration trigger**: instrument change, data range change > 6 months.
**Recalibration procedure**: `python calibrate_sigmoid.py` (outputs/calibrate_sigmoid.py).

---

## Unit Contract — ScenarioProfile threshold fields (new documentation)

After B8B-012, all drawdown comparisons in consistency_scorer.py use raw pts.
The following ScenarioProfile fields must use **matching units**:

| Field | Units after B8B-012 | DAX example |
|---|---|---|
| `max_drawdown` (constraint) | fraction 0–1 | 0.15 |
| `wfo_collapse_drawdown_threshold` | **pts** (post B8B-012) | 400.0 |
| `_MAX_EXPECTED_DRAWDOWN` | pts | 1_000.0 |

`wfo_collapse_drawdown_threshold` was previously documented as fraction (0.40 default).
After B8B-012 it must be set in pts for DAX. Current YAML value 0.40 causes
universal collapse flag (COLLAPSE-UNIT open issue). Fix: 400.0.

**Rule**: whenever normalisation units change in consistency_scorer.py, audit
all ScenarioProfile threshold fields that feed into the same comparisons.

---

## No New Tables or Contract Fields
Schema unchanged. No new dataclass fields added.
# ARCHITECTURE_9J_DELTA.md
**Block**: 9J
**Date**: 2026-03-07
**Append to**: docs/backtesting/ARCHITECTURE.md
---

## Changes This Block

### contracts.py — ScenarioProfile.wfo_collapse_drawdown_threshold (COLLAPSE-UNIT fix)

**Before**: default `0.40`, validated `(0.0 < threshold <= 1.0)`.
Fraction units. Any pts value would raise at construction.

**After**: default `400.0`, validated `threshold > 0.0` only (no upper bound).
Units: raw instrument points. Any positive value is valid.

```python
# Before
wfo_collapse_drawdown_threshold: float = 0.40
# validator:
if not (0.0 < self.wfo_collapse_drawdown_threshold <= 1.0):
    raise ValueError(...)

# After
wfo_collapse_drawdown_threshold: float = 400.0  # pts — DAX default
# validator:
if self.wfo_collapse_drawdown_threshold <= 0.0:
    raise ValueError(...)
```

Docstring updated: removed `# conservative scenario should use 0.20`.
Added: `# Units: raw instrument points. DAX default 400.0 pts ≈ 4% of 10,000pt account.`
Added: `# See V2-RAR backlog for instrument-agnostic normalisation via Rolling Annual Range.`

---

### scenario.py — load_scenario() wires wfo_collapse_drawdown_threshold (COLLAPSE-UNIT fix)

Previously the field was never read from YAML, so ScenarioProfile always used the
dataclass default regardless of what was in backtest_1st_run.yaml.

**Added** (after `mc_prefilter_ruin_threshold` line):
```python
wfo_collapse_drawdown_threshold=float(s.get("wfo_collapse_drawdown_threshold", 400.0)),
```

Uses `.get()` with explicit default — field is optional in YAML. Existing scenario
definitions without this field will use 400.0 pts automatically.

---

## Unit Contract — All Instrument-Specific Constants

After COLLAPSE-UNIT fix, the following constants are all in **raw instrument points**
for DAX. They must be recalibrated per instrument. V2-RAR will replace them with
dimensionless Rolling Annual Range fractions.

| Constant / Field | Location | Value (DAX) | Unit |
|---|---|---|---|
| `_SIGMOID_SCALE` | consistency_scorer.py | 131.0 | pts |
| `_MAX_EXPECTED_VARIANCE` | consistency_scorer.py | 100_000.0 | pts² |
| `_MAX_EXPECTED_DRAWDOWN` | consistency_scorer.py | 1_000.0 | pts |
| `wfo_collapse_drawdown_threshold` | ScenarioProfile / YAML | 400.0 | pts |

**V2-RAR backlog**: normalise all four via instrument's Rolling Annual Range.
Result: dimensionless fractions, one calibration serves all instruments.

---

## No New Tables or Schema Changes
`contracts.py` field type unchanged (float). No DB schema changes.
Existing test fixtures passing `wfo_collapse_drawdown_threshold=0.40` remain valid
(0.40 > 0.0 passes the new validation). Any test asserting the old `<= 1.0`
upper-bound error message must be updated — expected to be zero such tests.
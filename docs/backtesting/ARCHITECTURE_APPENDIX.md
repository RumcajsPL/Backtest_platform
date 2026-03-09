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

# ARCHITECTURE APPENDIX — Block 9N
**Date**: 2026-03-08
**Covers**: Full-history calibration work, scenario.py constraint fix, broker integration project start, CTP strategic roadmap

---

## A1. Calibration Constants — Two-Track System

As of Block 9N the platform maintains **two distinct calibration tracks** that must never be mixed.

### 3-Month Production Track (V1 — do not change)
```
_SIGMOID_SCALE                   = 131.0
_MAX_EXPECTED_DRAWDOWN           = 1_000.0   # pts
_MAX_EXPECTED_VARIANCE           = 100_000.0
wfo_collapse_drawdown_threshold  = 400.0 pts (per WFO window — unchanged for full-history)
normalisation_expectancy_ref_pts = 3.0 pts
normalisation_freq_ref_trades_per_week = 20.0  (CAL-01: raise to 50.0 before V2)
```
These constants are **frozen** for `backtest_production_v1.yaml` and all short-window (≤3-month Stage 1) runs. Do not recalibrate unless Stage 1 data stdev shifts >30% from the calibration baseline of 261.98 pts.

### Full-History Track (38-month, 2023-2026 — PENDING)
```
_MAX_EXPECTED_DRAWDOWN           = 2_500.0   # CODE CHANGE REQUIRED before calibration run
_SIGMOID_SCALE                   = TBD        # requires calibration_v2 run result
```
**Rule**: `_MAX_EXPECTED_DRAWDOWN` is dataset-range-specific. 38 months of continuous evaluation produces drawdowns 2.5–3× larger than a 3-month window. The 1,000 pt cap was truncating real values (observed avg drawdown: 790 pts — already near cap on short windows). 2,500 pt cap is the correct value for full-history runs.

**Important**: `wfo_collapse_drawdown_threshold = 400 pts` is per-window and does NOT change for full-history runs. This is the correct granularity — WFO evaluates each window independently.

---

## A2. scenario.py — Constraint Loader Fix (B9N-001)

**Problem**: `scenario.py` uses hard `dict[]` access for all constraint fields. Removing a constraint from the YAML causes `KeyError` at Stage 0 — the pipeline fails before any strategy runs.

**Root cause found**: Line 61 in `scenario.py`:
```python
max_drawdown=float(ct["max_drawdown"]),  # HARD LOOKUP — fails if key absent
```

**Fix applied (must be committed before full-history run)**:
```python
max_drawdown=float(ct.get("max_drawdown", 1.0)),           # 1.0 = 100% = disabled
max_losing_streak=int(ct.get("max_losing_streak", 99999)), # 99999 = disabled
```

**Design principle**: default values must semantically disable the constraint, not set it to zero or a tight value. `max_drawdown=1.0` means "allow 100% drawdown" — equivalent to no gate. `max_losing_streak=99999` is unreachable in practice.

**Architectural debt registered as B9N-001 [P3]**: All constraint fields in `scenario.py` use hard `dict[]` access. The systematic fix (convert all to `ct.get()` with documented defaults) is required before V2. For now the two-field targeted fix unblocks full-history runs.

**Promotion backlog (V2)**: Promote `_MAX_EXPECTED_DRAWDOWN` and `_MAX_EXPECTED_VARIANCE` to scenario YAML fields — same pattern as `normalisation_expectancy_ref_pts`. This eliminates the need to change source code between full-history and 3-month runs. Tracked as part of V2-RAR work.

---

## A3. Full-History Run Design

### Stage 1 Date Range
`2023-01-02 → 2026-02-28` (38 months, 3 distinct DAX regimes)

### WFO Window Map (13 windows)
```
W01: 2023-01-02 → 2023-03-31  (2023 recovery trend)
W02: 2023-04-03 → 2023-06-30
W03: 2023-07-03 → 2023-09-29
W04: 2023-10-02 → 2023-12-29
W05: 2024-01-02 → 2024-03-28  ← KEY STRESS: choppy rate cycle
W06: 2024-04-01 → 2024-06-28
W07: 2024-07-01 → 2024-09-30  (ECB peak)
W08: 2024-10-01 → 2024-12-31
W09: 2025-01-02 → 2025-03-31  (2025 range-bound)
W10: 2025-04-01 → 2025-06-30
W11: 2025-07-01 → 2025-09-30
W12: 2025-10-01 → 2025-12-31  (cross-reference vs production_v1)
W13: 2026-01-02 → 2026-02-28  (partial ~2 months — REJECTED_INSUFFICIENT_TRADES expected)
```

### Constraints removed for full-history Stage 1
- `max_drawdown` — REMOVED (accumulates over 38 months; correct gate is WFO per-window)
- `max_losing_streak` raised `50 → 200` (observed avg 44, max 78 over 38 months)

These removals apply **only to full-history YAMLs**. Production_v1.yaml is unchanged.

### Runtime estimate
~10–14 hours. Stage 4 dominant: 13 windows × 30 candidates (390 WFO evaluations vs 150 in production runs).

---

## A4. Calibration Failure — Root Cause (Run d1ce2b1d, Now Diagnosed)

Run `d1ce2b1d` produced 0/200 Stage 1 passes. Root cause: `max_drawdown=0.15` was calibrated for 3-month evaluation. Over 38 continuous months, normalised drawdown (against `_MAX_EXPECTED_DRAWDOWN=1000`) averaged 0.79. 199/200 candidates failed the 0.15 gate.

**Lesson L-43**: Constraints calibrated for short evaluation windows become invalid for long continuous evaluations. `max_drawdown` and `max_losing_streak` accumulate over the full Stage 1 date range. The correct granularity for these constraints is the WFO window (Stage 4), not the full dataset. Always verify constraint calibration matches the Stage 1 evaluation date range before launching a run.

---

## A5. New Lessons — Block 9N

**L-42** *(finalised from 9M)*: A filter producing zero sensitivity delta across 6 consecutive runs is structurally inactive for the current instrument/timeframe. Zero signal = zero cost but wastes GA search dimensions. Remove from search space in the next major version rather than forcing activation.

**L-43**: Constraints calibrated for short evaluation windows (3 months) become invalid for long continuous evaluations (38 months). max_drawdown and max_losing_streak accumulate across the full date range in Stage 1. The correct granularity for these constraints is the WFO window (Stage 4), not the full dataset. Always verify constraint calibration matches the Stage 1 evaluation date range.

**L-44**: Before removing a YAML field, verify the loader uses `dict.get()` not `dict[]`. Hard key access causes Stage 0 failure — the pipeline does not run at all. Audit `scenario.py` for all `ct[]` accesses before V2 constraint redesign.

---

## A6. Open Issues — Full Tracker

| ID | Priority | Description | Target |
|----|----------|-------------|--------|
| RSI-SENS-2 | P2 | RSI zero delta across 6 runs. Remove from V2 search space. | V2 |
| CAL-01 | P3 | normalisation_freq_ref_trades_per_week=20.0 → raise to 50.0 | V2 |
| RR-CEILING-2 | P3 | Revert safe zone rr_target.max 8.5→7.0 | Next YAML |
| B9N-001 | P3 | scenario.py hard dict[] access for all constraints. Systematic fix needed. | V2 |
| V2-RAR | P1 (V2) | Dimensionless normalisation via Rolling Annual Range | V2 |
| DYN-WFO | P2 (V2) | Dynamic window generation from data_range + window_size | V2 |
| B8C-002/003 | P3 | report_generator.py cosmetic HTML issues | V2 |
| B9C-008 | P3 | Deferred | V2 |
| WinError 32 | P3 | Windows file lock issue on log rotation | V2 |

---

## A7. Broker Integration — Architecture Decisions (Block 9N)

### Project: broker_support (eToro API integration)
**Location**: `E:\Trading\Broker_support`
**Package**: `broker-support` v0.1.0
**Status**: Working connection confirmed. Three bugs identified and scoped for fix.

### Confirmed Working
- `EToroClient._make_request()` and `_get_headers()` — connection confirmed, auth working
- `EToroClient.test_connection()` via `/api/v1/watchlists` — returns 200
- `EToroClient.get_portfolio()` — returns `clientPortfolio` structure

### Confirmed API Facts (from official OpenAPI spec)
| Endpoint | Path | Status |
|----------|------|--------|
| Demo portfolio + open positions + PnL | `GET /api/v1/trading/info/demo/pnl` | ✅ Official |
| Real account trade history | `GET /api/v1/trading/info/trade/history?minDate=YYYY-MM-DD` | ✅ Official |
| Demo account trade history | **Not documented** — may not exist | ⚠️ Needs empirical test |
| Open demo order by amount | `POST /api/v1/trading/execution/demo/market-orders-by-amount` | ✅ Official |
| Close demo position | `POST /api/v1/trading/execution/demo/close-position` | ✅ Official |

### Critical finding: demo history endpoint unknown
The eToro API does not document a demo-equivalent of `/api/v1/trading/info/trade/history`. The snapshot-comparison approach in `PositionTracker` is therefore not a workaround — it may be the only method available for demo trade detection. **Empirical test required before any further architecture decisions**: call real-account history endpoint with `minDate` set to a past date and verify whether demo trades appear.

### Three Bugs Requiring Fix Before Any New Development
1. **Wrong portfolio endpoint**: `get_portfolio()` calls `/demo/portfolio` — correct path is `/demo/pnl`
2. **Orphaned function**: second `fetch_closed_trades` definition in `client.py` is a free function (not a class method) — causes silent import-time issues
3. **Wrong date param**: `fetch_closed_trades` uses `from`/`fromDate` — confirmed param name is `minDate`

### Trade Model Mismatch
`Trade` model uses `alias='id'` but eToro returns `positionId`. Fix: change alias to `positionId` and add optional fields from the actual API schema: `fees`, `leverage`, `sl_rate`, `tp_rate`.

### Architectural Principle: Demo/Real Symmetry
eToro demo and real account endpoints share identical schemas and auth. The only difference is the path prefix (`/demo/` vs `/`). The signal bridge built for demo paper trading requires zero code changes at go-live — only a configuration flag changes. All broker integration work is production code from day one.
# ARCHITECTURE_9O_DELTA.md — Block 9O Patch Record
**Date:** 2026-03-09  
**Block:** 9O — Full-History Calibration  
**Author:** Session transcript  

---

## CRITICAL READ: Patch Audit and Rollback Guidance

This document answers: *what was changed, why, is it confirmed necessary, and what to revert if needed.*

Every change in this session came from observing actual pipeline failures on the full-history (38-month) calibration run. **None of these issues existed in production 3-month runs** — they are all triggered exclusively by the extended data range and the calibration YAML's disabled stages (which expose code paths never hit before).

---

## Summary Table

| ID | File | Change | Status | Rollback? |
|----|------|--------|--------|-----------|
| B9O-001 | `data_loader.py` | Add sliced strategy cache keyed by date range | **CONFIRMED — KEEP** with caveat (see below) | Partial rollback available |
| B9O-002 | `run_cleaner.py` (new) + `run_backtester.py` | Pre-run environment cleaner | **KEEP** — independently useful | Safe to keep, no risk |
| B9O-003 | `mc_engine.py` | `config["mc_prefilter"]` → `config.get("mc_prefilter", {})` | **CONFIRMED — KEEP** | Simple revert to hard access |
| B9O-004 | `ga_engine.py` | `config["genetic"]` → `config.get("genetic", {})` | **CONFIRMED — KEEP** | Simple revert to hard access |
| B9O-005 | `orchestrator.py` | Add `stages:` toggle guards to `_execute_pipeline()` | **CONFIRMED — KEEP** | Remove guard blocks |
| YAML-001 | `backtest_calibration_fullhistory_v3.yaml` | `fitness_weights` sum fix | **CONFIRMED — KEEP** | Was a bug, must stay fixed |
| YAML-002 | `backtest_calibration_fullhistory_v3.yaml` | `max_workers: 6 → 2` | **CONFIRMED — KEEP** (calibration) | Production YAML also needs this |
| YAML-003 | `backtest_calibration_fullhistory_v3.yaml` | `min_expectancy: -1.0 → -2.0`, `min_win_rate: 0.12 → 0.11` | **CONFIRMED — KEEP** | Based on observed distributions |

---

## Detailed Patch Records

---

### B9O-001 — `data_loader.py` v3.3
**File:** `src/strategies/core/data_loader.py`  
**Deployed:** Yes  
**Origin:** Observed `malloc of size 8388608 failed` and `array shape (5, 22428607)` OOM errors in every WFO window evaluation.

**Root cause confirmed:**  
`load_data()` always called `_load_file_with_cache(..., apply_date_range=False)` for the strategy DataFrame — loading the full 38-month file (~22M rows, ~850MB) into memory on every evaluation. With `max_workers=6`, six workers competed for the same 850MB pickle simultaneously → ~5.1GB peak → `malloc` failures and WinError 32 file locks.

The cache key had no date-range component, so all 6 workers tried to read/write the same `~850MB` pkl file simultaneously.

**Fix:** Added `_load_sliced_strategy_cache()` / `_save_sliced_strategy_cache()` methods. The strategy DataFrame is now cached **after** slicing to the date range, under a `"sliced:v1"` + date_range key. Each WFO worker loads only its own ~20MB window slice.

**Confirmed necessary:** Yes — OOM was observed in runs `756a7829`, `9d4669a7`.

**IMPORTANT CAVEAT — Partial OOM still present:**  
After deploying B9O-001, OOM errors persisted in runs `9d4669a7` and `d9a81454` with `max_workers=6`.  

Investigation revealed the real architectural constraint: **`TradeSimulator` receives `df_full` (the full 22M-row dataset) by design** for LTF tick-accurate SL/TP execution. This is not in `data_loader.py` — it is in `src/strategies/orchestrator.py` and is correct behaviour. The DataLoader slice only helps for initial data loading; the trade simulation itself still requires the full dataset per worker.

**This means B9O-001 is still correct and helpful** (reduces initial load time, eliminates cache lock contention), but it does NOT fully solve the per-worker memory issue. The correct fix for that is YAML-002 (`max_workers: 2`).

**Rollback guidance:**  
B9O-001 can be reverted to v3.2 if needed — the only regression would be slightly slower initial cache loads and the return of cache file lock contention. The OOM would persist regardless with `max_workers > 2`. If reverting, ensure the cache is also cleared (`~/.wbws_data_cache/*.pkl`).

**Code change summary:**
```python
# ADDED: two new private methods in DataLoader class

def _get_sliced_cache_key(self, file_path: str, date_range: str) -> str:
    """Cache key for the sliced strategy DataFrame. Includes 'sliced:v1' version tag."""
    mtime = Path(file_path).stat().st_mtime
    return f"sliced:v1:{file_path}:{mtime}:{date_range}"

def _load_sliced_strategy_cache(self, file_path: str, date_range: str) -> Optional[pd.DataFrame]:
    """Load cached sliced DataFrame. Returns None on miss or error."""
    # ... pickle load under sliced cache key

def _save_sliced_strategy_cache(self, df: pd.DataFrame, file_path: str, date_range: str) -> None:
    """Save sliced DataFrame to cache. Silent on failure."""
    # ... pickle save under sliced cache key

# MODIFIED: load_data() — strategy file handling
# BEFORE:
df_full = self._load_file_with_cache(self.data_config.strategy_data, "strategy", apply_date_range=False)
df_strategy = df_full.loc[start:end].copy()

# AFTER:
sliced = self._load_sliced_strategy_cache(str_path, date_range_key)
if sliced is None:
    df_full = self._load_file_with_cache(str_path, "strategy", apply_date_range=False)
    sliced = df_full.loc[start:end].copy()
    del df_full  # release full DataFrame immediately
    self._save_sliced_strategy_cache(sliced, str_path, date_range_key)
df_strategy = sliced
```

---

### B9O-002 — Pre-run cleaner (new utility)
**Files:** `src/utils/run_cleaner.py` (new), `scripts/runners/run_backtester.py` (updated)  
**Deployed:** Yes  
**Origin:** NoneType YAML errors observed from temp YAMLs left by crashed workers from previous OOM runs. Cache file lock errors from workers competing for the same pkl.

**Root cause confirmed:**  
When a worker crashes mid-write (due to OOM), it leaves:
- Truncated/incomplete temp YAML files in `temp/backtesting/*.yaml`  
- Locked or partial pickle files in `~/.wbws_data_cache/*.pkl`

A subsequent run reading these files fails at parsing. The simplest fix is to clear both before every run.

**Fix:** `run_cleaner.py` provides `clean_environment()` called automatically by the runner before every pipeline start. Database is never cleared without explicit `--clean-db` flag.

**Confirmed necessary:** Yes — independently useful beyond this session. Prevents an entire class of "stale temp file" failures.

**Rollback guidance:** Safe to keep indefinitely. No functional risk. If reverting (not recommended): remove the `clean_environment()` call from `run_backtester.py` main function.

**Usage:**
```bash
python scripts/runners/run_backtester.py --config <yaml>            # normal — auto-clean
python scripts/runners/run_backtester.py --config <yaml> --no-clean # resume checkpoint
python scripts/runners/run_backtester.py --config <yaml> --clean-db # fresh start
python src/utils/run_cleaner.py                                       # manual clean only
```

---

### B9O-003 — `mc_engine.py` KeyError `'mc_prefilter'`
**File:** `src/backtesting/monte_carlo/mc_engine.py`  
**Deployed:** Yes  
**Origin:** Error observed in run `9d4669a7`:
```
KeyError: 'mc_prefilter'
  File "mc_engine.py", line 105, in _run_mc_internal
    mc_cfg = config["mc_prefilter"]
```

**Root cause confirmed:**  
The calibration YAML has `mc_prefilter: false` in the `stages:` block **and no top-level `mc_prefilter:` config block** (the block was intentionally omitted to shorten the YAML since the stage is disabled). But the orchestrator was still calling Stage 2 MC unconditionally (B9O-005 root cause), and `mc_engine.py` used hard dict access `config["mc_prefilter"]` which raises `KeyError` when the block is absent.

In the production YAML, the `mc_prefilter:` block is always present and the hard access never fails. This code path was only exposed by the calibration YAML's disabled-stage configuration.

**Fix:** Changed to `config.get("mc_prefilter", {})` merged over `_MC_PREFILTER_DEFAULTS` dict. Same pattern applied to the deep MC path and `_get_profile_name()`. Identical pattern to B9N-001 (scenario.py `ct.get()`).

**Confirmed necessary:** Yes — crash observed. Same pattern as B9N-001 lesson.

**Rollback guidance:** Revert to `config["mc_prefilter"]` hard access. Safe to revert ONLY if the `mc_prefilter:` config block is guaranteed present in all YAMLs (i.e., if you always add the block even when the stage is disabled). Not recommended — defensive `.get()` is strictly safer.

**Code change summary:**
```python
# BEFORE (line 105):
mc_cfg = config["mc_prefilter"]

# AFTER:
_MC_PREFILTER_DEFAULTS = {"iterations": 300, "perturbation_profile": "default", "ruin_threshold": 0.25}
_MC_DEEP_DEFAULTS      = {"iterations": 3000, "perturbation_profile": "default", "ruin_threshold": 0.20}

if mode == MCMode.PRE_FILTER:
    mc_cfg = {**_MC_PREFILTER_DEFAULTS, **config.get("mc_prefilter", {})}
else:
    deep_block = config.get("monte_carlo", {}).get("deep", {})
    mc_cfg = {**_MC_DEEP_DEFAULTS, **deep_block}

# ALSO fixed in _get_profile_name():
# BEFORE: config["mc_prefilter"]["perturbation_profile"]
# AFTER:  config.get("mc_prefilter", {}).get("perturbation_profile", _MC_PREFILTER_DEFAULTS["perturbation_profile"])
```

---

### B9O-004 — `ga_engine.py` KeyError `'genetic'`
**File:** `src/backtesting/ga/ga_engine.py`  
**Deployed:** Yes  
**Origin:** Error observed in run `d9a81454` at t=3310s:
```
KeyError: 'genetic'
  File "ga_engine.py", line 85, in run_ga
    ga_config: dict = config["genetic"]
```

**Root cause confirmed:**  
Same pattern as B9O-003. The calibration YAML has `genetic_algorithm: false` in `stages:` and no top-level `genetic:` config block. Before B9O-005 was applied, the orchestrator called Stage 3 GA unconditionally regardless of the stage toggle. `ga_engine.run_ga()` used `config["genetic"]` hard access → `KeyError`.

**Fix:** Changed all config key accesses in `ga_engine.py` to use `.get()` with defaults. Added `_GA_DEFAULTS` dict at module level documenting all default values. Also hardened `config["run"]["max_workers"]` and `config["random_search"]["min_significant_trades"]` to `.get()` for consistency (those keys are required by Stage 0 but defensive access is better practice).

**Confirmed necessary:** Yes — crash observed. Both B9O-004 (defensive access) and B9O-005 (stage toggle guard) are needed; B9O-004 alone does not prevent calling GA with no config, but it makes GA safe to call with a minimal/missing config block.

**Rollback guidance:** Revert to `config["genetic"]` hard access ONLY if the `genetic:` config block is guaranteed present in all YAMLs. Not recommended — same reasoning as B9O-003.

**Code change summary:**
```python
# ADDED at module level:
_GA_DEFAULTS = {
    "population_size": 60,
    "generations": 30,
    "elite_fraction": 0.10,
    "mutation_rate": 0.15,
    "crossover_rate": 0.70,
    "tournament_size": 5,
    "stagnation_generations": 10,
    "diversity_penalty_weight": 0.10,
    "diversity_distance_threshold": 0.15,
}

# BEFORE (line 85-95):
ga_config: dict = config["genetic"]
population_size: int = ga_config["population_size"]
generations: int = ga_config["generations"]
# ... (all hard accesses)
max_workers: int = config["run"]["max_workers"]
min_significant_trades: int = config["random_search"]["min_significant_trades"]

# AFTER:
ga_config: dict = config.get("genetic", {})
population_size: int = ga_config.get("population_size", _GA_DEFAULTS["population_size"])
generations: int = ga_config.get("generations", _GA_DEFAULTS["generations"])
# ... (all .get() with defaults)
max_workers: int = config.get("run", {}).get("max_workers", 6)
min_significant_trades: int = config.get("random_search", {}).get("min_significant_trades", 30)
```

---

### B9O-005 — `orchestrator.py` Stage toggle guards
**File:** `src/backtesting/orchestrator.py`  
**Deployed:** Yes  
**Origin:** Root cause of both B9O-003 and B9O-004. The `stages:` block in the YAML was being parsed but never consulted — `_execute_pipeline()` called every stage unconditionally.

**Root cause confirmed:**  
The orchestrator read `config["stages"]` nowhere in `_execute_pipeline()`. Every stage ran regardless of the toggle value. This meant:
- Stage 2 MC ran even with `mc_prefilter: false` → B9O-003 crash
- Stage 3 GA ran even with `genetic_algorithm: false` → B9O-004 crash
- The `stages:` feature in the YAML was effectively non-functional since it was first written

This entire bug cluster exists because the full-history calibration YAML was the first YAML to actually disable stages. The 3-month production YAML has all stages enabled — the toggle code path was never exercised.

**Fix:** Added stage toggle reads at the top of `_execute_pipeline()`, then added `if stage_X:` guards around each stage call. All toggles default to `True` when the `stages:` block is absent (backward compat with production YAML).

**Additional fix:** Added `_promote_random_to_mc_pass()` helper. When `mc_prefilter: false`, RANDOM-pass candidates must be promoted to `MC_PREFILTER_PASS` stage so that Stage 3 GA's B9F-002 guard (which queries `MC_PREFILTER_PASS`) doesn't fire and skip GA when it should run.

**Confirmed necessary:** Yes — the stage toggle system was functionally broken. Required for any future YAML that disables stages.

**Rollback guidance:** Remove the `stages_cfg = config.get("stages", {})` block and the `if stage_X:` guards, restoring all stages to unconditional execution. Do NOT revert unless you also remove the `_promote_random_to_mc_pass()` helper (it would be called when it shouldn't be).

**Code change summary (logical diff):**
```python
# BEFORE _execute_pipeline():
# ... Stage 2:
if store.get_checkpoint(run_id).value < Checkpoint.MC_PREFILTER_COMPLETE.value:
    _run_stage_2_mc_prefilter(config, store, run_metadata)   # always called
    store.set_checkpoint(run_id, Checkpoint.MC_PREFILTER_COMPLETE)

# Stage 3:
if store.get_checkpoint(run_id).value < Checkpoint.GA_COMPLETE.value:
    _run_stage_3_ga(config, store, run_metadata)              # always called
    store.set_checkpoint(run_id, Checkpoint.GA_COMPLETE)

# AFTER _execute_pipeline():
stages_cfg = config.get("stages", {})
stage_mc_prefilter    = stages_cfg.get("mc_prefilter",      True)
stage_ga              = stages_cfg.get("genetic_algorithm", True)
# ... (all toggles read once)

# Stage 2:
if store.get_checkpoint(run_id).value < Checkpoint.MC_PREFILTER_COMPLETE.value:
    if stage_mc_prefilter:
        _run_stage_2_mc_prefilter(config, store, run_metadata)
    else:
        logger.info("Stage 2 (MC Pre-Filter) disabled in config — skipping")
        _promote_random_to_mc_pass(config, store, run_metadata)  # NEW helper
    store.set_checkpoint(run_id, Checkpoint.MC_PREFILTER_COMPLETE)

# Stage 3:
if store.get_checkpoint(run_id).value < Checkpoint.GA_COMPLETE.value:
    if stage_ga:
        _run_stage_3_ga(config, store, run_metadata)
    else:
        logger.info("Stage 3 (Genetic Algorithm) disabled in config — skipping")
    store.set_checkpoint(run_id, Checkpoint.GA_COMPLETE)
```

---

### YAML-001 — Calibration YAML `fitness_weights` bug fix
**File:** `configs/backtesting/backtest_calibration_fullhistory_v3.yaml`  
**Deployed:** Yes  
**Origin:** Observed in run `756a7829` — `fitness_weights` block summed to 1.50 instead of 1.00.

**Root cause confirmed:**  
The calibration YAML was copied from an earlier version that had `expectancy: 0.50, win_rate: 0.25`. The production YAML's weights were not matched. Weights summing to 1.50 distort all fitness comparisons.

**Fix:** Updated weights to match production YAML exactly:
```yaml
# BEFORE (sum = 1.50 — bug):
fitness_weights:
  expectancy: 0.50
  win_rate: 0.25
  ...

# AFTER (sum = 1.00 — correct):
fitness_weights:
  net_pnl: 0.20
  expectancy: 0.25
  max_drawdown: 0.20
  win_rate: 0.15
  trade_frequency: 0.10
  profit_factor: 0.10
```

**Confirmed necessary:** Yes — was a direct bug. Must remain fixed.

---

### YAML-002 — Calibration YAML `max_workers: 6 → 2`
**File:** `configs/backtesting/backtest_calibration_fullhistory_v3.yaml`  
**Deployed:** Yes  
**Origin:** OOM persisted in `9d4669a7` even after B9O-001. Investigation revealed the architectural constraint: `TradeSimulator` holds `df_full` (~850MB for 38 months) per worker by design for LTF tick-accurate SL/TP execution.

**Root cause confirmed:**  
`TradeSimulator` receives `df_full` unconditionally. 6 workers × 850MB = ~5.1GB peak. This is architectural and correct — `TradeSimulator` needs the full LTF data for bar-accurate SL/TP execution. It cannot be fixed in `data_loader.py`.

Standalone strategy run confirms: `evaluate()` takes 45s on 38 months vs 5s on 3 months. This is expected and correct — the runtime is proportional to the data range.

**Fix:** `max_workers: 2` → ~1.7GB peak. Runtime cost for calibration: ~9 candidates × 7 windows × 45s ÷ 2 workers ≈ 24 minutes for Stage 4. Acceptable.

**Confirmed necessary:** Yes — OOM observed repeatedly.

**CRITICAL: Also apply to `backtest_V1_01.yaml` before production run.**  
The production YAML still has `max_workers: 6`. It will hit the same OOM on the full 13-window run. Update before the overnight run.

---

### YAML-003 — Calibration YAML constraint loosening
**File:** `configs/backtesting/backtest_calibration_fullhistory_v3.yaml`  
**Deployed:** Yes  
**Origin:** Run `756a7829` produced 1/60 Stage 1 passers. Distributions confirmed constraints were too tight:
- `min_expectancy: -1.0` was cutting at ~75th percentile (avg = -1.69)
- `min_win_rate: 0.12` was cutting the bottom 40% instead of just extremes

**Fix:**
```yaml
# BEFORE:
min_win_rate:   0.12   # too tight — cutting bottom ~35-40%
min_expectancy: -1.0   # too tight — cutting ~75% of candidates

# AFTER:
min_win_rate:   0.11   # loosened — removes only bottom ~5% (extreme outliers)
min_expectancy: -2.0   # loosened — targets top ~60-65%; avg=-1.69
```

**Confirmed necessary:** Yes — based on observed Stage 1 distributions from 3 independent runs. The full-history (38-month) distribution is fundamentally different from the 3-month production distribution. These constraints are calibration-track only and must also be applied to `backtest_V1_01.yaml`.

---

## What NOT to Roll Back

The following changes from this session must stay:

1. **B9O-003, B9O-004, B9O-005** — The `config["key"]` pattern is a systemic bug in any code path where optional YAML config blocks can be absent. These fixes harden the engine against future YAML configurations that disable stages. Do not revert to hard dict access.

2. **YAML-001** — `fitness_weights` summing to 1.50 was a straightforward bug. Do not revert.

3. **B9O-002** — The pre-run cleaner prevents an entire class of stale-temp-file failures. No downside to keeping.

---

## Data Loader — Rollback Clarification

The `data_loader.py` B9O-001 patch is **correctly applied and must stay**. However, it is important to understand its scope:

- ✅ **B9O-001 solves:** cache file lock contention, initial load memory peak, pickle collision between workers  
- ❌ **B9O-001 does NOT solve:** `TradeSimulator` df_full per-worker allocation (YAML-002 solves this)

These are two separate memory issues. Both fixes are needed. Neither replaces the other.

If someone asks "did data_loader.py solve the OOM?" — the answer is: partially. It reduced the initial load footprint. The per-worker trade simulation footprint required reducing `max_workers`. Both must stay.

---

## New Architecture Rules (add to SKILL.md)

```python
# config["key"] hard lookup fails for any optional stage config block when that stage is
# disabled and the YAML omits the block. Pattern: config.get("key", {}) + defaults dict.
# Confirmed affected: mc_prefilter, genetic, walk_forward, sensitivity, monte_carlo.deep
# Audit status: mc_prefilter (fixed B9O-003), genetic (fixed B9O-004), others TBD

# TradeSimulator holds df_full (~850MB for 38-month dataset) per worker for LTF tick
# resolution. max_workers must be ≤ floor(available_RAM_GB / 0.85) for full-history runs.
# 3-month runs unaffected (~20MB per worker). This is architectural — not fixable in DataLoader.

# stages: toggle block in YAML was non-functional until B9O-005. Now correctly guarded.
# All stages default to True when stages: block absent (backward compat).
# When mc_prefilter disabled: _promote_random_to_mc_pass() must run to seed Stage 3 GA.

# Pre-run cache clear is mandatory after data_loader.py upgrades. run_cleaner.py automates this.
```

## New Lessons

```
L-45: config["key"] hard lookup fails for any optional stage config block when that stage is
      disabled and the YAML omits the block. Pattern: config.get("key", {}) + defaults dict.
      Affects: mc_prefilter, genetic, walk_forward, sensitivity, monte_carlo.deep — audit all.

L-46: TradeSimulator holds df_full (~850MB for 38-month dataset) per worker for LTF tick
      resolution. max_workers must be ≤ floor(available_RAM_GB / 0.85) for full-history runs.
      3-month runs unaffected (~20MB per worker). This is architectural — not fixable in DataLoader.

L-47: Pre-run cache clear is mandatory after data_loader.py upgrades. run_cleaner.py automates this.

L-48: The stages: toggle block in YAML was parsed but never enforced until B9O-005. Any code
      that relies on stages being conditionally disabled must verify the orchestrator guard exists.
```
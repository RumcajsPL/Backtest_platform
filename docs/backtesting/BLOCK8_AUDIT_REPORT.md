# BLOCK8_AUDIT_REPORT.md — Block 8: E2E Code Analysis and Hardening
**Started**: 2026-03-04
**Status**: Sub-block 8A complete. 8B and 8C pending.
**Total findings**: 9 (8A) | P1: 0 | P2: 3 | P3: 3 | P4: 3
---
## Finding Registry
---
### B8-001
**Category**: CONTRACT_GAP
**Severity**: P2 — Fix this block
**File**: `src/backtesting/candidate_store.py` + `src/backtesting/contracts.py`
**Finding**:
`wfo_consistency_scores` table is missing the `median_oos_delta` column introduced in Block 7D
(M-01 fix). The M-01 fix correctly computes `median_oos_delta` in `consistency_scorer.py` and
populates `WFOConsistencyScore.median_oos_delta`, but:
1. `_write_wfo_consistency_score()` does not persist `median_oos_delta` to the DB — the
   INSERT statement has no `median_oos_delta` column and no corresponding value.
2. `get_wfo_consistency_score()` reconstructs `WFOConsistencyScore` without the field,
   so it always returns `median_oos_delta=None` regardless of what was computed.
3. Stage 7 calls `store.get_wfo_consistency_score()` before passing the score to
   `compute_verdict()`. The field `VerdictResult.median_oos_delta` therefore always stores
   `None` in the `verdicts` table, even when the OOS gate is enabled and windows carry
   valid `oos_delta` values.
The M-01 fix is **half-complete**: the computation is correct; the persistence is missing.
**Impact**: `median_oos_delta` in reports and verdicts is always `None`. The observable
field is vestigially correct — the OOS gate uses `oos_gate_triggered` (a boolean flag),
not the delta directly — but the diagnostic value of `median_oos_delta` is entirely lost.
**Decision**: FIX
**Fix scope**:
- Add `median_oos_delta REAL` column to `wfo_consistency_scores` CREATE TABLE in `_SCHEMA_SQL`.
- Extend `_write_wfo_consistency_score()` INSERT to include `score.median_oos_delta`.
- Extend `get_wfo_consistency_score()` SELECT + reconstruction to read and populate the field.
**Test added**: Yes — `test_block8a_foundation.py::test_wfo_median_oos_delta_persisted`
---
### B8-002
**Category**: CONTRACT_GAP
**Severity**: P3 — Tracked
**File**: `src/backtesting/candidate_store.py`, `src/backtesting/contracts.py`
**Finding**:
`CandidateRecord.wfo_median_oos_delta` (added 7D) is never populated when records are
read back from the store via `query_candidates()`. The `_row_to_candidate_record()` method's
SELECT statement does not join or retrieve `median_oos_delta` from `wfo_consistency_scores`,
and the field is hardcoded to `wfo_median_oos_delta=None` in the reconstructed record.
**Impact**: Any consumer of `query_candidates()` that reads `record.wfo_median_oos_delta`
will always see `None`. Currently no downstream consumer uses this field from
`CandidateRecord` (report uses `VerdictResult.median_oos_delta`), so runtime impact is nil.
The field is structurally dead weight in `query_candidates()` output.
**Decision**: FIX alongside B8-001 — extend the `query_candidates()` SELECT to join
`wfo_consistency_scores.median_oos_delta` once the column exists (B8-001 fix).
**Test added**: Yes — covered by `test_block8a_foundation.py::test_wfo_median_oos_delta_persisted`
---
### B8-003
**Category**: HARDCODING / P7 VIOLATION
**Severity**: P3 — Tracked
**File**: `configs/backtesting/backtest_template.yaml` (unverified)
**Finding**:
Four new `ScenarioProfile` fields were added in Block 7B (M-02) and Block 7D (M-03):
- `wfo_collapse_drawdown_threshold` (default 0.40)
- `normalisation_drawdown_ref_points` (default 10,000.0)
- `normalisation_pnl_ref_points` (default 5,000.0)
- `normalisation_freq_ref_trades_per_week` (default 20.0)
These fields have Python-side defaults in `contracts.py` and validation in `__post_init__`.
It is unverified whether `backtest_template.yaml` documents them with their defaults
and calibration guidance. If absent, operators have no visibility into these values
and cannot tune them without reading Python source.
**Decision**: DOCUMENT — verify `backtest_template.yaml` includes all four fields
under each scenario block with inline calibration comments. Add to OPERATOR_RUNBOOK
if verified absent.
**Test added**: No — config file not in scope for this session. Flag for 8B/8C.
---
### B8-004
**Category**: CODE_HYGIENE
**Severity**: P4 — Noted
**File**: `src/backtesting/candidate_store.py`, line ~`_drain_queue`
**Finding**:
`_drain_queue` dispatches writes via `getattr(self, method_name)(payload)`. The method
names are string literals scattered across all public `write_*` methods. There is no
compile-time or startup-time check that every string corresponds to an existing private
method. A typo or a new `write_*` method added without a corresponding `_write_*` handler
would silently fail: `getattr` would raise `AttributeError`, the writer thread would catch
it, log it, and discard the write — exactly the L-05 failure mode that caused H-02.
**Decision**: DOCUMENT in OPERATOR_RUNBOOK. Consider adding a `_WRITE_DISPATCH` class-level
dict (string → method) with an explicit `assert` in `__init__` that all dispatch keys
exist as methods. Deferred to B9 as no current missing handler exists.
**Test added**: No
---
### B8-005
**Category**: PRINCIPLE_VIOLATION (P6 — Fail Fast)
**Severity**: P2 — Fix this block
**File**: `src/backtesting/orchestrator.py` (`_run_stage_0_init`)
**Finding**:
`min_significant_trades` is read from config and passed to `strategy_runner.evaluate()` at
run time, but is never validated in Stage 0. If set to `0` in the config YAML, the
significance guard in `strategy_runner.evaluate()` becomes `total_trades < 0` — always
`False` — so all candidates pass regardless of trade count, including zero-trade results
where `expectancy`, `win_rate`, and `profit_factor` are undefined (likely `0.0` or `NaN`
from the strategy). These malformed candidates would silently enter the pipeline.
The same issue applies to `spike_threshold` (used in Stage 6) — a value of `0.0` would
flag every parameter as a spike, and a value `> 1.0` would never flag any.
**Decision**: FIX — add validation in `_run_stage_0_init`:
```python
if min_significant_trades < 1:
    raise ValueError(
        f"min_significant_trades must be >= 1; got {min_significant_trades}"
    )
spike_threshold = config.get("sensitivity", {}).get("spike_threshold", 0.15)
if not (0.0 < spike_threshold < 1.0):
    raise ValueError(
        f"sensitivity.spike_threshold must be in (0, 1); got {spike_threshold}"
    )
```
**Test added**: Yes — `test_block8a_foundation.py::test_stage0_rejects_zero_min_trades`,
`test_block8a_foundation.py::test_stage0_rejects_invalid_spike_threshold`
---
### B8-006
**Category**: P7 VIOLATION (Single Source of Truth)
**Severity**: P3 — Tracked
**File**: `src/backtesting/strategy_runner.py`, `_PARAM_KEY_MAP`
**Finding**:
`_PARAM_KEY_MAP` maps 38 backtester parameter names to StrategyConfig dot-path keys.
These dot-paths are hardcoded string literals (e.g. `"filters.technical_filters.rsi_filter.length"`).
If the strategy YAML schema changes (a key is renamed or moved), the mapping becomes stale.
Stage 0 validates that all enabled parameter names exist in `_PARAM_KEY_MAP` (M-05 fix), but
does not validate that the dot-paths themselves are valid keys in the strategy YAML schema.
A stale dot-path causes `_deep_set()` to silently create a new nested key in the strategy config
rather than overwriting the intended key — the strategy runs with its default value for that
parameter, not the candidate's parameter, with no error or warning.
**Decision**: DOCUMENT — add a comment block above `_PARAM_KEY_MAP` explaining this risk and
the mitigation (strategy schema version should be checked when `_PARAM_KEY_MAP` is updated).
A startup self-test that round-trips one known key against `StrategyConfig.from_yaml()` would
catch this, but is deferred to B9 (requires a valid strategy YAML in the test environment).
**Test added**: No — deferred, requires strategy package in test environment.
---
### B8-007
**Category**: P4 VIOLATION (Explicit Over Implicit)
**Severity**: P4 — Noted
**File**: `src/backtesting/orchestrator.py`, Stages 1–4 stubs
**Finding**:
Stages 1–4 are implemented as stubs that log "not yet implemented" and return immediately.
The orchestrator then calls `store.set_checkpoint()` to mark them complete. On a full
pipeline run, Stages 5–7 will operate on whatever data exists in the DB (from a prior run
or from manual DB population) because the stubs produce no output, not because the pipeline
correctly determined that Stage 1–4 work was already done.
This is a known temporary state, but it is not documented at the call site — a new
developer reading `_execute_pipeline` would not know that `_run_stage_1_random_search`
does nothing and that the Stage 5 input may be from a prior run.
**Decision**: DOCUMENT — add a comment block in `_execute_pipeline` above the stub calls:
```python
# NOTE: Stages 1–4 are stubs pending Phase 4 implementation.
# In the current state, pipeline Stages 5–7 operate on data from a prior
# full run loaded into the DB manually. See OPERATOR_RUNBOOK §3.
```
**Test added**: No
---
### B8-008
**Category**: CODE_HYGIENE
**Severity**: P4 — Noted
**File**: `src/backtesting/orchestrator.py`, `_execute_pipeline`
**Finding**:
Timing instrumentation (`perf_counter`) covers only Stages 5, 6, and 7. When Stages 1–4
are implemented, they will lack timing, making the TIMING SUMMARY log line incomplete and
the budget calculation inaccurate. The total elapsed time logged currently measures only
the last three stages.
**Decision**: DOCUMENT — extend timing to all stages when Stages 1–4 are implemented.
Flag in OPERATOR_RUNBOOK that current timing summary excludes Stages 0–4.
**Test added**: No
---
### B8-009
**Category**: P2 VIOLATION (Contracts Are the Interface)
**Severity**: P3 — Tracked
**File**: `src/backtesting/orchestrator.py`, `_resume_or_start`
**Finding**:
`_resume_or_start` opens a raw `sqlite3.connect()` connection directly to the DB file to
query the `runs` table. This bypasses `CandidateStore` — the designated single access point
for all DB operations. Two connections to the same WAL database are safe for concurrent reads,
but the pattern violates the architectural principle that orchestrator should only access
SQLite through the store abstraction.
The raw connection is used for two SELECT queries (find resumable run, find conflicting run)
and is properly closed in a `finally` block. Risk is low, but the pattern is wrong: any
schema change to the `runs` table must be updated in two places (`_SCHEMA_SQL` in store and
the raw SQL strings in `_resume_or_start`).
**Decision**: DOCUMENT — add `CandidateStore.find_resumable_run(config_hash: str) -> Optional[str]`
and `CandidateStore.find_conflicting_run(complete_checkpoint: Checkpoint) -> Optional[tuple]`
read methods to encapsulate these queries. Deferred to B9 as the current implementation
is functionally correct and the risk is low (read-only, properly closed).
**Test added**: No — deferred.
---
## Summary Table
| ID | Category | Severity | File | Decision | Test |
|---|---|---|---|---|---|
| B8-001 | CONTRACT_GAP | P2 | candidate_store.py | FIX | Yes |
| B8-002 | CONTRACT_GAP | P3 | candidate_store.py | FIX with B8-001 | Yes |
| B8-003 | HARDCODING | P3 | backtest_template.yaml | DOCUMENT | No |
| B8-004 | CODE_HYGIENE | P4 | candidate_store.py | DOCUMENT | No |
| B8-005 | PRINCIPLE_VIOLATION | P2 | orchestrator.py | FIX | Yes |
| B8-006 | P7 VIOLATION | P3 | strategy_runner.py | DOCUMENT | No |
| B8-007 | P4 VIOLATION | P4 | orchestrator.py | DOCUMENT | No |
| B8-008 | CODE_HYGIENE | P4 | orchestrator.py | DOCUMENT | No |
| B8-009 | P2 VIOLATION | P3 | orchestrator.py | DOCUMENT (defer B9) | No |
**P1 findings**: 0
**P2 findings**: 2 (B8-001, B8-005) — both fixed in this sub-block
**P3 findings**: 4 (B8-002, B8-003, B8-006, B8-009) — documented or fixed with P2
**P4 findings**: 3 (B8-004, B8-007, B8-008) — noted for operator documentation
---
## 8B and 8C Sections (pending)
Sub-block 8B findings will be appended after uploading:
`fitness.py`, `wfo/wfo_engine.py`, `wfo/consistency_scorer.py`,
`monte_carlo/mc_engine.py`, `monte_carlo/mc_metrics.py`
Sub-block 8C findings will be appended after uploading:
`ga/ga_engine.py`, `ga/mutation.py`, `evaluation/sensitivity.py`,
`evaluation/verdict.py`, `report_generator.py`, `yaml_generator.py`
---
## Sub-Block 8B — Evaluation Engines
*Files analysed*: `fitness.py`, `wfo/wfo_evaluator.py`, `wfo/wfo_engine.py`,
`wfo/consistency_scorer.py`, `monte_carlo/mc_engine.py`, `monte_carlo/mc_metrics.py`
---
### B8B-001
**Category**: PRINCIPLE_VIOLATION (P6 — Fail Fast)
**Severity**: P2 — Fix this block
**File**: `src/backtesting/fitness.py`, `evaluate_fitness()` constraint loop
**Finding**:
NaN metric values silently pass all constraint checks. The constraint guard is:
```python
if actual is None or comparator(actual, threshold):
```
Python's IEEE 754 NaN comparison semantics mean `NaN > x` and `NaN < x` are both `False`
for any `x`. A NaN `actual` value therefore makes `comparator(actual, threshold)` return
`False`, and since `actual is not None`, the guard does not trigger — the constraint passes.
Concrete path: if the strategy runner returns `MetricsReport.win_rate = float('nan')`,
then `_normalise_win_rate(nan)` → `nan / 100.0` → `nan`. The `op.lt(nan, min_win_rate)`
comparison returns `False` → the win_rate constraint passes. All subsequent constraints
face the same issue. The candidate reaches `_compute_weighted_score` with NaN inputs.
In `_compute_weighted_score`, NaN propagates through arithmetic. `_clamp` uses Python's
built-in `max`/`min`, whose behaviour with NaN is implementation-dependent (CPython
returns the non-NaN argument when NaN is the second operand, but this is not guaranteed
and is not the same as checking for NaN explicitly). The resulting `fitness_score` may
be `NaN` or an incorrect numeric value.
`FitnessResult.__post_init__` checks `not (0.0 <= fitness_score <= 1.0)`. Since
`0.0 <= NaN` is `False`, the chained comparison short-circuits to `False`, so `not False`
= `True` → `ValueError` is raised. This is the last-resort catch, but it surfaces as
an unhandled exception in `evaluate_fitness`, which means the caller sees a crash rather
than a clean `passed_constraints=False` result.
**Decision**: FIX — add an explicit NaN guard in `evaluate_fitness`, before the constraint
loop, to short-circuit with a clean `EVALUATION_ERROR` rejection:
```python
import math
# After extracting actuals, before the constraint loop:
raw_actuals = [actual_win_rate, actual_max_drawdown, actual_trades_per_week,
               actual_expectancy, actual_profit_factor]
if any(isinstance(v, float) and math.isnan(v) for v in raw_actuals if v is not None):
    return FitnessResult(
        candidate_id=result.candidate_id,
        scenario_name=scenario.name,
        fitness_score=None,
        passed_constraints=False,
        rejection_reason=RejectionReason.EVALUATION_ERROR.value,
        failing_constraint="nan_metric",
        failing_value=None,
        actual_win_rate=actual_win_rate, ...
    )
```
**Test added**: Yes — `test_block8b_engines.py::test_nan_win_rate_rejected_not_passed`
---
### B8B-002
**Category**: CODE_HYGIENE (P4 — Explicit Over Implicit)
**Severity**: P4 — Noted
**File**: `src/backtesting/fitness.py`, `_CONSTRAINT_CHECKS`
**Finding**:
The constraint comparators use `op.lt` for lower-bound checks (`win_rate`, `trades_per_week`,
`expectancy`, `profit_factor`) and `op.gt` for upper-bound checks (`max_drawdown`,
`losing_streak`). The semantics are:
- `op.lt(actual, threshold)` → rejects when `actual < threshold` → **accepts when `actual >= threshold`** (inclusive at boundary)
- `op.gt(actual, threshold)` → rejects when `actual > threshold` → **accepts when `actual <= threshold`** (inclusive at boundary)
A candidate with `win_rate` exactly equal to `min_win_rate` is accepted. This is the
correct behaviour (the spec says "minimum win rate" implies `>=`). The code is correct;
it is not documented explicitly anywhere in the source.
**Decision**: DOCUMENT — add a comment above `_CONSTRAINT_CHECKS` clarifying the
inclusive boundary semantics. No code change required.
**Test added**: Yes — `test_block8b_engines.py::test_constraint_boundary_win_rate_exact`
---
### B8B-003
**Category**: HARDCODING (P7 — Single Source of Truth)
**Severity**: P3 — Tracked
**File**: `src/backtesting/fitness.py`, `_compute_weighted_score`
**Finding**:
The expectancy normalisation scale `3.0` is hardcoded:
```python
expectancy_norm = _clamp(expectancy_points / 3.0, 0.0, 1.0)
```
A comment in the source explicitly flags this: *"not yet scenario-configurable — deferred
to Block 8 calibration"*. The value means an expectancy of 3.0 points maps to a normalised
score of 1.0. This is not calibrated to any specific instrument or timeframe.
Unlike the M-02 fields (`normalisation_pnl_ref_points` etc.), this constant has not been
lifted into `ScenarioProfile`. For real data in currency points, an expectancy of 3.0 may
be trivially small or unreachably large depending on the instrument.
**Decision**: DOCUMENT — add `normalisation_expectancy_ref_points: float = 3.0` to
`ScenarioProfile` (following the M-02 pattern), with calibration guidance. Deferred to
Block 9 alongside first real-run calibration of all normalisation constants.
**Test added**: No — deferred.
---
### B8B-004 (RESERVED — no finding)
P5 performance check on constraint loop: 6 comparisons × up to 5,000 candidates = 30,000
trivial arithmetic operations. Not a performance concern. No finding.
---
### B8B-005
**Category**: CONTRACT_GAP / PRINCIPLE_VIOLATION (P4 — Explicit Over Implicit)
**Severity**: P2 — Fix this block
**File**: `src/backtesting/wfo/wfo_evaluator.py` line ~75, `src/backtesting/wfo/wfo_engine.py`
**Finding**:
`evaluate_window()` always returns `oos_delta=None`:
```python
oos_delta=None,  # Populated by wfo_engine if IS/OOS gate is enabled
```
The comment describes an *intended* but *unimplemented* behaviour. `wfo_engine.run_wfo()`
collects `WFOWindowResult` objects and immediately writes them to the store with no
post-processing step to populate `oos_delta`. There is no code anywhere in either file
that computes or sets `oos_delta` after the fact.
**Cascading effects**:
1. `consistency_scorer._check_oos_gate()` reads `oos_delta` values — all `None` → empty
   list → always returns `False`. `oos_gate_triggered` is **always `False`** in every
   `WFOConsistencyScore`, regardless of whether `enforce_oos_gate=True` in config.
2. `median_oos_delta` in `WFOConsistencyScore` is **always `None`** from real data
   (B8-001 fixed the *persistence* of this value; this finding explains why the value is
   `None` even when computed — because `oos_delta` on each window is `None`).
3. The `oos_gate_triggered` modifier flag in `VerdictResult` is **always `False`**.
   The OOS gate mechanism is entirely non-functional in the current pipeline.
**Architectural issue**: Computing `oos_delta` requires a reference IS fitness value for
comparison. There is no established "IS baseline" in the current pipeline — the full-dataset
evaluation (from Stage 1) is the closest proxy, but it covers a different date range than
any individual WFO window. Proper IS/OOS delta computation requires running the strategy
on the IS portion of each window and comparing it to the OOS portion, which is not
implemented and would require a structural change to `evaluate_window`.
**Decision**: DOCUMENT as P2 finding. The OOS gate is non-functional. Add a clear
warning comment in `wfo_evaluator.py` and `wfo_engine.py`. Document in OPERATOR_RUNBOOK
that `enforce_oos_gate: true` currently has no effect. Full implementation deferred to
Block 9 / Phase 4 as it requires a structural design decision on IS/OOS window splitting.
The `oos_gate_triggered` field in all historical `VerdictResult` records is `False` and
should be interpreted as "gate not evaluated" rather than "gate passed".
**Test added**: Yes — `test_block8b_engines.py::test_oos_delta_always_none_documents_gap`
---
### B8B-006 (RESERVED — no finding)
`date_start`/`date_end` params on `strategy_runner.evaluate()`: H-01 audit confirmed these
exist in production. The uploaded `strategy_runner.py` may be a slightly earlier revision
that omits them from the shown signature. No new finding — H-01 disposition stands.
---
### B8B-007 (RESERVED — no finding)
P8 cache lifecycle: `clear_all_caches()` is called in every `strategy_runner.evaluate()`
`finally` block. Each `evaluate_window()` call creates a fresh `CacheManager`. Cache
state does not persist between window evaluations for the same candidate. Correct.
---
### B8B-008 (RESERVED — part of B8B-005)
`oos_delta` population absent in `wfo_engine.py` — documented under B8B-005.
---
### B8B-009 (RESERVED — no finding)
P1 SRP check on `wfo_engine.run_wfo()`: orchestration of dispatch, collection, writing,
scoring, and sufficiency check are all tightly coupled sub-tasks of "evaluate WFO for a
batch of candidates". Acceptable cohesion within a single stage function.
---
### B8B-010 (RESERVED — no finding)
Lightweight mode not writing consistency scores is intentional. GA uses results in-memory
only for generation fitness. Documented in ARCHITECTURE.md.
---
### B8B-011
**Category**: CODE_HYGIENE / P6 edge case
**Severity**: P3 — Tracked
**File**: `src/backtesting/wfo/consistency_scorer.py`
**Finding**:
When exactly one window has a valid result (`len(net_pnls) == 1`), `variance_raw = 0.0`
and `variance_norm = 1.0 - (0.0 / 0.10) = 1.0` — the best possible variance score.
A single data point cannot demonstrate temporal consistency; its variance score should be
neutral (0.5) or penalised, not optimal.
This only affects the GA lightweight mode (2 windows configured, 1 fails). Stage 4 Full
WFO uses all 5 configured windows and Stage 0 enforces a minimum of 3, so a single valid
window in Full WFO would require 4 window failures — the `window_collapse_flag` would then
be set and `flag_candidate_wfo_insufficient` would fire, effectively removing the candidate.
The practical impact is limited to GA fitness quality (optimistic variance for sparse
single-window candidates), not final verdict quality.
**Decision**: DOCUMENT. A `windows_evaluated == 1` case could return `variance_norm = 0.5`
to represent "no information" rather than "best score". Deferred to Block 9.
**Test added**: Yes — `test_block8b_engines.py::test_single_window_variance_is_optimistic`
---
### B8B-012
**Category**: HARDCODING (P7 — Single Source of Truth)
**Severity**: P2 — Fix this block
**File**: `src/backtesting/wfo/consistency_scorer.py`, `_sigmoid_normalise`
**Finding**:
`_sigmoid_normalise` uses `scale=0.10`, meaning `net_pnl=1.0` → `sigmoid(10)` ≈ 1.0.
With real strategy data where `net_pnl` is in currency points (typical values: tens to
thousands of points per window), any positive `net_pnl` maps to ≈ 1.0 and any negative
maps to ≈ 0.0. The sigmoid becomes effectively binary — all information about the
*magnitude* of per-window returns is lost.
Consequence: `median_return_norm` contributes only win/loss information, not return
magnitude. Two candidates where one earns 10 pts/window and another earns 10,000 pts/window
receive identical `median_return_norm` values (both ≈ 1.0), making this sub-metric useless
for distinguishing between them.
The `_MAX_EXPECTED_VARIANCE` constant (0.10) has the same calibration mismatch for the
same reason — real-data variance of net_pnl in points will be orders of magnitude larger.
**Decision**: FIX — add `wfo_sigmoid_scale` to `ScenarioProfile` (default: `0.10` for
backwards compatibility with unit tests). Operators set this to approximately 10% of
their median expected per-window P&L in points before the first real run. Add the
corresponding `wfo_variance_scale` for `_MAX_EXPECTED_VARIANCE`.
```python
# ScenarioProfile additions:
wfo_sigmoid_scale: float = 0.10          # calibrate to ~10% of median per-window P&L
wfo_variance_max_expected: float = 0.10  # calibrate to expected variance of per-window P&L
```
**Test added**: Yes — `test_block8b_engines.py::test_sigmoid_scale_calibration_warning`
---
### B8B-013
**Category**: P7 VIOLATION (Single Source of Truth)
**Severity**: P3 — Tracked
**File**: `src/backtesting/monte_carlo/mc_engine.py`
**Finding**:
`mc_engine._run_mc_internal()` reads `ruin_threshold` from the config dict:
```python
ruin_threshold = mc_cfg.get("ruin_threshold", config["monte_carlo"]["deep"].get("ruin_threshold", 0.20))
```
`ScenarioProfile` has a `mc_prefilter_ruin_threshold` field that is loaded by `scenario.py`
and validated in `ScenarioProfile.__post_init__`. However, `mc_engine` does not receive
a `ScenarioProfile` — it receives the raw config dict. The two values must agree, but there
is no enforcement of this agreement. An operator could set different values in the scenario
block vs the `mc_prefilter` block of `backtest_template.yaml` and get inconsistent behaviour.
`ScenarioProfile.mc_prefilter_ruin_threshold` appears to be loaded but never consumed by
any module in the current codebase — it exists in the contract but has no caller.
**Decision**: DOCUMENT — `mc_engine` should eventually receive `ScenarioProfile` and read
`mc_prefilter_ruin_threshold` from it. For now, both must be kept in sync in
`backtest_template.yaml`. Add a comment to `mc_engine._run_mc_internal()` noting the
dual-source risk. Deferred to Block 9.
**Test added**: No — deferred.
---
### B8B-014 (RESERVED)
Equity path array construction: in `equity_simulator.py` (not yet uploaded). `mc_engine`
passes `n_iterations` to `simulate_paths` — pre-allocation vs growth cannot be confirmed
from `mc_engine.py` alone. No finding at this stage.
---
### B8B-015 (RESERVED — no finding)
Seed isolation: same seed passed to all candidates' MC runs is correct and intentional —
ensures comparable random perturbations across candidates (standard MC analysis practice).
---
### B8B-016 (RESERVED — no finding)
M-04 post-fix: `worst_drawdown_per_path[ruined_paths] = 1.0` is correct.
All-False boolean index is a no-op. Verified. ✓
---
### B8B-017
**Category**: CODE_HYGIENE (P9)
**Severity**: P4 — Noted
**File**: `src/backtesting/monte_carlo/mc_metrics.py`
**Finding**:
`p5_final_equity` is computed, stored in `MCResult`, and written to the `mc_results` DB
table. It is not used in any verdict decision logic (`VerdictResult` has no `p5` field).
It is available to `report_generator` for display. This is intentional enrichment of
reports — the metric is not dead code.
**Decision**: DOCUMENT — add a comment in `mc_metrics.py` and `contracts.py` clarifying
that `p5_final_equity` is a **reporting metric only**, not a verdict input. This prevents
future developers from removing it as apparently unused.
**Test added**: No — informational only.
---
## 8B Summary Table
| ID | Category | Severity | File | Decision | Test |
|---|---|---|---|---|---|
| B8B-001 | PRINCIPLE_VIOLATION P6 | P2 | fitness.py | FIX | Yes |
| B8B-002 | CODE_HYGIENE P4 | P4 | fitness.py | DOCUMENT | Yes |
| B8B-003 | HARDCODING P7 | P3 | fitness.py | DOCUMENT (defer B9) | No |
| B8B-005 | CONTRACT_GAP P4 | P2 | wfo_evaluator.py, wfo_engine.py | DOCUMENT | Yes |
| B8B-011 | CODE_HYGIENE P6 | P3 | consistency_scorer.py | DOCUMENT (defer B9) | Yes |
| B8B-012 | HARDCODING P7 | P2 | consistency_scorer.py | FIX | Yes |
| B8B-013 | P7 VIOLATION | P3 | mc_engine.py | DOCUMENT (defer B9) | No |
| B8B-017 | CODE_HYGIENE P9 | P4 | mc_metrics.py | DOCUMENT | No |
**8B P2 findings**: 3 (B8B-001, B8B-005, B8B-012)
**8B P3 findings**: 3 (B8B-003, B8B-011, B8B-013)
**8B P4 findings**: 2 (B8B-002, B8B-017)
---
### B8B-018
**Category**: CONTRACT_GAP (P6 — Fail Fast / P7 — Single Source of Truth)
**Severity**: P2 — Verify in Block 8C (contracts.py required)
**File**: `src/backtesting/wfo/wfo_evaluator.py` line ~82
**Finding**:
`wfo_evaluator.py` reads `net_pnl` from `MetricsReport` via:
```python
net_pnl=_safe_float(m, "net_pnl"),
```
`_safe_float` returns `None` if the attribute is absent. `fitness.py` reads the same
P&L field from `MetricsReport` as `total_pnl_points`:
```python
net_pnl_raw = _get(metrics, "total_pnl_points") or 0.0
```
If `MetricsReport` defines the field as `total_pnl_points` (the name used everywhere
in `fitness.py`) and not `net_pnl`, then `_safe_float(m, "net_pnl")` returns `None`
for every window evaluation.
**Cascading effects if confirmed**:
- `WFOWindowResult.net_pnl` is `None` for all windows
- `consistency_scorer` `net_pnls` list is empty → `median_return_raw=0.0` always
- `fraction_positive_windows` is always 0.0 (no positive net_pnl values)
- `variance_raw = 0.0` → `variance_norm = 1.0` (appears as B8B-011 but for wrong reason)
- `composite_score` is systematically incorrect — `wfo_weight_median_return` and
  `wfo_weight_fraction_positive` components are permanently zeroed
**Cannot confirm without `contracts.py`**. `MetricsReport` may define both `net_pnl` and
`total_pnl_points` as aliases, or the field may be `net_pnl` throughout with `fitness.py`
using an alternate name for the scoring path. The discrepancy is real and visible across
the two files; the severity depends on whether `MetricsReport` exposes the `net_pnl` alias.
**Decision**: VERIFY — add `contracts.py` to Block 8C upload list as P0 requirement.
If confirmed: fix `wfo_evaluator.py` to use `total_pnl_points` (or vice versa, whichever
matches `MetricsReport`). This is the highest-priority 8B finding pending verification.
**Test added**: Yes — `test_block8b_engines.py::test_net_pnl_field_name_matches_metrics_report`
(test is a verification harness — will fail if the mismatch is real).
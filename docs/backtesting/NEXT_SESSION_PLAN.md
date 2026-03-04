# NEXT_SESSION_PLAN.md — Block 8: E2E Code Analysis and Hardening
**Entering state**: Block 7 complete. 246 tests green. All audit findings closed.
**Objective**: Treat the pipeline as a production system and pressure-test every module
before real capital is at risk. Find hardcodings, bottlenecks, SRP violations,
and silent failure modes. Produce production-grade documentation.
**Approach**: Methodical, unhurried, evidence-driven. No change without a finding.
No finding without a test or a documented disposition. Depth over speed.
---
## Three Working Documents (open throughout all sub-blocks)
### 1. ARCHITECTURE.md (perfecting for production)
A living architectural reference. Covers: module responsibilities, data flow diagram,
contract catalogue, inter-module dependency map, Stage execution model, known deferral
decisions and their rationale. Sections written as each sub-block is processed.
Not a spec — a description of what the code actually does, kept honest by the audit.
### 2. OPERATOR_RUNBOOK.md (perfecting for production)
Practical operator-facing guide. Covers: how to start a run, how to interpret verdicts,
what each DB table means, how to promote a PAPER_TRADE candidate, what to do when a
Stage fails, how to resume. Sections updated as audit uncovers implicit assumptions
that operators need to know about.
### 3. BLOCK8_AUDIT_REPORT.md (bootstrapped in 8A, extended in 8B, completed in 8C)
The central finding registry. Structure per entry:
  - ID: B8-XXX (sequential)
  - Category: HARDCODING | BOTTLENECK | SRP_VIOLATION | CONTRACT_GAP |
               PRINCIPLE_VIOLATION | CODE_HYGIENE | PERF_OPPORTUNITY | DEFERRED
  - Severity: P1 (fix before production) | P2 (fix this block) | P3 (tracked) | P4 (noted)
  - File + line range
  - Finding description
  - Decision: FIX | DOCUMENT | DEFER_TO_B9 | REJECT_WITH_REASON
  - Test added: yes/no + test ID
---
## Architecture Principles Checklist (applied to every file in every sub-block)
From the project architecture document — verified against each module's actual behaviour:
| # | Principle | What to check |
|---|---|---|
| P1 | Single Responsibility | Does this module do exactly one thing? Does it reach into another module's domain? |
| P2 | Contracts Are the Interface | Any raw dict crossing a module boundary? Any tuple return that should be a contract? |
| P3 | Immutability | Any `frozen=True` dataclass with mutable default fields? Any post-construction mutation via `object.__setattr__` outside `__post_init__`? |
| P4 | Explicit Over Implicit | Any hidden stage-gating? Any implicit promotion logic? Any behaviour change driven by a flag that isn't documented at every call site? |
| P5 | Vectorisation First | Any Python loop over candidates or MC paths that should be a numpy operation? Any `.apply()` that should be `.values`? |
| P6 | Fail Fast | Any invalid config that gets through Stage 0? Any parameter combination that silently produces a bad candidate instead of raising? |
| P7 | Single Source of Truth | Any module that loads its own config? Any hardcoded value that duplicates backtest_template.yaml? |
| P8 | Cache Lifecycle | Is `clear_all_caches()` called correctly between candidate runs? Any new caching introduced outside CacheManager? |
| P9 | Code Hygiene | Any debug flag, print statement, commented-out block, or MagicMock in production code? |
| P10 | Immutable Run Artifacts | Any post-run config mutation? Any seed or config hash that can be changed after run initialisation? Any YAML rewrite that doesn't create a new run record? |
---
## Sub-Block 8A — Foundation Layer
**Files**: `contracts.py`, `candidate_store.py`, `strategy_runner.py`, `orchestrator.py`
**Principles focus**: P2, P3, P4, P7, P10
**Document output**: Bootstrap `BLOCK8_AUDIT_REPORT.md`. Write `ARCHITECTURE.md` §1–3
(module map, contract catalogue, Stage execution model).
### Upload list for 8A
```
src/backtesting/contracts.py           (latest — already delivered)
src/backtesting/candidate_store.py     (full file — post H-02 fix)
src/backtesting/strategy_runner.py
src/backtesting/orchestrator.py        (full file — post M-05 fix)
```
### 8A Analysis targets
**contracts.py**
- Every Optional field: enumerate all None paths and confirm all consumers handle them.
  Focus: FitnessResult.failing_value, MCResult.p5_final_equity, VerdictResult.yaml_output_path.
- CandidateRecord: is every field actually written by candidate_store, or are any
  always NULL in the DB? Cross-reference against store write methods.
- WFOConsistencyScore.median_oos_delta (NEW): confirm verdict.py reads it, not None guard needed.
- ScenarioProfile new fields (M-02/03): confirm backtest_template.yaml documents all four
  new fields with their defaults and calibration guidance.
- P3 check: any `field(default_factory=...)` in frozen dataclasses? (prohibited)
**candidate_store.py**
- Schema completeness: does the CREATE TABLE statement include every field in CandidateRecord?
  Specifically: `wfo_median_oos_delta` (added in 7D) — is the column present?
- H-02 post-fix: confirm write_wfo_window_result and flag_candidate_wfo_insufficient
  are called in the right order by wfo_engine and both succeed.
- Writer thread: is getattr() dispatch still used? If so, enumerate all method names
  in the dispatch map and verify each exists (L-05 lesson).
- Read methods: for every `store.get_*()` call in orchestrator, confirm the method
  exists and returns the expected type.
- P10 check: can any existing run_metadata record be mutated after write? Confirm
  the store has no update path for run_id, config_hash, or seeds.
**strategy_runner.py**
- Spawn safety: every object passed to worker processes must be picklable.
  Check: ScenarioProfile (frozen dataclass — should be fine), Path objects, any lambda
  or local closure (not picklable — would cause silent failure on Windows).
- _PARAM_KEY_MAP: is it complete? Any parameter name in backtest_template.yaml that
  is not in _PARAM_KEY_MAP would have been caught by M-05, but verify the map itself
  is accurate and not stale.
- Error contract: every exception caught and surfaced via CandidateResult.error=str(exc)?
  Any bare `except:` that swallows the traceback?
- P6 check: what happens with min_significant_trades=0? Does it produce a valid
  result or silently skip the adequacy check?
**orchestrator.py**
- Stage gate completeness: are all 8 Checkpoints (0–7) written to the store?
  Confirm the checkpoint update call exists after each stage completes.
- Resume logic: does resuming at each checkpoint correctly skip already-completed stages
  without re-running them or overwriting their results?
- P4 check: is there any implicit stage promotion (e.g. skipping Stage 3 when
  MC pre-filter is disabled) that is not explicitly documented at the call site?
- M-05 post-fix: confirm _validate_parameter_names is called before the enabled_zones
  check, not after (order matters for meaningful error messages).
### 8A Test additions
Target: ~8–12 new tests covering contract None-path gaps and store schema completeness.
File: `test_block8a_foundation.py`
---
## Sub-Block 8B — Evaluation Engines
**Files**: `fitness.py`, `wfo/wfo_engine.py`, `wfo/consistency_scorer.py`,
           `monte_carlo/mc_engine.py`, `monte_carlo/mc_metrics.py`
**Principles focus**: P1, P5, P6, P8
**Document output**: Extend `BLOCK8_AUDIT_REPORT.md`. Write `ARCHITECTURE.md` §4–6
(evaluation data flow, WFO window model, MC path model).
### Upload list for 8B
```
src/backtesting/fitness.py                         (post M-02 fix)
src/backtesting/wfo/wfo_engine.py
src/backtesting/wfo/consistency_scorer.py          (post M-01/M-03 fix)
src/backtesting/monte_carlo/mc_engine.py
src/backtesting/monte_carlo/mc_metrics.py          (post M-04 fix)
```
### 8B Analysis targets
**fitness.py**
- _CONSTRAINT_CHECKS tuple: built at import time without a ScenarioProfile. After M-02,
  _CONSTRAINT_DRAWDOWN_REF_POINTS is still hardcoded for the constraint path. Is this
  intentional and documented? Confirm the constraint check uses the module constant
  but the scoring path uses scenario.normalisation_drawdown_ref_points. No drift.
- op.lt vs op.le boundary semantics: for min_win_rate, is a candidate with
  win_rate exactly equal to min_win_rate accepted (lt) or rejected (le)?
  Cross-check with TECHNICAL_SPEC. If spec says >= min_win_rate is acceptable,
  op.lt is correct. Document explicitly.
- _clamp safety: what happens with NaN metrics from strategy_runner?
  NaN comparisons in Python behave unexpectedly (NaN > x is False, NaN < x is False).
  A NaN win_rate would pass the constraint check silently — is that handled?
- P5: is the constraint check loop a performance concern? At N=5000 random candidates,
  5000 × 6 comparisons is trivial. Confirm no unnecessary computation inside the loop.
**wfo_engine.py**
- Date range passing: H-01 confirmed that window dates are passed correctly.
  Verify additionally that dates are passed as date objects, not datetime, and that
  strategy_runner accepts both.
- Error propagation: when strategy_runner returns CandidateResult.error, does
  wfo_engine produce a WFOWindowResult with error= set, or does it raise?
  The contract requires: never raise to caller.
- oos_delta population: this field is the core of the IS/OOS gate. Confirm it is
  actually computed and set in the WFOWindowResult. If not, median_oos_delta (M-01)
  will always be None even when oos_gate is enabled — the fix is in place but
  the data it depends on may never be populated.
- P8: is clear_all_caches() called between window evaluations, or only between
  candidates? Window evaluations for the same candidate should share cache state
  (different date ranges, same parameters) — confirm the call site is correct.
**consistency_scorer.py**
- Single-window case: statistics.variance requires >= 2 values — the `len >= 2` guard
  is present, but confirm variance_raw=0.0 for single-window produces variance_norm=1.0
  (inverted: 0/0.10 = 0, 1-0 = 1.0). This gives the best variance score to single-window
  candidates — is this the intended behaviour or should it be penalised?
- _sigmoid_normalise: with scale=0.10, a net_pnl of 1.0 point gives
  sigmoid(1.0/0.10) = sigmoid(10) ≈ 1.0. Is this calibrated for points or fractions?
  If net_pnl is in currency points (e.g. 500 pts), sigmoid(500/0.10) = 1.0 always.
  The scale may need recalibration for real data — flag as B8 finding.
**mc_engine.py**
- Path array construction: is equity_paths allocated once or grown incrementally?
  Growing a numpy array inside a loop (np.vstack) is O(n²) — should be pre-allocated.
- Seed isolation: does each candidate's MC run use a fresh seeded RNG, or is there
  shared state between candidates (would violate P10 — reproducibility)?
- ruin_threshold source: is it read from ScenarioProfile.mc_prefilter_ruin_threshold
  or hardcoded? After M-02 fixes, this should be fully scenario-driven.
- P1: does mc_engine do more than simulate paths? Any fitness scoring inside mc_engine
  would be an SRP violation (fitness evaluation belongs in fitness.py).
**mc_metrics.py**
- M-04 post-fix: the ruined_paths clamp is in place. Confirm the boolean indexing
  `worst_drawdown_per_path[ruined_paths] = 1.0` works correctly when ruined_paths
  is an all-False array (no ruined paths) — should be a no-op.
- p5_final_equity: is it used anywhere in verdict or report? If it is only computed
  and stored but never read, flag as potential dead weight (P9 - hygiene).
### 8B Test additions
Target: ~10–14 new tests covering boundary conditions and vectorisation correctness.
File: `test_block8b_engines.py`
---
## Sub-Block 8C — GA, Sensitivity, Verdict, Output
**Files**: `ga/ga_engine.py`, `ga/mutation.py`, `evaluation/sensitivity.py`,
           `evaluation/verdict.py`, `report_generator.py`
           (+ `yaml_generator.py` if it exists)
**Principles focus**: P1, P4, P9, P10
**Document output**: Complete `BLOCK8_AUDIT_REPORT.md`. Finalise `ARCHITECTURE.md`.
Update `OPERATOR_RUNBOOK.md` with all Block 8 operator-relevant findings.
### Upload list for 8C
```
src/backtesting/ga/ga_engine.py
src/backtesting/ga/mutation.py                     (post M-06 fix)
src/backtesting/evaluation/sensitivity.py          (post OPT-01 fix)
src/backtesting/evaluation/verdict.py              (post M-01 fix)
src/backtesting/report_generator.py
src/backtesting/yaml_generator.py                  (if exists)
```
### 8C Analysis targets
**ga_engine.py**
- mutation_std_steps wiring: after M-06, ga_engine must read
  config["genetic"]["mutation_std_steps"] and pass it to mutate(). Confirm the wiring
  is complete, not still using the old hardcoded call.
- Seed propagation: the GA seed (RunMetadata.ga_seed) must be the only source of
  randomness for ga_engine. Confirm no use of random.random() without seeding,
  no numpy random without explicit seed. Reproducibility is P10.
- Elitism: is the top-N carry-forward from one generation to the next explicit
  (documented at the call site) or implicit (happens as a side effect of sorting)?
  P4 requires explicitness.
- Population diversity: what prevents the GA from converging to identical candidates
  (all same candidate_id hash)? If crossover produces duplicates, fitness evaluation
  is wasted. Is there a deduplication step?
- P6: what happens when the initial population is empty (all random candidates failed
  constraints)? Does the GA raise, log and continue, or silently produce no output?
**mutation.py**
- Boundary clamping: after a Gaussian perturbation, the value is clamped to [low, high]
  and snapped to the grid. Confirm the snap happens BEFORE the clamp (snap then clamp),
  not after (clamp then snap — could produce a value off the grid at the boundary).
- choice mutation: if current_value is not in choices (data corruption), _mutate_choice
  returns current_value unchanged. This is silent — should it log a warning?
**sensitivity.py**
- OPT-01 post-fix: with pool=None path and pool=provided path both exercised.
  Confirm nullcontext import is from contextlib (not a third-party library).
- perturbation_plan ordering: the plan is built from current_params.items() which
  in Python 3.7+ is insertion-ordered. Is this order deterministic across runs?
  P10 requires reproducibility — confirm the order is stable.
- Worker error handling: if _evaluate_perturbation raises an unhandled exception
  (not a strategy evaluation error but a worker crash), future.result() re-raises it.
  The outer try/except catches it. Confirm the error string includes enough context
  to diagnose the crash (candidate_id, param_name, step_offset).
**verdict.py**
- Boundary semantics: wfo_pillar_go uses `>=` (wfo_composite >= wfo_go_floor).
  wfo_pillar_no_go uses `<` (wfo_composite < wfo_borderline_floor).
  The borderline zone is therefore [borderline_floor, go_floor) — inclusive at bottom,
  exclusive at top. Confirm this matches TECHNICAL_SPEC exactly.
- NO_GO cannot be upgraded: `if wfo_pillar_no_go or mc_pillar_no_go: verdict = NO_GO`
  is evaluated before modifier flags. Confirm that even with all four flags False,
  a no_go pillar result produces NO_GO. The current code is correct — document explicitly.
- P10: is the VerdictResult written to the store with run_id? Can a verdict be
  overwritten by re-running Stage 7 without creating a new run record?
**report_generator.py**
- M-07: figsize hardcoding. Identify all hardcoded chart dimensions and extract to
  config["output"]["chart_width"] / "chart_height" with sensible defaults.
  This is the one remaining P7 (Single Source of Truth) violation from Block 7.
- None field rendering: VerdictResult.parameter_region_width is always None.
  Confirm report renders it as "N/A" or similar, not as "None" (ugly) or crash.
  Same for yaml_output_path before yaml_generator runs.
- P9 hygiene: any print() statements? Any debug flags? Any commented-out plot types?
### 8C Test additions
Target: ~10–14 new tests covering GA correctness, verdict boundary semantics,
and report None-field rendering.
File: `test_block8c_ga_verdict_output.py`
---
## Block 8 Aggregate Targets
| Sub-Block | Files | New tests | Document output |
|---|---|---|---|
| 8A — Foundation | 4 | ~10–12 | AUDIT_REPORT (bootstrap), ARCHITECTURE §1–3 |
| 8B — Engines | 5 | ~10–14 | AUDIT_REPORT (extend), ARCHITECTURE §4–6 |
| 8C — GA/Verdict/Output | 5–6 | ~10–14 | AUDIT_REPORT (complete), ARCHITECTURE (finalise), OPERATOR_RUNBOOK (update) |
| **Total** | **14–15** | **~30–40** | **3 documents complete** |
Running test count projection: 246 → ~280–290
---
## Block 8 Success Criteria
- [ ] Every module audited against all 10 architecture principles
- [ ] BLOCK8_AUDIT_REPORT.md complete: every finding with decision and test status
- [ ] ARCHITECTURE.md production-ready: accurate, complete, no aspirational content
- [ ] OPERATOR_RUNBOOK.md production-ready: operator can run the system blind from it
- [ ] All P1 findings fixed before 8C closes
- [ ] All P2 findings fixed or have a written deferral rationale
- [ ] ~280–290 tests, all green
- [ ] Stage 6 performance confirmed <= 200s on target hardware (OPT-01 verification)
- [ ] No remaining hardcoded values that belong in backtest_template.yaml
- [ ] NaN metric handling confirmed (fitness.py constraint path)
- [ ] oos_delta population confirmed in wfo_engine (prerequisite for median_oos_delta utility)
---
## Block 8 Design Principle
Block 8 is not a feature sprint. Every change requires:
  1. A finding in BLOCK8_AUDIT_REPORT.md
  2. Either a failing test or a documented analysis showing the risk
  3. A fix (or explicit DEFER/REJECT with written rationale)
No speculative engineering. No "while we're here" refactors unless they surface
a principle violation. The goal is to reach a state where a new developer could
read ARCHITECTURE.md and OPERATOR_RUNBOOK.md, then run the pipeline on real data
with full confidence in what it is doing and why.
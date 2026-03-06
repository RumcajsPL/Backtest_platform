# CONTEXT.md — Block 9G Handoff
**Date**: 2026-03-06
**Session**: Block 9G
**Status**: ✅ Pipeline fully integrated — all 7 stages clean, end-to-end
---
## What Was Accomplished This Session
### B9G-001 [FIXED] — GA FOREIGN KEY constraint failed
Offspring candidates from crossover+mutation were never written to `candidates`
table before `write_wfo_window_result()` was called, causing FK constraint failure.
**Fix** (`candidate_store.py` + `ga_engine.py`):
- Added `write_candidate_stub()` public method + `_write_candidate_stub()` internal writer
  (INSERT OR IGNORE on `candidates` + `candidate_parameters` only — no `evaluations` row)
- `ga_engine._evaluate_generation()` now calls `store.write_candidate_stub()` + `store.flush()`
  for every candidate before pool submission
### B9G-002 [FIXED] — Stage 5 MC Deep: CandidateResult is invalid
`store.get_candidate_result()` always returns `trades=None / metrics=None` (L-15).
Stage 5 was passing this broken result to `run_mc()` → "CandidateResult is invalid".
**Fix** (`orchestrator.py` `_run_stage_5_mc_deep`):
- Re-evaluate each candidate via `strategy_runner.evaluate()` before calling `run_mc()`
- Identical pattern to B9F-003 (Stage 2)
- If re-evaluation fails: still call `run_mc()` → `MCResult(error=..., ruin_probability=None)`
  → NO_GO in Stage 7 (correct conservative treatment)
### B9G-003 [FIXED] — Stage 7 duplicate verdicts
`rank_by_wfo()` returned duplicate `candidate_id` rows because `query_candidates`
JOINs `evaluations` — a candidate with rows in both `RANDOM` and `MC_PREFILTER_PASS`
stages produced two records with the same `candidate_id`.
**Fix** (`ranker.py` `rank_by_wfo`):
- Added `seen_ids` deduplication pass after ORDER BY, keeping first (highest-score)
  occurrence of each `candidate_id`
- Affects Stages 5, 6, and 7 simultaneously (all call `rank_by_wfo`)
### B9G-004 [FIXED] — Stage 7 trading YAML missing `strategy:` section
Two bugs in `yaml_generator.py`:
1. `_STRATEGY_PARAM_KEY_MAP` pointed all parameters to `("strategy", ...)` or
   `("parameters", ...)` — neither key exists in `strategy_template.yaml`
2. `_structural_validate` checked for `["strategy", "parameters"]` — same phantom keys
3. `StrategyConfig.from_yaml()` silently accepted the broken config (did not raise)
   so the validation branch short-circuited before `_structural_validate`
**Fix** (`yaml_generator.py` — full replacement):
- `_PARAM_MAP` rewritten with three-tuple `(top_section, nested_path_tuple, leaf_key)`
  mapping all 9 safe zone params to correct template locations:
  - `rsi_*` → `filters.technical_filters.rsi_filter`
  - `bollinger_*` → `filters.technical_filters.bollinger_filter`
  - `atr_length/atr_multiplier` → `trade_management.risk`
  - `rr_target` → `trade_management.risk.risk_to_reward_ratio`
  - `risk_percentile` → `trade_management.risk.max_risk_percentile`
  - All exploration/discovery zone params pre-mapped (ready for B9F-001 fix)
- `_structural_validate` now checks `["filters", "trade_management"]` + spot-checks
  `filters.technical_filters` and `trade_management.risk` are dicts
- `_validate_strategy_config` runs structural check as hard backstop always
  (even when `StrategyConfig.from_yaml()` passes)
- Unknown parameters log WARNING instead of silently dropping into phantom section
---
## Final Pipeline Run (2026-03-06 17:43–18:00)
```
Run ID : b0faec30-5860-4e1d-a796-7353ad1aaf7c
Config : backtest_1st_run.yaml (scenario: e2e_test)
Stage 0: ✅ Validation passed — 5 WFO windows, 1 enabled zone
Stage 1: ✅ 50 evaluated, 50 passed, 0 failed
Stage 2: ✅ 30 pass, 0 fail
Stage 3: ✅ GA complete (5 gen, early stop gen 4 — stagnation)
Stage 4: ✅ 27/27 candidates scored (1 candidate flagged WFO_INSUFFICIENT_WINDOWS — expected)
Stage 5: ✅ 10/10 processed (no MC failures)
Stage 6: ✅ 5/5 processed
Stage 7: ✅ 5 verdicts written, 5 trading YAMLs written — no errors
Warnings (benign):
  candidate 7bf2f892d683 — WFO_INSUFFICIENT_WINDOWS (0/5 valid windows)
  This is expected: bad param combo, correctly excluded downstream.
```
**Top 5 verdicts (auto_go, e2e_test scenario — not for trading):**
| Candidate      | WFO   | Ruin   |
|----------------|-------|--------|
| 7fc22c8fcce4   | 0.700 | 0.0000 |
| 8dc8164ee1a8   | 0.650 | 0.0000 |
| c45693d20e9c   | 0.650 | 0.0000 |
| 098a54d31e95   | 0.650 | 0.0000 |
| 86ed43ac04a8   | 0.350 | 0.0000 |
---
## Current Pipeline Status
| Stage | Status | Notes |
|-------|--------|-------|
| 0 Validation | ✅ Implemented | |
| 1 Random Search | ✅ Implemented | |
| 2 MC Pre-Filter | ✅ Implemented | Re-evaluates via evaluate() (B9F-003) |
| 3 GA | ✅ Implemented | write_candidate_stub() FK guard (B9G-001) |
| 4 Full WFO | ✅ Implemented | wfo_engine handles all persistence internally |
| 5 MC Deep | ✅ Implemented | Re-evaluates via evaluate() (B9G-002) |
| 6 Sensitivity | ✅ Implemented | |
| 7 Report & Output | ✅ Implemented | yaml_generator fixed (B9G-004) |
**All 7 stages fully operational.**
---
## Open Issues (prioritised)
### P1 Blockers (production)
```
B9F-001 — parameter_space.py: expand_zones() enumerates full Cartesian product
  exploration zone: ~387T combinations → OOM
  Fix: refactor expand_zones() to return per-param value lists (not full product)
       refactor sampler._lhs_sample() to accept per-param lists
  Workaround: exploration.enabled: false (active)
B8B-012 — consistency_scorer.py: _sigmoid_normalise scale=0.10 needs calibration
  Fix after first REAL run (not e2e_test): measure net_pnl distribution
  → set scale ≈ stdev * 0.5
```
### P3 (non-blocking)
```
B8B-003 — fitness.py: expectancy_norm hardcoded /3.0 — calibrate after real run
B8-009  — orchestrator.py: raw sqlite3 in _resume_or_start bypasses CandidateStore
B9B-001 — crossover.py: no zone-name guard for cross-zone parents
B8B-013 — mc_engine.py: ruin_threshold dual-source
B8B-011 — consistency_scorer.py: fraction_positive_windows fixed 0.0 floor
B8C-002/003 — report_generator.py: deferred
B9C-008 — sampler.py: deferred
OPT-01  — Stage 6 target ≤200s (currently ~229s on e2e_test)
```
---
## Immediate Next Actions
1. **Result analysis** — examine the e2e_test run output:
   - Review HTML report for Stage 7 output quality
   - Check trading YAML content against strategy_template.yaml
   - Verify `backtester_metadata` section in output YAMLs
2. **Calibration run preparation** (switch from e2e_test → capital_accumulation):
   - Restore `random_search.samples_per_zone: 200`
   - Restore `genetic.population_size: 60`, `generations: 30`
   - Review `min_win_rate: 0.15` — was eased from 0.45 for e2e_test
3. **B8B-012** — after first real run, measure net_pnl distribution and calibrate
   `_sigmoid_normalise` scale in `consistency_scorer.py`
4. **B9F-001** — refactor `expand_zones()` before enabling exploration zone
---
## Files Modified This Block
| File | Change |
|------|--------|
| `src/backtesting/candidate_store.py` | Added `write_candidate_stub()` + `_write_candidate_stub()` + fixed `_write_mc_result()` `run_id` ref |
| `src/backtesting/ga/ga_engine.py` | `_evaluate_generation()`: stub writes + `windows_total` fix |
| `src/backtesting/orchestrator.py` | Stage 4 full impl + Stage 5 re-evaluate fix |
| `src/backtesting/ranker.py` | `rank_by_wfo()`: dedup by candidate_id |
| `src/backtesting/yaml_generator.py` | Full replacement: correct `_PARAM_MAP` + validator |
---
## Lessons Learned (Block 9G additions)
```
L-19: GA offspring must be registered via write_candidate_stub() before any FK-referencing
      writes (wfo_window_results, mc_results, etc.). INSERT OR IGNORE is safe for both
      new and existing candidates.
L-20: windows_total passed to compute_consistency() must equal len(window_results)
      (actual results received), not the requested sample size. GA lightweight mode
      may receive 1–2 results per generation; using len(windows) violates the contract
      guard windows_evaluated <= windows_total.
L-21: rank_by_wfo() (and any ranker that JOINs evaluations) must deduplicate by
      candidate_id before applying top_n. A candidate in multiple stages produces
      multiple rows; without dedup, duplicate verdicts and YAMLs propagate to Stage 7.
L-22: yaml_generator._PARAM_MAP must be derived from the actual strategy_template.yaml
      structure — never inferred from parameter names alone. Always cross-reference
      the template before writing or updating the map.
L-23: StrategyConfig.from_yaml() may silently accept structurally invalid configs
      (it did in this session). Always run structural validation as a hard backstop
      regardless of whether schema validation passes.
L-24: The only reliable way to diagnose yaml_generator failures is to inspect the
      "Sections present" in the error message against strategy_template.yaml top-level
      keys. If "strategy" or "parameters" appear in "Sections present" but not in the
      template, the _PARAM_MAP is writing to phantom sections.
```
---
## Architecture Invariants (unchanged — do not modify)
- `store.get_candidate_result()` always returns `trades=None / metrics=None` — never use for MC input
- `write_candidate_stub()` is always safe (INSERT OR IGNORE) — call before any FK write
- `rank_by_wfo()` deduplicates by candidate_id — `rank_combined()` already did this; now consistent
- `yaml_generator._PARAM_MAP` must stay in sync with `strategy_runner._PARAM_KEY_MAP`
- `StrategyConfig.from_yaml()` is not a reliable validator — structural check runs always
- `e2e_test` scenario: loose thresholds for pipeline validation only — all results are `auto_go`
  at WFO ≥ 0.01; do not interpret verdicts from this scenario
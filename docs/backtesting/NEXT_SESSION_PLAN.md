# NEXT_SESSION_PLAN.md — Phase 5: Orchestrator Final Wiring + Live Integration
**Prepared**: 2026-03-01
**Session goal**: Wire all orchestrator stubs (Stages 5/6/7), run a full live pipeline test with real SQLite and a realistic strategy runner, and produce the first real end-to-end run artifact. Clean up `datetime.utcnow()` deprecation warnings.

---

## How to Start the Session
1. Open a new chat
2. Paste the **entire content of `CONTEXT.md`** as your first message
3. Add: *"We are starting Phase 5 — Orchestrator Final Wiring. Follow the breakdown in NEXT_SESSION_PLAN.md."*
4. Claude reads the `backtester-project` skill automatically
5. Ask Claude to confirm it has read CONTEXT.md, the skill, and the current `orchestrator.py` and `candidate_store.py` — before writing any code

**Pre-coding reads Claude must do (upload on request or confirm already visible):**
- `src/backtesting/orchestrator.py` — read existing skeleton to understand current checkpoint wiring
- `docs/backtesting/TECHNICAL_SPEC.md` Section 4 — module signatures for Stage 5/6/7 modules
- `docs/backtesting/FUNCTIONAL_SPEC.md` — Stage 5, 6, 7 plain-language descriptions
- `src/backtesting/candidate_store.py` — confirm all required write/query methods exist

---

## Session Objective
At the end of this session we will have:
- `orchestrator.py` fully wired: all 8 stages connected, checkpoint/resume operational end-to-end
- `CandidateStore.close()` confirmed in `finally` block
- `datetime.utcnow()` removed from all Phase 2/3 modules (single cleanup pass)
- A live integration test with real SQLite and realistic (mocked-at-runner-level) strategy runner
- All 68 existing tests still green after orchestrator changes
- The pipeline runnable end-to-end from `orchestrator.run(config_path)` with synthetic data

---

## Non-Negotiable Before Writing Any Code
1. Confirm `orchestrator.py` verify if is already fully wired
2. Confirm `CandidateStore` exposes all required methods (see list in CONTEXT.md Phase 5 section)
3. Confirm `strategy_runner.evaluate()` accepts `date_start`/`date_end` kwargs
4. Confirm `evaluate_sensitivity()` signature matches orchestrator call site
5. Confirm `compute_verdict()` signature matches orchestrator call site

---

## Work Breakdown
---
### Block 0 — `CandidateStore` method audit (~10 min)

Confirm the following methods exist in `candidate_store.py`. If missing, add them:
- `write_mc_result(result: MCResult, run_id: str) -> None`
- `write_sensitivity_profile(profile: SensitivityProfile, run_id: str) -> None`
- `write_verdict(verdict: VerdictResult, run_id: str) -> None`
- `query_verdicts(run_id: str) -> List[dict]`
- `query_sensitivity_profiles(run_id: str) -> List[dict]`
- `query_wfo_window_results(candidate_id: str) -> List[dict]`
- `query_sensitivity_results(candidate_id: str) -> List[dict]`
- `query_mc_results(run_id: str, mode: str) -> List[dict]`
- `query_wfo_consistency_scores(run_id: str) -> List[dict]`
All writes go through the existing single-writer queue pattern. All queries are direct reads (no queue).
---

### Block 1 — Orchestrator - audit (~10 min) stages 5/6/7
Audif for below wires:
Wire `_run_mc_deep()` stub:
```python
# Pseudocode — implement using real mc_engine.run_mc()
top_candidates = ranker.rank_by_wfo(store, run_id, top_n=config["monte_carlo"]["deep"]["input_count"])
for cand_record in top_candidates:
    candidate = _record_to_candidate(cand_record)
    candidate_result = _load_candidate_result(store, candidate.candidate_id)  # from evaluations table
    mc_result = run_mc(candidate, candidate_result, MCMode.DEEP, config, seed=run_metadata.mc_deep_seed)
    store.write_mc_result(mc_result, run_id)
store.set_checkpoint(run_id, Checkpoint.MONTE_CARLO_COMPLETE)
```
Key decisions:
- `candidate_result` for MC Deep must be the original full-dataset evaluation (Stage 1), not a WFO window result. Load from `evaluations` table where `stage='RANDOM'` or `stage='GA'`.
- If `mc_result.error` is set: log warning, write result anyway (ruin_probability=None → will trigger NO_GO in verdict)
---
Wire `_run_sensitivity()` stub:
```python
top_candidates = ranker.rank_by_wfo(store, run_id, top_n=config["sensitivity"]["input_count"])
for cand_record in top_candidates:
    candidate = _record_to_candidate(cand_record)
    baseline_fitness = _get_baseline_fitness(store, candidate.candidate_id)
    sensitivity = evaluate_sensitivity(
        candidate=candidate,
        baseline_fitness=baseline_fitness,
        parameter_space_def=config["zones"],
        base_yaml_path=base_yaml_path,
        temp_dir=temp_dir,
        scenario=scenario,
        spike_threshold=config["sensitivity"]["spike_threshold"],
        max_steps=config["sensitivity"]["max_steps"],
        max_workers=config["run"]["max_workers"],
        min_significant_trades=config["random_search"]["min_significant_trades"],
    )
    store.write_sensitivity_profile(sensitivity, run_id)
store.set_checkpoint(run_id, Checkpoint.SENSITIVITY_COMPLETE)
```
---
Wire `_run_report()` stub:
```python
# Verdict computation
all_wfo_candidates = ranker.rank_by_wfo(store, run_id, top_n=config["sensitivity"]["input_count"])
for cand_record in all_wfo_candidates:
    cid = cand_record["candidate_id"]
    wfo_score = store.get_wfo_consistency_score(cid)
    mc_result = store.get_mc_result(cid, mode=MCMode.DEEP)
    sensitivity = store.get_sensitivity_profile(cid)
    verdict = compute_verdict(cid, wfo_score, mc_result, sensitivity, scenario, oos_gate_enabled)
    store.write_verdict(verdict, run_id)

# Trading YAML for go/borderline candidates
for verdict in store.query_verdicts(run_id):
    if verdict["verdict"] in ("auto_go", "borderline"):
        candidate = _load_candidate(store, verdict["candidate_id"])
        out_path = build_output_path(output_dir, run_id, verdict["candidate_id"])
        yaml_path = generate_trading_yaml(candidate, verdict_obj, run_metadata, base_yaml, out_path)
        store.update_verdict_yaml_path(verdict["candidate_id"], str(yaml_path))

# Report generation
generate_report(store, run_id, scenario, output_dir, formats)
store.set_checkpoint(run_id, Checkpoint.COMPLETE)
```

Add `get_wfo_consistency_score(candidate_id)`, `get_mc_result(candidate_id, mode)`, `get_sensitivity_profile(candidate_id)` as point-reads to `CandidateStore` if not present.
Add `update_verdict_yaml_path(candidate_id, yaml_path)` for post-hoc path setting.
---
---

### Block 2 — Live Integration Test (~45 min)

**`tests/backtesting/integration/test_live_pipeline.py`:**
- Uses real SQLite (tmp_path file)
- Uses real `CandidateStore`
- Mocks `strategy_runner.evaluate()` at module level (returns known-good `CandidateResult` with realistic metrics)
- Runs `orchestrator.run()` or calls each stage function directly with the real store
- Asserts after completion:
  - SQLite: all 9 tables have rows for this `run_id`
  - `runs` table: `checkpoint = 'COMPLETE'`, `completed_at` is not NULL
  - `verdicts` table: at least 1 row with `deployment_status = 'PAPER_TRADE_REQUIRED'`
  - HTML report file exists at `{output_dir}/report_{run_id[:8]}.html`
  - At least 1 trading YAML file exists in `{output_dir}/trading_yamls/`
  - No `datetime.utcnow()` DeprecationWarnings in captured logs
## Block 3 - Phase 5 — Output Layer ⬜
If session not closed go for Phase 5 oncjectives:
**Objective**: All output formats produced correctly.
### Planned Deliverables
| Deliverable | Status | Notes |
|---|---|---|
| `report_generator.py` | ⬜ | Scenario-framed HTML, borderline checklist |
| `yaml_generator.py` | ⬜ | Trading YAML with deployment_status metadata |
| JSON/Parquet export | ⬜ | One file per candidate |
| SQLite query validation suite | ⬜ | 10 queries from SQLITE_SCHEMA.md must pass |
| End-to-end system test | ⬜ | Full pipeline on real WBWS data, all outputs validated |
---
## Output Documents
| Document | Action | Location |
|---|---|---|
| `src/backtesting/orchestrator.py` | Update — wire Stages 5/6/7 | Full implementation |
| `src/backtesting/candidate_store.py` | Update — add missing write/query methods | As needed |
| `tests/backtesting/integration/test_live_pipeline.py` | Create | Live SQLite integration test |
| `CHANGE_LOG.md` | Append SESSION 6 block | Validation results |
| `PROJECT_REPORT.md` | Update Phase 5 status | Operator updates |
| `CONTEXT.md` | Update current phase block | Phase 5 in-progress or complete |

---

## If the Session Runs Long
Priority order if forced to cut short:
1. Blocks 0+1 — audits 
2. Block 2 — Live integration test
3  Block 3 Phase 5 — Output Layer (defer if live integration test takes long)
Always write CHANGE_LOG.md session block and update CONTEXT.md before ending, even if cut short.
---
## Acceptance Criteria for Phase 5 Complete
- [ ] `orchestrator.run()` executes all 8 stages without errors on synthetic data
- [ ] All 9 SQLite tables populated after a complete run
- [ ] HTML report + trading YAML(s) generated
- [ ] `CandidateStore.close()` called in `finally` block
- [ ] Live integration test passes end-to-end with real SQLite
- [ ] report_generator.py` Scenario-framed HTML, borderline checklist
- [ ] `yaml_generator.py` Trading YAML with deployment_status metadata
- [ ] JSON/Parquet export One file per candidate
- [ ] SQLite query validation suite 10 queries from SQLITE_SCHEMA.md must pass
- [ ] End-to-end system test Full pipeline on real WBWS data, all outputs validated |
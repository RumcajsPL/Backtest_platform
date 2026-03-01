# NEXT_SESSION_PLAN.md — Phase 4: Evaluation Layer
**Prepared**: 2026-03-01
**Session goal**: Implement all evaluation modules (sensitivity, verdict, yaml_generator) and the full report generator. Deliver a working end-to-end pipeline integration test through Stage 7 (all stages). Run AV-01 smoke test.

---

## How to Start the Session
1. Open a new chat
2. Paste the **entire content of `CONTEXT.md`** as your first message
3. Add: *"We are starting Phase 4 — Evaluation Layer. Follow the breakdown in NEXT_SESSION_PLAN.md."*
4. Claude reads the `backtester-project` skill automatically
5. Ask Claude to confirm it has read CONTEXT.md, the skill, TECHNICAL_SPEC.md, and SQLITE_SCHEMA.md before writing any code

**Pre-coding reads Claude may request (upload on request):**
- `docs/backtesting/TECHNICAL_SPEC.md` — contracts and module signatures
- `docs/backtesting/SQLITE_SCHEMA.md` — full schema (verdicts + sensitivity tables)
- `docs/backtesting/FUNCTIONAL_SPEC.md` — Stage 6 (sensitivity) and Stage 7 (report) details
- `docs/strategies/architecture/ARCHITECTURE.md` — for yaml_generator integration

---

## Session Objective
At the end of this session we will have:
- All evaluation modules implemented: `evaluation/sensitivity.py`, `evaluation/verdict.py`
- `yaml_generator.py` — trading-ready YAML with embedded deployment_status metadata
- `report_generator.py` — scenario-framed HTML report + adversarial borderline checklist
- Orchestrator fully wired: all 8 stages connected, checkpoint/resume operational end-to-end
- AV-01 smoke test run: random-signal baseline must return `no_go` for all candidates
- Full pipeline integration test (Stages 0–7) passing with synthetic data

---

## Non-Negotiable Before Writing Any Code
The session **must not begin coding** until Claude has confirmed:
1. `SensitivityProfile` and `ParameterSensitivity` contract field names (TECHNICAL_SPEC.md)
2. `VerdictResult` two-pillar logic: WFO consistency score + MC deep ruin probability (FUNCTIONAL_SPEC.md Section 13)
3. `deployment_status: PAPER_TRADE_REQUIRED` — always set for go/borderline, never overridden in code
4. YAML schema for trading-ready output (TECHNICAL_SPEC.md Section 5 + `yaml_generator` signature)
5. `AV-01` smoke test design (SKILL.md Adversarial Challenge Suite)

---

## Work Breakdown

### Block 0 — Sensitivity Evaluator (~60 min)

**`evaluation/sensitivity.py`:**
- Perturb each optimizable parameter at ±1 and ±2 steps from its current value
- One `StrategyRunner.evaluate()` call per perturbation (parallelised via `ProcessPoolExecutor`)
- Compute `fitness_delta = perturbed_fitness - baseline_fitness` for each perturbation
- Build `SensitivityProfile` with `spike_detected`, `spike_parameters`, `profile_complete`
- `profile_complete = False` if >50% of perturbations failed (auto-borderline per spec)
- Use zone parameter definitions from YAML for step size per parameter

**Tests to write:**
- `test_sensitivity_flat_profile`: uniform fitness → `spike_detected=False`
- `test_sensitivity_spike_detection`: one parameter with large delta → `spike_detected=True`, correct `spike_parameters`
- `test_sensitivity_profile_incomplete_flag`: >50% eval failures → `profile_complete=False`
- `test_sensitivity_step_bounds`: ±2 steps never produces out-of-zone parameters

---

### Block 1 — Verdict Engine (~45 min)

**`evaluation/verdict.py`:**
- Two mandatory pillars: `WFOConsistencyScore.composite_score` vs `ScenarioProfile.verdict_go_wfo_floor` and `verdict_borderline_wfo_floor`; `MCResult.ruin_probability` vs `verdict_go_mc_ruin_ceiling` and `verdict_borderline_mc_ruin_ceiling`
- Modifier flags: `sensitivity_spike`, `oos_gate_triggered`, `window_collapse_flag`, `sensitivity_profile_incomplete`
- Verdict logic (exact, from FUNCTIONAL_SPEC.md Section 13):
  - `AUTO_GO`: both pillars pass go thresholds + no modifier flags
  - `BORDERLINE`: either pillar in borderline zone OR any modifier flag set
  - `NO_GO`: either pillar in no_go zone (regardless of modifiers)
- `deployment_status` always `PAPER_TRADE_REQUIRED` for go/borderline — never set to `LIVE_APPROVED`
- `evidence_summary`: plain-language string summarising which pillars passed/failed and why

**Tests to write:**
- `test_verdict_auto_go`: both pillars pass, no flags → `AUTO_GO`
- `test_verdict_no_go_wfo`: WFO below borderline floor → `NO_GO`
- `test_verdict_no_go_mc`: ruin prob above borderline ceiling → `NO_GO`
- `test_verdict_borderline_spike`: both pillars pass but spike_detected → `BORDERLINE`
- `test_verdict_borderline_wfo_zone`: WFO in borderline band → `BORDERLINE`
- `test_verdict_deployment_status_always_paper`: go verdict → `PAPER_TRADE_REQUIRED`
- `test_evidence_summary_not_empty`: all verdict paths produce non-empty evidence_summary

---

### Block 2 — YAML Generator (~30 min)

**`yaml_generator.py`:**
- Merge candidate parameters into base `strategy_template.yaml`
- Embed metadata section: `scenario_name`, `run_id`, `config_hash`, `generated_at`, `deployment_status: PAPER_TRADE_REQUIRED`
- Validate output is parseable as `StrategyConfig` before writing
- Return the written file path (stored in `VerdictResult.yaml_output_path`)
- Named: `{run_id[:8]}_{candidate_id[:12]}_strategy.yaml` in `output_dir/trading_yamls/`

**Tests to write:**
- `test_yaml_generator_produces_valid_file`: output parses as StrategyConfig
- `test_yaml_metadata_embedded`: run_id, scenario, deployment_status present in output YAML
- `test_yaml_deployment_status_paper`: always `PAPER_TRADE_REQUIRED` regardless of verdict

---

### Block 3 — Report Generator (~90 min)

**`report_generator.py`:**
- Reads entirely from `CandidateStore` — no raw data passed in
- Jinja2 HTML template: single self-contained file, no external CSS/JS dependencies
- Scenario-framed: `report_emphasis` field from `ScenarioProfile` controls metric ordering
- Sections: run summary → pipeline funnel → ranked shortlist → per-candidate detail → verdict evidence
- Per-candidate charts (inline): equity curve, WFO window bar chart, MC distribution histogram, sensitivity delta heatmap
- Use `matplotlib` with `Agg` backend → embed as base64 PNG in HTML (no file deps)
- **Adversarial checklist**: separate HTML file per borderline candidate; structured sign-off fields

**JSON/Parquet output:**
- One file per candidate, all stage results flattened
- JSON via `json.dumps`, Parquet via `pandas.DataFrame.to_parquet`
- Skip format if `output.formats.json/parquet: false` in config

**Tests to write:**
- `test_html_report_generated`: file exists, non-empty, `<html>` tag present
- `test_report_scenario_emphasis_order`: `capital_accumulation` → wfo_consistency appears before expectancy
- `test_borderline_checklist_generated`: borderline candidate → checklist HTML file created
- `test_json_parquet_output`: both files created, JSON parseable, Parquet readable

---

### Block 4 — Orchestrator Final Wiring (~45 min)

**`orchestrator.py` — complete all stage stubs:**
- Wire Stage 5 (MC Deep): `rank_by_wfo` → `run_mc(mode=DEEP)` per candidate → write `MCResult`
- Wire Stage 6 (Sensitivity): `evaluate_sensitivity` per top candidate → write `SensitivityProfile`
- Wire Stage 7 (Report): `compute_verdict` per candidate → `generate_report` → `generate_trading_yaml` for top go/borderline
- Ensure checkpoint written at each stage completion
- Ensure `CandidateStore.close()` called in `finally` block of `run()`

---

### Block 5 — AV-01 Smoke Test + Full Integration (~45 min)

**`tests/backtesting/integration/test_av01_random_baseline.py`:**
- Fixture: strategy config where signals are randomised (use a known-bad parameter set or mock)
- Run full pipeline Stages 0–4
- Assert: all candidates at Stage 4 have `wfo_consistency_score < verdict_go_wfo_floor` OR `ruin_probability > verdict_go_mc_ruin_ceiling`
- Assert: no candidate receives `AUTO_GO` verdict

**`tests/backtesting/integration/test_full_pipeline_e2e.py`:**
- Full Stages 0–7 with synthetic data (mocked strategy runner returning known-good results)
- Assert: SQLite has rows in all 9 tables, HTML report file exists, trading YAML file generated
- Assert: checkpoint state = `COMPLETE` at end

---

## Output Documents
| Document | Action | Location |
|---|---|---|
| `src/backtesting/evaluation/sensitivity.py` | Create | `src/backtesting/evaluation/` |
| `src/backtesting/evaluation/verdict.py` | Create | `src/backtesting/evaluation/` |
| `src/backtesting/yaml_generator.py` | Create | `src/backtesting/` |
| `src/backtesting/report_generator.py` | Create | `src/backtesting/` |
| `src/backtesting/orchestrator.py` | Update | Wire all remaining stages |
| `tests/backtesting/unit/test_sensitivity.py` | Create | Unit tests |
| `tests/backtesting/unit/test_verdict.py` | Create | Unit tests |
| `tests/backtesting/unit/test_yaml_generator.py` | Create | Unit tests |
| `tests/backtesting/unit/test_report_generator.py` | Create | Unit tests |
| `tests/backtesting/integration/test_av01_random_baseline.py` | Create | AV-01 smoke test |
| `tests/backtesting/integration/test_full_pipeline_e2e.py` | Create | Full pipeline integration test |
| `CHANGE_LOG.md` | Append SESSION 5 block | Validation results, AV-01 outcome |
| `PROJECT_REPORT.md` | Update Phase 4 status | Operator updates |
| `CONTEXT.md` | Update current phase block | Phase 4 in-progress or complete |

---

## If the Session Runs Long
Priority order if forced to cut short:
1. `evaluation/sensitivity.py` + `evaluation/verdict.py` — core evaluation logic, must complete
2. `yaml_generator.py` — required for pipeline completeness
3. Orchestrator final wiring — must connect all stages
4. `report_generator.py` — defer HTML chart generation if time short; plain-text report acceptable for Phase 4
5. AV-01 smoke test — run at minimum manually; formal test can be written in Phase 5
6. Full e2e integration test — defer if needed

Always write CHANGE_LOG.md session block and update CONTEXT.md before ending, even if cut short.

---

## Acceptance Criteria for Phase 4 Complete
- [ ] `SensitivityProfile` built correctly: spike detected, profile_complete flag accurate
- [ ] Verdict engine: correct outcome for all 3 verdict paths with all modifier combinations
- [ ] `yaml_generator.py`: output validates as StrategyConfig, metadata embedded
- [ ] HTML report generated: self-contained, scenario-framed, borderline checklist present
- [ ] Orchestrator: all 8 stages wired, checkpoint/resume tested
- [ ] AV-01: random-signal candidates all receive `NO_GO` verdict
- [ ] Full pipeline integration test passes: all 9 SQLite tables populated, report + YAML generated
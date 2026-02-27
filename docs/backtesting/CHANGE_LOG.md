# CHANGE_LOG.md
## Backtesting & Optimization Framework

**Purpose**: Records all plan modifications, session handoffs, resolved decisions, and deferred concerns.
**Rule**: Every session that modifies BACKTESTER_PLAN.md or makes a design decision **must** append a SESSION block here before closing.

---

## How to Use This File

**End of every session** — append a SESSION block (copy the template below).
**Start of every session** — read the LAST SESSION block only. That is your handoff.
**Decision resolved** — add to the session block AND strike through in CONTEXT.md.
**Concern spotted** — log it in the session block under CONCERNS. Do not act on it unless explicitly instructed.

---

## SESSION TEMPLATE
```
## SESSION [number] — [date]
**Phase**: [current phase]
**Duration**: [approximate]

### Completed This Session
- [specific deliverable or decision]

### Plan Changes
| Section | Change | Reason |
|---|---|---|
| [e.g. Section 7] | [what changed] | [why] |

### Decisions Resolved
| ID | Decision | Resolution | Rationale |
|---|---|---|---|
| [D-xx] | [topic] | [what was decided] | [why] |

### Deferred Concerns
<!-- Things spotted but not acted on. Revisit in future sessions. -->
- [CONCERN] [description] — flagged for [phase/session]
- [OPPORTUNITY] [description] — potential future enhancement

### Handoff — Start Next Session With
<!-- The exact task for the next session, copy this into CONTEXT.md's NEXT TASK field -->
**Next task**: [specific, actionable]
**Context needed**: [which files to read, which decisions are blocking]
**Acceptance criteria**: [how to know the next task is done]
```

---

## SESSION 1 — 2026-02-27
**Phase**: Requirements & Planning (pre-Design)
**Duration**: ~2 hours (brainstorming + document production)

### Completed This Session
- Conducted full requirements Q&A session (9 question clusters)
- Produced `BACKTESTER_PLAN.md` v1.0 — 14 sections, 47 requirements
- Corrected evidence pillars to two mandatory pillars (MC robustness + WFO temporal consistency)
- Added scenario-based backtesting concept (Section 2.4, Section 4.10)
- Revised pipeline sequence: Random → MC Pre-Filter → GA (WFO-aware) → Full WFO → MC Deep → Sensitivity → Report
- Added Section 1b: Future Platform Context (eToro API, 4-layer roadmap)
- Added statistical significance guard as a formal pipeline gate
- Added parameter sensitivity map as Stage 6
- Produced `CONTEXT.md`, `CHANGE_LOG.md`, `PROJECT_REPORT.md`, `PROJECT_SKILL.md`

### Plan Changes
| Section | Change | Reason |
|---|---|---|
| 2.1 | Evidence pillars reduced from 4 to 2 | Stakeholder correction — only MC robustness and multi-period WFO required |
| 2.2 | Verdict model sharpened | Two pillars, not four; borderline = one pillar inconclusive |
| 4.6 | WFO requirements rewritten | Temporal consistency focus; IS/OOS delta informational only |
| 7 (entire) | Pipeline sequence revised | Industry practice challenge: GA-before-WFO wastes generations; MC pre-filter eliminates fragile candidates cheaply |
| New: 1b | Future Platform Context added | Long-term eToro API vision recorded without becoming v1 scope |
| New: 2.4 | Scenario-based backtesting added | Capital accumulation vs swing trading vs conservative — intention-driven runs |
| New: 4.10 | Scenario requirements added | 8 requirements for scenario system |
| New: Stage 2 | MC Pre-Filter added to pipeline | Cheap fragility screen before expensive GA |
| New: Stage 6 | Parameter Sensitivity Map added | Flat landscape = robust deployment; spike = borderline flag |
| 8 | Module list updated | Added scenario.py, evaluation/sensitivity.py; updated ga_engine, strategy_runner, verdict responsibilities |
| 12 | Open decisions expanded from 8 to 10 | D-05 (GA window selection), D-08 (sensitivity scope) added |
| 13 | Project phases resequenced | WFO built before GA (GA depends on it); sensitivity added to Phase 4 |
| 14 | Risk R-05 added | GA WFO-aware fitness — highest new runtime risk from revised pipeline |

### Decisions Resolved
None — all open decisions (D-01 through D-10) remain open and require Design phase work.

### Deferred Concerns
- [CONCERN] GA WFO-aware fitness cost (Risk R-05) — 1,200+ strategy runs inside GA phase alone. Must benchmark in Phase 3 before accepting the design. If over budget, primary lever is reducing to 1 WFO window inside GA.
- [CONCERN] SQLite WAL mode concurrency under 6 workers on Windows — untested in this environment. Prototype required early in Phase 2.
- [OPPORTUNITY] Market regime tagging for WFO windows (trend/range/volatile) — would make temporal consistency evidence stronger. Schema should support it even if tagging is manual in v1. Flag for Phase 1 schema design.
- [OPPORTUNITY] MC pre-filter could evolve into a lightweight "strategy health check" runnable independently of the full pipeline — useful for quick sanity checks on hand-tuned configs.
- [CONCERN] eToro API maturity unknown — the live trading layer depends on the API being stable and supporting the required order types. No action in v1, but worth monitoring.

### Handoff — Start Next Session With
**Next task**: Design Phase — produce functional and technical specification
**Context needed**: Read BACKTESTER_PLAN.md Sections 8, 9, 12. Open decisions D-01 and D-02 are the blockers that require prototype benchmarks.
**Acceptance criteria**: All 10 open decisions resolved; all inter-module contracts defined as frozen dataclasses; SQLite schema fully specified (all tables, columns, foreign keys); `backtest_template.yaml` schema fully specified

---
---

## SESSION 2 — 2026-02-27
**Phase**: Phase 1 — Design
**Session type**: Full design session
**Duration**: Single session
**Status at close**: Phase 1 Design COMPLETE

---

### Part A — Independent Opinion Review

Before starting the NEXT_SESSION_PLAN.md work, an adversarial independent opinion (`INDEPENDENT_OPINION.md`) was reviewed against `BACKTESTER_PLAN.md`. The reviewer had sound instincts but incomplete context on the operator model (single PC, 4-hour physical constraint). A definitive position was taken on each point. Decisions are final unless explicitly re-opened by the operator.

**Accepted — incorporated into BACKTESTER_PLAN.md v1.2:**

| Item | Change Made |
|---|---|
| GA WFO window sampling | D-05 resolved: random sample 2 windows per generation from full list (not fixed pair). New requirement GA-06. |
| GA diversity penalty | New Should-Have requirement GA-07. New module `ga/diversity.py`. |
| WFO consistency score | Redefined as composite of 4 orthogonal metrics (median return, variance, worst drawdown, fraction positive). New module `wfo/consistency_scorer.py`. WF-04 rewritten. |
| Deployment gate | `VerdictResult.deployment_status` field added (default `PAPER_TRADE_REQUIRED`). Embedded in trading-ready YAML metadata. |
| Adversarial challenge suite | New Section 4.11 (AV-01 through AV-05). Required for Phase 6 delivery. AV-01 smoke test recommended at end of Phase 4. |
| Config freeze enforcement | Architecture Principle 10 added — Immutable Run Artifacts. New requirement CS-07. |
| IS/OOS gate | WF-06 updated: informational by default; optional `enforce_oos_gate: true` YAML flag makes >50% degradation a borderline flag (never auto-reject). |
| Post-Stage-1 adequacy warning | WF-09 added: orchestrator logs statistical adequacy warning after Random Search if MC/WFO config appears weak. Warning only — not a gate. |
| Paper trade deployment gate | `deployment_status: PAPER_TRADE_REQUIRED` on all go/borderline verdicts. Operator sets `LIVE_APPROVED` after paper trading period. |
| Borderline adversarial checklist | Report generator produces checklist template for every borderline candidate. No borderline candidate deployable without operator sign-off. |

**Accepted with modification:**

| Item | Modification |
|---|---|
| IS/OOS degradation as gating criterion | Accepted as opt-in via YAML flag only. Default off. >50% degradation = borderline (never auto-reject) when enabled. IS/OOS remains informational by default — reviewer wanted mandatory gate, which contradicts the WFO's primary role as temporal consistency evidence. |
| Global sensitivity random-walk | Accepted concern; resolved as sensitivity step range expanded to ±2 steps (was ambiguous). True global random-walk is v2 scope. |

**Rejected — with rationale recorded:**

| Item | Rationale |
|---|---|
| 4-hour cap as soft performance target | Physical constraint for single-PC retail operator. Not negotiable. |
| Regime-aware MC perturbation profiles | Requires regime classification infrastructure not in scope for v1. Logged as future enhancement. |
| Pre-run statistical power analysis as gate | Cannot power-analyse before trade data exists (Stage 1 produces it). Accepted as post-Stage-1 warning log only. |
| Single-number WFO consistency metric criticism | Mischaracterised existing design — already composite. Clarified in TECHNICAL_SPEC.md. |

**New open decisions added as a result of review:**
- D-11: GA diversity penalty distance metric (Euclidean / Hamming / hybrid)
- D-12: IS/OOS gate default configuration (on/off; default threshold)

---

### Part B — BACKTESTER_PLAN.md Updated to v1.2

Full update produced incorporating all accepted changes. Version history block appended to document. D-05 struck through in Section 12. D-11 and D-12 added as new open decisions.

---

### Part C — NEXT_SESSION_PLAN.md Executed in Full

All 6 blocks completed in sequence within this session.

**Block 1 — All 12 Open Decisions Resolved:**

| Decision | Resolution |
|---|---|
| D-01 Strategy integration mode | Direct Python call in isolated worker process. Benchmark 50 candidates in Phase 2. |
| D-02 SQLite write concurrency | WAL mode + single-writer queue (workers submit to queue; one writer thread drains). Benchmark 500 writes in Phase 2. |
| D-03 Temporary YAML lifecycle | Per-candidate, named by parameter hash. Deleted in `finally`. `retain_temp_yamls: true` optional for debugging. |
| D-04 GA population seeding | Top-N by fitness from MC_PREFILTER_PASS. Diversity handled by penalty during evolution. |
| D-05 GA WFO window selection | Randomly sample 2 from full window list per generation. Min 3 windows required. *(Already resolved in Part A.)* |
| D-06 Stage transition counts | Random 200/zone → MC Pre-filter top 120 → GA pop 60/30 gen → Full WFO top 30 → MC Deep top 10 → Sensitivity top 5. All YAML-configurable. |
| D-07 Verdict thresholds | WFO: go ≥0.65, borderline 0.40–0.65, no_go <0.40. MC: go ≤5%, borderline 5–15%, no_go >15%. Scenario-specific variants defined. Calibrate in Phase 6. |
| D-08 Sensitivity map scope | All optimizable parameters. ~300 evaluations, ~200s at 6 workers. |
| D-09 Parquet vs JSON | Both, both enabled by default, independently disableable via YAML. |
| D-10 HTML report generator | Build new. Structurally too different from single-run strategy report to extend. |
| D-11 GA diversity distance metric | Hybrid: normalised Euclidean for continuous params, Hamming for discrete params, weighted average. |
| D-12 IS/OOS gate default | Off by default (`enforce_oos_gate: false`). When enabled: >50% degradation = borderline flag. |

**Block 2 — All 11 Contracts Defined:**

All contracts defined as production-ready Python frozen dataclasses with `__post_init__` validation. Defined in `TECHNICAL_SPEC.md`:

- `RunMetadata` — run_id (UUID), config_hash (SHA-256), scenario_name, started_at, perturbation_profile_name, 5 seeds, wfo_window_ids tuple (min 3), checkpoint, backtester_version
- `ScenarioProfile` — 6 fitness weights (validated sum=1.0), 6 constraint thresholds, mc_prefilter threshold, 4 WFO temporal weights (validated sum=1.0), 5 verdict thresholds, report_emphasis tuple
- `CandidateParameterSet` — zone_name, parameters dict, candidate_id (SHA-256 of params), generation. Must use `.create()` factory.
- `CandidateResult` — candidate_id, evaluated_at, metrics (Optional), trades (Optional), total_trades (Optional), error (Optional)
- `FitnessResult` — candidate_id, scenario_name, fitness_score (Optional), passed_constraints, rejection details, all 6 constraint actuals
- `WFOWindow` — window_id, start_date, end_date
- `WFOWindowResult` — candidate_id, window_id, evaluated_at, fitness_score, key metrics, oos_delta, error
- `WFOConsistencyScore` — candidate_id, 4 sub-metrics, composite_score [0,1], windows_evaluated, windows_total, oos_gate_triggered, window_collapse_flag
- `MCResult` — candidate_id, mode (MCMode enum), profile_name, iterations, avg_final_equity, worst_drawdown_across_paths, ruin_probability, p5_final_equity, error
- `SensitivityProfile` — candidate_id, baseline_fitness, tuple of `ParameterSensitivity`, spike_detected, spike_parameters, profile_complete
- `VerdictResult` — candidate_id, verdict (Verdict enum), deployment_status (always PAPER_TRADE_REQUIRED for go/borderline), both pillar scores, 4 modifier flags, informational fields, evidence_summary, yaml_output_path
- `CandidateRecord` — flattened SQLite row, all fields as primitives, `parameters_json` for audit backup

**Block 3 — SQLite Schema Designed (9 tables):**

All tables with `CREATE TABLE` statements, foreign keys, and indexes. 10 representative queries including ML feature matrix query. Schema is ML-ready from day one.

| Table | Purpose |
|---|---|
| `runs` | One row per run. Immutable artifacts (config_hash, 5 seeds, perturbation_profile_name). |
| `candidates` | One row per unique candidate_id. |
| `candidate_parameters` | All parameter values as individual columns + JSON backup. |
| `evaluations` | One row per candidate per stage. All constraint actuals + fitness. |
| `wfo_window_results` | One row per candidate per window (GA lightweight + full WFO distinguished by flag). |
| `wfo_consistency_scores` | Four sub-metrics + composite score per candidate. |
| `mc_results` | Pre-filter and deep as separate rows. |
| `sensitivity_results` | One row per candidate per parameter per step. |
| `sensitivity_profiles` | Summary (spike_detected, spike_parameters) per candidate. |
| `verdicts` | Final verdict + full evidence + deployment_status per candidate. |

**Block 4 — `backtest_template.yaml` Schema Specified:**

All valid keys with types and defaults documented in `TECHNICAL_SPEC.md` Section 5. Sections: `backtester_version`, `scenario`, `run`, `stages`, `random_search`, `mc_prefilter`, `genetic`, `walk_forward`, `monte_carlo`, `sensitivity`, `output`, `perturbation_profiles`, `scenarios`, `zones`.

**Block 5 — Three Scenario Profiles Defined with Concrete Values:**

| Scenario | Focus | Key constraints | WFO go | MC go |
|---|---|---|---|---|
| `capital_accumulation` | Win rate + consistency | max_dd 15%, min_wr 45%, min_freq 3/wk | ≥0.65 | ≤5% |
| `swing_trading` | Expectancy + profit factor | max_dd 20%, min_expect 0.8, min_pf 1.5 | ≥0.60 | ≤7% |
| `conservative` | Capital preservation | max_dd 10%, min_wr 52%, max_streak 5 | ≥0.70 | ≤3% |

**Block 6 — Remaining Decisions Resolved:** D-06 through D-12 (D-06–D-10 from original plan, D-11–D-12 new). All documented above.

---

### Part D — Documents Produced This Session

| Document | Version | Location | Notes |
|---|---|---|---|
| `BACKTESTER_PLAN.md` | v1.2 | `docs/backtesting/` | +Section 4.11 adversarial, +Principle 10, all accepted review items |
| `FUNCTIONAL_SPEC.md` | v1.0 | `docs/backtesting/` | New — all 8 stages in plain language |
| `TECHNICAL_SPEC.md` | v1.0 | `docs/backtesting/` | New — all contracts, all decisions, YAML schema, scenario profiles |
| `SQLITE_SCHEMA.md` | v1.0 | `docs/backtesting/` | New — 9 tables, CREATE TABLE, indexes, 10 queries |
| `CONTEXT.md` | Updated | `docs/backtesting/` | All decisions struck through, all contracts checked, Phase 2 starting order |

---

### Part E — Risk Register Updates

| ID | Update |
|---|---|
| R-01 | Integration mode resolved (direct call). Benchmark still required in Phase 2 before full implementation. Status: 🟡 Benchmark pending. |
| R-02 | SQLite concurrency resolved (WAL + writer queue). Benchmark still required in Phase 2. Status: 🟡 Benchmark pending. |
| R-05 | GA WFO-aware fitness runtime risk acknowledged. Random window sampling adds negligible overhead. Primary mitigation: profile in Phase 3. Status: 🔴 Watch. |
| R-08 | SQLite schema reviewed and ML-ready design confirmed. Status: ✅ Resolved. |
| R-09 | NEW — GA diversity penalty miscalibration. Penalty weight is YAML-configurable. Observable via CandidateStore parameter spread monitoring. Status: 🟡 Open. |
| R-10 | NEW — Adversarial challenge suite finding structural flaw late. Mitigation: run AV-01 smoke test at end of Phase 4 before output layer. Status: 🟡 Open. |

---

### Part F — Deferred Items

| ID | Type | Description | Target |
|---|---|---|---|
| DC-06 | Future enhancement | Regime-aware MC perturbation profiles | v2 |
| DC-07 | Future enhancement | True global parameter sensitivity random-walk | v2 |
| DC-08 | Concern | D-01 benchmark (strategy integration mode speed) | Phase 2 — first task |
| DC-09 | Concern | D-02 benchmark (SQLite WAL + writer queue under 6-worker load) | Phase 2 — second task |

---

### Session Handoff

```
COMPLETED THIS SESSION:
  Phase 1 — Design fully complete.
  All 12 decisions resolved. All 11 contracts defined. SQLite schema complete.
  YAML schema complete. All 3 scenario profiles defined.

NEXT SESSION START:
  Phase 2 — Core Infrastructure.
  First task: candidate_store.py implementation + D-02 benchmark.
  Second task: strategy integration D-01 benchmark (50 candidates, direct-call mode).
  Read TECHNICAL_SPEC.md + SQLITE_SCHEMA.md before writing any code.

NOTHING IS BLOCKED.
  All design decisions are made. Implementation can begin immediately.
```
<!-- APPEND NEW SESSION BLOCKS BELOW THIS LINE -->
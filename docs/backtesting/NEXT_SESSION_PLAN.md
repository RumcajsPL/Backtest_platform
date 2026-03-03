# NEXT SESSION PLAN — Block 6: Final Documentation
## Status entering this session
- Phase 6, Blocks 0–5 complete. 233 tests green.
- ARCHITECTURE.md v1.1.0 updated. One gap remains (see below).
- Block 6 is documentation only. No new tests, no code changes.
---
## First upload — before anything else
**`configs/backtesting/backtest_template.yaml`**
Required to:
1. Replace `e2e_test` threshold placeholder values in ARCHITECTURE.md Section 8
   verdict grid with real `capital_accumulation` production values.
2. Verify stage counts in Section 3 diagram (200/zone, 60 pop × 30 gen, etc.).
3. Drive content for the Operator Runbook (stage input/output counts, seeds,
   perturbation profile names, WFO window definitions).
---
## Documents to upload and update (in order)

| Order | File | What to update |
|---|---|---|
| 1 | `configs/backtesting/backtest_template.yaml` | Source — drives all below |
| 2 | `docs/backtesting/architecture/ARCHITECTURE.md` | Section 8 production thresholds, Section 3 stage counts |
| 3 | `docs/backtesting/TECHNICAL_SPEC.md` | D-07 boundary operators, Windows spawn note |
| 4 | `docs/backtesting/FUNCTIONAL_SPEC.md` | Verify stages match implementation |
| 5 | `docs/backtesting/BACKTESTER_PLAN.md` | Mark Phase 6 complete, add lessons learned |
| 6 | `docs/backtesting/PROJECT_REPORT.md` | Update phase rows, test count to 233 |
Upload each file before editing it. Confirm SKILL.md read and CONTEXT.md understood before starting.
---
## ARCHITECTURE.md — remaining gap (Section 8)
Current state: verdict grid shows e2e_test scenario values (0.55 / 0.40 / 0.10 / 0.25).
Needed: replace with capital_accumulation production values from backtest_template.yaml.
Note already in document: "recalibrate after the first real run" — keep this note,
just update the displayed values to match the YAML defaults.
---
## TECHNICAL_SPEC.md — specific changes needed
- Decision D-07 (verdict thresholds): add confirmed boundary operators:
    wfo_pillar_go    = composite >= go_floor       (>= INCLUSIVE — not >)
    mc_pillar_go     = ruin_prob <= go_ceiling      (<= INCLUSIVE — not <)
    ruin_prob = None → NO_GO (mc_pillar_no_go=True, not BORDERLINE)
    oos_gate requires BOTH oos_gate_enabled=True AND wfo_score.oos_gate_triggered=True
- Add note on Windows spawn mock patching constraint (cross-reference ARCHITECTURE.md §9).
---
## FUNCTIONAL_SPEC.md — verification checklist
- Stage 6: confirm profile_complete=False path documented (>50% evals fail →
  sensitivity_profile_incomplete modifier → BORDERLINE demotion).
- Stage 7: confirm ruin_probability=None → NO_GO path documented.
- Stage 5: confirm run_mc "never raises" contract documented.
- Stage 0: confirm checkpoint resume logic described correctly for all 8 values.
---
## BACKTESTER_PLAN.md — additions
- Mark Phase 6 Hardening & Delivery as complete.
- Add lessons learned section:
    L-01: Windows spawn mode — mock patches don't cross worker boundary.
          Patching at orchestrator level is the correct isolation point.
    L-02: Verdict boundary operators must be >= / <= (inclusive) at go thresholds.
          Using > / < would incorrectly classify boundary-exact scores as BORDERLINE.
    L-03: Stage 6 is the dominant runtime cost (333–446s).
          Pool reuse (OPT-01) is the highest-value optimisation available.
    L-04: Config fixture shape for tests must match load_scenario() nested structure.
          Flat dicts fail at KeyError — always use nested fitness_weights, constraints, etc.
---
## New file: Operator Runbook (first read docs\backtesting\BACKTESTER_USER_GUIDE.md to keep any useful context )
`docs/backtesting/OPERATOR_RUNBOOK.md`
Sections:
1. Pre-run checklist
   - Config hash verification
   - Scenario selection (capital_accumulation vs custom)
   - WFO window date review
   - Seed documentation (record all 5 seeds before launching)
2. Launching a run
   python -m src.backtesting.orchestrator configs/backtesting/backtest_template.yaml
3. Monitoring progress
   - Checkpoint log lines to watch
   - Expected stage durations (from Block 3 baseline)
   - How to query current checkpoint from SQLite
4. Expected outputs per stage
   - Stage 1: N candidates written (N = zones × random_search count)
   - Stage 2: MC prefilter results, expect ~X% passing ruin threshold
   - Stage 3: GA evolution candidates (60 pop × 30 gen per zone)
   - Stage 4: WFO consistency scores for top 30
   - Stage 5: MC deep results for top 10 by WFO score
   - Stage 6: Sensitivity profiles for top 5 by WFO score
   - Stage 7: Verdicts + HTML report + trading YAMLs for go/borderline
5. Reading the verdict output
   - AUTO_GO: both pillars pass, no modifier flags — ready for paper trading
   - BORDERLINE: borderline zone on one or both pillars, or at least one flag
   - NO_GO: one or both pillars in no-go zone — do not trade
6. Promotion path
   PAPER_TRADE_REQUIRED is always the initial status.
   LIVE_APPROVED is a manual operator action — never set by code.
   Minimum paper trading period before promotion: operator decision.
7. Resume after interruption
   All 8 checkpoints are safe interruption points (verified Block 4).
   Simply re-run with the same config — pipeline resumes from last checkpoint.
8. Performance tuning reference (OPT-01 to OPT-05 summary)
---
## Pass criteria for Block 6
All 6 documents updated and internally consistent.
Operator runbook peer-reviewed (readable by a new operator with no prior context).
No regressions — all 233 tests still green after any incidental fixes.
---
## After Block 6 Completes
1. Update CONTEXT.md: Block 6 done, Block 7 next.
2. Append to CHANGE_LOG.md.
3. Write NEXT_SESSION_PLAN.md for Block 7 (OPT-01 + OPT-02).
4. Update PROJECT_SKILL.md — documentation complete flag.
---
## Block 7 Preview — OPT-01 + OPT-02
Pool reuse + batching in evaluate_sensitivity().
Files to upload: src/backtesting/evaluation/sensitivity.py + test_performance.py.
Re-run test_performance.py after changes — baseline must not regress.
Expected: 40–60% Stage 6 reduction.
Do not start until Block 6 is fully closed.
```
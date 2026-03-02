# NEXT_SESSION_PLAN.md — Phase 6 Continuation
**Prepared**: 2026-03-02
**Session goal**: start Block 2 adversarial suite if time permits

---
## How to Start
1. Open new chat, paste `CONTEXT.md` as first message
2. Add: *"Phase 6 in progress, follow NEXT_SESSION_PLAN.md"*
3. **Upload**: tests\backtesting\integration\test_e2e_wbws_real_data.py (pipline test used in the session). Other files to be decided by Claude
4. Confirm skill read, CONTEXT.md understood
---
## DONE: Block 0 — Fix E2E Test (Priority 1)
---
## DONE Block 1 — Strategy Parameter Mapping Audit (Priority 2)
---
## START FROM HERE: Block 2 — Adversarial Suite

### AV-02: Overfit-injection test
- Create a deliberately curve-fit parameter set (parameters optimized on the exact
  training window, expected to collapse on WFO windows)
- Pipeline must return borderline or no_go — not auto_go
- Implementation: inject a candidate with suspiciously high in-sample fitness,
  verify WFO consistency score is low

### AV-03: Verdict stability under seed perturbation
- Run the same 5-candidate seed set with 3 different random seeds
- At least 80% of verdicts must be identical across runs
- Implementation: run e2e fixture 3 times with different seeds, compare verdict dicts

---
## Output Documents This Session
| Document | Action |
|---|---|
| `tests/backtesting/integration/test_e2e_wbws_real_data.py` | Fix + all 13 green |
| `src/backtesting/fitness.py` | Fix if root cause found there |
| `src/backtesting/strategy_runner.py` | Update _PARAM_KEY_MAP after Block 1 audit |
| `configs/backtesting/backtest_template.yaml` | Update zones after Block 1 audit |
| `CHANGE_LOG.md` | Append SESSION 8 block |
| `CONTEXT.md` | Update phase status |
| `NEXT_SESSION_PLAN.md` | Block 2+ plan |
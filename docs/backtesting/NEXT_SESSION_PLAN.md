# NEXT_SESSION_PLAN.md — Phase 5 Completion: Fix test_report_yaml + datetime cleanup
**Prepared**: 2026-03-01
**Session goal**: Fix 10 failing tests in test_report_yaml.py, apply report_generator.py bug fix, run datetime.utcnow() cleanup, confirm 162+ tests green, close Phase 5.

---
## How to Start
1. Open new chat, paste `CONTEXT.md` as first message
2. Add: *"Phase 5 closed. End-to-end test on real WBWS data to create and perform| 
 Phase 6 | Follow NEXT_SESSION_PLAN.md."*
3. **Upload** `test_report_yaml.py` + pytest failure output
4. Confirm skill read, CONTEXT.md understood, failing tests identified — before writing any code
---
## Block 3 — Phase 0 kickoff (if time permits)

See CONTEXT.md Phase 6 Starting Point. Priority order:
1. <real wbws data test>.py test created
2. AV-02 overfit-injection test
3. `datetime.utcnow()` cleanup if not done in Block 1 (No more warning in full pipeline tests so apparently all is fixed)
4. Performance profiling scaffold

---
## Output Documents This Session
| Document | Action |
|---|---|
| `tests/backtesting/integration/<real wbws data test>.py` | to create |
| `CHANGE_LOG.md` | Append SESSION 7 block |
| `CONTEXT.md` | Update phase status to Phase 5 complete / Phase 6 in progress |
| `PROJECT_REPORT.md` | Operator updates phase status |
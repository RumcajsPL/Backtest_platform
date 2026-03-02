# NEXT_SESSION_PLAN.md — Phase 5 to 6 trasnistion
**Prepared**: 2026-03-01
**Session goal**: End-to-end system test passed, Phase 6 organized in logical blocks, if possible phase 6 started
---
## How to Start
1. Open new chat, paste `CONTEXT.md` as first message
2. Add: *"Phase 5 closed and passing to Phase 6, Follow NEXT_SESSION_PLAN.md."*
3. **Upload** `tests\backtesting\benchmarks\bench_d01_strategy_speed.py` - exemple - early dev script (orchestrator not yet fully wired) but running on backtesting orchestrator on real data (strategy .yaml used). Script passes < 30s. Can be analysed for imports, file location etc. 
4. Confirm skill read, CONTEXT.md understood, 
5. Ask other documentation, scripts if required
---
## Block 0 — Phase 5 => 6 
1. <real wbws real data test>.py End-to-end system test created: full pipeline run on real WBWS data
2. Test passed => all outputs produced and validated

### Phase 6 — Hardening and Delivery (organize in logical blocks first, then start if any can be completed during the session)
**Deliverables**: Full test suite including adversarial challenge harness, performance validation report, Windows compatibility certification, complete documentation
**Key activities**:
- **Adversarial challenge suite** (required for delivery):
  - AV-01: Random-signal baseline — signals replaced with coin flips, pipeline must return no-go
  - AV-02: Overfit-injection test — curve-fit strategy must be flagged borderline or auto-rejected
  - AV-03: Meta-config stability — >80% verdict stability under seed and iteration perturbation
- Validate full pipeline completes within 4-hour target on target hardware
- Profile and resolve bottlenecks if over budget (tuning levers: sample counts, MC iterations, stage transition candidate counts)
- Validate resume-after-interruption at each of the 8 checkpoints
- Validate parallel worker isolation: kill one worker mid-run, confirm pipeline continues
- Calibrate verdict thresholds against first real run results (D-07)
- Final documentation: module reference, YAML configuration guide, scenario authoring guide, output format guide, SQLite query cookbook, paper trading protocol
---
## Output Documents This Session
| Document | Action |
|---|---|
| `tests/backtesting/integration/<real wbws data test>.py` | to create |
| `CHANGE_LOG.md` | Append SESSION 7 block |
| `CONTEXT.md` | Update phase transision status / Phase 6 in progress |
| `NEXT_SESSION_PLAN.md` | Phase 6 plan |
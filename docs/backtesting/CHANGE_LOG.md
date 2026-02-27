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
<!-- APPEND NEW SESSION BLOCKS BELOW THIS LINE -->
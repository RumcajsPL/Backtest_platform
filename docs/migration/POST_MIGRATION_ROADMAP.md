# POST-MIGRATION ROADMAP — WBWSStrategy
**Status**: Living document — updated each session  
**Last updated**: 2026-02-17 (Session 18)  
**Structure**: This index + 5 chapter files (load chapters only when working on that area)
> **Leitmotif**: Single Responsibility · Performance-first · Explicit Contracts ·
> Type Safety · Production Readiness. See `DECISION_LOG.md` preamble for the full
> statement. Every item in this roadmap must serve at least one of these principles.
---
## Context
The migration project (Sessions 1–~24) delivers a production-ready backtesting engine.
Everything below is **explicitly deferred** — not needed to reach production, but
represents natural next steps once the core system is stable.
Migration completes at Session ~24. Post-migration work begins immediately after.
---
## Chapter Index
| Chapter | File | Topic | Priority |
|---------|------|-------|----------|
| A | `ROADMAP_A_reporting_formats.md` | Excel, PDF, CSV output | 🔴 High |
| B | `ROADMAP_B_html_enhancements.md` | Theme toggle, heatmaps, charts | 🟡 Medium |
| C | `ROADMAP_C_analytics_depth.md` | Baseline comparison, ML insights | 🟡 Medium |
| D | `ROADMAP_D_infrastructure.md` | CLI, logging, MagicMock, IndicatorStore | 🔴 High |
| E | `ROADMAP_E_e2e_test_findings.md` | Real-data test findings & fixes | 🔴 High |
---
## Priority Order (Post-Migration Execution Sequence)
> **Note**: Items E1 and E2 are elevated to **Phase 8 (Sessions 19-20)** — they are
> correctness and performance issues in production code, not deferred enhancements.
| # | Item | Chapter | Effort | Value |
|---|------|---------|--------|-------|
| — | **— Phase 8 (Sessions 19-22) —** | | | |
| E1 | Core mode trade sim overhead fix | E §E1 | 1-2h | 🔴 Correctness |
| E2 | Cache hit rate 50% → 100% | E §E2 | 1-3h | 🔴 Performance |
| E3 | ARTF false warning | E §E3 | 1h | 🟡 Log hygiene |
| E4 | Debug data loading slowdown | E §E4 | 1h | 🟡 Performance |
| — | **— Post-Migration —** | | | |
| 1 | MagicMock cleanup (DEC-020) | D §4.2 | 2h | Code quality |
| 2 | CSV data export | A §1.3 | 30min | Quick win |
| 3 | IndicatorStore refactor (DEC-007) | D §4.4 | 3h | Clean API |
| 4 | Excel output (.xlsx) | A §1.1 | 1 session | High user value |
| 5 | Drawdown chart | B §2.3 | 30min | Quick win |
| 6 | Theme toggle button | B §2.1 | 1h | Polish |
| 7 | Config schema validation | D §4.5 | 3h | Production hardening |
| 8 | Timezone handling verification | D §4.6 | 2h | Correctness |
| 9 | Structured logging | D §4.7 | 2h | Audit trail |
| 10 | PDF output | A §1.2 | 1-2 sessions | Sharing |
| 11 | Baseline comparison | C §3.1 | 1 session | Analytics depth |
| 12 | Hour-of-day heatmap | B §2.2 | 2h | Visual depth |
| 13 | Day × Hour matrix | B §2.4 | 2h | Visual depth |
| 14 | CLI integration | D §4.1 | 1 session | Usability |
| 15 | Two-phase TradeManager (DEC-009) | D §4.8 | 2-3h | 5-10% perf gain |
| 16 | Historical percentile ranking | C §3.2 | 1-2 sessions | Analytics depth |
---
## Items Found During Phase 8 Code Scan
> _Populated by Session 19 scan. See `PHASE8_SCAN_REPORT.md` for full findings._
| Priority | Item | Source | Target Session |
|----------|------|---------|----------------|
| — | _to be filled after Session 19 scan_ | — | — |
---
## Out of Scope (Permanently Rejected)
- **React/Vue front-end** — requires server, breaks single-file portability goal
- **Real-time streaming** — batch backtesting system by design
- **Multi-strategy portfolio analytics** — different system, different contracts
- **Broker API integration** — out of scope for a backtesting engine
- **Pydantic integration** — current `__post_init__` validation is sufficient
---
**Load chapter files on demand** — they contain full specs, API designs, and effort estimates  
**Scan findings**: `PHASE8_SCAN_REPORT.md` (populated Session 19)  
**Decisions**: `DECISION_LOG.md` for rationale on each deferred item
# DECISION LOG — WBWSStrategy Migration Project
**Format**: Append-only. Never edit past entries.  
**Last updated**: 2026-02-17 (Session 18)  
**Total decisions**: 20
---
## Leitmotif — Why We Build This Way
Every decision in this log is guided by five principles. When in doubt, return here.
**1. Single Responsibility**
One module, one concern. `DataLoader` loads. `SignalGenerator` signals.
`MetricsCalculator` measures. `ReportGenerator` renders. No module reaches
into another's domain. Violations always create hidden coupling and test debt.
**2. Performance — Multi-Run Backtester First**
The system runs hundreds of parameter combinations unattended. Every pipeline
stage must extract maximum speed without overwhelming the code. Concrete targets
over vague intent: if a stage can be 50% faster with clean code, it should be.
Micro-optimisations that obscure logic are rejected; architectural wins are not.
**3. Explicit Contracts**
No hidden assumptions. Every stage boundary is a typed dataclass — inputs and
outputs are self-documenting. If a module needs a value, it declares it in its
contract. If it produces a value, the contract captures it. Dict-based "anything
goes" interfaces are the root cause of the legacy system's fragility.
**4. Type Safety — Dataclasses Over Dicts**
`@dataclass(frozen=True)` everywhere. IDE autocomplete, mypy validation, and
immutability are not optional extras — they are how bugs are caught at development
time rather than in a 3am production run. String key lookups on dicts are banned
for all new code.
**5. Production Readiness**
Code delivered to production has no MagicMocks, no debug flags, no print statements,
no test artifacts, no dummies, no commented-out blocks. Type hints are present and minimal —
they document intent, not implementation. Comments explain *why*, never *what*.
Every file is the right size: not so small it hides structure, not so large it
hides complexity. All is tested on real-data with real conditions
---
## Quick Index
| ID | Title | Session | Status |
|----|-------|---------|--------|
| DEC-001 | Parallel architecture (old vs new) | 1 | ✅ |
| DEC-002 | Hybrid migration strategy | 1 | ✅ |
| DEC-003 | DataBundle validation — eager vs lazy | 1 | ✅ revised S2 |
| DEC-004 | Frozen dataclasses for all Phase 4+ contracts | 6 | ✅ |
| DEC-005 | Filter auto-instantiation via class mapping | 5 | ✅ |
| DEC-006 | Time filter always runs first | 5 | ✅ |
| DEC-007 | Mutable indicator dicts (not refactored) | 5 | ✅ |
| DEC-008 | Filter error handling — pass-through | 5 | ✅ |
| DEC-009 | RiskManager called before TradeManager | 9 | ✅ |
| DEC-010 | ProgressiveTracker — convert at boundary | 9 | ✅ |
| DEC-011 | TradeAnalytics aggregates MetricsReport | 14 | ✅ |
| DEC-012 | Optional metrics parameter | 14 | ✅ |
| DEC-013 | AI-like insights with confidence levels | 14 | ✅ |
| DEC-014 | Markdown primary output, JSON secondary | 14 | ✅ |
| DEC-015 | Single self-contained HTML file | 17 | ✅ |
| DEC-016 | Three-layer tabbed report structure | 17 | ✅ |
| DEC-017 | Dark/light theme via ReportConfig | 17 | ✅ |
| DEC-018 | Chart.js over Plotly or Matplotlib | 17 | ✅ |
| DEC-019 | HTML polish Track A before Excel Track B | 18 | ✅ |
| DEC-020 | MagicMock cleanup deferred | 15-18 | ⏳ carry-forward |
---
## Sessions 1-4: Project Strategy & Data Layer
### DEC-001 — Parallel Architecture (Old vs New)
**Date**: 2025-02-09 | **Session**: 1 | **Status**: ✅ DECIDED
**Decision**: Keep old system in `src/strategies/core/`, build new in `src/strategies/specific/`.
**Rationale**: Maximum safety — old system remains runnable for validation at every step. Easy rollback. No risk of breaking production during migration.
**Trade-offs accepted**: Duplicate code during migration. Final cleanup phase (Phase 8) removes `core/`.
---
### DEC-002 — Hybrid Migration Strategy
**Date**: 2025-02-09 | **Session**: 1 | **Status**: ✅ DECIDED
**Decision**: Big Bang for simple self-contained modules (DataLoader, SignalGenerator); Thin Slice for complex modules with many dependencies (FilterPipeline, TradeSimulator).
**Rationale**: Optimises for speed on simple modules without introducing risk on complex ones.
---
### DEC-003 — DataBundle Validation — Eager with Optional Override
**Date**: 2025-02-09 | **Session**: 1 | **Revised**: Session 2 | **Status**: ✅ DECIDED
**Decision**: Validate DataFrame structure in `DataBundle.__post_init__()` by default. Make validation optional (default=False in production) to avoid +31% performance regression.
**Context**: Eager validation in `__post_init__` caused a +31% performance regression in Session 1 benchmarks. Revised to validate only when `validate=True` is passed.
**Trade-offs accepted**: Validation off by default means structural errors surface later. Compensated by integration tests.
---
### DEC-004 — Frozen Dataclasses for All Phase 4+ Contracts
**Date**: 2025-02-12 | **Session**: 6 | **Status**: ✅ DECIDED
**Decision**: All Phase 4+ contracts use `@dataclass(frozen=True)`.
**Rationale**: Immutability prevents accidental modification. Thread-safe. Hashable (usable as dict keys). Easier to track data flow in debugging.
---
## Session 5: FilterPipeline Architecture
### DEC-005 — Filter Auto-Instantiation via Class Mapping
**Date**: 2025-02-13 | **Session**: 5 | **Status**: ✅ DECIDED
**Decision**: Use a `FILTER_CLASSES` dict mapping filter names to classes, not `importlib` dynamic imports.
**Rationale**: Explicit, fast (no import overhead per filter), type-safe (checked at import time), easy to extend (add one line).
**Trade-offs accepted**: Must update mapping when adding new filters.
---
### DEC-006 — Time Filter Always Runs First
**Date**: 2025-02-13 | **Session**: 5 | **Status**: ✅ DECIDED
**Decision**: Time filter is hardcoded to run before all technical filters, regardless of `filter_sequence` config.
**Rationale**: Time filtering requires no indicators — always makes sense to reduce data volume before computing technical indicators. Prevents user misconfiguration.
---
### DEC-007 — Mutable Indicator Dicts (Not Refactored to IndicatorStore)
**Date**: 2025-02-13 | **Session**: 5 | **Status**: ✅ DECIDED (revisit post-migration)
**Decision**: Keep mutable `indicators: Dict[str, pd.Series]` and `ind_np: Dict[str, np.ndarray]` pattern from legacy code.
**Rationale**: Proven performance, zero regression risk, consistency across all 11 filters.
**Post-migration note**: Refactor to `IndicatorStore` class for better encapsulation. See `POST_MIGRATION_ROADMAP.md §B4`.
---
### DEC-008 — Filter Error Handling — Pass-Through
**Date**: 2025-02-13 | **Session**: 5 | **Status**: ✅ DECIDED
**Decision**: If a filter raises an exception, log the error and pass signals through unchanged (`FilterStatus.ERROR`, signals unmodified).
**Rationale**: Pipeline resilience — one bad filter doesn't break everything. Matches legacy behaviour. Error is logged, visible in audit trail.
**Trade-offs accepted**: Could pass signals that the broken filter was meant to block. Compensated by monitoring and the structured logging audit trail.
---
## Session 9: TradeSimulator Integration
### DEC-009 — RiskManager Called Before TradeManager
**Date**: 2025-02-13 | **Session**: 9 | **Status**: ✅ DECIDED
**Decision**: `RiskManager.compute_trade_parameters()` always called first; result passed to `TradeManager.handle_signal()` with real prices.
**Context**: TradeManager v2 requires entry/SL/TP prices upfront. Legacy called RiskManager after TradeManager decision. Three options evaluated:
- A ✅ — Always call RiskManager first (chosen)
- B — Two-phase decision (quick check, then prices)
- C — Placeholder prices (violates contract integrity)
**Rationale**: Correctness over performance. TradeManager must have accurate prices. Measured overhead: 0.8% on 10,000 signals — negligible.
**Post-migration note**: Option B (two-phase) could recover 5-10% on high-rejection strategies. Defer to post-migration. See `POST_MIGRATION_ROADMAP.md`.
---
### DEC-010 — ProgressiveTracker — Convert at Boundary
**Date**: 2025-02-13 | **Session**: 9 | **Status**: ✅ DECIDED
**Decision**: Convert `TradeDecision` enum to string at the boundary when calling `ProgressiveTracker.update_position_management_details()`. Do not migrate ProgressiveTracker in Session 9.
**Rationale**: Keep Session 9 focused. ProgressiveTracker is a debug/analysis tool. Conversion is one line (`result.to_dict()['action']`).
---
## Sessions 14-16: Analytics Layer
### DEC-011 — TradeAnalytics Aggregates MetricsReport
**Date**: 2026-02-16 | **Session**: 14 | **Status**: ✅ DECIDED
**Decision**: `TradeAnalytics` receives a pre-computed `MetricsReport` as input (optional — auto-calculates if `None`) rather than computing metrics internally.
**Rationale**: Natural dependency — analytics need metrics to produce insights. No duplication with MetricsCalculator. Keeps ReportGenerator simple (pure visualisation). Optional parameter supports all usage patterns.
**Trade-offs accepted**: Slight API surface complexity (optional `metrics` param).
---
### DEC-012 — Optional Metrics Parameter
**Date**: 2026-02-16 | **Session**: 14 | **Status**: ✅ DECIDED
**Decision**: `TradeAnalytics.analyze(trade_result, config, metrics=None)` — metrics optional, auto-calculated if `None`.
**Rationale**: One entry point is cleaner. Expert users pass explicit metrics (backtester reuse). Beginners get convenience. Python `Optional` + `None` default is idiomatic.
---
### DEC-013 — AI-Like Insights with Confidence Levels
**Date**: 2026-02-16 | **Session**: 14 | **Status**: ✅ DECIDED
**Decision**: Every analytical observation wrapped in `Insight` dataclass: `message`, `recommendation`, `confidence` (High/Medium/Low), `impact_estimate`, `category`, `severity` (critical/warning/info/success).
**Rationale**: Raw data requires human interpretation. Structured insights are filterable, sortable, renderable in HTML. Severity maps to visual indicators. Impact estimates guide effort allocation.
---
### DEC-014 — Markdown Primary Output, JSON Secondary
**Date**: 2026-02-16 | **Session**: 14 | **Status**: ✅ DECIDED
**Decision**: `TradeAnalytics` primary output is human-readable Markdown (consulting-report style). Structured JSON available via `.to_dict()` / `.to_json()`.
**Rationale**: Markdown is portable (terminals, GitHub, Notion, Slack). JSON serves ReportGenerator and automated pipelines. Both generated cheaply from same data model.
---
## Sessions 17-18: Reporting Layer
### DEC-015 — Single Self-Contained HTML File
**Date**: 2026-02-17 | **Session**: 17 | **Status**: ✅ DECIDED
**Decision**: `ReportGenerator` produces a single `.html` file with all CSS/JS inlined. Chart.js loaded from CDN (one acceptable external dependency).
**Rationale**: Single file is maximally portable — share by email, no web server, no asset folder. CDN failure handler added in v1.1 for offline resilience.
**Trade-offs accepted**: CDN dependency (mitigated). ~32KB file size (acceptable).
---
### DEC-016 — Three-Layer Tabbed Report Structure
**Date**: 2026-02-17 | **Session**: 17 | **Status**: ✅ DECIDED
**Decision**: Three tabs — Executive (grade + top insights), Analytical (charts + full detail), Raw Data (collapsible tables, optional via `include_raw_data`).
**Rationale**: Different audiences need different depths in one file. Lazy chart init means Executive tab loads instantly. `include_raw_data=False` suppresses Layer 3 for cleaner exports.
---
### DEC-017 — Dark/Light Theme via ReportConfig, Not Auto-Detection
**Date**: 2026-02-17 | **Session**: 17 | **Status**: ✅ DECIDED
**Decision**: Theme controlled by `ReportConfig(theme="dark"|"light")`. No `prefers-color-scheme` auto-detection.
**Rationale**: Reports are shared. The sender's intent (dark for internal, light for client PDFs) must not be overridden by viewer's OS preference.
**Post-migration note**: JS theme toggle button is a reasonable v2.0 enhancement. See `POST_MIGRATION_ROADMAP.md §2.1`.
---
### DEC-018 — Chart.js over Plotly or Matplotlib
**Date**: 2026-02-17 | **Session**: 17 | **Status**: ✅ DECIDED
**Decision**: Chart.js 4.x from CDN for all charts.
**Options evaluated**: Chart.js ✅ | Plotly.js (~3MB bundle) | Matplotlib (PNG, no interactivity) | D3.js (high implementation cost).
**Rationale**: Lightweight (~200KB), interactive, JSON-driven API maps naturally to Python-generated data, no build step.
---
### DEC-019 — HTML Polish Before Excel Output
**Date**: 2026-02-17 | **Session**: 18 | **Status**: ✅ DECIDED
**Decision**: Session 18 delivers HTML polish (6 fixes) rather than Excel output (Track B).
**Rationale**: 6 known UX issues found after visual inspection of v1.0 sample report. Fixing brings HTML to production quality before adding new formats. Excel deferred to post-migration — `AnalyticsReport.to_dict()` already covers programmatic data needs.
**Trade-offs accepted**: Excel unavailable until post-migration. See `POST_MIGRATION_ROADMAP.md §1.1`.
---
### DEC-020 — MagicMock Cleanup Deferred
**Date**: 2026-02-17 | **Sessions**: 15-18 | **Status**: ⏳ CARRY-FORWARD (Session 22)
**Decision**: Test files using `MagicMock` for `MetricsReport` and `TradeResult` will be refactored to real dataclasses after ReportGenerator reaches production stability.
**Affected files**:
- `tests/migration/test_analytics_contracts.py`
- `tests/migration/test_trade_analytics_session15.py`
- `tests/migration/test_trade_analytics_session16.py`
- `tests/migration/test_report_generator_session17.py`
**Rationale**: Replacing during active development adds churn. One refactor after stabilisation is more efficient than incremental replacements.
**Target**: Session 22.
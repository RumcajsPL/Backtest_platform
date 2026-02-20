# DECISION LOG — WBWSStrategy Architecture
**Format**: Append-only. Never edit past entries.  
**Last updated**: 2026-02-20 (Session 20)  
**Total decisions**: 40

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
no test artifacts, no dummies, no commented-out blocks. Type hints are present and
minimal — they document intent, not implementation. Comments explain *why*, never *what*.
Every file is the right size: not so small it hides structure, not so large it hides complexity.
All is tested on real data with real conditions.
Fail-fast principle: no assumptions, no checking different folders, no trying, no guessing.
If something is not there, not matching, not answering, not data — the strategy aborts
with a clear error message. PRODUCTION READY.

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
| DEC-019 | HTML polish before Excel output | 18 | ✅ |
| DEC-020 | MagicMock cleanup — closed, N/A for architecture | 15–18 | ✅ |
| DEC-021 | No legacy backward compatibility in new architecture | 19 | ✅ |
| DEC-022 | Execution modes renamed: `debug` → `analytics` | 19 | ✅ |
| DEC-023 | `strategy_template.yaml` is generic and strategy-agnostic | 19 | ✅ |
| DEC-024 | SignalFrame iterators: explicit over implicit | 19 | ✅ |
| DEC-025 | WBWSTrigger must be stateless between calls | 19 | ✅ |
| DEC-026 | Filter cache key must include configuration fingerprint | 19 | ✅ |
| DEC-027 | Pipeline timing must be collected in all modes | 19 | ✅ |
| DEC-028 | Filter error handling: log and continue | 19 | ✅ confirmed |
| DEC-029 | TradeSimulator must respect mode parameter | 19 | ✅ |
| DEC-030 | RiskManager and TradeManager must support lazy initialization | 19 | ✅ |
| DEC-031 | RejectedSignal is not a Trade | 19 | ✅ confirmed |
| DEC-032 | Analytics thresholds must be configurable | 19 | ✅ |
| DEC-033 | ReportGenerator must be strategy-agnostic | 19 | ✅ |
| DEC-034 | TradeResult and AnalyticsReport must be consistent | 19 | ✅ |
| DEC-035 | Timezone handling: data timestamps are taken as-is | 19 | ✅ |
| DEC-036 | Performance targets are data-driven, not aspirational | 19 | ✅ |
| DEC-037 | TradeSimulator is the primary optimisation target | 19 | ✅ |
| DEC-038 | Core mode must be faster than analytics mode | 19 | ✅ |
| DEC-039 | All durations must be tracked in baselines | 19 | ✅ |
| DEC-040 | Phase 8 closed; Phase 9 orchestrator scope defined | 20 | ✅ |

---

## Sessions 1–4: Project Strategy & Data Layer

### DEC-001 — Parallel Architecture (Old vs New)
**Date**: 2025-02-09 | **Session**: 1 | **Status**: ✅ DECIDED  
**Decision**: Keep old system in `src/strategies/core/`, build new in `src/strategies/specific/`.  
**Rationale**: Maximum safety — old system remains runnable for validation at every step. Easy rollback. No risk of breaking production during migration.  
**Trade-offs accepted**: Duplicate code during migration. Final cleanup phase removes `core/`.

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
**Date**: 2025-02-13 | **Session**: 5 | **Status**: ✅ DECIDED  
**Decision**: Keep mutable `indicators: Dict[str, pd.Series]` and `ind_np: Dict[str, np.ndarray]` pattern.  
**Rationale**: Proven performance, zero regression risk, consistency across all 11 filters.  
**Post-Phase 8 note**: Refactor to `IndicatorStore` class is a candidate for Phase 9+ if encapsulation becomes a pain point. See `POST_MIGRATION_ROADMAP.md §B4`.

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
**Context**: TradeManager v2 requires entry/SL/TP prices upfront. Three options evaluated:
- A ✅ — Always call RiskManager first (chosen)
- B — Two-phase decision (quick check, then prices)
- C — Placeholder prices (violates contract integrity)

**Rationale**: Correctness over performance. TradeManager must have accurate prices. Measured overhead: 0.8% on 10,000 signals — negligible.  
**Post-Phase 8 note**: Option B (two-phase) could recover 5–10% on high-rejection strategies. Candidate for Phase 9 performance work.

---

### DEC-010 — ProgressiveTracker — Convert at Boundary
**Date**: 2025-02-13 | **Session**: 9 | **Status**: ✅ DECIDED  
**Decision**: Convert `TradeDecision` enum to string at the boundary when calling `ProgressiveTracker.update_position_management_details()`. ProgressiveTracker is analytics-mode only.  
**Rationale**: Keep session focused. ProgressiveTracker is an analytics tool. Conversion is one line.

---

## Sessions 14–16: Analytics Layer

### DEC-011 — TradeAnalytics Aggregates MetricsReport
**Date**: 2026-02-16 | **Session**: 14 | **Status**: ✅ DECIDED  
**Decision**: `TradeAnalytics` receives a pre-computed `MetricsReport` as input (optional — auto-calculates if `None`) rather than computing metrics internally.  
**Rationale**: Natural dependency — analytics need metrics to produce insights. No duplication with MetricsCalculator. Optional parameter supports all usage patterns.

---

### DEC-012 — Optional Metrics Parameter
**Date**: 2026-02-16 | **Session**: 14 | **Status**: ✅ DECIDED  
**Decision**: `TradeAnalytics.analyze(trade_result, config, metrics=None)` — metrics optional, auto-calculated if `None`.  
**Rationale**: One entry point is cleaner. Expert users pass explicit metrics (backtester reuse). Beginners get convenience. Python `Optional` + `None` default is idiomatic.

---

### DEC-013 — AI-Like Insights with Confidence Levels
**Date**: 2026-02-16 | **Session**: 14 | **Status**: ✅ DECIDED  
**Decision**: Every analytical observation wrapped in `Insight` dataclass: `message`, `recommendation`, `confidence` (High/Medium/Low), `impact_estimate`, `category`, `severity`.  
**Rationale**: Raw data requires human interpretation. Structured insights are filterable, sortable, renderable in HTML. Severity maps to visual indicators.

---

### DEC-014 — Markdown Primary Output, JSON Secondary
**Date**: 2026-02-16 | **Session**: 14 | **Status**: ✅ DECIDED  
**Decision**: `TradeAnalytics` primary output is human-readable Markdown. Structured JSON available via `.to_dict()` / `.to_json()`.  
**Rationale**: Markdown is portable. JSON serves ReportGenerator and automated pipelines. Both generated cheaply from the same data model.

---

## Sessions 17–18: Reporting Layer

### DEC-015 — Single Self-Contained HTML File
**Date**: 2026-02-17 | **Session**: 17 | **Status**: ✅ DECIDED  
**Decision**: `ReportGenerator` produces a single `.html` file with all CSS/JS inlined. Chart.js loaded from CDN.  
**Rationale**: Single file is maximally portable — share by email, no web server, no asset folder. CDN failure handler added in v1.1 for offline resilience.  
**Trade-offs accepted**: CDN dependency (mitigated). ~32KB file size (acceptable).

---

### DEC-016 — Three-Layer Tabbed Report Structure
**Date**: 2026-02-17 | **Session**: 17 | **Status**: ✅ DECIDED  
**Decision**: Three tabs — Executive (grade + top insights), Analytical (charts + full detail), Raw Data (collapsible tables, optional via `include_raw_data`).  
**Rationale**: Different audiences need different depths in one file. Lazy chart init means Executive tab loads instantly.

---

### DEC-017 — Dark/Light Theme via ReportConfig, Not Auto-Detection
**Date**: 2026-02-17 | **Session**: 17 | **Status**: ✅ DECIDED  
**Decision**: Theme controlled by `ReportConfig(theme="dark"|"light")`. No `prefers-color-scheme` auto-detection.  
**Rationale**: Reports are shared. The sender's intent must not be overridden by viewer's OS preference.

---

### DEC-018 — Chart.js over Plotly or Matplotlib
**Date**: 2026-02-17 | **Session**: 17 | **Status**: ✅ DECIDED  
**Decision**: Chart.js 4.x from CDN for all charts.  
**Options evaluated**: Chart.js ✅ | Plotly.js (~3MB bundle) | Matplotlib (PNG, no interactivity) | D3.js (high implementation cost).  
**Rationale**: Lightweight (~200KB), interactive, JSON-driven API maps naturally to Python-generated data, no build step.

---

### DEC-019 — HTML Polish Before Excel Output
**Date**: 2026-02-17 | **Session**: 18 | **Status**: ✅ DECIDED  
**Decision**: Session 18 delivers HTML polish rather than Excel output.  
**Rationale**: 6 known UX issues found after visual inspection of v1.0 sample report. Fixing brings HTML to production quality before adding new formats. Excel deferred to post-Phase 8 — `AnalyticsReport.to_dict()` already covers programmatic data needs.

---

### DEC-020 — MagicMock Cleanup
**Date**: 2026-02-17 | **Sessions**: 15–18 | **Status**: ✅ CLOSED (Session 20)  
**Original decision**: Test files using `MagicMock` for `MetricsReport` and `TradeResult` would be refactored to real dataclasses after ReportGenerator reached production stability.  
**Affected files** (original scope):
- `tests/migration/test_analytics_contracts.py`
- `tests/migration/test_trade_analytics_session15.py`
- `tests/migration/test_trade_analytics_session16.py`
- `tests/migration/test_report_generator_session17.py`

**Resolution (Session 20)**: MagicMock is a test infrastructure concern only. Architecture code (`src/`) contains no mocks by definition — `unittest.mock` has no place in production modules. The four listed test files are superseded by real-data integration tests already running in the test suite. No refactor of legacy test files is required.  
**Closed**: 2026-02-20. Removed from Session 21 carry-forward queue.

---

## Session 19: Phase 8 Scan Findings

### DEC-021 — No Legacy Backward Compatibility in New Architecture
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ DECIDED

**Decision**: The new architecture (`src/strategies/specific/`, `src/strategies/contracts/`, `src/config/`) must contain zero backward compatibility dependencies on the legacy system.

**Specifically prohibited**:
- Adapter methods for legacy YAML key names (`from_legacy_yaml()`, `from_yaml_config()`)
- Fallback dict-based interfaces inherited from legacy modules
- Conditional logic that branches on "legacy vs new" at runtime
- Hardcoded references to legacy strategy names in cache dirs or output paths
- The string `"debug"` as a mode name

**What this means**: `DataConfig.from_yaml_config()` removed. `DataLoader` defaults to `mode="analytics"`. Cache dir moved to `paths.py`-managed location.

**Legacy system**: Continues running independently via `scripts/runners/run_wbws_strategy.py` + `wbws_strategy.yaml` until decommissioned. No changes to legacy code.

---

### DEC-022 — Execution Modes Renamed: `debug` → `analytics`
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ DECIDED

| Mode | Purpose | Outputs |
|------|---------|---------|
| `core` | Multi-run backtester. Maximum speed. | `MetricsReport` only. |
| `analytics` | Single-run analysis. Full pipeline. | `MetricsReport` + `TradeAnalytics` + HTML report. |

Passing `mode="debug"` raises `ValueError` with migration message. The rename was applied globally in Session 20 (Block A).

---

### DEC-023 — `strategy_template.yaml` is Generic and Strategy-Agnostic
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ DECIDED

**Decision**: Config template lives at `configs/strategies/strategy_template.yaml`. Strategy-specific config is derived from it per strategy. WBWS-specific config (`wbws_strategy_v2.yaml`) is a Phase 9 deliverable.

---

### DEC-024 — SignalFrame Iterators: Explicit Over Implicit
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ DECIDED

`SignalFrame.__iter__` raises `RuntimeError` when `indicator_data is None` (core mode). Callers must explicitly use `iter_raw()` for core-mode iteration. Turns silent wrong-answer bugs into immediate, debuggable errors.

---

### DEC-025 — WBWSTrigger Must Be Stateless Between Calls
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ DECIDED

`calculate_signals()` is a pure function. No instance state stored after the call. `get_signals()` removed. Enables safe reuse in a parallel backtester.

---

### DEC-026 — Filter Cache Key Must Include Configuration Fingerprint
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ DECIDED

Cache key = data fingerprint (mtime + size) + filter config hash (parameters + enabled status). Hash computed once at `FilterPipeline.__init__`. Prevents silent use of wrong pre-computed indicators when backtester alternates filter configurations.

---

### DEC-027 — Pipeline Timing Must Be Collected in All Modes
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ DECIDED

`execution_time_ms` populated in all metadata objects in both modes. Only logging is gated on mode. `perf_counter()` overhead (~50ns) is negligible against the value of always-available timing data.

---

### DEC-028 — Filter Error Handling: Log and Continue
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ CONFIRMED (DEC-008)

Confirmed all 11 filters correctly implement DEC-008: on exception, return `FilterStatus.ERROR` with pass-through signals and continue.

---

### DEC-029 — TradeSimulator Must Respect Mode Parameter
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ DECIDED

`simulate_trades(mode: str = "core")` replaces the old `verbose: bool` flag. All internal expensive operations (LTF, progressive tracking, signal ID lookups) gated on `mode == "analytics"`.

---

### DEC-030 — RiskManager and TradeManager Must Support Lazy Initialisation
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ DECIDED

Expensive calculations (ATR, annual range) are lazy and cached at class level. `RiskManager.clear_cache()` must be called between backtester runs. SpreadManager caches YAML config at class level after first load.

---

### DEC-031 — RejectedSignal Is Not a Trade
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ CONFIRMED

`RejectedSignal` and `Trade` remain separate contracts. `to_legacy_trade_dict()` removed. Rejected signals never had valid prices, stops, or position sizes — treating them as trades with placeholder values is a correctness violation.

---

### DEC-032 — Analytics Thresholds Must Be Configurable
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ DECIDED

`AnalyticsConfig` contract added to `analytics_contracts.py`. Passed to `TradeAnalytics.analyze()` via optional parameter. Default values match prior hardcoded constants. Required for multi-instrument support (forex vs indices have different point scales).

---

### DEC-033 — ReportGenerator Must Be Strategy-Agnostic
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ DECIDED

`brand_name: str` added to `ReportConfig`. Used in HTML header and footer. No strategy name hardcoded in `ReportGenerator`.

---

### DEC-034 — TradeResult and AnalyticsReport Must Be Consistent
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ DECIDED

`ReportGenerator.generate()` compares `len(trade_result.trades)` with `analytics_report.input_metrics.total_trades` on every call. Mismatch → warning logged + equity curve skipped. Prevents misleading charts from mismatched inputs.

---

### DEC-035 — Timezone Handling: Data Timestamps Are Taken As-Is
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ DECIDED

No timezone conversion applied at load time. `timezone` field in `DataConfig` is informational only. Data preparation (CET alignment) is a responsibility of the upstream data pipeline (`generate_ohlcv.py`), not the backtester.

---

## Session 19: Performance Baseline Decisions

### DEC-036 — Performance Targets Are Data-Driven, Not Aspirational
**Date**: 2026-02-19 | **Session**: 19 | **Status**: ✅ DECIDED

Baseline (Session 19):
- Core mode: 42,680ms total (41,052ms TradeSimulator)
- Analytics mode: 31,663ms total (29,927ms TradeSimulator)

Session 20 targets:
- Core mode: < 12,000ms (71% improvement)
- TradeSimulator: < 10,000ms (76% improvement)

All future performance claims must reference a named baseline and a measured delta.

---

### DEC-037 — TradeSimulator Is the Primary Optimisation Target
**Date**: 2026-02-19 | **Session**: 19 | **Status**: ✅ DECIDED

TradeSimulator accounts for 94–96% of total runtime. Session 20 optimisation focused there: gate LTF precomputation, progressive tracking, and signal ID lookups on `analytics` mode only.

Other modules (DataLoader 3.3%, SignalGenerator 0.2%, FilterPipeline 0.2%) are already well-optimised.

---

### DEC-038 — Core Mode Must Be Faster Than Analytics Mode
**Date**: 2026-02-19 | **Session**: 19 | **Status**: ✅ DECIDED

Baseline showed a 26% inversion (core slower than analytics). Root cause: unconditional LTF precomputation in `trade_simulator.py`. Fixed in Session 20 (Block E) by gating LTF on `mode == "analytics"`.

---

### DEC-039 — All Durations Must Be Tracked in Baselines
**Date**: 2026-02-19 | **Session**: 19 | **Status**: ✅ DECIDED

Per-stage timing captured in the non-regression test suite. Enables automatic regression detection. No performance claim is valid without a baseline reference.

---

## Session 20: Phase 8 Close

### DEC-040 — Phase 8 Closed; Phase 9 Orchestrator Scope Defined
**Date**: 2026-02-20 | **Session**: 20 | **Status**: ✅ DECIDED

**Phase 8 outcome**: All P0 and P1 hardening issues resolved across Blocks B–I. Block A (global `"debug"` → `"analytics"` rename) completed. Architecture is production-hardened. Test count: ~302.

**Carry-forward to Phase 9**:
- DEC-020: MagicMock cleanup in 4 test files (first task, Session 21)
- P2 observability items: per-stage timing in `AnalyticsReport`, `AnalyticsConfig` contract, cache statistics surfaced in `MetricsReport`
- `wbws_strategy_v2.yaml`: WBWS-specific config derived from `strategy_template.yaml`

**Phase 9 scope** (Sessions 21–25, estimated):

*Step 9.0 — Stabilisation (Session 21)*: MagicMock removal, P2 observability, full E2E integration test on real data, performance regression baseline locked.

*Step 9.1 — Strategy Orchestrator (Sessions 22–23)*: Orchestrator module that composes the pipeline for a single strategy run. Clean entry point callable by both interactive scripts and automated parameter sweeps. Multi-run loop with `clear_cache()` discipline enforced at orchestrator level.

*Step 9.2 — Multi-Strategy Support (Session 23–24)*: `strategy_template.yaml` becomes the base; `wbws_strategy_v2.yaml` is the first derived config. Second strategy can be onboarded by supplying a new config and a new `SignalGenerator` — no changes to pipeline modules.

*Step 9.3 — Walk-Forward & Parameter Optimisation (Session 24–25)*: Walk-forward analysis wrapper over the orchestrator. Parameter grid search with `core` mode. Results aggregated into a comparison report.

*Step 9.4 — Production Deployment Guide*: Runbook for fresh environment setup, data pipeline prerequisites, first run validation, and ongoing monitoring.

**Architecture is locked during Phase 9**: any change to a contract or module interface requires a case analysis logged here before implementation.

**Rationale**: Phase 8 delivered a hardened, contract-clean, well-tested pipeline. Phase 9 builds on that foundation by adding the orchestration layer that makes the system usable as a complete trading research platform, not just a pipeline library.
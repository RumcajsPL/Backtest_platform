# DECISION LOG — WBWSStrategy Migration Project
**Format**: Append-only. Never edit past entries.  
**Last updated**: 2026-02-18 (Session 19)  
**Total decisions**: 34

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
Fail-fast princple, no assumption, no checking different folders, no trying, no guessing, no testing, something is not there, not matching, not answering, no data strategy aborts with error message => PRODUCTION READY  

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
| DEC-021 | No legacy backward compatibility in new architecture | 19 | ✅ |
| DEC-022 | Execution modes renamed: `debug` → `analytics` | 19 | ✅ |
| DEC-023 | `strategy_template.yaml` is generic and strategy-agnostic | 19 | ✅ |
| DEC-024 | SignalFrame iterators: explicit over implicit | 19 | ✅ |
| DEC-025 | WBWSTrigger must be stateless between calls | 19 | ✅ |
| DEC-026 | Filter cache key must include configuration fingerprint | 19 | ✅ |
| DEC-027 | Pipeline timing must be collected in all modes | 19 | ✅ |
| DEC-028 | Filter error handling: log and continue | 19 | ✅ CONFIRMED |
| DEC-029 | TradeSimulator must respect mode parameter | 19 | ✅ |
| DEC-030 | RiskManager and TradeManager must support lazy initialization | 19 | ✅ |
| DEC-031 | RejectedSignal is not a Trade | 19 | ✅ CONFIRMED |
| DEC-032 | Analytics thresholds must be configurable | 19 | ✅ |
| DEC-033 | ReportGenerator must be strategy-agnostic | 19 | ✅ |
| DEC-034 | TradeResult and AnalyticsReport must be consistent | 19 | ✅ |
| DEC-035 | Timezone handling: data timestamps are taken as-is | 19 | ✅ CLARIFIED |

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

---

## Session 19: Phase 8 Clarifications & Scan Findings

### DEC-021 — No Legacy Backward Compatibility in New Architecture
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ DECIDED

**Decision**: The new architecture (`src/strategies/specific/`, `src/strategies/contracts/`, `src/config/`) must contain **zero backward compatibility dependencies** on the legacy system (`src/strategies/core/`, `scripts/runners/run_wbws_strategy.py`, `configs/strategies/wbws/wbws_strategy.yaml`).

**Specifically prohibited in new architecture files**:
- Adapter methods for legacy YAML key names (e.g., `from_legacy_yaml()`, `from_yaml_config()` that reads `data.file` instead of `data.paths.strategy_ohlcv`)
- Fallback dict-based interfaces inherited from legacy modules
- Conditional logic that branches on "legacy vs new" at runtime
- Hardcoded references to legacy strategy names (e.g., `wbws` in cache directory names, output paths)
- The string `"debug"` as a mode name — renamed to `"analytics"` in new architecture

**What this means in practice**:
- `DataConfig.from_yaml_config()` reads legacy key names → must be removed (P1-CH2-5)
- `DataLoader.__init__` defaults to `mode="debug"` → must become `mode="analytics"` (P0-CH2-1)
- Cache dir `~/.wbws_data_cache` embeds strategy name → must move to `paths.py`-managed location (P1-CH2-2)
- `strategy_template.yaml` is written fresh for the new architecture — no key name inheritance from legacy YAML

**Rationale**: Backward compatibility adapters create hidden coupling. They make the new architecture dependent on the legacy system's design decisions. When the legacy system is decommissioned in Phase 8 cleanup, these adapters become dead code — or worse, they prevent decommissioning because removing them breaks tests. The clean break is better engineering and easier to maintain.

**Legacy system**: Continues running independently via `scripts/runners/run_wbws_strategy.py` + `wbws_strategy.yaml` until Phase 8 cleanup. No changes to legacy code during Phase 8.

**Trade-offs accepted**: Migration period requires two separate configs (legacy YAML + new template). Managed by keeping legacy YAML in place and building new template from scratch. The two configs are entirely independent.

---

### DEC-022 — Execution Modes Renamed: `debug` → `analytics`
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ DECIDED

**Decision**: The two execution modes in the new architecture are `"core"` and `"analytics"`. The name `"debug"` is permanently retired.

**Mode definitions (new architecture)**:
| Mode | Purpose | Outputs |
|------|---------|---------|
| `core` | Multi-run backtester. Maximum speed. | Minimal `MetricsReport` only. No `TradeAnalytics`, no `ReportGenerator`. |
| `analytics` | Single-run analysis. Full pipeline. | `MetricsReport` + `TradeAnalytics` + `ReportGenerator` (HTML). Production quality, not debug tooling. |

**Rationale**: The old `debug` label implied diagnostic tooling for developers. In reality, `analytics` mode is the **primary user-facing mode** — it produces the HTML reports and insights that make the system valuable. Calling it `debug` undersells it and creates confusion about when to use it.

**Impact on codebase**: Global search-and-replace `"debug"` → `"analytics"` in all new architecture files. Add `ValueError` on `mode="debug"` with migration message. The legacy system retains its own `core`/`debug` terminology unchanged.

**Trade-offs accepted**: One-time rename effort across all new architecture modules. `strategy_template.yaml` will document the two modes clearly.

---

### DEC-023 — `strategy_template.yaml` is Generic and Strategy-Agnostic
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ DECIDED

**Decision**: The new architecture YAML config template is named `strategy_template.yaml` (not `wbws_strategy_new.yaml` or any strategy-specific name). It lives at `configs/strategy_template.yaml`.

**Structure**:
- Generic sections applicable to any strategy: `data`, `execution`, `trade_management`, `filters`, `output`
- Strategy-specific sections (signal parameters, indicator config) are **blank placeholders** in the template — filled by each strategy's own YAML
- WBWS-specific config (`configs/strategies/wbws/wbws_strategy_v2.yaml`) will be derived from the template in Phase 10

**Living document policy**: During Phase 8 scan (Sessions 19-22), `strategy_template.yaml` is actively modified as each scan chapter reveals the actual parameters consumed by each module. It is NOT frozen until Phase 8 scan is complete.

**Rationale**: The new architecture is designed to support multiple strategies (Phase 10+). A strategy-named template would need to be forked for every new strategy. A generic template establishes the contract once; each strategy extends it.

**Trade-offs accepted**: Template requires placeholder sections for strategy-specific config — slightly more verbose than a minimal YAML. Acceptable given the multi-strategy goal.

---

### DEC-024 — SignalFrame iterators: explicit over implicit
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ DECIDED

`SignalFrame.__iter__` constructs full `Signal` objects requiring `indicator_data`. In core mode where `indicator_data=None`, this would silently produce invalid `Signal` objects with `mid_price=0.0`. The fast path `iter_raw()` exists but nothing prevents accidental misuse of `__iter__`.

**Decision**: Add runtime guard in `__iter__` raising `RuntimeError` when `indicator_data is None`, forcing callers to explicitly choose `iter_raw()` for core mode. This turns silent wrong-answer bugs into immediate, debuggable errors.

**Rationale**: Leitmotif principle "Production Readiness" - silent failures are worse than explicit errors.

---

### DEC-025 — WBWSTrigger must be stateless between calls
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ DECIDED

`WBWSTrigger` currently stores `self.signals_df` as instance state after `calculate_signals()`. This creates a race condition if the trigger is reused concurrently (future parallel backtester) and allows stale data to persist via `get_signals()`.

**Decision**: Remove all instance state from `calculate_signals()` - make it pure. The method returns results directly; caller is responsible for storage. Remove `get_signals()` entirely as it's unused in new architecture.

**Rationale**: Leitmotif principles "Single Responsibility" (trigger only triggers, doesn't store) and "Performance" (no hidden state between runs).

---

### DEC-026 — Filter cache key must include configuration fingerprint
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ DECIDED

`FilterPipelineCache.compute_cache_id()` currently keys only on DataFrame characteristics. When the backtester alternates between filter configurations (e.g., ADX length 14 vs 20), both share the same cache key, leading to a 50% cache hit rate **and**, more critically, silent use of wrong pre-computed indicators for the second configuration.

**Decision**: The cache key must include a fingerprint of the active filter configuration (parameters + enabled status). The fingerprint is computed once at `FilterPipeline.__init__` and passed to all cache operations.

**Rationale**: Leitmotif principles "Performance" (maximize cache hits) and "Production Readiness" (prevent silent correctness bugs). A cache that returns wrong data is worse than no cache.

---

### DEC-027 — Pipeline timing must be collected in all modes
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ DECIDED

Currently, `execution_time_ms` in `FilterMetadata` is set to `None` in core mode. For diagnosing the 11% core mode slowdown (P0-E1), per-filter timing data is essential - the slowdown can't be traced without it.

**Decision**: Always collect execution time with `perf_counter()`, regardless of mode. Only gate logging on mode. Store timing in all metadata objects.

**Rationale**: Leitmotif principle "Observability" - you can't fix what you can't measure. The cost of timing (50ns) is negligible compared to the value of performance data.

---

### DEC-028 — Filter error handling: log and continue
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ CONFIRMED (existing DEC-008)

Confirmed that all filters correctly implement DEC-008: on exception, return `FilterStatus.ERROR` with pass-through signals, and pipeline continues.

**Files verified**:
- `adx_filter.py` - error path returns ERROR with empty signal frame
- `bollinger_filter.py` - same pattern
- `filter_pipeline.py` - catches exceptions and creates error metadata

**Rationale**: Pipeline resilience - one bad filter doesn't break everything. Error is logged and visible in audit trail.

---

### DEC-029 — TradeSimulator must respect mode parameter
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ DECIDED

`TradeSimulator.simulate_trades()` currently uses a `verbose` boolean flag instead of the standard `mode` parameter used throughout the architecture. This creates inconsistency and prevents proper gating of expensive operations.

**Decision**: Add `mode: str = "core"` parameter to `simulate_trades()`. All internal operations must check `mode`:
- Core mode: strategy-bar OHLC execution, no LTF, minimal metadata
- Analytics mode: LTF execution, full metadata, progressive tracking

**Rationale**: Leitmotif principle "Performance - Multi-Run Backtester First" - core mode must be optimized for speed.

---

### DEC-030 — RiskManager and TradeManager must support lazy initialization
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ DECIDED

Both RiskManager and TradeManager perform expensive calculations in `__init__` (ATR, annual range) that are not needed in core mode, and are repeated across runs in multi-run backtester.

**Decision**: All expensive calculations must be lazy (computed on first use) and cached where possible. RiskManager should accept `mode` parameter and skip non-essential calculations in core mode.

**Rationale**: Leitmotif principle "Performance" - avoid repeated work across backtester runs.

---

### DEC-031 — RejectedSignal is not a Trade
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ CONFIRMED

Session 10.1 correctly separated `RejectedSignal` from `Trade`. This is now confirmed as the right design. Rejected signals never had valid entry prices, stop losses, or position sizes - they are fundamentally different from trades.

**Decision**: Maintain separation. Remove `to_legacy_trade_dict()` method (P1-CH5-1) after ensuring all consumers handle rejected signals appropriately.

**Rationale**: Leitmotif principle "Explicit Contracts" - don't pretend a rejection is a trade with placeholder values.

---

### DEC-032 — Analytics thresholds must be configurable
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ DECIDED

TradeAnalytics currently hardcodes insight thresholds (e.g., `SESSION_CRITICAL_LOSS_PTS = -30.0`). For different instruments (forex vs indices) and timeframes (1min vs 1hour), these thresholds may need adjustment.

**Decision**: Add `AnalyticsConfig` contract to `analytics_contracts.py` containing all threshold values. Pass to `TradeAnalytics.analyze()` via optional parameter. Default values match current hardcoded constants.

**Rationale**: Leitmotif principle "Production Readiness" - configuration should be explicit and adaptable.

---

### DEC-033 — ReportGenerator must be strategy-agnostic
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ DECIDED

ReportGenerator currently hardcodes "WBWSStrategy" in the HTML header. For multi-strategy support (Phase 10+), this must be configurable.

**Decision**: Add `brand_name: str = "Strategy"` to `ReportConfig`. Use this value in the HTML header instead of hardcoded string.

**Rationale**: Leitmotif principle "Single Responsibility" - ReportGenerator should serve all strategies, not just WBWS.

---

### DEC-034 — TradeResult and AnalyticsReport must be consistent
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ DECIDED

ReportGenerator accepts both `analytics_report` and optional `trade_result`. If these are inconsistent (different trade counts), the equity curve will mismatch the analytics, leading to confusion.

**Decision**: Add validation in `ReportGenerator.generate()` comparing `len(trade_result.trades)` with `analytics_report.input_metrics.total_trades`. Log a warning if mismatch detected.

**Rationale**: Leitmotif principle "Production Readiness" - silent inconsistencies are worse than explicit warnings.

---

### DEC-035 — Timezone handling: data timestamps are taken as-is
**Date**: 2026-02-18 | **Session**: 19 | **Status**: ✅ CLARIFIED

**Clarification**: OHLCV data files are already in CET (UTC+1) timezone. No conversion is required or performed. The system takes timestamps as given, assuming they are already in the correct timezone for the instrument (EUREX trading hours are CET-based).

**Decision**: No timezone conversion will be applied to loaded data. The `timezone` field in `DataConfig` is for informational/documentation purposes only, not for automatic conversion. The system will not assert or convert timezones at load time.

**Rationale**: Leitmotif principle "Production Readiness" - data preparation is a separate concern handled by the data pipeline (`generate_ohlcv.py`). The backtester consumes prepared data as-is, without second-guessing timezone correctness.

**Impact on code**:
- Remove P1-CH1-7 (timezone validation) from action plan
- `DataLoader` should not attempt to convert or validate timezones
- Documentation should clarify that data must be in the instrument's local timezone
---

---

### DEC-036 — Performance targets are data-driven, not aspirational
**Date**: 2026-02-19 | **Session**: 19 | **Status**: ✅ DECIDED

Baseline measurements established:
- Core mode: 42,680ms (96% in TradeSimulator at 41,052ms)
- Analytics mode: 31,663ms (94.5% in TradeSimulator at 29,927ms)

**Decision**: All Session 20 performance targets derive from these baselines:
- Core mode target: <12,000ms (71% improvement)
- TradeSimulator target: <10,000ms (76% improvement)
- "Faster" is not acceptable without "X% faster than baseline Y"

**Rationale**: Leitmotif principle "Production Readiness" — measurable over subjective.

---

### DEC-037 — TradeSimulator is the primary optimization target
**Date**: 2026-02-19 | **Session**: 19 | **Status**: ✅ DECIDED

Baseline profiling reveals TradeSimulator consumes 94–96% of total runtime.

**Decision**: Session 20 optimization targets TradeSimulator as primary focus:
- Gate LTF precomputation on analytics mode only
- Gate progressive tracking on analytics mode only
- Gate signal_id lookups on analytics mode only

Other modules (DataLoader 3.3%, SignalGenerator 0.2%, FilterPipeline 0.2%) are already well-optimized and are not primary optimization targets.

**Rationale**: Leitmotif principle "Performance — Multi-Run Backtester First" — optimize where it matters most.

---

### DEC-038 — Core mode must be faster than analytics mode
**Date**: 2026-02-19 | **Session**: 19 | **Status**: ✅ DECIDED

Baseline shows a 26% performance inversion: core 42,680ms vs analytics 31,663ms.

**Decision**: This inversion is a P0 correctness issue and must be resolved in Session 20. Core mode skips LTF execution, analytics generation, and progressive tracking — it must be materially faster than analytics mode.

**Root cause**: LTF tick precomputation runs unconditionally in trade_simulator.py, consuming ~15,000ms regardless of mode.

**Fix**: Add `mode` parameter to `simulate_trades()`. Gate LTF on `mode == "analytics"`.

**Rationale**: Leitmotif principle "Performance — Multi-Run Backtester First" — core mode is designed for speed.

---

### DEC-039 — All durations must be tracked in baselines
**Date**: 2026-02-19 | **Session**: 19 | **Status**: ✅ DECIDED

**Decision**: All future performance discussions must reference specific baseline numbers. Per-stage timing is captured in the non-regression test suite. This enables automatic regression detection and measurable improvement tracking.

**Rationale**: Leitmotif principle "Production Readiness" — measurable over subjective.
---

## Sessions 20–22 Preview (updated)

| Session | Focus | Expected Output | Test Target |
|---------|-------|-----------------|-------------|
| 20 | All P0 + P1 fixes | Strategy template, mode rename, performance fix | ~302 |
| 21 | P2 + Observability | AnalyticsConfig, per-stage timing, cache stats | ~322 |
| 22 | Integration + MagicMock cleanup | Full E2E test, MagicMock removal (DEC-020) | ~347 | new tests |
| 21 | P2 + Observability | Logging, timing, configurable thresholds |
| 22 | Integration + MagicMock cleanup | Full E2E test, MagicMock removal |
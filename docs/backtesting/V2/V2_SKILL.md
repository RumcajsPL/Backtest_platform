---
name: ctp-v2-backtester-project
description: >
  Use for any CTP V2 Backtester Project work. Triggers: backtest,
  backtester, backtesting, agent, agentic, staging, sandbox, 
  V2_GOV.md, V2_PLAN.md, V2_CONTEXT.md, V2_CHANGELOG.md, V2_SESSION_LOG.md.
---
# SKILL.md — CTP V2 Backtester Project
# Role: Claude.ai Program Director / PM
# Scope: V2 Backtesting Platform only
# Version: 1.0 | Date: 2026-04-02
---
## PURPOSE OF THIS SKILL
This SKILL governs Claude.ai's role as Program Director / PM for the CTP V2 Backtester project.
It defines what Claude.ai knows, what it must verify, how it instructs agents, and the
architecture rules that apply to every instruction issued.
Claude.ai never writes directly to project files. Every instruction is relayed by the
Project Owner to an agent. Claude.ai reviews all agent output before promotion.
---
## 1. PROJECT IDENTITY
**Project**: CTP Backtesting Platform V2
**Programme**: Complementary Trading Platform (CTP)
**Phase**: Phase 3 of CTP Roadmap (starts after Phase 2 gate: 20+ automated paper trades)
**Status at project start**: Phase 2 in progress — V2 does not start until Phase 2 gate confirmed
**Environments**:
```
Production: E:\Trading\Backtest_platform\
Staging:    E:\Trading\Backtest_platform_staging\
Sandbox:    E:\Trading\Backtest_platform_sandbox\
```
Raw data files (Parquet, OHLCV) are read-only and shared between production and staging —
no data cloning required.
**Phase 3 gate** (V2 complete): V2 produces auto_go candidates on a second instrument
without per-instrument recalibration. `max_workers` constraint removed — 6+ workers stable.
---
## 2. CLAUDE.AI ROLE AND CONSTRAINTS
### What Claude.ai does
- Designs all tasks and authors every agent instruction before it reaches an agent
- Reviews all agent output (diffs, test results) before Owner promotes to staging
- Maintains all project documents (CONTEXT.md, SESSION_LOG, this SKILL, GOV, PLAN, CHANGELOG)
- Makes all architecture decisions; escalation path for all technical ambiguities
- Plans sessions: defines tasks, assigns agents, sets order
- Enforces architecture rules in every instruction issued
### What Claude.ai does NOT do
- Does not read project files directly — relies on Owner relay or agent search output
- Does not execute code or run tests — delegates to agents
- Does not communicate with agents directly — Owner is the relay
- Does not write to project files
### Every instruction Claude.ai issues includes
1. Which file(s) to read first (exact paths)
2. Exact change to make, or precise question to answer
3. Relevant architecture rules that apply
4. Expected output format: diff / test result / search result / complete file
### Instruction format template
```
AGENT: [A / B / C]
ENV:   [staging / sandbox / read-only]
READ FIRST: [exact file paths]
TASK: [precise description]
CONSTRAINTS: [architecture rules from §4 that apply]
OUTPUT: [diff / complete file / test results / search results]
DO NOT: [explicit exclusions]
```
---
## 3. AGENT ROSTER AND ASSIGNMENT RULES
### Agent A — Claude Code (Dev Lead)
**Specialisation**: Complex multi-file changes, new module implementation, test writing
**Operates in**: Staging (read/write)
**Assign when**: New module, multi-file feature, refactoring, test suite implementation
**Must not**: Make architecture decisions, modify production files, deviate from instruction scope
### Agent B — Codex (Rapid Dev)
**Specialisation**: Well-scoped, self-contained tasks — boilerplate, config, utilities
**Operates in**: Sandbox (output reviewed before staging)
**Assign when**: Config schemas, simple helpers, test fixtures, stubs
**Must not**: Architectural changes, multi-module refactors, anything touching broker code
### Agent C — Qwen Code (QA / Search)
**Specialisation**: Codebase search, impact analysis, test execution, verification
**Operates in**: Read access to all environments
**Assign when**: Dependency mapping, "what breaks if we change X?", test suite runs, verifying Agent A/B output
**Must not**: Implement features, write to source files
### Agent role flexibility
At project start and at periodic reviews, Claude.ai evaluates agent performance.
Roles may temporarily shift based on:
- Volume of work (a heavy sprint may move simple tasks from A to B)
- Quality issues (repeated failures in a role → reassignment)
- Availability (agent-specific outages)
Any role change is documented in SESSION_LOG and CONTEXT.md.
### Agent assignment matrix
```
Task type                          → Agent
─────────────────────────────────────────────────────
New module / complex refactor      → A
Multi-file feature implementation  → A
Test suite implementation          → A
Simple utility / config / stub     → B
Boilerplate / repetitive code      → B
Codebase search / impact analysis  → C
Test execution + result reporting  → C
Dependency / usage mapping         → C
Cross-agent output verification    → C
```
---
## 4. V2 ARCHITECTURE RULES
These rules apply to ALL V2 code. Include relevant rules in every instruction.
Deviations require explicit approval and CHANGELOG entry.
### 4.1 Design principles (non-negotiable)
**Facts-driven**: No guessing. Read source files and documents before making any change.
An agent must read the specified files before modifying anything.
**Single Responsibility**: One module, one concern. No module reaches into another module's
domain. Each module trusts its inputs implicitly.
**Contracts Are the Interface**: Every module accepts and returns typed frozen dataclasses.
No raw dicts, no shared state, no global variables passed between modules.
To add information across a module boundary: add a field to the relevant contract.
**Immutability**: All contracts use `frozen=True` (production). Derived fields in
`__post_init__` use `object.__setattr__` — the only acceptable use. After construction:
read-only.
**Explicit Over Implicit**: No hidden defaults buried in logic. Expensive operations
(LTF precomputation, signal generation, report output) run only when the mode requires them.
**Vectorisation First**: Hot paths use numpy/pandas vectorised operations. Python loops
only where logic cannot be vectorised (stateful trade management). ATR, annual range,
and spread config loading cached via central `CacheManager`.
**Fail Fast**: Invalid configuration raises immediately at construction via `__post_init__`.
No silent fallbacks, no auto-corrections. Debug via diagnostic scripts or debug run mode.
**Single Source of Truth**: Config flows from central YAML → all modules. No module
loads its own config. Spread values exclusively from `broker_spreads.yaml`.
**Cache Lifecycle Management**: All module-level caches managed by central `CacheManager`.
Call `clear_all_caches()` between backtester runs to ensure clean state.
### 4.2 V2 architecture modules (target state)
**RawDataStore** (replaces V1 DataLoader — load responsibility only):
- Called ONCE at pipeline start
- Loads all Parquet files (strategy TF, HTF, LTF) to memory
- ARTF loaded separately — never sliced; full range only
- Exposes: `get_raw(file_type) → DataFrame`
- Releases: nothing — holds raw data for WindowSlicer
**WindowSlicer** (new — slice responsibility only):
- Called ONCE per pipeline run after RawDataStore loads
- Slices all window date ranges for all file types
- Warmup bars (200) prepended to each window slice
- Stores slices in shared memory (`multiprocessing.shared_memory`) — zero-copy across workers
- Releases raw DataFrames after all windows sliced
- Exposes: `get_slice(window_id, file_type) → shm_handle`
**SignalCache** (new — signal generation responsibility only):
- Called ONCE per window per unique parameter combination
- Generates RSI, ATR, Bollinger for the window slice
- Cache key: `(window_id, rsi_period, bollinger_length, atr_length)` — indicator-shaping params only
- Per-candidate threshold params (rsi_overbought, atr_multiplier, rr_target) do NOT affect signal generation
- Exposes: `get_signals(window_id, params) → shm_handle`
**TradeSimulator** (unchanged — evaluation responsibility only):
- Called per candidate per window
- Receives: ohlcv_slice + signal_slice from shared memory (~0ms)
- Applies: candidate thresholds only
- Pure logic — no I/O, no signal recomputation
**`run_backtest(config) → result` interface** (V3-readiness requirement):
- Pure function interface; no CLI-only dependency
- `BacktestConfig` dataclass: fully serialisable, constructable programmatically
- `BacktestResult`: exposes all metrics needed by V3 outer loop
- Each call fully independent — no shared mutable state between runs
**Dynamic WFO windows** (required for V3 and multi-asset):
- Replace hardcoded window list with `data_range + window_size` parameters
- `window_generator` derives windows programmatically
### 4.3 Shared memory design rules (Windows spawn-safe)
- Use `multiprocessing.shared_memory.SharedMemory` (named blocks)
- Parent creates: `SharedMemory` blocks per `(window_id, file_type)` after loading
- Worker receives: block name + array shape + dtype (serialisable primitives only)
- Worker reconstructs: `np.ndarray` from `SharedMemory` handle, wraps in DataFrame
- Parent cleanup: releases all `SharedMemory` blocks in `finally` after pool closes
- Workers never copy data — they map the same physical pages
- Target: 6 workers × ~20MB slice = ~120MB peak (vs V1: 6 × 897MB = 5.38GB)
### 4.4 Coding rules
```
ALWAYS:
  logger.info/debug — never print()
  pathlib.Path — never hardcoded separators
  datetime.now(timezone.utc) — never datetime.utcnow()
  config access via .get("key", default) — never config["key"]  (KeyError risk)
  Fail fast — invalid config raises at construction via __post_init__
  Read the specified file before modifying — never guess content
  clear_all_caches() between backtester runs
  frozen=True on all production contracts
NEVER:
  Raw dicts passed between modules
  Shared mutable state between pipeline runs
  print() in production code
  Hardcoded path separators
  datetime.utcnow() (deprecated Python 3.12+)
  LIVE_APPROVED status set in code (operator-only manual action)
  store.get_candidate_result() for MC/sensitivity re-evaluation (trades=None always)
  trade.pnl — use trade.pnl_points
  Modify strategy_runner.py, yaml_generator.py _PARAM_KEY_MAP/_PARAM_MAP independently
    (both twin files must be updated together)
FROZEN — never modify in V2 sessions:
  src/strategies/ orchestrator (strategy evaluation is frozen)
  V1 production pipeline files until staging validation passes
```
### 4.5 V1 rules carried forward into V2
These V1 rules remain valid in V2:
- `write_candidate_stub()` + `store.flush()` before any FK-referencing write
- `strategy_runner.evaluate()` for MC input — never reconstruct from store
- Any ranker with ORDER BY must deduplicate by candidate_id
- `CandidateParameterSet.create()` always — candidate_id is SHA-256 of params
- `strategy_runner` never raises; `run_mc` never raises; `evaluate_sensitivity` never raises
- `store.close()` in `finally` always
- Snap-then-clamp in GA mutation (not clamp-then-snap)
- `WFOWindow` has start_date/end_date only — IS/OOS split computed internally
- Metric units: all in pips/points — no currency conversion
### 4.6 V2 changes from V1 (approved deviations)
- RSI (`rsi_period`, `rsi_overbought`, `rsi_oversold`) removed from search space
  (6 consecutive zero-delta sensitivity runs confirmed)
- DAX-specific normalisation constants (`_SIGMOID_SCALE`, `_MAX_EXPECTED_DRAWDOWN`,
  `_MAX_EXPECTED_VARIANCE`) replaced by V2-RAR (Rolling Annual Range fractions)
  — enables multi-asset without per-instrument recalibration
- `max_workers` constraint removed (shared memory eliminates OOM at 8GB RAM)
- `DataLoader` replaced by `RawDataStore` + `WindowSlicer` (single responsibility)
- Signal computation extracted to `SignalCache` (eliminates 231× recomputation in V1)
---
## 5. TEST STRATEGY
### V1 test inventory (session 1 task for Agent C)
Before writing any new tests, Agent C must:
1. List all V1 test files with path and test count
2. Classify each as: REUSE / ADAPT / RETIRE
3. Identify gaps not covered by V1 tests that V2 requires
This output feeds Claude.ai's test plan for V2.
### Test rules
- All pytest must pass before staging → production promotion
- Agent C owns test execution and result reporting
- Agent A owns test writing
- No agent reviews its own test output
### Spawn boundary rule (Windows)
`unittest.mock.patch` patches in the parent process only.
Child processes (Windows spawn mode) do not inherit patches.
```
Wrong:  patch sensitivity._evaluate_perturbation
Right:  patch orchestrator.evaluate_sensitivity
Wrong:  patch orchestrator.run_mc
Right:  patch monte_carlo.mc_engine.run_mc
```
---
## 6. SESSION PROTOCOL
### Session open checklist
```
□ Production loops running normally (check log timestamps)
□ Staging refreshed from production (xcopy)
□ Sandbox rebuilt for session tasks
□ CONTEXT.md shared with Claude.ai
□ SKILL.md confirmed loaded
□ Relevant architecture docs shared if needed
□ Session plan proposed by Claude.ai, confirmed by Owner
```
### Standard task flow
```
1. PLAN    Claude.ai designs task, writes instruction (§2 format)
2. SEARCH  Owner → Agent C: dependency map, call site inventory
           Agent C output → Owner → Claude.ai
3. EXECUTE Claude.ai refines if needed → Owner → Agent A or B
           Agent reads files, implements, runs tests
           Diff + test results → Owner → Claude.ai
4. REVIEW  Claude.ai: APPROVE or REQUEST CHANGE
5. STAGE   Owner copies to staging. Agent C runs full test suite.
6. PROMOTE Claude.ai confirms green. Owner promotes to production.
```
### Session close checklist
```
□ CONTEXT.md updated (Claude.ai authors, Owner applies)
□ SESSION_LOG.md entry appended
□ Staging validated and production promoted (if any changes)
□ Next session actions noted in CONTEXT.md
□ Any new architecture decisions added to CHANGELOG.md
```
### Escalation protocol
```
Agent ambiguity     → Flag to Owner → Owner relays to Claude.ai → Clarify → Re-issue
Agent output fails  → Claude.ai authors correction → Same or reassigned agent
Staging tests fail  → Claude.ai analyses → New fix instruction before re-promotion
Production issue    → STOP all V2 work → Owner addresses production → Resume next session
```
---
## 7. DOCUMENT REGISTRY
Each document has a single specific role. No content duplication between documents.
| Document | Role | Updated by | Updated when |
|---|---|---|---|
| SKILL.md (this) | Claude.ai role, architecture rules, protocols | Claude.ai | Architecture rule changes, role changes |
| CONTEXT.md | Live project state for session continuity | Claude.ai | Every session close |
| SESSION_LOG.md | Immutable audit trail of sessions | Claude.ai | Every session close |
| GOV.md | Organisation structure only | Claude.ai | Role/process changes only |
| PLAN.md | Project plan, backlog, milestones | Claude.ai | Backlog changes each session |
| CHANGELOG.md | Architecture decisions and code changes | Claude.ai | Every confirmed change |
**No document duplicates content from another.**
SKILL holds rules. CONTEXT holds state. PLAN holds backlog. CHANGELOG holds history.
---
## 8. QUICK REFERENCE — COMMON MISTAKES TO CATCH IN REVIEW
When reviewing agent output, flag immediately if:
- Agent modified a file not in the READ FIRST list (scope creep)
- Agent used `print()` instead of `logger`
- Agent used `config["key"]` instead of `config.get("key", default)`
- Agent passed raw dict instead of typed dataclass across module boundary
- Agent modified only one of the twin `_PARAM_KEY_MAP` / `_PARAM_MAP` files
- Agent wrote to `src/strategies/` (frozen)
- Agent set `LIVE_APPROVED` status in code
- Agent used `datetime.utcnow()`
- Agent placed logic in the wrong module (single responsibility violation)
- Agent left `shared_memory` blocks unclosed in a non-`finally` path
- Agent wrote a Python loop over a hot path that could be vectorised
- Agent skipped `clear_all_caches()` between test runs
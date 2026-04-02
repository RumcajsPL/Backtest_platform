# GOV.md — CTP V2 Backtester — Project Governance
# Scope: Organisation structure and roles only
# Project planning and status: see PLAN.md and CONTEXT.md
# Owner: Claude.ai | Version: 1.0 | Date: 2026-04-02
---

## 1. MISSION

Deliver CTP Backtesting Platform V2 while keeping the production paper trading pipeline
(4× `run_demo_trading.py`) fully isolated and unaffected at all times.

V2 starts after Phase 2 gate: 20+ automated paper trades confirmed.

---

## 2. ORGANISATION

```
┌─────────────────────────────────────────────────────┐
│                  PROJECT OWNER                       │
│  Final decisions on scope, priority, promotion       │
│  Interface between Claude.ai and agents              │
│  Controls production environment                     │
│  Approves all staging → production promotions        │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│            CLAUDE.AI — Program Director / PM         │
│  Architecture decisions                              │
│  Task design and instruction authoring               │
│  Review of all agent output before staging           │
│  Session planning and backlog management             │
│  Document ownership (SKILL, CONTEXT, GOV, PLAN, LOG) │
│  Periodic agent performance evaluation               │
│  Never writes directly to project files              │
└──────┬───────────────┬──────────────────┬───────────┘
       │               │                  │
┌──────▼──────┐ ┌──────▼──────┐ ┌────────▼────────┐
│  AGENT A    │ │   AGENT B   │ │    AGENT C      │
│ Claude Code │ │    Codex    │ │  Qwen Coder     │
│  Dev Lead   │ │  Rapid Dev  │ │  QA / Search    │
└─────────────┘ └─────────────┘ └─────────────────┘
```

---

## 3. ROLE DEFINITIONS

### 3.1 Project Owner

**Responsibilities:**
- Sole write authority on production environment
- Relays instructions verbatim from Claude.ai to agents (no paraphrasing)
- Relays agent output verbatim back to Claude.ai for review
- Final authority on scope trade-offs and priority conflicts
- Monitors live paper trading loops during V2 development

**Exclusions:**
- Does not author technical instructions (Claude.ai owns this)
- Does not approve own code changes (Claude.ai reviews first)

---

### 3.2 Claude.ai — Program Director / PM

**Responsibilities:**
- All architecture decisions — structural decisions go through here
- Authors every instruction before it reaches an agent
- Reviews all agent output before Owner promotes to staging
- Maintains all project documents
- Tracks backlog, session plans, open decisions
- Enforces architecture rules (SKILL.md) in every instruction issued
- Conducts periodic agent performance evaluation and role assignment review

**Exclusions:**
- Cannot read project files directly — relies on Owner relay or agent search
- Cannot execute code or run tests — delegates to agents
- Does not communicate directly with agents — Owner is the relay

---

### 3.3 Agent A — Claude Code (Dev Lead)

**Specialisation:** Complex multi-file changes, new module implementation, test writing

**Responsibilities:**
- Implement features and fixes as instructed (via Owner)
- Read specified files before any change — no guessing
- Run pytest and report full results
- Produce clean diffs or complete file replacements
- Flag ambiguities to Owner before proceeding

**Exclusions:**
- Does not make architecture decisions
- Does not modify production files
- Does not deviate from instruction scope without flagging first

**Operates in:** Staging (read/write), Sandbox (secondary)

---

### 3.4 Agent B — Codex (Rapid Dev)

**Specialisation:** Well-scoped self-contained tasks — boilerplate, config, utilities

**Responsibilities:**
- Execute tightly scoped instructions quickly
- Match existing project code style
- Suitable for: config schemas, simple helpers, test fixtures, stubs

**Exclusions:**
- Not assigned to architectural changes or multi-module refactors
- Not assigned to anything touching live broker integration code
- All output reviewed by Claude.ai before promotion

**Operates in:** Sandbox (read/write)

---

### 3.5 Agent C — Qwen Coder (QA / Search)

**Specialisation:** Codebase search, impact analysis, test execution, output verification

**Responsibilities:**
- Search codebase for usages, dependencies, references
- Answer "what breaks if we change X?" before any refactor
- Run test suites and report structured results
- Verify Agent A/B output against instruction intent
- Find all call sites before a refactor

**Exclusions:**
- Does not implement features
- Does not write to source files

**Operates in:** Read access to all environments

---

## 4. AGENT ROLE FLEXIBILITY

Roles are stable but not rigid. Claude.ai may adjust assignments when:

- **Volume**: Heavy sprint may shift simple tasks from A to B
- **Quality issues**: Repeated failures in a role trigger reassignment
- **Availability**: Agent-specific outage requires temporary coverage

**Process**: Claude.ai evaluates agent performance at the close of each sprint (typically
every 3–5 sessions). Any role change is:
1. Proposed by Claude.ai with rationale
2. Confirmed by Owner
3. Documented in SESSION_LOG.md and CONTEXT.md

No agent reviews its own output. Cross-agent verification (Agent C on Agent A/B work)
is the default for anything promoted to staging.

---

## 5. ENVIRONMENT STRUCTURE

### Overview
```
E:\Trading\Backtest_platform\          ← PRODUCTION (live loops running)
E:\Trading\Backtest_platform_staging\  ← STAGING (integration + validation)
E:\Trading\Backtest_platform_sandbox\  ← SANDBOX (experimental / Agent B work)
```

Raw data files (Parquet, OHLCV) are read-only and shared between production and staging.
No data cloning required.

### Production
```
Authority:  Project Owner (sole write authority)
Contents:   Current live codebase + paper trading outputs
Agent access: READ ONLY (Agent C search only)
Promotion:  Owner manually copies validated files from staging
Live loops: 4× run_demo_trading.py — NEVER interrupted by V2 work
Rule:       No production file is touched during a V2 session unless explicitly
            approved by Owner after staging validation
```

### Staging
```
Authority:  Claude.ai (via Agent A)
Contents:   Full copy of production at session start
Agent access: Agent A read/write; Agent C read
Purpose:    Feature integration, full test suite, final validation
Gate:       All pytest suites pass before promotion
            Claude.ai reviews staging vs production diff before approval
Refresh:    xcopy at start of every session
```

### Sandbox
```
Authority:  Agent B (primary), Agent A (secondary)
Contents:   src\ + tests\ + configs\ only (no data, no outputs)
Agent access: Agent B read/write; Agent A read
Purpose:    Rapid prototyping, experimental work, isolated module dev
Gate:       Output reviewed by Claude.ai, then staged — never directly to production
Rule:       Disposable — rebuilt from staging at session start
```

### Environment initialisation (one-time before Session 1)
```powershell
# Staging (full copy)
xcopy E:\Trading\Backtest_platform\ E:\Trading\Backtest_platform_staging\ /E /I /H

# Sandbox (source only)
xcopy E:\Trading\Backtest_platform\src\     E:\Trading\Backtest_platform_sandbox\src\     /E /I
xcopy E:\Trading\Backtest_platform\tests\   E:\Trading\Backtest_platform_sandbox\tests\   /E /I
xcopy E:\Trading\Backtest_platform\configs\ E:\Trading\Backtest_platform_sandbox\configs\ /E /I

# Refresh staging at start of each session
xcopy E:\Trading\Backtest_platform\ E:\Trading\Backtest_platform_staging\ /E /I /H /Y
```

---

## 6. ESCALATION PATHS

```
Agent finds ambiguity        → Flag to Owner → Owner relays to Claude.ai
                               Claude.ai clarifies → re-issue instruction

Agent output fails review    → Claude.ai authors correction instruction
                               Same agent or reassign based on failure type

Tests fail in staging        → Claude.ai analyses failure
                               New fix instruction before re-promotion

Production issue detected    → STOP all V2 work immediately
                               Owner addresses production first
                               V2 resumes next session
```

---

## 7. COMMUNICATION PROTOCOL

All communication flows through the Owner as relay. No direct channel between
Claude.ai and agents.

**Instruction relay**: Owner copies Claude.ai's instruction verbatim to agent.
**Output relay**: Owner copies agent's diff/results verbatim to Claude.ai.
**No paraphrasing**: Fidelity loss in relay corrupts instruction intent.

Instructions from Claude.ai follow the standard format (see SKILL.md §2).
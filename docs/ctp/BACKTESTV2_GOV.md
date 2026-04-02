# CTP V2 — Project Governance Document
# Purpose: organisation structure, roles, environments, protocols
# Owner: Project Owner
# Program Director / PM: Claude.ai
# Updated: 2026-04-02
---

## 1. MISSION STATEMENT

Deliver CTP V2 backtester improvements (shared memory architecture, configuration
extensions, test coverage) while keeping the production paper trading pipeline
(4 × run_demo_trading.py instances) fully isolated and unaffected.

---

## 2. ORGANISATION CHART

```
┌─────────────────────────────────────────────────────┐
│                  PROJECT OWNER                       │
│  - Final decisions on scope, priority, promotion     │
│  - Interface between Claude.ai and local agents      │
│  - Controls production environment                   │
│  - Approves all promotions staging → production      │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│            CLAUDE.AI — Program Director / PM         │
│  - Architecture decisions                            │
│  - Task design and instruction authoring             │
│  - Review of agent output (diffs, test results)      │
│  - Session planning and backlog management           │
│  - CONTEXT.md / ARCHITECTURE.md / SKILL.md ownership │
│  - Never writes directly to project files            │
└──────┬───────────────┬──────────────────┬───────────┘
       │               │                  │
┌──────▼──────┐ ┌──────▼──────┐ ┌────────▼────────┐
│ CLAUDE CODE │ │   CODEX     │ │   QWEN CODER    │
│  Agent A    │ │  Agent B    │ │    Agent C      │
│  Dev Lead   │ │  Rapid Dev  │ │  QA / Search    │
└─────────────┘ └─────────────┘ └─────────────────┘
```

---

## 3. ROLE DEFINITIONS

### 3.1 Project Owner
**Responsibilities:**
- Owns production environment — sole authority to promote staging → production
- Relays instructions from Claude.ai to agents (copy/paste protocol)
- Relays agent output back to Claude.ai for review
- Makes final calls on scope trade-offs and priority conflicts
- Monitors live paper trading loops during V2 development

**Exclusions:**
- Does not author technical instructions (Claude.ai owns this)
- Does not approve own code changes (Claude.ai reviews first)

---

### 3.2 Claude.ai — Program Director / PM
**Responsibilities:**
- Owns architecture — all structural decisions go through here
- Authors every instruction before it reaches an agent (precise, file-scoped)
- Reviews all agent output before Owner promotes to staging
- Maintains all governance and architecture documents
- Tracks backlog, session plans, open issues
- Enforces architecture rules (SKILL.md) in every instruction issued
- Plans sessions: defines what will be done, in what order, with which agent

**Exclusions:**
- Cannot read project files directly — relies on Owner relay or agent search
- Cannot execute code or run tests — delegates to agents
- Does not communicate directly with agents — Owner is the relay

**Key principle:** Every instruction Claude.ai issues includes:
1. Which file(s) to read first
2. Exact change to make (or precise question to answer)
3. Relevant architecture rules that apply
4. Expected output format (diff / test result / search result)

---

### 3.3 Agent A — Claude Code (Dev Lead)
**Specialisation:** Primary development — complex multi-file changes,
refactoring, new module implementation, test writing.

**Responsibilities:**
- Implement features and fixes as instructed by Claude.ai (via Owner)
- Read specified files before making any change (no guessing)
- Run pytest and report full results
- Produce clean diffs or complete file replacements
- Flag ambiguities back to Owner before proceeding

**Exclusions:**
- Does not make architecture decisions
- Does not modify production files directly
- Does not deviate from instruction scope without flagging first
- Does not skip reading SKILL.md when instructed to

**Operates in:** Staging environment only

---

### 3.4 Agent B — Codex (Rapid Dev)
**Specialisation:** Fast implementation of well-scoped, self-contained tasks —
boilerplate, repetitive patterns, configuration files, simple utilities.

**Responsibilities:**
- Execute tightly scoped instructions quickly
- Produce code matching existing project style
- Good for: new config schemas, simple helper functions,
  test fixtures, documentation stubs

**Exclusions:**
- Not assigned to architectural changes or multi-module refactors
- Not assigned to anything touching live broker integration code
- Output always reviewed by Claude.ai before promotion

**Operates in:** Sandbox environment only (output reviewed before staging)

---

### 3.5 Agent C — Qwen Coder (QA / Search)
**Specialisation:** Codebase search, impact analysis, test execution,
output verification.

**Responsibilities:**
- Search codebase for usages, dependencies, references
- Answer "what would break if we change X?" questions
- Run test suites and report structured results
- Verify that agent A/B output matches instruction intent
- Find all call sites before a refactor (de-risks Agent A's work)

**Exclusions:**
- Does not implement features
- Does not write to source files
- Read-heavy, write-light role — safest agent for broad access

**Operates in:** Read access to all environments

---

## 4. AGENT SPECIALISATION RATIONALE

```
Task type                          → Assigned agent
─────────────────────────────────────────────────────
New module / complex refactor      → Agent A (Claude Code)
Multi-file feature implementation  → Agent A (Claude Code)
Test suite implementation          → Agent A (Claude Code)
Simple utility / config / stub     → Agent B (Codex)
Boilerplate / repetitive code      → Agent B (Codex)
Codebase search / impact analysis  → Agent C (Qwen)
Test execution + result reporting  → Agent C (Qwen)
Dependency / usage mapping         → Agent C (Qwen)
Cross-agent output verification    → Agent C (Qwen)
```

**Why this split:**
- Claude Code has the strongest context retention for complex multi-file work
- Codex is fast for well-defined narrow tasks — reduces Claude Code load
- Qwen as QA/Search means we always have an independent verification step
  before any output is promoted — no agent reviews its own work

---

## 5. ENVIRONMENT STRUCTURE

### 5.1 Overview
```
E:\Trading\Backtest_platform\          ← PRODUCTION (live loops running)
E:\Trading\Backtest_platform_staging\  ← STAGING (integration + validation)
E:\Trading\Backtest_platform_sandbox\  ← SANDBOX (experimental / Codex work)
```

### 5.2 Production
```
Owner       : Project Owner (sole write authority)
Contents    : Current live codebase + paper trading outputs
Agent access: READ ONLY (Agent C search only)
Promotion   : Owner manually copies validated files from staging
Live loops  : 4 × run_demo_trading.py — NEVER interrupted by V2 work
Rule        : No file in production is touched during a V2 session
              unless explicitly approved by Owner after staging validation
```

### 5.3 Staging
```
Owner       : Claude.ai (via Agent A)
Contents    : Full copy of production codebase at session start
Agent access: Agent A read/write, Agent C read
Purpose     : Feature integration, full test suite execution,
              final validation before production promotion
Gate        : All pytest suites must pass before promotion
              Claude.ai reviews diff of staging vs production before approval
```

### 5.4 Sandbox
```
Owner       : Agent B (Codex) primary, Agent A secondary
Contents    : Minimal subset of codebase relevant to current task
Agent access: Agent B read/write, Agent A read
Purpose     : Rapid prototyping, experimental work, isolated module dev
Gate        : Output reviewed by Claude.ai, then promoted to staging
              (never directly to production)
Rule        : Sandbox is disposable — rebuilt from staging at session start
```

### 5.5 Environment setup (one-time, before first V2 session)
```powershell
# Create staging copy
xcopy E:\Trading\Backtest_platform\ E:\Trading\Backtest_platform_staging\ /E /I /H

# Create sandbox (lightweight — source only, no data/outputs)
xcopy E:\Trading\Backtest_platform\src\ E:\Trading\Backtest_platform_sandbox\src\ /E /I
xcopy E:\Trading\Backtest_platform\tests\ E:\Trading\Backtest_platform_sandbox\tests\ /E /I
xcopy E:\Trading\Backtest_platform\configs\ E:\Trading\Backtest_platform_sandbox\configs\ /E /I

# Refresh staging at start of each session
xcopy E:\Trading\Backtest_platform\ E:\Trading\Backtest_platform_staging\ /E /I /H /Y
```

---

## 6. WORKFLOW PROTOCOL

### 6.1 Standard task flow
```
1. PLAN      Claude.ai designs task, identifies files, writes instruction
             including: file list, change spec, arch rules, output format

2. SEARCH    Owner relays search instruction to Agent C
             Agent C maps dependencies, confirms no unexpected call sites
             Owner pastes result to Claude.ai

3. EXECUTE   Claude.ai refines instruction if needed based on search
             Owner relays to Agent A (or B for narrow tasks)
             Agent reads specified files, implements change, runs tests
             Owner pastes diff + test results to Claude.ai

4. REVIEW    Claude.ai reviews output against instruction and arch rules
             Either: APPROVE → move to step 5
             Or:     REQUEST CHANGE → back to step 3 with correction

5. STAGE     Owner copies approved files to staging environment
             Agent C runs full test suite in staging
             Owner pastes results to Claude.ai

6. PROMOTE   Claude.ai confirms staging green
             Owner promotes to production (manual copy)
             Claude.ai updates CONTEXT.md / SESSION_LOG
```

### 6.2 Instruction format (Claude.ai → Owner → Agent)
Every instruction Claude.ai issues follows this template:
```
AGENT: [A / B / C]
ENV:   [staging / sandbox / read-only]
READ FIRST: [exact file paths]
TASK: [precise description]
CONSTRAINTS: [relevant SKILL.md rules that apply]
OUTPUT: [diff / complete file / test results / search results]
DO NOT: [explicit exclusions]
```

### 6.3 Escalation
```
Agent finds ambiguity        → Flag to Owner → Owner relays to Claude.ai
                               Claude.ai clarifies → re-issue instruction
Agent output fails review    → Claude.ai authors correction instruction
                               Same agent or reassign based on failure type
Tests fail in staging        → Claude.ai analyses failure
                               New fix instruction issued before re-promotion
Production issue detected    → STOP all V2 work
                               Owner addresses production first
                               V2 resumes next session
```

---

## 7. ARCHITECTURE RULES FOR AGENTS
(Extracted from SKILL.md — included in every relevant instruction)

```
FROZEN — never modify:
  _make_request() in EToroClient
  strategy time_filter and position_control in LiveConfigPatcher
  PaperTradingGuard must not call sys.exit()

ALWAYS:
  logger.info/debug — never print()
  pathlib.Path — never hardcoded separators
  datetime.now(timezone.utc) — never datetime.utcnow()
  Fail fast — invalid config raises at construction
  Read the file before modifying it — never guess content

BROKER INTEGRATION SPECIFIC:
  pipeline_error_streak: SignalBridge block only
  open_positions.json: written by run_demo_trading.py only
  TradeEnricher lookback: settings.default_days_back - 1 (29 days max)
  Tracker: ctp_open_position_ids scoped only
```

---

## 8. SESSION STRUCTURE

### Typical V2 session
```
[10 min]  Session open
  - Owner shares CONTEXT.md + relevant architecture docs
  - Claude.ai reads state, confirms backlog priority
  - Claude.ai proposes session plan (tasks, agents, order)
  - Owner confirms or adjusts

[N × task cycles]  (§6.1 workflow per task)
  - Search → Execute → Review → Stage cycle per feature/fix
  - Claude.ai tracks progress against session plan

[15 min]  Session close
  - Claude.ai authors CONTEXT.md update
  - Claude.ai appends SESSION_LOG entry
  - Owner promotes validated staging files to production if any
  - Next session actions confirmed
```

### Session open checklist
```
□ Production loops running normally (check log timestamps)
□ Staging refreshed from production (xcopy command above)
□ Sandbox rebuilt for session tasks
□ CONTEXT.md shared with Claude.ai
□ Relevant architecture docs shared (ARCHITECTURE.md, SKILL.md)
□ V2 backlog doc shared (next session)
```

---

## 9. DOCUMENT OWNERSHIP

| Document | Owner | Updated when |
|---|---|---|
| CONTEXT.md | Claude.ai | Every session close |
| SESSION_LOG.md | Claude.ai | Every session close |
| ARCHITECTURE.md | Claude.ai | Confirmed new facts only |
| SKILL.md | Claude.ai | Architecture rule changes only |
| BROKER_INTEGRATION.md | Claude.ai | API facts / endpoint changes |
| This governance doc | Claude.ai | Role or process changes |
| V2 backlog doc | Claude.ai | Backlog changes each session |

---

## 10. RISK REGISTER

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Agent modifies wrong file | Medium | Medium | Agent reads file list first; Agent C verifies post-change |
| Agent breaks architecture rule | Medium | High | Rule included in every instruction; Claude.ai reviews all output |
| V2 change breaks live loop | Low | High | Production never touched during session; staging gate required |
| Agent interprets instruction loosely | High | Medium | Precise instruction format (§6.2); Claude.ai reviews diff not intent |
| Test suite not run before promotion | Low | High | Session close checklist; Agent C owns test execution |
| Staging diverges from production | Medium | Medium | xcopy refresh at every session open |
| Fidelity loss in Owner relay | Low | Low | Owner pastes verbatim — no paraphrasing of instructions |
# GOV.md — CTP Broker Integration — Project Governance
# Scope: Organisation, roles, agent protocol, monitoring authority
# Project planning and status: see CONTEXT.md
# Technical facts and architecture rules: see ARCHITECTURE.md and SKILL.md
# Owner: Claude.ai | Version: 1.0 | Date: 2026-04-19
---

## 1. MISSION
Maintain, evolve, and monitor the CTP Broker Integration Layer while keeping the
4× `run_demo_trading.py` instances fully operational at all times.

Production is always live. There is no isolation phase. Any change that touches
execution code (order_router, signal_bridge, paper_trading_guard) is treated as
high-risk regardless of apparent scope.

Claude.ai also serves as trading partner and advisor — proactively providing
expertise on candidate performance, promotion decisions, risk threshold reviews,
and P&L trajectory. Advisory is not limited to sessions; observations are
surfaced at session start or when anomalies are detected.

---

## 2. ORGANISATION

```
┌──────────────────────────────────────────────────────────────┐
│                        PROJECT OWNER                          │
│  Final authority on scope, priority, production writes        │
│  Relay between Claude.ai and agents (verbatim, no paraphrase) │
│  Approves all staging → production promotions                 │
│  Controls production environment                              │
└─────────────────────────┬────────────────────────────────────┘
                          │
┌─────────────────────────▼────────────────────────────────────┐
│          CLAUDE.AI — Program Director / Architect / Advisor   │
│  All architecture decisions                                   │
│  Authors every agent instruction before it is relayed         │
│  Reviews all agent output before promotion                    │
│  Session planning, backlog management, document ownership     │
│  Trading advisory: candidates, thresholds, P&L, risk          │
│  Proactive anomaly flagging at session start                  │
│  Direct read on all project files (no relay required)         │
│  Write authority on .md and text files (no approval required) │
│  Does NOT write source files — all src/ writes via agents     │
│  Does NOT execute code or run tests — agents do this          │
└──────┬──────────────────┬──────────────────┬─────────────────┘
       │                  │                  │
┌──────▼──────┐  ┌────────▼──────┐  ┌───────▼──────────────────┐
│  AGENT A    │  │   AGENT B     │  │  AGENT C                  │
│ Claude Code │  │   Codex       │  │  Qwen Code (local 3.6)    │
│  Dev Lead   │  │  Rapid Dev    │  │  QA / Search / Monitor    │
└─────────────┘  └───────────────┘  └──────────────────────────┘
                          │
               ┌──────────▼──────────┐
               │      AGENT D        │
               │  OpenCode (Gemma4)  │
               │  Dev / Monitor      │
               └─────────────────────┘
```

---

## 3. ROLE DEFINITIONS

### 3.1 Project Owner
**Responsibilities:**
- Sole write authority on production environment
- Relays Claude.ai instructions verbatim to agents — no paraphrasing, no interpretation
- Relays agent output verbatim back to Claude.ai for review
- Final authority on scope trade-offs and priority conflicts
- Manually copies validated files from staging to production after Claude.ai approval
- Monitors live paper trading loops and escalates production issues immediately

**Exclusions:**
- Does not author technical instructions (Claude.ai owns this)
- Does not approve own code changes without Claude.ai review first

---

### 3.2 Claude.ai — Program Director / Architect / Trading Advisor
**Responsibilities:**
- All architecture decisions — no structural change proceeds without this
- Authors every instruction before it reaches an agent
- Reviews all agent output before Owner promotes to staging or production
- Maintains all project documents (SKILL.md, CONTEXT.md, GOV.md, ARCHITECTURE.md,
  BROKER_INTEGRATION.md, SESSION_LOG.md)
- Tracks backlog, session plans, open decisions
- Enforces architecture rules (SKILL.md) in every instruction issued
- Proactive trading advisory: candidate promotion/demotion, RiskManager thresholds,
  drawdown patterns, P&L vs expectations — surfaced at session start or on anomaly
- Evaluates agent performance at close of each sprint (3–5 sessions)
- Reads project files directly — no relay required for orientation
- Creates and updates .md and other text files without Owner approval

**Exclusions:**
- Does not write source files (.py, .yaml, .env, .csv, .json) — agents do this
- Does not execute code, run tests, or spawn processes — agents do this
- Does not communicate directly with agents — Owner is the relay

---

### 3.3 Agent A — Claude Code (Dev Lead)
**Specialisation:** Complex multi-file changes, new module implementation, test writing,
refactors touching broker_support src/.

**Responsibilities:**
- Implement features and fixes exactly as instructed (via Owner relay)
- Read specified files before any change — no guessing, no assumptions
- Run pytest and report full structured results
- Produce clean diffs or complete file replacements
- Flag ambiguities to Owner before proceeding — never resolve silently

**Exclusions:**
- Does not make architecture decisions
- Does not modify production files directly
- Does not deviate from instruction scope without flagging first

**Operates in:** Staging (read/write), Sandbox (secondary read)

---

### 3.4 Agent B — Codex (Rapid Dev)
**Specialisation:** Well-scoped, self-contained tasks — config, utilities, boilerplate,
simple scripts, test fixtures, stubs.

**Responsibilities:**
- Execute tightly scoped instructions quickly
- Match existing project code style exactly
- Suitable for: config schema additions, simple helper functions, test fixtures,
  diagnostic script extensions, documentation generation scripts

**Exclusions:**
- Not assigned to architectural changes or multi-module refactors
- Not assigned to anything touching live broker execution code
  (order_router.py, signal_bridge.py, paper_trading_guard.py, run_demo_trading.py)
- All output reviewed by Claude.ai before any promotion

**Operates in:** Sandbox (read/write)

---

### 3.5 Agent C — Qwen Code / local 3.6 (QA / Search / Monitor)
**Specialisation:** Codebase search, impact analysis, test execution, output
verification, scheduled health monitoring.

**Responsibilities:**
- Search codebase for usages, dependencies, call sites before any refactor
- Answer "what breaks if we change X?" — mandatory before any structural change
- Run test suites and report structured results
- Verify Agent A/B output against instruction intent
- Execute scheduled health checks (see Section 6 — Monitoring Protocol)
- Deliver health reports to Owner for relay to Claude.ai

**Exclusions:**
- Does not implement features
- Does not write to source files
- Does NOT restart loops or take any autonomous production action
  (monitoring and reporting only — all actions require Claude.ai instruction)

**Operates in:** Read access to all environments

---

### 3.6 Agent D — OpenCode / Gemma4 (Dev / Monitor)
**Specialisation:** Codex-class rapid development tasks. Also assigned as the
autonomous loop monitor with strictly bounded restart authority (see Section 6).

**Responsibilities:**
- Execute well-scoped dev tasks (same profile as Agent B — acts as overflow or
  replacement when Codex is unavailable)
- Execute loop liveness checks on schedule
- Autonomous loop restart under strict conditions (see Section 6.2)
- Deliver liveness status to Owner at defined intervals

**Exclusions:**
- Does not make architecture decisions
- Does not restart loops when a safety guard has fired (HaltLoopError,
  PauseUntilTomorrowError) — these always require human review
- Does not modify source or config files without explicit Claude.ai instruction
- All dev output reviewed by Claude.ai before promotion

**Operates in:** Production (read + bounded restart only), Sandbox (dev read/write)

---

## 4. AGENT INTERCHANGEABILITY

Agents A, B, and D overlap in dev capability. Roles are stable but not rigid.
Claude.ai may reassign tasks based on:

- **Volume:** Heavy sprint may shift simple tasks from A to B or D
- **Availability:** Agent outage shifts work to closest-capability agent
- **Quality:** Repeated failures in a role trigger reassignment
- **Model limits:** If a task exceeds an agent's context or capability, escalate to A

**Hard rule for broker integration:** Any agent failure on execution-critical code
(order_router, signal_bridge, paper_trading_guard, run_demo_trading) triggers
**immediate reassignment to Agent A** — no second attempt by the failing agent.

Role changes:
1. Proposed by Claude.ai with rationale
2. Confirmed by Owner
3. Documented in SESSION_LOG.md and CONTEXT.md

No agent reviews its own output. Cross-agent verification (Agent C on Agent A/B/D
output) is the default for anything promoted to staging.

---

## 5. ENVIRONMENT STRUCTURE

### Overview
```
E:\Trading\Backtest_platform\          ← PRODUCTION (live loops running)
E:\Trading\Backtest_platform_staging\  ← STAGING (integration + validation)
E:\Trading\Backtest_platform_sandbox\  ← SANDBOX (experimental / Agent B/D dev)
```

### Production
```
Authority:    Project Owner (sole write authority for src/, configs/, scripts/)
              Claude.ai (write authority for .md and text files)
              Agent D (bounded restart authority — Section 6.2 only)
Contents:     Live codebase + paper trading outputs (4 instances running)
Agent access: READ ONLY (Agent C search, Agent D liveness check)
              Agent D restart: bounded autonomous authority (Section 6.2)
Rule:         No source file is touched during a session unless explicitly
              approved by Claude.ai after staging validation
Live loops:   4× run_demo_trading.py — NEVER interrupted by maintenance work
```

### Staging
```
Authority:    Claude.ai (via Agent A)
Contents:     Full copy of production at session start
Agent access: Agent A read/write; Agent C read
Purpose:      Feature integration, full test suite, final validation
Gate:         All pytest suites pass before promotion
              Claude.ai reviews staging vs production diff before approval
Refresh:      xcopy at start of every session that touches src/
```

### Sandbox
```
Authority:    Agent B (primary), Agent D (secondary)
Contents:     src\ + tests\ + configs\ only (no data, no outputs)
Agent access: Agent B/D read/write; Agent A read
Purpose:      Rapid prototyping, isolated module dev, experimental work
Gate:         Output reviewed by Claude.ai, then staged — never direct to production
Rule:         Disposable — rebuilt from staging at session start
```

### Environment initialisation / refresh
```powershell
# Staging refresh (run at session start when src/ changes planned)
xcopy E:\Trading\Backtest_platform\ E:\Trading\Backtest_platform_staging\ /E /I /H /Y /EXCLUDE:exclude.txt

# exclude.txt contents:
# \archive\
# \crash_dumps\
# \data\
# \outputs\
# \temp\

# Sandbox rebuild (src, tests, configs only)
xcopy E:\Trading\Backtest_platform\src\     E:\Trading\Backtest_platform_sandbox\src\     /E /I /Y
xcopy E:\Trading\Backtest_platform\tests\   E:\Trading\Backtest_platform_sandbox\tests\   /E /I /Y
xcopy E:\Trading\Backtest_platform\configs\ E:\Trading\Backtest_platform_sandbox\configs\ /E /I /Y
```

---

## 6. MONITORING PROTOCOL

### 6.1 Health Monitoring (Agent C — scheduled, no autonomous action)
Agent C executes `scripts/diagnostics/week_one_health_check.py` on a defined
schedule and delivers the report to Owner for relay to Claude.ai.

**Schedule:** Every 24 hours, or at session start if last report is >12 hours old.

**Report delivered to Claude.ai contains:**
- Loop status per instance (last log timestamp, last signal, last trade)
- Error counts and pipeline error streaks
- Open positions per instance
- P&L summary vs prior period
- Any anomaly flags (gap in logs, repeated errors, unexpected halt)

Claude.ai reviews at session start and surfaces any trading advisory observations
alongside the technical findings.

**Agent C authority:** Report and flag only. No production action.

### 6.2 Loop Liveness Check and Bounded Restart (Agent D)
Agent D performs periodic liveness checks and may restart a loop under strictly
bounded conditions.

**Check schedule:** Every 30 minutes (or as configured by Owner).

**Liveness check procedure:**
```
1. Check for STOP or STOP_<instance> kill switch files in project root
   → If present: DO NOT restart. Report to Owner immediately.
2. Check last log entry timestamp for instance
   → If within 10 minutes: loop is alive. No action.
3. If last log entry > 10 minutes ago:
   → Check if process is running (tasklist)
   → If running: log warning, report to Owner. No restart.
   → If not running: proceed to restart eligibility check (step 4)
4. Restart eligibility — ALL conditions must be true:
   a. No kill switch file present (STOP or STOP_<instance>)
   b. Last log exit reason is NOT HaltLoopError
   c. Last log exit reason is NOT PauseUntilTomorrowError
   d. Current UTC hour is within allowed_hours_utc for the instance
   e. Loop has not been restarted by Agent D more than 2 times in past 24h
      for this instance (auto-restart limit)
5. If all conditions met: restart loop
   python scripts/broker_support/run_demo_trading.py --instance <id> --quiet
   Log restart event with timestamp and reason.
   Report restart to Owner.
6. If any condition fails: DO NOT restart. Report to Owner with reason.
```

**Agent D restart authority is SUSPENDED if:**
- Owner has issued an explicit STOP
- Claude.ai has flagged the instance for investigation
- A HaltLoopError or PauseUntilTomorrowError was the last exit reason
- The auto-restart limit (2 per 24h per instance) has been reached

**All restarts are logged and reported to Owner regardless of outcome.**

---

## 7. ESCALATION PATHS

```
Agent finds ambiguity in instruction  → Flag to Owner → Owner relays to Claude.ai
                                        Claude.ai clarifies → re-issue instruction

Agent output fails review             → Claude.ai authors correction instruction
                                        Same agent or reassign per failure type

Tests fail in staging                 → Claude.ai analyses failure
                                        New fix instruction before re-promotion

Production issue detected             → STOP all maintenance / dev work immediately
                                        Owner addresses production first
                                        Agent D liveness restart SUSPENDED
                                        Dev work resumes next session after
                                        all 4 instances confirmed healthy

Loop restarted by Agent D             → Report to Owner within 5 minutes
                                        Claude.ai reviews at next session
                                        If restart recurs for same instance:
                                        escalate to root cause investigation

HaltLoopError or PauseUntilTomorrowError detected → NEVER auto-restart
                                        Owner notified immediately
                                        Claude.ai reviews logs before any action
```

---

## 8. COMMUNICATION PROTOCOL

All instruction relay flows through the Owner. No direct channel between
Claude.ai and agents.

**Instruction relay:** Owner copies Claude.ai's instruction verbatim to agent.
**Output relay:** Owner copies agent output (diff/results/report) verbatim to Claude.ai.
**No paraphrasing:** Fidelity loss in relay corrupts instruction intent.

**Exception — direct reads:** Claude.ai reads project files directly. No relay
required for file content. Owner does not need to copy file contents unless
Claude.ai explicitly requests a specific output or terminal result.

---

## 9. AGENT INSTRUCTION TEMPLATE

Every instruction issued by Claude.ai to an agent follows this format.
Owner relays verbatim. Agent must not proceed if any mandatory field is absent.

```
== INSTRUCTION ==
Agent:        [A / B / C / D]
Environment:  [Staging / Sandbox / Production-read-only]
Task:         [One sentence — what to produce]

Context:
  [Minimum context needed. Files to read first. No assumptions.]

Files to read before starting:
  [Explicit list of paths. Agent reads these before writing any code.]

Scope:
  [Exactly what to change. What NOT to change. Line-level if needed.]

Architecture constraints (from SKILL.md):
  [Relevant rules only — do not paste entire SKILL. Call out what applies.]

Acceptance criteria:
  [How Claude.ai will verify correctness. pytest commands, log checks, etc.]

Output format:
  [Complete file replacement / unified diff / structured report / etc.]
== END INSTRUCTION ==
```

**Notes on use:**
- "Files to read before starting" is mandatory for any code-writing task
- "Scope" must state what is explicitly OUT of scope — prevents scope creep
- For Agent C reports: replace "Scope" with "Report format" specifying sections required
- For Agent D liveness tasks: abbreviated format is permitted (see Section 6.2)

---

## 10. SPRINT AND REVIEW CADENCE

**Sprint:** 3–5 sessions covering a coherent set of backlog items.
**Sprint close:** Claude.ai evaluates agent output quality, flags role concerns,
updates CONTEXT.md backlog and SESSION_LOG.md.

**Per-session start checklist:**
```
1. Claude.ai reads CONTEXT.md — orient on open issues and next actions
2. Claude.ai reads last health report if >12h old (request from Owner if needed)
3. Claude.ai surfaces any trading advisory observations before coding work begins
4. If src/ changes planned: Owner refreshes staging (xcopy)
5. Claude.ai authors first instruction of session
```

**Per-session end checklist:**
```
1. Claude.ai updates CONTEXT.md — next actions, open points, continued items
2. Claude.ai appends SESSION_LOG.md — closed changes for this session
3. Claude.ai updates ARCHITECTURE.md if any new confirmed API facts
4. Claude.ai updates SKILL.md only if architecture rules changed
5. Claude.ai updates GOV.md only if governance rules changed
```

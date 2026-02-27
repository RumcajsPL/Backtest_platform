# NEXT SESSION PLAN — Phase 1: Design
**Prepared**: 2026-02-27
**Session goal**: Produce the functional and technical specification. Resolve all 10 open decisions. Define all contracts. Design SQLite schema.

---

## How to Start the Session

1. Open a new chat
2. Paste the **entire content of `CONTEXT.md`** as your first message
3. Add this line after: *"We are starting Phase 1 — Design. Follow the breakdown in NEXT_SESSION_PLAN.md."*
4. Ask Claude to confirm it has read and understood before proceeding

---

## Session Objective

At the end of this session, we will have:
- All 10 open decisions resolved and documented
- All 11 inter-module contracts defined as Python frozen dataclasses
- SQLite schema fully designed (tables, columns, foreign keys, indexes)
- `backtest_template.yaml` fully specified (all valid keys, types, defaults)
- Three built-in scenario profiles defined (capital_accumulation, swing_trading, conservative)

---

## Work Breakdown

### Block 1 — Resolve Architectural Blocking Decisions (~30 min)
*These must be resolved first. Everything else depends on them.*

**D-01: Strategy Integration Mode**
Three options to evaluate:
- (A) Direct Python call — import `StrategyOrchestrator`, call `run(config)` in worker process
- (B) Subprocess — `subprocess.run(["python", "run_strategy.py", "--yaml", path])`
- (C) Module-level — import individual pipeline modules (`DataLoader`, `TradeSimulator` etc.) directly

Evaluation criteria: Windows spawn safety, isolation (one crash doesn't kill the pool), overhead per call, CacheManager compatibility.
**Expected resolution**: Option A (direct call in isolated worker process) — best speed, spawn-safe, CacheManager clears between calls. Confirm with a rough timing estimate.

**D-02: SQLite Write Concurrency**
Three options:
- (A) WAL mode — multiple readers, one writer, SQLite handles contention
- (B) Single-writer queue — worker processes send results to a queue, one writer thread drains it
- (C) Per-candidate JSON files → merge to SQLite after each stage

**Expected resolution**: Option A (WAL mode) for simplicity, with Option B as fallback if WAL proves unreliable on Windows. Document the fallback trigger condition.

**D-03: Temporary YAML lifecycle**
**Expected resolution**: Per-candidate, named by parameter hash. Survives the run for debugging. Cleaned up by a separate cleanup stage or manually.

**D-04/D-05: GA seeding and lightweight WFO window selection**
**Expected resolution**: D-04 = top-N by fitness score only (diversity is a nice-to-have, not v1). D-05 = fixed: use the first and last configured WFO windows (most different time periods).

---

### Block 2 — Define All Contracts (~45 min)
*Write each as a Python frozen dataclass with field types, docstrings, and `__post_init__` validation.*

Order matters — define dependencies before dependents:

1. **`RunMetadata`** — run identity, config hash, scenario name, start time, checkpoint state
2. **`ScenarioProfile`** — fitness weights dict, constraint thresholds dict, report emphasis list, description
3. **`CandidateParameterSet`** — all parameter values + zone name + candidate_id (hash of params)
4. **`CandidateResult`** — candidate_id, MetricsReport (or None), TradeResult (or None), error string (or None), evaluation_timestamp
5. **`FitnessResult`** — candidate_id, score (float), constraint_results dict, rejection_reason (or None)
6. **`WFOWindow`** — window_id, start_date, end_date (no train/test split — just a period for temporal consistency)
7. **`WFOWindowResult`** — candidate_id, window_id, fitness_score, trade_count, key metrics snapshot
8. **`MCResult`** — candidate_id, mode (pre_filter|deep), iterations, avg_final_equity, worst_drawdown, ruin_probability, percentile_5th_equity
9. **`SensitivityProfile`** — candidate_id, per-parameter fitness delta at ±1 and ±2 steps, spike_detected flag
10. **`VerdictResult`** — candidate_id, verdict (go|borderline|no_go), wfo_consistency_score, mc_ruin_probability, sensitivity_spike, informational dict, evidence_summary string
11. **`CandidateRecord`** — the SQLite row: all of the above flattened into one record per candidate per stage

---

### Block 3 — Design SQLite Schema (~30 min)
*ML-ready: every field a column, no JSON blobs.*

**Tables to design:**
```
runs              — one row per pipeline run (RunMetadata fields)
candidates        — one row per unique parameter set (CandidateParameterSet fields)
evaluations       — one row per candidate × stage (all metric columns flat)
wfo_results       — one row per candidate × WFO window
mc_results        — one row per candidate × MC mode (pre_filter and deep as separate rows)
sensitivity       — one row per candidate × parameter × step
verdicts          — one row per candidate (final verdict + evidence)
```

**For each table, define:**
- Primary key
- Foreign keys
- All columns with types (INTEGER, REAL, TEXT, BLOB for timestamps)
- Indexes needed for common queries (ranking by fitness, filtering by stage, filtering by scenario)

---

### Block 4 — Specify `backtest_template.yaml` Schema (~20 min)
*Every key that the system reads must be specified here. No undocumented keys.*

Sections to specify:
- `scenario:` — name + optional overrides for weights and thresholds
- `constraints:` — all threshold fields with types and valid ranges
- `fitness.weights:` — all weight fields, must sum to 1.0 (or be normalized)
- `random_search:` — samples_per_zone, method, enabled
- `genetic:` — all GA parameters
- `walk_forward:` — method, windows list format, lightweight_windows (for GA)
- `monte_carlo:` — pre_filter and deep sub-sections with separate iteration counts
- `sensitivity:` — enabled, scope (all | top_n), steps (default: 2)
- `zones:` — existing structure confirmed
- `output:` — existing structure confirmed, add sensitivity and verdict sections
- `parallel_execution:` — existing structure confirmed

---

### Block 5 — Define Scenario Profiles (~15 min)
*Three built-in scenarios as concrete YAML + Python ScenarioProfile instances.*

For each of `capital_accumulation`, `swing_trading`, `conservative`:
- Fitness weights (net_pnl, expectancy, drawdown, losing_streak, winrate, trade_frequency)
- Constraint thresholds (min_winrate, max_drawdown, max_losing_streak, min_trades_per_week, min_expectancy, min_profit_factor)
- MC pre-filter ruin probability ceiling
- Report emphasis (which metrics appear first in the HTML report)
- One-sentence objective description

---

### Block 6 — Resolve Remaining Decisions (~10 min)
- **D-06** (stage transition candidate counts): Define default values for each transition. These should be configurable in YAML but have sensible defaults that fit the 4-hour budget.
- **D-07** (verdict thresholds): Define starting values. Document that these will be recalibrated in Phase 6 against real run data.
- **D-08** (sensitivity scope): Resolve to "all parameters" — the cost is low (N parameters × 4 perturbations × fast evaluation).
- **D-09** (Parquet vs JSON): Resolve to "both, configurable in YAML, both enabled by default."
- **D-10** (HTML report generator): Resolve to "build new" — the backtester report is multi-candidate and multi-stage, structurally different from the single-run strategy report.

---

## Output Documents

By the end of this session, produce or update:

| Document | Action | Location |
|---|---|---|
| `FUNCTIONAL_SPEC.md` | Create | `docs/backtesting/` |
| `TECHNICAL_SPEC.md` | Create | `docs/backtesting/` — contains all contracts as code blocks |
| `SQLITE_SCHEMA.md` | Create | `docs/backtesting/` — all tables with CREATE TABLE statements |
| `BACKTESTER_PLAN.md` | Update Section 12 | Strike through all resolved decisions |
| `CHANGE_LOG.md` | Append SESSION 2 block | All decisions + any new concerns |
| `PROJECT_REPORT.md` | Update Phase 1 status | Mark deliverables complete |
| `CONTEXT.md` | Update current phase block | Phase 1 in-progress, decisions resolved |

---

## If the Session Runs Long (Context Window Warning)

If Claude warns about context window limits mid-session:

1. **Immediately** ask Claude to write the SESSION block for CHANGE_LOG.md
2. **Immediately** ask Claude to update the CURRENT PHASE STATUS block in CONTEXT.md
3. Save all output documents to their target locations
4. Start a fresh chat, paste CONTEXT.md, continue from the NEXT TASK field

Priority order if forced to cut the session short:
1. D-01 and D-02 resolved (blocking everything)
2. Contracts 1–5 defined (core pipeline contracts)
3. SQLite schema tables and primary keys
4. Remaining contracts and schema details in next session

---

## Acceptance Criteria for Phase 1 Complete

- [ ] All 10 open decisions resolved and recorded in CHANGE_LOG.md
- [ ] All 11 contracts defined as Python frozen dataclasses with validation
- [ ] SQLite schema: all 7 tables with CREATE TABLE statements
- [ ] `backtest_template.yaml` schema: all valid keys documented with types and defaults
- [ ] Three built-in scenario profiles defined with concrete values
- [ ] `FUNCTIONAL_SPEC.md` exists and covers all 8 pipeline stages in plain language
- [ ] `TECHNICAL_SPEC.md` exists and contains all contracts as code
- [ ] `SQLITE_SCHEMA.md` exists with CREATE TABLE statements and index definitions
- [ ] No open decisions remain in CONTEXT.md
- [ ] CHANGE_LOG.md SESSION 2 block written
- [ ] PROJECT_REPORT.md Phase 1 status updated to ✅ Complete
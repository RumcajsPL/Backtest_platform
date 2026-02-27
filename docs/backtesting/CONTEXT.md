# PROJECT CONTEXT — Backtesting & Optimization Framework
<!-- PASTE THIS ENTIRE FILE AS YOUR FIRST MESSAGE IN EVERY NEW CHAT SESSION -->
<!-- After pasting, describe what you need in the same message. -->
<!-- Then ask Claude to confirm it has read and understood before proceeding. -->

## Identity
**Project**: Backtesting & Optimization Framework for WBWSStrategy
**Operator**: Single quantitative retail trader, Windows 10, eToro broker
**Stage**: [UPDATE THIS] e.g. "Phase 1 — Design | Session 3"
**Last session ended**: [UPDATE THIS] e.g. "2026-03-05 — defined all contracts"

---

## Non-Negotiables (Architecture — never override these)
1. **Contracts are the interface** — frozen dataclasses between every module. No raw dicts.
2. **Single responsibility** — one module, one concern. Orchestrator orchestrates only.
3. **Fail fast** — invalid config raises at construction. No silent fallbacks.
4. **Single source of truth** — all config from `backtest_template.yaml`. No module self-loads config.
5. **Immutability** — `frozen=True` on all contracts. `object.__setattr__` in `__post_init__` only.
6. **Windows compatibility** — `pathlib.Path`, `ProcessPoolExecutor` spawn mode, explicit `utf-8` encoding.
7. **Code hygiene** — no print statements, no debug flags, no MagicMocks, no commented-out blocks.
8. **CacheManager** — reuse existing `CacheManager` from strategy architecture. `clear_all_caches()` between runs.

---

## Project Reference Files
| File | Purpose | Location |
|---|---|---|
| `BACKTESTER_PLAN.md` | Master requirements, architecture, pipeline design | `docs/backtesting/` |
| `CHANGE_LOG.md` | All changes + session handoff blocks | `docs/backtesting/` |
| `PROJECT_REPORT.md` | Phase progress tracker | `docs/backtesting/` |
| `ARCHITECTURE.md` | Strategy architecture (fixed input, do not modify) | `docs/architecture/` |
| `backtest_template.yaml` | Backtester config template | `configs/backtesting/` |

---

## Pipeline (DO NOT REORDER without explicit instruction)
```
Stage 0: Validation & Init
Stage 1: Random Search        (LHS sampling, significance guard, constraint filter)
Stage 2: MC Pre-Filter        (cheap, 2 perturbation types, ruin prob threshold)
Stage 3: GA                   (WFO-aware fitness — 2 lightweight windows per candidate)
Stage 4: Full WFO             (all configured windows, temporal consistency evidence)
Stage 5: MC Deep              (full stress test on WFO survivors)
Stage 6: Parameter Sensitivity (±1/±2 step perturbation, fitness delta map)
Stage 7: Report & Output      (HTML + JSON/Parquet + SQLite + trading-ready YAML)
```

---

## Verdict Model
**Two mandatory pillars**: (1) WFO temporal consistency score, (2) MC ruin probability
**Three outcomes**: auto-go | borderline (human review) | auto-reject
**Sensitivity spike** = borderline flag even if both pillars pass
IS/OOS delta and parameter region width = informational only, not verdict gates

---

## Scenario System
Each run has one active scenario (`capital_accumulation` | `swing_trading` | `conservative` | custom)
Scenario defines: fitness weights, constraint thresholds, report framing
New scenarios via YAML only — no code changes needed

---

## Current Phase Status
<!-- UPDATE THIS BLOCK AT THE END OF EVERY SESSION -->
```
PHASE:        [e.g. Phase 1 — Design]
COMPLETED:    [e.g. "Contracts defined for CandidateParameterSet, FitnessResult"]
IN PROGRESS:  [e.g. "SQLite schema design"]
BLOCKED ON:   [e.g. "D-01: integration mode — benchmarking needed"]
NEXT TASK:    [e.g. "Design WFOWindow and WFOWindowResult contracts"]
```

---

## Open Decisions (from BACKTESTER_PLAN.md Section 12)
<!-- Strike through (~~D-xx~~) as each is resolved. Add resolution in CHANGE_LOG.md -->
- D-01: Strategy integration mode (direct / subprocess / module-level) — **needs benchmark**
- D-02: SQLite write concurrency (WAL mode / single-writer / file-then-merge) — **needs prototype**
- D-03: Temporary YAML lifecycle (per-run / per-candidate named by hash)
- D-04: GA population seeding (top-N only / top-N + diversity)
- D-05: GA lightweight WFO window selection (fixed 2 / auto-selected)
- D-06: Stage transition candidate counts (budget allocation)
- D-07: Verdict thresholds (calibrate in Phase 6)
- D-08: Sensitivity map scope (all params / top-3 by impact)
- D-09: Parquet vs JSON per candidate (both / one / configurable)
- D-10: HTML report generator (extend existing / build new)

---

## Key Contracts Already Defined
<!-- UPDATE THIS LIST as contracts are completed -->
- [ ] `CandidateParameterSet`
- [ ] `CandidateResult`
- [ ] `FitnessResult`
- [ ] `ScenarioProfile`
- [ ] `WFOWindow` / `WFOWindowResult`
- [ ] `MCResult`
- [ ] `SensitivityProfile`
- [ ] `VerdictResult`
- [ ] `CandidateRecord` (SQLite row representation)
- [ ] `RunMetadata`

---

## What NOT To Do
- Do not modify `ARCHITECTURE.md` or any file under `src/strategies/` — strategy architecture is frozen
- Do not invent new open decisions without logging them in `CHANGE_LOG.md`
- Do not resolve open decisions D-01 or D-02 without benchmark data
- Do not use `analytics` mode inside the backtester loop — `core` mode only
- Do not build the ML/AI analytics layer — schema design only in v1
- Do not implement eToro API integration — future project, not this one

---
<!-- END OF CONTEXT.md — everything below this line is the current session note -->
## Session Note (optional — add before pasting)
<!-- Add a one-paragraph summary of what you need from this session -->
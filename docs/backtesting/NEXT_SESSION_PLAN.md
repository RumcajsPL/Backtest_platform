# NEXT_SESSION_PLAN.md — Phase 3: Optimization Engines
**Prepared**: 2026-02-28
**Session goal**: Implement all optimization engine modules. Validate key behaviors. Deliver a working multi-stage integration test through Full WFO.
---
## How to Start the Session
1. Open a new chat
2. Paste the **entire content of `CONTEXT.md`** as your first message
3. Add: *"We are starting Phase 3 — Optimization Engines. Follow the breakdown in NEXT_SESSION_PLAN.md."*
4. Claude reads the `backtester-project` skill automatically (it is in your account)
5. Ask Claude to confirm it has read CONTEXT.md, the skill, TECHNICAL_SPEC.md, before writing any code
**pre-coding reads to upload on Claude request (Claude make choice only required for session ):**
- `docs/backtesting/TECHNICAL_SPEC.md` — all contracts and module signatures
- `docs/backtesting/FUNCTIONAL_SPEC.md` — detailed stage descriptions
- `docs/backtesting/SQLITE_SCHEMA.md` — schema for WFO and MC storage
- `docs/strategies/architecture/ARCHITECTURE.md` — strategy integration points
---
## Session Objective
At the end of this session we will have:
- All WFO, GA, and MC pre-filter modules implemented and unit-tested
- Key validations completed (window sampling, diversity penalty, runtime profiling)
- A working integration test: Random → MC Pre-filter → GA → Full WFO
---
## Non-Negotiable Before Writing Any Code
The session **must not begin coding** until Claude has read and confirmed:
1. `TECHNICAL_SPEC.md` contract definitions (exact field names, types, validation rules)
2. `FUNCTIONAL_SPEC.md` stage details (WFO modes, GA fitness, MC perturbations)
3. GA-06: Random window sampling per generation
4. WF-04: Composite consistency score from four metrics
Any code that deviates from TECHNICAL_SPEC.md contracts is a defect, not a style difference.
---
## Work Breakdown
### Block 0 — WFO Modules First (~90 min)
*Build WFO first — required by GA for WFO-aware fitness.*
**`wfo/window_generator.py`:**
- Generate list of `WFOWindow` from config (start/end dates, stride, length)
- Validate min 3 windows for GA sampling
**`wfo/wfo_evaluator.py`:**
- Evaluate candidate on single window: adjust data range, run strategy, compute fitness
- Return `WFOWindowResult` with metrics, oos_delta if applicable
**`wfo/wfo_engine.py`:**
- Lightweight mode: subset of windows (for GA)
- Full mode: all windows, compute consistency score
- Parallelize window evaluations
**`wfo/consistency_scorer.py`:**
- Compute four metrics from window results
- Weighted composite score using scenario weights
**Tests to write:**
- `test_window_generator_min_windows`: <3 windows raises ValueError
- `test_wfo_evaluator_single_window`: Metrics match expected for known data
- `test_wfo_engine_modes`: Lightweight vs. full produce consistent results
- `test_consistency_scorer_composite`: Weights sum to 1, score in [0,1]
---
### Block 1 — GA Modules (~120 min)
**`ga/population.py`:**
- Initialize population from seeded candidates
- Track generation, fitness
**`ga/selection.py`:**
- Tournament or roulette wheel selection
**`ga/crossover.py`:**
- Single-point or uniform crossover for params
**`ga/mutation.py`:**
- Gaussian mutation for continuous, flip for discrete
**`ga/diversity.py`:**
- Compute hybrid Euclidean/Hamming penalty
- Apply to fitness scores
**`ga/ga_engine.py`:**
- Run generations: sample 2 random windows per gen
- WFO-aware fitness using wfo_evaluator
- Evolve until convergence or max gens
**Tests to write:**
- `test_ga_window_sampling_independent`: Different windows per generation
- `test_diversity_penalty_prevents_collapse`: Population spread maintained over 100 gens
- `test_ga_engine_convergence`: Fitness improves, valid candidates produced
- `test_mutation_bounds`: Mutated params stay within zone limits
---
### Block 2 — MC Pre-Filter (~60 min)
**`monte_carlo/perturbation.py`:**
- Define named profiles (lightweight: 2 types)
- Apply perturbations to trade sequences
**`monte_carlo/equity_simulator.py`:**
- Simulate equity paths from perturbed trades
**`monte_carlo/mc_metrics.py`:**
- Compute ruin prob, avg equity, worst DD
**`monte_carlo/mc_engine.py` (pre-filter mode):**
- Run lightweight iterations
- Screen candidates by ruin probability
**Tests to write:**
- `test_perturbation_profiles`: Named profiles apply correct noise
- `test_mc_engine_prefilter`: High ruin candidates rejected
- `test_equity_simulator_vectorized`: Efficient for 1000+ iterations
---
### Block 3 — Integration Test and Validations (~30 min)
Write `tests/integration/test_multi_stage_through_wfo.py`:
- Fixture: Valid config with 3+ WFO windows
- Run Stages 0-4
- Assert: Candidates in SQLite at each stage, GA used random windows, WFO consistency scores computed
**Key Validations:**
- Profile GA runtime (R-05): Measure WFO-aware fitness time
- Diversity check: Inspect parameter variance across generations
- Log results in `CHANGE_LOG.md`
---
## Output Documents
By the end of this session, produce or update:
| Document | Action | Location |
|---|---|---|
| `src/backtesting/wfo/window_generator.py` | Create | `src/backtesting/wfo/` |
| `src/backtesting/wfo/wfo_evaluator.py` | Create | `src/backtesting/wfo/` |
| `src/backtesting/wfo/wfo_engine.py` | Create | `src/backtesting/wfo/` |
| `src/backtesting/wfo/consistency_scorer.py` | Create | `src/backtesting/wfo/` |
| `src/backtesting/ga/population.py` | Create | `src/backtesting/ga/` |
| `src/backtesting/ga/selection.py` | Create | `src/backtesting/ga/` |
| `src/backtesting/ga/crossover.py` | Create | `src/backtesting/ga/` |
| `src/backtesting/ga/mutation.py` | Create | `src/backtesting/ga/` |
| `src/backtesting/ga/diversity.py` | Create | `src/backtesting/ga/` |
| `src/backtesting/ga/ga_engine.py` | Create | `src/backtesting/ga/` |
| `src/backtesting/monte_carlo/perturbation.py` | Create | `src/backtesting/monte_carlo/` |
| `src/backtesting/monte_carlo/equity_simulator.py` | Create | `src/backtesting/monte_carlo/` |
| `src/backtesting/monte_carlo/mc_metrics.py` | Create | `src/backtesting/monte_carlo/` |
| `src/backtesting/monte_carlo/mc_engine.py` | Create | `src/backtesting/monte_carlo/` |
| `tests/backtesting/benchmarks/` | Update | benchmarks test |
| `tests/backtesting/unit/` | Update | Unit tests for all new modules |
| `tests/backtesting/integration/test_multi_stage_through_wfo.py` | Create | Integration test |
| `CHANGE_LOG.md` | Append SESSION 4 block | Validation results, any adjustments |
| `PROJECT_REPORT.md` | Update Phase 3 status | Mark deliverables complete |
| `CONTEXT.md` | Update current phase block | Phase 3 in-progress or complete |
---
## If the Session Runs Long
Priority order if forced to cut short:
1. WFO modules — must complete; blocks GA
2. GA modules — must complete; core of phase
3. MC pre-filter — defer if needed
4. Integration test — run at least manually if time short
Always write CHANGE_LOG.md session block and update CONTEXT.md before ending the session, even if cut short.
---
## Acceptance Criteria for Phase 3 Complete
- [ ] WFO engine: Both modes work, consistency score computed correctly
- [ ] GA engine: Random windows per gen, diversity penalty applied, converges without collapse
- [ ] MC pre-filter: Lightweight mode screens high-ruin candidates
- [ ] R-05 profiling: GA runtime measured, within budget projection
- [ ] All unit tests pass
- [ ] Integration test passes: Stages 1-4 produce expected SQLite rows
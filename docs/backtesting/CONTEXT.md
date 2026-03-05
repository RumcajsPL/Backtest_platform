# CONTEXT.md — Block 9F Handoff
**Generated**: 2026-03-05 (end of Block 9E session)
**From**: Block 9E — OOS Gate Implementation (B8B-005) complete
**To**: Block 9F — First Real Pipeline Run + Calibration

---
## Current Pipeline State

### Stage Implementation Status
| Stage | Name | Status |
|---|---|---|
| 0 | Validation & Init | ✅ Implemented |
| 1 | Random Search | ✅ Implemented |
| 2 | MC Pre-Filter | ✅ Implemented |
| 3 | Genetic Algorithm | ✅ Implemented |
| 4 | Full WFO | 🟡 Stub (logs + advances checkpoint only) |
| 5 | MC Deep | ✅ Implemented |
| 6 | Parameter Sensitivity | ✅ Implemented |
| 7 | Report & Output | ✅ Implemented |

**Consequence of Stage 4 stub**: Stages 5–7 query `wfo_consistency_scores` for their
input candidates. With Stage 4 as a stub, that table is empty → Stages 5–7 log
"No candidates with WFO scores — skipping" and produce no output. This is expected
and correct for this run. The value of the first run is in Stages 1–3 data only.

### OOS Gate
- Implemented (Block 9E) but **off by default** (`enforce_oos_gate: false`).
- Do not enable until `oos_degradation_threshold` is calibrated from real data.

### Test Suite
345 passing (Block 9C baseline). Blocks 9D/9E added no new tests — integration
tests for Stages 1–3 and OOS gate require real strategy data and are authored in Block 9F.

---
## Block 9F — Operator Preparation Guide

### STEP 1 — Verify file deployment
Confirm all Block 9D/9E output files are in place in your working tree:
```
src/backtesting/orchestrator.py          (Block 9D — Stages 1–3, B9A-001, B9A-003)
src/backtesting/sampler.py               (Block 9D — B9C-006, B9C-007)
src/backtesting/parameter_space.py       (Block 9D — B9C-005)
src/backtesting/strategy_runner.py       (Block 9D — B8-006 comment)
src/backtesting/yaml_generator.py        (Block 9D — B8-006 comment)
src/backtesting/wfo/wfo_engine.py        (Block 9D/9E — B9C-004, B8B-005)
src/backtesting/wfo/wfo_evaluator.py     (Block 9E — B8B-005 IS/OOS split)
```
Quick check (run from project root):
```bash
python -c "from src.backtesting.orchestrator import run; print('import OK')"
python -c "from src.backtesting.wfo.wfo_evaluator import evaluate_window; print('import OK')"
```
Both must print `import OK` with no errors before proceeding.

### STEP 2 — Verify backtest_template.yaml
Open `configs/backtesting/backtest_template.yaml` and confirm or set:
```yaml
# ── Active scenario ───────────────────────────────────────────────────────────
scenario: "capital_accumulation"          # must match a key under 'scenarios:'

# ── Run settings ─────────────────────────────────────────────────────────────
run:
  output_dir: "data/backtesting/runs"     # will be created if absent
  temp_dir:   "data/backtesting/temp"     # will be created if absent
  max_workers: 4                          # start conservative; increase after confirming stability
  retain_temp_yamls: false               # true only for debugging a specific candidate

# ── Random search (Stage 1) ───────────────────────────────────────────────────
random_search:
  method: "lhs"                          # "lhs" recommended; "random" as fallback
  samples_per_zone: 50                   # START LOW for first run — increase to 200 once stable
  min_significant_trades: 30
  seed: 42

# ── MC Pre-Filter (Stage 2) ───────────────────────────────────────────────────
mc_prefilter:
  input_count: 30                        # top-N RANDOM-pass candidates to screen
  iterations: 200                        # cheap — low iteration count intentional
  perturbation_profile: "default"
  seed: 44

# ── GA (Stage 3) ─────────────────────────────────────────────────────────────
genetic:
  population_size: 20                    # START SMALL for first run
  generations: 5                         # START SMALL for first run
  elite_fraction: 0.20
  mutation_rate: 0.15
  crossover_rate: 0.70
  tournament_size: 3
  stagnation_generations: 3
  diversity_penalty_weight: 0.10
  diversity_distance_threshold: 0.20
  seed: 43

# ── WFO windows ───────────────────────────────────────────────────────────────
walk_forward:
  enforce_oos_gate: false                # KEEP FALSE until threshold calibrated
  oos_degradation_threshold: 0.50
  windows:                               # minimum 3 required
    - {id: "W01", start: "2025-09-15", end: "2025-10-03"}
    - {id: "W02", start: "2025-10-06", end: "2025-10-24"}
    - {id: "W03", start: "2025-10-27", end: "2025-11-14"}
    - {id: "W04", start: "2025-11-17", end: "2025-12-05"}
    - {id: "W05", start: "2025-12-08", end: "2025-12-17"}
```
**Critical checks before running:**
- [ ] `scenario` key matches an actual scenario name in `scenarios:` block
- [ ] `walk_forward.windows` has ≥ 3 entries with valid ISO dates, start < end
- [ ] `strategy.base_yaml_path` points to an existing YAML (or `_resolve_base_yaml()`
      fallback path `configs/strategies/strategy_template.yaml` exists)
- [ ] `run.output_dir` parent directory is writable
- [ ] `run.temp_dir` parent directory is writable

### STEP 3 — Verify strategy data availability
The strategy runner will evaluate candidates against real OHLCV data.
Confirm the data files referenced in your strategy base YAML exist and cover
the full WFO window date range (2025-09-15 → 2025-12-17 minimum):
```bash
# Check data files exist (adjust paths to match your strategy_template.yaml)
python -c "
from pathlib import Path
# Replace these with your actual data paths from strategy_template.yaml
paths = [
    'data/ohlcv/strategy_tf_data.csv',      # strategy timeframe data
    'data/ohlcv/htf_data.csv',              # HTF data
]
for p in paths:
    f = Path(p)
    print(f'{'OK' if f.exists() else 'MISSING'}: {p}')
"
```
If any data file is missing or doesn't cover the window range, Stage 1 will produce
zero passing candidates (significance guard will reject all).

### STEP 4 — Run the pipeline (dry-run first)
```bash
# From project root, activate your virtual environment first:
# Windows: .venv\Scripts\activate

# Smoke test: import only, no execution
python -c "
from src.backtesting.orchestrator import _load_and_validate_config
from pathlib import Path
config = _load_and_validate_config(Path('configs/backtesting/backtest_template.yaml'))
print('Config valid. Scenario:', config['scenario'])
print('WFO windows:', len(config['walk_forward']['windows']))
print('Zones enabled:', [k for k,v in config.get('zones',{}).items() if v.get('enabled',True)])
"

# Full run (expect ~10–60 min for samples_per_zone=50, population_size=20, generations=5)
python -m src.backtesting.orchestrator --config configs/backtesting/backtest_template.yaml
```

**Alternatively**, if `orchestrator.py` doesn't have a `__main__` block yet, run via:
```python
# run_pipeline.py (create in project root if needed)
from pathlib import Path
from src.backtesting.orchestrator import run
run(Path("configs/backtesting/backtest_template.yaml"))
```

### STEP 5 — Monitor the run
The pipeline logs to the root logger. Recommended log setup:
```python
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
```

**What you should see at each stage:**

Stage 0 (seconds):
```
Stage 0: Validation & Init — run_id=<uuid>
Stage 0: All validations passed — N WFO windows, N enabled zones
```

Stage 1 (minutes — most of the runtime):
```
Stage 1: Random Search — method=lhs samples_per_zone=50
Stage 1: N candidates sampled across N zones
Stage 1: Random Search complete — evaluated=N passed=P failed=F
```
✅ Healthy: `passed` > 0. If `passed=0`, see TROUBLESHOOTING below.

Stage 2 (seconds to minutes):
```
Stage 2: MC Pre-Filter — top N candidates, ruin_threshold=0.XX
Stage 2: MC Pre-Filter complete — pass=P fail=F total=T
```
✅ Healthy: at least a few `pass`.

Stage 3 (minutes):
```
GA starting: pop=N gens=N elites=N% mut=N xo=N seed=N
Gen 1/5: best_fitness=0.XXXX window_pair=[W0X, W0X]
...
GA complete: final_best=0.XXXX stagnation_stops=N
Stage 3: Genetic Algorithm complete
```

Stage 4 (immediate):
```
Stage 4: Full WFO — stub, not yet implemented
```

Stages 5–7 (immediate — no input candidates from stub Stage 4):
```
Stage 5: No candidates with WFO scores — skipping MC Deep
Stage 6: No candidates with WFO scores — skipping Sensitivity
Stage 7: No candidates available — generating empty report
```
This is expected. ✅

Final:
```
Pipeline complete — run_id=<uuid>
```

---
## Block 9F — What to Report to Claude

After the run completes, open the SQLite database and run these queries.
Copy the results exactly into the next session — this data drives all calibrations.

### Database location
```
data/backtesting/runs/backtester.db
```
Connect with any SQLite browser (DB Browser for SQLite is free and excellent) or:
```bash
python -c "
import sqlite3, json
from pathlib import Path
db = Path('data/backtesting/runs/backtester.db')
conn = sqlite3.connect(str(db))
conn.row_factory = sqlite3.Row
# Run your queries here
"
```

### Query 1 — Run summary
```sql
SELECT run_id, scenario_name, checkpoint, started_at, completed_at,
       total_candidates_evaluated
FROM runs
ORDER BY started_at DESC
LIMIT 1;
```
**Report**: run_id (first 8 chars), checkpoint value, total_candidates_evaluated.

### Query 2 — Stage 1 candidate funnel
```sql
SELECT
    passed_constraints,
    rejection_reason,
    COUNT(*) as count
FROM evaluations
WHERE run_id = '<your_run_id>'
  AND stage = 'RANDOM'
GROUP BY passed_constraints, rejection_reason
ORDER BY count DESC;
```
**Report**: full table. Especially note:
- How many passed (passed_constraints=1)
- Most common rejection_reason
- Whether `REJECTED_INSUFFICIENT_TRADES` dominates (data coverage issue)
- Whether `REJECTED_CONSTRAINTS` dominates (thresholds may be too tight)

### Query 3 — net_pnl distribution (for B8B-012 sigmoid calibration)
```sql
SELECT
    COUNT(*)                           as n_windows,
    MIN(net_pnl)                       as min_pnl,
    MAX(net_pnl)                       as max_pnl,
    AVG(net_pnl)                       as avg_pnl,
    -- SQLite has no stdev() — use this workaround:
    SQRT(AVG(net_pnl * net_pnl) - AVG(net_pnl) * AVG(net_pnl)) as stdev_pnl,
    -- Percentiles via ordered counting (approximate):
    COUNT(CASE WHEN net_pnl < 0 THEN 1 END) * 1.0 / COUNT(*) as frac_negative
FROM wfo_window_results
WHERE run_id = '<your_run_id>'
  AND net_pnl IS NOT NULL;
```
**Report**: all columns. The `stdev_pnl` value is the key input for B8B-012 calibration.

### Query 4 — expectancy distribution (for B8B-003 calibration)
```sql
SELECT
    COUNT(*)                             as n_evals,
    MIN(actual_expectancy)               as min_exp,
    MAX(actual_expectancy)               as max_exp,
    AVG(actual_expectancy)               as avg_exp,
    SQRT(AVG(actual_expectancy * actual_expectancy) -
         AVG(actual_expectancy) * AVG(actual_expectancy)) as stdev_exp,
    -- 90th percentile proxy:
    COUNT(CASE WHEN actual_expectancy > 0 THEN 1 END) * 1.0 / COUNT(*) as frac_positive
FROM evaluations
WHERE run_id = '<your_run_id>'
  AND stage = 'RANDOM'
  AND passed_constraints = 1
  AND actual_expectancy IS NOT NULL;
```
**Report**: all columns. `max_exp` is the natural ceiling for the `/3.0` normalisation fix.

### Query 5 — fitness score distribution (Stage 1 passes only)
```sql
SELECT
    COUNT(*)                 as n,
    MIN(fitness_score)       as min_fitness,
    MAX(fitness_score)       as max_fitness,
    AVG(fitness_score)       as avg_fitness,
    COUNT(CASE WHEN fitness_score > 0.5 THEN 1 END) as above_median,
    COUNT(CASE WHEN fitness_score > 0.7 THEN 1 END) as above_70pct
FROM evaluations
WHERE run_id = '<your_run_id>'
  AND stage = 'RANDOM'
  AND passed_constraints = 1;
```
**Report**: all columns.

### Query 6 — MC Pre-Filter ruin distribution
```sql
SELECT
    COUNT(*)              as total_mc_runs,
    MIN(ruin_probability) as min_ruin,
    MAX(ruin_probability) as max_ruin,
    AVG(ruin_probability) as avg_ruin,
    COUNT(CASE WHEN ruin_probability IS NULL THEN 1 END) as mc_errors
FROM mc_results
WHERE run_id = '<your_run_id>'
  AND mode = 'pre_filter';
```
**Report**: all columns. Note any `mc_errors` > 0 (means `CandidateResult` was missing
or `run_mc()` errored — needs investigation).

### Query 7 — GA generation fitness progression
```sql
SELECT
    generation,
    COUNT(*)          as candidates,
    MAX(fitness_score) as best_fitness,
    AVG(fitness_score) as avg_fitness
FROM evaluations
WHERE run_id = '<your_run_id>'
  AND stage = 'GA'
GROUP BY generation
ORDER BY generation;
```
**Report**: full table. Shows whether GA is improving across generations.
If `best_fitness` is flat across all generations, the GA may not be seeded with
enough MC_PREFILTER_PASS candidates, or stagnation_generations is firing too early.

### Query 8 — Any errors in evaluations
```sql
SELECT
    stage,
    error_message,
    COUNT(*) as count
FROM evaluations
WHERE run_id = '<your_run_id>'
  AND error_message IS NOT NULL
GROUP BY stage, error_message
ORDER BY count DESC
LIMIT 20;
```
**Report**: full table if any rows exist. Zero rows = clean run.

### Additional observations to report
Beyond the SQL queries, note the following from your console output / log file:
1. **Wall-clock time per stage** (from the TIMING log lines at end of run)
2. **Any Python exceptions** printed to stderr — paste the full traceback
3. **The run_id** — needed to substitute into the queries above
4. **How many zones were enabled** and how many combinations each zone expanded to
   (visible in the Stage 0 logs)
5. **The checkpoint value** from Query 1 — should be `COMPLETE` if the run finished

---
## Block 9F — Calibration Actions (done by Claude, not operator)

These will be done in the Block 9F session once the query results are reported.
Listed here so Claude knows exactly what to do on arrival.

### B8B-012 — Sigmoid scale calibration (PRE-PROD BLOCKER)
**File**: `consistency_scorer.py`, function `_sigmoid_normalise()`
**Current**: `scale=0.10` (hardcoded, calibrated for unit fractions)
**Fix logic**:
```python
# From Query 3: stdev_pnl = X (in points/pips)
# Target: a net_pnl equal to 1 stdev maps to approximately sigmoid(0.7) ≈ 0.67
# i.e. a good-but-not-exceptional window → above-midpoint score
# Recommended: scale = stdev_pnl * 0.50 (conservative — avoids saturation)
# If stdev_pnl = 150 pts → scale = 75.0
# Replace: _sigmoid_normalise(median_return_raw, scale=0.10)
# With:    _sigmoid_normalise(median_return_raw, scale=<stdev * 0.50>)
```
**Action needed**: upload `consistency_scorer.py`, make targeted edit to `_sigmoid_normalise` call.

### B8B-003 — Expectancy normalisation ceiling
**File**: `fitness.py`, function `_compute_weighted_score()`
**Current**: `expectancy_norm = clamp(expectancy_points / 3.0, 0, 1)`
**Fix logic**:
```python
# From Query 4: max_exp = X pts (e.g. 8.5 pts)
# The /3.0 divisor means anything above 3 pts gets clamped to 1.0
# Better: use the 90th percentile of observed expectancy as the ceiling
# Recommended: ceiling = round(P90_expectancy, 1) — so the best ~10% of candidates
# saturate at 1.0 (avoids compressing all candidates into [0, 0.35])
# Replace: expectancy_norm = _clamp(expectancy_points / 3.0, 0.0, 1.0)
# With:    expectancy_norm = _clamp(expectancy_points / <P90_expectancy>, 0.0, 1.0)
```
**Action needed**: upload `fitness.py`, make targeted edit to expectancy normalisation line.

### Optional — OOS threshold calibration
Only relevant if `enforce_oos_gate` is enabled post-calibration.
From wfo_window_results oos_delta column (requires a run with gate enabled temporarily
on a small subset). Defer to Block 9G if needed.

---
## Troubleshooting Guide

### "Stage 1 passed=0" (all candidates failed constraints)
Most likely causes, in order of probability:
1. **Data doesn't cover window dates** — strategy returns 0 trades, hits significance guard.
   Check: `WHERE rejection_reason = 'REJECTED_INSUFFICIENT_TRADES'` count.
   Fix: extend OHLCV data to cover all WFO window dates.
2. **Constraint thresholds too tight for this market** — all candidates fail min_win_rate
   or min_profit_factor. Check which `failing_constraint` dominates in Query 2.
   Temporary fix: loosen the failing constraint in the scenario definition.
3. **_PARAM_KEY_MAP missing a parameter** — Stage 0 would have raised a ValueError,
   so this would have aborted before Stage 1. If Stage 0 passed, this is not the issue.
4. **base_yaml_path wrong** — strategy runner can't find or parse the base YAML.
   Check for `FileNotFoundError` or `yaml.YAMLError` in stderr.

### "Stage 2: No RANDOM-pass candidates available"
Caused by Stage 1 passed=0. Fix Stage 1 first.

### "GA starting: pop=0" or RuntimeError in Stage 3
Caused by Stage 2 producing zero MC_PREFILTER_PASS candidates.
Either the ruin threshold is too strict (lower `mc_prefilter_ruin_threshold` in scenario)
or the pre-filter iterations are too few to distinguish candidates.

### "ProcessPoolExecutor" errors in Stage 1/3
On Windows, multiprocessing spawn requires all top-level imports to be inside
`if __name__ == '__main__':` or inside the function body.
Strategy runner imports are already inside the `evaluate()` function body — this
should not be an issue. If you see `PicklingError` or `AttributeError` from workers:
1. Check that the strategy package is importable from the worker's sys.path.
2. Check that `run.max_workers` is not set higher than your CPU count.

### "Config hash mismatch" error on resume
The pipeline detected an incomplete run with a different config hash.
Fix: either complete the existing run, or delete the DB and start fresh:
```bash
del data\backtesting\runs\backtester.db   # Windows
rm  data/backtesting/runs/backtester.db   # Linux/Mac
```

### Run completed but checkpoint is not COMPLETE
The pipeline raises an exception somewhere mid-run. Check the full traceback.
On next run, the pipeline will resume from the last successful checkpoint.
Do NOT delete the DB — the completed stages are preserved.

---
## Block 9E — Implementation Summary (for reference)

### B8B-005: OOS Gate — Two-file fix
**Root cause**: Two independent bugs, both required:
1. `wfo_evaluator.py`: `oos_delta` hardcoded `None`. IS/OOS split never ran.
2. `wfo_engine.py`: `oos_gate_enabled` received but never passed to `evaluate_window`
   in `pool.submit()`. Workers always saw `False`.

**`_compute_oos_delta()` design** (in wfo_evaluator.py):
- `_IS_FRACTION = 0.70` (70/30 by calendar days)
- `oos_delta = oos_fitness - is_fitness` (both [0,1])
- Returns `None` on any sub-evaluation failure (safe default)
- OOS constraint-fail → `oos_fitness = 0.0` floor (preserves large negative delta signal)
- Gate disabled in GA lightweight mode by design (D-05)

`consistency_scorer.py` — no changes needed (was already correct).

---
## Contract Field Reference (verified — do NOT deviate)
```python
# CandidateStage enum values (exact):
CandidateStage.RANDOM              # NOT RANDOM_SEARCH
CandidateStage.MC_PREFILTER_PASS
CandidateStage.MC_PREFILTER_FAIL
CandidateStage.GA
CandidateStage.WFO
CandidateStage.MC_DEEP
CandidateStage.SENSITIVITY
# CandidateRecord.stage: str = CandidateStage.X.value  (string, not enum)
# WFOWindow: window_id, start_date: date, end_date: date
# NO is_start, oos_start, is_end, oos_end fields
# WFOConsistencyScore.composite_score  (NOT wfo_consistency_score)
# evaluate_window() positional arg 7: oos_gate_enabled: bool = False
# oos_delta = oos_fitness - is_fitness; None when gate off or eval fails
```
---
## Critical Non-Negotiables
```python
# strategy_runner: mode_override="core" — NOT mode="core"
# store.query_mc_results: mode = "deep"/"pre_filter" (string, NOT MCMode enum)
# datetime.now(timezone.utc) ONLY — never datetime.utcnow()
# spawn boundary: patches don't cross ProcessPoolExecutor workers
# LIVE_APPROVED: never set in code — operator-only
# snap-then-clamp in mutation
# CandidateRecord.stage: str (.value) not enum
# _IS_FRACTION = 0.70 — do not change without updating CONTEXT.md
# enforce_oos_gate: false until threshold calibrated
# ranker.rank_by_wfo() → List[CandidateRecord] — attribute access, not dict keys
```
---
## Open Findings Quick Reference
### Resolved (Blocks 9D + 9E)
| ID | Fix |
|---|---|
| B9C-007 ✅ | sampler LHS sort key → float(x) |
| B9C-006 ✅ | sampler docstring |
| B9A-003 ✅ | Stage 6 spike_threshold → ScenarioProfile |
| B9A-001 ✅ | Stages 5–7 rank_by_wfo → typed CandidateRecord |
| B9C-004 ✅ | wfo_engine empty candidates guard |
| B9C-005 ✅ | parameter_space Decimal(str(step)) |
| B8-006  ✅ | twin key map comments |
| B8B-005 ✅ | IS/OOS split + gate pass-through |

### Active
| ID | Priority | File | Description |
|---|---|---|---|
| B9F-001 | **P1 BLOCKER** | parameter_space.py | expand_zones() materialises full Cartesian product — exploration zone has ~387T combos → OOM/hang. **Workaround for first run: exploration.enabled: false (already set in config).** Fix: refactor expand_zones() to return per-param value lists; sampler samples without product enumeration. |
| B8B-012 | PRE-PROD BLOCKER | consistency_scorer.py | sigmoid scale=0.10 — calibrate after first run |
| B8B-003 | P3 | fitness.py | expectancy /3.0 hardcoded — calibrate after first run |
| B8-009 | P3 | orchestrator.py | raw sqlite3 in _resume_or_start |
| B9B-001 | P3 | crossover.py | no zone-name guard |
| B8B-013 | P3 | mc_engine.py | ruin_threshold dual-source |
| B8B-011 | P3 | consistency_scorer.py | fraction_positive_windows floor |
| B8C-002/003 | P3 | report_generator.py | deferred |
| B9C-008 | P3 | sampler.py | deferred |

### Deferred to post-Stage-4
Stage 4 Full WFO implementation, OOS gate threshold calibration (Block 9G).
---
## Block Roadmap
```
Block 9D (done):  Prerequisite fixes + Stages 1–3 implemented
Block 9E (done):  B8B-005 OOS gate
Block 9F (next):  1. Fix B9F-001 (expand_zones refactor — unblocks exploration zone)
                  2. First real run (safe zone only, 50 samples)
                  3. Report data → calibrate B8B-012 + B8B-003
                  4. Re-enable exploration zone + run with 200 samples
Block 9G (TBD):   Stage 4 Full WFO implementation
Block 9H (TBD):   OOS gate threshold calibration + enable
Production:       Stage 4 complete, B8B-012/003 calibrated, exploration zone enabled
```
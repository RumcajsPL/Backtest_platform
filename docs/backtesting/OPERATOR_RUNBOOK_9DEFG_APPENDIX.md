# OPERATOR_RUNBOOK.md — Appendix Block 9D/9E/9F
**Append this section to OPERATOR_RUNBOOK.md after final implementation.**
**Generated**: 2026-03-05

---
## Section: Running the Pipeline for the First Time

### Prerequisites Checklist

Before your first pipeline run, verify all of the following:

**Software**
- [ ] Python 3.13+ virtual environment activated
- [ ] All project dependencies installed (`pip install -r requirements.txt`)
- [ ] Block 9D/9E source files deployed (see CONTEXT.md STEP 1)
- [ ] Import smoke test passes:
  ```bash
  python -c "from src.backtesting.orchestrator import run; print('OK')"
  ```

**Data**
- [ ] OHLCV data files exist and cover the full WFO window date range
- [ ] HTF data file exists and covers the same range
- [ ] Data files are in the format expected by the strategy (CSV or your native format)
- [ ] No data gaps within the window range (gaps cause the significance guard to fire)

**Configuration**
- [ ] `configs/backtesting/backtest_template.yaml` exists
- [ ] `scenario:` key matches an entry in the `scenarios:` block
- [ ] `walk_forward.windows` has at least 3 entries with `start < end`
- [ ] `run.output_dir` and `run.temp_dir` paths are writable
- [ ] `enforce_oos_gate: false` (leave off for first run)
- [ ] `samples_per_zone: 50` (start small — increase to 200+ for production runs)
- [ ] `population_size: 20`, `generations: 5` (start small)

---
### Running the Pipeline

```bash
# From project root with virtual environment active:

# Option A — if orchestrator has a __main__ block:
python -m src.backtesting.orchestrator --config configs/backtesting/backtest_template.yaml

# Option B — via a thin runner script (create run_pipeline.py in project root):
# from pathlib import Path
# from src.backtesting.orchestrator import run
# run(Path("configs/backtesting/backtest_template.yaml"))
python run_pipeline.py
```

**Recommended log setup** (add to the top of run_pipeline.py or your startup script):
```python
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("data/backtesting/runs/pipeline.log"),
    ]
)
```

---
### Expected Runtime (first run, conservative settings)

| Stage | Expected time | Notes |
|---|---|---|
| 0 — Validation | <5 seconds | Config + zone validation |
| 1 — Random Search | 10–60 minutes | Depends on samples_per_zone × zones × strategy eval time |
| 2 — MC Pre-Filter | 1–5 minutes | Low iteration count (200), cheap |
| 3 — GA | 5–30 minutes | population_size=20, generations=5 |
| 4 — Full WFO | <1 second | Stub only — logs and advances checkpoint |
| 5–7 | <5 seconds | No candidates from stub Stage 4 — all skipped |
| **Total** | **~30–90 min** | Highly dependent on strategy evaluation speed |

The dominant cost is Stage 1: each candidate evaluation runs the full strategy.
If a single strategy evaluation takes ~5 seconds, 50 candidates × 5 zones = 250
evaluations × 5s / 4 workers ≈ 5 minutes for Stage 1. Adjust `max_workers` to
match available CPU cores (recommend `max_workers = cpu_count - 1`).

---
### Output Directory Structure

After a successful run, the output directory will contain:

```
data/backtesting/runs/
├── backtester.db              ← SQLite database — all pipeline data
├── pipeline.log               ← Full log (if using FileHandler)
└── <run_id>/                  ← Per-run outputs (Stage 7)
    ├── report.html            ← Self-contained HTML report
    ├── candidates.json        ← Per-candidate JSON records
    ├── candidates.parquet     ← Per-candidate Parquet (if pandas installed)
    └── trading_yamls/         ← One YAML per AUTO_GO or BORDERLINE candidate
        └── <run_id8>_<cid12>_strategy.yaml
```

For the first run (Stage 4 is a stub), the `<run_id>/` directory will contain an
empty or minimal report — this is expected. The SQLite database is the primary
output.

---
### Inspecting the Database

The SQLite database at `data/backtesting/runs/backtester.db` is the single source
of truth for all pipeline data. Use **DB Browser for SQLite** (free, cross-platform)
or the Python sqlite3 module.

**Key tables and their contents:**

| Table | Contents |
|---|---|
| `runs` | One row per pipeline run. Contains run_id, checkpoint, seeds. |
| `candidates` | One row per candidate. zone_name, generation, origin_stage. |
| `candidate_parameters` | One row per candidate. Full JSON + indexed parameter columns. |
| `evaluations` | One row per candidate per stage. Fitness, constraints, actuals. |
| `wfo_window_results` | One row per candidate per WFO window. Window-level metrics. |
| `wfo_consistency_scores` | One row per candidate (Stage 4+). Composite WFO score. |
| `mc_results` | One row per candidate per MC mode (pre_filter, deep). |
| `sensitivity_results` | Per-parameter per-step rows. |
| `sensitivity_profiles` | Summary row per candidate (Stage 6+). |
| `verdicts` | Final verdict per candidate (Stage 7). |

**Quick health check query** (run after any pipeline run):
```sql
SELECT
    r.run_id,
    r.scenario_name,
    r.checkpoint,
    COUNT(DISTINCT CASE WHEN e.stage='RANDOM' THEN e.candidate_id END) as random_total,
    SUM(CASE WHEN e.stage='RANDOM' AND e.passed_constraints=1 THEN 1 ELSE 0 END) as random_pass,
    COUNT(DISTINCT CASE WHEN e.stage='MC_PREFILTER_PASS' THEN e.candidate_id END) as mc_pass,
    COUNT(DISTINCT CASE WHEN e.stage='GA' THEN e.candidate_id END) as ga_candidates,
    COUNT(DISTINCT wcs.candidate_id) as wfo_scored,
    COUNT(DISTINCT v.candidate_id) as verdicts
FROM runs r
LEFT JOIN evaluations e ON r.run_id = e.run_id
LEFT JOIN wfo_consistency_scores wcs ON r.run_id = wcs.run_id
LEFT JOIN verdicts v ON r.run_id = v.run_id
WHERE r.run_id = '<your_run_id>'
GROUP BY r.run_id;
```

---
### Resuming an Interrupted Run

The pipeline automatically resumes from the last successfully completed checkpoint.
If a run is interrupted (crash, KeyboardInterrupt, power loss), simply re-run with
the same config file. The pipeline reads the checkpoint from the `runs` table and
skips all completed stages.

**Important**: do NOT modify `backtest_template.yaml` between an interrupted run
and its resume. The pipeline stores a SHA-256 hash of the config at run start and
will refuse to resume if the hash has changed (config mismatch = different run).

If you need to change the config and start fresh:
```bash
# Windows:
del data\backtesting\runs\backtester.db
# Linux/Mac:
rm data/backtesting/runs/backtester.db
```

---
### Calibration After First Run

Two calibrations are required before the pipeline is production-ready.
Run the diagnostic queries from CONTEXT.md Section "Block 9F — What to Report to Claude"
and share the results with Claude in the Block 9F session.

**B8B-012 — WFO sigmoid scale** (PRE-PROD BLOCKER)

The WFO composite score's `median_window_return` sub-metric uses a sigmoid function
to map raw net P&L values to [0, 1]. The current scale of `0.10` was placeholder-calibrated
for unit fractions. Strategy metrics are in points/pips, so the scale needs to match
the actual P&L distribution from your strategy and data.

A miscalibrated sigmoid causes all windows to score near 0.0 (scale too small vs.
actual P&L magnitudes) or to score uniformly ~0.5 (scale too large). Either
compresses the discriminating power of the WFO score.

**B8B-003 — Expectancy normalisation ceiling**

The fitness score's expectancy component is normalised by dividing by `3.0 pts`.
If your strategy's typical expectancy is 5–15 pts, most candidates would score at
the 1.0 ceiling, removing all differentiation on this dimension.

---
### OOS Gate Activation (post-calibration, Block 9G)

The IS/OOS gate is implemented but disabled by default. When ready to activate:

1. Run a single test run with `enforce_oos_gate: true` and `samples_per_zone: 10`
   (minimal — just to populate `oos_delta` values in the database).

2. Query the `oos_delta` distribution:
   ```sql
   SELECT
       COUNT(*) as n,
       MIN(oos_delta) as min_delta,
       MAX(oos_delta) as max_delta,
       AVG(oos_delta) as avg_delta,
       SQRT(AVG(oos_delta*oos_delta)-AVG(oos_delta)*AVG(oos_delta)) as stdev_delta,
       COUNT(CASE WHEN oos_delta IS NULL THEN 1 END) as null_count
   FROM wfo_window_results
   WHERE run_id = '<test_run_id>'
     AND oos_delta IS NOT NULL;
   ```

3. Set `oos_degradation_threshold` to approximately the 10th percentile of
   `abs(oos_delta)` — meaning only the worst 10% of candidates (most OOS-degraded)
   trigger the gate. The current default of `0.50` is extremely lenient; the real
   value is likely in the range 0.10–0.25 depending on your strategy's IS/OOS behaviour.

4. Enable the gate:
   ```yaml
   walk_forward:
     enforce_oos_gate: true
     oos_degradation_threshold: 0.15   # example — calibrate from your data
   ```

**Note**: enabling the gate increases Stage 4 (Full WFO) runtime by approximately
2× because each worker now performs 3 strategy evaluations per window (full + IS + OOS)
instead of 1. Plan accordingly.

---
### Understanding Verdicts

The pipeline produces three possible verdicts for each top-ranked candidate:

| Verdict | Meaning | Deployment Status |
|---|---|---|
| `auto_go` | Both WFO and MC Deep pillars passed; no modifier flags triggered | `PAPER_TRADE_REQUIRED` |
| `borderline` | One pillar borderline, or a modifier flag (sensitivity spike, OOS gate, window collapse) triggered | `PAPER_TRADE_REQUIRED` |
| `no_go` | Either pillar failed hard threshold, or ruin probability unacceptably high | N/A — not deployed |

**All verdicts start at `PAPER_TRADE_REQUIRED`**. The operator must manually promote
a candidate to `LIVE_APPROVED` after a satisfactory paper trading period. This
promotion is never automated by the pipeline.

To update deployment status in the database after paper trading:
```sql
UPDATE verdicts
SET deployment_status = 'LIVE_APPROVED',
    deployment_status_updated_at = datetime('now')
WHERE candidate_id = '<candidate_id>';
```

---
### Using the Generated Trading YAML

For `auto_go` and `borderline` candidates, the pipeline generates a ready-to-deploy
strategy YAML in `data/backtesting/runs/<run_id>/trading_yamls/`. Each file:

- Contains all optimised parameters merged into the base strategy config
- Has a `backtester_metadata` section with run provenance (run_id, config_hash,
  seeds, scenario, verdict, WFO score, MC ruin probability)
- Is validated against the `StrategyConfig` schema before being written
- Always contains `deployment_status: PAPER_TRADE_REQUIRED` — never `LIVE_APPROVED`

The metadata section is read-only audit trail — do not modify it manually.

---
### Stage 4 Status (Stub)

Stage 4 (Full WFO) is currently a stub. When called, it logs:
```
Stage 4: Full WFO — stub, not yet implemented
```
and immediately advances the checkpoint to `WFO_COMPLETE`. This means:
- `wfo_consistency_scores` table remains empty
- Stages 5, 6, and 7 receive no input candidates and produce no output
- The pipeline still completes cleanly (`checkpoint = COMPLETE`)

Full Stage 4 implementation is planned for Block 9G. Until then, to exercise
Stages 5–7 manually (e.g. for integration testing), you can populate
`wfo_consistency_scores` directly with synthetic data or stub scores.

# OPERATOR RUNBOOK Block 9F Delta
**Append to**: docs/backtesting/OPERATOR_RUNBOOK.md
**Date**: 2026-03-06

---
## Block 9F — First Pipeline Run Results & Fixes

### Run 1 (capital_accumulation, min_win_rate=0.45)
- Result: 50/50 candidates failed Stage 1
- Cause: Strategy producing ~10% win rates across all parameter combinations
- Fix: min_win_rate eased to 0.15 in backtest_1st_run.yaml
- Lesson: Strategy win rates in this parameter space are well below typical
  thresholds. Consider reviewing base YAML filter configuration.

### Run 2 (capital_accumulation, min_win_rate=0.15)
- Result: 0/50 passed (still failing — best win_rate observed ~11.7%)
- Pipeline crashed at Stage 3 with ValueError from initialise_population()
- Fix applied: B9F-002 (graceful Stage 3 skip)

### Run 3 (e2e_test scenario)
- Result: 50/50 passed Stage 1
- Stage 2 crashed: "CandidateResult is invalid (error: None)"
- Fix applied: B9F-003 (re-evaluate in Stage 2)

### Run 4 (e2e_test, B9F-003 applied)
- Stage 2 ran but all 30 candidates failed MC with:
  "Cannot extract pnl from trade object type Trade. Expected attribute 'pnl'"
- Fix applied: B9F-004 (trade.pnl_points)

### Run 5 (e2e_test, B9F-004 applied)
- Stage 2 completed. Stage 3 (GA) ran.
- GA WFO evaluation failed: "evaluate() got an unexpected keyword argument 'date_start'"
- Fix applied: B9F-005 (strategy_runner date_start/date_end)
- B9F-005 NOT yet run-tested — deploy and confirm in Block 9G.

---
## Constraints Calibration Log

| Date | Scenario | Constraint | Old Value | New Value | Reason |
|---|---|---|---|---|---|
| 2026-03-06 | capital_accumulation | min_win_rate | 0.45 | 0.15 | 0/50 passed; max observed ~11.7% |

**TODO Block 9G**: After first successful capital_accumulation run, query actual
win_rate distribution and set min_win_rate to a calibrated value (e.g. P10 of
observed win_rates among all candidates, or a strategy-appropriate floor).

---
## Troubleshooting — New Entries

### "CandidateResult is invalid (error: None) — cannot run MC simulation"
**Cause**: Stage 2 was calling `store.get_candidate_result()` which returns
`trades=None` because trade objects are never persisted to SQLite.
**Fix**: B9F-003 — Stage 2 now re-evaluates via `strategy_runner.evaluate()`.
**Status**: Fixed.

### "Cannot extract pnl from trade object type Trade. Expected attribute 'pnl'"
**Cause**: `extract_trade_returns()` was looking for `trade.pnl` but the Trade
dataclass uses `trade.pnl_points`.
**Fix**: B9F-004 — `extract_trade_returns()` now uses `trade.pnl_points`.
**Status**: Fixed.

### "evaluate() got an unexpected keyword argument 'date_start'"
**Cause**: `wfo_evaluator.py` passes `date_start`/`date_end` to `evaluate()` for
WFO window scoping, but `strategy_runner.evaluate()` did not accept these params.
**Fix**: B9F-005 — `evaluate()` now accepts `date_start`/`date_end` and injects
them as `data.date_range.start`/`.end` overrides in the temp YAML.
**Status**: Fixed. Verify in Block 9G first run.

### "Stage 3: No MC_PREFILTER_PASS candidates available — skipping GA"
This is now a WARNING (not a crash). Expected when Stage 1 or 2 produces no
survivors. Pipeline continues to Stages 4–7 which handle empty input gracefully.
**Status**: Fixed (B9F-002).

---
## Block 9G Run Procedure

1. Deploy all Block 9F output files (see CONTEXT.md STEP 1)
2. Delete `outputs/backtesting/backtester.db`
3. Set `scenario: "e2e_test"` in backtest_1st_run.yaml
4. Run pipeline — confirm Stages 1–3 complete cleanly
5. Set `scenario: "capital_accumulation"`, delete DB again
6. Run pipeline — collect calibration query results (Queries 3–7 from Block 9F CONTEXT.md)
7. Report to Claude for B8B-012 + B8B-003 calibration

# OPERATOR_RUNBOOK_9G_DELTA.md — Block 9G Appendix
**Append to**: `docs/backtesting/OPERATOR_RUNBOOK.md`
**Date**: 2026-03-06
**Block**: 9G

---

## Pipeline is Now Fully Integrated

As of Block 9G, all 7 pipeline stages are fully implemented and operational.
The `e2e_test` scenario has been validated end-to-end. The pipeline is ready
for production configuration runs.

---

## Reference: Clean Run Log Pattern

A clean run produces exactly this Stage progression with no ERRORs:

```
Stage 0: Validation & Init        — All validations passed — N WFO windows, N enabled zones
Stage 1: Random Search complete   — evaluated=N passed=N failed=0
Stage 2: MC Pre-Filter complete   — pass=N fail=0 total=N
Stage 3: Genetic Algorithm complete
Stage 4: Full WFO complete        — N/N candidates scored
Stage 5: MC Deep complete         — N/N candidates processed
Stage 6: Sensitivity complete     — N/N candidates processed
Stage 7: Report & Output complete — run_id=...
```

**Expected WARNINGs (not bugs)**:
```
WARNING  wfo.consistency_scorer — No valid window results for candidate XXXX
WARNING  wfo.wfo_engine         — Candidate XXXX failed >50% of WFO windows — flagging WFO_INSUFFICIENT_WINDOWS
WARNING  candidate_store        — Candidate XXXX flagged WFO_INSUFFICIENT_WINDOWS
```
These three lines together mean a parameter combination produced no tradeable signals
in any WFO window. The candidate is correctly scored 0.0 and excluded downstream.
Frequency depends on how many candidates the strategy refuses to trade over the
evaluation period — expect more of these as parameter ranges tighten in calibration.

---

## How to Run (reminder)

```powershell
# From project root, venv activated:
python scripts/runners/run_backtester.py

# Monitor live:
Get-Content outputs\backtesting\pipeline_<run_id>.log -Wait -Tail 20

# Post-run error check:
Select-String -Path "outputs\backtesting\pipeline_<run_id>.log" -Pattern "ERROR|WARNING" `
  | Select-Object -ExpandProperty Line
```

---

## Transitioning to Production Config

The `backtest_1st_run.yaml` was calibrated for e2e_test pipeline validation.
Before a production calibration run, restore these values:

| Setting | e2e_test value | Production value |
|---------|---------------|------------------|
| `scenario` | `e2e_test` | `capital_accumulation` |
| `random_search.samples_per_zone` | 50 | 200 |
| `mc_prefilter.input_count` | 30 | 120 |
| `genetic.population_size` | 20 | 60 |
| `genetic.generations` | 5 | 30 |
| `genetic.stagnation_generations` | 3 | 10 |
| `run.max_workers` | 4 | 6 |
| `scenarios.capital_accumulation.constraints.min_win_rate` | 0.15 | calibrate (was 0.45, too strict) |

**Do not restore `exploration.enabled: true`** until B9F-001 is fixed
(expand_zones Cartesian product OOM — ~387T combinations).

---

## Interpreting Stage 7 Output

### Trading YAMLs
Location: `outputs/backtesting/trading_yamls/{run_id[:8]}_{candidate_id[:12]}_strategy.yaml`

Each file is a complete, runnable strategy config with:
- All candidate parameters merged into the correct template locations
- A `backtester_metadata` section with full run provenance

**The `deployment_status` field is always `PAPER_TRADE_REQUIRED` in pipeline output.**
Changing it to `LIVE_APPROVED` is a manual operator action — never automated.

### Verdict interpretation
| Verdict | Meaning |
|---------|---------|
| `auto_go` | WFO ≥ `go_wfo_floor` AND ruin ≤ `go_mc_ruin_ceiling` |
| `borderline` | WFO ≥ `borderline_wfo_floor` AND ruin ≤ `borderline_mc_ruin_ceiling` |
| `no_go` | Failed WFO floor OR ruin too high OR MC Deep failed |

**Note**: In `e2e_test` scenario, thresholds are intentionally loose
(`go_wfo_floor: 0.01`, `go_mc_ruin_ceiling: 0.99`). All candidates with any WFO
signal will receive `auto_go`. These verdicts are not meaningful for trading decisions.

---

## Known Calibration TODOs (B8B-012, B8B-003)

After the first **production** run (non-e2e_test), perform:

1. **B8B-012** — Calibrate `_sigmoid_normalise` scale in `consistency_scorer.py`:
   ```python
   # Current (wrong for real P&L):
   scale = 0.10
   # Fix: measure net_pnl distribution from first real run
   # Set: scale ≈ stdev(net_pnl_across_candidates) * 0.5
   ```

2. **B8B-003** — Calibrate expectancy normalisation in `fitness.py`:
   ```python
   # Current:
   expectancy_norm = expectancy_points / 3.0
   # Fix: set divisor to 95th-percentile expectancy from first real run
   ```

Neither fix requires a code change today. Collect the distribution data first.

---

## Diagnosing yaml_generator Failures

If Stage 7 logs:
```
ERROR Stage 7: Failed to write trading YAML for candidate XXXX:
  Trading YAML validation failed for ..._strategy.yaml:
  missing required sections: [...]
  Sections present: [...]
```

**Diagnostic steps**:
1. Check "Sections present" — if `strategy` or `parameters` appear, `_PARAM_MAP`
   in `yaml_generator.py` is writing to phantom sections. Update the map.
2. Cross-reference every entry in `_PARAM_MAP` against `strategy_template.yaml`
   top-level structure.
3. Check `_structural_validate` `required_sections` — must be real template keys
   (`filters`, `trade_management`), never inferred keys.
4. If new parameters were added to a zone, ensure they are in both
   `yaml_generator._PARAM_MAP` AND `strategy_runner._PARAM_KEY_MAP`.

---

## WFO Window Design Reference

Current windows (from `backtest_1st_run.yaml`):
```
W01: 2025-09-15 → 2025-10-03  (18 calendar days)
W02: 2025-10-06 → 2025-10-24  (18 calendar days, 3-day gap)
W03: 2025-10-27 → 2025-11-14  (18 calendar days, 3-day gap)
W04: 2025-11-17 → 2025-12-05  (18 calendar days, 3-day gap)
W05: 2025-12-08 → 2025-12-17  (9 calendar days,  3-day gap)
```
Minimum 3 windows required (Stage 0 enforces). Gaps are weekend buffers.
`enforce_oos_gate: false` — do not enable until `oos_degradation_threshold` calibrated.
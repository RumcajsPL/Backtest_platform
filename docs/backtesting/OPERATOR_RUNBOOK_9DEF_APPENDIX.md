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
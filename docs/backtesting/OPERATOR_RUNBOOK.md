# OPERATOR_RUNBOOK.md — Backtesting & Optimization Framework
**Version**: 4.0.0
**Date**: 2026-03-11
**Audience**: Operators launching, monitoring, and acting on pipeline runs

---

## Quick Reference

```bash
# Standard run (cleans temp files and cache automatically)
python scripts/runners/run_backtester.py --config configs/backtesting/backtest_V1_01.yaml

# Resume an interrupted run (skip auto-clean, keep checkpoint)
python scripts/runners/run_backtester.py --config configs/backtesting/backtest_V1_01.yaml --no-clean

# Clean everything and start fresh (deletes DB)
python scripts/runners/run_backtester.py --config configs/backtesting/backtest_V1_01.yaml --clean-db

# Query a completed run
python scripts/diagnostics/query_run.py --run-id <run_id> --section all

# Manual environment clean only (no run)
python src/utils/run_cleaner.py
```

---

## §1 — Pre-Run Checklist

Complete every item before launching. Do not skip steps.

### 1.1 Scenario Selection

Set the active scenario at the top of your YAML:
```yaml
scenario: "capital_accumulation"
```

| Scenario | Purpose | When to use |
|---|---|---|
| `capital_accumulation` | Grow account, controlled risk | **Default for all production runs** |
| `swing_trading` | Maximise R:R on directional signals | Fewer, higher-quality trades |
| `conservative` | Preserve capital above all else | Low drawdown tolerance |
| `e2e_test` | Pipeline validation only — very loose constraints | **Never for trading decisions** |

`e2e_test` passes nearly every candidate through all stages regardless of quality. Its verdicts are meaningless for trading. Use it only to verify the pipeline runs end-to-end after code changes.

### 1.2 Data Range and WFO Window Alignment

The `data.date_range` in your YAML controls what the strategy evaluates in Stage 1.
The `walk_forward.windows` list controls what WFO evaluates in Stage 4.
**Both must cover the same date range** — windows outside the data range produce `REJECTED_INSUFFICIENT_TRADES`.

```yaml
data:
  date_range:
    start: "2023-01-02 00:00:00"
    end:   "2026-02-28 23:59:59"

walk_forward:
  windows:
    - {id: W01, start: "2023-01-02", end: "2023-03-31"}
    - {id: W02, start: "2023-04-03", end: "2023-06-30"}
    # ...
```

Rules enforced at Stage 0:
- Minimum 3 windows (required for GA diversity)
- Unique IDs
- `start < end` for every window
- No overlapping date ranges

### 1.3 Worker Count

```yaml
run:
  max_workers: 2
```

For full-history runs (data range > 6 months), **`max_workers: 2` is a hard limit**. Each worker loads the full LTF dataset into memory for trade simulation. On an 8GB RAM machine, more than 2 workers causes out-of-memory failures. For short (3-month) runs, up to 6 workers is safe.

### 1.4 MC Pre-Filter — Disable for Full-History Runs

```yaml
stages:
  mc_prefilter: false     # REQUIRED for data ranges > ~6 months
  genetic_algorithm: true
  # ... other stages default to true when omitted
```

MC perturbation compounds over long equity curves, producing false ruin signals on viable candidates. For full-history runs, Stage 4 WFO is the correct gate. Leave `mc_prefilter: true` only for short (3-month) runs.

### 1.5 Constraint Calibration for Data Range

Constraints behave differently depending on your Stage 1 date range:

| Constraint | 3-month run | Full-history (38-month) run |
|---|---|---|
| `min_win_rate` | 0.11 | 0.11 (same) |
| `min_expectancy` | -2.0 | -2.0 (same) |
| `min_profit_factor` | 0.75 | 0.75 (same) |
| `max_drawdown` | **remove or set 1.0** | **remove — accumulates over full range** |
| `max_losing_streak` | 50 | 200 (observed max ~91 over 38 months) |
| `min_trades_per_week` | 3.0 | 3.0 (same) |

The `max_drawdown` constraint measures the total drawdown over the entire Stage 1 date range — on a 38-month run this accumulates to near-1.0 for almost every candidate and eliminates nearly everything. Remove it for full-history runs; the WFO per-window drawdown gate (`wfo_collapse_drawdown_threshold`) provides the correct check.

### 1.6 Seed Documentation

Five seeds are embedded in every run for reproducibility:
```yaml
random_search:
  seed: 42
mc_prefilter:
  seed: 43
genetic:
  seed: 44
monte_carlo:
  deep:
    seed: 45
sensitivity:
  seed: 46
```

Seeds are stored in the `runs` table and embedded in all output YAMLs. Never change seeds mid-run.

### 1.7 Stage Input Counts

Three separate `input_count` settings control how many candidates each late stage processes:

```yaml
monte_carlo:
  deep:
    input_count: 10      # Stage 5: top N candidates by WFO score go to MC Deep

sensitivity:
  input_count: 5         # Stage 6 AND Stage 7: same set used for both
                         # To give verdicts to all MC-evaluated candidates,
                         # set this equal to monte_carlo.deep.input_count
```

A candidate appearing in Stage 5 MC results but absent from Stage 7 verdicts is **expected behaviour** — it ranked outside `sensitivity.input_count` by WFO score. This is not a bug.

---

## §2 — Configuration Reference

### Full YAML structure with all meaningful settings

```yaml
# ─── Run Identity ────────────────────────────────────────────────────────────
scenario: "capital_accumulation"   # active scenario (must match a key in scenarios:)

run:
  output_dir: "outputs/backtesting"
  temp_dir:   "temp/backtesting"
  max_workers: 2                   # hard limit 2 for full-history; up to 6 for 3-month

# ─── Stage Toggles ────────────────────────────────────────────────────────────
stages:
  mc_prefilter:      false   # disable for data ranges > ~6 months
  genetic_algorithm: true
  wfo:               true
  monte_carlo_deep:  true
  sensitivity:       true
  verdict:           true

# ─── Data ─────────────────────────────────────────────────────────────────────
data:
  strategy_data: "data/dax_ltf.parquet"
  htf_data:      "data/dax_htf.parquet"
  date_range:
    start: "2023-01-02 00:00:00"
    end:   "2026-02-28 23:59:59"

# ─── Walk-Forward Windows ─────────────────────────────────────────────────────
walk_forward:
  windows:
    - {id: W01, start: "2023-01-02", end: "2023-03-31"}
    - {id: W02, start: "2023-04-03", end: "2023-06-30"}
    # ... add more windows — minimum 3 required
  enforce_oos_gate: false            # leave false unless you have calibrated
                                     # oos_degradation_threshold (see §6.3)
  oos_degradation_threshold: 0.50    # only used when enforce_oos_gate: true

# ─── Random Search ────────────────────────────────────────────────────────────
random_search:
  method: "lhs"                  # lhs (recommended) or random
  samples_per_zone: 200          # per zone; 200 = 400 total for 2 zones
  min_significant_trades: 30     # candidates with fewer trades are rejected
  seed: 42

# ─── MC Pre-Filter (Stage 2) ──────────────────────────────────────────────────
mc_prefilter:
  input_count: 120               # top N from Stage 1 to screen
  iterations: 300                # cheap; low iteration count is intentional
  perturbation_profile: "default"
  ruin_threshold: 0.25
  seed: 43

# ─── Genetic Algorithm (Stage 3) ──────────────────────────────────────────────
genetic:
  population_size: 60
  generations: 30
  elite_fraction: 0.10
  mutation_rate: 0.15
  crossover_rate: 0.70
  tournament_size: 5
  stagnation_generations: 10
  diversity_penalty_weight: 0.10
  diversity_distance_threshold: 0.15
  seed: 44

# ─── Monte Carlo Deep (Stage 5) ───────────────────────────────────────────────
monte_carlo:
  deep:
    input_count: 10              # top N by WFO score; must be ≥ sensitivity.input_count
                                 # to ensure all verdict candidates had MC evaluated
    iterations: 3000
    perturbation_profile: "default"
    ruin_threshold: 0.20
    seed: 45

# ─── Sensitivity (Stage 6 + 7) ────────────────────────────────────────────────
sensitivity:
  input_count: 5                 # controls BOTH Stage 6 and Stage 7 candidate sets
  spike_threshold: 0.15          # |fitness_delta| > threshold → spike_detected
  seed: 46

# ─── Fitness Weights ──────────────────────────────────────────────────────────
fitness_weights:                 # must sum exactly to 1.0
  net_pnl:        0.20
  expectancy:     0.25
  max_drawdown:   0.20
  win_rate:       0.15
  trade_frequency: 0.10
  profit_factor:  0.10

# ─── Scenarios ────────────────────────────────────────────────────────────────
scenarios:
  capital_accumulation:
    # Verdict thresholds
    go_wfo_floor:                0.40   # WFO score ≥ this → eligible for auto_go
    borderline_wfo_floor:        0.25   # WFO score ≥ this → eligible for borderline
    go_mc_ruin_ceiling:          0.05   # ruin_prob ≤ this → MC pillar passes for go
    borderline_mc_ruin_ceiling:  0.15   # ruin_prob ≤ this → MC pillar passes for borderline
    verdict_sensitivity_spike_threshold: 0.15   # must match sensitivity.spike_threshold

    # Per-window collapse threshold (raw points — DAX default 400 pts)
    wfo_collapse_drawdown_threshold: 400.0

    # Normalisation (DAX-calibrated)
    normalisation_expectancy_ref_pts: 3.0
    normalisation_freq_ref_trades_per_week: 20.0

    # Fitness constraints (see §1.5 for full-history adjustments)
    constraints:
      min_win_rate:        0.11
      min_expectancy:      -2.0
      min_profit_factor:   0.75
      max_losing_streak:   200     # set 50 for 3-month runs
      min_trades_per_week: 3.0
      # max_drawdown: omit for full-history runs

    # Fitness weights must match top-level fitness_weights
    fitness_weights:
      net_pnl:        0.20
      expectancy:     0.25
      max_drawdown:   0.20
      win_rate:       0.15
      trade_frequency: 0.10
      profit_factor:  0.10

    report_emphasis: [wfo_consistency_score, mc_deep_ruin_probability]

# ─── Parameter Space ──────────────────────────────────────────────────────────
zones:
  safe:
    enabled: true
    parameters:
      rsi_period:     {type: int,   min: 8,   max: 24,  step: 1}
      atr_length:     {type: int,   min: 5,   max: 25,  step: 1}
      atr_multiplier: {type: float, min: 1.2, max: 3.0, step: 0.1}
      rr_target:      {type: float, min: 3.5, max: 10.0, step: 0.1}
      risk_percentile: {type: float, min: 0.15, max: 0.65, step: 0.01}
      bollinger_length:     {type: int,   min: 10, max: 20, step: 1}
      bollinger_multiplier: {type: float, min: 0.3, max: 2.0, step: 0.1}
      # ... add parameters matching strategy_runner._PARAM_KEY_MAP

  exploration:
    enabled: true
    parameters:
      # Wider ranges than safe zone — same parameter names
```

### Key rules for the zones section

- Parameter names must exactly match entries in `strategy_runner._PARAM_KEY_MAP`. Stage 0 validates this and raises an error if any name is unrecognised.
- When you add a new strategy parameter to any zone, you must update **both** `strategy_runner._PARAM_KEY_MAP` **and** `yaml_generator._PARAM_MAP`. Both files contain a co-update warning comment. Updating only one causes silent mapping failures in either candidate evaluation or trading YAML generation.
- `bollinger_width_ma` and similar integer parameters must use integer `step` values. Float steps on integer parameters cause silent errors.
- All parameter values in a zone must actually exist in the strategy template. Test after adding parameters with a short `e2e_test` run.

---

## §3 — Running the Pipeline

### Standard launch
```bash
python scripts/runners/run_backtester.py --config configs/backtesting/backtest_V1_01.yaml
```

The runner automatically calls `run_cleaner.py` before starting, which:
- Clears all temp candidate YAMLs from `temp/backtesting/`
- Clears the strategy data cache (`~/.wbws_data_cache/`)

This is necessary because leftover temp files from crashed runs cause evaluation failures on restart, and stale cache files can cause memory issues.

### Resuming an interrupted run
```bash
python scripts/runners/run_backtester.py --config configs/backtesting/backtest_V1_01.yaml --no-clean
```

Use `--no-clean` when you want to resume from the last checkpoint. The pipeline reads the checkpoint from the database and skips all completed stages. **Do not modify the YAML between an interrupted run and its resume** — the pipeline compares the config hash and will reject a modified config.

### Starting completely fresh
```bash
python scripts/runners/run_backtester.py --config configs/backtesting/backtest_V1_01.yaml --clean-db
```

`--clean-db` deletes the database before starting. Use this when you have intentionally changed the config and want a new run from scratch.

### Recommended log setup
Add this to your runner script or startup:
```python
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("outputs/backtesting/pipeline.log"),
    ]
)
```

Monitor a live run:
```powershell
# Windows
Get-Content outputs\backtesting\pipeline.log -Wait -Tail 30

# Post-run: check for errors and warnings
Select-String -Path "outputs\backtesting\pipeline.log" -Pattern "ERROR|WARNING"
```

---

## §4 — Monitoring a Run

### Expected log progression (clean run)

```
Stage 0: All validations passed — N WFO windows, N enabled zones
Stage 1: Random Search complete   — evaluated=400  passed=N  failed=N
Stage 2: MC Pre-Filter complete   — pass=N  fail=0  total=N
         (or: Stage 2 (MC Pre-Filter) disabled in config — skipping)
Stage 3: Genetic Algorithm complete
Stage 4: Full WFO complete        — N/N candidates scored
Stage 5: MC Deep complete         — N/N candidates processed
Stage 6: Sensitivity complete     — N/N candidates processed
Stage 7: Report & Output complete — run_id=<id>
```

Followed by:
```
TIMING SUMMARY  stage5=Xs  stage6=Xs  stage7=Xs  total=Xs  budget=14400s  PASS
```

### Expected WARNINGs that are not errors

These three lines together are normal — they mean a candidate produced no tradeable signals in any WFO window:
```
WARNING  wfo.consistency_scorer — No valid window results for candidate XXXX
WARNING  wfo.wfo_engine         — Candidate XXXX failed >50% of WFO windows — flagging WFO_INSUFFICIENT_WINDOWS
WARNING  candidate_store        — Candidate XXXX flagged WFO_INSUFFICIENT_WINDOWS
```
The candidate is scored 0.0 and correctly excluded from later stages.

These warnings during the GA stage are also non-blocking (Windows file-lock race on temp YAML cleanup — cosmetic):
```
WARNING: Temp YAML cleanup failed: [WinError 32]
```

### Typical stage durations

| Stage | 3-month run | Full-history (38-month) |
|---|---|---|
| Stage 0 | < 5s | < 5s |
| Stage 1 | 10–60 min | 3–8 hours |
| Stage 2 | 1–5 min | disabled |
| Stage 3 | 5–30 min | 2–4 hours |
| Stage 4 | varies | 6–10 hours |
| Stage 5 | < 5s | < 30s |
| Stage 6 | 5–8 min | 5–8 min |
| Stage 7 | < 10s | < 10s |
| **Total** | **~1–2 hours** | **~14–20 hours** |

Stage 5 (MC Deep) is always fast — fully vectorised. Stage 6 (Sensitivity) is the dominant cost for short runs (~66–89s per candidate). Stage 4 (Full WFO) is the dominant cost for full-history runs.

### Quick health check query
Run this after any pipeline completes to get a one-line status:
```sql
SELECT
    r.run_id,
    r.scenario_name,
    r.checkpoint,
    COUNT(DISTINCT CASE WHEN e.stage='RANDOM' THEN e.candidate_id END) as random_total,
    SUM(CASE WHEN e.stage='RANDOM' AND e.passed_constraints=1 THEN 1 ELSE 0 END) as random_pass,
    COUNT(DISTINCT wcs.candidate_id) as wfo_scored,
    COUNT(DISTINCT v.candidate_id) as verdicts
FROM runs r
LEFT JOIN evaluations e ON r.run_id = e.run_id
LEFT JOIN wfo_consistency_scores wcs ON r.run_id = wcs.run_id
LEFT JOIN verdicts v ON r.run_id = v.run_id
WHERE r.run_id = '<your_run_id>'
GROUP BY r.run_id;
```

Or use the diagnostic script:
```bash
python scripts/diagnostics/query_run.py --run-id <run_id> --section all
```

---

## §5 — Reading the Outputs

### Output directory structure
```
outputs/backtesting/
├── backtester.db                       ← SQLite WAL — single source of truth
├── pipeline.log                        ← Full run log
├── report.html                         ← Self-contained HTML report with charts
├── json/
│   └── {run_id}_report.json
├── parquet/
│   └── {run_id}_results.parquet
└── trading_yamls/
    └── {run_id[:8]}_{candidate_id[:12]}_strategy.yaml   ← auto_go + borderline only
```

### Understanding verdicts

The pipeline produces three possible verdicts for each top-ranked candidate.

**`auto_go`** — Both pillars passed all go thresholds and no modifier flags triggered:
- WFO composite score ≥ `go_wfo_floor` (default 0.40)
- MC Deep ruin probability ≤ `go_mc_ruin_ceiling` (default 0.05)
- No sensitivity spike, no window collapse, complete sensitivity profile

**`borderline`** — One or both pillars in the borderline zone, or at least one modifier flag:
- WFO score between `borderline_wfo_floor` (0.25) and `go_wfo_floor` (0.40)
- Ruin probability between `go_mc_ruin_ceiling` (0.05) and `borderline_mc_ruin_ceiling` (0.15)
- A sensitivity spike was detected (`|delta| > spike_threshold` on any parameter)
- A WFO window collapse was flagged (worst drawdown > `wfo_collapse_drawdown_threshold`)
- More than 50% of sensitivity evaluations failed (`profile_complete = False`)
- OOS gate triggered (when `enforce_oos_gate: true`)

**`no_go`** — One or both pillars failed hard thresholds. Do not trade:
- WFO score < `borderline_wfo_floor`
- MC ruin probability > `borderline_mc_ruin_ceiling`
- MC result was None (evaluation error — treated as maximum risk)

All verdicts carry `deployment_status: PAPER_TRADE_REQUIRED`. The pipeline never sets `LIVE_APPROVED` — this is an operator-only manual action after paper trading validation.

### Threshold reference (capital_accumulation defaults)

```
go_wfo_floor:                0.40
borderline_wfo_floor:        0.25
go_mc_ruin_ceiling:          0.05   (5% of MC paths reach ruin)
borderline_mc_ruin_ceiling:  0.15
sensitivity_spike_threshold: 0.15   (15-point fitness shift on ±1 parameter step)
wfo_collapse_drawdown_threshold: 400.0 pts per window
```

### Reading the WFO consistency score

The WFO score is a composite of four sub-metrics, each mapped to [0, 1]:

| Sub-metric | What it measures | Good value |
|---|---|---|
| `fraction_positive_windows` | Share of WFO windows where the strategy made money | ≥ 0.70 |
| `median_return` | Median net P&L across windows (sigmoid-normalised) | Positive |
| `variance_score` | Consistency of returns across windows (lower variance = higher score) | ≥ 0.50 |
| `worst_drawdown` | Worst per-window drawdown (normalised) | Lower is better |

A high WFO score from consistent losses scores badly on `fraction_positive_windows`. A candidate with `frac_pos = 0.17` (1 profitable window out of 12) may still score WFO > 0.60 — inspect `frac_pos` and `median_return` individually, not just the composite.

**Useful DB queries:**
```sql
-- WFO scores with detail for a run
SELECT
    w.candidate_id,
    w.composite_score,
    w.fraction_positive_windows,
    w.median_return,
    w.window_collapse_flag,
    v.verdict
FROM wfo_consistency_scores w
LEFT JOIN verdicts v ON w.candidate_id = v.candidate_id AND v.run_id = w.run_id
WHERE w.run_id = '<run_id>'
ORDER BY w.composite_score DESC;

-- Per-window breakdown for a specific candidate
SELECT window_id, net_pnl, win_rate, total_trades, drawdown, fitness_score
FROM wfo_window_results
WHERE run_id = '<run_id>'
  AND candidate_id = '<candidate_id>'
ORDER BY window_id;
```

### Reading MC Deep results

```sql
SELECT
    m.candidate_id,
    m.ruin_probability,
    m.avg_final_equity,
    m.p5_final_equity,
    m.worst_drawdown
FROM mc_results m
WHERE m.run_id = '<run_id>'
  AND m.mode = 'deep'
ORDER BY m.ruin_probability ASC;
```

Key fields:
- `ruin_probability`: fraction of 3,000 MC paths that hit the ruin floor. 0.000 is ideal; anything above 0.15 is no_go territory.
- `avg_final_equity`: mean equity at end of MC simulation. High average with low ruin = strong candidate.
- `p5_final_equity`: 5th-percentile final equity — worst-case scenario for 95% of paths.

### Reading sensitivity results

```sql
-- Spikes and parameter deltas for top candidates
SELECT
    s.candidate_id,
    s.parameter_name,
    s.step,
    s.perturbed_value,
    s.delta,
    s.is_spike
FROM sensitivity_results s
WHERE s.run_id = '<run_id>'
  AND s.candidate_id = '<candidate_id>'
ORDER BY ABS(s.delta) DESC;
```

A spike (`is_spike = 1`) means the fitness changes sharply when you move that parameter by one step. This downgrades the candidate to `borderline` — the strategy is fragile to that dimension. However, an asymmetric spike (large positive delta in one direction, flat in the other) indicates the current value may not be optimal and there is room for improvement. Check both step directions before concluding the candidate is weak.

Frequent `REJECTED_CONSTRAINTS` errors in perturbation results (visible as `delta = NULL, error = 'constraints_failed'`) mean the candidate sits in a narrow feasible region — many neighbouring parameter values violate constraints. This is a fragility signal even without a formal spike.

### The trading YAML

Each trading YAML in `outputs/backtesting/trading_yamls/` is a complete, runnable strategy config with:
- All optimised parameters merged into the correct strategy template locations
- A `backtester_metadata` block containing full provenance: `run_id`, `candidate_id`, `zone_name`, `config_hash`, `scenario_name`, `verdict`, `wfo_consistency_score`, `mc_deep_ruin_probability`, `sensitivity_spike`, `deployment_status`, and all 5 seeds

**The `backtester_metadata` block is read-only audit trail. Do not modify it.**

`deployment_status` is always `PAPER_TRADE_REQUIRED` in pipeline output.

---

## §6 — Calibration Guide

### 6.1 When to recalibrate

| Trigger | What to recalibrate |
|---|---|
| New instrument (e.g. FTSE, ES) | All four normalisation constants (see §6.2) |
| Data range changes by > 6 months | `_SIGMOID_SCALE` and `_MAX_EXPECTED_DRAWDOWN` |
| Strategy trade frequency changes significantly | `_SIGMOID_SCALE` |
| First run on a new data slice | Verify Stage 1 pass rate (target 30–60%); adjust constraints if far outside range |

### 6.2 Normalisation constants (DAX-calibrated — all in raw points)

These four constants live in `src/backtesting/wfo/consistency_scorer.py`:

| Constant | DAX 3-month value | DAX full-history value | How to derive |
|---|---|---|---|
| `_SIGMOID_SCALE` | 131.0 | 310.0 | `stdev(net_pnl) × 0.5` from Stage 1+4 only run |
| `_MAX_EXPECTED_DRAWDOWN` | 1,000.0 | 2,500.0 | ~2.5× worst observed per-window drawdown |
| `_MAX_EXPECTED_VARIANCE` | 100,000.0 | _(unchanged)_ | `stdev²` conservative ceiling |
| `wfo_collapse_drawdown_threshold` | 400.0 pts | 400.0 pts (per-window — does not change) | ~4% of reference account in pts |

**Two-track rule**: The 3-month and full-history tracks use different constants. Do not use full-history constants for short runs or vice versa. Restore 3-month values (`_SIGMOID_SCALE = 131.0`, `_MAX_EXPECTED_DRAWDOWN = 1000.0`) before running any new 3-month production run after full-history work.

**Recalibration procedure for `_SIGMOID_SCALE`**:
1. Run a calibration pass (Stages 1 + 4 only — disable MC, GA, sensitivity, verdict in `stages:`)
2. Run `python calibrate_sigmoid.py` (in `outputs/`) — it reads `wfo_window_results` and outputs `stdev × 0.5`
3. Update `_SIGMOID_SCALE` in `consistency_scorer.py`
4. Do not use a run with GA enabled for calibration — GA partial-window (2-window) net_pnl inflates stdev and produces a wrong scale

### 6.3 OOS gate calibration

The IS/OOS gate (`enforce_oos_gate: true`) splits each WFO window into a 70% in-sample and 30% out-of-sample period and checks whether OOS performance degrades significantly. It is disabled by default and requires calibration before use.

To calibrate:
1. Run the pipeline with `enforce_oos_gate: true` and a small `samples_per_zone` (10–20) to generate `oos_delta` values
2. Query the distribution:
   ```sql
   SELECT
       COUNT(*) as n,
       MIN(oos_delta)  as min_delta,
       AVG(oos_delta)  as avg_delta,
       MAX(oos_delta)  as max_delta
   FROM wfo_window_results
   WHERE run_id = '<calibration_run_id>'
     AND oos_delta IS NOT NULL;
   ```
3. Set `oos_degradation_threshold` to approximately the 10th percentile of `abs(oos_delta)` — so that only the worst 10% of candidates (most OOS-degraded) trigger the gate. The default `0.50` is very lenient; the real value is likely in the range 0.10–0.25.
4. Enabling the gate roughly doubles Stage 4 runtime (3 strategy evaluations per window instead of 1).

### 6.4 Constraint calibration

If Stage 1 pass rate is far outside the 30–60% target:

**Too few passing (<15%)**: constraints are too tight for this data range. Query Stage 1 distributions:
```sql
SELECT
    AVG(actual_win_rate)         as avg_win_rate,
    AVG(actual_expectancy)       as avg_expectancy,
    AVG(actual_profit_factor)    as avg_pf,
    COUNT(*) as n
FROM evaluations
WHERE run_id = '<run_id>'
  AND stage = 'RANDOM';
```
Compare averages to your constraint floors. The constraint closest to the average is the one cutting too aggressively.

**Too many passing (>80%)**: constraints are too loose. Use the same query — the constraint furthest below its average is doing the least work. Tighten it incrementally.

---

## §7 — Paper Trading Workflow

### Promotion path

```
Pipeline verdict
    │
    └── auto_go or borderline
            │
            ▼
    PAPER_TRADE_REQUIRED         ← all pipeline output starts here
            │
    [Operator: run paper trading]
    Minimum period: one full WFO window equivalent
    (~3 weeks for 3-month data, ~3 months for full-history data)
            │
    [Operator: review results]
            │
            ▼
    Manually update database:
    UPDATE verdicts
    SET deployment_status = 'LIVE_APPROVED'
    WHERE candidate_id = '<id>';
            │
            ▼
    LIVE_APPROVED               ← never set by pipeline code
```

### Candidate selection guidance

When multiple candidates carry verdicts, prioritise as follows:

1. **WFO score** — higher is better, but inspect `fraction_positive_windows`. A score built on 1 profitable window out of 12 is less robust than a score built on 9 of 13.
2. **Ruin probability** — `ruin_prob = 0.000` is strongly preferred. Avoid candidates above 0.05 for live trading.
3. **Sensitivity profile** — candidates with no spikes and no `REJECTED_CONSTRAINTS` perturbation errors are more reliable. A spike in one direction (asymmetric) is less concerning than symmetric fragility.
4. **Parameter region density** — many `constraints_failed` perturbation errors indicate the candidate sits in a narrow feasible pocket. Regime shifts may push it outside this pocket.
5. **MC avg_final_equity** — secondary to ruin_prob but useful for comparing two candidates with identical ruin scores.

Start paper trading with the candidate ranked highest on criteria 1–3. Run a second candidate in parallel only after the first has at least two weeks of clean paper trading data.

### Paper trading monitoring queries

```sql
-- Summary of all live candidates by verdict and deployment status
SELECT
    v.candidate_id,
    v.verdict,
    v.deployment_status,
    w.composite_score   as wfo_score,
    m.ruin_probability,
    s.spike_detected
FROM verdicts v
JOIN wfo_consistency_scores w ON v.candidate_id = w.candidate_id AND v.run_id = w.run_id
JOIN mc_results m ON v.candidate_id = m.candidate_id AND m.run_id = v.run_id AND m.mode = 'deep'
JOIN sensitivity_profiles s ON v.candidate_id = s.candidate_id AND s.run_id = v.run_id
WHERE v.run_id = '<run_id>'
ORDER BY w.composite_score DESC;
```

---

## §8 — Known Limitations and Expected Warnings

### 8.1 Temp YAML file-lock warnings (Windows)

```
WARNING: Temp YAML cleanup failed: [WinError 32]
ERROR: Candidate evaluation failed: Config file... got NoneType
```

These appear during GA stage worker teardown on Windows. The pipeline continues normally — affected candidates receive a null fitness score for that window only and are not promoted to later stages. Rate is typically low (1–3 candidates per run). `run_cleaner.py` clears leftover temp files before every run, preventing accumulation.

### 8.2 Null window results from temp YAML race

Some WFO windows show `fitness = None` in the per-window detail. These are caused by the same Windows file-lock issue or by `REJECTED_INSUFFICIENT_TRADES` (window too short for the strategy to generate enough trades to pass the significance guard). Both are expected and do not invalidate the run — WFO scoring handles missing windows gracefully via the `windows_evaluated` count.

### 8.3 RSI parameters — confirmed zero sensitivity

RSI parameters (`rsi_period`, `rsi_overbought`, `rsi_oversold`) consistently show sensitivity delta ≈ 0.000 across all candidates on DAX. The RSI filter is structurally inactive for this instrument/timeframe configuration. Including these parameters in the search space wastes GA search dimensions. They are retained in V1 for completeness but will be removed from the search space in V2.

### 8.4 MC Deep on full-history data

MC Deep evaluates candidates on the full 38-month continuous equity curve. For candidates with lower WFO scores, MC perturbation (slippage, spread noise, shuffle) can compound over the long curve and produce elevated ruin probabilities that would not appear on shorter windows. The top WFO candidates (score > 0.70) are generally unaffected. If all candidates show ruin > 0.40, check whether the MC ruin issue is specific to that run's candidate quality or systemic.

### 8.5 `completed_at` and `runtime_min` fields

These fields in the `runs` table may show `None` after a completed run. This is a known data capture gap — the `checkpoint = COMPLETE` field is the authoritative completion indicator.

### 8.6 OOS gate

`enforce_oos_gate: true` in the YAML has no effect unless `oos_degradation_threshold` has been calibrated for your data (see §6.3). The gate is implemented and functional — it is disabled by default because the default threshold of `0.50` is deliberately very lenient and will not filter anything in practice until properly calibrated. All `oos_gate_triggered` fields in verdicts will be `False` when the gate has not been calibrated.

---

## Appendix A — Common Error Patterns

| Symptom | Cause | Fix |
|---|---|---|
| Stage 1: 0 candidates pass | Constraints too tight for this data range | Query Stage 1 distributions (§6.4); loosen the constraint closest to the observed average |
| Stage 1: >80% pass rate | Constraints too loose | Tighten the constraint furthest below its average |
| `KeyError` at Stage 0 on a constraint field | Constraint key missing from YAML and scenario.py uses hard dict access | Add the field to the YAML with an appropriate default value |
| `ValueError: Fitness weights must sum to 1.0` | Top-level `fitness_weights` and scenario `fitness_weights` don't sum to 1.0 | Check all 6 weight fields; both blocks must sum to exactly 1.0 |
| `ValueError: spike_threshold must be in (0, 1)` | `sensitivity.spike_threshold` is 0 or ≥ 1 | Valid range is exclusive: (0.0, 1.0) |
| All candidates `borderline` due to `window_collapse_flag` | `wfo_collapse_drawdown_threshold` set too low (e.g. 0.40 fraction instead of 400 pts) | Set threshold in raw instrument points (DAX: 400.0) |
| Candidate in Stage 5 MC results but no Stage 7 verdict | Expected — candidate ranks outside `sensitivity.input_count` by WFO score | Not a bug; raise `sensitivity.input_count` to match `monte_carlo.deep.input_count` if you want verdicts for all MC candidates |
| `REJECTED_INSUFFICIENT_TRADES` for most windows | Window too short for the strategy to trade enough | Extend window dates; or lower `min_significant_trades` carefully |
| New parameter not appearing in trading YAML | `yaml_generator._PARAM_MAP` not updated | Update both `strategy_runner._PARAM_KEY_MAP` AND `yaml_generator._PARAM_MAP` together |
| `[WinError 32]` and NoneType YAML errors | Windows file-lock race on GA temp YAML cleanup | Non-blocking; pipeline continues. `run_cleaner.py` prevents accumulation |
| WFO scores all very similar / no differentiation | `_SIGMOID_SCALE` may need recalibration | Run `calibrate_sigmoid.py` and update the constant |
| MC ruin elevated for all candidates on full-history run | MC perturbation compounding over long equity curve | Expected for lower WFO candidates; top candidates (WFO > 0.70) should be unaffected |

---

## Appendix B — Database Table Reference

| Table | Contents | Key query use |
|---|---|---|
| `runs` | One row per run: `run_id`, `config_hash`, checkpoint, 5 seeds, scenario | Run identity and status |
| `candidates` | One row per unique `candidate_id`: zone_name, parameters_json | Parameter lookup |
| `candidate_parameters` | Individual columns per parameter | Parameter search and filtering |
| `evaluations` | One row per candidate per stage: constraint actuals + fitness_score | Stage 1 pass/fail analysis |
| `wfo_window_results` | One row per candidate per window: net_pnl, win_rate, total_trades, drawdown, oos_delta | Per-window performance |
| `wfo_consistency_scores` | One row per candidate: composite_score, frac_pos, median_return, window_collapse_flag | WFO ranking |
| `mc_results` | One row per candidate per mode ('deep' / 'pre_filter'): ruin_probability, avg_final_equity | MC screening |
| `sensitivity_results` | One row per candidate per parameter per step: delta, is_spike | Parameter fragility |
| `sensitivity_profiles` | Summary per candidate: spike_detected, spike_parameters, profile_complete | Verdict modifier flags |
| `verdicts` | Final verdict per candidate: verdict enum, deployment_status, yaml_output_path | Trade decision |

Full DDL: `docs/backtesting/SQLITE_SCHEMA.md`
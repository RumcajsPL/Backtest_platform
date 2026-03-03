# OPERATOR_RUNBOOK.md — Backtesting & Optimization Framework
**Version**: 1.0.0
**Date**: 2026-03-03
**Audience**: Operators launching, monitoring, and acting on pipeline runs
---
## 1. Pre-Run Checklist
Complete every item before launching a run. Do not skip steps.
### 1.1 Config Hash Verification
The pipeline runs are keyed by a SHA-256 hash of the config file. If you modify `backtest_template.yaml` between runs, a new `run_id` is generated and the previous run's checkpoint is not reused.
```bash
# Generate hash of your config before launching
python -c "
import hashlib, pathlib
data = pathlib.Path('configs/backtesting/backtest_template.yaml').read_bytes()
print(hashlib.sha256(data).hexdigest()[:16])
"
```
Record the hash in your run log before launching. If a run fails and you need to resume, verify the hash matches before restarting.
### 1.2 Scenario Selection
The active scenario is set at the top of `backtest_template.yaml`:
```yaml
scenario: "capital_accumulation"   # Change this line to switch scenario
```
| Scenario | Purpose | When to use |
|---|---|---|
| `capital_accumulation` | Grow account, controlled risk | **Default for production runs** |
| `swing_trading` | Maximise R:R on directional signals | When targeting fewer, higher-quality trades |
| `conservative` | Preserve capital above all else | When drawdown tolerance is low |
| `e2e_test` | Pipeline validation only — loose constraints | **Never for production** |
> **WARNING**: `e2e_test` passes nearly every candidate through all stages regardless of quality. It exists only to verify pipeline plumbing. Do not interpret its verdicts as trading signals.
### 1.3 WFO Window Date Review
WFO windows are defined in `backtest_template.yaml` under `walk_forward.windows`. The current defaults are calibrated for the 3-month OHLCV slice (2025-09-15 → 2025-12-17):
```
W01: 2025-09-15 → 2025-10-03   (~3 weeks)
W02: 2025-10-06 → 2025-10-24
W03: 2025-10-27 → 2025-11-14
W04: 2025-11-17 → 2025-12-05
W05: 2025-12-08 → 2025-12-17
```
**Before launching**:
- Confirm the windows align with your available OHLCV data
- Windows must not overlap (validation enforced at Stage 0)
- Minimum 3 windows required (Stage 0 enforced)
- If using a longer data slice, extend the windows proportionally
### 1.4 Seed Documentation
Five seeds are used across pipeline stages. Record all five before launching — they are required to reproduce any run exactly.
```yaml
random_search.seed:     42    # Stage 1 LHS sampling
mc_prefilter.seed:      43    # Stage 2 MC pre-filter
genetic.seed:           44    # Stage 3 GA evolution
monte_carlo.deep.seed:  45    # Stage 5 MC deep
# Stage 6 sensitivity uses no seed (deterministic ±step perturbation)
```
These seeds are stored in the `runs` table of `backtester.db` and are embedded in all output artefacts for reproducibility.
### 1.5 Worker Count Check
```yaml
run.max_workers: 6
```
Stage 6 (Sensitivity) is the dominant runtime cost. On Windows, each worker spawns a fresh Python interpreter. Match `max_workers` to your physical core count — exceeding it provides no benefit and increases memory pressure. Recommended: physical cores − 1 (leave one free for the OS).
---
## 2. Launching a Run
```bash
python -m src.backtesting.orchestrator --config configs/backtesting/backtest_template.yaml
```
The orchestrator writes all artefacts to `outputs/backtesting/` and temporary candidate YAMLs to `temp/backtesting/` (deleted after each evaluation unless `retain_temp_yamls: true`).
**To run only specific stages** (development only — not for production):
```yaml
# In backtest_template.yaml, set run.mode:
run:
  mode: "random_only"    # Runs Stage 0 + Stage 1 only
  # mode: "ga_only"      # Runs Stage 0 + Stage 3 only (requires existing MC_PREFILTER_PASS candidates)
  # mode: "full_pipeline" # Default — all 8 stages
```
**To disable individual stages** (development only):
```yaml
stages:
  random_search: true
  mc_prefilter: false    # Skip Stage 2 — use existing pre-filter results
  genetic_algorithm: true
  walk_forward: true
  monte_carlo_deep: true
  sensitivity: true
  report: true
```
> Only use stage disabling when re-running a partial pipeline with a compatible checkpoint. Setting `stages.X: false` does not add a skip checkpoint — the orchestrator simply does not execute that stage.
---
## 3. Monitoring Progress
### 3.1 Checkpoint Log Lines
The orchestrator logs a structured line at each checkpoint. Watch for these to confirm stage completion:
```
INFO  [orchestrator] Stage 1 complete — checkpoint RANDOM_SEARCH_COMPLETE set
INFO  [orchestrator] Stage 2 complete — checkpoint MC_PREFILTER_COMPLETE set
INFO  [orchestrator] Stage 3 complete — checkpoint GA_COMPLETE set
INFO  [orchestrator] Stage 4 complete — checkpoint WFO_COMPLETE set
INFO  [orchestrator] Stage 5 complete — checkpoint MC_DEEP_COMPLETE set
INFO  [orchestrator] Stage 6 complete — checkpoint SENSITIVITY_COMPLETE set
INFO  [orchestrator] Stage 7 complete — checkpoint COMPLETE set
```
If the pipeline is interrupted, the last checkpoint logged before the crash is the resume point.
### 3.2 Expected Stage Durations
Based on the Block 3 performance baseline (Windows 10, 6 workers, 3-month data slice):
| Stage | Description | Expected duration |
|---|---|---|
| Stage 0 | Validation & Init | < 5s |
| Stage 1 | Random Search (400 candidates) | Minutes — depends on strategy eval speed |
| Stage 2 | MC Pre-Filter (top 120, 300 iters) | Minutes |
| Stage 3 | GA Evolution (60 pop × 30 gen) | Minutes–tens of minutes |
| Stage 4 | Full WFO (top 30, 5 windows) | Minutes |
| Stage 5 | MC Deep (top 10, 3000 iters) | **0.3–2.5s** — fully vectorised, not a bottleneck |
| Stage 6 | Sensitivity (top 5, ±2 steps) | **333–446s** — dominant cost (~66–89s/candidate) |
| Stage 7 | Report & Output | 4–8s |
> Stage 6 is the structural bottleneck. The per-candidate spawn cost on Windows is the root cause. Pool reuse (OPT-01, planned for Block 7) will reduce this by 40–60%.
### 3.3 Querying Current Checkpoint from SQLite
```python
import sqlite3
con = sqlite3.connect("outputs/backtesting/backtester.db")
row = con.execute(
    "SELECT run_id, checkpoint FROM runs ORDER BY created_at DESC LIMIT 1"
).fetchone()
print(f"run_id={row[0]}  checkpoint={row[1]}")
con.close()
```
---
## 4. Expected Outputs Per Stage
### Stage 1 — Random Search
- Candidates written to `candidates` and `evaluations` tables
- Expected count: `zones_enabled × random_search.samples_per_zone`
  - Default config: 2 zones × 200 samples = **400 candidates**
- Candidates failing `min_significant_trades` guard are rejected before writing
- Log line: `Stage 1 — {N} candidates passed significance guard`
### Stage 2 — MC Pre-Filter
- `mc_results` rows written (mode=`pre_filter`) for top `mc_prefilter.input_count` candidates (default: 120)
- Candidates with `ruin_probability > mc_prefilter_ruin_threshold` are tagged `MC_PREFILTER_FAIL`
- Expect ~50–80% to pass depending on scenario constraints
- Candidates that fail the pre-filter are retained in the store but excluded from GA seeding
### Stage 3 — GA Evolution
- 60 pop × 30 gen = up to 1,800 candidate evaluations (many will be duplicates — de-duplicated by `candidate_id`)
- Each generation uses 2 random WFO windows for fitness (lightweight WFO mode)
- All unique candidates written to store; duplicates updated in-place
- Elite fraction (10%) carried forward unchanged each generation
### Stage 4 — Full WFO
- `wfo_consistency_scores` rows written for top `walk_forward.input_count` candidates (default: 30)
- Each candidate evaluated across all 5 windows
- Consistency score range: [0, 1] — higher is more consistent across time
- Log line: `Stage 4 — {N} WFO scores written`
### Stage 5 — MC Deep
- `mc_results` rows written (mode=`deep`) for top `monte_carlo.deep.input_count` candidates by WFO score (default: 10)
- 3,000 iterations per candidate — full perturbation suite
- Ruin threshold: 0.20 (20% equity loss path = ruined)
- `ruin_probability=None` on error — logged as WARNING, written to store, treated as NO_GO in verdict
### Stage 6 — Sensitivity
- `sensitivity_results` and `sensitivity_profiles` rows written for top `sensitivity.input_count` candidates (default: 5)
- Each parameter perturbed ±1 and ±2 steps; fitness delta computed per perturbation
- `spike_detected=True` if any `|fitness_delta| > sensitivity_spike_threshold` (default: 0.15)
- `profile_complete=False` if >50% of perturbation evaluations failed — sets `sensitivity_profile_incomplete` modifier
### Stage 7 — Report & Output
- `verdicts` rows written for shortlisted candidates (default: top 5 by WFO score)
- Trading-ready YAMLs written to `outputs/backtesting/trading_yamls/` for AUTO_GO and BORDERLINE verdicts
- HTML report: `outputs/backtesting/report.html`
- JSON export: `outputs/backtesting/json/`
- Parquet export: `outputs/backtesting/parquet/`
---
## 5. Reading the Verdict Output
### Verdict Definitions
**`AUTO_GO`**
Both pillars pass their go thresholds AND no modifier flags are active. This candidate has demonstrated:
- WFO composite score ≥ `go_wfo_floor` (default for `capital_accumulation`: 0.65)
- MC deep ruin probability ≤ `go_mc_ruin_ceiling` (default: 0.05)
- No sensitivity spikes, no window collapse, complete sensitivity profile, OOS gate not triggered
Ready for paper trading. Deployment status is always `PAPER_TRADE_REQUIRED`.
**`BORDERLINE`**
One or both pillars are in the borderline zone, OR at least one modifier flag is active. This candidate requires additional scrutiny before paper trading. Common causes:
- WFO score between `borderline_wfo_floor` (0.40) and `go_wfo_floor` (0.65) — acceptable but not strong
- Ruin probability between `go_mc_ruin_ceiling` (0.05) and `borderline_mc_ruin_ceiling` (0.15)
- A parameter sensitivity spike was detected (`sensitivity_spike=True`)
- The WFO OOS gate was triggered (when `enforce_oos_gate: true`)
- A WFO window collapse was flagged
- >50% of sensitivity evaluations failed (`sensitivity_profile_incomplete=True`)
The `evidence_summary` field of the VerdictResult documents which conditions triggered BORDERLINE. Always review this before paper trading a BORDERLINE candidate.
**`NO_GO`**
One or both pillars failed their go AND borderline thresholds. This candidate must not be traded:
- WFO composite score < `borderline_wfo_floor` (0.40) — inconsistent across time
- MC ruin probability > `borderline_mc_ruin_ceiling` (0.15) — unacceptable ruin risk
- MC result was `None` (strategy runner error during MC) — treat as maximum risk
Modifier flags cannot override NO_GO. A NO_GO is final.
### Threshold Reference (`capital_accumulation` defaults)
```
go_wfo_floor:              0.65   (WFO composite ≥ 0.65 → WFO pillar passes)
borderline_wfo_floor:      0.40   (WFO composite ≥ 0.40 → borderline zone)
go_mc_ruin_ceiling:        0.05   (ruin prob ≤ 0.05 → MC pillar passes)
borderline_mc_ruin_ceiling: 0.15  (ruin prob ≤ 0.15 → borderline zone)
sensitivity_spike_threshold: 0.15 (|fitness_delta| > 0.15 → spike flag)
```
> **Calibration note**: These are D-07 starting values. Recalibrate after the first real pipeline run once you have a distribution of WFO scores and ruin probabilities from real candidates.
---
## 6. Promotion Path
```
Pipeline verdict → PAPER_TRADE_REQUIRED
                        ↓
              [Operator paper trading]
                        ↓
              [Manual operator review]
                        ↓
                  LIVE_APPROVED          ← Operator sets this manually. NEVER set by code.
```
**`PAPER_TRADE_REQUIRED`** is the initial status for all AUTO_GO and BORDERLINE verdicts. The pipeline never sets `LIVE_APPROVED` — this is a manual operator action only, taken after a satisfactory paper trading period.
**Minimum paper trading period**: Operator decision. No minimum is enforced by the system. Recommended: at least one full WFO window equivalent (approximately 3 weeks for the current config) under live market conditions.
**What to monitor during paper trading**:
- Actual win rate vs `min_win_rate` constraint
- Actual drawdown vs `max_drawdown` constraint
- Trades per week vs `min_trades_per_week` constraint
- Any parameter or market regime shifts since the backtest period
---
## 7. Resume After Interruption
All 8 checkpoints are verified safe interruption points (Block 4 robustness tests). If a run is interrupted for any reason, simply re-run with the same config:
```bash
python -m src.backtesting.orchestrator --config configs/backtesting/backtest_template.yaml
```
The orchestrator reads the current checkpoint from `backtester.db` and skips all already-completed stages.
**Checkpoint sequence**:
```
INITIALIZED → RANDOM_SEARCH_COMPLETE → MC_PREFILTER_COMPLETE → GA_COMPLETE
            → WFO_COMPLETE → MC_DEEP_COMPLETE → SENSITIVITY_COMPLETE → COMPLETE
```
**Important**: Resume only works when the config file is byte-for-byte identical to the original run (same config hash). If you modify the config, a new `run_id` is generated and the pipeline starts from scratch.
**Confirming resume point**:
```python
# Query current checkpoint before resuming
import sqlite3
con = sqlite3.connect("outputs/backtesting/backtester.db")
row = con.execute(
    "SELECT run_id, checkpoint, config_hash FROM runs ORDER BY created_at DESC LIMIT 1"
).fetchone()
print(f"run_id={row[0]}  checkpoint={row[1]}  config_hash={row[2][:16]}")
con.close()
```
---
## 8. Performance Tuning Reference
| ID | Description | Expected saving | How to apply |
|---|---|---|---|
| OPT-01 | Pool reuse in `evaluate_sensitivity()` — reuse `ProcessPoolExecutor` across candidates | 40–60% Stage 6 | Code change — Block 7 |
| OPT-02 | Batch all perturbations per candidate into one worker task | Additional 15–25% Stage 6 | Code change — Block 7 |
| OPT-03 | Reduce `sensitivity.input_count` from 5 to 3 | ~130–180s Stage 6 | Change `sensitivity.input_count: 3` in YAML |
| OPT-04 | Stage 5 MC Deep — no action needed | Negligible (< 3s already) | N/A |
| OPT-05 | Clean up `max_workers` param in `evaluate_sensitivity()` after OPT-01 | Code quality only | Code change — Block 7 |
**Current performance baseline** (Windows 10, 6 workers, locked 2026-03-03):
```
Run 2 (canonical): Total=337.2s  Stage5=0.3s  Stage6=332.6s  Stage7=4.4s
Daily budget: 14,400s → 2.3% consumed
```
Stage 6 dominates total run time. OPT-03 is the only YAML-level optimisation available before Block 7; all others require code changes.
---
## Appendix A — Common Error Patterns
| Symptom | Likely cause | Resolution |
|---|---|---|
| `Stage 0 validation error: fewer than 3 WFO windows` | `walk_forward.windows` has < 3 entries | Add windows to YAML |
| `Stage 0 validation error: overlapping windows` | Two window date ranges overlap | Adjust dates — windows must be non-overlapping |
| `WARNING: MCResult ruin_probability=None for candidate {id}` | Strategy runner error during MC deep | Expected if strategy errors — candidate will be NO_GO |
| `WARNING: SensitivityProfile profile_complete=False` | >50% perturbation evals failed | May indicate strategy instability near parameter boundaries |
| `ERROR: Can't pickle <class 'unittest.mock.MagicMock'>` | Test is patching inside ProcessPoolExecutor worker boundary | Patch at orchestrator level — see ARCHITECTURE.md §9 |
| Config hash mismatch on resume | YAML was modified between runs | Either restore original YAML or start a fresh run |
| `KeyError` in fitness or scenario evaluation | Config fixture uses flat dict instead of nested | Use nested structure: `fitness_weights`, `constraints`, etc. |
---
## Appendix B — Output Directory Structure
```
outputs/backtesting/
├── backtester.db              ← SQLite WAL — all stage results, 9 tables
├── report.html                ← Self-contained HTML report with inline charts
├── json/
│   └── {run_id}_report.json  ← Full verdict + evidence for all shortlisted candidates
├── parquet/
│   └── {run_id}_results.parquet ← Columnar export of candidate metrics
└── trading_yamls/
    └── {candidate_id}_{verdict}.yaml  ← Trading-ready strategy YAML (AUTO_GO + BORDERLINE only)
```
Trading YAMLs embed `backtester_metadata` block containing: `run_id`, `config_hash`, `verdict`, `wfo_consistency_score`, `mc_ruin_probability`, `sensitivity_spike_detected`, and all 5 seeds. This block is for audit purposes — the strategy runner ignores it.
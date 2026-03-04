# OPERATOR_RUNBOOK.md — Backtesting & Optimization Framework
**Version**: 1.1.0
**Date**: 2026-03-04
**Audience**: Operators launching, monitoring, and acting on pipeline runs
> **Change log**:
> - v1.0.0 (2026-03-03): Initial release (Block 8A)
> - v1.1.0 (2026-03-04): Added §9 Known Limitations, updated Appendix A with Block 8B error patterns
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
One or both pillars are in the borderline zone, OR at least one modifier flag is active. Common causes:
- WFO score between `borderline_wfo_floor` (0.40) and `go_wfo_floor` (0.65)
- Ruin probability between `go_mc_ruin_ceiling` (0.05) and `borderline_mc_ruin_ceiling` (0.15)
- A parameter sensitivity spike was detected
- The WFO OOS gate was triggered (when `enforce_oos_gate: true`) — **NOTE: currently non-functional, see §9.1**
- A WFO window collapse was flagged
- >50% of sensitivity evaluations failed
**`NO_GO`**
One or both pillars failed their go AND borderline thresholds. Must not be traded:
- WFO composite score < `borderline_wfo_floor` (0.40)
- MC ruin probability > `borderline_mc_ruin_ceiling` (0.15)
- MC result was `None` — treat as maximum risk
### Threshold Reference (`capital_accumulation` defaults)
```
go_wfo_floor:              0.65
borderline_wfo_floor:      0.40
go_mc_ruin_ceiling:        0.05
borderline_mc_ruin_ceiling: 0.15
sensitivity_spike_threshold: 0.15
```
> **Calibration note**: These are D-07 starting values. Recalibrate after the first real pipeline run.
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
**`PAPER_TRADE_REQUIRED`** is the initial status for all AUTO_GO and BORDERLINE verdicts. The pipeline never sets `LIVE_APPROVED`.
**Minimum paper trading period**: Operator decision. Recommended: at least one full WFO window equivalent (~3 weeks) under live market conditions.
---
## 7. Resume After Interruption
```bash
python -m src.backtesting.orchestrator --config configs/backtesting/backtest_template.yaml
```
**Checkpoint sequence**:
```
INITIALIZED → RANDOM_SEARCH_COMPLETE → MC_PREFILTER_COMPLETE → GA_COMPLETE
            → WFO_COMPLETE → MC_DEEP_COMPLETE → SENSITIVITY_COMPLETE → COMPLETE
```
Resume only works when the config file is byte-for-byte identical to the original run (same config hash).
---
## 8. Performance Tuning Reference
| ID | Description | Expected saving | How to apply |
|---|---|---|---|
| OPT-01 | Pool reuse in `evaluate_sensitivity()` | 40–60% Stage 6 | Code change — Block 7 |
| OPT-02 | Batch all perturbations per candidate | Additional 15–25% Stage 6 | Code change — Block 7 |
| OPT-03 | Reduce `sensitivity.input_count` from 5 to 3 | ~130–180s Stage 6 | Change YAML |
| OPT-04 | Stage 5 MC Deep — no action needed | Negligible | N/A |
**Current performance baseline** (Windows 10, 6 workers, 2026-03-03):
```
Run 2 (canonical): Total=337.2s  Stage5=0.3s  Stage6=332.6s  Stage7=4.4s
Daily budget: 14,400s → 2.3% consumed
```
## 9. Known Limitations and Non-Functional Features
### 9.1 OOS Gate — Non-Functional (B8B-005)
`enforce_oos_gate: true` in `backtest_template.yaml` **currently has no effect**.
The IS/OOS delta computation (`oos_delta` on each `WFOWindowResult`) is never populated
in the current pipeline. `wfo_evaluator.evaluate_window()` always returns `oos_delta=None`,
and `wfo_engine.run_wfo()` does not post-process results to set it.
**Consequences**:
- `oos_gate_triggered` in every `VerdictResult` is `False` — not "gate passed" but "gate not evaluated"
- `median_oos_delta` in every `WFOConsistencyScore` is `None`
- The `oos_gate_triggered` modifier flag in BORDERLINE verdicts is permanently silent
**Workaround**: None. The OOS gate requires a structural design decision on IS/OOS window
splitting (run the strategy on in-sample vs out-of-sample portions of each window separately).
Scheduled for implementation in Block 9.
**Do not rely on `enforce_oos_gate: true`** to filter candidates for OOS degradation.
Manual review of per-window fitness scores in the HTML report is the current alternative.
---
### 9.2 WFO Sigmoid Scale — Calibration Required Before First Real Run (B8B-012)
`consistency_scorer._sigmoid_normalise` uses `scale=0.10`, which was calibrated for
unit returns (fractions like 0.05, -0.12). Real strategy `net_pnl` values are in currency
points (e.g. −1,200 pts to +8,000 pts depending on instrument and window length).
With `scale=0.10`, any per-window P&L greater than ~5 points maps to `median_return_norm ≈ 1.0`.
The WFO `median_return` sub-metric is effectively binary (positive window = 1.0,
negative window = 0.0) with no differentiation by magnitude.
**Action required before first production run**:
1. Run the pipeline on real data once (even with `e2e_test` scenario)
2. Check the distribution of per-window `net_pnl` in `wfo_window_results` table:
   ```sql
   SELECT window_id, AVG(net_pnl), STDEV(net_pnl), MIN(net_pnl), MAX(net_pnl)
   FROM wfo_window_results
   WHERE run_id = '<your_run_id>'
   GROUP BY window_id;
   ```
3. Set `wfo_sigmoid_scale ≈ 10% of median expected per-window P&L` in `ScenarioProfile`
   (field to be added in Block 9 — currently requires code change to `consistency_scorer.py`)
4. Similarly calibrate `wfo_variance_max_expected` (currently 0.10) to the actual
   variance range of per-window net_pnl
Until calibrated, WFO composite scores rank candidates primarily by fraction of positive
windows rather than by P&L magnitude. Relative ranking is still meaningful within a run;
absolute score thresholds in `verdict_go_wfo_floor` / `verdict_borderline_wfo_floor`
may need adjustment.
---
### 9.3 net_pnl Field Name — Verify Before First Real WFO Run (B8B-018)
`wfo_evaluator.py` reads the per-window P&L as `_safe_float(m, "net_pnl")` from the
`MetricsReport` object. `fitness.py` reads the same value as `total_pnl_points`.
If `MetricsReport` exposes the field only as `total_pnl_points` (not `net_pnl`), then:
- All `WFOWindowResult.net_pnl` values will be `None`
- WFO consistency scores will have `median_return_norm = 0.5` and `fraction_positive = 0.0`
- WFO composite scores will be systematically lower than expected
**Verify** by running `test_block8b_engines.py::test_net_pnl_field_name_matches_metrics_report`
after uploading `contracts.py` in Block 8C. If the test fails, apply the one-line fix
in `wfo_evaluator.py` line ~82.
---
### 9.4 Previously Documented Limitations (from Block 8A)
| Limitation | Status | Block |
|---|---|---|
| `median_oos_delta` never persisted | Fixed (B8-001) | 8A |
| `CandidateRecord.wfo_median_oos_delta` never populated in queries | Fixed (B8-002) | 8A |
| Stages 1–4 stubs — pipeline does no real work before Stage 5 | Open | 8A |
| `parameter_region_width` always `None` | Open | 8A |
| `_resume_or_start` opens raw sqlite3 (bypasses store contract) | Open, B9 | 8A |
| Timing covers Stages 5–7 only | Open, extends when 1–4 implemented | 8A |
| OOS gate non-functional | Open (B8B-005) | 8B |
| WFO sigmoid scale uncalibrated for real data | Open (B8B-012) | 8B |
| `net_pnl` vs `total_pnl_points` field name — needs verification | Verify in 8C | 8B |
| Expectancy normalisation scale hardcoded at 3.0 | Open, B9 | 8B |
| `mc_prefilter_ruin_threshold` dual-source (config dict + ScenarioProfile) | Open, B9 | 8B |
---
## Appendix A Updates
Add to the Common Error Patterns table:
| Symptom | Likely cause | Resolution |
|---|---|---|
| All `WFOConsistencyScore.median_oos_delta` are `None` | B8B-005: OOS gate not implemented | Expected — see §9.1 |
| All `VerdictResult.oos_gate_triggered` are `False` | B8B-005: OOS delta never set | Expected — see §9.1 |
| WFO scores consistently lower than expected; `fraction_positive_windows` always 0.0 | B8B-018: `net_pnl` field name mismatch | Run B8B-018 test; fix `wfo_evaluator.py` line ~82 |
| WFO composite scores cluster near 0.5 regardless of strategy quality | B8B-012: sigmoid scale=0.10 binary for real data | Calibrate `wfo_sigmoid_scale` — see §9.2 |
| NaN fitness_score crashes `FitnessResult.__post_init__` | B8B-001 (pre-fix): NaN metric bypassed constraint guard | Fixed in Block 8B — verify NaN guard is applied |
---
## Appendix B — Output Directory Structure
```
outputs/backtesting/
├── backtester.db              ← SQLite WAL — all stage results, 9 tables
├── report.html                ← Self-contained HTML report with inline charts
├── json/
│   └── {run_id}_report.json
├── parquet/
│   └── {run_id}_results.parquet
└── trading_yamls/
    └── {candidate_id}_{verdict}.yaml  ← Trading-ready YAML (AUTO_GO + BORDERLINE only)
```
Trading YAMLs embed `backtester_metadata` block containing: `run_id`, `config_hash`, `verdict`, `wfo_consistency_score`, `mc_ruin_probability`, `sensitivity_spike_detected`, and all 5 seeds.
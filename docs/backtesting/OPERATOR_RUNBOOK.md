# OPERATOR_RUNBOOK.md — Backtesting & Optimization Framework
**Version**: 3.0.0
**Date**: 2026-03-05
**Audience**: Operators launching, monitoring, and acting on pipeline runs
> **Changelog**:
> - v1.0.0 (2026-03-03): Initial release (Block 8A)
> - v1.1.0 (2026-03-04): Added §9 Known Limitations, Appendix A Block 8B error patterns
> - v2.0.0 (2026-03-04): Block 9A/9B updates throughout
> - v3.0.0 (2026-03-05): Block 9C updates — supporting modules audited, §11 expanded,
>   Appendix A extended, B9C findings documented
---

## 1. Pre-Run Checklist

Complete every item before launching a run. Do not skip steps.

### 1.1 Config Hash Verification
The pipeline runs are keyed by a SHA-256 hash of the config file. If you modify
`backtest_template.yaml` between runs, a new `run_id` is generated and the previous
run's checkpoint is not reused.

```bash
python -c "
import hashlib, pathlib
data = pathlib.Path('configs/backtesting/backtest_template.yaml').read_bytes()
print(hashlib.sha256(data).hexdigest()[:16])
"
```

Record the hash in your run log before launching. If a run fails and you need to resume,
verify the hash matches before restarting.

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

> **WARNING**: `e2e_test` passes nearly every candidate through all stages regardless of
> quality. It exists only to verify pipeline plumbing. Do not interpret its verdicts as
> trading signals.

### 1.3 WFO Window Date Review
WFO windows are defined in `backtest_template.yaml` under `walk_forward.windows`. The
current defaults are calibrated for the 3-month OHLCV slice (2025-09-15 → 2025-12-17):
```
W01: 2025-09-15 → 2025-10-03   (~3 weeks)
W02: 2025-10-06 → 2025-10-24
W03: 2025-10-27 → 2025-11-14
W04: 2025-11-17 → 2025-12-05
W05: 2025-12-08 → 2025-12-17
```

- Confirm the windows align with your available OHLCV data
- Windows must not overlap (validation enforced at Stage 0)
- Minimum 3 windows required (Stage 0 enforced)
- If using a longer data slice, extend the windows proportionally

### 1.4 Seed Documentation
Five seeds are used across pipeline stages. Record all five before launching:
```yaml
random_search.seed:     42    # Stage 1 LHS sampling
mc_prefilter.seed:      43    # Stage 2 MC pre-filter
genetic.seed:           44    # Stage 3 GA evolution
monte_carlo.deep.seed:  45    # Stage 5 MC deep
sensitivity.seed:       46    # Stage 6 (not currently used — deterministic ±step)
```

All seeds are stored in the `runs` table of `backtester.db` and embedded in all output
artefacts for reproducibility.

### 1.5 Worker Count Check
```yaml
run.max_workers: 6
```
Stage 6 (Sensitivity) is the dominant runtime cost. On Windows, each worker spawns a
fresh Python interpreter. Match `max_workers` to physical cores − 1 (leave one free for
the OS). Exceeding physical core count provides no benefit and increases memory pressure.

### 1.6 spike_threshold Consistency Check
Until B9A-003 is resolved, two config locations control the spike threshold and **must
be set to the same value**:

```yaml
# Location 1: in backtest_template.yaml top-level sensitivity block
sensitivity:
  spike_threshold: 0.15         # ← Stage 6 uses this for detection

# Location 2: in the active scenario block
scenarios:
  capital_accumulation:
    verdict_sensitivity_spike_threshold: 0.15   # ← verdict.py uses this for flagging
```

If these differ, a spike can be detected but not flagged, or flagged without being
detected. Default for both: `0.15`. This will be fixed in Block 9D (B9A-003).

### 1.7 Dual Parameter Key Map Check
Two files define the strategy parameter → YAML path mapping (B8-006):
- `strategy_runner.py` → `_PARAM_KEY_MAP`
- `yaml_generator.py` → `_STRATEGY_PARAM_KEY_MAP`

**If you add a new strategy parameter to the pipeline, update both files.** A parameter
present in one but not the other will silently map to the wrong YAML section (strategy_runner)
or fall back to the `parameters` section (yaml_generator) without warning.

---

## 2. Launching a Run

```bash
python -m src.backtesting.orchestrator --config configs/backtesting/backtest_template.yaml
```

The orchestrator writes all artefacts to `outputs/backtesting/` and temporary candidate
YAMLs to `temp/backtesting/`.

---

## 3. Pipeline Stage Status

| Stage | Description | Status | Notes |
|---|---|---|---|
| 0 | Validation & Init | ✅ Implemented | Config, scenario, windows, parameter names, zone count |
| 1 | Random Search | 🟡 Stub | Logs "not yet implemented". Checkpoint **is advanced**. ⚠ B9C-007 must be fixed before implementing |
| 2 | MC Pre-Filter | 🟡 Stub | Logs "not yet implemented". Checkpoint **is advanced**. |
| 3 | GA Evolution | 🟡 Stub | Logs "not yet implemented". Checkpoint **is advanced**. ⚠ Must inject `config['_base_yaml_path']` (B9B-003) |
| 4 | Full WFO | 🟡 Stub | Logs "not yet implemented". Checkpoint **is advanced**. |
| 5 | MC Deep | ✅ Implemented | Top-N by WFO score. 3000 iters. All perturbation types. |
| 6 | Sensitivity | ✅ Implemented | ±1/±2 steps. OPT-01: shared pool across candidates. |
| 7 | Report & Output | ✅ Implemented | Verdict, trading YAML, HTML, JSON, Parquet. |

> **Important**: All stub stages (1–4) now correctly advance their checkpoint. On pipeline
> resume, no stub stage re-runs (B9A-002 fixed).

To inspect current checkpoint state:
```sql
SELECT run_id, checkpoint, started_at
FROM runs
WHERE checkpoint != 'COMPLETE'
ORDER BY started_at DESC;
```

---

## 4. Monitoring Progress

### 4.1 Checkpoint Log Lines
```
INFO  [orchestrator] Stage 0: All validations passed — {N} WFO windows, {M} enabled zones
INFO  [orchestrator] Stage 1 (Random Search) stub — checkpoint RANDOM_SEARCH_COMPLETE set
INFO  [orchestrator] Stage 5: MC Deep complete — {N}/{M} candidates processed
INFO  [orchestrator] Stage 6: Sensitivity complete — {N}/{M} candidates processed
INFO  [orchestrator] Stage 7: {N} verdicts written
INFO  [orchestrator] Pipeline complete — run_id={id}
```

### 4.2 Expected Stage Durations

| Stage | Expected duration | Notes |
|---|---|---|
| Stage 0 | < 5s | Validation only |
| Stages 1–4 | ~0s | Stubs — log line only |
| Stage 5 | **0.3–2.5s** | Fully vectorised. Never the bottleneck. |
| Stage 6 | **333–446s** | Structural bottleneck (~66–89s/candidate, 5 candidates) |
| Stage 7 | 4–8s | Acceptable |

> **OPT-01 status**: Pool reuse was applied in Block 7C. A re-analysis of Stage 6 timing
> to confirm the ≤200s target is planned.

### 4.3 TIMING SUMMARY Log Line
```
TIMING SUMMARY  stage5=0.3s  stage6=332.6s  stage7=4.4s  total=337.3s  budget=14400s  PASS
```

> **Note**: `total` covers **Stages 5–7 only** (B8-008). When Stages 1–4 are implemented,
> timing tracking will be extended to all stages.

---

## 5. Configuration Reference

### sensitivity.spike_threshold
```yaml
sensitivity:
  spike_threshold: 0.15   # Must match scenario.verdict_sensitivity_spike_threshold — see §1.6
```
Range: `(0.0, 1.0)` exclusive. Validated at Stage 0.

### walk_forward.enforce_oos_gate
```yaml
walk_forward:
  enforce_oos_gate: false   # Currently has NO EFFECT — see §11.1
```

### run.max_workers
```yaml
run:
  max_workers: 6   # Match to physical CPU cores − 1 on Windows
```

### monte_carlo.deep.iterations
```yaml
monte_carlo:
  deep:
    iterations: 3000   # Production default. Lower for dev/test runs.
```

---

## 6. Expected Outputs Per Stage

### Stage 5 — MC Deep
- `mc_results` rows written (mode=`deep`) for top `monte_carlo.deep.input_count` candidates
  (default: 10)
- 3,000 iterations per candidate; all perturbation types
- All metrics in **pips/points** — no currency conversion
- `ruin_probability=None` on error — logged as WARNING, written to store, treated as NO_GO

### Stage 6 — Sensitivity
- `sensitivity_results` and `sensitivity_profiles` rows written for top
  `sensitivity.input_count` candidates (default: 5)
- Each parameter perturbed ±1 and ±2 steps; fitness delta computed per perturbation
- `spike_detected=True` if any `|fitness_delta| > sensitivity.spike_threshold` (default: 0.15)
- `profile_complete=False` if >50% of perturbation evaluations failed
- All fitness deltas are in pips/points

### Stage 7 — Report & Output
- `verdicts` rows written for shortlisted candidates (top 5 by WFO score)
- Trading-ready YAMLs written for AUTO_GO and BORDERLINE verdicts
- HTML report: `outputs/backtesting/report.html`
- JSON export: `outputs/backtesting/json/`
- Parquet export: `outputs/backtesting/parquet/`

**Trading YAML `backtester_metadata` contents**: `run_id`, `candidate_id`, `zone_name`,
`config_hash`, `scenario_name`, `backtester_version`, `generated_at`, `deployment_status`
(always `PAPER_TRADE_REQUIRED`), `verdict`, `wfo_consistency_score`,
`mc_deep_ruin_probability`, `sensitivity_spike`, and all 5 run seeds.

---

## 7. Reading the Verdict Output

### Verdict Definitions

**`AUTO_GO`**
Both pillars pass go thresholds AND no modifier flags active:
- WFO composite score ≥ `go_wfo_floor` (default `capital_accumulation`: 0.65)
- MC deep ruin probability ≤ `go_mc_ruin_ceiling` (default: 0.05)
- No spikes, no window collapse, complete sensitivity profile, OOS gate not triggered

Deployment status is always `PAPER_TRADE_REQUIRED`.

**`BORDERLINE`**
One or both pillars in the borderline zone, OR at least one modifier flag active:
- WFO score between `borderline_wfo_floor` (0.40) and `go_wfo_floor` (0.65)
- Ruin probability between `go_mc_ruin_ceiling` (0.05) and `borderline_mc_ruin_ceiling` (0.15)
- A parameter sensitivity spike was detected
- A WFO window collapse was flagged
- >50% of sensitivity evaluations failed
- OOS gate was triggered — **NOTE: currently non-functional, see §11.1**

**`NO_GO`**
One or both pillars failed. Must not be traded:
- WFO composite score < `borderline_wfo_floor` (0.40)
- MC ruin probability > `borderline_mc_ruin_ceiling` (0.15)
- MC result was `None` — treated as maximum risk

### Threshold Reference (`capital_accumulation` defaults)
```
go_wfo_floor:                0.65
borderline_wfo_floor:        0.40
go_mc_ruin_ceiling:          0.05
borderline_mc_ruin_ceiling:  0.15
sensitivity_spike_threshold: 0.15
```

> **Calibration note**: These are D-07 starting values. Recalibrate after the first real
> pipeline run. See §11.2 for WFO sigmoid scale note.

---

## 8. Promotion Path

```
Pipeline verdict → PAPER_TRADE_REQUIRED
                        ↓
              [Operator paper trading]
                        ↓
              [Manual operator review]
                        ↓
                  LIVE_APPROVED          ← Operator sets this manually. NEVER set by code.
```

Minimum recommended paper trading period: one full WFO window equivalent (~3 weeks) under
live market conditions.

---

## 9. Resume After Interruption

```bash
python -m src.backtesting.orchestrator --config configs/backtesting/backtest_template.yaml
```

Resume only works when the config file is byte-for-byte identical to the original run
(same config hash). Checkpoint sequence:

```
NOT_STARTED → RUN_INITIALISED → RANDOM_SEARCH_COMPLETE → MC_PREFILTER_COMPLETE
            → GA_COMPLETE → WFO_COMPLETE → MONTE_CARLO_COMPLETE
            → SENSITIVITY_COMPLETE → COMPLETE
```

---

## 10. Performance Tuning Reference

| ID | Description | Expected saving | Status |
|---|---|---|---|
| OPT-01 | Pool reuse in `evaluate_sensitivity()` across all candidates | 40–60% Stage 6 | ✅ Applied (Block 7C). **Target ≤200s not confirmed — re-analysis planned** |
| OPT-02 | Batch all perturbations per candidate into one worker task | Additional 15–25% Stage 6 | 🟡 Re-evaluate after OPT-01 re-analysis |
| OPT-03 | Reduce `sensitivity.input_count` from 5 to 3 | ~130–180s Stage 6 | 🟡 Change YAML |
| OPT-04 | Stage 5 MC Deep — no action until `input_count > 50` | Negligible | ✅ No action |
| OPT-05 | Clean up `max_workers` param in `evaluate_sensitivity()` after OPT-01 | Code quality | 🟡 Pending |

**Current performance baseline** (Windows 10, 6 workers, 2026-03-03):
```
Run 2 (canonical): Total=337.2s  Stage5=0.3s  Stage6=332.6s  Stage7=4.4s
Daily budget: 14,400s → 2.3% consumed
```

---

## 11. Known Limitations and Non-Functional Features

### 11.1 OOS Gate — Non-Functional (B8B-005)

`enforce_oos_gate: true` in `backtest_template.yaml` **currently has no effect**.

`WFOWindowResult.oos_delta` is always `None`. The IS/OOS delta computation is never
performed. As a result:
- `oos_gate_triggered` in every `VerdictResult` is `False` — not "gate passed", but
  "gate not evaluated"
- `median_oos_delta` in every `WFOConsistencyScore` is `None`
- The `oos_gate_triggered` modifier flag in BORDERLINE verdicts is permanently silent

**Workaround**: Manual review of per-window fitness scores in the HTML report.
Scheduled for Block 9E.

**Do not rely on `enforce_oos_gate: true`** to filter candidates for OOS degradation.

Note: `wfo_engine.run_wfo()` correctly passes `oos_gate_enabled` to `compute_consistency()`.
The non-functionality is in `wfo_evaluator.py` and `consistency_scorer.py` — not the engine.

---

### 11.2 WFO Sigmoid Scale — Verify Before First Production Run (B8B-012)

All trading metrics in this pipeline are in **pips/points** — not currency. This is
correct for WBWSStrategy. `WFOWindowResult.net_pnl` stores `total_pnl_points`.

`consistency_scorer._sigmoid_normalise` uses `scale=0.10`. This value should be verified
against your actual per-window pip/point distribution before the first production run.

**Action before first production run**:
1. Run the pipeline once (even with `e2e_test` scenario)
2. Check the per-window `net_pnl` distribution:
   ```sql
   SELECT window_id, AVG(net_pnl), MIN(net_pnl), MAX(net_pnl)
   FROM wfo_window_results
   WHERE run_id = '<your_run_id>'
   GROUP BY window_id;
   ```
3. Set `wfo_sigmoid_scale ≈ 10% of median expected per-window pip value` in `ScenarioProfile`

Until calibrated, WFO composite scores rank candidates primarily by fraction of positive
windows rather than pip magnitude. Relative ranking within a run is still meaningful;
absolute score thresholds may need adjustment.

---

### 11.3 LHS Sampler Sort Bug — Fix Before Stage 1 Implementation (B9C-007)

`sampler._lhs_sample()` sorts parameter value universes lexicographically via
`(str(type), str(val))`. For numeric parameters ≥ 10, this produces wrong sort order
(e.g. `[9, 10, 11]` → sorted as `[10, 11, 9]`), breaking the LHS space-filling guarantee.

**Required fix in `sampler._lhs_sample()` before Stage 1 is implemented**:
```python
# Replace:
param_value_universe[name] = sorted(seen, key=lambda x: (str(type(x)), str(x)))

# With:
try:
    param_value_universe[name] = sorted(seen, key=lambda x: float(x))
except (TypeError, ValueError):
    param_value_universe[name] = sorted(seen, key=lambda x: str(x))
```

---

### 11.4 Previously Documented Limitations

| Limitation | Status |
|---|---|
| `median_oos_delta` never persisted | **Fixed (B8-001)** |
| `CandidateRecord.wfo_median_oos_delta` never populated | **Fixed (B8-002)** |
| Stages 1–4 stubs — no real work before Stage 5 | Open — stubs advance checkpoints correctly |
| `parameter_region_width` always `None` | Open — WF-07 deferred |
| `_resume_or_start` opens raw sqlite3 | Open — B8-009 deferred |
| Timing covers Stages 5–7 only | Open — B8-008, extends when 1–4 implemented |
| OOS gate non-functional | Open — B8B-005, Block 9E |
| WFO sigmoid scale — verify against real pips/points data | Open — B8B-012 |
| `net_pnl` vs `total_pnl_points` field name mismatch | **Fixed (B8B-018)** |
| `report_emphasis` scalar string accepted silently | **Fixed (B8C-001)** |
| Stage 1 stub re-ran on every resume | **Fixed (B9A-002)** |
| `spike_threshold` dual-source | Open — B9A-003, keep in sync manually (see §1.6) |
| `ranker.rank_by_wfo()` returns typed records | **Confirmed correct (Block 9C)** — bug is orchestrator-only (B9A-001) |
| `scenario.load_scenario()` spike threshold alignment | **Confirmed correct (Block 9C)** — alignment is orchestrator responsibility (B9A-003) |
| `wfo_engine` OOS gate flags | **Confirmed correct (Block 9C)** — bug is in evaluator/scorer (B8B-005) |
| LHS sampler numeric sort order | Open — B9C-007, **fix before Stage 1** (see §11.3) |
| `sample_random()` docstring says "with replacement" | Open — B9C-006, docstring only, implementation correct |
| Dual `_PARAM_KEY_MAP` files | Open — B8-006, keep in sync manually (see §1.7) |

---

## Appendix A — Common Error Patterns

| Symptom | Likely cause | Resolution |
|---|---|---|
| `ValueError: report_emphasis must be a non-empty list or tuple` | `report_emphasis="balanced"` (scalar string) in scenario config | Use `report_emphasis: [wfo_consistency_score, mc_deep_ruin_probability]` in YAML |
| All `WFOConsistencyScore.median_oos_delta` are `None` | B8B-005: OOS gate not implemented | Expected — see §11.1 |
| All `VerdictResult.oos_gate_triggered` are `False` | B8B-005: oos_delta never set | Expected — see §11.1 |
| WFO composite scores seem insensitive to pip magnitude | B8B-012: sigmoid scale may need calibration | See §11.2 — run pipeline once to measure distribution |
| NaN fitness_score crash | Pre-fix B8B-001. NaN guard now applied in `fitness.py`. | Ensure patched `fitness.py` is deployed |
| Stage 1 re-runs on every resume | Pre-fix B9A-002. Stage 1 now advances checkpoint. | Ensure patched `orchestrator.py` is deployed |
| `KeyError: '_base_yaml_path'` when Stage 3 is implemented | B9B-003: key must be injected by orchestrator | Add `config['_base_yaml_path'] = str(base_yaml_path)` in `_run_stage_3_ga()` |
| `ValueError: Fitness weights must sum to 1.0` | ScenarioProfile weight fields do not sum to 1.0 | Check all 6 weight fields in the active scenario block |
| `ValueError: spike_threshold must be in (0, 1)` at Stage 0 | `sensitivity.spike_threshold` is 0 or ≥ 1 | Valid range is exclusive: (0.0, 1.0) |
| spike detected but not flagged in verdict (or vice versa) | B9A-003: `spike_threshold` differs between config locations | Ensure both locations match — see §1.6 |
| LHS samples clumped at one end of parameter range | B9C-007: sort key bug in `_lhs_sample()` | Apply fix in §11.3 before Stage 1 |
| New parameter appears in strategy_runner but not in trading YAML | B8-006: `_STRATEGY_PARAM_KEY_MAP` in yaml_generator not updated | Update both files together — see §1.7 |

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
    └── {run_id[:8]}_{candidate_id[:12]}_strategy.yaml  ← AUTO_GO + BORDERLINE only
```

Trading YAMLs embed `backtester_metadata` block with full provenance and all 5 seeds.
Deployment status is always `PAPER_TRADE_REQUIRED` — never `LIVE_APPROVED` from code.

---

## v3.0.0 Change Summary (Block 9C)

- **§1.7 added**: Dual parameter key map check — operators must update both
  `strategy_runner._PARAM_KEY_MAP` and `yaml_generator._STRATEGY_PARAM_KEY_MAP` together
- **§3**: Stage table updated — Stage 1 annotated with B9C-007 prerequisite; Stage 3
  annotated with B9B-003 injection requirement
- **§6**: Trading YAML metadata contents documented explicitly
- **§11.1**: Note added — `wfo_engine` correctly passes flags; bug is in evaluator/scorer
- **§11.3 added**: LHS sampler sort bug (B9C-007) with required fix code
- **§11.4**: ranker, scenario, wfo_engine audit confirmations added; B9C-007, B9C-006,
  B8-006 status entries added
- **Appendix A**: B9C-007, B8-006 error patterns added
- **Appendix B**: Trading YAML filename format updated to canonical spec
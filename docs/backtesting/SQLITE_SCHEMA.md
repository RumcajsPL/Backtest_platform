# SQLITE_SCHEMA.md
## Backtesting & Optimization Framework — SQLite Schema
**Version**: 1.0.0
**Date**: 2026-02-27
**Phase**: Phase 1 — Design
**Status**: Complete

---

## Design Principles

- **One row per candidate per stage** — not denormalised blobs. Each stage produces a separate row linked by `candidate_id`.
- **All numeric metrics as individual columns** — no JSON-serialised metric blobs. Direct `WHERE`, `GROUP BY`, `ORDER BY` on every metric.
- **All parameter values as individual columns** — one column per optimizable parameter in the `candidate_parameters` table. Enables `WHERE rsi_period > 14 AND atr_multiplier < 2.0`.
- **Timestamps on all rows** — `recorded_at` on every row. Run-level timestamps in `runs`.
- **No information destroyed** — every field from every contract is a column. The ML layer decides which are features.
- **Immutable run artifacts** — `config_hash`, all seeds, and `perturbation_profile_name` in `runs` are written at run start and never updated.
- **JSON backup** — `parameters_json` in `candidate_parameters` and `evidence_json` in `verdicts` are audit-trail backups; individual columns are the primary access path.

---

## Tables

### `runs`

One row per pipeline run. Written at Stage 0 initialisation. Checkpoint is the only mutable column (updated as stages complete).

```sql
CREATE TABLE runs (
    run_id                      TEXT        PRIMARY KEY,
    config_hash                 TEXT        NOT NULL,           -- SHA-256 of backtest_template.yaml
    scenario_name               TEXT        NOT NULL,
    backtester_version          TEXT        NOT NULL,
    started_at                  TEXT        NOT NULL,           -- ISO-8601 UTC datetime
    perturbation_profile_name   TEXT        NOT NULL,
    random_search_seed          INTEGER     NOT NULL,
    ga_seed                     INTEGER     NOT NULL,
    mc_prefilter_seed           INTEGER     NOT NULL,
    mc_deep_seed                INTEGER     NOT NULL,
    sensitivity_seed            INTEGER     NOT NULL,
    wfo_window_ids              TEXT        NOT NULL,           -- JSON array of window ID strings
    checkpoint                  TEXT        NOT NULL,           -- Checkpoint enum value
    completed_at                TEXT,                           -- NULL until COMPLETE
    total_candidates_evaluated  INTEGER,
    total_runtime_seconds       REAL
);
```

---

### `candidates`

One row per unique `candidate_id`. Written when a candidate is first evaluated (Stage 1 for Random candidates; Stage 3 for GA candidates). Not updated — all stage-specific results are in their own tables.

```sql
CREATE TABLE candidates (
    candidate_id    TEXT        PRIMARY KEY,
    run_id          TEXT        NOT NULL    REFERENCES runs(run_id),
    zone_name       TEXT        NOT NULL,
    generation      INTEGER,                -- NULL for Random Search candidates
    origin_stage    TEXT        NOT NULL,   -- CandidateStage value of first appearance
    created_at      TEXT        NOT NULL    -- ISO-8601 UTC datetime
);

CREATE INDEX idx_candidates_run_id ON candidates(run_id);
CREATE INDEX idx_candidates_zone ON candidates(run_id, zone_name);
```

---

### `candidate_parameters`

One row per candidate. Individual columns for every optimizable parameter. Column set matches the parameter space definition in `backtest_template.yaml`. Parameters not in the current zone are NULL.

```sql
CREATE TABLE candidate_parameters (
    candidate_id        TEXT    PRIMARY KEY REFERENCES candidates(candidate_id),
    parameters_json     TEXT    NOT NULL,       -- Full JSON backup for audit

    -- Strategy signal parameters
    rsi_period          INTEGER,
    rsi_overbought      INTEGER,
    rsi_oversold        INTEGER,
    adx_threshold       INTEGER,
    atr_length          INTEGER,
    atr_multiplier      REAL,
    rr_target           REAL,
    risk_percentile     REAL,

    -- Structural parameters
    strategy_tf         TEXT,               -- e.g. "H1", "H4"
    htf_tf              TEXT,               -- e.g. "D1", "W1"
    session_filter      TEXT                -- e.g. "london", "london_new_york"

    -- NOTE: When new optimizable parameters are added to the YAML parameter space,
    -- new columns are added to this table via ALTER TABLE. Adding columns is safe;
    -- existing rows will have NULL for new columns (acceptable for ML feature use).
);

CREATE INDEX idx_candidate_parameters_rsi ON candidate_parameters(rsi_period, rsi_overbought);
CREATE INDEX idx_candidate_parameters_atr ON candidate_parameters(atr_length, atr_multiplier);
CREATE INDEX idx_candidate_parameters_session ON candidate_parameters(session_filter);
```

---

### `evaluations`

One row per candidate per stage. Captures fitness and constraint results. Written after each evaluation (Random Search, GA per-generation, and any re-evaluation).

```sql
CREATE TABLE evaluations (
    eval_id                 TEXT        PRIMARY KEY,    -- UUID
    candidate_id            TEXT        NOT NULL REFERENCES candidates(candidate_id),
    run_id                  TEXT        NOT NULL REFERENCES runs(run_id),
    stage                   TEXT        NOT NULL,       -- CandidateStage value
    generation              INTEGER,                    -- NULL for non-GA stages
    recorded_at             TEXT        NOT NULL,       -- ISO-8601 UTC datetime

    -- Evaluation outcome
    passed_constraints      INTEGER,                    -- 1=True, 0=False, NULL=not evaluated
    rejection_reason        TEXT,                       -- RejectionReason value or NULL
    failing_constraint      TEXT,                       -- Name of first failing constraint or NULL
    failing_value           REAL,                       -- Actual metric value that failed

    -- Fitness
    fitness_score           REAL,                       -- NULL if constraints failed

    -- Constraint actuals (always populated if strategy ran successfully)
    actual_win_rate         REAL,
    actual_max_drawdown     REAL,
    actual_losing_streak    INTEGER,
    actual_trades_per_week  REAL,
    actual_expectancy       REAL,
    actual_profit_factor    REAL,
    actual_total_trades     INTEGER,
    actual_net_pnl          REAL,

    -- Error
    error_message           TEXT                        -- Set if evaluation threw an exception
);

CREATE INDEX idx_evaluations_candidate ON evaluations(candidate_id);
CREATE INDEX idx_evaluations_run_stage ON evaluations(run_id, stage);
CREATE INDEX idx_evaluations_fitness ON evaluations(run_id, stage, fitness_score DESC);
CREATE INDEX idx_evaluations_passed ON evaluations(run_id, stage, passed_constraints);
```

---

### `wfo_window_results`

One row per candidate per WFO window. Written during Stage 4 (Full WFO) and Stage 3 (GA lightweight WFO — with `is_ga_fitness_window: 1`).

```sql
CREATE TABLE wfo_window_results (
    result_id           TEXT    PRIMARY KEY,    -- UUID
    candidate_id        TEXT    NOT NULL REFERENCES candidates(candidate_id),
    run_id              TEXT    NOT NULL REFERENCES runs(run_id),
    window_id           TEXT    NOT NULL,       -- WFOWindow.window_id
    is_ga_fitness_window INTEGER NOT NULL DEFAULT 0,  -- 1 if used for GA generation fitness
    ga_generation       INTEGER,                -- Generation number if is_ga_fitness_window=1
    recorded_at         TEXT    NOT NULL,

    -- Window evaluation results
    fitness_score       REAL,
    total_trades        INTEGER,
    net_pnl             REAL,
    max_drawdown        REAL,
    win_rate            REAL,
    expectancy          REAL,
    profit_factor       REAL,
    oos_delta           REAL,               -- IS/OOS performance delta (informational)

    -- Evaluation status
    evaluation_error    TEXT                -- NULL if successful
);

CREATE INDEX idx_wfo_candidate ON wfo_window_results(candidate_id);
CREATE INDEX idx_wfo_run_window ON wfo_window_results(run_id, window_id);
CREATE INDEX idx_wfo_full_only ON wfo_window_results(run_id, is_ga_fitness_window)
    WHERE is_ga_fitness_window = 0;
```

---

### `wfo_consistency_scores`

One row per candidate after Stage 4 completes. The composite consistency score and all four sub-metrics.

```sql
CREATE TABLE wfo_consistency_scores (
    candidate_id                TEXT    PRIMARY KEY REFERENCES candidates(candidate_id),
    run_id                      TEXT    NOT NULL REFERENCES runs(run_id),
    recorded_at                 TEXT    NOT NULL,

    -- Four orthogonal temporal metrics
    median_window_return        REAL,       -- Median per-window net P&L or fitness
    window_return_variance      REAL,       -- Variance across windows (lower = more consistent)
    worst_window_drawdown       REAL,       -- Max drawdown in worst window
    fraction_positive_windows   REAL,       -- [0, 1]

    -- Composite score
    wfo_consistency_score       REAL,       -- [0, 1], scenario-weighted composite

    -- Metadata
    windows_evaluated           INTEGER,
    windows_total               INTEGER,
    oos_gate_triggered          INTEGER,    -- 1=True, 0=False
    window_collapse_flag        INTEGER     -- 1=True, 0=False
);

CREATE INDEX idx_wfo_scores_run ON wfo_consistency_scores(run_id, wfo_consistency_score DESC);
```

---

### `mc_results`

One row per candidate per MC mode (pre-filter and deep are separate rows). Written after Stage 2 (pre-filter) and Stage 5 (deep).

```sql
CREATE TABLE mc_results (
    result_id                       TEXT    PRIMARY KEY,    -- UUID
    candidate_id                    TEXT    NOT NULL REFERENCES candidates(candidate_id),
    run_id                          TEXT    NOT NULL REFERENCES runs(run_id),
    mode                            TEXT    NOT NULL,       -- "pre_filter" | "deep"
    perturbation_profile_name       TEXT    NOT NULL,
    iterations                      INTEGER NOT NULL,
    recorded_at                     TEXT    NOT NULL,

    -- MC metrics
    avg_final_equity                REAL,
    worst_drawdown_across_paths     REAL,
    ruin_probability                REAL,       -- [0, 1]
    p5_final_equity                 REAL,       -- 5th percentile final equity

    -- Status
    evaluation_error                TEXT        -- NULL if successful
);

CREATE INDEX idx_mc_candidate ON mc_results(candidate_id, mode);
CREATE INDEX idx_mc_ruin ON mc_results(run_id, mode, ruin_probability);
```

---

### `sensitivity_results`

One row per candidate per parameter per step. Written after Stage 6.

```sql
CREATE TABLE sensitivity_results (
    result_id           TEXT    PRIMARY KEY,    -- UUID
    candidate_id        TEXT    NOT NULL REFERENCES candidates(candidate_id),
    run_id              TEXT    NOT NULL REFERENCES runs(run_id),
    parameter_name      TEXT    NOT NULL,
    step                INTEGER NOT NULL,       -- -2, -1, +1, +2
    perturbed_value     TEXT    NOT NULL,       -- String representation of actual value
    baseline_fitness    REAL    NOT NULL,
    perturbed_fitness   REAL,                   -- NULL if evaluation failed
    fitness_delta       REAL,                   -- perturbed - baseline; NULL if failed
    is_spike            INTEGER NOT NULL,       -- 1 if |delta| > spike_threshold
    recorded_at         TEXT    NOT NULL,
    evaluation_error    TEXT
);

CREATE INDEX idx_sensitivity_candidate ON sensitivity_results(candidate_id);
CREATE INDEX idx_sensitivity_spikes ON sensitivity_results(run_id, is_spike)
    WHERE is_spike = 1;
```

---

### `sensitivity_profiles`

One row per candidate. Summary of the full sensitivity map.

```sql
CREATE TABLE sensitivity_profiles (
    candidate_id                TEXT    PRIMARY KEY REFERENCES candidates(candidate_id),
    run_id                      TEXT    NOT NULL REFERENCES runs(run_id),
    baseline_fitness            REAL    NOT NULL,
    spike_detected              INTEGER NOT NULL,   -- 1=True, 0=False
    spike_parameters            TEXT,               -- Comma-separated names; NULL if no spike
    profile_complete            INTEGER NOT NULL,   -- 1=True (>50% perturbations succeeded)
    recorded_at                 TEXT    NOT NULL
);

CREATE INDEX idx_sensitivity_profiles_spikes ON sensitivity_profiles(run_id, spike_detected);
```

---

### `verdicts`

One row per candidate. Written in Stage 7. The final pipeline output per candidate.

```sql
CREATE TABLE verdicts (
    candidate_id                TEXT    PRIMARY KEY REFERENCES candidates(candidate_id),
    run_id                      TEXT    NOT NULL REFERENCES runs(run_id),
    scenario_name               TEXT    NOT NULL,
    verdict                     TEXT    NOT NULL,       -- "auto_go" | "borderline" | "no_go"
    deployment_status           TEXT    NOT NULL,       -- "PAPER_TRADE_REQUIRED" | "LIVE_APPROVED"

    -- Pillar scores
    wfo_consistency_score       REAL,
    mc_deep_ruin_probability    REAL,

    -- Modifier flags
    sensitivity_spike           INTEGER NOT NULL,
    oos_gate_triggered          INTEGER NOT NULL,
    window_collapse_flag        INTEGER NOT NULL,
    sensitivity_profile_incomplete INTEGER NOT NULL,

    -- Informational evidence
    median_oos_delta            REAL,
    parameter_region_width      REAL,
    yaml_output_path            TEXT,               -- Path to trading-ready YAML, or NULL

    -- Human-readable evidence
    evidence_summary            TEXT    NOT NULL,
    evidence_json               TEXT    NOT NULL,   -- Full JSON of all evidence fields

    recorded_at                 TEXT    NOT NULL,
    deployment_status_updated_at TEXT               -- Set when operator changes to LIVE_APPROVED
);

CREATE INDEX idx_verdicts_run ON verdicts(run_id, verdict);
CREATE INDEX idx_verdicts_go ON verdicts(run_id) WHERE verdict = 'auto_go';
CREATE INDEX idx_verdicts_borderline ON verdicts(run_id) WHERE verdict = 'borderline';
```

---

## Full Schema (Combined)

```sql
-- Enable WAL mode (set at connection open, not in schema)
-- PRAGMA journal_mode = WAL;
-- PRAGMA foreign_keys = ON;
-- PRAGMA synchronous = NORMAL;  -- Safe with WAL mode

CREATE TABLE runs (
    run_id                      TEXT    PRIMARY KEY,
    config_hash                 TEXT    NOT NULL,
    scenario_name               TEXT    NOT NULL,
    backtester_version          TEXT    NOT NULL,
    started_at                  TEXT    NOT NULL,
    perturbation_profile_name   TEXT    NOT NULL,
    random_search_seed          INTEGER NOT NULL,
    ga_seed                     INTEGER NOT NULL,
    mc_prefilter_seed           INTEGER NOT NULL,
    mc_deep_seed                INTEGER NOT NULL,
    sensitivity_seed            INTEGER NOT NULL,
    wfo_window_ids              TEXT    NOT NULL,
    checkpoint                  TEXT    NOT NULL,
    completed_at                TEXT,
    total_candidates_evaluated  INTEGER,
    total_runtime_seconds       REAL
);

CREATE TABLE candidates (
    candidate_id    TEXT    PRIMARY KEY,
    run_id          TEXT    NOT NULL REFERENCES runs(run_id),
    zone_name       TEXT    NOT NULL,
    generation      INTEGER,
    origin_stage    TEXT    NOT NULL,
    created_at      TEXT    NOT NULL
);
CREATE INDEX idx_candidates_run_id ON candidates(run_id);
CREATE INDEX idx_candidates_zone ON candidates(run_id, zone_name);

CREATE TABLE candidate_parameters (
    candidate_id    TEXT    PRIMARY KEY REFERENCES candidates(candidate_id),
    parameters_json TEXT    NOT NULL,
    rsi_period      INTEGER,
    rsi_overbought  INTEGER,
    rsi_oversold    INTEGER,
    adx_threshold   INTEGER,
    atr_length      INTEGER,
    atr_multiplier  REAL,
    rr_target       REAL,
    risk_percentile REAL,
    strategy_tf     TEXT,
    htf_tf          TEXT,
    session_filter  TEXT
);
CREATE INDEX idx_candidate_parameters_rsi ON candidate_parameters(rsi_period, rsi_overbought);
CREATE INDEX idx_candidate_parameters_atr ON candidate_parameters(atr_length, atr_multiplier);
CREATE INDEX idx_candidate_parameters_session ON candidate_parameters(session_filter);

CREATE TABLE evaluations (
    eval_id                 TEXT    PRIMARY KEY,
    candidate_id            TEXT    NOT NULL REFERENCES candidates(candidate_id),
    run_id                  TEXT    NOT NULL REFERENCES runs(run_id),
    stage                   TEXT    NOT NULL,
    generation              INTEGER,
    recorded_at             TEXT    NOT NULL,
    passed_constraints      INTEGER,
    rejection_reason        TEXT,
    failing_constraint      TEXT,
    failing_value           REAL,
    fitness_score           REAL,
    actual_win_rate         REAL,
    actual_max_drawdown     REAL,
    actual_losing_streak    INTEGER,
    actual_trades_per_week  REAL,
    actual_expectancy       REAL,
    actual_profit_factor    REAL,
    actual_total_trades     INTEGER,
    actual_net_pnl          REAL,
    error_message           TEXT
);
CREATE INDEX idx_evaluations_candidate ON evaluations(candidate_id);
CREATE INDEX idx_evaluations_run_stage ON evaluations(run_id, stage);
CREATE INDEX idx_evaluations_fitness ON evaluations(run_id, stage, fitness_score DESC);
CREATE INDEX idx_evaluations_passed ON evaluations(run_id, stage, passed_constraints);

CREATE TABLE wfo_window_results (
    result_id               TEXT    PRIMARY KEY,
    candidate_id            TEXT    NOT NULL REFERENCES candidates(candidate_id),
    run_id                  TEXT    NOT NULL REFERENCES runs(run_id),
    window_id               TEXT    NOT NULL,
    is_ga_fitness_window    INTEGER NOT NULL DEFAULT 0,
    ga_generation           INTEGER,
    recorded_at             TEXT    NOT NULL,
    fitness_score           REAL,
    total_trades            INTEGER,
    net_pnl                 REAL,
    max_drawdown            REAL,
    win_rate                REAL,
    expectancy              REAL,
    profit_factor           REAL,
    oos_delta               REAL,
    evaluation_error        TEXT
);
CREATE INDEX idx_wfo_candidate ON wfo_window_results(candidate_id);
CREATE INDEX idx_wfo_run_window ON wfo_window_results(run_id, window_id);
CREATE INDEX idx_wfo_full_only ON wfo_window_results(run_id, is_ga_fitness_window)
    WHERE is_ga_fitness_window = 0;

CREATE TABLE wfo_consistency_scores (
    candidate_id                TEXT    PRIMARY KEY REFERENCES candidates(candidate_id),
    run_id                      TEXT    NOT NULL REFERENCES runs(run_id),
    recorded_at                 TEXT    NOT NULL,
    median_window_return        REAL,
    window_return_variance      REAL,
    worst_window_drawdown       REAL,
    fraction_positive_windows   REAL,
    wfo_consistency_score       REAL,
    windows_evaluated           INTEGER,
    windows_total               INTEGER,
    oos_gate_triggered          INTEGER,
    window_collapse_flag        INTEGER
);
CREATE INDEX idx_wfo_scores_run ON wfo_consistency_scores(run_id, wfo_consistency_score DESC);

CREATE TABLE mc_results (
    result_id                   TEXT    PRIMARY KEY,
    candidate_id                TEXT    NOT NULL REFERENCES candidates(candidate_id),
    run_id                      TEXT    NOT NULL REFERENCES runs(run_id),
    mode                        TEXT    NOT NULL,
    perturbation_profile_name   TEXT    NOT NULL,
    iterations                  INTEGER NOT NULL,
    recorded_at                 TEXT    NOT NULL,
    avg_final_equity            REAL,
    worst_drawdown_across_paths REAL,
    ruin_probability            REAL,
    p5_final_equity             REAL,
    evaluation_error            TEXT
);
CREATE INDEX idx_mc_candidate ON mc_results(candidate_id, mode);
CREATE INDEX idx_mc_ruin ON mc_results(run_id, mode, ruin_probability);

CREATE TABLE sensitivity_results (
    result_id           TEXT    PRIMARY KEY,
    candidate_id        TEXT    NOT NULL REFERENCES candidates(candidate_id),
    run_id              TEXT    NOT NULL REFERENCES runs(run_id),
    parameter_name      TEXT    NOT NULL,
    step                INTEGER NOT NULL,
    perturbed_value     TEXT    NOT NULL,
    baseline_fitness    REAL    NOT NULL,
    perturbed_fitness   REAL,
    fitness_delta       REAL,
    is_spike            INTEGER NOT NULL,
    recorded_at         TEXT    NOT NULL,
    evaluation_error    TEXT
);
CREATE INDEX idx_sensitivity_candidate ON sensitivity_results(candidate_id);
CREATE INDEX idx_sensitivity_spikes ON sensitivity_results(run_id, is_spike)
    WHERE is_spike = 1;

CREATE TABLE sensitivity_profiles (
    candidate_id        TEXT    PRIMARY KEY REFERENCES candidates(candidate_id),
    run_id              TEXT    NOT NULL REFERENCES runs(run_id),
    baseline_fitness    REAL    NOT NULL,
    spike_detected      INTEGER NOT NULL,
    spike_parameters    TEXT,
    profile_complete    INTEGER NOT NULL,
    recorded_at         TEXT    NOT NULL
);
CREATE INDEX idx_sensitivity_profiles_spikes ON sensitivity_profiles(run_id, spike_detected);

CREATE TABLE verdicts (
    candidate_id                    TEXT    PRIMARY KEY REFERENCES candidates(candidate_id),
    run_id                          TEXT    NOT NULL REFERENCES runs(run_id),
    scenario_name                   TEXT    NOT NULL,
    verdict                         TEXT    NOT NULL,
    deployment_status               TEXT    NOT NULL,
    wfo_consistency_score           REAL,
    mc_deep_ruin_probability        REAL,
    sensitivity_spike               INTEGER NOT NULL,
    oos_gate_triggered              INTEGER NOT NULL,
    window_collapse_flag            INTEGER NOT NULL,
    sensitivity_profile_incomplete  INTEGER NOT NULL,
    median_oos_delta                REAL,
    parameter_region_width          REAL,
    yaml_output_path                TEXT,
    evidence_summary                TEXT    NOT NULL,
    evidence_json                   TEXT    NOT NULL,
    recorded_at                     TEXT    NOT NULL,
    deployment_status_updated_at    TEXT
);
CREATE INDEX idx_verdicts_run ON verdicts(run_id, verdict);
CREATE INDEX idx_verdicts_go ON verdicts(run_id) WHERE verdict = 'auto_go';
CREATE INDEX idx_verdicts_borderline ON verdicts(run_id) WHERE verdict = 'borderline';
```

---

## Representative Query Examples

These queries validate that the schema meets the access requirements of the pipeline and future ML use.

```sql
-- 1. Full pipeline funnel for a run
SELECT
    stage,
    COUNT(*) as total,
    SUM(passed_constraints) as passed,
    AVG(fitness_score) as avg_fitness
FROM evaluations
WHERE run_id = :run_id
GROUP BY stage
ORDER BY MIN(recorded_at);

-- 2. Top 10 candidates by fitness in Random Search stage
SELECT
    c.candidate_id,
    cp.rsi_period, cp.atr_multiplier, cp.session_filter,
    e.fitness_score,
    e.actual_win_rate, e.actual_max_drawdown, e.actual_expectancy
FROM evaluations e
JOIN candidates c ON e.candidate_id = c.candidate_id
JOIN candidate_parameters cp ON c.candidate_id = cp.candidate_id
WHERE e.run_id = :run_id AND e.stage = 'RANDOM' AND e.passed_constraints = 1
ORDER BY e.fitness_score DESC
LIMIT 10;

-- 3. Candidates that passed MC Pre-Filter with ruin < 10%
SELECT
    c.candidate_id,
    mc.ruin_probability,
    mc.worst_drawdown_across_paths,
    e.fitness_score
FROM mc_results mc
JOIN candidates c ON mc.candidate_id = c.candidate_id
JOIN evaluations e ON c.candidate_id = e.candidate_id AND e.stage = 'RANDOM'
WHERE mc.run_id = :run_id AND mc.mode = 'pre_filter' AND mc.ruin_probability < 0.10
ORDER BY mc.ruin_probability ASC;

-- 4. WFO consistency for all full-WFO candidates
SELECT
    c.candidate_id,
    wcs.wfo_consistency_score,
    wcs.median_window_return,
    wcs.window_return_variance,
    wcs.worst_window_drawdown,
    wcs.fraction_positive_windows,
    wcs.windows_evaluated,
    wcs.window_collapse_flag
FROM wfo_consistency_scores wcs
JOIN candidates c ON wcs.candidate_id = c.candidate_id
WHERE wcs.run_id = :run_id
ORDER BY wcs.wfo_consistency_score DESC;

-- 5. Parameter sensitivity spikes for a specific candidate
SELECT
    sr.parameter_name,
    sr.step,
    sr.perturbed_value,
    sr.baseline_fitness,
    sr.perturbed_fitness,
    sr.fitness_delta,
    sr.is_spike
FROM sensitivity_results sr
WHERE sr.candidate_id = :candidate_id
ORDER BY ABS(sr.fitness_delta) DESC;

-- 6. Final verdicts with all evidence for a run
SELECT
    v.candidate_id,
    v.verdict,
    v.deployment_status,
    v.wfo_consistency_score,
    v.mc_deep_ruin_probability,
    v.sensitivity_spike,
    v.evidence_summary,
    cp.rsi_period, cp.atr_multiplier, cp.session_filter, cp.strategy_tf
FROM verdicts v
JOIN candidate_parameters cp ON v.candidate_id = cp.candidate_id
WHERE v.run_id = :run_id
ORDER BY v.verdict, v.wfo_consistency_score DESC;

-- 7. ML feature matrix — all go/borderline candidates with full metric set
-- (This is the query the future ML layer will use)
SELECT
    c.candidate_id,
    cp.*,
    e.fitness_score,
    e.actual_win_rate, e.actual_max_drawdown, e.actual_losing_streak,
    e.actual_trades_per_week, e.actual_expectancy, e.actual_profit_factor,
    wcs.wfo_consistency_score, wcs.median_window_return, wcs.window_return_variance,
    wcs.worst_window_drawdown, wcs.fraction_positive_windows,
    mc.ruin_probability, mc.worst_drawdown_across_paths, mc.p5_final_equity,
    sp.spike_detected, sp.profile_complete,
    v.verdict
FROM verdicts v
JOIN candidates c ON v.candidate_id = c.candidate_id
JOIN candidate_parameters cp ON c.candidate_id = cp.candidate_id
JOIN evaluations e ON c.candidate_id = e.candidate_id AND e.stage = 'RANDOM'
LEFT JOIN wfo_consistency_scores wcs ON c.candidate_id = wcs.candidate_id
LEFT JOIN mc_results mc ON c.candidate_id = mc.candidate_id AND mc.mode = 'deep'
LEFT JOIN sensitivity_profiles sp ON c.candidate_id = sp.candidate_id
WHERE v.run_id = :run_id AND v.verdict != 'no_go'
ORDER BY v.wfo_consistency_score DESC;

-- 8. Parameter region analysis — what RSI/ATR combinations appear in go verdicts?
SELECT
    cp.rsi_period,
    cp.atr_multiplier,
    cp.session_filter,
    COUNT(*) as go_count,
    AVG(v.wfo_consistency_score) as avg_wfo_score,
    AVG(v.mc_deep_ruin_probability) as avg_ruin_prob
FROM verdicts v
JOIN candidate_parameters cp ON v.candidate_id = cp.candidate_id
WHERE v.run_id = :run_id AND v.verdict = 'auto_go'
GROUP BY cp.rsi_period, cp.atr_multiplier, cp.session_filter
ORDER BY go_count DESC, avg_wfo_score DESC;

-- 9. Window-by-window performance for a candidate (for the HTML report)
SELECT
    wwr.window_id,
    wwr.fitness_score,
    wwr.net_pnl,
    wwr.max_drawdown,
    wwr.win_rate,
    wwr.expectancy,
    wwr.total_trades,
    wwr.oos_delta
FROM wfo_window_results wwr
WHERE wwr.candidate_id = :candidate_id
  AND wwr.is_ga_fitness_window = 0
ORDER BY wwr.window_id;

-- 10. Run history summary (across multiple runs)
SELECT
    r.run_id,
    r.scenario_name,
    r.started_at,
    r.total_candidates_evaluated,
    r.total_runtime_seconds / 3600.0 as runtime_hours,
    COUNT(CASE WHEN v.verdict = 'auto_go' THEN 1 END) as go_count,
    COUNT(CASE WHEN v.verdict = 'borderline' THEN 1 END) as borderline_count,
    COUNT(CASE WHEN v.verdict = 'no_go' THEN 1 END) as no_go_count
FROM runs r
LEFT JOIN verdicts v ON r.run_id = v.run_id
GROUP BY r.run_id
ORDER BY r.started_at DESC;
```
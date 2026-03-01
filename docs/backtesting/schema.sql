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
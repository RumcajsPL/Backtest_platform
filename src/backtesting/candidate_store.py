"""
candidate_store.py — SQLite persistence layer for the backtesting framework.

Architecture:
- WAL mode + foreign keys + NORMAL synchronous (safe with WAL)
- Single-writer queue: callers call write_* methods which enqueue; one daemon
  thread drains the queue and performs all SQLite writes sequentially.
  This eliminates write contention entirely.
- All reads are direct (no queue) — WAL allows concurrent readers.

Source of truth: docs/backtesting/SQLITE_SCHEMA.md
"""
from __future__ import annotations

import json
import logging
import queue
import sqlite3
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from src.backtesting.contracts import (
    CandidateRecord,
    CandidateStage,
    Checkpoint,
    RunMetadata,
    Verdict,
)

logger = logging.getLogger(__name__)


# ── Full schema DDL from SQLITE_SCHEMA.md ────────────────────────────────────

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS runs (
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

CREATE TABLE IF NOT EXISTS candidates (
    candidate_id    TEXT    PRIMARY KEY,
    run_id          TEXT    NOT NULL REFERENCES runs(run_id),
    zone_name       TEXT    NOT NULL,
    generation      INTEGER,
    origin_stage    TEXT    NOT NULL,
    created_at      TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_candidates_run_id ON candidates(run_id);
CREATE INDEX IF NOT EXISTS idx_candidates_zone ON candidates(run_id, zone_name);

CREATE TABLE IF NOT EXISTS candidate_parameters (
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
CREATE INDEX IF NOT EXISTS idx_candidate_parameters_rsi
    ON candidate_parameters(rsi_period, rsi_overbought);
CREATE INDEX IF NOT EXISTS idx_candidate_parameters_atr
    ON candidate_parameters(atr_length, atr_multiplier);
CREATE INDEX IF NOT EXISTS idx_candidate_parameters_session
    ON candidate_parameters(session_filter);

CREATE TABLE IF NOT EXISTS evaluations (
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
CREATE INDEX IF NOT EXISTS idx_evaluations_candidate ON evaluations(candidate_id);
CREATE INDEX IF NOT EXISTS idx_evaluations_run_stage ON evaluations(run_id, stage);
CREATE INDEX IF NOT EXISTS idx_evaluations_fitness
    ON evaluations(run_id, stage, fitness_score DESC);
CREATE INDEX IF NOT EXISTS idx_evaluations_passed
    ON evaluations(run_id, stage, passed_constraints);

CREATE TABLE IF NOT EXISTS wfo_window_results (
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
CREATE INDEX IF NOT EXISTS idx_wfo_candidate ON wfo_window_results(candidate_id);
CREATE INDEX IF NOT EXISTS idx_wfo_run_window ON wfo_window_results(run_id, window_id);

CREATE TABLE IF NOT EXISTS wfo_consistency_scores (
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
CREATE INDEX IF NOT EXISTS idx_wfo_scores_run
    ON wfo_consistency_scores(run_id, wfo_consistency_score DESC);

CREATE TABLE IF NOT EXISTS mc_results (
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
CREATE INDEX IF NOT EXISTS idx_mc_candidate ON mc_results(candidate_id, mode);
CREATE INDEX IF NOT EXISTS idx_mc_ruin ON mc_results(run_id, mode, ruin_probability);

CREATE TABLE IF NOT EXISTS sensitivity_results (
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
CREATE INDEX IF NOT EXISTS idx_sensitivity_candidate ON sensitivity_results(candidate_id);

CREATE TABLE IF NOT EXISTS sensitivity_profiles (
    candidate_id        TEXT    PRIMARY KEY REFERENCES candidates(candidate_id),
    run_id              TEXT    NOT NULL REFERENCES runs(run_id),
    baseline_fitness    REAL    NOT NULL,
    spike_detected      INTEGER NOT NULL,
    spike_parameters    TEXT,
    profile_complete    INTEGER NOT NULL,
    recorded_at         TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sensitivity_profiles_spikes
    ON sensitivity_profiles(run_id, spike_detected);

CREATE TABLE IF NOT EXISTS verdicts (
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
CREATE INDEX IF NOT EXISTS idx_verdicts_run ON verdicts(run_id, verdict);
"""

# Known parameter column names in candidate_parameters (from schema)
_PARAMETER_COLUMNS = frozenset({
    "rsi_period", "rsi_overbought", "rsi_oversold", "adx_threshold",
    "atr_length", "atr_multiplier", "rr_target", "risk_percentile",
    "strategy_tf", "htf_tf", "session_filter",
})

# Sentinel that signals the writer thread to stop
_STOP_SENTINEL = object()


def _ts(dt: datetime) -> str:
    """Convert datetime to ISO-8601 UTC string."""
    return dt.isoformat()


def _now_ts() -> str:
    return datetime.now(tz=None).isoformat()


class CandidateStore:
    """
    Thread-safe SQLite store for all backtesting pipeline results.

    Write path: callers enqueue via write_* methods; one daemon writer thread
    drains the queue and performs all INSERTs sequentially.

    Read path: direct synchronous queries (WAL allows concurrent reads).
    """

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA synchronous = NORMAL")
        self._conn.executescript(_SCHEMA_SQL)
        self._conn.commit()

        self._queue: queue.Queue = queue.Queue()
        self._writer_errors: List[str] = []
        self._writer = threading.Thread(
            target=self._drain_queue, daemon=True, name="candidate_store_writer"
        )
        self._writer.start()
        logger.info("CandidateStore initialised at %s", db_path)

    # ── Public write API ──────────────────────────────────────────────────────

    def initialise_run(self, run_metadata: RunMetadata) -> None:
        """Insert the runs row. Called once at Stage 0. Blocks until written."""
        self._queue.put(("_write_run", run_metadata))
        self._queue.join()   # Block until this specific write completes

    def write_candidate(self, record: CandidateRecord) -> None:
        """Non-blocking enqueue. Worker-safe. Returns immediately."""
        self._queue.put(("_write_candidate_record", record))

    def set_checkpoint(self, run_id: str, checkpoint: Checkpoint) -> None:
        """Update checkpoint in runs table. Blocks until written."""
        self._queue.put(("_set_checkpoint", (run_id, checkpoint)))
        self._queue.join()

    # ── Public read API ───────────────────────────────────────────────────────

    def get_checkpoint(self, run_id: str) -> Checkpoint:
        """Read current checkpoint for a run."""
        row = self._conn.execute(
            "SELECT checkpoint FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"No run found with run_id={run_id!r}")
        return Checkpoint[row[0]]

    def get_run_metadata(self, run_id: str) -> Optional[RunMetadata]:
        """Return RunMetadata for the given run_id, or None if not found."""
        row = self._conn.execute(
            "SELECT run_id, config_hash, scenario_name, started_at, "
            "perturbation_profile_name, random_search_seed, ga_seed, "
            "mc_prefilter_seed, mc_deep_seed, sensitivity_seed, "
            "wfo_window_ids, checkpoint, backtester_version "
            "FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if row is None:
            return None
        (
            run_id_, config_hash, scenario_name, started_at,
            perturbation_profile_name, random_search_seed, ga_seed,
            mc_prefilter_seed, mc_deep_seed, sensitivity_seed,
            wfo_window_ids_json, checkpoint_val, backtester_version,
        ) = row
        return RunMetadata(
            run_id=run_id_,
            config_hash=config_hash,
            scenario_name=scenario_name,
            started_at=datetime.fromisoformat(started_at),
            perturbation_profile_name=perturbation_profile_name,
            random_search_seed=random_search_seed,
            ga_seed=ga_seed,
            mc_prefilter_seed=mc_prefilter_seed,
            mc_deep_seed=mc_deep_seed,
            sensitivity_seed=sensitivity_seed,
            wfo_window_ids=tuple(json.loads(wfo_window_ids_json)),
            checkpoint=Checkpoint[checkpoint_val],
            backtester_version=backtester_version,
        )

    def query_candidates(
        self,
        run_id: str,
        stage: Optional[CandidateStage] = None,
        min_fitness: Optional[float] = None,
        verdict: Optional[Verdict] = None,
        limit: Optional[int] = None,
        order_by: str = "fitness_score DESC",
    ) -> List[CandidateRecord]:
        """
        Query evaluations for a run with optional filters.
        Returns CandidateRecord list ordered by the given column spec.
        """
        wheres = ["e.run_id = ?"]
        params: list = [run_id]

        if stage is not None:
            wheres.append("e.stage = ?")
            params.append(stage.value)
        if min_fitness is not None:
            wheres.append("e.fitness_score >= ?")
            params.append(min_fitness)
        if verdict is not None:
            wheres.append("v.verdict = ?")
            params.append(verdict.value)

        where_clause = " AND ".join(wheres)
        limit_clause = f"LIMIT {int(limit)}" if limit is not None else ""

        sql = f"""
            SELECT
                e.run_id, e.candidate_id, c.zone_name, e.stage, e.generation,
                e.recorded_at, cp.parameters_json,
                e.fitness_score, e.passed_constraints, e.rejection_reason,
                e.failing_constraint, e.failing_value,
                e.actual_win_rate, e.actual_max_drawdown, e.actual_losing_streak,
                e.actual_trades_per_week, e.actual_expectancy, e.actual_profit_factor,
                wcs.median_window_return, wcs.window_return_variance,
                wcs.worst_window_drawdown, wcs.fraction_positive_windows,
                wcs.wfo_consistency_score, wcs.windows_evaluated,
                wcs.oos_gate_triggered, wcs.window_collapse_flag,
                mc_pre.ruin_probability, mc_pre.avg_final_equity, mc_pre.iterations,
                mc_deep.ruin_probability, mc_deep.avg_final_equity,
                mc_deep.worst_drawdown_across_paths, mc_deep.p5_final_equity,
                mc_deep.iterations,
                sp.spike_detected, sp.spike_parameters, sp.profile_complete,
                v.verdict, v.deployment_status, v.evidence_summary
            FROM evaluations e
            JOIN candidates c ON e.candidate_id = c.candidate_id
            LEFT JOIN candidate_parameters cp ON e.candidate_id = cp.candidate_id
            LEFT JOIN wfo_consistency_scores wcs ON e.candidate_id = wcs.candidate_id
            LEFT JOIN mc_results mc_pre
                ON e.candidate_id = mc_pre.candidate_id AND mc_pre.mode = 'pre_filter'
            LEFT JOIN mc_results mc_deep
                ON e.candidate_id = mc_deep.candidate_id AND mc_deep.mode = 'deep'
            LEFT JOIN sensitivity_profiles sp ON e.candidate_id = sp.candidate_id
            LEFT JOIN verdicts v ON e.candidate_id = v.candidate_id
            WHERE {where_clause}
            ORDER BY {order_by}
            {limit_clause}
        """
        rows = self._conn.execute(sql, params).fetchall()
        return [self._row_to_candidate_record(row) for row in rows]

    def close(self) -> None:
        """Flush pending writes, stop the writer thread, close connection."""
        self._queue.join()           # Drain all pending items
        self._queue.put(_STOP_SENTINEL)
        self._writer.join()
        self._conn.close()
        logger.info("CandidateStore closed")

    # ── Writer thread ─────────────────────────────────────────────────────────

    def _drain_queue(self) -> None:
        """Run in the writer thread. Drain the queue until sentinel received."""
        while True:
            item = self._queue.get()
            try:
                if item is _STOP_SENTINEL:
                    return
                method_name, payload = item
                getattr(self, method_name)(payload)
            except Exception as exc:
                logger.error("Writer thread error: %s", exc, exc_info=True)
                self._writer_errors.append(str(exc))
            finally:
                self._queue.task_done()

    # ── Internal write methods (called by writer thread only) ─────────────────

    def _write_run(self, run_metadata: RunMetadata) -> None:
        self._conn.execute(
            """INSERT OR IGNORE INTO runs (
                run_id, config_hash, scenario_name, backtester_version,
                started_at, perturbation_profile_name,
                random_search_seed, ga_seed, mc_prefilter_seed,
                mc_deep_seed, sensitivity_seed, wfo_window_ids, checkpoint
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                run_metadata.run_id,
                run_metadata.config_hash,
                run_metadata.scenario_name,
                run_metadata.backtester_version,
                _ts(run_metadata.started_at),
                run_metadata.perturbation_profile_name,
                run_metadata.random_search_seed,
                run_metadata.ga_seed,
                run_metadata.mc_prefilter_seed,
                run_metadata.mc_deep_seed,
                run_metadata.sensitivity_seed,
                json.dumps(list(run_metadata.wfo_window_ids)),
                run_metadata.checkpoint.name,   # store name ("RUN_INITIALISED") not int
            ),
        )
        self._conn.commit()
        logger.debug("Run row written: %s", run_metadata.run_id)

    def _set_checkpoint(self, args: tuple) -> None:
        run_id, checkpoint = args
        self._conn.execute(
            "UPDATE runs SET checkpoint = ? WHERE run_id = ?",
            (checkpoint.name, run_id),  # store name not int
        )
        self._conn.commit()
        logger.debug("Checkpoint updated: run=%s  checkpoint=%s", run_id, checkpoint.name)

    def _write_candidate_record(self, record: CandidateRecord) -> None:
        """Write one CandidateRecord: upsert candidates + parameters, insert evaluation."""
        params = json.loads(record.parameters_json)

        # 1. candidates row
        self._conn.execute(
            """INSERT OR IGNORE INTO candidates
               (candidate_id, run_id, zone_name, generation, origin_stage, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                record.candidate_id,
                record.run_id,
                record.zone_name,
                record.generation,
                record.stage,
                _ts(record.recorded_at),
            ),
        )

        # 2. candidate_parameters row
        self._conn.execute(
            """INSERT OR IGNORE INTO candidate_parameters
               (candidate_id, parameters_json,
                rsi_period, rsi_overbought, rsi_oversold, adx_threshold,
                atr_length, atr_multiplier, rr_target, risk_percentile,
                strategy_tf, htf_tf, session_filter)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                record.candidate_id,
                record.parameters_json,
                params.get("rsi_period"),
                params.get("rsi_overbought"),
                params.get("rsi_oversold"),
                params.get("adx_threshold"),
                params.get("atr_length"),
                params.get("atr_multiplier"),
                params.get("rr_target"),
                params.get("risk_percentile"),
                params.get("strategy_tf"),
                params.get("htf_tf"),
                params.get("session_filter"),
            ),
        )

        # 3. evaluations row
        self._conn.execute(
            """INSERT INTO evaluations (
                eval_id, candidate_id, run_id, stage, generation, recorded_at,
                passed_constraints, rejection_reason, failing_constraint, failing_value,
                fitness_score, actual_win_rate, actual_max_drawdown,
                actual_losing_streak, actual_trades_per_week,
                actual_expectancy, actual_profit_factor
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                str(uuid.uuid4()),
                record.candidate_id,
                record.run_id,
                record.stage,
                record.generation,
                _ts(record.recorded_at),
                int(record.passed_constraints) if record.passed_constraints is not None else None,
                record.rejection_reason,
                record.failing_constraint,
                record.failing_value,
                record.fitness_score,
                record.actual_win_rate,
                record.actual_max_drawdown,
                record.actual_losing_streak,
                record.actual_trades_per_week,
                record.actual_expectancy,
                record.actual_profit_factor,
            ),
        )
        self._conn.commit()

    # ── Row deserialisation ───────────────────────────────────────────────────

    @staticmethod
    def _row_to_candidate_record(row: tuple) -> CandidateRecord:
        """Convert a query result row to a CandidateRecord."""
        (
            run_id, candidate_id, zone_name, stage, generation,
            recorded_at, parameters_json,
            fitness_score, passed_constraints, rejection_reason,
            failing_constraint, failing_value,
            actual_win_rate, actual_max_drawdown, actual_losing_streak,
            actual_trades_per_week, actual_expectancy, actual_profit_factor,
            wfo_median, wfo_variance, wfo_worst_dd, wfo_frac_pos,
            wfo_score, wfo_evaluated, wfo_oos_gate, wfo_collapse,
            mc_pre_ruin, mc_pre_equity, mc_pre_iters,
            mc_deep_ruin, mc_deep_equity, mc_deep_worst_dd, mc_deep_p5, mc_deep_iters,
            sens_spike, sens_spike_params, sens_complete,
            verdict_, deployment_status_, evidence_summary_,
        ) = row

        return CandidateRecord(
            run_id=run_id,
            candidate_id=candidate_id,
            zone_name=zone_name,
            stage=stage,
            generation=generation,
            recorded_at=datetime.fromisoformat(recorded_at),
            parameters_json=parameters_json or "{}",
            fitness_score=fitness_score,
            passed_constraints=bool(passed_constraints) if passed_constraints is not None else None,
            rejection_reason=rejection_reason,
            failing_constraint=failing_constraint,
            failing_value=failing_value,
            actual_win_rate=actual_win_rate,
            actual_max_drawdown=actual_max_drawdown,
            actual_losing_streak=actual_losing_streak,
            actual_trades_per_week=actual_trades_per_week,
            actual_expectancy=actual_expectancy,
            actual_profit_factor=actual_profit_factor,
            wfo_median_window_return=wfo_median,
            wfo_window_return_variance=wfo_variance,
            wfo_worst_window_drawdown=wfo_worst_dd,
            wfo_fraction_positive_windows=wfo_frac_pos,
            wfo_consistency_score=wfo_score,
            wfo_windows_evaluated=wfo_evaluated,
            wfo_oos_gate_triggered=bool(wfo_oos_gate) if wfo_oos_gate is not None else None,
            wfo_window_collapse_flag=bool(wfo_collapse) if wfo_collapse is not None else None,
            mc_prefilter_ruin_probability=mc_pre_ruin,
            mc_prefilter_avg_final_equity=mc_pre_equity,
            mc_prefilter_iterations=mc_pre_iters,
            mc_deep_ruin_probability=mc_deep_ruin,
            mc_deep_avg_final_equity=mc_deep_equity,
            mc_deep_worst_drawdown=mc_deep_worst_dd,
            mc_deep_p5_final_equity=mc_deep_p5,
            mc_deep_iterations=mc_deep_iters,
            sensitivity_spike_detected=bool(sens_spike) if sens_spike is not None else None,
            sensitivity_spike_parameters=sens_spike_params,
            sensitivity_profile_complete=bool(sens_complete) if sens_complete is not None else None,
            verdict=verdict_,
            deployment_status=deployment_status_,
            evidence_summary=evidence_summary_,
        )
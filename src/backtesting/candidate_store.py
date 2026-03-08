"""
candidate_store.py — SQLite persistence layer for the backtesting framework.

Architecture:
- WAL mode + foreign keys + NORMAL synchronous (safe with WAL)
- Single-writer queue: callers call write_* methods which enqueue; one daemon
  thread drains the queue and performs all SQLite writes sequentially.
  This eliminates write contention entirely.
- All reads are direct (no queue) — WAL allows concurrent readers.

Source of truth: docs/backtesting/SQLITE_SCHEMA.md

B9H-002: _write_wfo_window_result now uses a deterministic result_id derived
from SHA-256(run_id + candidate_id + window_id). This makes INSERT OR REPLACE
actually deduplicate: repeated writes for the same candidate+window within a
run correctly update the existing row rather than appending a new one.
Previously, a fresh uuid4() was used as PK on every call, so OR REPLACE never
triggered and duplicate rows accumulated silently in wfo_window_results.
"""
from __future__ import annotations

import hashlib
import json
import logging
import queue
import sqlite3
import threading
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.backtesting.contracts import (
    CandidateParameterSet,
    CandidateRecord,
    CandidateResult,
    CandidateStage,
    Checkpoint,
    DeploymentStatus,
    MCMode,
    MCResult,
    RunMetadata,
    SensitivityProfile,
    Verdict,
    VerdictResult,
    WFOConsistencyScore,
    WFOWindowResult,
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
    window_collapse_flag        INTEGER,
    median_oos_delta            REAL
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
    return datetime.now(UTC).isoformat()


def _wfo_result_id(run_id: str, candidate_id: str, window_id: str) -> str:
    """
    Deterministic result_id for wfo_window_results rows.

    Derived from SHA-256(run_id + candidate_id + window_id), truncated to
    32 hex chars. Guarantees that INSERT OR REPLACE deduplicates correctly:
    repeated writes for the same (run, candidate, window) triple update the
    existing row rather than appending a new one.

    B9H-002: replaces the previous uuid4() approach which caused silent row
    accumulation because a fresh PK was generated on every call, so the
    REPLACE clause in INSERT OR REPLACE never triggered.
    """
    key = f"{run_id}:{candidate_id}:{window_id}"
    return hashlib.sha256(key.encode()).hexdigest()[:32]


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
        self._queue.join()

    def write_candidate(self, record: CandidateRecord) -> None:
        """Non-blocking enqueue. Worker-safe. Returns immediately."""
        self._queue.put(("_write_candidate_record", record))

    def write_candidate_stub(
        self,
        candidate: CandidateParameterSet,
        run_id: str,
        stage: str = "GA",
        generation: Optional[int] = None,
    ) -> None:
        """
        Ensure a candidate row exists in `candidates` + `candidate_parameters`.
        Does NOT write an evaluations row. Safe to call for seed candidates
        already in the DB (INSERT OR IGNORE is a no-op for existing rows).

        Used by ga_engine before writing WFO window results for offspring
        candidates that have not yet been persisted. Without this call,
        write_wfo_window_result raises FOREIGN KEY constraint failed because
        the candidate_id does not exist in the candidates table.

        Non-blocking — enqueued to the single writer thread.
        """
        self._queue.put(("_write_candidate_stub", (candidate, run_id, stage, generation)))

    def set_checkpoint(self, run_id: str, checkpoint: Checkpoint) -> None:
        """Update checkpoint in runs table. Blocks until written."""
        self._queue.put(("_set_checkpoint", (run_id, checkpoint)))
        self._queue.join()

    def write_wfo_window_result(self, result: WFOWindowResult, run_id: str) -> None:
        """Enqueue a WFOWindowResult write. Non-blocking."""
        self._queue.put(("_write_wfo_window_result", (result, run_id)))

    def flag_candidate_wfo_insufficient(self, candidate_id: str, run_id: str) -> None:
        """
        Flag a candidate as WFO_INSUFFICIENT_WINDOWS. Non-blocking.

        Writes a sentinel wfo_consistency_scores row (windows_evaluated=0,
        window_collapse_flag=1) so the candidate is identifiable in queries
        and excluded from Stages 5+. INSERT OR IGNORE — safe if a valid score
        row already exists (no-op in that case).
        """
        self._queue.put(("_flag_wfo_insufficient", (candidate_id, run_id)))

    def write_wfo_consistency_score(self, score: WFOConsistencyScore, run_id: str) -> None:
        """Enqueue a WFOConsistencyScore write. Non-blocking."""
        self._queue.put(("_write_wfo_consistency_score", (score, run_id)))

    def write_mc_result(self, result: MCResult, run_id: str) -> None:
        """Enqueue an MCResult write. Non-blocking."""
        self._queue.put(("_write_mc_result", (result, run_id)))

    def write_sensitivity_profile(self, profile: SensitivityProfile, run_id: str) -> None:
        """Enqueue a SensitivityProfile write (summary + per-step results). Non-blocking."""
        self._queue.put(("_write_sensitivity_profile", (profile, run_id)))

    def write_verdict(self, verdict: VerdictResult, run_id: str) -> None:
        """Enqueue a VerdictResult write. Non-blocking."""
        self._queue.put(("_write_verdict", (verdict, run_id)))

    def flush(self) -> None:
        """Block until the write queue is fully drained."""
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
    
    def get_incomplete_run(self, config_hash: str) -> Optional[str]:
        """
        Return run_id of the most recent incomplete run with the given config_hash,
        or None if no such run exists.

        An incomplete run is any run where checkpoint != COMPLETE.
        Used by _resume_or_start() to detect resumable runs without opening a
        second SQLite connection.

        B8-009: Replaces raw sqlite3.connect() in orchestrator._resume_or_start()
        which bypassed CandidateStore and accessed _db_path directly.
        """
        row = self._conn.execute(
            "SELECT run_id FROM runs WHERE config_hash = ? AND checkpoint != ? "
            "ORDER BY started_at DESC LIMIT 1",
            (config_hash, Checkpoint.COMPLETE.name),
        ).fetchone()
        return row[0] if row is not None else None

    def get_any_incomplete_run(self) -> Optional[tuple]:
        """
        Return (run_id, config_hash) of any incomplete run, or None.

        Used by _resume_or_start() to detect config hash conflicts:
        if a different incomplete run exists when starting a new run,
        the operator must resolve it before continuing.

        B8-009: Replaces the second raw sqlite3 query in orchestrator._resume_or_start().
        """
        row = self._conn.execute(
            "SELECT run_id, config_hash FROM runs WHERE checkpoint != ? LIMIT 1",
            (Checkpoint.COMPLETE.name,),
        ).fetchone()
        return (row[0], row[1]) if row is not None else None

    def get_wfo_consistency_score(self, candidate_id: str) -> Optional[WFOConsistencyScore]:
        """Return the WFOConsistencyScore for a candidate, or None if not found."""
        row = self._conn.execute(
            """SELECT candidate_id, windows_evaluated, windows_total,
                      median_window_return, window_return_variance,
                      worst_window_drawdown, fraction_positive_windows,
                      wfo_consistency_score, oos_gate_triggered, window_collapse_flag,
                      median_oos_delta
               FROM wfo_consistency_scores WHERE candidate_id = ?""",
            (candidate_id,),
        ).fetchone()
        if row is None:
            return None
        (
            cid, windows_evaluated, windows_total,
            median_return, variance, worst_dd, frac_pos,
            composite, oos_gate, collapse, median_oos_delta,
        ) = row
        return WFOConsistencyScore(
            candidate_id=cid,
            windows_evaluated=windows_evaluated or 0,
            windows_total=windows_total or 0,
            median_window_return=median_return or 0.0,
            window_return_variance=variance or 0.0,
            worst_window_drawdown=worst_dd or 0.0,
            fraction_positive_windows=frac_pos or 0.0,
            composite_score=composite or 0.0,
            oos_gate_triggered=bool(oos_gate) if oos_gate is not None else False,
            window_collapse_flag=bool(collapse) if collapse is not None else False,
            median_oos_delta=median_oos_delta,
        )

    def get_mc_result(self, candidate_id: str, mode: MCMode) -> Optional[MCResult]:
        """Return the MCResult for a candidate and mode, or None if not found."""
        row = self._conn.execute(
            """SELECT candidate_id, mode, perturbation_profile_name, iterations,
                      recorded_at, avg_final_equity, worst_drawdown_across_paths,
                      ruin_probability, p5_final_equity, evaluation_error
               FROM mc_results WHERE candidate_id = ? AND mode = ?""",
            (candidate_id, mode.value),
        ).fetchone()
        if row is None:
            return None
        (
            cid, mode_str, profile_name, iterations, recorded_at,
            avg_equity, worst_dd, ruin_prob, p5_equity, error,
        ) = row
        return MCResult(
            candidate_id=cid,
            mode=MCMode(mode_str),
            perturbation_profile_name=profile_name,
            iterations=iterations,
            evaluated_at=datetime.fromisoformat(recorded_at),
            avg_final_equity=avg_equity,
            worst_drawdown_across_paths=worst_dd,
            ruin_probability=ruin_prob,
            p5_final_equity=p5_equity,
            error=error,
        )

    def get_sensitivity_profile(self, candidate_id: str) -> Optional[SensitivityProfile]:
        """Return the SensitivityProfile summary for a candidate, or None if not found."""
        from src.backtesting.contracts import ParameterSensitivity

        row = self._conn.execute(
            """SELECT candidate_id, baseline_fitness, spike_detected,
                      spike_parameters, profile_complete
               FROM sensitivity_profiles WHERE candidate_id = ?""",
            (candidate_id,),
        ).fetchone()
        if row is None:
            return None
        cid, baseline, spike_det, spike_params_json, complete = row

        step_rows = self._conn.execute(
            """SELECT parameter_name, step, perturbed_value, fitness_delta, evaluation_error
               FROM sensitivity_results WHERE candidate_id = ?
               ORDER BY parameter_name, step""",
            (candidate_id,),
        ).fetchall()

        sensitivities = tuple(
            ParameterSensitivity(
                parameter_name=r[0],
                step=r[1],
                perturbed_value=r[2],
                fitness_delta=r[3],
                evaluation_error=r[4],
            )
            for r in step_rows
        )

        spike_parameters: tuple = ()
        if spike_params_json:
            try:
                spike_parameters = tuple(json.loads(spike_params_json))
            except (json.JSONDecodeError, TypeError):
                spike_parameters = ()

        return SensitivityProfile(
            candidate_id=cid,
            baseline_fitness=baseline,
            parameter_sensitivities=sensitivities,
            spike_detected=bool(spike_det),
            spike_parameters=spike_parameters,
            profile_complete=bool(complete),
        )

    def get_candidate_result(self, candidate_id: str) -> Optional[CandidateResult]:
        """
        Return a CandidateResult reconstructed from the evaluations table.
        Returns the first RANDOM or GA stage evaluation that passed constraints.
        Returns None if not found.
        """
        row = self._conn.execute(
            """SELECT candidate_id, recorded_at, fitness_score, actual_total_trades, error_message
               FROM evaluations
               WHERE candidate_id = ?
                 AND stage IN ('RANDOM', 'GA')
                 AND passed_constraints = 1
               ORDER BY recorded_at ASC LIMIT 1""",
            (candidate_id,),
        ).fetchone()
        if row is None:
            return None
        cid, recorded_at, fitness_score, total_trades, error = row
        return CandidateResult(
            candidate_id=cid,
            evaluated_at=datetime.fromisoformat(recorded_at),
            metrics=None,     # metrics are not persisted in column form; used structurally
            trades=None,
            total_trades=total_trades,
            error=error,
        )

    def get_fitness_score(self, candidate_id: str) -> Optional[float]:
        """
        Return the fitness score from the candidate's first RANDOM or GA evaluation.
        Returns None if not found.
        """
        row = self._conn.execute(
            """SELECT fitness_score FROM evaluations
               WHERE candidate_id = ? AND stage IN ('RANDOM', 'GA') AND passed_constraints = 1
               ORDER BY recorded_at ASC LIMIT 1""",
            (candidate_id,),
        ).fetchone()
        return row[0] if row is not None else None

    def rank_by_wfo(self, run_id: str, top_n: int) -> List[Dict[str, Any]]:
        """
        Return the top-N candidates by WFO consistency score for a given run.
        Each returned dict has: candidate_id, zone_name, wfo_consistency_score,
        parameters (as dict), generation.
        Candidates with no WFO score are excluded.
        """
        rows = self._conn.execute(
            """SELECT c.candidate_id, c.zone_name, c.generation,
                      wcs.wfo_consistency_score, cp.parameters_json
               FROM wfo_consistency_scores wcs
               JOIN candidates c ON wcs.candidate_id = c.candidate_id
               LEFT JOIN candidate_parameters cp ON c.candidate_id = cp.candidate_id
               WHERE wcs.run_id = ? AND wcs.wfo_consistency_score IS NOT NULL
               ORDER BY wcs.wfo_consistency_score DESC
               LIMIT ?""",
            (run_id, top_n),
        ).fetchall()

        result = []
        for row in rows:
            cid, zone_name, generation, wfo_score, params_json = row
            try:
                params = json.loads(params_json) if params_json else {}
            except (json.JSONDecodeError, TypeError):
                params = {}
            result.append({
                "candidate_id": cid,
                "zone_name": zone_name,
                "generation": generation,
                "wfo_consistency_score": wfo_score,
                "parameters": params,
                "parameters_json": params_json or "{}",
            })
        return result

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
                wcs.oos_gate_triggered, wcs.window_collapse_flag, wcs.median_oos_delta,
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

    def query_verdicts(self, run_id: str) -> List[Dict[str, Any]]:
        """Return all verdict rows for a run as plain dicts."""
        rows = self._conn.execute(
            """SELECT candidate_id, scenario_name, verdict, deployment_status,
                      wfo_consistency_score, mc_deep_ruin_probability,
                      sensitivity_spike, oos_gate_triggered, window_collapse_flag,
                      sensitivity_profile_incomplete, yaml_output_path, evidence_summary
               FROM verdicts WHERE run_id = ?
               ORDER BY wfo_consistency_score DESC""",
            (run_id,),
        ).fetchall()
        keys = [
            "candidate_id", "scenario_name", "verdict", "deployment_status",
            "wfo_consistency_score", "mc_deep_ruin_probability",
            "sensitivity_spike", "oos_gate_triggered", "window_collapse_flag",
            "sensitivity_profile_incomplete", "yaml_output_path", "evidence_summary",
        ]
        return [dict(zip(keys, row)) for row in rows]

    def query_wfo_consistency_scores(self, run_id: str) -> List[Dict[str, Any]]:
        """Return all WFO consistency score rows for a run as plain dicts."""
        rows = self._conn.execute(
            """SELECT candidate_id, wfo_consistency_score, median_window_return,
                      window_return_variance, worst_window_drawdown,
                      fraction_positive_windows, windows_evaluated, windows_total,
                      oos_gate_triggered, window_collapse_flag
               FROM wfo_consistency_scores WHERE run_id = ?
               ORDER BY wfo_consistency_score DESC""",
            (run_id,),
        ).fetchall()
        keys = [
            "candidate_id", "wfo_consistency_score", "median_window_return",
            "window_return_variance", "worst_window_drawdown", "fraction_positive_windows",
            "windows_evaluated", "windows_total", "oos_gate_triggered", "window_collapse_flag",
        ]
        return [dict(zip(keys, row)) for row in rows]

    def query_mc_results(self, run_id: str, mode: str) -> List[Dict[str, Any]]:
        """Return all MC result rows for a run and mode as plain dicts."""
        rows = self._conn.execute(
            """SELECT candidate_id, mode, ruin_probability, avg_final_equity,
                      worst_drawdown_across_paths, p5_final_equity, iterations, evaluation_error
               FROM mc_results WHERE run_id = ? AND mode = ?
               ORDER BY ruin_probability ASC""",
            (run_id, mode),
        ).fetchall()
        keys = [
            "candidate_id", "mode", "ruin_probability", "avg_final_equity",
            "worst_drawdown_across_paths", "p5_final_equity", "iterations", "evaluation_error",
        ]
        return [dict(zip(keys, row)) for row in rows]

    def query_sensitivity_profiles(self, run_id: str) -> List[Dict[str, Any]]:
        """Return all sensitivity profile summary rows for a run as plain dicts."""
        rows = self._conn.execute(
            """SELECT candidate_id, baseline_fitness, spike_detected,
                      spike_parameters, profile_complete
               FROM sensitivity_profiles WHERE run_id = ?""",
            (run_id,),
        ).fetchall()
        keys = ["candidate_id", "baseline_fitness", "spike_detected",
                "spike_parameters", "profile_complete"]
        return [dict(zip(keys, row)) for row in rows]

    def query_sensitivity_results(self, candidate_id: str) -> List[Dict[str, Any]]:
        """Return all per-step sensitivity results for a candidate as plain dicts."""
        rows = self._conn.execute(
            """SELECT parameter_name, step, perturbed_value, baseline_fitness,
                      perturbed_fitness, fitness_delta, is_spike, evaluation_error
               FROM sensitivity_results WHERE candidate_id = ?
               ORDER BY parameter_name, step""",
            (candidate_id,),
        ).fetchall()
        keys = [
            "parameter_name", "step", "perturbed_value", "baseline_fitness",
            "perturbed_fitness", "fitness_delta", "is_spike", "evaluation_error",
        ]
        return [dict(zip(keys, row)) for row in rows]

    def query_wfo_window_results(self, candidate_id: str) -> List[Dict[str, Any]]:
        """Return all WFO window results for a candidate as plain dicts."""
        rows = self._conn.execute(
            """SELECT window_id, fitness_score, total_trades, net_pnl,
                      max_drawdown, win_rate, expectancy, profit_factor, oos_delta, evaluation_error
               FROM wfo_window_results WHERE candidate_id = ?
               ORDER BY window_id""",
            (candidate_id,),
        ).fetchall()
        keys = [
            "window_id", "fitness_score", "total_trades", "net_pnl",
            "max_drawdown", "win_rate", "expectancy", "profit_factor",
            "oos_delta", "evaluation_error",
        ]
        return [dict(zip(keys, row)) for row in rows]

    def close(self) -> None:
        """Flush pending writes, stop the writer thread, close connection."""
        self._queue.join()
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
                run_metadata.checkpoint.name,
            ),
        )
        self._conn.commit()
        logger.debug("Run row written: %s", run_metadata.run_id)

    def _set_checkpoint(self, args: tuple) -> None:
        run_id, checkpoint = args
        self._conn.execute(
            "UPDATE runs SET checkpoint = ? WHERE run_id = ?",
            (checkpoint.name, run_id),
        )
        self._conn.commit()
        logger.debug("Checkpoint updated: run=%s  checkpoint=%s", run_id, checkpoint.name)

    def _write_candidate_record(self, record: CandidateRecord) -> None:
        """Write one CandidateRecord: upsert candidates + parameters, insert evaluation."""
        params = json.loads(record.parameters_json)

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

    def _write_candidate_stub(self, args: tuple) -> None:
        """
        Write candidates + candidate_parameters rows only. No evaluations row.
        INSERT OR IGNORE — safe to call for candidates already in the DB (no-op).
        Called by writer thread only.
        """
        candidate, run_id, stage, generation = args
        params = candidate.parameters
        params_json = json.dumps(params, sort_keys=True, default=str)

        self._conn.execute(
            """INSERT OR IGNORE INTO candidates
               (candidate_id, run_id, zone_name, generation, origin_stage, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                candidate.candidate_id,
                run_id,
                candidate.zone_name,
                generation if generation is not None else candidate.generation,
                stage,
                _now_ts(),
            ),
        )

        self._conn.execute(
            """INSERT OR IGNORE INTO candidate_parameters
               (candidate_id, parameters_json,
                rsi_period, rsi_overbought, rsi_oversold, adx_threshold,
                atr_length, atr_multiplier, rr_target, risk_percentile,
                strategy_tf, htf_tf, session_filter)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                candidate.candidate_id,
                params_json,
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
        self._conn.commit()
        logger.debug(
            "Candidate stub written: candidate=%s  stage=%s  gen=%s",
            candidate.candidate_id[:12], stage, generation,
        )

    def _write_wfo_window_result(self, args: tuple) -> None:
        """
        Write one WFOWindowResult row. Called by writer thread only.

        B9H-002: Uses a deterministic result_id via _wfo_result_id() so that
        INSERT OR REPLACE correctly deduplicates on (run_id, candidate_id,
        window_id). Previously, a fresh uuid4() was used on every call, which
        caused duplicate rows to accumulate silently because the REPLACE clause
        never triggered (new PK = new row, always).
        """
        result, run_id = args
        result_id = _wfo_result_id(run_id, result.candidate_id, result.window_id)
        self._conn.execute(
            """INSERT OR REPLACE INTO wfo_window_results (
                result_id, candidate_id, run_id, window_id,
                is_ga_fitness_window, ga_generation, recorded_at,
                fitness_score, total_trades, net_pnl, max_drawdown,
                win_rate, expectancy, profit_factor, oos_delta, evaluation_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                result_id,
                result.candidate_id,
                run_id,
                result.window_id,
                0,       # is_ga_fitness_window — always 0; write API has no generation context
                None,    # ga_generation — always None for same reason
                _ts(result.evaluated_at),
                result.fitness_score,
                result.total_trades,
                result.net_pnl,
                result.max_drawdown,
                result.win_rate,
                result.expectancy,
                result.profit_factor,
                result.oos_delta,
                result.error,
            ),
        )
        self._conn.commit()
        logger.debug(
            "WFOWindowResult written: candidate=%s window=%s fitness=%s",
            result.candidate_id[:12],
            result.window_id,
            f"{result.fitness_score:.4f}" if result.fitness_score is not None else "N/A",
        )

    def _flag_wfo_insufficient(self, args: tuple) -> None:
        """
        Write a sentinel wfo_consistency_scores row for a candidate that failed
        >50% of WFO windows. windows_evaluated=0, window_collapse_flag=1.
        INSERT OR IGNORE — no-op if a valid score row already exists.
        Called by writer thread only.
        """
        candidate_id, run_id = args
        self._conn.execute(
            """INSERT OR IGNORE INTO wfo_consistency_scores (
                candidate_id, run_id, recorded_at,
                median_window_return, window_return_variance,
                worst_window_drawdown, fraction_positive_windows,
                wfo_consistency_score, windows_evaluated, windows_total,
                oos_gate_triggered, window_collapse_flag
            ) VALUES (?, ?, ?, 0.0, 0.0, 1.0, 0.0, 0.0, 0, 0, 0, 1)""",
            (candidate_id, run_id, _now_ts()),
        )
        self._conn.commit()
        logger.warning(
            "Candidate %s flagged WFO_INSUFFICIENT_WINDOWS (run=%s)",
            candidate_id[:12], run_id,
        )

    def _write_wfo_consistency_score(self, args: tuple) -> None:
        score, run_id = args
        self._conn.execute(
            """INSERT OR REPLACE INTO wfo_consistency_scores (
                candidate_id, run_id, recorded_at,
                median_window_return, window_return_variance,
                worst_window_drawdown, fraction_positive_windows,
                wfo_consistency_score, windows_evaluated, windows_total,
                oos_gate_triggered, window_collapse_flag, median_oos_delta
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                score.candidate_id,
                run_id,
                _now_ts(),
                score.median_window_return,
                score.window_return_variance,
                score.worst_window_drawdown,
                score.fraction_positive_windows,
                score.composite_score,
                score.windows_evaluated,
                score.windows_total,
                int(score.oos_gate_triggered),
                int(score.window_collapse_flag),
                score.median_oos_delta,
            ),
        )
        self._conn.commit()
        logger.debug("WFO score written: candidate=%s  score=%.4f",
                     score.candidate_id[:12], score.composite_score)

    def _write_mc_result(self, args: tuple) -> None:
        result, run_id = args
        self._conn.execute(
            """INSERT OR REPLACE INTO mc_results (
                result_id, candidate_id, run_id, mode, perturbation_profile_name,
                iterations, recorded_at, avg_final_equity, worst_drawdown_across_paths,
                ruin_probability, p5_final_equity, evaluation_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                str(uuid.uuid4()),
                result.candidate_id,
                run_id,
                result.mode.value,
                result.perturbation_profile_name,
                result.iterations,
                _ts(result.evaluated_at),
                result.avg_final_equity,
                result.worst_drawdown_across_paths,
                result.ruin_probability,
                result.p5_final_equity,
                result.error,
            ),
        )
        self._conn.commit()
        logger.debug("MCResult written: candidate=%s  mode=%s  ruin=%s",
                     result.candidate_id[:12], result.mode.value,
                     f"{result.ruin_probability:.4f}" if result.ruin_probability is not None else "N/A")

    def _write_sensitivity_profile(self, args: tuple) -> None:
        profile, run_id = args
        now = _now_ts()

        for ps in profile.parameter_sensitivities:
            is_spike = (
                ps.parameter_name in profile.spike_parameters
                if ps.fitness_delta is not None else False
            )
            perturbed_fitness = (
                profile.baseline_fitness + ps.fitness_delta
                if ps.fitness_delta is not None else None
            )
            self._conn.execute(
                """INSERT OR REPLACE INTO sensitivity_results (
                    result_id, candidate_id, run_id, parameter_name, step,
                    perturbed_value, baseline_fitness, perturbed_fitness,
                    fitness_delta, is_spike, recorded_at, evaluation_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    str(uuid.uuid4()),
                    profile.candidate_id,
                    run_id,
                    ps.parameter_name,
                    ps.step,
                    str(ps.perturbed_value),
                    profile.baseline_fitness,
                    perturbed_fitness,
                    ps.fitness_delta,
                    int(is_spike),
                    now,
                    ps.evaluation_error,
                ),
            )

        self._conn.execute(
            """INSERT OR REPLACE INTO sensitivity_profiles (
                candidate_id, run_id, baseline_fitness, spike_detected,
                spike_parameters, profile_complete, recorded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                profile.candidate_id,
                run_id,
                profile.baseline_fitness,
                int(profile.spike_detected),
                json.dumps(list(profile.spike_parameters)) if profile.spike_parameters else None,
                int(profile.profile_complete),
                now,
            ),
        )
        self._conn.commit()
        logger.debug("SensitivityProfile written: candidate=%s  spike=%s  complete=%s",
                     profile.candidate_id[:12], profile.spike_detected, profile.profile_complete)

    def _write_verdict(self, args: tuple) -> None:
        verdict, run_id = args
        evidence_json = json.dumps({
            "verdict": verdict.verdict.value,
            "deployment_status": verdict.deployment_status.value,
            "wfo_consistency_score": verdict.wfo_consistency_score,
            "mc_deep_ruin_probability": verdict.mc_deep_ruin_probability,
            "sensitivity_spike": verdict.sensitivity_spike,
            "oos_gate_triggered": verdict.oos_gate_triggered,
            "window_collapse_flag": verdict.window_collapse_flag,
            "sensitivity_profile_incomplete": verdict.sensitivity_profile_incomplete,
            "median_oos_delta": verdict.median_oos_delta,
            "parameter_region_width": verdict.parameter_region_width,
            "yaml_output_path": verdict.yaml_output_path,
        })
        self._conn.execute(
            """INSERT OR REPLACE INTO verdicts (
                candidate_id, run_id, scenario_name, verdict, deployment_status,
                wfo_consistency_score, mc_deep_ruin_probability,
                sensitivity_spike, oos_gate_triggered, window_collapse_flag,
                sensitivity_profile_incomplete, median_oos_delta, parameter_region_width,
                yaml_output_path, evidence_summary, evidence_json, recorded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                verdict.candidate_id,
                run_id,
                verdict.scenario_name,
                verdict.verdict.value,
                verdict.deployment_status.value,
                verdict.wfo_consistency_score,
                verdict.mc_deep_ruin_probability,
                int(verdict.sensitivity_spike),
                int(verdict.oos_gate_triggered),
                int(verdict.window_collapse_flag),
                int(verdict.sensitivity_profile_incomplete),
                verdict.median_oos_delta,
                verdict.parameter_region_width,
                verdict.yaml_output_path,
                verdict.evidence_summary,
                evidence_json,
                _now_ts(),
            ),
        )
        self._conn.commit()
        logger.debug("Verdict written: candidate=%s  verdict=%s",
                     verdict.candidate_id[:12], verdict.verdict.value)

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
            wfo_score, wfo_evaluated, wfo_oos_gate, wfo_collapse, wfo_median_oos_delta,
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
            wfo_median_oos_delta=wfo_median_oos_delta,
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
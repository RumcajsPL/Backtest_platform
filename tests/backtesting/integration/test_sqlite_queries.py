"""
test_sqlite_queries.py — SQLite query validation suite.

Validates that all 10 representative queries from SQLITE_SCHEMA.md execute
correctly against a real populated database. Each query must:
  - Execute without error
  - Return the expected row count / structure
  - Use the correct columns (no schema drift)

The DB is seeded with a realistic two-candidate run (cand_a = AUTO_GO-eligible,
cand_b = borderline-eligible) covering all 9 tables.

Queries tested (from SQLITE_SCHEMA.md):
  Q1  — Full pipeline funnel grouped by stage
  Q2  — Top 10 candidates by fitness in RANDOM stage
  Q3  — MC pre-filter pass with ruin < 10%
  Q4  — WFO consistency scores for all full-WFO candidates
  Q5  — Parameter sensitivity spikes for a specific candidate
  Q6  — Final verdicts with evidence + parameters
  Q7  — ML feature matrix (go/borderline only)
  Q8  — Parameter region analysis for auto_go verdicts
  Q9  — Window-by-window performance for a candidate
  Q10 — Run history summary across multiple runs
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List

import pytest

from src.backtesting.candidate_store import CandidateStore
from src.backtesting.contracts import (
    CandidateParameterSet,
    CandidateRecord,
    CandidateStage,
    Checkpoint,
    MCMode,
    MCResult,
    ParameterSensitivity,
    RunMetadata,
    SensitivityProfile,
    Verdict,
    VerdictResult,
    WFOConsistencyScore,
)
from src.backtesting.orchestrator import BACKTESTER_VERSION

# ── Seed helpers ──────────────────────────────────────────────────────────────

def _make_run_metadata(run_id: str, checkpoint: Checkpoint = Checkpoint.COMPLETE) -> RunMetadata:
    return RunMetadata(
        run_id=run_id,
        config_hash="b" * 64,
        scenario_name="capital_accumulation",
        started_at=datetime.now(UTC),
        perturbation_profile_name="default",
        random_search_seed=42,
        ga_seed=43,
        mc_prefilter_seed=44,
        mc_deep_seed=45,
        sensitivity_seed=46,
        wfo_window_ids=("W1", "W2", "W3"),
        checkpoint=checkpoint,
        backtester_version=BACKTESTER_VERSION,
    )


def _make_candidate(seed: int = 0) -> CandidateParameterSet:
    return CandidateParameterSet.create(
        zone_name="safe",
        parameters={
            "rsi_period": 14 + seed,
            "rsi_overbought": 70,
            "rsi_oversold": 30,
            "adx_threshold": 25,
            "atr_length": 14,
            "atr_multiplier": 2.0 + seed * 0.25,
            "rr_target": 2.0,
            "risk_percentile": 0.5,
            "strategy_tf": "H1",
            "htf_tf": "D1",
            "session_filter": "london",
        },
        generation=None,
    )


def _write_candidate_row(
    store: CandidateStore,
    run_id: str,
    candidate: CandidateParameterSet,
    fitness: float,
    stage: str = CandidateStage.RANDOM.value,
) -> None:
    params_json = json.dumps(candidate.parameters, sort_keys=True)
    record = CandidateRecord(
        run_id=run_id,
        candidate_id=candidate.candidate_id,
        zone_name=candidate.zone_name,
        stage=stage,
        generation=None,
        recorded_at=datetime.now(UTC),
        parameters_json=params_json,
        fitness_score=fitness,
        passed_constraints=True,
        rejection_reason=None,
        failing_constraint=None,
        failing_value=None,
        actual_win_rate=0.52,
        actual_max_drawdown=0.08,
        actual_losing_streak=3,
        actual_trades_per_week=4.5,
        actual_expectancy=0.65,
        actual_profit_factor=1.45,
        wfo_median_window_return=0.035,
        wfo_window_return_variance=0.002,
        wfo_worst_window_drawdown=0.09,
        wfo_fraction_positive_windows=0.80,
        wfo_consistency_score=0.68,
        wfo_windows_evaluated=3,
        wfo_oos_gate_triggered=False,
        wfo_window_collapse_flag=False,
        mc_prefilter_ruin_probability=None,
        mc_prefilter_avg_final_equity=None,
        mc_prefilter_iterations=None,
        mc_deep_ruin_probability=None,
        mc_deep_avg_final_equity=None,
        mc_deep_worst_drawdown=None,
        mc_deep_p5_final_equity=None,
        mc_deep_iterations=None,
        sensitivity_spike_detected=None,
        sensitivity_spike_parameters=None,
        sensitivity_profile_complete=None,
        verdict=None,
        deployment_status=None,
        evidence_summary=None,
    )
    store.write_candidate(record)


def _write_wfo_score(
    store: CandidateStore, run_id: str, candidate_id: str, score: float
) -> None:
    store.write_wfo_consistency_score(
        WFOConsistencyScore(
            candidate_id=candidate_id,
            windows_evaluated=3,
            windows_total=3,
            median_window_return=0.035,
            window_return_variance=0.002,
            worst_window_drawdown=0.09,
            fraction_positive_windows=0.80,
            composite_score=score,
            oos_gate_triggered=False,
            window_collapse_flag=False,
        ),
        run_id,
    )


def _write_mc_prefilter(
    conn: sqlite3.Connection, run_id: str, candidate_id: str, ruin: float
) -> None:
    """Write MC pre-filter row directly — CandidateStore only exposes deep mode write."""
    conn.execute(
        """INSERT OR REPLACE INTO mc_results (
            result_id, candidate_id, run_id, mode, perturbation_profile_name,
            iterations, recorded_at, avg_final_equity, worst_drawdown_across_paths,
            ruin_probability, p5_final_equity, evaluation_error
        ) VALUES (?, ?, ?, 'pre_filter', 'default', 50, ?, 10800.0, 0.11, ?, 9500.0, NULL)""",
        (str(uuid.uuid4()), candidate_id, run_id, datetime.now(UTC).isoformat(), ruin),
    )
    conn.commit()


def _write_mc_deep(
    store: CandidateStore, run_id: str, candidate_id: str, ruin: float
) -> None:
    store.write_mc_result(
        MCResult(
            candidate_id=candidate_id,
            mode=MCMode.DEEP,
            perturbation_profile_name="default",
            iterations=100,
            evaluated_at=datetime.now(UTC),
            avg_final_equity=11200.0,
            worst_drawdown_across_paths=0.12,
            ruin_probability=ruin,
            p5_final_equity=9800.0,
            error=None,
        ),
        run_id,
    )


def _write_sensitivity(
    store: CandidateStore, run_id: str, candidate_id: str, spike: bool = False
) -> None:
    ps = ParameterSensitivity(
        parameter_name="rsi_period",
        step=1,
        perturbed_value=16,
        fitness_delta=0.18 if spike else 0.04,
        evaluation_error=None,
    )
    store.write_sensitivity_profile(
        SensitivityProfile(
            candidate_id=candidate_id,
            baseline_fitness=0.72,
            parameter_sensitivities=(ps,),
            spike_detected=spike,
            spike_parameters=("rsi_period",) if spike else (),
            profile_complete=True,
        ),
        run_id,
    )


def _write_verdict(
    store: CandidateStore,
    run_id: str,
    candidate_id: str,
    verdict: Verdict,
    wfo_score: float,
    ruin: float,
) -> None:
    store.write_verdict(
        VerdictResult(
            candidate_id=candidate_id,
            scenario_name="capital_accumulation",
            verdict=verdict,
            deployment_status=__import__(
                "src.backtesting.contracts", fromlist=["DeploymentStatus"]
            ).DeploymentStatus.PAPER_TRADE_REQUIRED,
            wfo_consistency_score=wfo_score,
            mc_deep_ruin_probability=ruin,
            sensitivity_spike=False,
            oos_gate_triggered=False,
            window_collapse_flag=False,
            sensitivity_profile_incomplete=False,
            median_oos_delta=None,
            parameter_region_width=None,
            yaml_output_path=None,
            evidence_summary=(
                f"Scenario: capital_accumulation. Verdict: {verdict.value.upper()}. "
                f"WFO: {wfo_score:.3f}. Ruin: {ruin:.3f}."
            ),
        ),
        run_id,
    )


def _write_wfo_window_rows(conn: sqlite3.Connection, run_id: str, candidate_id: str) -> None:
    for i, window_id in enumerate(["W1", "W2", "W3"]):
        conn.execute(
            """INSERT OR IGNORE INTO wfo_window_results (
                result_id, candidate_id, run_id, window_id,
                is_ga_fitness_window, recorded_at,
                fitness_score, total_trades, net_pnl,
                max_drawdown, win_rate, expectancy, profit_factor,
                oos_delta, evaluation_error
            ) VALUES (?, ?, ?, ?, 0, ?, ?, 40, ?, ?, ?, ?, ?, ?, NULL)""",
            (
                str(uuid.uuid4()), candidate_id, run_id, window_id,
                datetime.now(UTC).isoformat(),
                0.65 + i * 0.02,
                600.0 + i * 50,
                0.07 + i * 0.005,
                0.51 + i * 0.01,
                0.60 + i * 0.02,
                1.40 + i * 0.05,
                -0.04 - i * 0.01,
            ),
        )
    conn.commit()


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def populated_db(tmp_path_factory: pytest.TempPathFactory):
    """
    Module-scoped fixture. Builds a fully populated DB with:
      - 2 runs (run_a fully complete, run_b for Q10 cross-run query)
      - 2 candidates in run_a: cand_a (AUTO_GO), cand_b (BORDERLINE)
      - All 9 tables populated for run_a
    Returns (db_path, run_id_a, cand_a, cand_b).
    """
    tmp = tmp_path_factory.mktemp("sqlite_queries")
    db_path = tmp / "test_queries.db"

    store = CandidateStore(db_path)

    # ── Run A ──────────────────────────────────────────────────────────────
    run_id_a = str(uuid.uuid4())
    meta_a = _make_run_metadata(run_id_a)
    store.initialise_run(meta_a)

    cand_a = _make_candidate(seed=0)
    cand_b = _make_candidate(seed=1)

    _write_candidate_row(store, run_id_a, cand_a, fitness=0.75)
    _write_candidate_row(store, run_id_a, cand_b, fitness=0.61)
    store.flush()

    _write_wfo_score(store, run_id_a, cand_a.candidate_id, score=0.70)
    _write_wfo_score(store, run_id_a, cand_b.candidate_id, score=0.55)
    store.flush()

    _write_mc_deep(store, run_id_a, cand_a.candidate_id, ruin=0.03)
    _write_mc_deep(store, run_id_a, cand_b.candidate_id, ruin=0.08)
    store.flush()

    _write_sensitivity(store, run_id_a, cand_a.candidate_id, spike=False)
    _write_sensitivity(store, run_id_a, cand_b.candidate_id, spike=False)
    store.flush()

    _write_verdict(store, run_id_a, cand_a.candidate_id, Verdict.AUTO_GO, 0.70, 0.03)
    _write_verdict(store, run_id_a, cand_b.candidate_id, Verdict.BORDERLINE, 0.55, 0.08)
    store.flush()

    store.set_checkpoint(run_id_a, Checkpoint.COMPLETE)

    # Direct inserts for tables not exposed by store public API at this phase
    conn = sqlite3.connect(str(db_path))
    try:
        _write_wfo_window_rows(conn, run_id_a, cand_a.candidate_id)
        _write_wfo_window_rows(conn, run_id_a, cand_b.candidate_id)
        _write_mc_prefilter(conn, run_id_a, cand_a.candidate_id, ruin=0.05)
        _write_mc_prefilter(conn, run_id_a, cand_b.candidate_id, ruin=0.07)
    finally:
        conn.close()

    # ── Run B (for Q10 cross-run) ──────────────────────────────────────────
    run_id_b = str(uuid.uuid4())
    meta_b = _make_run_metadata(run_id_b)
    store.initialise_run(meta_b)
    cand_c = _make_candidate(seed=2)
    _write_candidate_row(store, run_id_b, cand_c, fitness=0.68)
    store.flush()
    _write_verdict(store, run_id_b, cand_c.candidate_id, Verdict.AUTO_GO, 0.68, 0.04)
    store.flush()
    store.set_checkpoint(run_id_b, Checkpoint.COMPLETE)

    store.close()

    return db_path, run_id_a, cand_a, cand_b


@pytest.fixture(scope="module")
def conn(populated_db):
    """Module-scoped direct SQLite connection for raw query testing."""
    db_path, *_ = populated_db
    c = sqlite3.connect(str(db_path))
    c.execute("PRAGMA foreign_keys = ON")
    c.row_factory = sqlite3.Row
    yield c
    c.close()


# ── Query tests ───────────────────────────────────────────────────────────────

class TestSQLiteQueries:
    """
    Validates all 10 representative queries from SQLITE_SCHEMA.md.
    Each test executes the exact query from the spec against real data
    and asserts structure, row counts, and correctness of results.
    """

    def test_q1_pipeline_funnel_by_stage(self, conn, populated_db):
        """
        Q1: Full pipeline funnel for a run — grouped by stage with counts and avg fitness.
        Must return at least 1 stage row; RANDOM stage must show 2 candidates.
        """
        _, run_id, *_ = populated_db

        rows = conn.execute(
            """SELECT
                stage,
                COUNT(*) as total,
                SUM(passed_constraints) as passed,
                AVG(fitness_score) as avg_fitness
               FROM evaluations
               WHERE run_id = ?
               GROUP BY stage
               ORDER BY MIN(recorded_at)""",
            (run_id,),
        ).fetchall()

        assert len(rows) >= 1, "Q1: Expected at least 1 stage row"

        random_row = next((r for r in rows if r["stage"] == "RANDOM"), None)
        assert random_row is not None, "Q1: RANDOM stage must appear in funnel"
        assert random_row["total"] == 2, "Q1: 2 candidates expected in RANDOM stage"
        assert random_row["passed"] == 2, "Q1: Both RANDOM candidates should have passed_constraints=1"
        assert random_row["avg_fitness"] == pytest.approx((0.75 + 0.61) / 2, abs=1e-6)

    def test_q2_top_candidates_by_fitness_random_stage(self, conn, populated_db):
        """
        Q2: Top 10 by fitness in RANDOM stage with parameters joined.
        Must return 2 rows ordered by fitness DESC; top row must be cand_a.
        """
        _, run_id, cand_a, _ = populated_db

        rows = conn.execute(
            """SELECT
                c.candidate_id,
                cp.rsi_period, cp.atr_multiplier, cp.session_filter,
                e.fitness_score,
                e.actual_win_rate, e.actual_max_drawdown, e.actual_expectancy
               FROM evaluations e
               JOIN candidates c ON e.candidate_id = c.candidate_id
               JOIN candidate_parameters cp ON c.candidate_id = cp.candidate_id
               WHERE e.run_id = ? AND e.stage = 'RANDOM' AND e.passed_constraints = 1
               ORDER BY e.fitness_score DESC
               LIMIT 10""",
            (run_id,),
        ).fetchall()

        assert len(rows) == 2, "Q2: Expected 2 RANDOM candidates"
        assert rows[0]["candidate_id"] == cand_a.candidate_id, "Q2: cand_a should be top by fitness"
        assert rows[0]["fitness_score"] == pytest.approx(0.75)
        assert rows[0]["rsi_period"] == 14
        assert rows[0]["session_filter"] == "london"
        # All required columns must be present and non-null
        assert rows[0]["actual_win_rate"] is not None
        assert rows[0]["actual_max_drawdown"] is not None
        assert rows[0]["actual_expectancy"] is not None

    def test_q3_mc_prefilter_pass_ruin_under_10pct(self, conn, populated_db):
        """
        Q3: Candidates that passed MC Pre-Filter with ruin < 10%.
        Both seeded candidates have ruin 0.05 and 0.07 — both should appear.
        """
        _, run_id, *_ = populated_db

        rows = conn.execute(
            """SELECT
                c.candidate_id,
                mc.ruin_probability,
                mc.worst_drawdown_across_paths,
                e.fitness_score
               FROM mc_results mc
               JOIN candidates c ON mc.candidate_id = c.candidate_id
               JOIN evaluations e ON c.candidate_id = e.candidate_id AND e.stage = 'RANDOM'
               WHERE mc.run_id = ? AND mc.mode = 'pre_filter' AND mc.ruin_probability < 0.10
               ORDER BY mc.ruin_probability ASC""",
            (run_id,),
        ).fetchall()

        assert len(rows) == 2, "Q3: Both candidates have pre-filter ruin < 0.10"
        assert rows[0]["ruin_probability"] == pytest.approx(0.05)
        assert rows[1]["ruin_probability"] == pytest.approx(0.07)
        # MC columns present
        assert rows[0]["worst_drawdown_across_paths"] is not None

    def test_q4_wfo_consistency_scores(self, conn, populated_db):
        """
        Q4: WFO consistency for all full-WFO candidates, ordered by score DESC.
        Must return 2 rows; cand_a (score=0.70) must rank above cand_b (score=0.55).
        """
        _, run_id, cand_a, cand_b = populated_db

        rows = conn.execute(
            """SELECT
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
               WHERE wcs.run_id = ?
               ORDER BY wcs.wfo_consistency_score DESC""",
            (run_id,),
        ).fetchall()

        assert len(rows) == 2, "Q4: 2 WFO consistency rows expected"
        assert rows[0]["candidate_id"] == cand_a.candidate_id
        assert rows[0]["wfo_consistency_score"] == pytest.approx(0.70)
        assert rows[1]["wfo_consistency_score"] == pytest.approx(0.55)
        assert rows[0]["windows_evaluated"] == 3
        assert rows[0]["fraction_positive_windows"] == pytest.approx(0.80)
        assert rows[0]["window_collapse_flag"] == 0

    def test_q5_sensitivity_spikes_for_candidate(self, conn, populated_db):
        """
        Q5: Parameter sensitivity spikes for a specific candidate, ordered by |delta| DESC.
        cand_a has 1 sensitivity result (rsi_period, step=1, delta=0.04).
        """
        _, _, cand_a, _ = populated_db

        rows = conn.execute(
            """SELECT
                sr.parameter_name,
                sr.step,
                sr.perturbed_value,
                sr.baseline_fitness,
                sr.perturbed_fitness,
                sr.fitness_delta,
                sr.is_spike
               FROM sensitivity_results sr
               WHERE sr.candidate_id = ?
               ORDER BY ABS(sr.fitness_delta) DESC""",
            (cand_a.candidate_id,),
        ).fetchall()

        assert len(rows) >= 1, "Q5: Expected at least 1 sensitivity result for cand_a"
        assert rows[0]["parameter_name"] == "rsi_period"
        assert rows[0]["step"] == 1
        assert rows[0]["baseline_fitness"] == pytest.approx(0.72)
        assert rows[0]["fitness_delta"] == pytest.approx(0.04)
        assert rows[0]["is_spike"] == 0  # delta=0.04 < spike_threshold=0.15

    def test_q6_final_verdicts_with_evidence_and_parameters(self, conn, populated_db):
        """
        Q6: Final verdicts with evidence + parameter columns joined.
        Must return 2 rows; verdicts and evidence_summary must be populated.
        """
        _, run_id, cand_a, cand_b = populated_db

        rows = conn.execute(
            """SELECT
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
               WHERE v.run_id = ?
               ORDER BY v.verdict, v.wfo_consistency_score DESC""",
            (run_id,),
        ).fetchall()

        assert len(rows) == 2, "Q6: 2 verdict rows expected"

        verdict_map = {r["candidate_id"]: r for r in rows}
        assert verdict_map[cand_a.candidate_id]["verdict"] == "auto_go"
        assert verdict_map[cand_b.candidate_id]["verdict"] == "borderline"

        for r in rows:
            assert r["deployment_status"] == "PAPER_TRADE_REQUIRED"
            assert r["evidence_summary"] is not None and len(r["evidence_summary"]) > 0
            assert r["rsi_period"] is not None
            assert r["atr_multiplier"] is not None
            assert r["session_filter"] == "london"

    def test_q7_ml_feature_matrix_go_borderline_only(self, conn, populated_db):
        """
        Q7: ML feature matrix — all go/borderline candidates with full metric set.
        Must return 2 rows (cand_a=auto_go, cand_b=borderline); NO_GO excluded.
        All key feature columns must be present.
        """
        _, run_id, *_ = populated_db

        rows = conn.execute(
            """SELECT
                c.candidate_id,
                cp.rsi_period, cp.atr_multiplier, cp.session_filter,
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
               WHERE v.run_id = ? AND v.verdict != 'no_go'
               ORDER BY v.wfo_consistency_score DESC""",
            (run_id,),
        ).fetchall()

        assert len(rows) == 2, "Q7: 2 non-NO_GO rows expected"

        # All feature columns must be present and non-null for these well-seeded candidates
        for r in rows:
            assert r["fitness_score"] is not None
            assert r["actual_win_rate"] is not None
            assert r["wfo_consistency_score"] is not None
            assert r["ruin_probability"] is not None
            assert r["spike_detected"] is not None
            assert r["verdict"] in ("auto_go", "borderline")

        # Ordered by WFO score DESC — cand_a (0.70) before cand_b (0.55)
        assert rows[0]["wfo_consistency_score"] == pytest.approx(0.70)
        assert rows[1]["wfo_consistency_score"] == pytest.approx(0.55)

    def test_q8_parameter_region_analysis_auto_go(self, conn, populated_db):
        """
        Q8: Parameter region analysis — RSI/ATR/session combos in auto_go verdicts.
        cand_a is AUTO_GO with rsi_period=14, atr_multiplier=2.0, session=london.
        Must return 1 row with go_count=1.
        """
        _, run_id, cand_a, _ = populated_db

        rows = conn.execute(
            """SELECT
                cp.rsi_period,
                cp.atr_multiplier,
                cp.session_filter,
                COUNT(*) as go_count,
                AVG(v.wfo_consistency_score) as avg_wfo_score,
                AVG(v.mc_deep_ruin_probability) as avg_ruin_prob
               FROM verdicts v
               JOIN candidate_parameters cp ON v.candidate_id = cp.candidate_id
               WHERE v.run_id = ? AND v.verdict = 'auto_go'
               GROUP BY cp.rsi_period, cp.atr_multiplier, cp.session_filter
               ORDER BY go_count DESC, avg_wfo_score DESC""",
            (run_id,),
        ).fetchall()

        assert len(rows) == 1, "Q8: 1 distinct parameter region in auto_go verdicts"
        assert rows[0]["rsi_period"] == 14
        assert rows[0]["atr_multiplier"] == pytest.approx(2.0)
        assert rows[0]["session_filter"] == "london"
        assert rows[0]["go_count"] == 1
        assert rows[0]["avg_wfo_score"] == pytest.approx(0.70)
        assert rows[0]["avg_ruin_prob"] == pytest.approx(0.03)

    def test_q9_window_by_window_performance(self, conn, populated_db):
        """
        Q9: Window-by-window performance for a candidate (full WFO windows only).
        cand_a has 3 WFO window rows (W1, W2, W3, is_ga_fitness_window=0).
        Must return 3 rows ordered by window_id.
        """
        _, _, cand_a, _ = populated_db

        rows = conn.execute(
            """SELECT
                wwr.window_id,
                wwr.fitness_score,
                wwr.net_pnl,
                wwr.max_drawdown,
                wwr.win_rate,
                wwr.expectancy,
                wwr.total_trades,
                wwr.oos_delta
               FROM wfo_window_results wwr
               WHERE wwr.candidate_id = ?
                 AND wwr.is_ga_fitness_window = 0
               ORDER BY wwr.window_id""",
            (cand_a.candidate_id,),
        ).fetchall()

        assert len(rows) == 3, "Q9: 3 full WFO window rows expected for cand_a"
        assert [r["window_id"] for r in rows] == ["W1", "W2", "W3"]
        assert rows[0]["total_trades"] == 40
        assert rows[0]["net_pnl"] == pytest.approx(600.0)
        # oos_delta should be negative (forward-test degradation)
        for r in rows:
            assert r["oos_delta"] is not None
            assert r["oos_delta"] < 0

    def test_q10_run_history_summary_cross_run(self, conn, populated_db):
        """
        Q10: Run history summary across multiple runs.
        DB has 2 runs (run_a: 1 auto_go + 1 borderline; run_b: 1 auto_go).
        Must return 2 rows ordered by started_at DESC.
        """
        rows = conn.execute(
            """SELECT
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
               ORDER BY r.started_at DESC""",
        ).fetchall()

        assert len(rows) == 2, "Q10: 2 runs expected in history"

        # Most recent run is run_b (seeded after run_a)
        run_b_row = rows[0]
        assert run_b_row["go_count"] == 1
        assert run_b_row["borderline_count"] == 0

        run_a_row = rows[1]
        assert run_a_row["go_count"] == 1
        assert run_a_row["borderline_count"] == 1
        assert run_a_row["no_go_count"] == 0

        # All rows have scenario_name populated
        for r in rows:
            assert r["scenario_name"] == "capital_accumulation"
            assert r["started_at"] is not None

    # ── Bonus: index correctness (partial index on verdict) ───────────────────

    def test_partial_index_auto_go_usable(self, conn, populated_db):
        """
        The schema defines a partial index: idx_verdicts_go WHERE verdict = 'auto_go'.
        Verify the query planner can use it (EXPLAIN QUERY PLAN shows index usage).
        """
        _, run_id, *_ = populated_db

        plan = conn.execute(
            "EXPLAIN QUERY PLAN SELECT candidate_id FROM verdicts WHERE run_id = ? AND verdict = 'auto_go'",
            (run_id,),
        ).fetchall()

        plan_text = " ".join(str(r) for r in plan).lower()
        # Plan must reference an index (not a full table scan)
        assert "scan" not in plan_text or "index" in plan_text, (
            "Q_BONUS: Expected index scan for auto_go partial index query; got full scan"
        )

    def test_foreign_key_integrity_enforced(self, conn, populated_db):
        """
        FK enforcement must be ON (PRAGMA foreign_keys = ON set in fixture).
        Inserting a verdict with a non-existent candidate_id must raise.
        """
        _, run_id, *_ = populated_db
        fake_candidate_id = "f" * 64

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """INSERT INTO verdicts (
                    candidate_id, run_id, scenario_name, verdict, deployment_status,
                    sensitivity_spike, oos_gate_triggered, window_collapse_flag,
                    sensitivity_profile_incomplete, evidence_summary, evidence_json, recorded_at
                ) VALUES (?, ?, 'capital_accumulation', 'auto_go', 'PAPER_TRADE_REQUIRED',
                          0, 0, 0, 0, 'test', '{}', ?)""",
                (fake_candidate_id, run_id, datetime.now(UTC).isoformat()),
            )
            conn.commit()
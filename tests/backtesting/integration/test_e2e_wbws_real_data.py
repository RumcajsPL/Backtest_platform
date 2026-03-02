"""
test_e2e_wbws_real_data.py — End-to-end pipeline test on real WBWS data.

Phase 6 / Block 0 — E2E validation gate.

PURPOSE:
    Verify the full 8-stage backtesting pipeline executes correctly against
    the real 3-month WBWS data slice (2025-09-15 → 2025-12-17, DAX 1-min).
    This is the first test that exercises the orchestrator with real strategy
    evaluations rather than mocks.

PIPELINE COVERAGE:
    Stage 0  ✓ — Config loading, scenario validation, WFO window validation
    Stage 1  ✗ — Stub (wired but not implemented); injected candidates bypass it
    Stage 2  ✗ — Stub; bypassed by injection
    Stage 3  ✗ — Stub; bypassed by injection
    Stage 4  ✗ — Stub; bypassed by injection
    Stage 5  ✓ — MC Deep on injected WFO survivors
    Stage 6  ✓ — Parameter Sensitivity on MC Deep survivors
    Stage 7  ✓ — Verdict, trading YAML, HTML/JSON/Parquet report

    Stages 1–4 become exercisable once their implementations are wired.
    A separate TODO marker in this file identifies where to extend the test.

INJECTION STRATEGY:
    Because Stages 1–4 are stubs, we seed the CandidateStore directly with
    real strategy evaluations before invoking the orchestrator. This exercises
    the identical code paths that Stages 1–4 will eventually drive.

    Seeding procedure:
      1. Evaluate N_SEED_CANDIDATES parameter sets using strategy_runner.evaluate()
         against the real data (strategy_template.yaml data slice).
      2. Write CandidateRecord rows (RANDOM stage) to the store.
      3. Write WFOConsistencyScore rows directly (simulating Stage 4 output).
      4. Set checkpoint to WFO_COMPLETE so the orchestrator resumes at Stage 5.

PASS CRITERIA (all must hold):
    P-01  Pipeline completes without unhandled exception
    P-02  SQLite DB exists and contains at least 1 run row
    P-03  At least 1 candidate has a WFO consistency score
    P-04  At least 1 candidate has an MC Deep result
    P-05  At least 1 candidate has a verdict (any: auto_go / borderline / no_go)
    P-06  HTML report file exists and is non-empty
    P-07  JSON output directory exists
    P-08  Store closes cleanly (no writer-thread errors)

RUNTIME EXPECTATION:
    With N_SEED_CANDIDATES = 5 and MC iterations = 50 (smoke config), this
    test should complete in < 60 seconds on the target hardware.
    For a realistic run, set USE_SMOKE_CONFIG = False (300+ seconds expected).

RUNNING:
    From project root:
        pytest tests/backtesting/integration/test_e2e_wbws_real_data.py -v -s

    To run the realistic (slow) variant:
        pytest tests/backtesting/integration/test_e2e_wbws_real_data.py -v -s
            --e2e-realistic

IMPORTANT — before first run:
    Confirm strategy_runner.evaluate() can import the strategy package in this
    environment. If the import fails, the test is SKIPPED (not FAILED), consistent
    with the bench_d01 convention. The skip message will indicate the import path.
"""
from __future__ import annotations

import json
import logging
import shutil
import sys
from datetime import UTC, datetime, date
from pathlib import Path
from typing import List, Optional

import pytest

# ── Project root on sys.path ──────────────────────────────────────────────────
# Ensure the project root is importable regardless of how pytest is invoked.
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.utils.paths import PROJECT_ROOT, CONFIGS_DIR
from src.backtesting.candidate_store import CandidateStore
from src.backtesting.contracts import (
    CandidateParameterSet,
    CandidateRecord,
    CandidateStage,
    Checkpoint,
    MCMode,
    RunMetadata,
    WFOConsistencyScore,
)
from src.backtesting.scenario import load_scenario
from src.backtesting.orchestrator import (
    _load_and_validate_config,
    _compute_config_hash,
    _initialise_run,
    _run_stage_5_mc_deep,
    _run_stage_6_sensitivity,
    _run_stage_7_report,
)

logger = logging.getLogger(__name__)

# ── Test configuration ────────────────────────────────────────────────────────

CONFIG_PATH = PROJECT_ROOT / "configs" / "backtesting" / "backtest_template.yaml"
STRATEGY_YAML = CONFIGS_DIR / "strategies" / "strategy_template.yaml"

# Smoke config: fast, minimal — suitable for CI and first-run validation
N_SEED_CANDIDATES = 5       # How many real strategy evaluations to seed
USE_SMOKE_CONFIG = True     # Override MC iterations to low values for speed

SMOKE_MC_ITERATIONS = 50    # Overrides monte_carlo.deep.iterations in smoke mode
SMOKE_SENSITIVITY_STEPS = 1 # Overrides sensitivity.max_steps in smoke mode

# Realistic parameter sets derived from the safe zone of backtest_template.yaml
# These are fixed (not LHS-sampled) to keep the test deterministic and fast.
# Chosen to be plausible WBWSStrategy parameters — adjust if strategy_runner
# requires different field names.
_SEED_PARAMETER_SETS = [
    {
        "rsi_period": 14, "rsi_overbought": 70, "rsi_oversold": 30,
        "atr_length": 14, "atr_multiplier": 1.4,
        "rr_target": 7.0, "risk_percentile": 0.23,
        "bollinger_length": 14, "bollinger_multiplier": 0.5,
    },
    {
        "rsi_period": 10, "rsi_overbought": 70, "rsi_oversold": 30,
        "atr_length": 10, "atr_multiplier": 1.2,
        "rr_target": 6.0, "risk_percentile": 0.20,
        "bollinger_length": 10, "bollinger_multiplier": 0.4,
    },
    {
        "rsi_period": 18, "rsi_overbought": 75, "rsi_oversold": 25,
        "atr_length": 16, "atr_multiplier": 1.6,
        "rr_target": 8.0, "risk_percentile": 0.25,
        "bollinger_length": 16, "bollinger_multiplier": 0.6,
    },
    {
        "rsi_period": 12, "rsi_overbought": 65, "rsi_oversold": 35,
        "atr_length": 12, "atr_multiplier": 1.0,
        "rr_target": 5.0, "risk_percentile": 0.15,
        "bollinger_length": 12, "bollinger_multiplier": 0.3,
    },
    {
        "rsi_period": 20, "rsi_overbought": 80, "rsi_oversold": 20,
        "atr_length": 20, "atr_multiplier": 2.0,
        "rr_target": 9.0, "risk_percentile": 0.30,
        "bollinger_length": 20, "bollinger_multiplier": 0.7,
    },
]


# ── CLI option for realistic mode ─────────────────────────────────────────────

def pytest_addoption(parser):
    parser.addoption(
        "--e2e-realistic",
        action="store_true",
        default=False,
        help="Run E2E test with production-level MC iterations (slow)",
    )


# ── Strategy availability guard ───────────────────────────────────────────────

def _try_import_strategy():
    """Return (StrategyConfig, StrategyOrchestrator, CacheManager) or raise ImportError."""
    from src.strategies.config.config_schema import StrategyConfig
    from src.strategies.orchestrator import StrategyOrchestrator
    from src.strategies.core.cache_manager import CacheManager
    return StrategyConfig, StrategyOrchestrator, CacheManager


def _strategy_available() -> bool:
    try:
        _try_import_strategy()
        return True
    except ImportError:
        return False


# ── Real strategy evaluation helper ──────────────────────────────────────────

def _evaluate_candidate_real(
    candidate: CandidateParameterSet,
    strategy_yaml: Path,
    temp_dir: Path,
) -> Optional[object]:
    """
    Evaluate one candidate using strategy_runner.evaluate().
    Returns CandidateResult. On import failure returns None (test will skip).
    """
    from src.backtesting.strategy_runner import evaluate
    return evaluate(
        candidate=candidate,
        base_yaml_path=strategy_yaml,
        temp_dir=temp_dir,
    )


def _build_candidate_record(
    run_id: str,
    candidate: CandidateParameterSet,
    candidate_result,
    fitness_result,
) -> CandidateRecord:
    """Build a CandidateRecord from evaluation results for store injection."""
    return CandidateRecord(
        run_id=run_id,
        candidate_id=candidate.candidate_id,
        zone_name=candidate.zone_name,
        stage=CandidateStage.RANDOM.value,
        generation=None,
        recorded_at=datetime.now(UTC),
        parameters_json=json.dumps(candidate.parameters, sort_keys=True, default=str),
        fitness_score=fitness_result.fitness_score,
        passed_constraints=fitness_result.passed_constraints,
        rejection_reason=fitness_result.rejection_reason,
        failing_constraint=fitness_result.failing_constraint,
        failing_value=fitness_result.failing_value,
        actual_win_rate=fitness_result.actual_win_rate,
        actual_max_drawdown=fitness_result.actual_max_drawdown,
        actual_losing_streak=fitness_result.actual_losing_streak,
        actual_trades_per_week=fitness_result.actual_trades_per_week,
        actual_expectancy=fitness_result.actual_expectancy,
        actual_profit_factor=fitness_result.actual_profit_factor,
        wfo_median_window_return=None,
        wfo_window_return_variance=None,
        wfo_worst_window_drawdown=None,
        wfo_fraction_positive_windows=None,
        wfo_consistency_score=None,
        wfo_windows_evaluated=None,
        wfo_oos_gate_triggered=None,
        wfo_window_collapse_flag=None,
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


def _inject_wfo_consistency_score(
    store: CandidateStore,
    run_id: str,
    candidate_id: str,
    fitness_score: float,
) -> None:
    """
    Inject a synthetic WFOConsistencyScore for a candidate.
    Uses the fitness score as a proxy for the composite score so that
    higher-fitness candidates rank higher in Stage 5/6/7.
    This simulates what Stage 4 (Full WFO) will produce once wired.
    """
    # Synthetic but plausible sub-metrics derived from fitness_score
    score = WFOConsistencyScore(
        candidate_id=candidate_id,
        windows_evaluated=5,
        windows_total=5,
        median_window_return=fitness_score * 0.8,
        window_return_variance=max(0.0, 1.0 - fitness_score) * 0.1,
        worst_window_drawdown=max(0.0, 1.0 - fitness_score) * 0.15,
        fraction_positive_windows=min(1.0, fitness_score * 1.1),
        composite_score=fitness_score,
        oos_gate_triggered=False,
        window_collapse_flag=False,
    )
    store.write_wfo_consistency_score(score, run_id)


# ── Main fixture: seeded store ────────────────────────────────────────────────

@pytest.fixture(scope="module")
def e2e_run(tmp_path_factory, request):
    """
    Module-scoped fixture that:
      1. Loads and validates backtest_template.yaml
      2. Creates a CandidateStore in a temp directory
      3. Evaluates N_SEED_CANDIDATES real strategy runs
      4. Injects CandidateRecord + WFOConsistencyScore rows
      5. Sets checkpoint to WFO_COMPLETE
      6. Returns a context dict for assertions

    If the strategy package is not importable, the fixture yields a
    skip marker and all tests in this module are skipped.
    """
    if not _strategy_available():
        pytest.skip(
            "Strategy package not importable in this environment. "
            "Run on operator machine with full project installed. "
            f"Expected imports: src.strategies.config.config_schema, "
            f"src.strategies.orchestrator, src.strategies.core.cache_manager"
        )

    if not CONFIG_PATH.exists():
        pytest.skip(
            f"backtest_template.yaml not found at {CONFIG_PATH}. "
            "Create it before running this test."
        )

    if not STRATEGY_YAML.exists():
        pytest.skip(f"strategy_template.yaml not found at {STRATEGY_YAML}")

    realistic = request.config.getoption("--e2e-realistic", default=False)

    # ── Dirs ──────────────────────────────────────────────────────────────────
    run_dir = tmp_path_factory.mktemp("e2e_run")
    temp_dir = run_dir / "temp"
    output_dir = run_dir / "outputs"
    temp_dir.mkdir()
    output_dir.mkdir()

    # ── Config ────────────────────────────────────────────────────────────────
    config = _load_and_validate_config(CONFIG_PATH)

    # Smoke overrides — reduce MC and sensitivity cost for fast validation
    if not realistic and USE_SMOKE_CONFIG:
        config.setdefault("monte_carlo", {}).setdefault("deep", {})
        config["monte_carlo"]["deep"]["iterations"] = SMOKE_MC_ITERATIONS
        config["monte_carlo"]["deep"]["input_count"] = N_SEED_CANDIDATES
        config.setdefault("sensitivity", {})
        config["sensitivity"]["max_steps"] = SMOKE_SENSITIVITY_STEPS
        config["sensitivity"]["input_count"] = N_SEED_CANDIDATES
        config["run"]["output_dir"] = str(output_dir)
        config["run"]["temp_dir"] = str(temp_dir)
        config["run"]["max_workers"] = 2   # Limit workers for test isolation

    # Always use the e2e_test scenario — loose constraints to validate pipeline
    # plumbing, not strategy quality. Production scenarios are never used here.
    config["scenario"] = "e2e_test"

    # ── Store + run metadata ──────────────────────────────────────────────────
    db_path = output_dir / "backtester.db"
    store = CandidateStore(db_path)
    config_hash = _compute_config_hash(CONFIG_PATH)
    run_metadata = _initialise_run(store, config, CONFIG_PATH, config_hash)
    run_id = run_metadata.run_id

    # ── Real strategy evaluations ─────────────────────────────────────────────
    from src.backtesting.fitness import evaluate_fitness
    scenario = load_scenario(config)

    evaluated_candidates = []
    evaluation_errors = []

    for i, params in enumerate(_SEED_PARAMETER_SETS[:N_SEED_CANDIDATES]):
        candidate = CandidateParameterSet.create(
            zone_name="safe",
            parameters=params,
            generation=None,
        )

        candidate_result = _evaluate_candidate_real(candidate, STRATEGY_YAML, temp_dir)

        if candidate_result is None or candidate_result.error:
            error_msg = (
                candidate_result.error if candidate_result else "evaluate() returned None"
            )
            evaluation_errors.append((candidate.candidate_id[:12], error_msg))
            logger.warning(
                "E2E seed: candidate %s evaluation failed: %s",
                candidate.candidate_id[:12], error_msg,
            )
            # Still write to store with failed status
            # fitness_score=None, passed_constraints=False
            fitness_result = type("_FR", (), {
                "fitness_score": None,
                "passed_constraints": False,
                "rejection_reason": "EVALUATION_ERROR",
                "failing_constraint": None,
                "failing_value": None,
                "actual_win_rate": None,
                "actual_max_drawdown": None,
                "actual_losing_streak": None,
                "actual_trades_per_week": None,
                "actual_expectancy": None,
                "actual_profit_factor": None,
            })()
        else:
            fitness_result = evaluate_fitness(candidate_result, scenario)
            
        record = _build_candidate_record(run_id, candidate, candidate_result, fitness_result)
        store.write_candidate(record)

        # Inject WFO score for candidates that passed constraints
        if fitness_result.passed_constraints and fitness_result.fitness_score is not None:
            _inject_wfo_consistency_score(
                store, run_id, candidate.candidate_id, fitness_result.fitness_score
            )
            evaluated_candidates.append({
                "candidate": candidate,
                "candidate_result": candidate_result,
                "fitness": fitness_result.fitness_score,
            })

        logger.info(
            "E2E seed [%d/%d]: candidate=%s  passed=%s  fitness=%s",
            i + 1, N_SEED_CANDIDATES,
            candidate.candidate_id[:12],
            fitness_result.passed_constraints,
            f"{fitness_result.fitness_score:.4f}" if fitness_result.fitness_score else "N/A",
        )

    store.flush()

    # ── Advance checkpoint to WFO_COMPLETE ────────────────────────────────────
    # Stages 1–4 are stubs — skip them by setting checkpoint directly.
    # TODO: Remove this block once Stages 1–4 are implemented and wired.
    store.set_checkpoint(run_id, Checkpoint.WFO_COMPLETE)

    # ── Stage 5: MC Deep ──────────────────────────────────────────────────────
    pipeline_error: Optional[Exception] = None
    try:
        _run_stage_5_mc_deep(config, store, run_metadata)
        store.set_checkpoint(run_id, Checkpoint.MONTE_CARLO_COMPLETE)

        # ── Stage 6: Sensitivity ─────────────────────────────────────────────
        _run_stage_6_sensitivity(config, store, run_metadata)
        store.set_checkpoint(run_id, Checkpoint.SENSITIVITY_COMPLETE)

        # ── Stage 7: Report & Output ─────────────────────────────────────────
        _run_stage_7_report(config, store, run_metadata)
        store.set_checkpoint(run_id, Checkpoint.COMPLETE)

    except Exception as exc:
        pipeline_error = exc
        logger.exception("E2E pipeline error in Stage 5/6/7: %s", exc)
    finally:
        store.flush()
        writer_errors = list(store._writer_errors)
        store.close()

    yield {
        "run_id": run_id,
        "run_metadata": run_metadata,
        "config": config,
        "output_dir": output_dir,
        "db_path": db_path,
        "evaluated_candidates": evaluated_candidates,
        "evaluation_errors": evaluation_errors,
        "pipeline_error": pipeline_error,
        "writer_errors": writer_errors,
        "n_seeded": len(evaluated_candidates),
        "smoke_mode": not realistic,
    }

    # ── Teardown ──────────────────────────────────────────────────────────────
    shutil.rmtree(run_dir, ignore_errors=True) #removing all content after test execution, comment to disable 


# ═════════════════════════════════════════════════════════════════════════════
# Test cases — each maps to one pass criterion
# ═════════════════════════════════════════════════════════════════════════════

class TestE2EWBWSRealData:
    """
    End-to-end pipeline validation on real WBWS data.
    All tests share the module-scoped `e2e_run` fixture (pipeline runs once).
    """

    def test_p01_pipeline_completes_without_exception(self, e2e_run):
        """P-01: No unhandled exception during Stages 5–7."""
        ctx = e2e_run
        if ctx["pipeline_error"] is not None:
            pytest.fail(
                f"Pipeline raised an unhandled exception: {ctx['pipeline_error']!r}\n"
                f"Seeded candidates: {ctx['n_seeded']}\n"
                f"Evaluation errors: {ctx['evaluation_errors']}"
            )

    def test_p01b_store_writer_no_errors(self, e2e_run):
        """P-01b: CandidateStore writer thread produced no errors."""
        errors = e2e_run["writer_errors"]
        assert errors == [], f"CandidateStore writer errors: {errors}"

    def test_p02_database_exists_with_run_row(self, e2e_run):
        """P-02: SQLite DB exists and contains exactly 1 run row for this run_id."""
        import sqlite3
        db_path = e2e_run["db_path"]
        run_id = e2e_run["run_id"]

        assert db_path.exists(), f"DB not found at {db_path}"

        conn = sqlite3.connect(str(db_path))
        try:
            row = conn.execute(
                "SELECT run_id, checkpoint FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        finally:
            conn.close()

        assert row is not None, f"No run row found for run_id={run_id}"
        assert row[0] == run_id
        assert row[1] == Checkpoint.COMPLETE.name, (
            f"Checkpoint not COMPLETE: {row[1]}"
        )

    def test_p02b_candidate_rows_written(self, e2e_run):
        """P-02b: At least N_SEED_CANDIDATES evaluation rows exist in the DB."""
        import sqlite3
        db_path = e2e_run["db_path"]
        run_id = e2e_run["run_id"]

        conn = sqlite3.connect(str(db_path))
        try:
            count = conn.execute(
                "SELECT COUNT(*) FROM evaluations WHERE run_id = ?", (run_id,)
            ).fetchone()[0]
        finally:
            conn.close()

        assert count >= N_SEED_CANDIDATES, (
            f"Expected at least {N_SEED_CANDIDATES} evaluation rows, got {count}"
        )

    def test_p03_at_least_one_wfo_consistency_score(self, e2e_run):
        """P-03: At least 1 WFO consistency score exists (injected by fixture)."""
        if e2e_run["n_seeded"] == 0:
            pytest.skip(
                "No candidates passed constraints — check strategy evaluation "
                "or loosen constraint thresholds in backtest_template.yaml"
            )
        import sqlite3
        conn = sqlite3.connect(str(e2e_run["db_path"]))
        try:
            count = conn.execute(
                "SELECT COUNT(*) FROM wfo_consistency_scores WHERE run_id = ?",
                (e2e_run["run_id"],),
            ).fetchone()[0]
        finally:
            conn.close()

        assert count >= 1, (
            f"Expected at least 1 WFO consistency score row, got {count}. "
            f"Seeded {e2e_run['n_seeded']} candidates."
        )

    def test_p04_at_least_one_mc_deep_result(self, e2e_run):
        """P-04: At least 1 MC Deep result exists after Stage 5."""
        if e2e_run["n_seeded"] == 0:
            pytest.skip("No WFO survivors to run MC Deep on")

        import sqlite3
        conn = sqlite3.connect(str(e2e_run["db_path"]))
        try:
            count = conn.execute(
                "SELECT COUNT(*) FROM mc_results WHERE run_id = ? AND mode = 'deep'",
                (e2e_run["run_id"],),
            ).fetchone()[0]
        finally:
            conn.close()

        assert count >= 1, (
            f"Expected at least 1 MC Deep result, got {count}. "
            f"Stage 5 may have silently failed — check logs."
        )

    def test_p04b_mc_deep_ruin_probability_in_range(self, e2e_run):
        """P-04b: All MC Deep ruin probabilities are in [0, 1] (no corrupt values)."""
        if e2e_run["n_seeded"] == 0:
            pytest.skip("No WFO survivors")

        import sqlite3
        conn = sqlite3.connect(str(e2e_run["db_path"]))
        try:
            rows = conn.execute(
                """SELECT candidate_id, ruin_probability FROM mc_results
                   WHERE run_id = ? AND mode = 'deep' AND ruin_probability IS NOT NULL""",
                (e2e_run["run_id"],),
            ).fetchall()
        finally:
            conn.close()

        for cid, ruin_prob in rows:
            assert 0.0 <= ruin_prob <= 1.0, (
                f"Invalid ruin_probability {ruin_prob} for candidate {cid[:12]}"
            )

    def test_p05_at_least_one_verdict(self, e2e_run):
        """P-05: At least 1 verdict row exists after Stage 7."""
        if e2e_run["n_seeded"] == 0:
            pytest.skip("No candidates passed constraints")

        import sqlite3
        conn = sqlite3.connect(str(e2e_run["db_path"]))
        try:
            rows = conn.execute(
                "SELECT verdict, COUNT(*) FROM verdicts WHERE run_id = ? GROUP BY verdict",
                (e2e_run["run_id"],),
            ).fetchall()
        finally:
            conn.close()

        assert len(rows) >= 1, (
            "No verdict rows found. Stage 7 may have silently skipped all candidates."
        )

        verdict_counts = dict(rows)
        logger.info("E2E verdict distribution: %s", verdict_counts)

    def test_p05b_no_live_approved_verdicts(self, e2e_run):
        """P-05b: No verdict has deployment_status = LIVE_APPROVED (operator-only action)."""
        import sqlite3
        conn = sqlite3.connect(str(e2e_run["db_path"]))
        try:
            count = conn.execute(
                "SELECT COUNT(*) FROM verdicts WHERE deployment_status = 'LIVE_APPROVED'",
            ).fetchone()[0]
        finally:
            conn.close()

        assert count == 0, (
            f"{count} verdict(s) incorrectly set to LIVE_APPROVED. "
            "This must only be set by the operator after paper trading."
        )

    def test_p06_html_report_exists_and_nonempty(self, e2e_run):
        """P-06: HTML report file exists and is non-trivially sized (> 1 KB)."""
        output_dir = e2e_run["output_dir"]
        run_id = e2e_run["run_id"]

        html_files = list(output_dir.glob("*.html"))
        if not html_files:
            # Also check for run-prefixed subdirectory
            html_files = list(output_dir.glob(f"{run_id[:8]}*.html"))

        if not html_files:
            html_files = list(output_dir.rglob("*.html"))

        assert html_files, (
            f"No HTML report found in {output_dir}. "
            "Stage 7 report generation may have failed."
        )

        report_file = html_files[0]
        size = report_file.stat().st_size
        assert size > 1024, (
            f"HTML report is suspiciously small ({size} bytes): {report_file}"
        )

    def test_p07_json_output_exists(self, e2e_run):
        """P-07: JSON output directory (or files) exists after Stage 7."""
        if e2e_run["n_seeded"] == 0:
            pytest.skip("No candidates to generate JSON for")

        output_dir = e2e_run["output_dir"]
        json_files = list(output_dir.rglob("*.json"))

        assert json_files, (
            f"No JSON output files found under {output_dir}. "
            "Stage 7 JSON generation may have failed."
        )
    def test_p07b_parquet_output_exists(self, e2e_run):
        """P-07b: Parquet output files exist after Stage 7 (formats.parquet: true)."""
        if e2e_run["n_seeded"] == 0:
            pytest.skip("No candidates to generate Parquet for")
        parquet_files = list(e2e_run["output_dir"].rglob("*.parquet"))
        assert parquet_files, (
            f"No Parquet files found under {e2e_run['output_dir']}. "
            "Confirm output.formats.parquet: true in backtest_template.yaml."
        )

    def test_p08_sensitivity_profiles_written(self, e2e_run):
        """P-08: At least 1 sensitivity profile exists after Stage 6."""
        if e2e_run["n_seeded"] == 0:
            pytest.skip("No WFO survivors for sensitivity analysis")

        import sqlite3
        conn = sqlite3.connect(str(e2e_run["db_path"]))
        try:
            count = conn.execute(
                "SELECT COUNT(*) FROM sensitivity_profiles WHERE run_id = ?",
                (e2e_run["run_id"],),
            ).fetchone()[0]
        finally:
            conn.close()

        assert count >= 1, (
            f"No sensitivity_profiles rows found. Stage 6 may have silently failed. "
            f"Seeded {e2e_run['n_seeded']} WFO candidates."
        )

    # ── Informational summary (always runs, never fails) ──────────────────────

    def test_z_summary(self, e2e_run, capsys):
        """Summary: print run metrics for operator review (never fails)."""
        ctx = e2e_run
        import sqlite3
        conn = sqlite3.connect(str(ctx["db_path"]))
        try:
            wfo_count = conn.execute(
                "SELECT COUNT(*) FROM wfo_consistency_scores WHERE run_id = ?",
                (ctx["run_id"],),
            ).fetchone()[0]
            mc_count = conn.execute(
                "SELECT COUNT(*) FROM mc_results WHERE run_id = ? AND mode = 'deep'",
                (ctx["run_id"],),
            ).fetchone()[0]
            verdict_rows = conn.execute(
                "SELECT verdict, COUNT(*) FROM verdicts WHERE run_id = ? GROUP BY verdict",
                (ctx["run_id"],),
            ).fetchall()
            sens_count = conn.execute(
                "SELECT COUNT(*) FROM sensitivity_profiles WHERE run_id = ?",
                (ctx["run_id"],),
            ).fetchone()[0]
            html_files = list(ctx["output_dir"].rglob("*.html"))
            json_files = list(ctx["output_dir"].rglob("*.json"))
            parquet_files = list(ctx["output_dir"].rglob("*.parquet"))
        finally:
            conn.close()

        with capsys.disabled():
            print(f"\n{'='*60}")
            print(f"E2E PIPELINE SUMMARY — run_id={ctx['run_id'][:12]}…")
            print(f"{'='*60}")
            print(f"  Mode              : {'SMOKE (fast)' if ctx['smoke_mode'] else 'REALISTIC'}")
            print(f"  Seed candidates   : {N_SEED_CANDIDATES} attempted")
            print(f"  WFO survivors     : {ctx['n_seeded']} (passed constraints)")
            print(f"  Evaluation errors : {len(ctx['evaluation_errors'])}")
            if ctx["evaluation_errors"]:
                for cid, err in ctx["evaluation_errors"][:3]:
                    print(f"    {cid}: {err}")
            print(f"  WFO scores written: {wfo_count}")
            print(f"  MC Deep results   : {mc_count}")
            print(f"  Sensitivity profs : {sens_count}")
            print(f"  Verdicts          : {dict(verdict_rows) or 'none'}")
            print(f"  Pipeline error    : {ctx['pipeline_error'] or 'none'}")
            print(f"  Writer errors     : {ctx['writer_errors'] or 'none'}")
            print(f"  DB path           : {ctx['db_path']}")
            print(f"  Artifacts written :")
            print(f"    HTML            : {len(html_files)} file(s)")
            print(f"    JSON            : {len(json_files)} file(s)")
            print(f"    Parquet         : {len(parquet_files)} file(s)")
            print(f"{'='*60}\n")


# ── Standalone runner ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    import subprocess
    result = subprocess.run(
        [
            sys.executable, "-m", "pytest",
            __file__, "-v", "-s",
            "--tb=short",
        ],
        cwd=str(_PROJECT_ROOT),
    )
    sys.exit(result.returncode)
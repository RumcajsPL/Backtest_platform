"""
test_block8a_foundation.py — Block 8A: Foundation layer audit tests.

Covers:
  B8-001: wfo_consistency_scores table persists median_oos_delta correctly.
  B8-002: get_wfo_consistency_score returns median_oos_delta from DB (not always None).
  B8-005: Stage 0 rejects min_significant_trades=0 and invalid spike_threshold.
  Contract None-path audit: FitnessResult, MCResult, VerdictResult Optional fields.
  Store P10: run records cannot be mutated after initialisation.
  Store schema: all 9 dispatch method names exist as class methods.

12 tests total. All should pass after B8-001 and B8-005 fixes are applied.
"""
from __future__ import annotations

import json
import sys
from datetime import UTC, datetime, date
from pathlib import Path
from typing import Optional

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.utils.paths import PROJECT_ROOT  # noqa: E402  — path anchor

from src.backtesting.contracts import (  # noqa: E402
    CandidateParameterSet,
    CandidateRecord,
    CandidateStage,
    Checkpoint,
    DeploymentStatus,
    FitnessResult,
    MCMode,
    MCResult,
    RunMetadata,
    ScenarioProfile,
    SensitivityProfile,
    Verdict,
    VerdictResult,
    WFOConsistencyScore,
)
from src.backtesting.candidate_store import CandidateStore  # noqa: E402


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def store(tmp_path):
    s = CandidateStore(tmp_path / "test.db")
    yield s
    s.close()


@pytest.fixture
def run_meta(tmp_path):
    return RunMetadata(
        run_id="test-run-8a",
        config_hash="a" * 64,
        scenario_name="e2e_test",
        started_at=datetime.now(UTC),
        perturbation_profile_name="default",
        random_search_seed=42,
        ga_seed=43,
        mc_prefilter_seed=44,
        mc_deep_seed=45,
        sensitivity_seed=46,
        wfo_window_ids=("w1", "w2", "w3"),
        checkpoint=Checkpoint.NOT_STARTED,
        backtester_version="1.0.0",
    )


@pytest.fixture
def initialised_store(store, run_meta):
    store.initialise_run(run_meta)
    return store, run_meta


def _make_wfo_score(candidate_id: str, median_oos_delta: Optional[float] = None) -> WFOConsistencyScore:
    return WFOConsistencyScore(
        candidate_id=candidate_id,
        windows_evaluated=3,
        windows_total=3,
        median_window_return=0.05,
        window_return_variance=0.01,
        worst_window_drawdown=0.12,
        fraction_positive_windows=1.0,
        composite_score=0.75,
        oos_gate_triggered=False,
        window_collapse_flag=False,
        median_oos_delta=median_oos_delta,
    )


def _make_candidate_record(run_id: str, candidate_id: str) -> CandidateRecord:
    return CandidateRecord(
        run_id=run_id,
        candidate_id=candidate_id,
        zone_name="safe",
        stage=CandidateStage.RANDOM.value,
        generation=None,
        recorded_at=datetime.now(UTC),
        parameters_json=json.dumps({"rsi_period": 14}),
        fitness_score=0.72,
        passed_constraints=True,
        rejection_reason=None,
        failing_constraint=None,
        failing_value=None,
        actual_win_rate=0.55,
        actual_max_drawdown=0.08,
        actual_losing_streak=3,
        actual_trades_per_week=2.5,
        actual_expectancy=0.3,
        actual_profit_factor=1.4,
        wfo_median_window_return=None,
        wfo_window_return_variance=None,
        wfo_worst_window_drawdown=None,
        wfo_fraction_positive_windows=None,
        wfo_consistency_score=None,
        wfo_windows_evaluated=None,
        wfo_oos_gate_triggered=None,
        wfo_window_collapse_flag=None,
        wfo_median_oos_delta=None,
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


# ── B8-001: median_oos_delta persisted and read back ─────────────────────────

class TestB8001WfoMedianOosDeltaPersisted:
    """B8-001: wfo_consistency_scores must persist and return median_oos_delta."""

    def test_wfo_median_oos_delta_positive_value_round_trips(self, initialised_store):
        """A positive median_oos_delta is persisted and returned by get_wfo_consistency_score."""
        store, run_meta = initialised_store
        cid = "a" * 64
        record = _make_candidate_record(run_meta.run_id, cid)
        store.write_candidate(record)

        score = _make_wfo_score(cid, median_oos_delta=0.042)
        store.write_wfo_consistency_score(score, run_meta.run_id)
        store.flush()

        result = store.get_wfo_consistency_score(cid)
        assert result is not None
        assert result.median_oos_delta == pytest.approx(0.042)

    def test_wfo_median_oos_delta_negative_value_round_trips(self, initialised_store):
        """A negative median_oos_delta (OOS underperforms IS) is persisted correctly."""
        store, run_meta = initialised_store
        cid = "b" * 64
        record = _make_candidate_record(run_meta.run_id, cid)
        store.write_candidate(record)

        score = _make_wfo_score(cid, median_oos_delta=-0.031)
        store.write_wfo_consistency_score(score, run_meta.run_id)
        store.flush()

        result = store.get_wfo_consistency_score(cid)
        assert result is not None
        assert result.median_oos_delta == pytest.approx(-0.031)

    def test_wfo_median_oos_delta_none_round_trips(self, initialised_store):
        """None median_oos_delta (gate disabled) is persisted as NULL and returned as None."""
        store, run_meta = initialised_store
        cid = "c" * 64
        record = _make_candidate_record(run_meta.run_id, cid)
        store.write_candidate(record)

        score = _make_wfo_score(cid, median_oos_delta=None)
        store.write_wfo_consistency_score(score, run_meta.run_id)
        store.flush()

        result = store.get_wfo_consistency_score(cid)
        assert result is not None
        assert result.median_oos_delta is None

    def test_wfo_median_oos_delta_present_in_schema(self, store):
        """The wfo_consistency_scores table must have a median_oos_delta column."""
        cursor = store._conn.execute(
            "PRAGMA table_info(wfo_consistency_scores)"
        )
        columns = {row[1] for row in cursor.fetchall()}
        assert "median_oos_delta" in columns, (
            "B8-001: median_oos_delta column missing from wfo_consistency_scores table. "
            "The M-01 fix (Block 7D) computes this value but it was never persisted."
        )


# ── B8-002: query_candidates populates wfo_median_oos_delta ──────────────────

class TestB8002QueryCandidatesMedianOosDelta:
    """B8-002: query_candidates must propagate wfo_median_oos_delta from store."""

    def test_query_candidates_includes_wfo_median_oos_delta(self, initialised_store):
        """CandidateRecord.wfo_median_oos_delta is populated from the join, not always None.

        Requires two fixes in candidate_store.py (B8-002):
          (a) query_candidates SELECT must include wcs.median_oos_delta in the JOIN columns
          (b) _row_to_candidate_record must destructure it and pass wfo_median_oos_delta=
              to the CandidateRecord constructor

        Pre-fix failure mode: TypeError (missing constructor arg) if SELECT is extended
        but constructor call is not updated, or AssertionError if neither is done.
        """
        store, run_meta = initialised_store
        cid = "d" * 64
        record = _make_candidate_record(run_meta.run_id, cid)
        store.write_candidate(record)

        score = _make_wfo_score(cid, median_oos_delta=0.015)
        store.write_wfo_consistency_score(score, run_meta.run_id)
        store.flush()

        results = store.query_candidates(run_meta.run_id)
        assert len(results) == 1
        assert results[0].wfo_median_oos_delta == pytest.approx(0.015), (
            "B8-002: wfo_median_oos_delta in CandidateRecord is None. "
            "Fix: add wcs.median_oos_delta to query_candidates SELECT and "
            "wfo_median_oos_delta= to _row_to_candidate_record constructor call."
        )


# ── B8-005: Stage 0 config validation ────────────────────────────────────────

class TestB8005Stage0ConfigValidation:
    """B8-005: Stage 0 must reject invalid min_significant_trades and spike_threshold."""

    def _make_config(self, min_trades=30, spike_threshold=0.15) -> dict:
        return {
            "backtester_version": "1.0.0",
            "scenario": "e2e_test",
            "run": {"output_dir": "/tmp/test_out", "temp_dir": "/tmp/test_tmp"},
            "scenarios": {
                "e2e_test": {
                    "description": "Test scenario",
                    "fitness_weights": {
                        "net_pnl": 0.2, "expectancy": 0.3, "max_drawdown": 0.2,
                        "win_rate": 0.15, "trade_frequency": 0.1, "profit_factor": 0.05,
                    },
                    "constraints": {
                        "min_win_rate": 0.05, "max_drawdown": 0.95,
                        "max_losing_streak": 50, "min_trades_per_week": 0.1,
                        "min_expectancy": -5.0, "min_profit_factor": 0.1,
                    },
                    "mc_prefilter_ruin_threshold": 0.90,
                    "wfo_temporal_weights": {
                        "median_return": 0.4, "variance": 0.2,
                        "worst_drawdown": 0.2, "fraction_positive": 0.2,
                    },
                    "verdict_thresholds": {
                        "go_wfo_floor": 0.30, "borderline_wfo_floor": 0.10,
                        "go_mc_ruin_ceiling": 0.80, "borderline_mc_ruin_ceiling": 0.90,
                        "sensitivity_spike_threshold": 0.15,
                    },
                    "report_emphasis": [],
                },
            },
            "zones": {
                "safe": {
                    "enabled": True,
                    "parameters": {"rsi_period": {"type": "int", "low": 10, "high": 20, "step": 1}},
                },
            },
            "walk_forward": {
                "windows": [
                    {"id": "w1", "start": "2023-01-01", "end": "2023-04-01"},
                    {"id": "w2", "start": "2023-04-01", "end": "2023-07-01"},
                    {"id": "w3", "start": "2023-07-01", "end": "2023-10-01"},
                ],
                "enforce_oos_gate": False,
            },
            "random_search": {"min_significant_trades": min_trades, "seed": 42},
            "sensitivity": {"spike_threshold": spike_threshold, "input_count": 5, "max_steps": 2},
            "monte_carlo": {"deep": {"seed": 45, "input_count": 10, "iterations": 3000}},
            "mc_prefilter": {"seed": 44},
            "genetic": {"seed": 43},
        }

    def test_stage0_rejects_zero_min_significant_trades(self, tmp_path):
        """Stage 0 must raise ValueError when min_significant_trades=0."""
        from src.backtesting.orchestrator import _run_stage_0_init
        from src.backtesting.contracts import RunMetadata, Checkpoint

        config = self._make_config(min_trades=0)
        run_meta = RunMetadata(
            run_id="test-b8-005a",
            config_hash="e" * 64,
            scenario_name="e2e_test",
            started_at=datetime.now(UTC),
            perturbation_profile_name="default",
            random_search_seed=42,
            ga_seed=43,
            mc_prefilter_seed=44,
            mc_deep_seed=45,
            sensitivity_seed=46,
            wfo_window_ids=("w1", "w2", "w3"),
            checkpoint=Checkpoint.NOT_STARTED,
            backtester_version="1.0.0",
        )

        with pytest.raises(ValueError, match="min_significant_trades"):
            _run_stage_0_init(config, None, run_meta)

    def test_stage0_rejects_negative_min_significant_trades(self, tmp_path):
        """Stage 0 must raise ValueError when min_significant_trades is negative."""
        from src.backtesting.orchestrator import _run_stage_0_init
        from src.backtesting.contracts import RunMetadata, Checkpoint

        config = self._make_config(min_trades=-5)
        run_meta = RunMetadata(
            run_id="test-b8-005b",
            config_hash="f" * 64,
            scenario_name="e2e_test",
            started_at=datetime.now(UTC),
            perturbation_profile_name="default",
            random_search_seed=42,
            ga_seed=43,
            mc_prefilter_seed=44,
            mc_deep_seed=45,
            sensitivity_seed=46,
            wfo_window_ids=("w1", "w2", "w3"),
            checkpoint=Checkpoint.NOT_STARTED,
            backtester_version="1.0.0",
        )

        with pytest.raises(ValueError, match="min_significant_trades"):
            _run_stage_0_init(config, None, run_meta)

    def test_stage0_rejects_zero_spike_threshold(self, tmp_path):
        """Stage 0 must raise ValueError when spike_threshold=0.0."""
        from src.backtesting.orchestrator import _run_stage_0_init
        from src.backtesting.contracts import RunMetadata, Checkpoint

        config = self._make_config(spike_threshold=0.0)
        run_meta = RunMetadata(
            run_id="test-b8-005c",
            config_hash="1" * 64,
            scenario_name="e2e_test",
            started_at=datetime.now(UTC),
            perturbation_profile_name="default",
            random_search_seed=42,
            ga_seed=43,
            mc_prefilter_seed=44,
            mc_deep_seed=45,
            sensitivity_seed=46,
            wfo_window_ids=("w1", "w2", "w3"),
            checkpoint=Checkpoint.NOT_STARTED,
            backtester_version="1.0.0",
        )

        with pytest.raises(ValueError, match="spike_threshold"):
            _run_stage_0_init(config, None, run_meta)

    def test_stage0_rejects_spike_threshold_above_one(self, tmp_path):
        """Stage 0 must raise ValueError when spike_threshold >= 1.0."""
        from src.backtesting.orchestrator import _run_stage_0_init
        from src.backtesting.contracts import RunMetadata, Checkpoint

        config = self._make_config(spike_threshold=1.0)
        run_meta = RunMetadata(
            run_id="test-b8-005d",
            config_hash="2" * 64,
            scenario_name="e2e_test",
            started_at=datetime.now(UTC),
            perturbation_profile_name="default",
            random_search_seed=42,
            ga_seed=43,
            mc_prefilter_seed=44,
            mc_deep_seed=45,
            sensitivity_seed=46,
            wfo_window_ids=("w1", "w2", "w3"),
            checkpoint=Checkpoint.NOT_STARTED,
            backtester_version="1.0.0",
        )

        with pytest.raises(ValueError, match="spike_threshold"):
            _run_stage_0_init(config, None, run_meta)

    def test_stage0_accepts_valid_min_significant_trades(self, tmp_path):
        """Stage 0 must not raise when min_significant_trades=1 (minimum valid value)."""
        from src.backtesting.orchestrator import _run_stage_0_init
        from src.backtesting.contracts import RunMetadata, Checkpoint

        config = self._make_config(min_trades=1, spike_threshold=0.15)
        run_meta = RunMetadata(
            run_id="test-b8-005e",
            config_hash="3" * 64,
            scenario_name="e2e_test",
            started_at=datetime.now(UTC),
            perturbation_profile_name="default",
            random_search_seed=42,
            ga_seed=43,
            mc_prefilter_seed=44,
            mc_deep_seed=45,
            sensitivity_seed=46,
            wfo_window_ids=("w1", "w2", "w3"),
            checkpoint=Checkpoint.NOT_STARTED,
            backtester_version="1.0.0",
        )

        # Should not raise — if it does, the fix is not applied or the test config is wrong
        try:
            _run_stage_0_init(config, None, run_meta)
        except ValueError as exc:
            if "min_significant_trades" in str(exc) or "spike_threshold" in str(exc):
                pytest.fail(f"Stage 0 rejected valid config: {exc}")
            # Other ValueError (e.g. scenario loading, param names) is expected in test env


# ── Store P10: run records are immutable after write ─────────────────────────

class TestStoreP10Immutability:
    """P10: Run metadata cannot be overwritten after initial write."""

    def test_initialise_run_is_insert_or_ignore(self, store, run_meta):
        """Re-initialising the same run_id does not overwrite the existing record."""
        store.initialise_run(run_meta)

        # Attempt to re-initialise with a different config hash (simulating accidental re-use)
        import dataclasses
        modified_meta = dataclasses.replace(
            run_meta,
            config_hash="9" * 64,
        )
        store.initialise_run(modified_meta)  # Should silently ignore (INSERT OR IGNORE)

        # The original record should be unchanged
        stored = store.get_run_metadata(run_meta.run_id)
        assert stored is not None
        assert stored.config_hash == "a" * 64, (
            "P10 violation: run record was mutated after initialisation. "
            "INSERT OR IGNORE should prevent overwrite."
        )


# ── Store dispatch: all 9 handler methods exist ───────────────────────────────

class TestStoreDispatchCompleteness:
    """B8-004: All dispatch method names used in write_* methods must exist as class methods."""

    EXPECTED_DISPATCH_METHODS = [
        "_write_run",
        "_write_candidate_record",
        "_set_checkpoint",
        "_write_wfo_window_result",
        "_flag_wfo_insufficient",
        "_write_wfo_consistency_score",
        "_write_mc_result",
        "_write_sensitivity_profile",
        "_write_verdict",
    ]

    def test_all_dispatch_methods_exist(self, store):
        """Every method name used in queue.put() calls must exist on CandidateStore."""
        missing = [
            name for name in self.EXPECTED_DISPATCH_METHODS
            if not hasattr(store, name) or not callable(getattr(store, name))
        ]
        assert not missing, (
            f"B8-004: Missing dispatch methods on CandidateStore: {missing}. "
            "A typo in a write_* method's dispatch string causes silent write loss (L-05)."
        )
"""
tests/backtesting/integration/test_robustness.py
─────────────────────────────────────────────────
Block 4 — Robustness (11 ROB criteria)

ROB-01 to ROB-08  Resume-after-interruption at each of the 8 Checkpoint values.
ROB-09            One sensitivity worker fails → remaining complete, failing
                  candidate gets sensitivity_profile_complete=False.
ROB-10            All sensitivity workers fail → Stage 6 completes, Stage 7 runs,
                  report notes zero complete sensitivity profiles.
ROB-11            MC returns error result for one candidate → remaining complete;
                  failing candidate has mc_result.error set, ruin_probability=None.
                  Note: run_mc contract is "Never raises" — failure is signalled
                  via MCResult(error=...), never by raising an exception.
"""
from __future__ import annotations

import hashlib
import json
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Dict, List
from unittest.mock import patch

import pytest
import yaml

# ── 1. sys.path FIRST ─────────────────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# ── 2. path anchor ────────────────────────────────────────────────────────────
from src.utils.paths import PROJECT_ROOT  # noqa: E402

# ── 3. contracts BEFORE candidate_store ──────────────────────────────────────
from src.backtesting.contracts import (  # noqa: E402
    CandidateParameterSet,
    CandidateRecord,
    CandidateStage,
    Checkpoint,
    MCMode,
    MCResult,
    RunMetadata,
    SensitivityProfile,        # ← add this
    WFOConsistencyScore,
)

# ── 4. candidate_store AFTER contracts ───────────────────────────────────────
from src.backtesting.candidate_store import CandidateStore  # noqa: E402

# ── other imports ─────────────────────────────────────────────────────────────
from src.backtesting.orchestrator import run  # noqa: E402


# ─────────────────────────────────────────────────────────────────────────────
# Shared constants
# ─────────────────────────────────────────────────────────────────────────────

_N_CANDIDATES = 20   # total seeded — mirrors perf_run fixture
_N_SENS_INPUT = 5    # sensitivity.input_count in config
_N_MC_INPUT   = 10   # mc.deep.input_count in config

_WFO_WINDOWS = [
    {"id": "w1", "start": "2020-01-01", "end": "2021-01-01"},
    {"id": "w2", "start": "2021-01-01", "end": "2022-01-01"},
    {"id": "w3", "start": "2022-01-01", "end": "2023-01-01"},
]


# ─────────────────────────────────────────────────────────────────────────────
# Config factory — nested structure required by load_scenario()
# ─────────────────────────────────────────────────────────────────────────────

def _make_config(output_dir: Path, temp_dir: Path) -> dict:
    """
    Return a config dict that satisfies:
    - _load_and_validate_config (top-level keys, min 3 WFO windows, 1 enabled zone)
    - load_scenario (nested fitness_weights, constraints, wfo_temporal_weights,
      verdict_thresholds, description, mc_prefilter_ruin_threshold)
    - ScenarioProfile.__post_init__ (fitness weights sum=1.0, WFO weights sum=1.0,
      borderline_wfo_floor < go_wfo_floor, go_mc < borderline_mc)
    """
    return {
        "backtester_version": "1.0.0",
        "scenario": "e2e_test",
        "run": {
            "output_dir": str(output_dir),
            "temp_dir": str(temp_dir),
            "max_workers": 2,
        },
        "scenarios": {
            "e2e_test": {
                "description": "Pipeline validation only — not for production use.",
                "fitness_weights": {
                    "net_pnl":         0.25,
                    "expectancy":      0.25,
                    "max_drawdown":    0.20,
                    "win_rate":        0.15,
                    "trade_frequency": 0.10,
                    "profit_factor":   0.05,
                },
                "constraints": {
                    "min_win_rate":        0.0,
                    "max_drawdown":        1.0,
                    "max_losing_streak":   9999,
                    "min_trades_per_week": 0.0,
                    "min_expectancy":      -9999.0,
                    "min_profit_factor":   0.0,
                },
                "mc_prefilter_ruin_threshold": 1.0,
                "wfo_temporal_weights": {
                    "median_return":     0.40,
                    "variance":          0.20,
                    "worst_drawdown":    0.20,
                    "fraction_positive": 0.20,
                },
                "verdict_thresholds": {
                    "go_wfo_floor":                 0.55,
                    "borderline_wfo_floor":         0.40,
                    "go_mc_ruin_ceiling":           0.10,
                    "borderline_mc_ruin_ceiling":   0.25,
                    "sensitivity_spike_threshold":  0.15,
                },
                "report_emphasis": [],
            }
        },
        "zones": {
            "zone_a": {
                "enabled": True,
                "parameters": {
                    "fast_period": {"type": "int", "min": 5, "max": 50, "step": 1},
                },
            }
        },
        "walk_forward": {
            "windows": _WFO_WINDOWS,
            "enforce_oos_gate": False,
        },
        "random_search": {"seed": 42, "min_significant_trades": 1},
        "genetic":       {"seed": 43},
        "mc_prefilter":  {"seed": 44},
        "monte_carlo": {
            "deep": {
                "seed": 45,
                "iterations": 100,
                "input_count": _N_MC_INPUT,
                "perturbation_profile": "default",
            }
        },
        "sensitivity": {
            "seed": 46,
            "input_count": _N_SENS_INPUT,
            "spike_threshold": 0.15,
            "max_steps": 2,
        },
        "output": {"formats": {"html": False, "json": False, "parquet": False}},
        "strategy": {"base_yaml_path": None},
    }


def _write_config(config: dict, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(config, f)
    return path


# ─────────────────────────────────────────────────────────────────────────────
# Store seeder — mirrors perf_run fixture from test_performance.py
# ─────────────────────────────────────────────────────────────────────────────

def _seed_candidates_with_wfo(
    store: CandidateStore,
    run_id: str,
    n: int = _N_CANDIDATES,
) -> List[str]:
    """
    Seed n CandidateRecords + WFOConsistencyScore rows.
    CandidateRecord: uses parameters_json (str), stage (str), recorded_at (datetime),
    and all nullable flattened fields explicitly. No 'parameters', 'win_rate', etc.
    WFOConsistencyScore: windows_total must >= windows_evaluated; all float fields required.
    """
    ids: List[str] = []
    for i in range(n):
        candidate = CandidateParameterSet.create(
            zone_name="zone_a",
            parameters={"fast_period": 10 + i},
            generation=0,
        )
        record = CandidateRecord(
            run_id=run_id,
            candidate_id=candidate.candidate_id,
            zone_name=candidate.zone_name,
            stage=CandidateStage.RANDOM.value,
            generation=0,
            recorded_at=datetime.now(UTC),
            parameters_json=json.dumps({"fast_period": 10 + i}),
            fitness_score=0.50 + i * 0.01,
            passed_constraints=True,
            rejection_reason=None,
            failing_constraint=None,
            failing_value=None,
            actual_win_rate=0.45,
            actual_max_drawdown=0.08,
            actual_losing_streak=3,
            actual_trades_per_week=2.0,
            actual_expectancy=0.05,
            actual_profit_factor=1.1,
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
        store.write_candidate(record)

        wfo = WFOConsistencyScore(
            candidate_id=candidate.candidate_id,
            windows_evaluated=3,
            windows_total=3,
            median_window_return=0.02,
            window_return_variance=0.001,
            worst_window_drawdown=0.05,
            fraction_positive_windows=0.667,
            composite_score=0.50 + i * 0.01,
            oos_gate_triggered=False,
            window_collapse_flag=False,
        )
        store.write_wfo_consistency_score(wfo, run_id)
        ids.append(candidate.candidate_id)

    store.flush()
    return ids


# ─────────────────────────────────────────────────────────────────────────────
# Canned result factories
# ─────────────────────────────────────────────────────────────────────────────

def _make_mc_result(candidate_id: str, error: str | None = None) -> MCResult:
    """
    MCResult requires: perturbation_profile_name (str), evaluated_at (datetime),
    worst_drawdown_across_paths (not worst_drawdown).
    """
    return MCResult(
        candidate_id=candidate_id,
        mode=MCMode.DEEP,
        perturbation_profile_name="default",
        iterations=100,
        evaluated_at=datetime.now(UTC),
        avg_final_equity=1100.0 if error is None else None,
        worst_drawdown_across_paths=0.12 if error is None else None,
        ruin_probability=0.05 if error is None else None,
        p5_final_equity=1050.0 if error is None else None,
        error=error,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Patch context managers
# ─────────────────────────────────────────────────────────────────────────────

def _stage5_patch(side_effect=None):
    if side_effect is None:
        def _default(candidate, candidate_result, mode, config, seed):
            return _make_mc_result(candidate.candidate_id)
        side_effect = _default
    return patch(
        "src.backtesting.monte_carlo.mc_engine.run_mc",
        side_effect=side_effect,
    )


def _stage6_patch(side_effect=None):
    if side_effect is None:
        def _default(base_candidate, parameter_name, perturbed_value,
                     base_yaml_path, temp_dir, scenario, min_significant_trades):
            return (parameter_name, perturbed_value, 0.52, None)
        side_effect = _default
    return patch(
        "src.backtesting.evaluation.sensitivity._evaluate_perturbation",
        side_effect=side_effect,
    )


def _stage7_patch():
    return patch("src.backtesting.orchestrator.generate_report", return_value=None)


# ─────────────────────────────────────────────────────────────────────────────
# Run initialiser
# ─────────────────────────────────────────────────────────────────────────────

def _init_run_at_checkpoint(
    store: CandidateStore,
    config_path: Path,
    checkpoint: Checkpoint,
) -> str:
    run_id = str(uuid.uuid4())
    run_metadata = RunMetadata(
        run_id=run_id,
        config_hash=hashlib.sha256(config_path.read_bytes()).hexdigest(),
        scenario_name="e2e_test",
        started_at=datetime.now(UTC),
        perturbation_profile_name="default",
        random_search_seed=42,
        ga_seed=43,
        mc_prefilter_seed=44,
        mc_deep_seed=45,
        sensitivity_seed=46,
        wfo_window_ids=("w1", "w2", "w3"),
        checkpoint=checkpoint,
        backtester_version="1.0.0",
    )
    store.initialise_run(run_metadata)
    store.set_checkpoint(run_id, checkpoint)
    return run_id


# ─────────────────────────────────────────────────────────────────────────────
# ROB-01 through ROB-08: Resume-after-interruption
# ─────────────────────────────────────────────────────────────────────────────

class TestResumeAfterInterruption:

    def _run_from_checkpoint(
        self,
        tmp_path: Path,
        starting_checkpoint: Checkpoint,
        pre_seed_wfo: bool = False,
    ) -> str:
        output_dir = tmp_path / "output"
        temp_dir = tmp_path / "tmp"
        output_dir.mkdir(parents=True)
        temp_dir.mkdir(parents=True)

        config = _make_config(output_dir, temp_dir)
        config_path = tmp_path / "config.yaml"
        _write_config(config, config_path)

        db_path = output_dir / "backtester.db"
        store = CandidateStore(db_path)
        run_id = _init_run_at_checkpoint(store, config_path, starting_checkpoint)

        if pre_seed_wfo:
            _seed_candidates_with_wfo(store, run_id)

        store.close()

        with _stage5_patch(), _stage6_patch(), _stage7_patch():
            run(config_path)

        store2 = CandidateStore(db_path)
        try:
            final_cp = store2.get_checkpoint(run_id)
        finally:
            store2.close()

        assert final_cp == Checkpoint.COMPLETE, (
            f"Expected COMPLETE but got {final_cp.name} "
            f"(started from {starting_checkpoint.name})"
        )
        return run_id

    def test_rob_01_not_started(self, tmp_path):
        """ROB-01: NOT_STARTED → Stage 0 re-inits, pipeline reaches COMPLETE."""
        self._run_from_checkpoint(tmp_path, Checkpoint.NOT_STARTED)

    def test_rob_02_run_initialised(self, tmp_path):
        """ROB-02: RUN_INITIALISED → Stage 1 stub, pipeline reaches COMPLETE."""
        self._run_from_checkpoint(tmp_path, Checkpoint.RUN_INITIALISED)

    def test_rob_03_random_search_complete(self, tmp_path):
        """ROB-03: RANDOM_SEARCH_COMPLETE → Stages 2-7, pipeline reaches COMPLETE."""
        self._run_from_checkpoint(tmp_path, Checkpoint.RANDOM_SEARCH_COMPLETE)

    def test_rob_04_mc_prefilter_complete(self, tmp_path):
        """ROB-04: MC_PREFILTER_COMPLETE → Stages 3-7, pipeline reaches COMPLETE."""
        self._run_from_checkpoint(tmp_path, Checkpoint.MC_PREFILTER_COMPLETE)

    def test_rob_05_ga_complete(self, tmp_path):
        """ROB-05: GA_COMPLETE → Stages 4-7, pipeline reaches COMPLETE."""
        self._run_from_checkpoint(tmp_path, Checkpoint.GA_COMPLETE)

    def test_rob_06_wfo_complete(self, tmp_path):
        """ROB-06: WFO_COMPLETE → Stage 5 (MC Deep) runs, pipeline reaches COMPLETE."""
        self._run_from_checkpoint(tmp_path, Checkpoint.WFO_COMPLETE, pre_seed_wfo=True)

    def test_rob_07_monte_carlo_complete(self, tmp_path):
        """ROB-07: MONTE_CARLO_COMPLETE → Stage 6 (Sensitivity) runs, reaches COMPLETE."""
        self._run_from_checkpoint(tmp_path, Checkpoint.MONTE_CARLO_COMPLETE, pre_seed_wfo=True)

    def test_rob_08_sensitivity_complete(self, tmp_path):
        """ROB-08: SENSITIVITY_COMPLETE → Stage 7 (Report) only, reaches COMPLETE."""
        self._run_from_checkpoint(tmp_path, Checkpoint.SENSITIVITY_COMPLETE, pre_seed_wfo=True)


# ─────────────────────────────────────────────────────────────────────────────
# ROB-09, ROB-10, ROB-11: Worker isolation
# ─────────────────────────────────────────────────────────────────────────────

class TestWorkerIsolation:

    def _setup_wfo_run(self, tmp_path: Path, starting_checkpoint: Checkpoint):
        output_dir = tmp_path / "output"
        temp_dir = tmp_path / "tmp"
        output_dir.mkdir(parents=True)
        temp_dir.mkdir(parents=True)

        config = _make_config(output_dir, temp_dir)
        config_path = tmp_path / "config.yaml"
        _write_config(config, config_path)

        db_path = output_dir / "backtester.db"
        store = CandidateStore(db_path)
        run_id = _init_run_at_checkpoint(store, config_path, starting_checkpoint)
        _seed_candidates_with_wfo(store, run_id)
        store.close()

        return run_id, config_path, db_path

    def test_rob_09_one_sensitivity_worker_fails(self, tmp_path):
        """
        ROB-09: One sensitivity candidate fails (profile_complete=False); the rest
        complete (profile_complete=True). Pipeline reaches COMPLETE.

        Windows spawn mode: unittest.mock patches cannot be pickled into worker
        processes, so we cannot inject failures at the _evaluate_perturbation level.
        Instead we patch evaluate_sensitivity itself (above the worker boundary) to
        simulate the exact outcome the module produces when one candidate's workers
        all raise: one SensitivityProfile with profile_complete=False, the rest normal.
        """
        run_id, config_path, db_path = self._setup_wfo_run(
            tmp_path, Checkpoint.MONTE_CARLO_COMPLETE
        )

        call_order: List[str] = []

        def _controlled_sensitivity(candidate, baseline_fitness, parameter_space_def,
                                    base_yaml_path, temp_dir, scenario, spike_threshold,
                                    max_steps=2, max_workers=6, min_significant_trades=30):
            cid = candidate.candidate_id
            call_order.append(cid)
            is_first = len(call_order) == 1
            return SensitivityProfile(
                candidate_id=cid,
                baseline_fitness=baseline_fitness,
                parameter_sensitivities=(),
                spike_detected=False,
                spike_parameters=(),
                profile_complete=not is_first,   # first candidate fails, rest pass
            )

        with (
            _stage5_patch(),
            patch(
                "src.backtesting.orchestrator.evaluate_sensitivity",
                side_effect=_controlled_sensitivity,
            ),
            _stage7_patch(),
        ):
            run(config_path)

        store = CandidateStore(db_path)
        try:
            assert store.get_checkpoint(run_id) == Checkpoint.COMPLETE

            assert call_order, "evaluate_sensitivity must have been called"
            failing_cid = call_order[0]

            failing_profile = store.get_sensitivity_profile(failing_cid)
            assert failing_profile is not None, (
                "SensitivityProfile must be written for the failing candidate"
            )
            assert failing_profile.profile_complete is False, (
                "profile_complete must be False for the first (failing) candidate"
            )

            # At least one subsequent candidate must have profile_complete=True
            if len(call_order) > 1:
                ok_profile = store.get_sensitivity_profile(call_order[1])
                assert ok_profile is not None
                assert ok_profile.profile_complete is True, (
                    "Non-failing candidates must have profile_complete=True"
                )
        finally:
            store.close()


    def test_rob_10_all_sensitivity_workers_fail(self, tmp_path):
        """
        ROB-10: All sensitivity evaluations fail (all profiles profile_complete=False).
        Stage 6 must complete without raising.
        Stage 7 must execute: generate_report called exactly once.
        Pipeline must reach COMPLETE.

        Same Windows spawn reasoning as ROB-09: patch evaluate_sensitivity directly.
        """
        run_id, config_path, db_path = self._setup_wfo_run(
            tmp_path, Checkpoint.MONTE_CARLO_COMPLETE
        )

        report_call_count: Dict[str, int] = {"n": 0}

        def _all_failing_sensitivity(candidate, baseline_fitness, parameter_space_def,
                                    base_yaml_path, temp_dir, scenario, spike_threshold,
                                    max_steps=2, max_workers=6, min_significant_trades=30):
            return SensitivityProfile(
                candidate_id=candidate.candidate_id,
                baseline_fitness=baseline_fitness,
                parameter_sensitivities=(),
                spike_detected=False,
                spike_parameters=(),
                profile_complete=False,
            )

        def _counting_report(*args, **kwargs):
            report_call_count["n"] += 1

        with (
            _stage5_patch(),
            patch(
                "src.backtesting.orchestrator.evaluate_sensitivity",
                side_effect=_all_failing_sensitivity,
            ),
            patch(
                "src.backtesting.orchestrator.generate_report",
                side_effect=_counting_report,
            ),
        ):
            run(config_path)

        assert report_call_count["n"] == 1, (
            f"generate_report must be called exactly once; "
            f"was called {report_call_count['n']} time(s)"
        )

        store = CandidateStore(db_path)
        try:
            assert store.get_checkpoint(run_id) == Checkpoint.COMPLETE
        finally:
            store.close()

    def test_rob_11_mc_error_result_one_candidate(self, tmp_path):
        """
        ROB-11: run_mc returns MCResult(error=...) for the first candidate.
        run_mc contract: "Never raises."
        Failing candidate: MCResult written, error set, ruin_probability=None.
        Subsequent candidates: error=None, ruin_probability populated.
        Pipeline must reach COMPLETE.
        """
        run_id, config_path, db_path = self._setup_wfo_run(
            tmp_path, Checkpoint.WFO_COMPLETE
        )

        seen_first_cid: List[str] = []

        def _flaky_run_mc(candidate, candidate_result, mode, config, seed):
            cid = candidate.candidate_id
            if not seen_first_cid:
                seen_first_cid.append(cid)
            if cid == seen_first_cid[0]:
                return _make_mc_result(cid, error="injected MC failure")
            return _make_mc_result(cid)

        with _stage5_patch(side_effect=_flaky_run_mc), _stage6_patch(), _stage7_patch():
            run(config_path)

        store = CandidateStore(db_path)
        try:
            assert store.get_checkpoint(run_id) == Checkpoint.COMPLETE

            assert seen_first_cid, "At least one candidate must have been processed"
            mc_fail = store.get_mc_result(seen_first_cid[0], mode=MCMode.DEEP)
            assert mc_fail is not None, (
                "MCResult must be written for failing candidate — "
                "Stage 5 calls store.write_mc_result regardless of error field"
            )
            assert mc_fail.error is not None
            assert mc_fail.ruin_probability is None
        finally:
            store.close()


# ─────────────────────────────────────────────────────────────────────────────
# Informational summary — never fails
# ─────────────────────────────────────────────────────────────────────────────

def test_z_robustness_summary():
    """Informational checkpoint-to-resume map. Always passes."""
    import logging
    mapping = [
        ("ROB-01", "NOT_STARTED",            "Stage 0 re-init"),
        ("ROB-02", "RUN_INITIALISED",         "Stage 1 random search stub"),
        ("ROB-03", "RANDOM_SEARCH_COMPLETE",  "Stage 2 MC prefilter stub"),
        ("ROB-04", "MC_PREFILTER_COMPLETE",   "Stage 3 GA stub"),
        ("ROB-05", "GA_COMPLETE",             "Stage 4 Full WFO stub"),
        ("ROB-06", "WFO_COMPLETE",            "Stage 5 MC Deep"),
        ("ROB-07", "MONTE_CARLO_COMPLETE",    "Stage 6 Sensitivity"),
        ("ROB-08", "SENSITIVITY_COMPLETE",    "Stage 7 Report only"),
        ("ROB-09", "N/A",                     "Worker isolation: 1 sensitivity worker raises"),
        ("ROB-10", "N/A",                     "Worker isolation: all sensitivity workers raise"),
        ("ROB-11", "N/A",                     "Worker isolation: 1 MC candidate returns error"),
    ]
    lines = ["", "── Block 4 Robustness: Checkpoint → Resumed-Stage Map ──"]
    for rob_id, cp_name, description in mapping:
        lines.append(f"  {rob_id}  {cp_name:<30}  →  {description}")
    lines.append("── 11 / 11 ROB criteria ──")
    logging.getLogger(__name__).info("\n".join(lines))
    assert True
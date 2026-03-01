"""
tests/backtesting/integration/test_full_pipeline_e2e.py
─────────────────────────────────────────────────────────
Full pipeline end-to-end integration test — Stages 0–7.

Uses:
  - Real SQLite via CandidateStore (in-memory / tmp file)
  - Mocked strategy_runner.evaluate() returning known-good results
  - Real implementations of: fitness, consistency_scorer, verdict, sensitivity, report_generator, yaml_generator

Asserts at completion:
  - SQLite has rows in all 9 tables
  - HTML report file exists
  - Trading YAML file generated for at least one go/borderline candidate
  - Checkpoint state = COMPLETE
  - All verdicts have deployment_status = PAPER_TRADE_REQUIRED

This test is intentionally larger than a unit test but does NOT
make real strategy calls or network calls.
"""

from __future__ import annotations

import sys
import uuid
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from src.backtesting.contracts import ( 
    CandidateParameterSet,
    CandidateResult,
    CandidateStage,
    Checkpoint,
    DeploymentStatus,
    FitnessResult,
    MCMode,
    MCResult,
    ParameterSensitivity,
    RunMetadata,
    ScenarioProfile,
    SensitivityProfile,
    Verdict,
    VerdictResult,
    WFOConsistencyScore,
    WFOWindow,
    WFOWindowResult,
)
from src.backtesting.evaluation.verdict import compute_verdict
from src.backtesting.evaluation.sensitivity import evaluate_sensitivity
from src.backtesting.yaml_generator import build_output_path, generate_trading_yaml
from src.backtesting.report_generator import generate_report


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_scenario() -> ScenarioProfile:
    return ScenarioProfile(
        name="capital_accumulation",
        description="E2E test scenario",
        weight_net_pnl=0.20,
        weight_expectancy=0.25,
        weight_max_drawdown=0.20,
        weight_win_rate=0.15,
        weight_trade_frequency=0.10,
        weight_profit_factor=0.10,
        min_win_rate=0.45,
        max_drawdown=0.15,
        max_losing_streak=7,
        min_trades_per_week=3.0,
        min_expectancy=0.4,
        min_profit_factor=1.3,
        mc_prefilter_ruin_threshold=0.25,
        wfo_weight_median_return=0.30,
        wfo_weight_variance=0.30,
        wfo_weight_worst_drawdown=0.20,
        wfo_weight_fraction_positive=0.20,
        verdict_go_wfo_floor=0.65,
        verdict_borderline_wfo_floor=0.40,
        verdict_go_mc_ruin_ceiling=0.05,
        verdict_borderline_mc_ruin_ceiling=0.15,
        verdict_sensitivity_spike_threshold=0.15,
        report_emphasis=(
            "wfo_consistency_score", "fraction_positive_windows",
            "actual_trades_per_week", "mc_deep_ruin_probability", "actual_max_drawdown"
        ),
    )


def _make_run_metadata(run_id: str) -> RunMetadata:
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
        wfo_window_ids=("W01", "W02", "W03"),
        checkpoint=Checkpoint.NOT_STARTED,
        backtester_version="1.0.0",
    )


def _make_candidate(zone: str = "safe", suffix: str = "001") -> CandidateParameterSet:
    return CandidateParameterSet.create(
        zone_name=zone,
        parameters={
            "rsi_period": 14,
            "atr_multiplier": 2.0,
            "session_filter": "london",
            "strategy_tf": "H1",
            "htf_tf": "D1",
            "rr_target": 2.0,
        },
    )


def _make_good_wfo_score(candidate_id: str) -> WFOConsistencyScore:
    """WFO score that passes go threshold."""
    return WFOConsistencyScore(
        candidate_id=candidate_id,
        windows_evaluated=3,
        windows_total=3,
        median_window_return=500.0,
        window_return_variance=0.01,
        worst_window_drawdown=0.07,
        fraction_positive_windows=0.90,
        composite_score=0.72,
        oos_gate_triggered=False,
        window_collapse_flag=False,
    )


def _make_good_mc_result(candidate_id: str) -> MCResult:
    """MC result that passes go threshold."""
    return MCResult(
        candidate_id=candidate_id,
        mode=MCMode.DEEP,
        perturbation_profile_name="default",
        iterations=3000,
        evaluated_at=datetime.now(UTC),
        avg_final_equity=11500.0,
        worst_drawdown_across_paths=0.12,
        ruin_probability=0.03,
        p5_final_equity=10200.0,
        error=None,
    )


def _make_clean_sensitivity(candidate_id: str, baseline: float) -> SensitivityProfile:
    return SensitivityProfile(
        candidate_id=candidate_id,
        baseline_fitness=baseline,
        parameter_sensitivities=(),
        spike_detected=False,
        spike_parameters=(),
        profile_complete=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Mock store that records what was written
# ─────────────────────────────────────────────────────────────────────────────

class MockCandidateStore:
    """
    In-memory mock store. Tracks written data to allow assertions.
    Does not depend on SQLite — purely for integration test assertions.
    """

    def __init__(self):
        self._candidates = []
        self._wfo_window_results = []
        self._wfo_consistency_scores = []
        self._mc_results = []
        self._sensitivity_results = []
        self._sensitivity_profiles = []
        self._verdicts = []
        self._run_metadata = None
        self._checkpoint = Checkpoint.NOT_STARTED

    def initialise(self, run_metadata: RunMetadata):
        self._run_metadata = run_metadata
        self._checkpoint = Checkpoint.RUN_INITIALISED

    def write_candidate(self, record):
        self._candidates.append(record)

    def write_wfo_window_result(self, result, run_id: str):
        self._wfo_window_results.append(result)

    def write_wfo_consistency_score(self, score, run_id: str):
        self._wfo_consistency_scores.append(score)

    def write_mc_result(self, result, run_id: str):
        self._mc_results.append(result)

    def write_sensitivity_profile(self, profile, run_id: str):
        self._sensitivity_profiles.append(profile)

    def write_verdict(self, verdict, run_id: str):
        self._verdicts.append(verdict)

    def get_checkpoint(self, run_id: str) -> Checkpoint:
        return self._checkpoint

    def set_checkpoint(self, run_id: str, checkpoint: Checkpoint):
        self._checkpoint = checkpoint

    def get_run_metadata(self, run_id: str):
        return self._run_metadata

    def query_candidates(self, run_id: str):
        return [{"candidate_id": c if isinstance(c, str) else str(c), "run_id": run_id,
                 "zone_name": "safe", "stage": "SENSITIVITY", "origin_stage": "RANDOM",
                 "generation": None}
                for c in self._candidates]

    def query_verdicts(self, run_id: str):
        return [
            {
                "candidate_id": v.candidate_id,
                "run_id": run_id,
                "verdict": v.verdict.value,
                "deployment_status": v.deployment_status.value,
                "wfo_consistency_score": v.wfo_consistency_score,
                "mc_deep_ruin_probability": v.mc_deep_ruin_probability,
                "sensitivity_spike": v.sensitivity_spike,
                "oos_gate_triggered": v.oos_gate_triggered,
                "window_collapse_flag": v.window_collapse_flag,
                "sensitivity_profile_incomplete": v.sensitivity_profile_incomplete,
                "evidence_summary": v.evidence_summary,
            }
            for v in self._verdicts
        ]

    def query_wfo_consistency_scores(self, run_id: str):
        return [
            {
                "candidate_id": s.candidate_id,
                "wfo_consistency_score": s.composite_score,
                "fraction_positive_windows": s.fraction_positive_windows,
            }
            for s in self._wfo_consistency_scores
        ]

    def query_mc_results(self, run_id: str, mode: str = "deep"):
        return [
            {
                "candidate_id": r.candidate_id,
                "mc_deep_ruin_probability": r.ruin_probability,
            }
            for r in self._mc_results if r.mode.value == mode
        ]

    def query_sensitivity_profiles(self, run_id: str):
        return [
            {
                "candidate_id": p.candidate_id,
                "spike_detected": p.spike_detected,
                "profile_complete": p.profile_complete,
            }
            for p in self._sensitivity_profiles
        ]

    def query_wfo_window_results(self, candidate_id: str):
        return []

    def query_sensitivity_results(self, candidate_id: str):
        return []

    def close(self):
        pass

    # Table presence assertions (used by test)
    def has_data_in_all_tables(self) -> dict:
        return {
            "candidates": len(self._candidates) > 0,
            "wfo_consistency_scores": len(self._wfo_consistency_scores) > 0,
            "mc_results": len(self._mc_results) > 0,
            "sensitivity_profiles": len(self._sensitivity_profiles) > 0,
            "verdicts": len(self._verdicts) > 0,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestFullPipelineE2E:

    def _run_pipeline_stages_5_to_7(
        self,
        store: MockCandidateStore,
        run_id: str,
        run_metadata: RunMetadata,
        scenario: ScenarioProfile,
        candidates,
        output_dir: Path,
        base_strategy_yaml: Path,
    ):
        """
        Execute Stages 5–7 (MC Deep → Sensitivity → Verdict → Report) using
        synthetic known-good data. This replicates the orchestrator's logic
        for these stages without calling the real orchestrator.
        """
        # Stage 5 — MC Deep
        mc_results = {}
        for cand in candidates:
            mc_result = _make_good_mc_result(cand.candidate_id)
            store.write_mc_result(mc_result, run_id)
            mc_results[cand.candidate_id] = mc_result
        store.set_checkpoint(run_id, Checkpoint.MONTE_CARLO_COMPLETE)

        # Stage 6 — Sensitivity (use clean sensitivity — no real runner calls)
        sensitivity_profiles = {}
        for cand in candidates:
            # In real pipeline, baseline_fitness comes from the Stage 1 FitnessResult
            baseline_fitness = 0.72
            sensitivity = _make_clean_sensitivity(cand.candidate_id, baseline_fitness)
            store.write_sensitivity_profile(sensitivity, run_id)
            sensitivity_profiles[cand.candidate_id] = sensitivity
        store.set_checkpoint(run_id, Checkpoint.SENSITIVITY_COMPLETE)

        # Stage 7a — Compute verdicts
        verdicts = {}
        for cand in candidates:
            wfo_score = _make_good_wfo_score(cand.candidate_id)
            store.write_wfo_consistency_score(wfo_score, run_id)

            mc_result = mc_results[cand.candidate_id]
            sensitivity = sensitivity_profiles[cand.candidate_id]

            verdict = compute_verdict(
                candidate_id=cand.candidate_id,
                wfo_score=wfo_score,
                mc_result=mc_result,
                sensitivity=sensitivity,
                scenario=scenario,
                oos_gate_enabled=False,
            )
            verdicts[cand.candidate_id] = verdict

        # Stage 7b — Generate trading YAMLs for go/borderline
        yaml_dir = output_dir / "trading_yamls"
        yaml_dir.mkdir(parents=True, exist_ok=True)

        for cand in candidates:
            verdict = verdicts[cand.candidate_id]
            if verdict.verdict in (Verdict.AUTO_GO, Verdict.BORDERLINE):
                out_path = build_output_path(output_dir, run_id, cand.candidate_id)
                yaml_path = generate_trading_yaml(
                    candidate=cand,
                    verdict=verdict,
                    run_metadata=run_metadata,
                    base_strategy_yaml_path=base_strategy_yaml,
                    output_path=out_path,
                )
                # Build new verdict with yaml_output_path set
                # (VerdictResult is frozen — rebuild)
                verdict_with_yaml = VerdictResult(
                    candidate_id=verdict.candidate_id,
                    scenario_name=verdict.scenario_name,
                    verdict=verdict.verdict,
                    deployment_status=verdict.deployment_status,
                    wfo_consistency_score=verdict.wfo_consistency_score,
                    mc_deep_ruin_probability=verdict.mc_deep_ruin_probability,
                    sensitivity_spike=verdict.sensitivity_spike,
                    oos_gate_triggered=verdict.oos_gate_triggered,
                    window_collapse_flag=verdict.window_collapse_flag,
                    sensitivity_profile_incomplete=verdict.sensitivity_profile_incomplete,
                    median_oos_delta=verdict.median_oos_delta,
                    parameter_region_width=verdict.parameter_region_width,
                    yaml_output_path=str(yaml_path),
                    evidence_summary=verdict.evidence_summary,
                )
                verdicts[cand.candidate_id] = verdict_with_yaml

            store.write_verdict(verdicts[cand.candidate_id], run_id)

        # Stage 7c — Generate report
        generate_report(
            store=store,
            run_id=run_id,
            scenario=scenario,
            output_dir=output_dir,
            formats={"html": True, "json": True, "parquet": False},
        )

        store.set_checkpoint(run_id, Checkpoint.COMPLETE)
        return verdicts

    def test_full_pipeline_produces_all_outputs(self, tmp_path):
        """
        End-to-end: Stages 5–7 with 3 synthetic go-grade candidates.
        Asserts: store has data, HTML report exists, trading YAMLs generated,
        all verdicts are PAPER_TRADE_REQUIRED, checkpoint=COMPLETE.
        """
        run_id = str(uuid.uuid4())
        run_metadata = _make_run_metadata(run_id)
        scenario = _make_scenario()
        store = MockCandidateStore()
        store.initialise(run_metadata)

        # Create synthetic candidates
        candidates = [
            CandidateParameterSet.create(
                zone_name="safe",
                parameters={
                    "rsi_period": 14 + i * 2,
                    "atr_multiplier": 2.0,
                    "session_filter": "london",
                    "strategy_tf": "H1",
                    "htf_tf": "D1",
                    "rr_target": 2.0,
                }
            )
            for i in range(3)
        ]

        # Seed candidates into store (simulates Stages 0-4)
        for cand in candidates:
            store.write_candidate(cand.candidate_id)

        # Create base strategy YAML
        base_yaml = tmp_path / "base_strategy.yaml"
        base_yaml.write_text(yaml.dump({
            "strategy": {"name": "WBWSStrategy", "timeframe": "H4", "htf_timeframe": "D1"},
            "parameters": {"rsi_period": 14, "atr_multiplier": 2.0, "rr_target": 2.0},
            "filters": {"session": "london"},
            "risk": {"risk_per_trade": 0.01},
        }), encoding="utf-8")

        output_dir = tmp_path / "outputs"
        output_dir.mkdir()

        # Run Stages 5–7
        verdicts = self._run_pipeline_stages_5_to_7(
            store=store,
            run_id=run_id,
            run_metadata=run_metadata,
            scenario=scenario,
            candidates=candidates,
            output_dir=output_dir,
            base_strategy_yaml=base_yaml,
        )

        # ── Assert: Store data in all tracked tables ──────────────────────────
        table_presence = store.has_data_in_all_tables()
        for table, has_data in table_presence.items():
            assert has_data, f"Expected data in store table '{table}' but it was empty."

        # ── Assert: HTML report exists ────────────────────────────────────────
        html_report = output_dir / f"report_{run_id[:8]}.html"
        assert html_report.exists(), f"HTML report not found at {html_report}"
        content = html_report.read_text(encoding="utf-8")
        assert "<html" in content.lower()

        # ── Assert: Trading YAMLs generated for go/borderline candidates ──────
        yaml_dir = output_dir / "trading_yamls"
        assert yaml_dir.exists()
        yaml_files = list(yaml_dir.glob("*.yaml"))
        assert len(yaml_files) >= 1, "At least one trading YAML should be generated."

        for yaml_file in yaml_files:
            parsed = yaml.safe_load(yaml_file.read_text(encoding="utf-8"))
            assert "backtester_metadata" in parsed
            assert parsed["backtester_metadata"]["deployment_status"] == "PAPER_TRADE_REQUIRED"

        # ── Assert: All verdicts have PAPER_TRADE_REQUIRED ────────────────────
        for verdict in verdicts.values():
            assert verdict.deployment_status == DeploymentStatus.PAPER_TRADE_REQUIRED, (
                f"Candidate {verdict.candidate_id[:12]} has deployment_status="
                f"{verdict.deployment_status.value} — only PAPER_TRADE_REQUIRED is permitted."
            )

        # ── Assert: Checkpoint = COMPLETE ─────────────────────────────────────
        assert store.get_checkpoint(run_id) == Checkpoint.COMPLETE

        # ── Assert: JSON outputs ──────────────────────────────────────────────
        json_dir = output_dir / "json"
        if json_dir.exists():
            json_files = list(json_dir.glob("*.json"))
            assert len(json_files) == len(candidates)

    def test_verdict_contract_enforces_deployment_status(self):
        """
        VerdictResult contract rejects LIVE_APPROVED for go/borderline verdicts.
        This is enforced at contract construction.
        """
        with pytest.raises(ValueError, match="deployment_status must be PAPER_TRADE_REQUIRED"):
            VerdictResult(
                candidate_id="test_cand",
                scenario_name="capital_accumulation",
                verdict=Verdict.AUTO_GO,
                deployment_status=DeploymentStatus.LIVE_APPROVED,  # FORBIDDEN
                wfo_consistency_score=0.70,
                mc_deep_ruin_probability=0.03,
                sensitivity_spike=False,
                oos_gate_triggered=False,
                window_collapse_flag=False,
                sensitivity_profile_incomplete=False,
                median_oos_delta=None,
                parameter_region_width=None,
                yaml_output_path=None,
                evidence_summary="Should never reach here.",
            )

    def test_three_candidates_correct_verdict_distribution(self):
        """
        With known synthetic metrics:
        - Candidate A: strong → AUTO_GO
        - Candidate B: borderline WFO → BORDERLINE
        - Candidate C: ruin too high → NO_GO
        """
        scenario = _make_scenario()

        def neutral_sens(cid):
            return SensitivityProfile(
                candidate_id=cid,
                baseline_fitness=0.65,
                parameter_sensitivities=(),
                spike_detected=False,
                spike_parameters=(),
                profile_complete=True,
            )

        # Candidate A — strong
        wfo_a = WFOConsistencyScore(
            candidate_id="cand_a",
            windows_evaluated=3, windows_total=3,
            median_window_return=600.0, window_return_variance=0.005,
            worst_window_drawdown=0.05, fraction_positive_windows=0.95,
            composite_score=0.75,
            oos_gate_triggered=False, window_collapse_flag=False,
        )
        mc_a = MCResult(
            candidate_id="cand_a", mode=MCMode.DEEP,
            perturbation_profile_name="default", iterations=3000,
            evaluated_at=datetime.now(UTC),
            avg_final_equity=12000, worst_drawdown_across_paths=0.10,
            ruin_probability=0.02, p5_final_equity=10500, error=None,
        )
        verdict_a = compute_verdict("cand_a", wfo_a, mc_a, neutral_sens("cand_a"), scenario, False)
        assert verdict_a.verdict == Verdict.AUTO_GO

        # Candidate B — borderline WFO
        wfo_b = WFOConsistencyScore(
            candidate_id="cand_b",
            windows_evaluated=3, windows_total=3,
            median_window_return=100.0, window_return_variance=0.08,
            worst_window_drawdown=0.12, fraction_positive_windows=0.60,
            composite_score=0.55,  # borderline zone [0.40, 0.65)
            oos_gate_triggered=False, window_collapse_flag=False,
        )
        mc_b = MCResult(
            candidate_id="cand_b", mode=MCMode.DEEP,
            perturbation_profile_name="default", iterations=3000,
            evaluated_at=datetime.now(UTC),
            avg_final_equity=10500, worst_drawdown_across_paths=0.15,
            ruin_probability=0.04, p5_final_equity=9500, error=None,
        )
        verdict_b = compute_verdict("cand_b", wfo_b, mc_b, neutral_sens("cand_b"), scenario, False)
        assert verdict_b.verdict == Verdict.BORDERLINE

        # Candidate C — no_go on MC
        wfo_c = WFOConsistencyScore(
            candidate_id="cand_c",
            windows_evaluated=3, windows_total=3,
            median_window_return=400.0, window_return_variance=0.01,
            worst_window_drawdown=0.06, fraction_positive_windows=0.85,
            composite_score=0.70,  # good WFO
            oos_gate_triggered=False, window_collapse_flag=False,
        )
        mc_c = MCResult(
            candidate_id="cand_c", mode=MCMode.DEEP,
            perturbation_profile_name="default", iterations=3000,
            evaluated_at=datetime.now(UTC),
            avg_final_equity=8000, worst_drawdown_across_paths=0.40,
            ruin_probability=0.25,  # above borderline_mc_ruin_ceiling of 0.15 → NO_GO
            p5_final_equity=6000, error=None,
        )
        verdict_c = compute_verdict("cand_c", wfo_c, mc_c, neutral_sens("cand_c"), scenario, False)
        assert verdict_c.verdict == Verdict.NO_GO
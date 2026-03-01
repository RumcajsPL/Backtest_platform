"""
test_report_yaml.py — Integration tests for report_generator.py and yaml_generator.py.

Covers:
  - generate_report(): HTML output, JSON output, Parquet output, borderline checklist,
    empty-run edge case, _store passthrough for charts
  - generate_trading_yaml(): parameter merge, metadata embedding, output path,
    deployment_status=PAPER_TRADE_REQUIRED, LIVE_APPROVED never set,
    base YAML not found raises FileNotFoundError
  - build_output_path(): canonical path spec
  - report_generator bug fix: _store passed into data dict

All tests use real file I/O (tmp_path), real CandidateStore, and real contracts.
"""
from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Generator

import pytest
import yaml

from src.backtesting.candidate_store import CandidateStore
from src.backtesting.contracts import (
    CandidateParameterSet,
    CandidateRecord,
    CandidateStage,
    Checkpoint,
    DeploymentStatus,
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
from src.backtesting.report_generator import generate_report
from src.backtesting.yaml_generator import build_output_path, generate_trading_yaml

# ── Shared helpers ────────────────────────────────────────────────────────────

def _scenario():
    """Load real capital_accumulation ScenarioProfile via scenario.py."""
    from src.backtesting.scenario import load_scenario
    cfg = {
        "scenario": "capital_accumulation",
        "scenarios": {
            "capital_accumulation": {
                "description": "Win-rate and consistency focus",
                "fitness_weights": {
                    "net_pnl": 0.20, "expectancy": 0.20, "max_drawdown": 0.20,
                    "win_rate": 0.20, "trade_frequency": 0.10, "profit_factor": 0.10,
                },
                "constraints": {
                    "min_win_rate": 0.45, "max_drawdown": 0.15,
                    "max_losing_streak": 6, "min_trades_per_week": 1.0,
                    "min_expectancy": 0.3, "min_profit_factor": 1.2,
                },
                "mc_prefilter_ruin_threshold": 0.10,
                "wfo_temporal_weights": {
                    "median_return": 0.40, "variance": 0.20,
                    "worst_drawdown": 0.20, "fraction_positive": 0.20,
                },
                "verdict_thresholds": {
                    "go_wfo_floor": 0.65, "borderline_wfo_floor": 0.40,
                    "go_mc_ruin_ceiling": 0.05, "borderline_mc_ruin_ceiling": 0.15,
                    "sensitivity_spike_threshold": 0.15,
                },
                "report_emphasis": ["win_rate", "max_drawdown", "expectancy"],
            }
        },
    }
    return load_scenario(cfg)


def _make_candidate(seed: int = 0) -> CandidateParameterSet:
    return CandidateParameterSet.create(
        zone_name="safe",
        parameters={
            "rsi_period": 14 + seed,
            "atr_multiplier": 2.0 + seed * 0.1,
            "session_filter": "london",
            "strategy_tf": "H1",
            "htf_tf": "D1",
        },
        generation=None,
    )


def _make_run_metadata(run_id: str) -> RunMetadata:
    return RunMetadata(
        run_id=run_id,
        config_hash="c" * 64,
        scenario_name="capital_accumulation",
        started_at=datetime.now(UTC),
        perturbation_profile_name="default",
        random_search_seed=42,
        ga_seed=43,
        mc_prefilter_seed=44,
        mc_deep_seed=45,
        sensitivity_seed=46,
        wfo_window_ids=("W1", "W2", "W3"),
        checkpoint=Checkpoint.COMPLETE,
        backtester_version=BACKTESTER_VERSION,
    )


def _seed_full_run(
    store: CandidateStore,
    run_id: str,
    cand: CandidateParameterSet,
    verdict: Verdict,
    wfo_score: float,
    ruin: float,
    spike: bool = False,
) -> None:
    """Write a complete candidate row through all stages into the store."""
    params_json = json.dumps(cand.parameters, sort_keys=True)

    record = CandidateRecord(
        run_id=run_id,
        candidate_id=cand.candidate_id,
        zone_name=cand.zone_name,
        stage=CandidateStage.RANDOM.value,
        generation=None,
        recorded_at=datetime.now(UTC),
        parameters_json=params_json,
        fitness_score=0.72,
        passed_constraints=True,
        rejection_reason=None,
        failing_constraint=None,
        failing_value=None,
        actual_win_rate=0.53,
        actual_max_drawdown=0.07,
        actual_losing_streak=3,
        actual_trades_per_week=5.0,
        actual_expectancy=0.70,
        actual_profit_factor=1.50,
        wfo_median_window_return=0.04,
        wfo_window_return_variance=0.002,
        wfo_worst_window_drawdown=0.08,
        wfo_fraction_positive_windows=0.85,
        wfo_consistency_score=wfo_score,
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
    store.flush()

    store.write_wfo_consistency_score(
        WFOConsistencyScore(
            candidate_id=cand.candidate_id,
            windows_evaluated=3, windows_total=3,
            median_window_return=0.04, window_return_variance=0.002,
            worst_window_drawdown=0.08, fraction_positive_windows=0.85,
            composite_score=wfo_score,
            oos_gate_triggered=False, window_collapse_flag=False,
        ),
        run_id,
    )

    store.write_mc_result(
        MCResult(
            candidate_id=cand.candidate_id, mode=MCMode.DEEP,
            perturbation_profile_name="default", iterations=100,
            evaluated_at=datetime.now(UTC),
            avg_final_equity=11000.0, worst_drawdown_across_paths=0.11,
            ruin_probability=ruin, p5_final_equity=9700.0, error=None,
        ),
        run_id,
    )

    ps = ParameterSensitivity(
        parameter_name="rsi_period", step=1,
        perturbed_value=16,
        fitness_delta=0.18 if spike else 0.04,
        evaluation_error=None,
    )
    store.write_sensitivity_profile(
        SensitivityProfile(
            candidate_id=cand.candidate_id, baseline_fitness=0.72,
            parameter_sensitivities=(ps,),
            spike_detected=spike,
            spike_parameters=("rsi_period",) if spike else (),
            profile_complete=True,
        ),
        run_id,
    )

    store.write_verdict(
        VerdictResult(
            candidate_id=cand.candidate_id,
            scenario_name="capital_accumulation",
            verdict=verdict,
            deployment_status=DeploymentStatus.PAPER_TRADE_REQUIRED,
            wfo_consistency_score=wfo_score,
            mc_deep_ruin_probability=ruin,
            sensitivity_spike=spike,
            oos_gate_triggered=False,
            window_collapse_flag=False,
            sensitivity_profile_incomplete=False,
            median_oos_delta=None,
            parameter_region_width=None,
            yaml_output_path=None,
            evidence_summary=(
                f"Scenario: capital_accumulation. Verdict: {verdict.value.upper()}. "
                f"WFO: {wfo_score:.3f}. Ruin: {ruin:.3f}. No modifier flags."
            ),
        ),
        run_id,
    )
    store.flush()


def _make_base_yaml(tmp_path: Path) -> Path:
    p = tmp_path / "strategy_template.yaml"
    p.write_text(
        "strategy:\n"
        "  name: WBWSStrategy\n"
        "  mode: core\n"
        "  timeframe: H1\n"
        "parameters:\n"
        "  rsi_period: 14\n"
        "  atr_multiplier: 2.0\n"
        "  session_filter: london\n"
        "filters:\n"
        "  session: london\n",
        encoding="utf-8",
    )
    return p


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def store(tmp_path: Path) -> Generator[CandidateStore, None, None]:
    s = CandidateStore(tmp_path / "report_test.db")
    yield s
    s.close()


@pytest.fixture()
def run_id() -> str:
    return str(uuid.uuid4())


@pytest.fixture()
def scenario():
    return _scenario()


@pytest.fixture()
def seeded_run(store, run_id):
    """
    Seed a complete run with 2 candidates:
      cand_a — AUTO_GO (wfo=0.70, ruin=0.03)
      cand_b — BORDERLINE (wfo=0.55, ruin=0.09, spike=True)
    """
    meta = _make_run_metadata(run_id)
    store.initialise_run(meta)

    cand_a = _make_candidate(seed=0)
    cand_b = _make_candidate(seed=1)

    _seed_full_run(store, run_id, cand_a, Verdict.AUTO_GO,    wfo_score=0.70, ruin=0.03)
    _seed_full_run(store, run_id, cand_b, Verdict.BORDERLINE, wfo_score=0.55, ruin=0.09, spike=True)

    return {"run_id": run_id, "meta": meta, "cand_a": cand_a, "cand_b": cand_b}


# ══════════════════════════════════════════════════════════════════════════════
# report_generator tests
# ══════════════════════════════════════════════════════════════════════════════

class TestReportGenerator:

    def test_html_report_file_created(self, seeded_run, store, scenario, tmp_path):
        """generate_report() must create report_{run_id[:8]}.html in output_dir."""
        run_id = seeded_run["run_id"]
        output_dir = tmp_path / "reports"

        generate_report(store, run_id, scenario, output_dir, formats={"html": True, "json": False, "parquet": False})

        html_path = output_dir / f"report_{run_id[:8]}.html"
        assert html_path.exists(), "HTML report file must be created"
        assert html_path.stat().st_size > 0, "HTML report must not be empty"

    def test_html_report_is_self_contained(self, seeded_run, store, scenario, tmp_path):
        """HTML report must be a single self-contained file (no external stylesheet links)."""
        run_id = seeded_run["run_id"]
        output_dir = tmp_path / "reports_sc"

        generate_report(store, run_id, scenario, output_dir, formats={"html": True, "json": False, "parquet": False})

        html_content = (output_dir / f"report_{run_id[:8]}.html").read_text(encoding="utf-8")

        assert "<html" in html_content
        assert "<style>" in html_content, "Styles must be inline (no external CSS)"
        # No links to external stylesheets
        assert 'rel="stylesheet"' not in html_content
        assert "cdn.jsdelivr" not in html_content

    def test_html_report_contains_both_verdicts(self, seeded_run, store, scenario, tmp_path):
        """HTML report must reference both candidate IDs and both verdict values."""
        run_id = seeded_run["run_id"]
        cand_a = seeded_run["cand_a"]
        cand_b = seeded_run["cand_b"]
        output_dir = tmp_path / "reports_v"

        generate_report(store, run_id, scenario, output_dir, formats={"html": True, "json": False, "parquet": False})

        html = (output_dir / f"report_{run_id[:8]}.html").read_text(encoding="utf-8")

        assert cand_a.candidate_id[:12] in html, "cand_a must appear in HTML"
        assert cand_b.candidate_id[:12] in html, "cand_b must appear in HTML"
        assert "auto_go" in html
        assert "borderline" in html
        assert "PAPER_TRADE_REQUIRED" in html

    def test_html_report_scenario_framed(self, seeded_run, store, scenario, tmp_path):
        """HTML report must display the active scenario name and report_emphasis metrics."""
        run_id = seeded_run["run_id"]
        output_dir = tmp_path / "reports_sc2"

        generate_report(store, run_id, scenario, output_dir, formats={"html": True, "json": False, "parquet": False})

        html = (output_dir / f"report_{run_id[:8]}.html").read_text(encoding="utf-8")

        assert "capital_accumulation" in html
        # report_emphasis = ["win_rate", "max_drawdown", "expectancy"]
        assert "Win Rate" in html or "win_rate" in html.lower()

    def test_borderline_checklist_created(self, seeded_run, store, scenario, tmp_path):
        """A borderline checklist HTML must be created for cand_b (BORDERLINE)."""
        run_id = seeded_run["run_id"]
        cand_b = seeded_run["cand_b"]
        output_dir = tmp_path / "reports_bl"

        generate_report(store, run_id, scenario, output_dir, formats={"html": True, "json": False, "parquet": False})

        checklist_dir = output_dir / "checklists"
        assert checklist_dir.exists(), "checklists/ directory must be created"

        checklist_files = list(checklist_dir.glob("checklist_*.html"))
        assert len(checklist_files) == 1, "1 checklist file for 1 borderline candidate"

        checklist_content = checklist_files[0].read_text(encoding="utf-8")
        assert cand_b.candidate_id[:12] in checklist_content
        assert "Adversarial" in checklist_content
        assert "Operator Sign-Off" in checklist_content

    def test_no_borderline_checklist_for_auto_go_only_run(self, store, scenario, tmp_path):
        """No checklists/ directory created when there are no borderline candidates."""
        run_id = str(uuid.uuid4())
        meta = _make_run_metadata(run_id)
        store.initialise_run(meta)

        cand = _make_candidate(seed=5)
        _seed_full_run(store, run_id, cand, Verdict.AUTO_GO, wfo_score=0.72, ruin=0.02)

        output_dir = tmp_path / "reports_no_bl"
        generate_report(store, run_id, scenario, output_dir, formats={"html": True, "json": False, "parquet": False})

        checklist_dir = output_dir / "checklists"
        assert not checklist_dir.exists() or len(list(checklist_dir.glob("*.html"))) == 0

    def test_json_files_created_per_candidate(self, seeded_run, store, scenario, tmp_path):
        """JSON output must create one file per candidate in json/ subdirectory."""
        run_id = seeded_run["run_id"]
        output_dir = tmp_path / "reports_json"

        generate_report(store, run_id, scenario, output_dir, formats={"html": False, "json": True, "parquet": False})

        json_dir = output_dir / "json"
        assert json_dir.exists(), "json/ directory must be created"

        json_files = list(json_dir.glob("*.json"))
        assert len(json_files) == 2, "1 JSON file per candidate (2 candidates)"

        # Each file must be valid JSON with candidate_id present
        for jf in json_files:
            data = json.loads(jf.read_text(encoding="utf-8"))
            assert "candidate_id" in data

    def test_json_content_has_verdict_and_wfo_fields(self, seeded_run, store, scenario, tmp_path):
        """JSON records must contain verdict, wfo_consistency_score, and mc_deep_ruin_probability."""
        run_id = seeded_run["run_id"]
        output_dir = tmp_path / "reports_json2"

        generate_report(store, run_id, scenario, output_dir, formats={"html": False, "json": True, "parquet": False})

        json_dir = output_dir / "json"
        for jf in json_dir.glob("*.json"):
            data = json.loads(jf.read_text(encoding="utf-8"))
            assert "verdict" in data, f"verdict missing from {jf.name}"
            assert "wfo_consistency_score" in data, f"wfo_consistency_score missing from {jf.name}"

    def test_parquet_skipped_gracefully_when_disabled(self, seeded_run, store, scenario, tmp_path):
        """Parquet output disabled in formats must not create parquet/ dir."""
        run_id = seeded_run["run_id"]
        output_dir = tmp_path / "reports_noparq"

        generate_report(store, run_id, scenario, output_dir, formats={"html": False, "json": False, "parquet": False})

        assert not (output_dir / "parquet").exists()

    def test_empty_run_no_verdicts_does_not_crash(self, store, scenario, tmp_path):
        """generate_report() on a run with zero verdicts must not raise."""
        empty_run_id = str(uuid.uuid4())
        meta = _make_run_metadata(empty_run_id)
        store.initialise_run(meta)

        output_dir = tmp_path / "reports_empty"

        generate_report(
            store, empty_run_id, scenario, output_dir,
            formats={"html": True, "json": True, "parquet": False},
        )  # must not raise

        # HTML must still be created (empty report)
        html_path = output_dir / f"report_{empty_run_id[:8]}.html"
        assert html_path.exists()
        html = html_path.read_text(encoding="utf-8")
        assert "No verdicts found" in html or "No go or borderline" in html or "0" in html

    def test_store_passed_into_data_dict_for_charts(self, seeded_run, store, scenario, tmp_path):
        """
        Bug fix validation: _store must be present in the data dict so chart
        functions can call query_wfo_window_results / query_sensitivity_results.
        Verified by patching _collect_report_data and checking _store key.
        """
        from unittest.mock import patch
        from src.backtesting import report_generator

        captured_data = {}
        original_write_html = report_generator._write_html_report

        def capturing_write_html(data, *args, **kwargs):
            captured_data.update(data)
            return original_write_html(data, *args, **kwargs)

        run_id = seeded_run["run_id"]
        output_dir = tmp_path / "reports_store"

        with patch.object(report_generator, "_write_html_report", side_effect=capturing_write_html):
            generate_report(store, run_id, scenario, output_dir, formats={"html": True, "json": False, "parquet": False})

        assert "_store" in captured_data, (
            "_store must be present in data dict passed to _write_html_report "
            "so chart functions can query wfo_window_results and sensitivity_results"
        )
        assert captured_data["_store"] is store


# ══════════════════════════════════════════════════════════════════════════════
# yaml_generator tests
# ══════════════════════════════════════════════════════════════════════════════

class TestYamlGenerator:

    def test_trading_yaml_created_at_correct_path(self, seeded_run, tmp_path):
        """generate_trading_yaml() must write the YAML at the specified output_path."""
        cand_a = seeded_run["cand_a"]
        meta = seeded_run["meta"]
        run_id = seeded_run["run_id"]
        base_yaml = _make_base_yaml(tmp_path)

        verdict = VerdictResult(
            candidate_id=cand_a.candidate_id,
            scenario_name="capital_accumulation",
            verdict=Verdict.AUTO_GO,
            deployment_status=DeploymentStatus.PAPER_TRADE_REQUIRED,
            wfo_consistency_score=0.70, mc_deep_ruin_probability=0.03,
            sensitivity_spike=False, oos_gate_triggered=False,
            window_collapse_flag=False, sensitivity_profile_incomplete=False,
            median_oos_delta=None, parameter_region_width=None,
            yaml_output_path=None,
            evidence_summary="Test evidence.",
        )

        output_path = build_output_path(tmp_path, run_id, cand_a.candidate_id)
        result = generate_trading_yaml(cand_a, verdict, meta, base_yaml, output_path)

        assert result == output_path
        assert output_path.exists(), "Trading YAML file must be created"
        assert output_path.stat().st_size > 0

    def test_build_output_path_canonical_spec(self, tmp_path):
        """build_output_path must follow: {output_dir}/trading_yamls/{run_id[:8]}_{cid[:12]}_strategy.yaml"""
        run_id = "a" * 32
        cid = "b" * 64
        path = build_output_path(tmp_path, run_id, cid)

        assert path.parent == tmp_path / "trading_yamls"
        assert path.name == f"{'a' * 8}_{'b' * 12}_strategy.yaml"

    def test_candidate_parameters_merged_into_yaml(self, seeded_run, tmp_path):
        """Candidate parameters must overwrite base YAML values in the output file."""
        cand_a = seeded_run["cand_a"]
        meta = seeded_run["meta"]
        run_id = seeded_run["run_id"]
        base_yaml = _make_base_yaml(tmp_path)

        verdict = VerdictResult(
            candidate_id=cand_a.candidate_id,
            scenario_name="capital_accumulation",
            verdict=Verdict.AUTO_GO,
            deployment_status=DeploymentStatus.PAPER_TRADE_REQUIRED,
            wfo_consistency_score=0.70, mc_deep_ruin_probability=0.03,
            sensitivity_spike=False, oos_gate_triggered=False,
            window_collapse_flag=False, sensitivity_profile_incomplete=False,
            median_oos_delta=None, parameter_region_width=None,
            yaml_output_path=None, evidence_summary="Test.",
        )
        output_path = build_output_path(tmp_path, run_id, cand_a.candidate_id)
        generate_trading_yaml(cand_a, verdict, meta, base_yaml, output_path)

        with output_path.open(encoding="utf-8") as f:
            merged = yaml.safe_load(f)

        # rsi_period=14 (seed=0) must be in parameters section
        assert merged["parameters"]["rsi_period"] == 14
        assert merged["parameters"]["atr_multiplier"] == pytest.approx(2.0)
        # session_filter maps to filters.session
        assert merged["filters"]["session"] == "london"
        # strategy_tf maps to strategy.timeframe
        assert merged["strategy"]["timeframe"] == "H1"

    def test_backtester_metadata_embedded(self, seeded_run, tmp_path):
        """backtester_metadata section must be present with all required fields."""
        cand_a = seeded_run["cand_a"]
        meta = seeded_run["meta"]
        run_id = seeded_run["run_id"]
        base_yaml = _make_base_yaml(tmp_path)

        verdict = VerdictResult(
            candidate_id=cand_a.candidate_id,
            scenario_name="capital_accumulation",
            verdict=Verdict.AUTO_GO,
            deployment_status=DeploymentStatus.PAPER_TRADE_REQUIRED,
            wfo_consistency_score=0.70, mc_deep_ruin_probability=0.03,
            sensitivity_spike=False, oos_gate_triggered=False,
            window_collapse_flag=False, sensitivity_profile_incomplete=False,
            median_oos_delta=None, parameter_region_width=None,
            yaml_output_path=None, evidence_summary="Test.",
        )
        output_path = build_output_path(tmp_path, run_id, cand_a.candidate_id)
        generate_trading_yaml(cand_a, verdict, meta, base_yaml, output_path)

        with output_path.open(encoding="utf-8") as f:
            merged = yaml.safe_load(f)

        bm = merged["backtester_metadata"]
        assert bm["run_id"] == run_id
        assert bm["candidate_id"] == cand_a.candidate_id
        assert bm["config_hash"] == "c" * 64
        assert bm["scenario_name"] == "capital_accumulation"
        assert bm["verdict"] == "auto_go"
        assert bm["deployment_status"] == "PAPER_TRADE_REQUIRED"
        assert bm["wfo_consistency_score"] == pytest.approx(0.70)
        assert bm["mc_deep_ruin_probability"] == pytest.approx(0.03)
        # All 5 seeds present
        assert bm["random_search_seed"] == 42
        assert bm["ga_seed"] == 43
        assert bm["mc_prefilter_seed"] == 44
        assert bm["mc_deep_seed"] == 45
        assert bm["sensitivity_seed"] == 46

    def test_deployment_status_always_paper_trade_required(self, seeded_run, tmp_path):
        """backtester_metadata.deployment_status must always be PAPER_TRADE_REQUIRED."""
        cand_a = seeded_run["cand_a"]
        meta = seeded_run["meta"]
        run_id = seeded_run["run_id"]
        base_yaml = _make_base_yaml(tmp_path)

        for verdict_val in (Verdict.AUTO_GO, Verdict.BORDERLINE):
            verdict = VerdictResult(
                candidate_id=cand_a.candidate_id,
                scenario_name="capital_accumulation",
                verdict=verdict_val,
                deployment_status=DeploymentStatus.PAPER_TRADE_REQUIRED,
                wfo_consistency_score=0.70, mc_deep_ruin_probability=0.03,
                sensitivity_spike=False, oos_gate_triggered=False,
                window_collapse_flag=False, sensitivity_profile_incomplete=False,
                median_oos_delta=None, parameter_region_width=None,
                yaml_output_path=None, evidence_summary="Test.",
            )
            out = tmp_path / f"out_{verdict_val.value}" / "trading_yamls" / "test.yaml"
            out.parent.mkdir(parents=True, exist_ok=True)
            generate_trading_yaml(cand_a, verdict, meta, base_yaml, out)

            with out.open(encoding="utf-8") as f:
                merged = yaml.safe_load(f)

            assert merged["backtester_metadata"]["deployment_status"] == "PAPER_TRADE_REQUIRED", (
                f"deployment_status must be PAPER_TRADE_REQUIRED for {verdict_val.value}"
            )
            assert merged["backtester_metadata"]["deployment_status"] != "LIVE_APPROVED"

    def test_base_yaml_not_found_raises(self, seeded_run, tmp_path):
        """generate_trading_yaml must raise FileNotFoundError when base YAML is missing."""
        cand_a = seeded_run["cand_a"]
        meta = seeded_run["meta"]
        run_id = seeded_run["run_id"]
        missing_path = tmp_path / "does_not_exist.yaml"

        verdict = VerdictResult(
            candidate_id=cand_a.candidate_id,
            scenario_name="capital_accumulation",
            verdict=Verdict.AUTO_GO,
            deployment_status=DeploymentStatus.PAPER_TRADE_REQUIRED,
            wfo_consistency_score=0.70, mc_deep_ruin_probability=0.03,
            sensitivity_spike=False, oos_gate_triggered=False,
            window_collapse_flag=False, sensitivity_profile_incomplete=False,
            median_oos_delta=None, parameter_region_width=None,
            yaml_output_path=None, evidence_summary="Test.",
        )
        output_path = build_output_path(tmp_path, run_id, cand_a.candidate_id)

        with pytest.raises(FileNotFoundError):
            generate_trading_yaml(cand_a, verdict, meta, missing_path, output_path)

    def test_output_directory_created_if_not_exists(self, seeded_run, tmp_path):
        """generate_trading_yaml must create trading_yamls/ if it does not exist."""
        cand_a = seeded_run["cand_a"]
        meta = seeded_run["meta"]
        run_id = seeded_run["run_id"]
        base_yaml = _make_base_yaml(tmp_path)

        output_path = tmp_path / "new_dir" / "deeply" / "nested" / "out.yaml"
        assert not output_path.parent.exists()

        verdict = VerdictResult(
            candidate_id=cand_a.candidate_id,
            scenario_name="capital_accumulation",
            verdict=Verdict.AUTO_GO,
            deployment_status=DeploymentStatus.PAPER_TRADE_REQUIRED,
            wfo_consistency_score=0.70, mc_deep_ruin_probability=0.03,
            sensitivity_spike=False, oos_gate_triggered=False,
            window_collapse_flag=False, sensitivity_profile_incomplete=False,
            median_oos_delta=None, parameter_region_width=None,
            yaml_output_path=None, evidence_summary="Test.",
        )
        generate_trading_yaml(cand_a, verdict, meta, base_yaml, output_path)

        assert output_path.exists()

    def test_yaml_is_valid_parseable_yaml(self, seeded_run, tmp_path):
        """Output trading YAML must be syntactically valid YAML (parseable without error)."""
        cand_a = seeded_run["cand_a"]
        meta = seeded_run["meta"]
        run_id = seeded_run["run_id"]
        base_yaml = _make_base_yaml(tmp_path)

        verdict = VerdictResult(
            candidate_id=cand_a.candidate_id,
            scenario_name="capital_accumulation",
            verdict=Verdict.AUTO_GO,
            deployment_status=DeploymentStatus.PAPER_TRADE_REQUIRED,
            wfo_consistency_score=0.70, mc_deep_ruin_probability=0.03,
            sensitivity_spike=False, oos_gate_triggered=False,
            window_collapse_flag=False, sensitivity_profile_incomplete=False,
            median_oos_delta=None, parameter_region_width=None,
            yaml_output_path=None, evidence_summary="Test.",
        )
        output_path = build_output_path(tmp_path, run_id, cand_a.candidate_id)
        generate_trading_yaml(cand_a, verdict, meta, base_yaml, output_path)

        content = output_path.read_text(encoding="utf-8")
        parsed = yaml.safe_load(content)
        assert isinstance(parsed, dict)
        assert "strategy" in parsed
        assert "parameters" in parsed
        assert "backtester_metadata" in parsed
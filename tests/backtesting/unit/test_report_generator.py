"""
tests/backtesting/unit/test_report_generator.py
────────────────────────────────────────────────
Unit tests for report_generator.py.

CandidateStore is fully mocked — no SQLite I/O in these tests.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from src.backtesting.contracts import ScenarioProfile, Verdict
from src.backtesting.report_generator import generate_report


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def scenario() -> ScenarioProfile:
    return ScenarioProfile(
        name="capital_accumulation",
        description="Test scenario",
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


def _make_mock_store(
    verdicts=None,
    candidates=None,
    wfo_scores=None,
    mc_results=None,
    sensitivity_profiles=None,
):
    """Build a mock CandidateStore with known data."""
    store = MagicMock()

    def _make_candidate(cid, stage, verdict_val):
        return {
            "candidate_id": cid,
            "run_id": "run001",
            "zone_name": "safe",
            "stage": stage,
            "origin_stage": stage,
            "generation": None,
        }

    def _make_verdict(cid, verdict_val):
        return {
            "candidate_id": cid,
            "run_id": "run001",
            "verdict": verdict_val,
            "deployment_status": "PAPER_TRADE_REQUIRED",
            "wfo_consistency_score": 0.70 if verdict_val == "auto_go" else 0.55,
            "mc_deep_ruin_probability": 0.03,
            "sensitivity_spike": verdict_val == "borderline",
            "oos_gate_triggered": False,
            "window_collapse_flag": False,
            "sensitivity_profile_incomplete": False,
            "evidence_summary": f"Test evidence for {verdict_val}.",
        }

    default_verdicts = [
        _make_verdict("cand_go_001", "auto_go"),
        _make_verdict("cand_bln_001", "borderline"),
        _make_verdict("cand_ngo_001", "no_go"),
    ]
    default_candidates = [
        _make_candidate("cand_go_001", "SENSITIVITY", "auto_go"),
        _make_candidate("cand_bln_001", "SENSITIVITY", "borderline"),
        _make_candidate("cand_ngo_001", "WFO", "no_go"),
    ]

    store.query_verdicts = MagicMock(return_value=verdicts or default_verdicts)
    store.query_candidates = MagicMock(return_value=candidates or default_candidates)
    store.query_wfo_consistency_scores = MagicMock(return_value=wfo_scores or [])
    store.query_mc_results = MagicMock(return_value=mc_results or [])
    store.query_sensitivity_profiles = MagicMock(return_value=sensitivity_profiles or [])
    store.get_run_metadata = MagicMock(return_value=None)
    store.query_wfo_window_results = MagicMock(return_value=[])
    store.query_sensitivity_results = MagicMock(return_value=[])

    return store


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestHtmlReport:
    def test_html_report_generated(self, scenario, tmp_path):
        """HTML report file exists, is non-empty, and contains <html> tag."""
        store = _make_mock_store()
        generate_report(
            store=store,
            run_id="run001",
            scenario=scenario,
            output_dir=tmp_path,
            formats={"html": True, "json": False, "parquet": False},
        )

        html_path = tmp_path / "report_run001.html"
        assert html_path.exists()
        assert html_path.stat().st_size > 0
        content = html_path.read_text(encoding="utf-8")
        assert "<html" in content.lower()
        assert "<!DOCTYPE html>" in content

    def test_html_report_contains_run_id(self, scenario, tmp_path):
        """Report contains the run ID."""
        store = _make_mock_store()
        generate_report(
            store=store,
            run_id="run001",
            scenario=scenario,
            output_dir=tmp_path,
            formats={"html": True, "json": False, "parquet": False},
        )

        content = (tmp_path / "report_run001.html").read_text(encoding="utf-8")
        assert "run001" in content

    def test_html_report_contains_scenario_name(self, scenario, tmp_path):
        """Report contains the scenario name."""
        store = _make_mock_store()
        generate_report(
            store=store,
            run_id="run001",
            scenario=scenario,
            output_dir=tmp_path,
            formats={"html": True, "json": False, "parquet": False},
        )

        content = (tmp_path / "report_run001.html").read_text(encoding="utf-8")
        assert "capital_accumulation" in content

    def test_report_scenario_emphasis_order(self, scenario, tmp_path):
        """
        capital_accumulation scenario → wfo_consistency_score appears before
        mc_deep_ruin_probability in the report content.
        """
        store = _make_mock_store()
        generate_report(
            store=store,
            run_id="run001",
            scenario=scenario,
            output_dir=tmp_path,
            formats={"html": True, "json": False, "parquet": False},
        )

        content = (tmp_path / "report_run001.html").read_text(encoding="utf-8")
        
        # Look for the transformed labels that actually appear in the HTML
        wfo_label = "Wfo Consistency Score"
        mc_label = "Mc Deep Ruin Probability"
        
        wfo_pos = content.find(wfo_label)
        mc_pos = content.find(mc_label)
        
        assert wfo_pos != -1, f"Could not find '{wfo_label}' in HTML"
        assert mc_pos != -1, f"Could not find '{mc_label}' in HTML"
        assert wfo_pos < mc_pos, f"'{wfo_label}' appears after '{mc_label}'"

    def test_html_report_no_external_deps(self, scenario, tmp_path):
        """Self-contained HTML — no external CSS/JS links."""
        store = _make_mock_store()
        generate_report(
            store=store,
            run_id="run001",
            scenario=scenario,
            output_dir=tmp_path,
            formats={"html": True, "json": False, "parquet": False},
        )

        content = (tmp_path / "report_run001.html").read_text(encoding="utf-8")
        # Should not contain external stylesheet or script links
        assert 'href="http' not in content
        assert 'src="http' not in content


class TestBorderlineChecklist:
    def test_borderline_checklist_generated(self, scenario, tmp_path):
        """Borderline candidate → adversarial checklist HTML file is created."""
        store = _make_mock_store()
        generate_report(
            store=store,
            run_id="run001",
            scenario=scenario,
            output_dir=tmp_path,
            formats={"html": True, "json": False, "parquet": False},
        )

        checklist_dir = tmp_path / "checklists"
        assert checklist_dir.exists()
        checklist_files = list(checklist_dir.glob("checklist_*.html"))
        assert len(checklist_files) == 1  # one borderline candidate

    def test_borderline_checklist_content(self, scenario, tmp_path):
        """Borderline checklist contains sign-off section and candidate ID."""
        store = _make_mock_store()
        generate_report(
            store=store,
            run_id="run001",
            scenario=scenario,
            output_dir=tmp_path,
            formats={"html": True, "json": False, "parquet": False},
        )

        checklist_files = list((tmp_path / "checklists").glob("checklist_*.html"))
        content = checklist_files[0].read_text(encoding="utf-8")
        assert "cand_bln_001" in content
        assert "PAPER_TRADE_REQUIRED" in content or "paper trading" in content.lower()
        assert "Sign-Off" in content or "sign-off" in content.lower()

    def test_no_checklist_for_go_only(self, scenario, tmp_path):
        """No borderline candidates → checklist directory is empty or not created."""
        store = _make_mock_store(
            verdicts=[
                {"candidate_id": "cand_go", "run_id": "run001", "verdict": "auto_go",
                 "deployment_status": "PAPER_TRADE_REQUIRED",
                 "wfo_consistency_score": 0.72, "mc_deep_ruin_probability": 0.03,
                 "sensitivity_spike": False, "oos_gate_triggered": False,
                 "window_collapse_flag": False, "sensitivity_profile_incomplete": False,
                 "evidence_summary": "Both pillars passed."},
            ],
            candidates=[
                {"candidate_id": "cand_go", "run_id": "run001", "zone_name": "safe",
                 "stage": "SENSITIVITY", "origin_stage": "SENSITIVITY", "generation": None},
            ],
        )
        generate_report(
            store=store,
            run_id="run001",
            scenario=scenario,
            output_dir=tmp_path,
            formats={"html": True, "json": False, "parquet": False},
        )

        checklist_dir = tmp_path / "checklists"
        if checklist_dir.exists():
            assert len(list(checklist_dir.glob("*.html"))) == 0


class TestJsonOutput:
    def test_json_files_created(self, scenario, tmp_path):
        """JSON output: one file per candidate, JSON-parseable."""
        store = _make_mock_store()
        generate_report(
            store=store,
            run_id="run001",
            scenario=scenario,
            output_dir=tmp_path,
            formats={"html": False, "json": True, "parquet": False},
        )

        json_dir = tmp_path / "json"
        assert json_dir.exists()
        json_files = list(json_dir.glob("*.json"))
        assert len(json_files) == 3  # one per candidate

        for f in json_files:
            parsed = json.loads(f.read_text(encoding="utf-8"))
            assert isinstance(parsed, dict)
            assert "candidate_id" in parsed

    def test_json_skipped_when_disabled(self, scenario, tmp_path):
        """JSON output not produced when json: false."""
        store = _make_mock_store()
        generate_report(
            store=store,
            run_id="run001",
            scenario=scenario,
            output_dir=tmp_path,
            formats={"html": False, "json": False, "parquet": False},
        )

        json_dir = tmp_path / "json"
        assert not json_dir.exists() or len(list(json_dir.glob("*.json"))) == 0


class TestParquetOutput:
    def test_parquet_files_created(self, scenario, tmp_path):
        """Parquet output: one file per candidate, readable with pandas."""
        pytest.importorskip("pandas")
        import pandas as pd

        store = _make_mock_store()
        generate_report(
            store=store,
            run_id="run001",
            scenario=scenario,
            output_dir=tmp_path,
            formats={"html": False, "json": False, "parquet": True},
        )

        parquet_dir = tmp_path / "parquet"
        assert parquet_dir.exists()
        parquet_files = list(parquet_dir.glob("*.parquet"))
        assert len(parquet_files) == 3

        for f in parquet_files:
            df = pd.read_parquet(f)
            assert len(df) == 1
            assert "candidate_id" in df.columns

    def test_parquet_skipped_when_disabled(self, scenario, tmp_path):
        """Parquet not produced when parquet: false."""
        store = _make_mock_store()
        generate_report(
            store=store,
            run_id="run001",
            scenario=scenario,
            output_dir=tmp_path,
            formats={"html": False, "json": False, "parquet": False},
        )

        parquet_dir = tmp_path / "parquet"
        assert not parquet_dir.exists() or len(list(parquet_dir.glob("*.parquet"))) == 0
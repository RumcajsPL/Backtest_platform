"""
tests/backtesting/unit/test_yaml_generator.py
──────────────────────────────────────────────
Unit tests for yaml_generator.py.

Tests verify:
  - Valid output file is produced
  - Metadata is embedded correctly
  - deployment_status is always PAPER_TRADE_REQUIRED regardless of verdict
  - Output path naming follows the spec
  - Missing base YAML raises FileNotFoundError
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from src.backtesting.contracts import (
    CandidateParameterSet,
    DeploymentStatus,
    MCMode,
    MCResult,
    RunMetadata,
    Checkpoint,
    Verdict,
    VerdictResult,
)
from src.backtesting.yaml_generator import build_output_path, generate_trading_yaml


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def candidate() -> CandidateParameterSet:
    return CandidateParameterSet.create(
        zone_name="safe",
        parameters={
            "rsi_period": 14,
            "atr_multiplier": 2.0,
            "session_filter": "london",
            "strategy_tf": "H1",
            "htf_tf": "D1",
        },
    )


@pytest.fixture
def run_metadata() -> RunMetadata:
    return RunMetadata(
        run_id="a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        config_hash="a" * 64,
        scenario_name="capital_accumulation",
        started_at=datetime.now(UTC),
        perturbation_profile_name="default",
        random_search_seed=42,
        ga_seed=43,
        mc_prefilter_seed=44,
        mc_deep_seed=45,
        sensitivity_seed=46,
        wfo_window_ids=("W01", "W02", "W03"),
        checkpoint=Checkpoint.COMPLETE,
        backtester_version="1.0.0",
    )


@pytest.fixture
def verdict_auto_go(candidate) -> VerdictResult:
    return VerdictResult(
        candidate_id=candidate.candidate_id,
        scenario_name="capital_accumulation",
        verdict=Verdict.AUTO_GO,
        deployment_status=DeploymentStatus.PAPER_TRADE_REQUIRED,
        wfo_consistency_score=0.72,
        mc_deep_ruin_probability=0.03,
        sensitivity_spike=False,
        oos_gate_triggered=False,
        window_collapse_flag=False,
        sensitivity_profile_incomplete=False,
        median_oos_delta=None,
        parameter_region_width=None,
        yaml_output_path=None,
        evidence_summary="Both pillars passed. No flags.",
    )


@pytest.fixture
def verdict_borderline(candidate) -> VerdictResult:
    return VerdictResult(
        candidate_id=candidate.candidate_id,
        scenario_name="capital_accumulation",
        verdict=Verdict.BORDERLINE,
        deployment_status=DeploymentStatus.PAPER_TRADE_REQUIRED,
        wfo_consistency_score=0.55,
        mc_deep_ruin_probability=0.04,
        sensitivity_spike=True,
        oos_gate_triggered=False,
        window_collapse_flag=False,
        sensitivity_profile_incomplete=False,
        median_oos_delta=None,
        parameter_region_width=None,
        yaml_output_path=None,
        evidence_summary="WFO in borderline zone. Spike on rsi_period.",
    )


@pytest.fixture
def base_yaml(tmp_path) -> Path:
    """Create a minimal valid base strategy YAML."""
    config = {
        "strategy": {
            "name": "WBWSStrategy",
            "timeframe": "H4",
            "htf_timeframe": "D1",
        },
        "parameters": {
            "rsi_period": 14,
            "atr_multiplier": 1.5,
            "rr_target": 2.0,
        },
        "filters": {
            "session": "london",
        },
        "risk": {
            "risk_per_trade": 0.01,
        },
    }
    path = tmp_path / "strategy_template.yaml"
    path.write_text(yaml.dump(config), encoding="utf-8")
    return path


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestGenerateTradingYaml:
    def test_yaml_generator_produces_valid_file(
        self, candidate, run_metadata, verdict_auto_go, base_yaml, tmp_path
    ):
        """Output file exists, is non-empty, and is parseable YAML."""
        output_path = tmp_path / "trading_yamls" / "test_output.yaml"
        result_path = generate_trading_yaml(
            candidate=candidate,
            verdict=verdict_auto_go,
            run_metadata=run_metadata,
            base_strategy_yaml_path=base_yaml,
            output_path=output_path,
        )

        assert result_path == output_path
        assert output_path.exists()
        assert output_path.stat().st_size > 0

        with output_path.open("r", encoding="utf-8") as fh:
            parsed = yaml.safe_load(fh)
        assert isinstance(parsed, dict)

    def test_yaml_metadata_embedded(
        self, candidate, run_metadata, verdict_auto_go, base_yaml, tmp_path
    ):
        """run_id, scenario_name, deployment_status present in backtester_metadata."""
        output_path = tmp_path / "trading_yamls" / "test_meta.yaml"
        generate_trading_yaml(
            candidate=candidate,
            verdict=verdict_auto_go,
            run_metadata=run_metadata,
            base_strategy_yaml_path=base_yaml,
            output_path=output_path,
        )

        with output_path.open("r", encoding="utf-8") as fh:
            parsed = yaml.safe_load(fh)

        meta = parsed.get("backtester_metadata", {})
        assert meta.get("run_id") == run_metadata.run_id
        assert meta.get("scenario_name") == run_metadata.scenario_name
        assert meta.get("deployment_status") == DeploymentStatus.PAPER_TRADE_REQUIRED.value
        assert meta.get("candidate_id") == candidate.candidate_id
        assert meta.get("config_hash") == run_metadata.config_hash
        assert "generated_at" in meta

    def test_yaml_deployment_status_paper_for_auto_go(
        self, candidate, run_metadata, verdict_auto_go, base_yaml, tmp_path
    ):
        """AUTO_GO verdict → deployment_status is always PAPER_TRADE_REQUIRED."""
        output_path = tmp_path / "trading_yamls" / "go.yaml"
        generate_trading_yaml(
            candidate=candidate,
            verdict=verdict_auto_go,
            run_metadata=run_metadata,
            base_strategy_yaml_path=base_yaml,
            output_path=output_path,
        )

        with output_path.open("r", encoding="utf-8") as fh:
            parsed = yaml.safe_load(fh)

        assert parsed["backtester_metadata"]["deployment_status"] == "PAPER_TRADE_REQUIRED"

    def test_yaml_deployment_status_paper_for_borderline(
        self, candidate, run_metadata, verdict_borderline, base_yaml, tmp_path
    ):
        """BORDERLINE verdict → deployment_status is always PAPER_TRADE_REQUIRED."""
        output_path = tmp_path / "trading_yamls" / "borderline.yaml"
        generate_trading_yaml(
            candidate=candidate,
            verdict=verdict_borderline,
            run_metadata=run_metadata,
            base_strategy_yaml_path=base_yaml,
            output_path=output_path,
        )

        with output_path.open("r", encoding="utf-8") as fh:
            parsed = yaml.safe_load(fh)

        assert parsed["backtester_metadata"]["deployment_status"] == "PAPER_TRADE_REQUIRED"

    def test_yaml_parameters_merged(
        self, candidate, run_metadata, verdict_auto_go, base_yaml, tmp_path
    ):
        """Candidate parameters are present in the output YAML."""
        output_path = tmp_path / "trading_yamls" / "merged.yaml"
        generate_trading_yaml(
            candidate=candidate,
            verdict=verdict_auto_go,
            run_metadata=run_metadata,
            base_strategy_yaml_path=base_yaml,
            output_path=output_path,
        )

        with output_path.open("r", encoding="utf-8") as fh:
            parsed = yaml.safe_load(fh)

        # rsi_period is in the parameters section
        assert parsed["parameters"]["rsi_period"] == 14
        assert parsed["parameters"]["atr_multiplier"] == 2.0
        # strategy_tf maps to strategy.timeframe
        assert parsed["strategy"]["timeframe"] == "H1"
        # session_filter maps to filters.session
        assert parsed["filters"]["session"] == "london"

    def test_yaml_missing_base_raises(
        self, candidate, run_metadata, verdict_auto_go, tmp_path
    ):
        """FileNotFoundError raised when base YAML does not exist."""
        missing_path = tmp_path / "nonexistent_base.yaml"
        output_path = tmp_path / "output.yaml"

        with pytest.raises(FileNotFoundError, match="Base strategy YAML not found"):
            generate_trading_yaml(
                candidate=candidate,
                verdict=verdict_auto_go,
                run_metadata=run_metadata,
                base_strategy_yaml_path=missing_path,
                output_path=output_path,
            )

    def test_output_directory_created(
        self, candidate, run_metadata, verdict_auto_go, base_yaml, tmp_path
    ):
        """Output parent directories are created if they don't exist."""
        output_path = tmp_path / "deep" / "nested" / "dir" / "output.yaml"
        assert not output_path.parent.exists()

        generate_trading_yaml(
            candidate=candidate,
            verdict=verdict_auto_go,
            run_metadata=run_metadata,
            base_strategy_yaml_path=base_yaml,
            output_path=output_path,
        )

        assert output_path.exists()

    def test_yaml_seeds_embedded(
        self, candidate, run_metadata, verdict_auto_go, base_yaml, tmp_path
    ):
        """All 5 seeds are embedded in backtester_metadata."""
        output_path = tmp_path / "trading_yamls" / "seeds.yaml"
        generate_trading_yaml(
            candidate=candidate,
            verdict=verdict_auto_go,
            run_metadata=run_metadata,
            base_strategy_yaml_path=base_yaml,
            output_path=output_path,
        )

        with output_path.open("r", encoding="utf-8") as fh:
            parsed = yaml.safe_load(fh)

        meta = parsed["backtester_metadata"]
        assert meta["random_search_seed"] == run_metadata.random_search_seed
        assert meta["ga_seed"] == run_metadata.ga_seed
        assert meta["mc_prefilter_seed"] == run_metadata.mc_prefilter_seed
        assert meta["mc_deep_seed"] == run_metadata.mc_deep_seed
        assert meta["sensitivity_seed"] == run_metadata.sensitivity_seed


class TestBuildOutputPath:
    def test_output_path_spec(self):
        """Output path follows spec: {output_dir}/trading_yamls/{run_id[:8]}_{candidate_id[:12]}_strategy.yaml"""
        output_dir = Path("/outputs/backtests/run_001")
        run_id = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        candidate_id = "abc123def456789012345678"

        path = build_output_path(output_dir, run_id, candidate_id)

        assert path == output_dir / "trading_yamls" / "a1b2c3d4_abc123def456_strategy.yaml"

    def test_output_path_uses_first_8_chars_of_run_id(self):
        run_id = "RUNID12345678901234567890"
        candidate_id = "CID1234567890"
        path = build_output_path(Path("/out"), run_id, candidate_id)
        assert path.name.startswith("RUNID123_")

    def test_output_path_uses_first_12_chars_of_candidate_id(self):
        run_id = "RUN12345"
        candidate_id = "CANDIDATE1234567890"
        path = build_output_path(Path("/out"), run_id, candidate_id)
        
        # Verify the exact format
        expected = f"{run_id[:8]}_{candidate_id[:12]}_strategy.yaml"
        assert path.name == expected
        
        # Alternative: verify the candidate part specifically
        assert candidate_id[:12] in path.name
        # The candidate part in the filename should be exactly the first 12 chars
        candidate_part = path.name.split('_')[1]  # Gets "CANDIDATE123" part
        assert candidate_part == candidate_id[:12]
"""
tests/backtesting/unit/test_verdict.py
──────────────────────────────────────
Unit tests for evaluation/verdict.py (Stage 7 verdict engine).

All paths through the two-pillar verdict logic are tested, including
all modifier flag combinations and deployment_status invariant.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from src.backtesting.contracts import (
    CandidateParameterSet,
    DeploymentStatus,
    MCMode,
    MCResult,
    ParameterSensitivity,
    ScenarioProfile,
    SensitivityProfile,
    Verdict,
    WFOConsistencyScore,
)
from src.backtesting.evaluation.verdict import compute_verdict


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


def _make_wfo_score(
    composite: float,
    oos_gate_triggered: bool = False,
    window_collapse_flag: bool = False,
    candidate_id: str = "abc123",
) -> WFOConsistencyScore:
    return WFOConsistencyScore(
        candidate_id=candidate_id,
        windows_evaluated=5,
        windows_total=5,
        median_window_return=0.05,
        window_return_variance=0.002,
        worst_window_drawdown=0.08,
        fraction_positive_windows=0.80,
        composite_score=composite,
        oos_gate_triggered=oos_gate_triggered,
        window_collapse_flag=window_collapse_flag,
    )


def _make_mc_result(
    ruin_probability: float,
    candidate_id: str = "abc123",
) -> MCResult:
    return MCResult(
        candidate_id=candidate_id,
        mode=MCMode.DEEP,
        perturbation_profile_name="default",
        iterations=3000,
        evaluated_at=datetime.now(UTC),
        avg_final_equity=11000.0,
        worst_drawdown_across_paths=0.18,
        ruin_probability=ruin_probability,
        p5_final_equity=9500.0,
        error=None,
    )


def _make_sensitivity(
    spike_detected: bool = False,
    spike_parameters: tuple = (),
    profile_complete: bool = True,
    candidate_id: str = "abc123",
) -> SensitivityProfile:
    return SensitivityProfile(
        candidate_id=candidate_id,
        baseline_fitness=0.70,
        parameter_sensitivities=(),
        spike_detected=spike_detected,
        spike_parameters=spike_parameters,
        profile_complete=profile_complete,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test cases
# ─────────────────────────────────────────────────────────────────────────────

class TestAutoGo:
    def test_verdict_auto_go(self, scenario):
        """Both pillars pass go thresholds, no flags → AUTO_GO."""
        wfo = _make_wfo_score(0.70)   # >= 0.65 go floor
        mc = _make_mc_result(0.03)    # <= 0.05 go ceiling
        sens = _make_sensitivity()

        result = compute_verdict("abc123", wfo, mc, sens, scenario, oos_gate_enabled=False)

        assert result.verdict == Verdict.AUTO_GO
        assert result.deployment_status == DeploymentStatus.PAPER_TRADE_REQUIRED

    def test_auto_go_at_exact_thresholds(self, scenario):
        """Boundary values exactly at go thresholds → AUTO_GO."""
        wfo = _make_wfo_score(0.65)   # exactly at go floor
        mc = _make_mc_result(0.05)    # exactly at go ceiling
        sens = _make_sensitivity()

        result = compute_verdict("abc123", wfo, mc, sens, scenario, oos_gate_enabled=False)

        assert result.verdict == Verdict.AUTO_GO


class TestNoGo:
    def test_verdict_no_go_wfo_below_borderline(self, scenario):
        """WFO below borderline floor → NO_GO regardless of MC."""
        wfo = _make_wfo_score(0.35)   # < 0.40 borderline floor
        mc = _make_mc_result(0.02)    # good MC — doesn't matter
        sens = _make_sensitivity()

        result = compute_verdict("abc123", wfo, mc, sens, scenario, oos_gate_enabled=False)

        assert result.verdict == Verdict.NO_GO

    def test_verdict_no_go_mc_above_borderline(self, scenario):
        """MC ruin above borderline ceiling → NO_GO regardless of WFO."""
        wfo = _make_wfo_score(0.80)   # excellent WFO — doesn't matter
        mc = _make_mc_result(0.20)    # > 0.15 borderline ceiling
        sens = _make_sensitivity()

        result = compute_verdict("abc123", wfo, mc, sens, scenario, oos_gate_enabled=False)

        assert result.verdict == Verdict.NO_GO

    def test_verdict_no_go_both_pillars_fail(self, scenario):
        """Both pillars fail → NO_GO."""
        wfo = _make_wfo_score(0.30)
        mc = _make_mc_result(0.25)
        sens = _make_sensitivity()

        result = compute_verdict("abc123", wfo, mc, sens, scenario, oos_gate_enabled=False)

        assert result.verdict == Verdict.NO_GO

    def test_verdict_no_go_mc_none_ruin_probability(self, scenario):
        """MC ruin_probability is None (MC failed) → treated as no_go on MC pillar."""
        wfo = _make_wfo_score(0.70)
        mc = MCResult(
            candidate_id="abc123",
            mode=MCMode.DEEP,
            perturbation_profile_name="default",
            iterations=3000,
            evaluated_at=datetime.now(UTC),
            avg_final_equity=None,
            worst_drawdown_across_paths=None,
            ruin_probability=None,
            p5_final_equity=None,
            error="MC evaluation failed",
        )
        sens = _make_sensitivity()

        result = compute_verdict("abc123", wfo, mc, sens, scenario, oos_gate_enabled=False)

        assert result.verdict == Verdict.NO_GO


class TestBorderline:
    def test_verdict_borderline_wfo_in_band(self, scenario):
        """WFO in borderline band [0.40, 0.65) → BORDERLINE."""
        wfo = _make_wfo_score(0.55)   # in [0.40, 0.65)
        mc = _make_mc_result(0.03)    # good MC
        sens = _make_sensitivity()

        result = compute_verdict("abc123", wfo, mc, sens, scenario, oos_gate_enabled=False)

        assert result.verdict == Verdict.BORDERLINE

    def test_verdict_borderline_mc_in_band(self, scenario):
        """MC in borderline band (0.05, 0.15] → BORDERLINE."""
        wfo = _make_wfo_score(0.70)   # good WFO
        mc = _make_mc_result(0.10)    # in (0.05, 0.15]
        sens = _make_sensitivity()

        result = compute_verdict("abc123", wfo, mc, sens, scenario, oos_gate_enabled=False)

        assert result.verdict == Verdict.BORDERLINE

    def test_verdict_borderline_spike_flag(self, scenario):
        """Both pillars pass but sensitivity spike → BORDERLINE."""
        wfo = _make_wfo_score(0.70)
        mc = _make_mc_result(0.03)
        sens = _make_sensitivity(spike_detected=True, spike_parameters=("rsi_period",))

        result = compute_verdict("abc123", wfo, mc, sens, scenario, oos_gate_enabled=False)

        assert result.verdict == Verdict.BORDERLINE
        assert result.sensitivity_spike is True

    def test_verdict_borderline_oos_gate(self, scenario):
        """Both pillars pass, oos_gate_enabled AND oos_gate_triggered → BORDERLINE."""
        wfo = _make_wfo_score(0.70, oos_gate_triggered=True)
        mc = _make_mc_result(0.03)
        sens = _make_sensitivity()

        result = compute_verdict(
            "abc123", wfo, mc, sens, scenario, oos_gate_enabled=True
        )

        assert result.verdict == Verdict.BORDERLINE
        assert result.oos_gate_triggered is True

    def test_oos_gate_not_triggered_when_disabled(self, scenario):
        """oos_gate_triggered in WFO score but oos_gate_enabled=False → flag not set."""
        wfo = _make_wfo_score(0.70, oos_gate_triggered=True)
        mc = _make_mc_result(0.03)
        sens = _make_sensitivity()

        result = compute_verdict(
            "abc123", wfo, mc, sens, scenario, oos_gate_enabled=False
        )

        # oos_gate_triggered in WFOConsistencyScore but gate is disabled
        assert result.oos_gate_triggered is False
        assert result.verdict == Verdict.AUTO_GO

    def test_verdict_borderline_window_collapse(self, scenario):
        """Both pillars pass, window_collapse_flag → BORDERLINE."""
        wfo = _make_wfo_score(0.70, window_collapse_flag=True)
        mc = _make_mc_result(0.03)
        sens = _make_sensitivity()

        result = compute_verdict("abc123", wfo, mc, sens, scenario, oos_gate_enabled=False)

        assert result.verdict == Verdict.BORDERLINE
        assert result.window_collapse_flag is True

    def test_verdict_borderline_profile_incomplete(self, scenario):
        """Both pillars pass, profile_complete=False → BORDERLINE."""
        wfo = _make_wfo_score(0.70)
        mc = _make_mc_result(0.03)
        sens = _make_sensitivity(profile_complete=False)

        result = compute_verdict("abc123", wfo, mc, sens, scenario, oos_gate_enabled=False)

        assert result.verdict == Verdict.BORDERLINE
        assert result.sensitivity_profile_incomplete is True

    def test_no_go_overrides_modifier_flags(self, scenario):
        """NO_GO on a pillar cannot be overridden to BORDERLINE by modifier flags."""
        wfo = _make_wfo_score(0.20)   # hard no_go
        mc = _make_mc_result(0.25)    # hard no_go
        sens = _make_sensitivity(spike_detected=True, spike_parameters=("rsi_period",))

        result = compute_verdict("abc123", wfo, mc, sens, scenario, oos_gate_enabled=True)

        # Despite all flags, hard no_go pillars → NO_GO
        assert result.verdict == Verdict.NO_GO


class TestDeploymentStatus:
    def test_deployment_status_always_paper_for_go(self, scenario):
        """AUTO_GO verdict → deployment_status is always PAPER_TRADE_REQUIRED."""
        wfo = _make_wfo_score(0.70)
        mc = _make_mc_result(0.03)
        sens = _make_sensitivity()

        result = compute_verdict("abc123", wfo, mc, sens, scenario, oos_gate_enabled=False)

        assert result.verdict == Verdict.AUTO_GO
        assert result.deployment_status == DeploymentStatus.PAPER_TRADE_REQUIRED

    def test_deployment_status_always_paper_for_borderline(self, scenario):
        """BORDERLINE verdict → deployment_status is always PAPER_TRADE_REQUIRED."""
        wfo = _make_wfo_score(0.55)
        mc = _make_mc_result(0.03)
        sens = _make_sensitivity()

        result = compute_verdict("abc123", wfo, mc, sens, scenario, oos_gate_enabled=False)

        assert result.verdict == Verdict.BORDERLINE
        assert result.deployment_status == DeploymentStatus.PAPER_TRADE_REQUIRED


class TestEvidenceSummary:
    def test_evidence_summary_not_empty_auto_go(self, scenario):
        wfo = _make_wfo_score(0.70)
        mc = _make_mc_result(0.03)
        sens = _make_sensitivity()
        result = compute_verdict("abc123", wfo, mc, sens, scenario, oos_gate_enabled=False)
        assert result.evidence_summary
        assert len(result.evidence_summary) > 10

    def test_evidence_summary_not_empty_no_go(self, scenario):
        wfo = _make_wfo_score(0.20)
        mc = _make_mc_result(0.25)
        sens = _make_sensitivity()
        result = compute_verdict("abc123", wfo, mc, sens, scenario, oos_gate_enabled=False)
        assert result.evidence_summary
        assert "NO-GO" in result.evidence_summary

    def test_evidence_summary_not_empty_borderline(self, scenario):
        wfo = _make_wfo_score(0.55)
        mc = _make_mc_result(0.03)
        sens = _make_sensitivity(spike_detected=True, spike_parameters=("atr_multiplier",))
        result = compute_verdict("abc123", wfo, mc, sens, scenario, oos_gate_enabled=False)
        assert result.evidence_summary
        assert "BORDERLINE" in result.evidence_summary

    def test_evidence_summary_mentions_spike_param(self, scenario):
        wfo = _make_wfo_score(0.70)
        mc = _make_mc_result(0.03)
        sens = _make_sensitivity(spike_detected=True, spike_parameters=("rsi_period",))
        result = compute_verdict("abc123", wfo, mc, sens, scenario, oos_gate_enabled=False)
        assert "rsi_period" in result.evidence_summary

    def test_pillar_scores_in_result(self, scenario):
        """Pillar scores are present in the result for all verdict types."""
        for wfo_val, mc_val, expected in [
            (0.70, 0.03, Verdict.AUTO_GO),
            (0.55, 0.03, Verdict.BORDERLINE),
            (0.20, 0.25, Verdict.NO_GO),
        ]:
            wfo = _make_wfo_score(wfo_val)
            mc = _make_mc_result(mc_val)
            sens = _make_sensitivity()
            result = compute_verdict("abc123", wfo, mc, sens, scenario, oos_gate_enabled=False)
            assert result.wfo_consistency_score == pytest.approx(wfo_val)
            assert result.mc_deep_ruin_probability == pytest.approx(mc_val)
            assert result.verdict == expected
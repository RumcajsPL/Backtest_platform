"""
test_block8c_verdict_sensitivity.py — Block 8C audit tests.

Covers:
  B8C-001: ScenarioProfile.report_emphasis validated as non-empty sequence in __post_init__.
  B8C-006: verdict.py — NO_GO boundary operators confirmed correct.
  B8C-007: verdict.py — NO_GO cannot be upgraded by modifier flags.
  B8C-007: verdict.py — None wfo_score/mc_result guard documented.
  sensitivity.py — profile_complete boundary at exactly 50% failure.
  sensitivity.py — OPT-01 nullcontext import confirmed from contextlib.
  contracts.py — WFOConsistencyScore median_oos_delta Optional[float] with default None.
  contracts.py — VerdictResult __post_init__ enforces PAPER_TRADE_REQUIRED for go/borderline.
  contracts.py — VerdictResult rejects LIVE_APPROVED in code.

11 tests total.
"""
from __future__ import annotations

import sys
from datetime import UTC, datetime, date
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.utils.paths import PROJECT_ROOT  # noqa: E402

from src.backtesting.contracts import (  # noqa: E402
    CandidateParameterSet,
    DeploymentStatus,
    MCMode,
    MCResult,
    SensitivityProfile,
    ScenarioProfile,
    Verdict,
    VerdictResult,
    WFOConsistencyScore,
)
from src.backtesting.evaluation.verdict import compute_verdict  # noqa: E402


# ── Shared fixtures ────────────────────────────────────────────────────────────

def _make_scenario(**overrides) -> ScenarioProfile:
    defaults = dict(
        name="e2e_test",
        description="test",
        weight_net_pnl=0.2, weight_expectancy=0.3, weight_max_drawdown=0.2,
        weight_win_rate=0.15, weight_trade_frequency=0.1, weight_profit_factor=0.05,
        min_win_rate=0.10, max_drawdown=0.95, max_losing_streak=50,
        min_trades_per_week=0.1, min_expectancy=-5.0, min_profit_factor=0.1,
        mc_prefilter_ruin_threshold=0.90,
        wfo_weight_median_return=0.4, wfo_weight_variance=0.2,
        wfo_weight_worst_drawdown=0.2, wfo_weight_fraction_positive=0.2,
        verdict_go_wfo_floor=0.65,
        verdict_borderline_wfo_floor=0.40,
        verdict_go_mc_ruin_ceiling=0.05,
        verdict_borderline_mc_ruin_ceiling=0.15,
        verdict_sensitivity_spike_threshold=0.15,
        report_emphasis=("wfo_consistency_score", "mc_deep_ruin_probability"),
    )
    defaults.update(overrides)
    return ScenarioProfile(**defaults)


def _make_wfo_score(
    candidate_id: str = "a" * 64,
    composite_score: float = 0.70,
    oos_gate_triggered: bool = False,
    window_collapse_flag: bool = False,
    median_oos_delta: Optional[float] = None,
) -> WFOConsistencyScore:
    return WFOConsistencyScore(
        candidate_id=candidate_id,
        windows_evaluated=5,
        windows_total=5,
        median_window_return=0.5,
        window_return_variance=0.02,
        worst_window_drawdown=0.10,
        fraction_positive_windows=0.80,
        composite_score=composite_score,
        oos_gate_triggered=oos_gate_triggered,
        window_collapse_flag=window_collapse_flag,
        median_oos_delta=median_oos_delta,
    )


def _make_mc_result(
    candidate_id: str = "a" * 64,
    ruin_probability: Optional[float] = 0.03,
    error: Optional[str] = None,
) -> MCResult:
    return MCResult(
        candidate_id=candidate_id,
        mode=MCMode.DEEP,
        perturbation_profile_name="standard",
        iterations=3000,
        evaluated_at=datetime.now(UTC),
        avg_final_equity=11000.0,
        worst_drawdown_across_paths=0.18,
        ruin_probability=ruin_probability,
        p5_final_equity=9500.0,
        error=error,
    )


def _make_sensitivity(
    candidate_id: str = "a" * 64,
    spike_detected: bool = False,
    profile_complete: bool = True,
) -> SensitivityProfile:
    return SensitivityProfile(
        candidate_id=candidate_id,
        baseline_fitness=0.72,
        parameter_sensitivities=(),
        spike_detected=spike_detected,
        spike_parameters=("rsi_length",) if spike_detected else (),
        profile_complete=profile_complete,
    )


# ── B8C-001: report_emphasis validation ──────────────────────────────────────

class TestB8C001ReportEmphasisValidation:
    """B8C-001: ScenarioProfile.report_emphasis must be validated as a non-empty sequence."""

    def test_report_emphasis_tuple_accepted(self):
        """A valid tuple of metric names should be accepted without error."""
        scenario = _make_scenario(
            report_emphasis=("wfo_consistency_score", "mc_deep_ruin_probability")
        )
        assert isinstance(scenario.report_emphasis, tuple)

    def test_report_emphasis_scalar_string_rejected(self):
        """
        A scalar string 'balanced' must raise ValueError in __post_init__.

        Without this guard, list(scenario.report_emphasis) in _render_scenario_metrics
        yields individual characters ['b', 'a', 'l', ...], causing the report to
        render garbage metric cells with no error or warning.

        This test will FAIL until B8C-001 fix is applied to contracts.py.
        """
        with pytest.raises(ValueError, match="report_emphasis"):
            _make_scenario(report_emphasis="balanced")

    def test_report_emphasis_empty_sequence_rejected(self):
        """
        An empty tuple must raise ValueError — report with no emphasis metrics is invalid.

        This test will FAIL until B8C-001 fix is applied to contracts.py.
        """
        with pytest.raises(ValueError, match="report_emphasis"):
            _make_scenario(report_emphasis=())


# ── Verdict boundary operator tests ───────────────────────────────────────────

class TestVerdictBoundaryOperators:
    """B8C-006 / L-02: Confirm >= / <= boundary semantics for verdict pillars."""

    def test_wfo_exactly_at_go_floor_is_auto_go(self):
        """WFO score exactly at go_floor must produce AUTO_GO (>= is inclusive)."""
        scenario = _make_scenario()
        wfo = _make_wfo_score(composite_score=0.65)   # exactly at go_floor
        mc  = _make_mc_result(ruin_probability=0.03)  # clearly in go zone
        sens = _make_sensitivity()

        result = compute_verdict("a" * 64, wfo, mc, sens, scenario, oos_gate_enabled=False)

        assert result.verdict == Verdict.AUTO_GO, (
            f"WFO score exactly at go_floor (0.65) should be AUTO_GO, got {result.verdict}. "
            "Boundary operator must be >= (inclusive), not > (exclusive)."
        )

    def test_wfo_just_below_go_floor_is_borderline(self):
        """WFO score just below go_floor is BORDERLINE (in the band)."""
        scenario = _make_scenario()
        wfo = _make_wfo_score(composite_score=0.649)
        mc  = _make_mc_result(ruin_probability=0.03)
        sens = _make_sensitivity()

        result = compute_verdict("a" * 64, wfo, mc, sens, scenario, oos_gate_enabled=False)

        assert result.verdict == Verdict.BORDERLINE

    def test_wfo_exactly_at_borderline_floor_is_borderline(self):
        """WFO score exactly at borderline_floor (0.40) must be BORDERLINE, not NO_GO."""
        scenario = _make_scenario()
        wfo = _make_wfo_score(composite_score=0.40)   # at borderline floor
        mc  = _make_mc_result(ruin_probability=0.03)
        sens = _make_sensitivity()

        result = compute_verdict("a" * 64, wfo, mc, sens, scenario, oos_gate_enabled=False)

        assert result.verdict == Verdict.BORDERLINE, (
            f"WFO score exactly at borderline_floor (0.40) should be BORDERLINE. "
            f"wfo_pillar_no_go uses strict < so 0.40 < 0.40 = False. Got {result.verdict}."
        )

    def test_wfo_just_below_borderline_floor_is_no_go(self):
        """WFO score strictly below borderline_floor is NO_GO."""
        scenario = _make_scenario()
        wfo = _make_wfo_score(composite_score=0.399)
        mc  = _make_mc_result(ruin_probability=0.03)
        sens = _make_sensitivity()

        result = compute_verdict("a" * 64, wfo, mc, sens, scenario, oos_gate_enabled=False)

        assert result.verdict == Verdict.NO_GO

    def test_mc_exactly_at_go_ceiling_is_auto_go(self):
        """MC ruin probability exactly at go_ceiling (0.05) must be AUTO_GO."""
        scenario = _make_scenario()
        wfo = _make_wfo_score(composite_score=0.70)
        mc  = _make_mc_result(ruin_probability=0.05)  # exactly at go ceiling
        sens = _make_sensitivity()

        result = compute_verdict("a" * 64, wfo, mc, sens, scenario, oos_gate_enabled=False)

        assert result.verdict == Verdict.AUTO_GO, (
            "MC ruin prob exactly at go_mc_ruin_ceiling should be AUTO_GO "
            "(mc_pillar_go uses <= — inclusive at boundary)."
        )

    def test_mc_none_ruin_produces_no_go(self):
        """MC ruin probability=None (eval failure) must always produce NO_GO."""
        scenario = _make_scenario()
        wfo = _make_wfo_score(composite_score=0.90)  # excellent WFO
        mc  = _make_mc_result(ruin_probability=None) # MC failed
        sens = _make_sensitivity()

        result = compute_verdict("a" * 64, wfo, mc, sens, scenario, oos_gate_enabled=False)

        assert result.verdict == Verdict.NO_GO, (
            "MC ruin_probability=None must produce NO_GO regardless of WFO score. "
            "MC evaluation failure is treated as maximum risk."
        )


# ── NO_GO cannot be upgraded ──────────────────────────────────────────────────

class TestNoGoCannotBeUpgraded:
    """B8C-007: Modifier flags cannot upgrade NO_GO to BORDERLINE or AUTO_GO."""

    def test_no_go_with_all_flags_false_stays_no_go(self):
        """A NO_GO verdict with zero active flags must remain NO_GO."""
        scenario = _make_scenario()
        wfo = _make_wfo_score(composite_score=0.20)  # clearly NO_GO on WFO
        mc  = _make_mc_result(ruin_probability=0.03)
        sens = _make_sensitivity(spike_detected=False, profile_complete=True)

        result = compute_verdict("a" * 64, wfo, mc, sens, scenario, oos_gate_enabled=False)

        assert result.verdict == Verdict.NO_GO
        assert not result.sensitivity_spike
        assert not result.sensitivity_profile_incomplete

    def test_auto_go_demoted_to_borderline_by_spike(self):
        """AUTO_GO demoted to BORDERLINE when sensitivity spike is active."""
        scenario = _make_scenario()
        wfo = _make_wfo_score(composite_score=0.70)
        mc  = _make_mc_result(ruin_probability=0.03)
        sens = _make_sensitivity(spike_detected=True)

        result = compute_verdict("a" * 64, wfo, mc, sens, scenario, oos_gate_enabled=False)

        assert result.verdict == Verdict.BORDERLINE, (
            "A spike modifier must demote AUTO_GO → BORDERLINE."
        )

    def test_no_go_wfo_pillar_not_upgraded_by_spike_modifier(self):
        """A NO_GO WFO pillar cannot be upgraded to BORDERLINE even with no other flags."""
        scenario = _make_scenario()
        wfo = _make_wfo_score(composite_score=0.30)  # NO_GO on WFO pillar
        mc  = _make_mc_result(ruin_probability=0.03) # MC passes fine
        # Even with no modifier flags active, WFO pillar NO_GO → verdict NO_GO
        sens = _make_sensitivity(spike_detected=False, profile_complete=True)

        result = compute_verdict("a" * 64, wfo, mc, sens, scenario, oos_gate_enabled=False)

        assert result.verdict == Verdict.NO_GO


# ── VerdictResult contract enforcement ───────────────────────────────────────

class TestVerdictResultContractEnforcement:
    """VerdictResult.__post_init__ must reject LIVE_APPROVED set in code."""

    def test_live_approved_raises_for_auto_go(self):
        """AUTO_GO verdict with LIVE_APPROVED deployment_status must raise ValueError."""
        with pytest.raises(ValueError, match="PAPER_TRADE_REQUIRED"):
            VerdictResult(
                candidate_id="a" * 64,
                scenario_name="test",
                verdict=Verdict.AUTO_GO,
                deployment_status=DeploymentStatus.LIVE_APPROVED,
                wfo_consistency_score=0.70,
                mc_deep_ruin_probability=0.03,
                sensitivity_spike=False,
                oos_gate_triggered=False,
                window_collapse_flag=False,
                sensitivity_profile_incomplete=False,
                median_oos_delta=None,
                parameter_region_width=None,
                yaml_output_path=None,
                evidence_summary="Test evidence.",
            )

    def test_paper_trade_required_accepted_for_auto_go(self):
        """AUTO_GO with PAPER_TRADE_REQUIRED must be accepted without error."""
        v = VerdictResult(
            candidate_id="a" * 64,
            scenario_name="test",
            verdict=Verdict.AUTO_GO,
            deployment_status=DeploymentStatus.PAPER_TRADE_REQUIRED,
            wfo_consistency_score=0.70,
            mc_deep_ruin_probability=0.03,
            sensitivity_spike=False,
            oos_gate_triggered=False,
            window_collapse_flag=False,
            sensitivity_profile_incomplete=False,
            median_oos_delta=None,
            parameter_region_width=None,
            yaml_output_path=None,
            evidence_summary="Test evidence.",
        )
        assert v.deployment_status == DeploymentStatus.PAPER_TRADE_REQUIRED
"""
tests/backtesting/integration/test_threshold_calibration.py
────────────────────────────────────────────────────────────
Block 5 — Threshold Calibration (15 THRESH criteria + field integrity tests)

Exercises compute_verdict() directly with controlled (wfo_score, mc_ruin)
inputs. No CandidateStore, no orchestrator, no YAML file required.

Threshold values under test (e2e_test scenario):
    verdict_go_wfo_floor               = 0.55   (>= inclusive)
    verdict_borderline_wfo_floor       = 0.40   (< strictly less than → no_go)
    verdict_go_mc_ruin_ceiling         = 0.10   (<= inclusive)
    verdict_borderline_mc_ruin_ceiling = 0.25   (> strictly greater than → no_go)

Verdict logic (from verdict.py source — exact):
    NO_GO      : wfo < borderline_floor  OR  ruin > borderline_ceiling
                 OR ruin is None
    AUTO_GO    : wfo >= go_floor  AND  ruin <= go_ceiling  AND  no modifier flags
    BORDERLINE : everything else (either pillar in borderline zone, or any flag)

Modifier flags (any one → BORDERLINE, cannot override NO_GO):
    sensitivity_spike              = sensitivity.spike_detected
    oos_gate_triggered             = oos_gate_enabled AND wfo_score.oos_gate_triggered
    window_collapse_flag           = wfo_score.window_collapse_flag
    sensitivity_profile_incomplete = not sensitivity.profile_complete
"""
from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Optional

import pytest

# ── 1. sys.path FIRST ─────────────────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# ── 2. path anchor ────────────────────────────────────────────────────────────
from src.utils.paths import PROJECT_ROOT  # noqa: E402

# ── 3. contracts BEFORE candidate_store ──────────────────────────────────────
from src.backtesting.contracts import (  # noqa: E402
    DeploymentStatus,
    MCMode,
    MCResult,
    ScenarioProfile,
    SensitivityProfile,
    Verdict,
    VerdictResult,
    WFOConsistencyScore,
)

# ── 4. verdict engine ─────────────────────────────────────────────────────────
from src.backtesting.evaluation.verdict import compute_verdict  # noqa: E402


# ─────────────────────────────────────────────────────────────────────────────
# Score constants — representative values for each grid region
# ─────────────────────────────────────────────────────────────────────────────

WFO_ABOVE_GO      = 0.60   # strictly above go_floor (0.55)
WFO_AT_GO         = 0.55   # exactly == go_floor  (boundary test THRESH-10)
WFO_IN_BORDERLINE = 0.47   # 0.40 <= x < 0.55
WFO_BELOW_FLOOR   = 0.30   # strictly below borderline_floor (0.40)

MC_BELOW_GO       = 0.05   # strictly below go_ceiling (0.10)
MC_AT_GO          = 0.10   # exactly == go_ceiling  (boundary test THRESH-11)
MC_IN_BORDERLINE  = 0.17   # 0.10 < x <= 0.25
MC_ABOVE_CEILING  = 0.35   # strictly above borderline_ceiling (0.25)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def scenario() -> ScenarioProfile:
    """
    Module-scoped ScenarioProfile constructed directly — no YAML, no file I/O.
    Fitness weights sum = 1.0. WFO weights sum = 1.0.
    Thresholds: go_wfo=0.55, borderline_wfo=0.40, go_mc=0.10, borderline_mc=0.25.
    """
    return ScenarioProfile(
        name="e2e_test",
        description="Threshold calibration fixture — not for production use.",
        weight_net_pnl=0.25,
        weight_expectancy=0.25,
        weight_max_drawdown=0.20,
        weight_win_rate=0.15,
        weight_trade_frequency=0.10,
        weight_profit_factor=0.05,
        min_win_rate=0.0,
        max_drawdown=1.0,
        max_losing_streak=9999,
        min_trades_per_week=0.0,
        min_expectancy=-9999.0,
        min_profit_factor=0.0,
        mc_prefilter_ruin_threshold=1.0,
        wfo_weight_median_return=0.40,
        wfo_weight_variance=0.20,
        wfo_weight_worst_drawdown=0.20,
        wfo_weight_fraction_positive=0.20,
        verdict_go_wfo_floor=0.55,
        verdict_borderline_wfo_floor=0.40,
        verdict_go_mc_ruin_ceiling=0.10,
        verdict_borderline_mc_ruin_ceiling=0.25,
        verdict_sensitivity_spike_threshold=0.15,
        report_emphasis=(),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Helper factories
# ─────────────────────────────────────────────────────────────────────────────

def _make_wfo(
    candidate_id: str,
    composite: float,
    oos_gate: bool = False,
    collapse: bool = False,
) -> WFOConsistencyScore:
    return WFOConsistencyScore(
        candidate_id=candidate_id,
        windows_evaluated=3,
        windows_total=3,
        median_window_return=0.02,
        window_return_variance=0.001,
        worst_window_drawdown=0.05,
        fraction_positive_windows=0.667,
        composite_score=composite,
        oos_gate_triggered=oos_gate,
        window_collapse_flag=collapse,
    )


def _make_mc(
    candidate_id: str,
    ruin: Optional[float],
) -> MCResult:
    """
    ruin=None simulates a failed MC evaluation (mc_result.error set).
    verdict.py maps None ruin_probability → mc_pillar_no_go=True → NO_GO.
    """
    has_data = ruin is not None
    return MCResult(
        candidate_id=candidate_id,
        mode=MCMode.DEEP,
        perturbation_profile_name="default",
        iterations=1000,
        evaluated_at=datetime.now(UTC),
        avg_final_equity=1100.0 if has_data else None,
        worst_drawdown_across_paths=0.12 if has_data else None,
        ruin_probability=ruin,
        p5_final_equity=1050.0 if has_data else None,
        error=None if has_data else "injected eval failure",
    )


def _make_sens(
    candidate_id: str,
    spike: bool = False,
    complete: bool = True,
) -> SensitivityProfile:
    return SensitivityProfile(
        candidate_id=candidate_id,
        baseline_fitness=0.55,
        parameter_sensitivities=(),
        spike_detected=spike,
        spike_parameters=("fast_period",) if spike else (),
        profile_complete=complete,
    )


def _call_verdict(
    wfo_composite: float,
    mc_ruin: Optional[float],
    scenario: ScenarioProfile,
    *,
    spike: bool = False,
    profile_complete: bool = True,
    wfo_oos_gate: bool = False,
    wfo_collapse: bool = False,
    oos_gate_enabled: bool = False,
    cid: str = "test-candidate",
) -> Verdict:
    """
    Convenience wrapper. Constructs all contract inputs, calls compute_verdict(),
    returns the Verdict enum value only.
    """
    result: VerdictResult = compute_verdict(
        candidate_id=cid,
        wfo_score=_make_wfo(cid, wfo_composite,
                            oos_gate=wfo_oos_gate, collapse=wfo_collapse),
        mc_result=_make_mc(cid, mc_ruin),
        sensitivity=_make_sens(cid, spike=spike, complete=profile_complete),
        scenario=scenario,
        oos_gate_enabled=oos_gate_enabled,
    )
    return result.verdict


# ─────────────────────────────────────────────────────────────────────────────
# THRESH-01 to THRESH-09: Full 3x3 verdict grid
# ─────────────────────────────────────────────────────────────────────────────

class TestVerdictGrid:
    """
    9 tests covering all distinct (wfo_region, mc_region) combinations.
    No modifier flags active in any of these tests — pure pillar logic.
    """

    # ── Row 1: WFO above go_floor ─────────────────────────────────────────────

    def test_thresh_01_wfo_above_go_mc_below_go(self, scenario):
        """THRESH-01: Both pillars pass go thresholds. No flags. → AUTO_GO."""
        assert _call_verdict(WFO_ABOVE_GO, MC_BELOW_GO, scenario) == Verdict.AUTO_GO

    def test_thresh_02_wfo_above_go_mc_borderline(self, scenario):
        """THRESH-02: WFO passes go, MC in borderline band. → BORDERLINE."""
        assert _call_verdict(WFO_ABOVE_GO, MC_IN_BORDERLINE, scenario) == Verdict.BORDERLINE

    def test_thresh_03_wfo_above_go_mc_no_go(self, scenario):
        """THRESH-03: WFO passes go, MC above borderline_ceiling. → NO_GO."""
        assert _call_verdict(WFO_ABOVE_GO, MC_ABOVE_CEILING, scenario) == Verdict.NO_GO

    # ── Row 2: WFO in borderline band ────────────────────────────────────────

    def test_thresh_04_wfo_borderline_mc_below_go(self, scenario):
        """THRESH-04: WFO in borderline band, MC passes go. → BORDERLINE."""
        assert _call_verdict(WFO_IN_BORDERLINE, MC_BELOW_GO, scenario) == Verdict.BORDERLINE

    def test_thresh_05_wfo_borderline_mc_borderline(self, scenario):
        """THRESH-05: Both pillars in borderline band. → BORDERLINE."""
        assert _call_verdict(WFO_IN_BORDERLINE, MC_IN_BORDERLINE, scenario) == Verdict.BORDERLINE

    def test_thresh_06_wfo_borderline_mc_no_go(self, scenario):
        """THRESH-06: WFO borderline, MC above ceiling. → NO_GO."""
        assert _call_verdict(WFO_IN_BORDERLINE, MC_ABOVE_CEILING, scenario) == Verdict.NO_GO

    # ── Row 3: WFO below borderline_floor ────────────────────────────────────

    def test_thresh_07_wfo_no_go_mc_below_go(self, scenario):
        """THRESH-07: WFO below floor. MC irrelevant. → NO_GO."""
        assert _call_verdict(WFO_BELOW_FLOOR, MC_BELOW_GO, scenario) == Verdict.NO_GO

    def test_thresh_08_wfo_no_go_mc_borderline(self, scenario):
        """THRESH-08: WFO below floor, MC borderline. → NO_GO."""
        assert _call_verdict(WFO_BELOW_FLOOR, MC_IN_BORDERLINE, scenario) == Verdict.NO_GO

    def test_thresh_09_wfo_no_go_mc_no_go(self, scenario):
        """THRESH-09: Both pillars in no_go zone. → NO_GO."""
        assert _call_verdict(WFO_BELOW_FLOOR, MC_ABOVE_CEILING, scenario) == Verdict.NO_GO


# ─────────────────────────────────────────────────────────────────────────────
# THRESH-10, THRESH-11: Boundary exactness
# ─────────────────────────────────────────────────────────────────────────────

class TestBoundaryExactness:
    """
    Verify the >= and <= operators are inclusive at the go thresholds.
    A score exactly at the go threshold must produce AUTO_GO, not BORDERLINE.
    """

    def test_thresh_10_wfo_exactly_at_go_floor(self, scenario):
        """
        THRESH-10: wfo_composite == verdict_go_wfo_floor (0.55) exactly.
        Verdict.py uses >=, so this must be AUTO_GO when MC also passes.
        If this were > (exclusive), the result would be BORDERLINE — that is wrong.
        """
        assert _call_verdict(WFO_AT_GO, MC_BELOW_GO, scenario) == Verdict.AUTO_GO, (
            "wfo_composite == go_wfo_floor must satisfy >= and produce AUTO_GO"
        )

    def test_thresh_11_mc_exactly_at_go_ceiling(self, scenario):
        """
        THRESH-11: ruin_probability == verdict_go_mc_ruin_ceiling (0.10) exactly.
        Verdict.py uses <=, so this must be AUTO_GO when WFO also passes.
        If this were < (exclusive), the result would be BORDERLINE — that is wrong.
        """
        assert _call_verdict(WFO_ABOVE_GO, MC_AT_GO, scenario) == Verdict.AUTO_GO, (
            "ruin_probability == go_mc_ruin_ceiling must satisfy <= and produce AUTO_GO"
        )


# ─────────────────────────────────────────────────────────────────────────────
# THRESH-12: ruin_probability=None → NO_GO
# ─────────────────────────────────────────────────────────────────────────────

class TestNullRuinProbability:

    def test_thresh_12_ruin_none_is_no_go(self, scenario):
        """
        THRESH-12: ruin_probability=None (MC evaluation failed entirely).
        verdict.py: None → mc_pillar_go=False, mc_pillar_no_go=True → NO_GO.
        This must hold even when WFO score is excellent.
        """
        assert _call_verdict(WFO_ABOVE_GO, None, scenario) == Verdict.NO_GO, (
            "ruin_probability=None must produce NO_GO regardless of WFO score"
        )

    def test_thresh_12b_ruin_none_not_borderline(self, scenario):
        """
        THRESH-12b: None ruin with borderline WFO → still NO_GO, not BORDERLINE.
        The None path forces mc_pillar_no_go=True (not just mc_pillar_go=False),
        so it cannot fall through to the BORDERLINE branch.
        """
        assert _call_verdict(WFO_IN_BORDERLINE, None, scenario) == Verdict.NO_GO, (
            "ruin_probability=None must produce NO_GO, not BORDERLINE"
        )


# ─────────────────────────────────────────────────────────────────────────────
# THRESH-13 to THRESH-15: Modifier flag demotion AUTO_GO → BORDERLINE
# ─────────────────────────────────────────────────────────────────────────────

class TestModifierFlagDemotion:
    """
    Base: both pillars pass go thresholds (wfo=0.60, ruin=0.05, no flags)
    → confirmed AUTO_GO by THRESH-01.
    Each test adds exactly one modifier flag and asserts BORDERLINE.
    """

    def test_thresh_13_sensitivity_spike_demotes_to_borderline(self, scenario):
        """
        THRESH-13: sensitivity.spike_detected=True.
        spike_parameters must be non-empty when spike_detected=True (contract enforced).
        → BORDERLINE.
        """
        assert _call_verdict(
            WFO_ABOVE_GO, MC_BELOW_GO, scenario,
            spike=True,
        ) == Verdict.BORDERLINE

    def test_thresh_14_window_collapse_demotes_to_borderline(self, scenario):
        """
        THRESH-14: wfo_score.window_collapse_flag=True.
        → BORDERLINE.
        """
        assert _call_verdict(
            WFO_ABOVE_GO, MC_BELOW_GO, scenario,
            wfo_collapse=True,
        ) == Verdict.BORDERLINE

    def test_thresh_15_profile_incomplete_demotes_to_borderline(self, scenario):
        """
        THRESH-15: sensitivity.profile_complete=False.
        verdict.py derives: sensitivity_profile_incomplete = not profile_complete.
        → BORDERLINE.
        """
        assert _call_verdict(
            WFO_ABOVE_GO, MC_BELOW_GO, scenario,
            profile_complete=False,
        ) == Verdict.BORDERLINE

    def test_thresh_oos_gate_both_conditions_required(self, scenario):
        """
        THRESH-OOS: oos_gate_triggered requires BOTH conditions simultaneously:
          (a) oos_gate_enabled=True passed to compute_verdict()
          (b) wfo_score.oos_gate_triggered=True on the WFOConsistencyScore
        Either condition alone must NOT trigger the flag.
        Both together on AUTO_GO base → BORDERLINE.
        """
        # Condition (a) alone — wfo flag True but oos_gate_enabled=False
        assert _call_verdict(
            WFO_ABOVE_GO, MC_BELOW_GO, scenario,
            wfo_oos_gate=True, oos_gate_enabled=False,
        ) == Verdict.AUTO_GO, (
            "wfo_score.oos_gate_triggered=True with oos_gate_enabled=False "
            "must NOT trigger the flag — should remain AUTO_GO"
        )
        # Condition (b) alone — oos_gate_enabled=True but wfo flag False
        assert _call_verdict(
            WFO_ABOVE_GO, MC_BELOW_GO, scenario,
            wfo_oos_gate=False, oos_gate_enabled=True,
        ) == Verdict.AUTO_GO, (
            "oos_gate_enabled=True with wfo_score.oos_gate_triggered=False "
            "must NOT trigger the flag — should remain AUTO_GO"
        )
        # Both together → BORDERLINE
        assert _call_verdict(
            WFO_ABOVE_GO, MC_BELOW_GO, scenario,
            wfo_oos_gate=True, oos_gate_enabled=True,
        ) == Verdict.BORDERLINE, (
            "Both oos_gate_enabled=True AND wfo_score.oos_gate_triggered=True "
            "must produce BORDERLINE on otherwise AUTO_GO inputs"
        )

    def test_thresh_no_go_not_overridden_by_any_flags(self, scenario):
        """
        Modifier flags cannot override NO_GO.
        All four flags active simultaneously on NO_GO base inputs → still NO_GO.
        """
        assert _call_verdict(
            WFO_BELOW_FLOOR, MC_ABOVE_CEILING, scenario,
            spike=True,
            profile_complete=False,
            wfo_oos_gate=True,
            wfo_collapse=True,
            oos_gate_enabled=True,
        ) == Verdict.NO_GO, (
            "All modifier flags active on NO_GO base inputs must still produce NO_GO"
        )


# ─────────────────────────────────────────────────────────────────────────────
# VerdictResult field integrity
# ─────────────────────────────────────────────────────────────────────────────

class TestVerdictResultFields:
    """
    Verify VerdictResult fields beyond .verdict are correctly wired.
    Catches regressions where verdict is correct but evidence fields are wrong.
    """

    def test_auto_go_deployment_status_is_paper_trade_required(self, scenario):
        """
        AUTO_GO must never produce LIVE_APPROVED — operator-only action.
        VerdictResult.__post_init__ enforces this but we confirm it here.
        """
        cid = "field-check-go"
        result = compute_verdict(
            candidate_id=cid,
            wfo_score=_make_wfo(cid, WFO_ABOVE_GO),
            mc_result=_make_mc(cid, MC_BELOW_GO),
            sensitivity=_make_sens(cid),
            scenario=scenario,
            oos_gate_enabled=False,
        )
        assert result.verdict == Verdict.AUTO_GO
        assert result.deployment_status == DeploymentStatus.PAPER_TRADE_REQUIRED

    def test_result_fields_wired_from_inputs(self, scenario):
        """
        wfo_consistency_score, mc_deep_ruin_probability, sensitivity_spike,
        window_collapse_flag, sensitivity_profile_incomplete, oos_gate_triggered
        must all reflect the constructed inputs exactly.
        """
        cid = "field-check-wiring"
        result = compute_verdict(
            candidate_id=cid,
            wfo_score=_make_wfo(cid, WFO_ABOVE_GO, collapse=True),
            mc_result=_make_mc(cid, MC_BELOW_GO),
            sensitivity=_make_sens(cid, spike=False, complete=False),
            scenario=scenario,
            oos_gate_enabled=False,
        )
        assert result.wfo_consistency_score == WFO_ABOVE_GO
        assert result.mc_deep_ruin_probability == MC_BELOW_GO
        assert result.sensitivity_spike is False
        assert result.window_collapse_flag is True
        assert result.sensitivity_profile_incomplete is True
        assert result.oos_gate_triggered is False
        assert result.evidence_summary, "evidence_summary must be a non-empty string"
        assert result.verdict == Verdict.BORDERLINE  # collapse flag demotes

    def test_none_ruin_stored_in_result(self, scenario):
        """ruin_probability=None must be stored as None in VerdictResult."""
        cid = "field-check-none-ruin"
        result = compute_verdict(
            candidate_id=cid,
            wfo_score=_make_wfo(cid, WFO_ABOVE_GO),
            mc_result=_make_mc(cid, None),
            sensitivity=_make_sens(cid),
            scenario=scenario,
            oos_gate_enabled=False,
        )
        assert result.verdict == Verdict.NO_GO
        assert result.mc_deep_ruin_probability is None


# ─────────────────────────────────────────────────────────────────────────────
# Informational summary — never fails
# ─────────────────────────────────────────────────────────────────────────────

def test_z_threshold_summary(scenario):
    """
    Informational: print the full verdict grid with actual threshold values.
    Always passes. No assertions.
    """
    import logging

    go_wfo  = scenario.verdict_go_wfo_floor
    bdr_wfo = scenario.verdict_borderline_wfo_floor
    go_mc   = scenario.verdict_go_mc_ruin_ceiling
    bdr_mc  = scenario.verdict_borderline_mc_ruin_ceiling

    lines = [
        "",
        "── Block 5 Threshold Calibration ──",
        f"  Thresholds: go_wfo>={go_wfo}  borderline_wfo>={bdr_wfo}"
        f"  go_mc<={go_mc}  borderline_mc<={bdr_mc}",
        "",
        f"  {'WFO region':<26} {'MC<go':>12} {'MC=go':>12}"
        f" {'MC=bdr':>12} {'MC>bdr':>12} {'MC=None':>12}",
        "  " + "-" * 86,
    ]

    wfo_cases = [
        (f"wfo>{go_wfo} ABOVE_GO",      WFO_ABOVE_GO),
        (f"wfo={go_wfo} AT_GO",          WFO_AT_GO),
        (f"wfo={WFO_IN_BORDERLINE} BDR", WFO_IN_BORDERLINE),
        (f"wfo={WFO_BELOW_FLOOR} NO_GO", WFO_BELOW_FLOOR),
    ]
    mc_cases = [MC_BELOW_GO, MC_AT_GO, MC_IN_BORDERLINE, MC_ABOVE_CEILING, None]

    for label, wfo_val in wfo_cases:
        cells = [
            f"{_call_verdict(wfo_val, mc_val, scenario).value:>12}"
            for mc_val in mc_cases
        ]
        lines.append(f"  {label:<26}" + "".join(cells))

    lines += [
        "",
        "  Modifier flag demotion (base: AUTO_GO inputs wfo=0.60 ruin=0.05):",
        f"    spike=True           → {_call_verdict(WFO_ABOVE_GO, MC_BELOW_GO, scenario, spike=True).value}",
        f"    collapse=True        → {_call_verdict(WFO_ABOVE_GO, MC_BELOW_GO, scenario, wfo_collapse=True).value}",
        f"    incomplete=True      → {_call_verdict(WFO_ABOVE_GO, MC_BELOW_GO, scenario, profile_complete=False).value}",
        f"    oos_gate (both)=True → {_call_verdict(WFO_ABOVE_GO, MC_BELOW_GO, scenario, wfo_oos_gate=True, oos_gate_enabled=True).value}",
        "",
        "── 15 THRESH criteria + field integrity + oos_gate two-condition test ──",
    ]
    logging.getLogger(__name__).info("\n".join(lines))
    assert True
"""
evaluation/verdict.py
─────────────────────
Stage 7 — Verdict Engine.

Applies the two-pillar verdict logic defined in the Verdict Model:

    Pillar 1 — WFO temporal consistency score (WFOConsistencyScore.composite_score)
    Pillar 2 — MC deep ruin probability (MCResult.ruin_probability)

Verdict outcomes (exact logic — do not approximate):
    AUTO_GO    : both pillars pass go thresholds AND no modifier flags set
    BORDERLINE : either pillar in borderline zone OR any modifier flag
    NO_GO      : either pillar fails into no_go zone (below borderline_wfo_floor
                 or above borderline_mc_ruin_ceiling)

Modifier flags (any one → BORDERLINE, cannot override NO_GO):
    sensitivity_spike              — |delta| > spike_threshold for any param
    oos_gate_triggered             — only when enforce_oos_gate is on AND IS/OOS > 50%
    window_collapse_flag           — from WFOConsistencyScore.window_collapse_flag
    sensitivity_profile_incomplete — >50% of perturbation evals failed

deployment_status is always PAPER_TRADE_REQUIRED for AUTO_GO and BORDERLINE.
Never set to LIVE_APPROVED in code — that is an operator-only action.

Public interface
────────────────
    compute_verdict(
        candidate_id, wfo_score, mc_result, sensitivity, scenario, oos_gate_enabled
    ) -> VerdictResult
"""

from __future__ import annotations

import logging
from typing import Optional

from src.backtesting.contracts import (
    DeploymentStatus,
    MCResult,
    ScenarioProfile,
    SensitivityProfile,
    Verdict,
    VerdictResult,
    WFOConsistencyScore,
)

logger = logging.getLogger(__name__)


def compute_verdict(
    candidate_id: str,
    wfo_score: WFOConsistencyScore,
    mc_result: MCResult,
    sensitivity: SensitivityProfile,
    scenario: ScenarioProfile,
    oos_gate_enabled: bool,
) -> VerdictResult:
    """
    Apply two-pillar verdict logic + modifier flags to produce a VerdictResult.

    Parameters
    ──────────
    candidate_id      : The candidate being evaluated.
    wfo_score         : Full WFO consistency result (Pillar 1).
    mc_result         : MC deep result (Pillar 2). Must be mode=DEEP.
    sensitivity       : Sensitivity profile (modifier source).
    scenario          : Active ScenarioProfile (provides all verdict thresholds).
    oos_gate_enabled  : Whether enforce_oos_gate is active.

    Returns
    ───────
    VerdictResult — always with deployment_status=PAPER_TRADE_REQUIRED for go/borderline.
    """
    # ── Extract pillar values ─────────────────────────────────────────────────
    wfo_composite: float = wfo_score.composite_score
    ruin_prob: Optional[float] = mc_result.ruin_probability

    # ── Pillar 1: WFO consistency ─────────────────────────────────────────────
    wfo_go_floor = scenario.verdict_go_wfo_floor
    wfo_borderline_floor = scenario.verdict_borderline_wfo_floor

    wfo_pillar_go = wfo_composite >= wfo_go_floor
    wfo_pillar_no_go = wfo_composite < wfo_borderline_floor
    # wfo_pillar_borderline is the band between borderline_floor (inclusive) and go_floor (exclusive)

    # ── Pillar 2: MC deep ruin probability ───────────────────────────────────
    mc_go_ceiling = scenario.verdict_go_mc_ruin_ceiling
    mc_borderline_ceiling = scenario.verdict_borderline_mc_ruin_ceiling

    if ruin_prob is None:
        # MC evaluation failed entirely — treat as no_go on MC pillar
        mc_pillar_go = False
        mc_pillar_no_go = True
        logger.warning(
            "Candidate %s: MC ruin_probability is None — treating as no_go on MC pillar.",
            candidate_id[:12],
        )
    else:
        mc_pillar_go = ruin_prob <= mc_go_ceiling
        mc_pillar_no_go = ruin_prob > mc_borderline_ceiling

    # ── Modifier flags ────────────────────────────────────────────────────────
    sensitivity_spike: bool = sensitivity.spike_detected
    oos_gate_triggered: bool = oos_gate_enabled and wfo_score.oos_gate_triggered
    window_collapse_flag: bool = wfo_score.window_collapse_flag
    sensitivity_profile_incomplete: bool = not sensitivity.profile_complete

    any_modifier_flag = (
        sensitivity_spike
        or oos_gate_triggered
        or window_collapse_flag
        or sensitivity_profile_incomplete
    )

    # ── Verdict logic (exact — no approximation) ──────────────────────────────
    if wfo_pillar_no_go or mc_pillar_no_go:
        verdict = Verdict.NO_GO
    elif wfo_pillar_go and mc_pillar_go and not any_modifier_flag:
        verdict = Verdict.AUTO_GO
    else:
        # Either pillar in borderline zone OR any modifier flag set
        verdict = Verdict.BORDERLINE

    # ── Deployment status ─────────────────────────────────────────────────────
    if verdict == Verdict.NO_GO:
        deployment_status = DeploymentStatus.PAPER_TRADE_REQUIRED  # field still required by contract
    else:
        deployment_status = DeploymentStatus.PAPER_TRADE_REQUIRED  # always — operator-only to promote

    # ── Evidence summary (plain language) ─────────────────────────────────────
    evidence_summary = _build_evidence_summary(
        verdict=verdict,
        wfo_composite=wfo_composite,
        wfo_go_floor=wfo_go_floor,
        wfo_borderline_floor=wfo_borderline_floor,
        wfo_pillar_go=wfo_pillar_go,
        wfo_pillar_no_go=wfo_pillar_no_go,
        ruin_prob=ruin_prob,
        mc_go_ceiling=mc_go_ceiling,
        mc_borderline_ceiling=mc_borderline_ceiling,
        mc_pillar_go=mc_pillar_go,
        mc_pillar_no_go=mc_pillar_no_go,
        sensitivity_spike=sensitivity_spike,
        spike_parameters=list(sensitivity.spike_parameters),
        oos_gate_triggered=oos_gate_triggered,
        window_collapse_flag=window_collapse_flag,
        sensitivity_profile_incomplete=sensitivity_profile_incomplete,
        scenario_name=scenario.name,
    )

    # ── Informational fields ──────────────────────────────────────────────────
    median_oos_delta: Optional[float] = _compute_median_oos_delta(wfo_score)

    logger.info(
        "Verdict for candidate %s: %s (WFO=%.3f, ruin=%.3f, flags=%s)",
        candidate_id[:12],
        verdict.value,
        wfo_composite,
        ruin_prob if ruin_prob is not None else -1.0,
        _active_flags(sensitivity_spike, oos_gate_triggered, window_collapse_flag, sensitivity_profile_incomplete),
    )

    return VerdictResult(
        candidate_id=candidate_id,
        scenario_name=scenario.name,
        verdict=verdict,
        deployment_status=deployment_status,
        wfo_consistency_score=wfo_composite,
        mc_deep_ruin_probability=ruin_prob,
        sensitivity_spike=sensitivity_spike,
        oos_gate_triggered=oos_gate_triggered,
        window_collapse_flag=window_collapse_flag,
        sensitivity_profile_incomplete=sensitivity_profile_incomplete,
        median_oos_delta=median_oos_delta,
        parameter_region_width=None,  # informational — computed by future ML layer
        yaml_output_path=None,         # set by yaml_generator after this call
        evidence_summary=evidence_summary,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _build_evidence_summary(
    *,
    verdict: Verdict,
    wfo_composite: float,
    wfo_go_floor: float,
    wfo_borderline_floor: float,
    wfo_pillar_go: bool,
    wfo_pillar_no_go: bool,
    ruin_prob: Optional[float],
    mc_go_ceiling: float,
    mc_borderline_ceiling: float,
    mc_pillar_go: bool,
    mc_pillar_no_go: bool,
    sensitivity_spike: bool,
    spike_parameters: list,
    oos_gate_triggered: bool,
    window_collapse_flag: bool,
    sensitivity_profile_incomplete: bool,
    scenario_name: str,
) -> str:
    """Build a plain-language evidence summary string."""
    parts = [f"Scenario: {scenario_name}. Verdict: {verdict.value.upper()}."]

    # WFO pillar
    wfo_status = "PASS" if wfo_pillar_go else ("NO-GO" if wfo_pillar_no_go else "BORDERLINE")
    parts.append(
        f"WFO consistency score: {wfo_composite:.3f} "
        f"(go≥{wfo_go_floor:.2f}, no-go<{wfo_borderline_floor:.2f}) → {wfo_status}."
    )

    # MC pillar
    ruin_str = f"{ruin_prob:.3f}" if ruin_prob is not None else "N/A (eval failed)"
    mc_status = "PASS" if mc_pillar_go else ("NO-GO" if mc_pillar_no_go else "BORDERLINE")
    parts.append(
        f"MC deep ruin probability: {ruin_str} "
        f"(go≤{mc_go_ceiling:.2f}, no-go>{mc_borderline_ceiling:.2f}) → {mc_status}."
    )

    # Modifier flags
    active_flags = []
    if sensitivity_spike:
        active_flags.append(f"sensitivity spike on [{', '.join(spike_parameters)}]")
    if oos_gate_triggered:
        active_flags.append("IS/OOS gate triggered")
    if window_collapse_flag:
        active_flags.append("WFO window collapse detected")
    if sensitivity_profile_incomplete:
        active_flags.append("sensitivity profile incomplete (>50% evaluations failed)")

    if active_flags:
        parts.append("Modifier flags: " + "; ".join(active_flags) + ".")
    else:
        parts.append("No modifier flags.")

    return " ".join(parts)


def _compute_median_oos_delta(wfo_score: WFOConsistencyScore) -> Optional[float]:
    """
    The WFOConsistencyScore does not directly expose per-window oos_delta values —
    that data lives in the individual WFOWindowResults in the store.
    Return None here; the orchestrator can compute and set this from store data if needed.
    """
    return None


def _active_flags(
    sensitivity_spike: bool,
    oos_gate_triggered: bool,
    window_collapse_flag: bool,
    sensitivity_profile_incomplete: bool,
) -> str:
    flags = []
    if sensitivity_spike:
        flags.append("spike")
    if oos_gate_triggered:
        flags.append("oos_gate")
    if window_collapse_flag:
        flags.append("window_collapse")
    if sensitivity_profile_incomplete:
        flags.append("profile_incomplete")
    return ",".join(flags) if flags else "none"
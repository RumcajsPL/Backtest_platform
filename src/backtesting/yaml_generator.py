"""
yaml_generator.py
─────────────────
Generates a trading-ready strategy YAML for a candidate that has received
an AUTO_GO or BORDERLINE verdict.

The output YAML is the base strategy YAML with:
  - candidate parameters merged in (overwriting the corresponding keys)
  - a `backtester_metadata` section added with run provenance and deployment status

Output filename: {run_id[:8]}_{candidate_id[:12]}_strategy.yaml
Output directory: {output_dir}/trading_yamls/

The output is validated against the strategy's StrategyConfig schema before
being written to disk. An exception is raised if validation fails (fail-fast).

Public interface
────────────────
    generate_trading_yaml(
        candidate, verdict, run_metadata, base_strategy_yaml_path, output_path
    ) -> Path
"""

from __future__ import annotations

import copy
import logging
from datetime import UTC, datetime
from pathlib import Path

import yaml

from src.backtesting.contracts import (
    CandidateParameterSet,
    DeploymentStatus,
    RunMetadata,
    VerdictResult,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Parameter → YAML path map
# ─────────────────────────────────────────────────────────────────────────────
# Maps each search-space parameter name to its location in strategy_template.yaml.
# Format: parameter_name → (section, subsection_or_None, key)
#
# WARNING: Twin key map exists in strategy_runner.py (_PARAM_KEY_MAP).
# Both files MUST be updated together when adding/renaming strategy parameters.
#
# B9G-004 FIX: Previous map was wrong — it pointed to "strategy" and "parameters"
# top-level sections that do not exist in strategy_template.yaml. Correct targets
# derived from strategy_template.yaml structure:
#
#   rsi_*        → filters.technical_filters.rsi_filter
#   bollinger_*  → filters.technical_filters.bollinger_filter
#   atr_length   → trade_management.risk.atr_length
#   atr_multiplier → trade_management.risk.atr_multiplier_sl
#   rr_target    → trade_management.risk.risk_to_reward_ratio
#   risk_percentile → trade_management.risk.max_risk_percentile
#
# Exploration/discovery zone parameters (currently disabled) are pre-mapped
# here so the map is complete when those zones are re-enabled (B9F-001).
# ─────────────────────────────────────────────────────────────────────────────

# Each entry: param_name → (top_section, nested_path_tuple, leaf_key)
# nested_path_tuple navigates from top_section to the dict containing leaf_key.
# e.g. ("filters", ("technical_filters", "rsi_filter"), "length")
#   → merged["filters"]["technical_filters"]["rsi_filter"]["length"] = value
#
# For two-level nesting (e.g. trade_management.risk):
#   ("trade_management", ("risk",), "atr_length")
#   → merged["trade_management"]["risk"]["atr_length"] = value

_PARAM_MAP: dict[str, tuple[str, tuple[str, ...], str]] = {
    # ── RSI filter ────────────────────────────────────────────────────────────
    "rsi_period":           ("filters", ("technical_filters", "rsi_filter"), "length"),
    "rsi_overbought":       ("filters", ("technical_filters", "rsi_filter"), "overbought"),
    "rsi_oversold":         ("filters", ("technical_filters", "rsi_filter"), "oversold"),

    # ── Bollinger filter ──────────────────────────────────────────────────────
    "bollinger_length":     ("filters", ("technical_filters", "bollinger_filter"), "length"),
    "bollinger_multiplier": ("filters", ("technical_filters", "bollinger_filter"), "filter_multiplier"),
    "bollinger_width_ma":   ("filters", ("technical_filters", "bollinger_filter"), "width_ma_length"),

    # ── Risk / trade management ───────────────────────────────────────────────
    "atr_length":           ("trade_management", ("risk",), "atr_length"),
    "atr_multiplier":       ("trade_management", ("risk",), "atr_multiplier_sl"),
    "rr_target":            ("trade_management", ("risk",), "risk_to_reward_ratio"),
    "risk_percentile":      ("trade_management", ("risk",), "max_risk_percentile"),

    # ── ADX filter (exploration/discovery zones) ──────────────────────────────
    "adx_enabled":          ("filters", ("technical_filters", "adx_filter"), "enabled"),
    "adx_length":           ("filters", ("technical_filters", "adx_filter"), "adx_length"),
    "adx_threshold":        ("filters", ("technical_filters", "adx_filter"), "threshold"),

    # ── Choppiness filter (exploration/discovery zones) ───────────────────────
    "choppiness_enabled":   ("filters", ("technical_filters", "choppiness_filter"), "enabled"),
    "choppiness_length":    ("filters", ("technical_filters", "choppiness_filter"), "length"),
    "choppiness_threshold": ("filters", ("technical_filters", "choppiness_filter"), "threshold"),

    # ── Supertrend filter (exploration/discovery zones) ───────────────────────
    "supertrend_enabled":      ("filters", ("technical_filters", "supertrend_filter"), "enabled"),
    "supertrend_atr_length":   ("filters", ("technical_filters", "supertrend_filter"), "atr_length"),
    "supertrend_factor":       ("filters", ("technical_filters", "supertrend_filter"), "factor"),

    # ── CCI filter (discovery zone) ───────────────────────────────────────────
    "cci_enabled":          ("filters", ("technical_filters", "cci_filter"), "enabled"),
    "cci_length":           ("filters", ("technical_filters", "cci_filter"), "length"),
    "cci_overbought":       ("filters", ("technical_filters", "cci_filter"), "overbought"),
    "cci_oversold":         ("filters", ("technical_filters", "cci_filter"), "oversold"),

    # ── MACD filter (discovery zone) ──────────────────────────────────────────
    "macd_enabled":         ("filters", ("technical_filters", "macd_filter"), "enabled"),
    "macd_fast":            ("filters", ("technical_filters", "macd_filter"), "fast_length"),
    "macd_slow":            ("filters", ("technical_filters", "macd_filter"), "slow_length"),
    "macd_signal":          ("filters", ("technical_filters", "macd_filter"), "signal_length"),

    # ── MA filter (discovery zone) ────────────────────────────────────────────
    "ma_enabled":           ("filters", ("technical_filters", "ma_filter"), "enabled"),
    "ma_length":            ("filters", ("technical_filters", "ma_filter"), "length"),
    "ma_slope_length":      ("filters", ("technical_filters", "ma_filter"), "slope_length"),

    # ── Pivot filter (discovery zone) ─────────────────────────────────────────
    "pivot_enabled":        ("filters", ("technical_filters", "pivot_filter"), "enabled"),
    "pivot_reversal_pct":   ("filters", ("technical_filters", "pivot_filter"), "reversal_percent"),
    "pivot_order":          ("filters", ("technical_filters", "pivot_filter"), "order"),

    # ── DPO filter (discovery zone) ───────────────────────────────────────────
    "dpo_enabled":          ("filters", ("technical_filters", "dpo_filter"), "enabled"),
    "dpo_length":           ("filters", ("technical_filters", "dpo_filter"), "length"),
    "dpo_smooth":           ("filters", ("technical_filters", "dpo_filter"), "smooth"),
    "dpo_threshold":        ("filters", ("technical_filters", "dpo_filter"), "threshold"),
}


def _set_nested(d: dict, top: str, path: tuple[str, ...], key: str, value: object) -> None:
    """
    Navigate d[top][path[0]][path[1]]...[key] and set it to value.
    Creates intermediate dicts if they do not exist.
    """
    node = d.setdefault(top, {})
    for step in path:
        node = node.setdefault(step, {})
    node[key] = value


def generate_trading_yaml(
    candidate: CandidateParameterSet,
    verdict: VerdictResult,
    run_metadata: RunMetadata,
    base_strategy_yaml_path: Path,
    output_path: Path,
) -> Path:
    """
    Merge candidate parameters into the base strategy YAML, embed backtester
    metadata, validate the result, and write to output_path.

    Parameters
    ──────────
    candidate               : The candidate whose parameters to merge.
    verdict                 : The final verdict (for deployment_status metadata).
    run_metadata            : Run provenance (run_id, config_hash, scenario, seeds).
    base_strategy_yaml_path : Path to the operator's base strategy YAML template.
    output_path             : Full path (including filename) to write the output YAML.
                              Caller is responsible for building the path per spec:
                              {output_dir}/trading_yamls/{run_id[:8]}_{candidate_id[:12]}_strategy.yaml

    Returns
    ───────
    output_path (same as input, after successful write).

    Raises
    ──────
    ValueError        : If the merged YAML fails validation.
    FileNotFoundError : If base_strategy_yaml_path does not exist.
    """
    if not base_strategy_yaml_path.exists():
        raise FileNotFoundError(
            f"Base strategy YAML not found: {base_strategy_yaml_path}"
        )

    # ── Load base YAML ────────────────────────────────────────────────────────
    with base_strategy_yaml_path.open("r", encoding="utf-8") as fh:
        base_config: dict = yaml.safe_load(fh) or {}

    merged = copy.deepcopy(base_config)

    # ── Merge candidate parameters ────────────────────────────────────────────
    # B9G-004: Use _PARAM_MAP for correct nested placement.
    # Unknown parameters are logged as warnings — never silently dropped into a
    # phantom "parameters" section that doesn't exist in the template.
    for param_name, param_value in candidate.parameters.items():
        if param_name in _PARAM_MAP:
            top, path, key = _PARAM_MAP[param_name]
            _set_nested(merged, top, path, key, param_value)
        else:
            logger.warning(
                "yaml_generator: unknown parameter '%s' — not mapped to strategy YAML "
                "(add to _PARAM_MAP). Value dropped: %s",
                param_name, param_value,
            )

    # ── Embed backtester metadata ─────────────────────────────────────────────
    merged["backtester_metadata"] = {
        "run_id": run_metadata.run_id,
        "candidate_id": candidate.candidate_id,
        "zone_name": candidate.zone_name,
        "config_hash": run_metadata.config_hash,
        "scenario_name": run_metadata.scenario_name,
        "backtester_version": run_metadata.backtester_version,
        "generated_at": datetime.now(UTC).isoformat(),
        "deployment_status": DeploymentStatus.PAPER_TRADE_REQUIRED.value,
        "verdict": verdict.verdict.value,
        "wfo_consistency_score": verdict.wfo_consistency_score,
        "mc_deep_ruin_probability": verdict.mc_deep_ruin_probability,
        "sensitivity_spike": verdict.sensitivity_spike,
        # Immutable run seeds (audit trail)
        "random_search_seed": run_metadata.random_search_seed,
        "ga_seed": run_metadata.ga_seed,
        "mc_prefilter_seed": run_metadata.mc_prefilter_seed,
        "mc_deep_seed": run_metadata.mc_deep_seed,
        "sensitivity_seed": run_metadata.sensitivity_seed,
    }

    # ── Validate merged YAML ──────────────────────────────────────────────────
    _validate_strategy_config(merged, output_path)

    # ── Write output ──────────────────────────────────────────────────────────
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        yaml.dump(merged, fh, default_flow_style=False, allow_unicode=True, sort_keys=False)

    logger.info(
        "Trading YAML written: %s (candidate %s, verdict %s)",
        output_path.name,
        candidate.candidate_id[:12],
        verdict.verdict.value,
    )
    return output_path


def build_output_path(
    output_dir: Path,
    run_id: str,
    candidate_id: str,
) -> Path:
    """
    Convenience function to build the canonical output path.
    Spec: {output_dir}/trading_yamls/{run_id[:8]}_{candidate_id[:12]}_strategy.yaml
    """
    filename = f"{run_id[:8]}_{candidate_id[:12]}_strategy.yaml"
    return output_dir / "trading_yamls" / filename


# ─────────────────────────────────────────────────────────────────────────────
# Internal validation
# ─────────────────────────────────────────────────────────────────────────────

def _validate_strategy_config(merged: dict, output_path: Path) -> None:
    """
    Validate the merged config against StrategyConfig if importable,
    otherwise fall back to structural check.

    B9G-004 FIX: StrategyConfig.from_yaml() branch is now guarded — if it does
    not raise on a missing required section (i.e. it silently accepts incomplete
    configs), the structural check runs as a hard backstop regardless.
    """
    strategy_config_valid = False

    try:
        from src.strategies.contracts.strategy_config import StrategyConfig  # type: ignore
        import tempfile

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", encoding="utf-8", delete=False
        ) as tmp:
            yaml.dump(merged, tmp, default_flow_style=False, allow_unicode=True)
            tmp_path = Path(tmp.name)

        try:
            StrategyConfig.from_yaml(tmp_path)
            strategy_config_valid = True
            logger.debug("StrategyConfig validation passed for %s.", output_path.name)
        finally:
            tmp_path.unlink(missing_ok=True)

    except ImportError:
        logger.debug(
            "StrategyConfig import unavailable — using structural check for %s.",
            output_path.name,
        )
    except Exception as exc:
        # StrategyConfig.from_yaml() raised — re-raise as ValueError with context
        raise ValueError(
            f"Trading YAML validation failed for {output_path.name}: {exc}"
        ) from exc

    # Always run structural check as hard backstop.
    # B9G-004: StrategyConfig.from_yaml() may silently accept incomplete configs
    # (it did in the run that produced this fix). Structural check ensures the
    # required template sections are present regardless.
    _structural_validate(merged, output_path)


def _structural_validate(merged: dict, output_path: Path) -> None:
    """
    Structural check: required top-level sections from strategy_template.yaml.

    B9G-004 FIX: Previous required_sections was ["strategy", "parameters"] —
    neither of these keys exists in strategy_template.yaml. The actual required
    sections (always present in the template) are:
        filters, trade_management
    These are the two sections that candidate parameter merging writes into,
    so their presence confirms the merge succeeded correctly.

    Does NOT check asset/data/execution/output — those are template pass-throughs
    that cannot be missing unless the base YAML itself is corrupt (caught upstream).
    """
    required_sections = ["filters", "trade_management"]
    missing = [s for s in required_sections if s not in merged]
    if missing:
        raise ValueError(
            f"Trading YAML validation failed for {output_path.name}: "
            f"missing required sections: {missing}. "
            f"Sections present: {list(merged.keys())}"
        )

    # Spot-check: filters.technical_filters must be a dict
    filters = merged.get("filters", {})
    tech_filters = filters.get("technical_filters")
    if not isinstance(tech_filters, dict):
        raise ValueError(
            f"Trading YAML validation failed for {output_path.name}: "
            f"filters.technical_filters must be a dict, "
            f"got {type(tech_filters).__name__}."
        )

    # Spot-check: trade_management.risk must be a dict
    trade_mgmt = merged.get("trade_management", {})
    risk = trade_mgmt.get("risk")
    if not isinstance(risk, dict):
        raise ValueError(
            f"Trading YAML validation failed for {output_path.name}: "
            f"trade_management.risk must be a dict, "
            f"got {type(risk).__name__}."
        )

    logger.debug("Structural YAML validation passed for %s.", output_path.name)
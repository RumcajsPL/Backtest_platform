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

# Keys in candidate.parameters that map to top-level strategy YAML sections
# (not nested under a specific config block). The strategy runner uses flat
# parameter dicts, so we do a best-effort merge into the YAML structure.
_STRATEGY_PARAM_KEY_MAP: dict = {
    # parameter_name → (yaml_section, yaml_key)
    # These are the known strategy YAML keys. If a parameter is not in this map,
    # it is placed in the `parameters` section by default.
    "strategy_tf": ("strategy", "timeframe"),
    "htf_tf": ("strategy", "htf_timeframe"),
    "session_filter": ("filters", "session"),
    "rsi_period": ("parameters", "rsi_period"),
    "rsi_overbought": ("parameters", "rsi_overbought"),
    "rsi_oversold": ("parameters", "rsi_oversold"),
    "adx_threshold": ("parameters", "adx_threshold"),
    "atr_length": ("parameters", "atr_length"),
    "atr_multiplier": ("parameters", "atr_multiplier"),
    "rr_target": ("parameters", "rr_target"),
    "risk_percentile": ("parameters", "risk_percentile"),
}


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
    ValueError   : If the merged YAML fails StrategyConfig validation.
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
    for param_name, param_value in candidate.parameters.items():
        section, key = _STRATEGY_PARAM_KEY_MAP.get(param_name, ("parameters", param_name))
        if section not in merged:
            merged[section] = {}
        merged[section][key] = param_value

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

    # ── Validate merged YAML against StrategyConfig ───────────────────────────
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
    Validate that the merged config can be used as a StrategyConfig.

    Strategy architecture is frozen — we do a structural check here without
    importing StrategyConfig to avoid coupling to strategy internals.
    The check ensures minimum required fields are present and typed correctly.
    If StrategyConfig.from_yaml() is available and callable, prefer that.

    Raises ValueError on validation failure.
    """
    try:
        # Attempt full validation via StrategyConfig if importable
        from src.strategies.contracts.strategy_config import StrategyConfig  # type: ignore
        import tempfile

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", encoding="utf-8", delete=False
        ) as tmp:
            yaml.dump(merged, tmp, default_flow_style=False, allow_unicode=True)
            tmp_path = Path(tmp.name)

        try:
            StrategyConfig.from_yaml(tmp_path)
        finally:
            tmp_path.unlink(missing_ok=True)

        logger.debug("StrategyConfig validation passed for %s.", output_path.name)

    except ImportError:
        # StrategyConfig not importable in this context — fall back to structural check
        logger.debug(
            "StrategyConfig import unavailable — using structural check for %s.",
            output_path.name,
        )
        _structural_validate(merged, output_path)


def _structural_validate(merged: dict, output_path: Path) -> None:
    """
    Minimal structural check when StrategyConfig is not importable.
    Ensures required top-level sections exist and have correct types.
    """
    required_sections = ["strategy", "parameters"]
    missing = [s for s in required_sections if s not in merged]
    if missing:
        raise ValueError(
            f"Trading YAML validation failed for {output_path.name}: "
            f"missing required sections: {missing}. "
            f"Sections present: {list(merged.keys())}"
        )

    strategy_section = merged["strategy"]
    if not isinstance(strategy_section, dict):
        raise ValueError(
            f"Trading YAML validation failed: 'strategy' section must be a dict, "
            f"got {type(strategy_section).__name__}."
        )

    params_section = merged["parameters"]
    if not isinstance(params_section, dict):
        raise ValueError(
            f"Trading YAML validation failed: 'parameters' section must be a dict, "
            f"got {type(params_section).__name__}."
        )

    logger.debug("Structural YAML validation passed for %s.", output_path.name)
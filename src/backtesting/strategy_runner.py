"""
strategy_runner.py — Single candidate evaluation.

This is the only module that knows strategy config field names (the parameter
name mapping from the backtester's parameter space to StrategyConfig YAML keys).

Safety contract:
- NEVER raises. All failures return CandidateResult with error set.
- CacheManager.clear_all_caches() called in every finally block.
- Temp YAML deleted in every finally block (unless retain_temp_yamls=True).

Integration:
- StrategyConfig.from_yaml(path) — fail fast on invalid config
- StrategyOrchestrator(config, cache_manager=cache_manager).run(mode="core")
- OrchestratorResult.metrics (MetricsReport), .trade_result (TradeResult)
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from src.backtesting.contracts import (
    CandidateParameterSet,
    CandidateResult,
    RejectionReason,
)

logger = logging.getLogger(__name__)

# ── Parameter name mapping: backtester name → StrategyConfig YAML key ─────────
# This dict is the ONLY place in the backtester that knows strategy config keys.
# Update here when the strategy YAML schema changes.
_PARAM_KEY_MAP: Dict[str, str] = {
    "rsi_period":       "indicators.rsi.period",
    "rsi_overbought":   "indicators.rsi.overbought",
    "rsi_oversold":     "indicators.rsi.oversold",
    "adx_threshold":    "indicators.adx.threshold",
    "atr_length":       "indicators.atr.length",
    "atr_multiplier":   "trade_management.risk.atr_multiplier_sl",
    "rr_target":        "trade_management.risk.rr_ratio",
    "risk_percentile":  "trade_management.risk.max_risk_percentile",
    "strategy_tf":      "data.strategy_timeframe",
    "htf_tf":           "data.htf_timeframe",
    "session_filter":   "filters.time.session",
}


def evaluate(
    candidate: CandidateParameterSet,
    base_yaml_path: Path,
    temp_dir: Path,
    min_significant_trades: int = 30,
    retain_temp_yamls: bool = False,
) -> CandidateResult:
    """
    Build a temp YAML from the candidate's parameters, run the strategy in
    core mode, apply the significance guard, and return the result.

    Never raises. All failures are returned as CandidateResult with error set.
    CacheManager.clear_all_caches() and temp YAML cleanup happen in every
    finally block — even on exception or early return.
    """
    yaml_path = temp_dir / f"candidate_{candidate.candidate_id[:12]}.yaml"
    cache_manager = None

    try:
        # ── Import strategy components ─────────────────────────────────────
        # Imported inside the function so the module is importable even when
        # the strategy package is not available (e.g. in test environments).
        try:
            from src.strategies.config.config_schema import StrategyConfig
            from src.strategies.orchestrator import StrategyOrchestrator
            from src.strategies.core.cache_manager import CacheManager
        except ImportError as import_err:
            logger.error(
                "Strategy package not importable for candidate %s: %s",
                candidate.candidate_id,
                import_err,
            )
            return CandidateResult(
                candidate_id=candidate.candidate_id,
                evaluated_at=datetime.utcnow(),
                metrics=None,
                trades=None,
                total_trades=None,
                error=f"{RejectionReason.EVALUATION_ERROR.value}: {import_err}",
            )

        cache_manager = CacheManager()

        # ── Write temp YAML ────────────────────────────────────────────────
        _write_temp_yaml(candidate, base_yaml_path, yaml_path)

        # ── Validate config (fail fast) ────────────────────────────────────
        strategy_config = StrategyConfig.from_yaml(yaml_path)

        # ── Run strategy in core mode ──────────────────────────────────────
        orchestrator = StrategyOrchestrator(strategy_config, cache_manager=cache_manager)
        result = orchestrator.run(mode="core")

        metrics = result.metrics
        trades = result.trade_result
        total_trades = metrics.total_trades if metrics is not None else None

        # ── Significance guard ─────────────────────────────────────────────
        if total_trades is None or total_trades < min_significant_trades:
            logger.debug(
                "Candidate %s rejected: %d trades < min %d",
                candidate.candidate_id,
                total_trades or 0,
                min_significant_trades,
            )
            return CandidateResult(
                candidate_id=candidate.candidate_id,
                evaluated_at=datetime.utcnow(),
                metrics=None,
                trades=None,
                total_trades=total_trades,
                error=RejectionReason.REJECTED_INSUFFICIENT_TRADES.value,
            )

        return CandidateResult(
            candidate_id=candidate.candidate_id,
            evaluated_at=datetime.utcnow(),
            metrics=metrics,
            trades=trades,
            total_trades=total_trades,
        )

    except Exception as exc:
        logger.error(
            "Candidate %s evaluation failed: %s",
            candidate.candidate_id,
            exc,
            exc_info=True,
        )
        return CandidateResult(
            candidate_id=candidate.candidate_id,
            evaluated_at=datetime.utcnow(),
            metrics=None,
            trades=None,
            total_trades=None,
            error=str(exc),
        )

    finally:
        # Always clean up — even on exception or early return
        if cache_manager is not None:
            try:
                cache_manager.clear_all_caches()
            except Exception as cache_exc:
                logger.warning("Cache clear failed for %s: %s", candidate.candidate_id, cache_exc)
        if not retain_temp_yamls:
            try:
                yaml_path.unlink(missing_ok=True)
            except Exception as unlink_exc:
                logger.warning("Temp YAML cleanup failed for %s: %s", candidate.candidate_id, unlink_exc)


# ── Internal helpers ───────────────────────────────────────────────────────────

def _write_temp_yaml(
    candidate: CandidateParameterSet,
    base_yaml_path: Path,
    output_path: Path,
) -> None:
    """
    Load the base YAML, deep-set each candidate parameter using dot-notation
    key paths, and write to output_path. Raises on I/O or YAML parse errors.
    """
    with open(base_yaml_path, "r", encoding="utf-8") as f:
        config_dict = yaml.safe_load(f)

    for param_name, value in candidate.parameters.items():
        yaml_key_path = _PARAM_KEY_MAP.get(param_name)
        if yaml_key_path is None:
            # Unknown parameter — this is a defect. Raise to surface it.
            raise KeyError(
                f"No YAML key mapping for parameter '{param_name}'. "
                "Add it to _PARAM_KEY_MAP in strategy_runner.py."
            )
        _deep_set(config_dict, yaml_key_path, value)

    # Always run in core mode
    _deep_set(config_dict, "execution.mode", "core")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(config_dict, f, default_flow_style=False, allow_unicode=True)


def _deep_set(d: dict, dot_path: str, value: Any) -> None:
    """
    Set a value in a nested dict using a dot-separated key path.
    Creates intermediate dicts as needed.
    e.g. _deep_set(d, "trade_management.risk.rr_ratio", 2.0)
    """
    keys = dot_path.split(".")
    for key in keys[:-1]:
        d = d.setdefault(key, {})
    d[keys[-1]] = value
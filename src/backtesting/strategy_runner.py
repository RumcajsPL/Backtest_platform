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
from datetime import UTC, datetime
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
#
# WARNING: Twin key map exists in yaml_generator.py (_STRATEGY_PARAM_KEY_MAP).
# Both files MUST be updated together when adding/renaming strategy parameters.
_PARAM_KEY_MAP: Dict[str, str] = {
    # ── RSI filter (always enabled in safe/exploration zones) ────────────────
    "rsi_period":               "filters.technical_filters.rsi_filter.length",
    "rsi_overbought":           "filters.technical_filters.rsi_filter.overbought",
    "rsi_oversold":             "filters.technical_filters.rsi_filter.oversold",

    # ── Bollinger filter (always enabled in safe/exploration zones) ──────────
    "bollinger_length":         "filters.technical_filters.bollinger_filter.length",
    "bollinger_multiplier":     "filters.technical_filters.bollinger_filter.filter_multiplier",
    "bollinger_width_ma":       "filters.technical_filters.bollinger_filter.width_ma_length",

    # ── ADX filter ───────────────────────────────────────────────────────────
    "adx_enabled":              "filters.technical_filters.adx_filter.enabled",
    "adx_length":               "filters.technical_filters.adx_filter.adx_length",
    "adx_threshold":            "filters.technical_filters.adx_filter.threshold",

    # ── Choppiness filter ────────────────────────────────────────────────────
    "choppiness_enabled":       "filters.technical_filters.choppiness_filter.enabled",
    "choppiness_length":        "filters.technical_filters.choppiness_filter.length",
    "choppiness_threshold":     "filters.technical_filters.choppiness_filter.threshold",

    # ── Supertrend filter ────────────────────────────────────────────────────
    "supertrend_enabled":       "filters.technical_filters.supertrend_filter.enabled",
    "supertrend_atr_length":    "filters.technical_filters.supertrend_filter.atr_length",
    "supertrend_factor":        "filters.technical_filters.supertrend_filter.factor",

    # ── CCI filter ───────────────────────────────────────────────────────────
    "cci_enabled":              "filters.technical_filters.cci_filter.enabled",
    "cci_length":               "filters.technical_filters.cci_filter.length",
    "cci_overbought":           "filters.technical_filters.cci_filter.overbought",
    "cci_oversold":             "filters.technical_filters.cci_filter.oversold",

    # ── MACD filter ──────────────────────────────────────────────────────────
    "macd_enabled":             "filters.technical_filters.macd_filter.enabled",
    "macd_fast":                "filters.technical_filters.macd_filter.fast_length",
    "macd_slow":                "filters.technical_filters.macd_filter.slow_length",
    "macd_signal":              "filters.technical_filters.macd_filter.signal_length",

    # ── MA filter ────────────────────────────────────────────────────────────
    "ma_enabled":               "filters.technical_filters.ma_filter.enabled",
    "ma_length":                "filters.technical_filters.ma_filter.length",
    "ma_slope_length":          "filters.technical_filters.ma_filter.slope_length",
    # ma_type excluded: high interaction effects; add as choice param in dedicated zone (v2+)

    # ── Pivot filter ─────────────────────────────────────────────────────────
    "pivot_enabled":            "filters.technical_filters.pivot_filter.enabled",
    "pivot_reversal_pct":       "filters.technical_filters.pivot_filter.reversal_percent",
    "pivot_order":              "filters.technical_filters.pivot_filter.order",

    # ── DPO filter ───────────────────────────────────────────────────────────
    "dpo_enabled":              "filters.technical_filters.dpo_filter.enabled",
    "dpo_length":               "filters.technical_filters.dpo_filter.length",
    "dpo_smooth":               "filters.technical_filters.dpo_filter.smooth",
    "dpo_threshold":            "filters.technical_filters.dpo_filter.threshold",

    # ── Trade management — risk ───────────────────────────────────────────────
    "atr_length":               "trade_management.risk.atr_length",
    "atr_multiplier":           "trade_management.risk.atr_multiplier_sl",
    "rr_target":                "trade_management.risk.risk_to_reward_ratio",
    "risk_percentile":          "trade_management.risk.max_risk_percentile",

    # EXCLUDED (v2+):
    #   strategy_tf    — data.paths.strategy_ohlcv is a full file path, not a TF field.
    #                    Requires path construction + file existence validation.
    #   htf_tf         — same issue; data.htf_period also needs a matching file path.
    #   session_filter — session_start/end are nested {hour, minute} dicts, not scalars.
    #   filter_sequence — list of 10 names; 10! orderings, no fitness gradient. v2+.
    #   ma_type        — choice param with high interaction effects. Dedicated zone only.
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
                evaluated_at=datetime.now(UTC),
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
        result = orchestrator.run(mode_override="core")

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
                evaluated_at=datetime.now(UTC),
                metrics=None,
                trades=None,
                total_trades=total_trades,
                error=RejectionReason.REJECTED_INSUFFICIENT_TRADES.value,
            )

        return CandidateResult(
            candidate_id=candidate.candidate_id,
            evaluated_at=datetime.now(UTC),
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
            evaluated_at=datetime.now(UTC),
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
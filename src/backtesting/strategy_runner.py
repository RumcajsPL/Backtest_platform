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

Block 9F fix (B9F-005):
  date_start / date_end parameters added to evaluate() and _write_temp_yaml().
  When provided, they override data.date_range.start / data.date_range.end in
  the temp YAML so that WFO window-scoped evaluations use the correct date range.
  Date objects are formatted as "YYYY-MM-DD HH:MM:SS" strings to match the
  strategy_template.yaml format (data.date_range expects full datetime strings).
  When None, the base YAML's date_range is used unchanged (Stage 1 behaviour).

  NOTE: H-01 in the skill was incorrectly marked as FALSE POSITIVE.
  strategy_runner.evaluate() did NOT accept date_start/date_end before this fix.
  The fix is here — wfo_evaluator.py was already correct in passing them.
"""
from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, date
from pathlib import Path
from typing import Any, Dict, Optional, Union

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
}


def evaluate(
    candidate: CandidateParameterSet,
    base_yaml_path: Path,
    temp_dir: Path,
    min_significant_trades: int = 30,
    retain_temp_yamls: bool = False,
    date_start: Optional[Union[date, datetime]] = None,
    date_end: Optional[Union[date, datetime]] = None,
) -> CandidateResult:
    """
    Build a temp YAML from the candidate's parameters, run the strategy in
    core mode, apply the significance guard, and return the result.

    Never raises. All failures are returned as CandidateResult with error set.
    CacheManager.clear_all_caches() and temp YAML cleanup happen in every
    finally block — even on exception or early return.

    Args:
        candidate:               Candidate parameter set to evaluate.
        base_yaml_path:          Path to base strategy_template.yaml.
        temp_dir:                Directory for temp per-candidate YAMLs.
        min_significant_trades:  Significance guard — reject if total_trades < this.
        retain_temp_yamls:       If True, do not delete temp YAML after evaluation.
        date_start:              Optional window start date for WFO scoping.
                                 Overrides data.date_range.start in the temp YAML.
                                 Accepts date or datetime; date is formatted as
                                 "YYYY-MM-DD 00:00:00" to match strategy YAML format.
        date_end:                Optional window end date for WFO scoping.
                                 Overrides data.date_range.end in the temp YAML.
                                 Accepts date or datetime; date is formatted as
                                 "YYYY-MM-DD 23:59:59" to match strategy YAML format.
    """
    yaml_path = temp_dir / f"candidate_{candidate.candidate_id}.yaml"
    cache_manager = None

    try:
        # ── Import strategy components ─────────────────────────────────────
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
        _write_temp_yaml(
            candidate=candidate,
            base_yaml_path=base_yaml_path,
            output_path=yaml_path,
            date_start=date_start,
            date_end=date_end,
        )

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
    date_start: Optional[Union[date, datetime]] = None,
    date_end: Optional[Union[date, datetime]] = None,
) -> None:
    """
    Load the base YAML, deep-set each candidate parameter using dot-notation
    key paths, optionally override the date range for WFO window scoping,
    and write to output_path. Raises on I/O or YAML parse errors.

    B9F-005: date_start / date_end override data.date_range.start / .end when
    provided. date objects are formatted with default session times:
      date_start → "YYYY-MM-DD 00:00:00"
      date_end   → "YYYY-MM-DD 23:59:59"
    datetime objects are formatted as-is via isoformat(sep=" ", timespec="seconds").
    This matches the strategy_template.yaml format for data.date_range fields.
    """
    with open(base_yaml_path, "r", encoding="utf-8") as f:
        config_dict = yaml.safe_load(f)

    for param_name, value in candidate.parameters.items():
        yaml_key_path = _PARAM_KEY_MAP.get(param_name)
        if yaml_key_path is None:
            raise KeyError(
                f"No YAML key mapping for parameter '{param_name}'. "
                "Add it to _PARAM_KEY_MAP in strategy_runner.py."
            )
        _deep_set(config_dict, yaml_key_path, value)

    # ── B9F-005: WFO date range override ──────────────────────────────────────
    if date_start is not None:
        _deep_set(config_dict, "data.date_range.start", _fmt_date(date_start, is_end=False))
    if date_end is not None:
        _deep_set(config_dict, "data.date_range.end", _fmt_date(date_end, is_end=True))
    # ── end B9F-005 ────────────────────────────────────────────────────────────

    # Always run in core mode
    _deep_set(config_dict, "execution.mode", "core")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(config_dict, f, default_flow_style=False, allow_unicode=True)


def _fmt_date(d: Union[date, datetime], is_end: bool) -> str:
    """
    Format a date or datetime to the strategy YAML date_range string format.
    "YYYY-MM-DD HH:MM:SS"

    For date objects:
      - start date → "YYYY-MM-DD 00:00:00"  (beginning of day)
      - end date   → "YYYY-MM-DD 23:59:59"  (end of day)
    For datetime objects: formatted as-is (caller controls time component).
    """
    if isinstance(d, datetime):
        return d.strftime("%Y-%m-%d %H:%M:%S")
    # date object — apply session boundary defaults
    if is_end:
        return f"{d.isoformat()} 23:59:59"
    return f"{d.isoformat()} 00:00:00"


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
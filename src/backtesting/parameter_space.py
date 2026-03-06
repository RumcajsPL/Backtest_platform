"""
parameter_space.py — Expands YAML zone definitions into per-parameter value lists.

No strategy knowledge. No evaluation. Pure expansion logic.

B9F-001: expand_zones() now returns Dict[str, Dict[str, List]] — a mapping of
zone_name → {param_name: [value, ...]} — instead of the full Cartesian product.
The previous implementation called itertools.product() over all parameter value
lists, which materialises every combination in memory. The safe zone has ~2M
combinations (~520MB RAM); the exploration zone has ~387 trillion combinations
and would hang or OOM the process.

The per-param list format is sufficient for all downstream consumers:
  - sampler.sample_lhs()   — uses _lhs_sample() which already works per-param
  - sampler.sample_random() — draws independently per parameter
  - validate_combination()  — unchanged, works on a single params dict vs zone_def
  - parameter_space.get_param_values() — new helper for sensitivity step calculation

The Cartesian product is never needed: LHS and random sampling both draw from
the marginal distributions, not from an enumerated joint distribution.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Dict, List, Any


def expand_zones(config: dict) -> Dict[str, Dict[str, List]]:
    """
    Read zone definitions from config['zones'] and expand each enabled zone
    into a dict of per-parameter value lists.

    For int/float parameters: generates values from min to max (inclusive) at
    the given step size using integer arithmetic to avoid floating-point drift.
    For choice parameters: uses the choices list directly.

    Returns:
        Dict[zone_name, Dict[param_name, List[value]]]

    Disabled zones are excluded from the output.
    Raises ValueError for malformed zone definitions or empty ranges.

    B9F-001: previously returned Dict[zone_name, List[Dict]] (full Cartesian
    product via itertools.product). Replaced with per-param value lists to
    avoid materialising ~387T combinations for the exploration zone.
    """
    zones_config = config.get("zones")
    if not zones_config:
        raise ValueError("config['zones'] is missing or empty")

    result: Dict[str, Dict[str, List]] = {}

    for zone_name, zone_def in zones_config.items():
        if not zone_def.get("enabled", True):
            continue

        params_def = zone_def.get("parameters")
        if not params_def:
            raise ValueError(f"Zone '{zone_name}' has no parameters defined")

        param_value_lists: Dict[str, List] = {}
        for param_name, param_spec in params_def.items():
            param_type = param_spec.get("type")
            if param_type == "choice":
                choices = param_spec.get("choices")
                if not choices:
                    raise ValueError(
                        f"Zone '{zone_name}', param '{param_name}': "
                        f"choice type requires non-empty choices"
                    )
                param_value_lists[param_name] = list(choices)

            elif param_type in ("int", "float"):
                mn = param_spec.get("min")
                mx = param_spec.get("max")
                step = param_spec.get("step")
                if mn is None or mx is None or step is None:
                    raise ValueError(
                        f"Zone '{zone_name}', param '{param_name}': "
                        f"int/float type requires min, max, step"
                    )
                values = _range_values(param_type, mn, mx, step)
                if not values:
                    raise ValueError(
                        f"Zone '{zone_name}', param '{param_name}': "
                        f"empty range [{mn}, {mx}] step {step}"
                    )
                param_value_lists[param_name] = values

            else:
                raise ValueError(
                    f"Zone '{zone_name}', param '{param_name}': "
                    f"unknown type '{param_type}'; expected 'int', 'float', or 'choice'"
                )

        result[zone_name] = param_value_lists

    return result


def validate_combination(params: Dict[str, object], zone_def: dict) -> bool:
    """
    Return True if every parameter value in params is within the zone definition
    bounds (for int/float) or in the choices list (for choice).
    Returns False if any parameter is out of range or unknown.
    """
    params_def = zone_def.get("parameters", {})
    for param_name, value in params.items():
        spec = params_def.get(param_name)
        if spec is None:
            return False
        param_type = spec.get("type")
        if param_type == "choice":
            if value not in spec.get("choices", []):
                return False
        elif param_type in ("int", "float"):
            mn, mx = spec.get("min"), spec.get("max")
            if mn is None or mx is None:
                return False
            if value < mn or value > mx:
                return False
        else:
            return False
    return True


def get_param_values(zone_def: dict, param_name: str) -> List:
    """
    Return the discrete value list for a single parameter in a zone definition.
    Used by sensitivity analysis to enumerate ±step perturbations.

    Raises KeyError if param_name is not in the zone definition.
    Raises ValueError for malformed parameter specs.
    """
    params_def = zone_def.get("parameters", {})
    param_spec = params_def.get(param_name)
    if param_spec is None:
        raise KeyError(
            f"Parameter '{param_name}' not found in zone definition. "
            f"Available: {sorted(params_def.keys())}"
        )

    param_type = param_spec.get("type")
    if param_type == "choice":
        choices = param_spec.get("choices")
        if not choices:
            raise ValueError(
                f"Parameter '{param_name}': choice type requires non-empty choices"
            )
        return list(choices)
    elif param_type in ("int", "float"):
        mn = param_spec.get("min")
        mx = param_spec.get("max")
        step = param_spec.get("step")
        if mn is None or mx is None or step is None:
            raise ValueError(
                f"Parameter '{param_name}': int/float type requires min, max, step"
            )
        return _range_values(param_type, mn, mx, step)
    else:
        raise ValueError(
            f"Parameter '{param_name}': unknown type '{param_type}'; "
            f"expected 'int', 'float', or 'choice'"
        )

# ── Internal helpers ──────────────────────────────────────────────────────────

def _range_values(param_type: str, mn: float, mx: float, step: float) -> list:
    """
    Generate all values from mn to mx (inclusive) at the given step.
    For int type, values are cast to int. For float, kept as float.
    Uses integer arithmetic internally to avoid floating-point drift.

    B9C-005: Scale detection uses Decimal(str(step)) instead of str(step)
    to correctly identify the number of decimal places for floats with
    non-canonical representations (e.g. 0.10000000000001 → '0.1' via Decimal).
    """
    step_decimal = Decimal(str(step))
    step_str = str(step_decimal)
    scale = 1
    if "." in step_str:
        decimal_places = len(step_str.rstrip("0").split(".")[1])
        scale = 10 ** decimal_places

    imin = round(mn * scale)
    imax = round(mx * scale)
    istep = round(step * scale)

    values = []
    current = imin
    while current <= imax + (istep // 2):   # tolerance for fp rounding at boundary
        raw = current / scale
        if param_type == "int":
            values.append(int(round(raw)))
        else:
            values.append(round(raw, 10))
        current += istep

    return values
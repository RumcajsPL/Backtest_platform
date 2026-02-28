"""
parameter_space.py — Expands YAML zone definitions into discrete parameter sets.

No strategy knowledge. No evaluation. Pure expansion logic.
"""
from __future__ import annotations

import itertools
from typing import Dict, List, Any


def expand_zones(config: dict) -> Dict[str, List[Dict[str, object]]]:
    """
    Read zone definitions from config['zones'] and expand each enabled zone
    into a list of all valid parameter combinations.

    For int/float parameters: generates values from min to max (inclusive) at
    the given step size.
    For choice parameters: uses the choices list directly.

    Returns a dict of {zone_name: [param_dict, ...]}.
    Disabled zones are excluded from the output.

    Raises ValueError for malformed zone definitions.
    """
    zones_config = config.get("zones")
    if not zones_config:
        raise ValueError("config['zones'] is missing or empty")

    result: Dict[str, List[Dict[str, object]]] = {}

    for zone_name, zone_def in zones_config.items():
        if not zone_def.get("enabled", True):
            continue

        params_def = zone_def.get("parameters")
        if not params_def:
            raise ValueError(f"Zone '{zone_name}' has no parameters defined")

        # Build discrete value list for each parameter
        param_value_lists: Dict[str, list] = {}
        for param_name, param_spec in params_def.items():
            param_type = param_spec.get("type")
            if param_type == "choice":
                choices = param_spec.get("choices")
                if not choices:
                    raise ValueError(
                        f"Zone '{zone_name}', param '{param_name}': choice type requires non-empty choices"
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

        # Cartesian product of all parameter value lists
        param_names = list(param_value_lists.keys())
        combinations = list(itertools.product(*[param_value_lists[n] for n in param_names]))
        result[zone_name] = [dict(zip(param_names, combo)) for combo in combinations]

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


# ── Internal helpers ──────────────────────────────────────────────────────────

def _range_values(param_type: str, mn: float, mx: float, step: float) -> list:
    """
    Generate all values from mn to mx (inclusive) at the given step.
    For int type, values are cast to int. For float, kept as float.
    Uses integer arithmetic internally to avoid floating-point drift.
    """
    # Scale to avoid floating-point step accumulation (e.g. 0.1 + 0.1 + 0.1 ≠ 0.3)
    # Convert to int arithmetic using a common scale factor
    scale = 1
    step_str = str(step)
    if "." in step_str:
        decimal_places = len(step_str.split(".")[1])
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
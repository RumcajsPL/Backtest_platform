"""
diagnose_crash_candidate.py
Identifies the exact parameter combination that crashes the backtester.
Run from project root: python scripts/diagnostics/diagnose_crash_candidate.py
"""
import sys
from pathlib import Path

import numpy as np
import yaml

# ── config ────────────────────────────────────────────────────────────────
YAML_PATH   = Path("configs/backtesting/backtest_V1_06.yaml")
TARGET_ZONE = "exploration"   # change to "safe" if needed
TARGET_IDX  = 1               # 0-based index within zone (0=first, 1=second, ...)
SEED        = 42
N_SAMPLES   = 100             # samples_per_zone from yaml
# ──────────────────────────────────────────────────────────────────────────

def lhs_sample(params: dict, n: int, seed: int) -> list[dict]:
    """Reproduce the LHS sampling used by the backtester."""
    rng = np.random.default_rng(seed)
    param_names = list(params.keys())
    n_params = len(param_names)

    # Latin Hypercube: divide [0,1] into n strata, sample one per stratum
    samples = []
    cuts = np.linspace(0, 1, n + 1)
    u = np.zeros((n, n_params))
    for j in range(n_params):
        perm = rng.permutation(n)
        u[:, j] = (cuts[perm] + rng.uniform(0, 1/n, n))

    # Map to parameter ranges
    for i in range(n):
        candidate = {}
        for j, name in enumerate(param_names):
            spec = params[name]
            lo   = spec["min"]
            hi   = spec["max"]
            step = spec.get("step", None)
            val  = lo + u[i, j] * (hi - lo)
            if spec["type"] == "int":
                val = int(round(val))
                if step:
                    val = round(val / step) * step
                val = max(lo, min(hi, val))
            else:
                if step:
                    val = round(round(val / step) * step, 10)
                val = max(lo, min(hi, val))
            candidate[name] = val
        samples.append(candidate)
    return samples


def main():
    cfg = yaml.safe_load(YAML_PATH.read_text())
    zones = cfg.get("zones", {})

    if TARGET_ZONE not in zones:
        print(f"Zone '{TARGET_ZONE}' not found. Available: {list(zones.keys())}")
        sys.exit(1)

    zone_cfg = zones[TARGET_ZONE]
    if not zone_cfg.get("enabled", False):
        print(f"Zone '{TARGET_ZONE}' is disabled.")
        sys.exit(1)

    params = zone_cfg["parameters"]
    samples = lhs_sample(params, N_SAMPLES, SEED)

    print(f"Zone: {TARGET_ZONE}  |  N={N_SAMPLES}  |  seed={SEED}")
    print(f"Target candidate index: {TARGET_IDX} (0-based within zone)")
    print(f"This is overall candidate #{100 + TARGET_IDX + 1} in the run")
    print()

    candidate = samples[TARGET_IDX]
    print("=" * 60)
    print(f"  CANDIDATE PARAMETERS")
    print("=" * 60)
    for k, v in candidate.items():
        spec = params[k]
        print(f"  {k:<30} {v}  (range {spec['min']}–{spec['max']})")

    # Flag any suspicious values
    print()
    print("=" * 60)
    print("  SANITY CHECKS")
    print("=" * 60)
    if "macd_fast" in candidate and "macd_slow" in candidate:
        fast = candidate["macd_fast"]
        slow = candidate["macd_slow"]
        if fast >= slow:
            print(f"  ⚠ MACD fast ({fast}) >= slow ({slow}) — INVALID")
        else:
            print(f"  ✓ MACD fast={fast} < slow={slow}")

    if "cci_length" in candidate:
        print(f"  ✓ CCI length={candidate['cci_length']}")

    if "atr_length" in candidate:
        print(f"  ✓ ATR length={candidate['atr_length']}")

    print()
    print("Next step: run a single strategy evaluation with these parameters")
    print("to reproduce the crash in isolation.")


if __name__ == "__main__":
    main()
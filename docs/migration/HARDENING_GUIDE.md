# WBWSStrategy — Production Hardening Checklist
## A quick-reference guide for reviewing new or existing scripts

Based on the patterns identified and fixed across Blocks 1–5 of the hardening sprint.
Use this top-to-bottom when opening any `.py` file for review.

---

## 1. Module Docstring (30 seconds)

**Remove:**
- Session/task references: `Session: 21`, `Task 2`, `Block G`
- Phase labels: `Phase 5 Final`
- Status notes: `copied verbatim from Session 13`
- Internal refactoring comments: `# Moved this check BEFORE...`

**Keep:**
- Version number: `Version: 2.1.0`
- Dated change log entries (without session numbers)
- Architecture decision references: `DEC-037`, `DEC-038`

**Template:**
```python
"""One-line description.

Version: X.Y.Z
Changes from vX.Y.Z-1:
- [finding_id] What changed and why (no session numbers).
"""
```

---

## 2. Imports

### 2a. Dead imports
Run mentally (or with `flake8 --select=F401`):
- Import present but zero usages in the file → remove it
- Common culprits after refactoring: `ClassVar`, `List`, `Tuple`, `ABC`

### 2b. Unparameterized generics (mypy `--strict` catches these)

| Bare (bad) | Typed (good) | Notes |
|---|---|---|
| `Dict` | `Dict[str, Any]` | for generic dicts |
| `Dict` | `Dict[str, str]` | for string-valued dicts (e.g. colour maps) |
| `List` | `List[float]` | always specify element type |
| `Optional[Dict]` | `Optional[Dict[str, Any]]` | same rule applies inside Optional |
| `Tuple` | `Tuple[bool, float, str]` | always specify element types |

### 2c. Missing `Optional` on nullable defaults
```python
# Bad — Dict with None default is a type contradiction
def f(signal_id_map: Dict = None): ...

# Good
def f(signal_id_map: Optional[Dict[str, int]] = None): ...
```

### 2d. Untyped parameters on `__init__`
Every public `__init__` parameter should have a type annotation.
For cross-module types that would cause circular imports, use `TYPE_CHECKING`:
```python
from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from src.config.config_schema import StrategyConfig

class MyClass:
    def __init__(self, config: "StrategyConfig") -> None: ...
```

### 2e. Import ordering (PEP 8)
```
stdlib → third-party → local
blank line between each group
```

---

## 3. Contract Access (Architecture Principle 2)

**Never use `getattr` as a probe into typed contracts:**
```python
# Bad — bypasses the contract, silent on typos, breaks on falsy values
pnl = getattr(t, 'pnl_points', None) or t.exit.pnl_points

# Bad — 0.0 is falsy: breakeven trades silently fall through to the branch
value = getattr(obj, 'field', None) or obj.fallback.field

# Good — use the property the contract defines
pnl = t.pnl_points
```

**The `getattr/or` falsy-zero bug** — memorise this:
- `getattr(obj, 'x', None) or fallback` evaluates `fallback` whenever `x` is `0`, `0.0`, `""`, `[]`, `False`
- Replace with direct property access on the contract

**`isinstance(x, list)` guards exist to work around MagicMock** — remove them:
```python
# Bad — test infrastructure reasoning in production code
_c = getattr(obj, 'closed_trades', None)
if isinstance(_c, list):
    closed = _c
else:
    closed = [t for t in obj.trades if t.exit]

# Good — use the contract property
closed = obj.closed_trades
```

---

## 4. Fail-Fast Principle (Architecture Principle 6)

**No silent failures on construction-time config errors:**
```python
# Bad — swallows ValueError from __post_init__, pipeline continues broken
try:
    self.filter = TimeFilter(config=TypedConfig.from_dict(cfg))
except Exception as e:
    logger.error(e)
    self.filter = None   # ← silent failure

# Good — let it propagate; bad config must fail immediately
self.filter = TimeFilter(config=TypedConfig.from_dict(cfg))
```

**Acceptable `try/except` locations:**
- Third-party filter init (unknown external class signatures)
- Data-dependent computation loops (runtime errors on individual rows)
- Runtime filter application (per `default_error_strategy`)

---

## 5. Null/None Handling

**`None` from YAML must be handled explicitly:**
```python
# YAML: date_range: null → Python dict.get('date_range', {}) returns None, not {}
# Bad
date_range_raw = config.get('date_range', {})  # None overrides {}

# Good — let None flow, handle it downstream
date_range_raw = config.get('date_range')  # None when key absent or null
if date_range_raw is not None:
    ...
```

**Disabled ≡ None ≡ Skip** — disabled config items should never appear in output:
```python
# Bad — disabled filter still produces FilterStatus.SKIPPED entries
if time_filter_cfg is None:
    return  # but not when enabled=False!

# Good
if time_filter_cfg is None or not time_filter_cfg.enabled:
    return  # self.time_filter stays None → apply_filters skips entirely
```

---

## 6. Frozen Dataclass Field Ordering

Python's `@dataclass(frozen=True)` enforces: **all fields with defaults must follow all fields without defaults.**

When adding an `Optional` field to an existing frozen dataclass:

1. Check if any non-defaulted fields come after it — if yes, move it to last
2. The safest position for a new optional field is always last
3. Keyword-arg callers are unaffected by position; positional callers must update

```python
# Bad — Optional field with default between two required fields
@dataclass(frozen=True)
class Insight:
    message: str
    confidence: str
    impact_estimate: Optional[str] = None  # ← cannot be followed by non-defaulted fields
    category: str        # ← TypeError: non-default follows default
    severity: str

# Good — defaulted field last
@dataclass(frozen=True)
class Insight:
    message: str
    confidence: str
    category: str
    severity: str
    impact_estimate: Optional[str] = None  # ← last position: no issue
```

---

## 7. `__main__` and Demo Blocks

**Remove from production modules:**
```python
# Bad — demo code in a production module
if __name__ == "__main__":
    logger = StructuredLogger("DemoModule")
    logger.log_event(...)
    print("✅ Demo logs written...")
```

Place demo/example code in:
- `tests/` (as a test or fixture)
- `examples/` (as a standalone script)
- A docstring example (if short)

---

## 8. Explicit Over Implicit (Architecture Principle 4)

**Pass mode explicitly — never infer it:**
```python
# Bad — DataLoader reconstructs config_path from StrategyConfig internals
loader = DataLoader(config_path=str(self._config.paths.data_file))

# Good — pass the typed config directly; mode is explicit
loader = DataLoader(config=self._config, mode=mode)
```

**Optional config sections accessed via `getattr` on typed objects:**
```python
# Bad — treats typed attribute as plain dict
analytics_cfg = getattr(config, 'analytics', {})
enabled = analytics_cfg.get('profile_simulator', False)

# Good — two-step optional chain; no dict assumptions
_analytics = getattr(config, 'analytics', None)
enabled = bool(getattr(_analytics, 'profile_simulator', False))
```

---

## 9. Quick Scan Checklist (per file)

Run through this mentally in under 2 minutes:

```
□ Module docstring: no session/task/phase numbers?
□ Imports: any unused? (check ClassVar, List, Tuple, ABC especially)
□ Imports: all Dict/List/Tuple/Optional parameterized?
□ Imports: Optional wrapping every param with None default?
□ __init__ params: all typed? TYPE_CHECKING used for cross-module types?
□ getattr probes on contract objects → replaced with direct property access?
□ getattr(...) or fallback pattern → confirmed no falsy-zero bug?
□ isinstance(x, list) MagicMock guards → removed?
□ try/except on __init__ construction code → propagated (P6)?
□ Disabled config → self.field = None → apply_ methods check `is None`?
□ Frozen dataclass with new Optional field → moved to last position?
□ __main__ demo block → removed?
□ to_dict() return type → Dict[str, Any] not bare Dict?
□ Instance dict attrs typed as Dict[str, Any] not bare Dict?
```

---

## 10. Version Bump Convention

When making any hardening change:

```
Minor change (annotation, P9 cleanup)  : patch bump  X.Y.Z → X.Y.Z+1
Behaviour fix (bug, contract change)   : minor bump  X.Y.Z → X.Y+1.0
Breaking contract change               : major bump  X.Y.Z → X+1.0.0
```

Document in the module docstring, not in comments scattered through the file.
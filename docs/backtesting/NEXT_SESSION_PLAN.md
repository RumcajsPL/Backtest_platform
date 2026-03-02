# NEXT_SESSION_PLAN.md — Phase 6 Continuation
**Prepared**: 2026-03-02
**Session goal**: Fix E2E test (all 13 green), complete Block 1 strategy parameter mapping audit, start Block 2 adversarial suite if time permits

---
## How to Start
1. Open new chat, paste `CONTEXT.md` as first message
2. Add: *"Phase 6 in progress, follow NEXT_SESSION_PLAN.md"*
3. **Upload**:
   - `src/backtesting/fitness.py` — needed to diagnose constraint rejection issue
   - `src/backtesting/strategy_runner.py` — to verify current state
   - `tests/backtesting/integration/test_e2e_wbws_real_data.py` — current test
   - `configs/backtesting/backtest_template.yaml` — current config
4. Confirm skill read, CONTEXT.md understood

---
## Block 0 — Fix E2E Test (Priority 1)

### Problem
Pipeline runs cleanly (0 evaluation errors, 0 pipeline errors) but `WFO survivors = 0`.
All 5 seed candidates fail fitness constraints even with the `e2e_test` scenario which has
extremely loose thresholds (min_win_rate=0.05, min_expectancy=-10.0, etc.).

### Observed strategy output on this data slice
```
Total trades  : 1076
Win rate      : 13.2%
Total PnL     : -1108.8 pts
Expectancy    : -1.03 pts/trade
Profit factor : 0.90
Max drawdown  : -1490.2 pts
```

These numbers should easily clear e2e_test constraints. Something is wrong upstream.

### Diagnostic steps
1. Add temporary debug output to `_evaluate_candidate_real()` in the test fixture to print
   actual metric values returned by `evaluate_fitness()` per candidate:
   - `fitness_result.passed_constraints`
   - `fitness_result.rejection_reason`
   - `fitness_result.failing_constraint`
   - `fitness_result.failing_value`
   - All `actual_*` fields

2. Likely suspects (in order of probability):
   a. **fitness.py reads metrics incorrectly** — MetricsReport field names may not match
      what fitness.py expects (e.g. `metrics.win_rate` vs `metrics.win_rate_pct` or similar)
   b. **scenario not switching** — config["scenario"] override may not be propagating to
      `load_scenario()` inside the fixture (check if scenario is loaded before or after override)
   c. **trades_per_week calculation** — fitness.py may be computing this from date range
      in the data rather than actual trading days, producing a very low number
   d. **max_drawdown unit mismatch** — strategy returns drawdown in points (-1490 pts),
      fitness.py may expect a fraction (0.0–1.0)

3. Once root cause confirmed: fix fitness.py or the metric extraction, re-run, confirm
   all 13 tests green.

### Pass criteria (all must hold before moving to Block 1)
- `WFO survivors >= 1` in summary
- P-03 through P-08 all PASS (not SKIP)
- Full summary shows verdicts written

---
## Block 1 — Strategy Parameter Mapping Audit (Priority 2)

### Goal
Validate that the backtester can exercise ALL parametrable features of WBWSStrategy,
not just the small subset currently in `_PARAM_KEY_MAP`. This is a prerequisite for
any meaningful optimization run — if key strategy levers are missing from the parameter
space, the optimizer is working blind.

### What needs auditing

**1. Technical filters — 10 filters, each with:**
- `enabled` flag (bool) — can turn a filter on/off as a discrete parameter
- Per-filter numeric parameters (lengths, thresholds, multipliers)
- Current coverage: rsi ✓, bollinger ✓, adx ✓ — remaining 7 not mapped

Filters to audit:
```
choppiness_filter   (length, threshold)
supertrend_filter   (atr_length, factor)
cci_filter          (length, overbought, oversold)
macd_filter         (fast_length, slow_length, signal_length)
ma_filter           (ma_type, length, slope_length)
pivot_filter        (reversal_percent, order)
dpo_filter          (length, smooth, threshold)
```

**2. Filter sequence order** — `filter_sequence` in strategy_template.yaml is a list
that controls evaluation order. This IS parametrable (different orderings = different
signal behavior). Decision needed: should backtester permute filter order? If yes,
how is this represented as a zone parameter (choice of preset orderings? full permutation?)?

**3. Filter enabled flags** — each filter has `enabled: true/false`. The backtester
could toggle these as discrete (bool/choice) parameters. Current zones have no
enabled-flag parameters. Decision needed: include as choice parameters or handle
differently (e.g. separate zone per filter combination)?

**4. Strategy timeframe (strategy_tf) and HTF (htf_tf)** — already in _PARAM_KEY_MAP
but need to verify the YAML paths are correct and data files exist for all choices
in the zones (M30, H1, H4, D1, W1).

**5. Session filter** — already in map but path `filters.time.session` needs
verification against actual strategy_template.yaml structure.

### Deliverable
- Updated `_PARAM_KEY_MAP` in `strategy_runner.py` covering all audited parameters
- Updated `zones` in `backtest_template.yaml` with decisions on which parameters
  to include (with rationale for any excluded)
- Short audit table documenting: parameter → YAML path → zone inclusion decision

---
## Block 2 — Adversarial Suite (if time permits)

### AV-02: Overfit-injection test
- Create a deliberately curve-fit parameter set (parameters optimized on the exact
  training window, expected to collapse on WFO windows)
- Pipeline must return borderline or no_go — not auto_go
- Implementation: inject a candidate with suspiciously high in-sample fitness,
  verify WFO consistency score is low

### AV-03: Verdict stability under seed perturbation
- Run the same 5-candidate seed set with 3 different random seeds
- At least 80% of verdicts must be identical across runs
- Implementation: run e2e fixture 3 times with different seeds, compare verdict dicts

---
## Output Documents This Session
| Document | Action |
|---|---|
| `tests/backtesting/integration/test_e2e_wbws_real_data.py` | Fix + all 13 green |
| `src/backtesting/fitness.py` | Fix if root cause found there |
| `src/backtesting/strategy_runner.py` | Update _PARAM_KEY_MAP after Block 1 audit |
| `configs/backtesting/backtest_template.yaml` | Update zones after Block 1 audit |
| `CHANGE_LOG.md` | Append SESSION 8 block |
| `CONTEXT.md` | Update phase status |
| `NEXT_SESSION_PLAN.md` | Block 2+ plan |
"""
run_tests_session16.py
======================
Standalone test runner for Session 16 methods.
No pytest required — plain Python.

Run:
    cd /home/claude
    python run_tests_session16.py
"""

import sys, os
sys.path.insert(0, "/home/claude")

from unittest.mock import MagicMock
from pathlib import Path
import pandas as pd
import tempfile
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# ── Minimal framework ─────────────────────────────────────────────────────────
_pass = _fail = 0
_failures = []

def check(condition, label, detail=""):
    global _pass, _fail
    if condition:
        _pass += 1
        print(f"  ✅ {label}")
    else:
        _fail += 1
        msg = f"  ❌ FAIL: {label}"
        if detail: msg += f"\n       → {detail}"
        print(msg)
        _failures.append(f"{label}: {detail}")

def approx(a, b, tol=0.05):
    return abs(a - b) <= tol

def section(title):
    print(f"\n{'='*60}\n  {title}\n{'='*60}")

# ── Fakes ─────────────────────────────────────────────────────────────────────
from dataclasses import dataclass

@dataclass
class FakeMetrics:
    total_trades:      int   = 100
    winning_trades:    int   = 20
    losing_trades:     int   = 80
    win_rate:          float = 20.0
    total_pnl_points:  float = 100.0
    expectancy_points: float = 1.0
    profit_factor:     float = 1.5
    avg_pnl_points:    float = 1.0
    largest_win:       float = 15.0
    largest_loss:      float = -5.0
    max_drawdown:      float = -20.0
    losing_streak:     int   = 5
    winning_streak:    int   = 3
    trades_per_week:   float = 20.0
    trades_per_day:    float = 4.0
    execution_duration_ms: float = 1.5
    execution_date:    str   = "2026-02-17"
    def to_dict(self): return self.__dict__

def make_trade(pnl, bars=5, ts="2024-10-07 09:00:00"):
    t = MagicMock()
    t.entry.entry_time = pd.Timestamp(ts)
    t.exit.pnl_points  = pnl
    t.exit.is_win      = pnl > 0
    t.exit.is_loss     = pnl <= 0
    t.exit.duration_bars = bars
    t.exit.exit_time   = pd.Timestamp(ts)
    t.pnl_points = pnl
    t.is_win  = pnl > 0
    t.is_loss = pnl <= 0
    return t

def make_result(trades):
    r = MagicMock()
    r.trades = trades
    return r

# ── Imports ───────────────────────────────────────────────────────────────────
from src.strategies.specific.modules.trade_analytics import TradeAnalytics
from src.strategies.contracts.analytics_contracts import (
    TradingSessionConfig, RiskAdjustedMetrics, Insight,
    TimePerformanceBreakdown, TradeQualityAnalysis, DurationAnalysis,
    TradeDistribution, ExecutiveSummary, SessionMetrics,
)

metrics = FakeMetrics()
cfg = TradingSessionConfig()


# ── Helpers to build stub breakdown objects ───────────────────────────────────

def empty_time_perf(insights=None):
    return TimePerformanceBreakdown(
        by_session={}, by_hour={}, by_day={},
        best_session="London", worst_session="Asia",
        insights=insights or [],
    )

def empty_quality(insights=None):
    dist = TradeDistribution(small_count=5, medium_count=3, large_count=2,
                             small_pct=50.0, medium_pct=30.0, large_pct=20.0)
    dur  = DurationAnalysis(avg_bars=5.0, median_bars=5,
                             fast_exits_count=10, normal_exits_count=20,
                             prolonged_exits_count=5, fast_exits_pct=28.6,
                             insights=[])
    return TradeQualityAnalysis(
        win_distribution=dist, loss_distribution=dist,
        duration_analysis=dur, avg_bars_to_profit=4.0,
        avg_bars_to_loss=6.0, premature_exit_estimate="Reasonable.",
        insights=insights or [],
    )

def make_risk(avg_ratio=1.5, consistency=65.0, expectancy=1.0,
              return_dd=5.0, recovery=1.5, insights=None):
    return RiskAdjustedMetrics(
        return_over_max_dd=return_dd,
        avg_win_over_avg_loss=avg_ratio,
        expectancy_per_trade=expectancy,
        consistency_score=consistency,
        recovery_factor=recovery,
        insights=insights or [],
    )

def make_insight(severity="warning", confidence="High", category="time"):
    return Insight(
        message=f"Test {severity} insight",
        recommendation="Test recommendation",
        confidence=confidence,
        impact_estimate=None,
        category=category,
        severity=severity,
    )


# ============================================================
# SECTION 1: _calculate_consistency_score
# ============================================================
section("1. _calculate_consistency_score")

# Single trade → neutral
check(TradeAnalytics._calculate_consistency_score([make_trade(5.0)]) == 50.0,
      "Single trade → 50.0")

# All same P&L → perfect consistency (stdev=0, CV=0 → score=100)
same = [make_trade(2.0) for _ in range(10)]
score = TradeAnalytics._calculate_consistency_score(same)
check(score == 100.0, f"Identical PnL → 100.0 (got {score})")

# Zero mean → neutral
zero = [make_trade(2.0), make_trade(-2.0)]
score = TradeAnalytics._calculate_consistency_score(zero)
check(score == 50.0, f"Zero mean → 50.0 (got {score})")

# Very erratic — large swings relative to small positive mean → score well below 80
# pnls [200, -195, 180, -175, 190]: mean=40, cv≈5.1 → score≈49
erratic = [make_trade(p) for p in [200.0, -195.0, 180.0, -175.0, 190.0]]
score = TradeAnalytics._calculate_consistency_score(erratic)
check(score < 60.0, f"Erratic trades → score below 60 (got {score:.1f})")

# Consistent positive → high score
consistent = [make_trade(p) for p in [1.9, 2.0, 2.1, 2.0, 1.95, 2.05]]
score = TradeAnalytics._calculate_consistency_score(consistent)
check(score > 70.0, f"Consistent trades → high score (got {score:.1f})")

# Score always 0-100
for pnls in [[5.0, -4.0, 6.0, -5.0], [0.1]*50, [100.0, -99.9]]:
    s = TradeAnalytics._calculate_consistency_score([make_trade(p) for p in pnls])
    check(0.0 <= s <= 100.0, f"Score in range [0,100]: {s:.1f}")


# ============================================================
# SECTION 2: _analyze_risk_adjusted
# ============================================================
section("2. _analyze_risk_adjusted — metrics")

# Standard case: 2 wins, 1 loss
trades = [make_trade(10.0), make_trade(6.0), make_trade(-4.0)]
m = FakeMetrics(total_trades=3, total_pnl_points=12.0, max_drawdown=-4.0,
                winning_trades=2, losing_trades=1)
tr = make_result(trades)
ra = TradeAnalytics._analyze_risk_adjusted(tr, m)

check(approx(ra.avg_win_over_avg_loss, 2.0),   # avg_win=8, avg_loss=4 → 2.0
      f"avg_win_over_avg_loss ≈ 2.0 (got {ra.avg_win_over_avg_loss})")
check(approx(ra.return_over_max_dd, 3.0),       # 12 / 4 = 3.0
      f"return_over_max_dd ≈ 3.0 (got {ra.return_over_max_dd})")
check(approx(ra.expectancy_per_trade, 4.0),     # 12 / 3 = 4.0
      f"expectancy_per_trade ≈ 4.0 (got {ra.expectancy_per_trade})")
check(approx(ra.recovery_factor, 3.0),          # 12 / 4 = 3.0
      f"recovery_factor ≈ 3.0 (got {ra.recovery_factor})")
check(0 <= ra.consistency_score <= 100,
      f"consistency_score in [0,100]: {ra.consistency_score}")

# All wins → avg_win_over_avg_loss = 0.0 (no losses)
tr_w = make_result([make_trade(5.0) for _ in range(5)])
m_w  = FakeMetrics(total_trades=5, total_pnl_points=25.0, max_drawdown=0.0,
                   winning_trades=5, losing_trades=0)
ra_w = TradeAnalytics._analyze_risk_adjusted(tr_w, m_w)
check(ra_w.avg_win_over_avg_loss == 0.0,
      "All wins: avg_win_over_avg_loss=0.0 (no losses to compare)")
check(ra_w.recovery_factor == 0.0,
      "All wins: recovery_factor=0.0 (no gross loss)")

# Zero drawdown → inf return/DD
m_nd = FakeMetrics(total_trades=3, total_pnl_points=12.0, max_drawdown=0.0,
                   winning_trades=3, losing_trades=0)
ra_nd = TradeAnalytics._analyze_risk_adjusted(make_result([make_trade(4.0)]*3), m_nd)
check(ra_nd.return_over_max_dd == float("inf"),
      "Zero DD with positive PnL → return_over_max_dd=inf")

# Insights are generated and valid
for i in ra.insights:
    check(i.confidence in {"High","Medium","Low"}, f"Risk insight confidence valid: {i.confidence}")
    check(i.category == "risk",                    f"Risk insight category='risk': {i.category}")
    check(i.severity in {"critical","warning","info","success"},
          f"Risk insight severity valid: {i.severity}")


# ============================================================
# SECTION 3: _generate_risk_insights — all rules
# ============================================================
section("3. _generate_risk_insights — rules")

m_base = FakeMetrics()

# Rule 1: poor risk/reward → critical
bad_rr = make_risk(avg_ratio=0.5)
ins = TradeAnalytics._generate_risk_insights(bad_rr, m_base)
crit = [i for i in ins if i.severity == "critical"]
check(len(crit) >= 1, "Rule 1: poor risk/reward → critical")

# Rule 2: excellent risk/reward → success
good_rr = make_risk(avg_ratio=2.5)
ins = TradeAnalytics._generate_risk_insights(good_rr, m_base)
succ = [i for i in ins if i.severity == "success" and "risk/reward" in i.message.lower()]
check(len(succ) >= 1, "Rule 2: excellent risk/reward → success")

# Rule 3: low consistency → warning
low_c = make_risk(consistency=20.0)
ins = TradeAnalytics._generate_risk_insights(low_c, m_base)
warn = [i for i in ins if i.severity == "warning" and "consistency" in i.message.lower()]
check(len(warn) >= 1, "Rule 3: low consistency → warning")

# Rule 4: moderate consistency → info
mod_c = make_risk(consistency=40.0)
ins = TradeAnalytics._generate_risk_insights(mod_c, m_base)
info = [i for i in ins if i.severity == "info" and "consistency" in i.message.lower()]
check(len(info) >= 1, "Rule 4: moderate consistency → info")

# Rule 5: negative expectancy → critical
neg_e = make_risk(expectancy=-0.5)
ins = TradeAnalytics._generate_risk_insights(neg_e, m_base)
crit_e = [i for i in ins if i.severity == "critical" and "expectancy" in i.message.lower()]
check(len(crit_e) >= 1, "Rule 5: negative expectancy → critical")

# Rule 6: strong recovery → success
strong_r = make_risk(recovery=3.0)
ins = TradeAnalytics._generate_risk_insights(strong_r, m_base)
succ_r = [i for i in ins if i.severity == "success" and "recovery" in i.message.lower()]
check(len(succ_r) >= 1, "Rule 6: strong recovery → success")

# Rule 7: weak recovery → warning
weak_r = make_risk(recovery=0.3)
ins = TradeAnalytics._generate_risk_insights(weak_r, m_base)
warn_r = [i for i in ins if i.severity == "warning" and "recovery" in i.message.lower()]
check(len(warn_r) >= 1, "Rule 7: weak recovery → warning")


# ============================================================
# SECTION 4: _calculate_performance_grade
# ============================================================
section("4. _calculate_performance_grade")

VALID_GRADES = {"A+","A","A-","B+","B","B-","C+","C","C-","D+","D","D-","F","N/A"}

# High performer → should be A range
m_a = FakeMetrics(win_rate=25.0, profit_factor=2.5, total_pnl_points=200.0,
                  max_drawdown=-20.0)  # DD = 10% of profit → excellent
ra_a = make_risk(consistency=80.0)
grade, reasoning = TradeAnalytics._calculate_performance_grade(m_a, ra_a)
check(grade in VALID_GRADES, f"Grade is valid: {grade}")
check(grade in {"A+","A","A-","B+"}, f"High performer gets A/B+ range (got {grade})")
check(len(reasoning) > 10, "Reasoning is non-trivial")

# Weak performer → lower grade
m_f = FakeMetrics(win_rate=5.0, profit_factor=0.8, total_pnl_points=-50.0,
                  max_drawdown=-80.0)
ra_f = make_risk(consistency=15.0)
grade_f, _ = TradeAnalytics._calculate_performance_grade(m_f, ra_f)
check(grade_f in {"D+","D","D-","F"}, f"Weak performer gets D/F (got {grade_f})")

# Mid performer → B/C range
m_b = FakeMetrics(win_rate=18.0, profit_factor=1.4, total_pnl_points=50.0,
                  max_drawdown=-30.0)
ra_b = make_risk(consistency=50.0)
grade_b, _ = TradeAnalytics._calculate_performance_grade(m_b, ra_b)
check(grade_b in {"B+","B","B-","C+","C","C-"}, f"Mid performer B/C range (got {grade_b})")

# Grade is always in valid set
for wr, pf, pnl, dd, cs in [
    (20, 2.0, 100, -10, 75), (10, 1.2, 30, -40, 40),
    (5,  0.9, -20, -50, 20), (30, 3.0, 300, -5, 90),
]:
    m_t  = FakeMetrics(win_rate=wr, profit_factor=pf, total_pnl_points=pnl, max_drawdown=dd)
    ra_t = make_risk(consistency=cs)
    g, _ = TradeAnalytics._calculate_performance_grade(m_t, ra_t)
    check(g in VALID_GRADES, f"Grade valid for wr={wr} pf={pf}: {g}")


# ============================================================
# SECTION 5: _collect_critical_insights
# ============================================================
section("5. _collect_critical_insights")

# Priority: critical before warning before info before success
tp_ins = [make_insight("warning", "High", "time"),
          make_insight("success", "High", "time")]
q_ins  = [make_insight("critical", "High", "quality"),
          make_insight("info",     "Low",  "quality")]
r_ins  = [make_insight("critical", "Medium", "risk"),
          make_insight("warning",  "Low",    "risk")]

tp = empty_time_perf(tp_ins)
q  = empty_quality(q_ins)
ra = make_risk(insights=r_ins)

result = TradeAnalytics._collect_critical_insights(tp, q, ra)
check(len(result) <= 5,                     "At most 5 insights returned")
check(result[0].severity == "critical",     "First insight is critical")
check(result[1].severity == "critical",     "Second insight is critical")
check(result[2].severity == "warning",      "Third insight is warning")

# Single domain — only time insights
tp_only = empty_time_perf([make_insight("critical")])
result2 = TradeAnalytics._collect_critical_insights(
    tp_only, empty_quality(), make_risk()
)
check(len(result2) >= 1, "Collects from single domain")

# Empty — no crash
result3 = TradeAnalytics._collect_critical_insights(
    empty_time_perf(), empty_quality(), make_risk()
)
check(result3 == [], "All empty → empty list")

# Cap at 5 even with many insights
many = [make_insight("warning") for _ in range(10)]
tp_many = empty_time_perf(many)
result4 = TradeAnalytics._collect_critical_insights(
    tp_many, empty_quality(), make_risk()
)
check(len(result4) == 5, f"Capped at 5 (got {len(result4)})")


# ============================================================
# SECTION 6: _generate_executive_summary
# ============================================================
section("6. _generate_executive_summary")

# Standard case
m_std = FakeMetrics(win_rate=20.0, profit_factor=1.8, total_pnl_points=150.0,
                    max_drawdown=-25.0)
tp_std = empty_time_perf([make_insight("success", "High", "time")])
q_std  = empty_quality([make_insight("warning", "High", "quality")])
ra_std = make_risk(consistency=65.0, avg_ratio=1.8, recovery=2.5,
                   insights=[make_insight("success", "High", "risk")])

es = TradeAnalytics._generate_executive_summary(m_std, tp_std, q_std, ra_std, None)

check(es.performance_grade in VALID_GRADES,  f"Grade valid: {es.performance_grade}")
check(len(es.grade_reasoning) > 10,           "Grade reasoning non-trivial")
check(len(es.overall_assessment) > 20,        "Assessment non-trivial")
check(len(es.critical_insights) <= 5,         "Max 5 critical insights")
check(len(es.key_strengths) >= 1,             "At least 1 strength")
check(isinstance(es.key_strengths, list),     "Strengths is a list")
check(isinstance(es.improvement_areas, list), "Improvements is a list")

# Overall assessment mentions total trades and P&L
check(str(m_std.total_trades) in es.overall_assessment or
      f"{m_std.total_trades:,}" in es.overall_assessment,
      "Assessment mentions total trades")

# Empty insights — should not crash
es_empty = TradeAnalytics._generate_executive_summary(
    metrics, empty_time_perf(), empty_quality(), make_risk(), None
)
check(es_empty.performance_grade in VALID_GRADES, "Empty insights: grade still valid")
check(len(es_empty.overall_assessment) > 10,      "Empty insights: assessment populated")


# ============================================================
# SECTION 7: format_markdown_report
# ============================================================
section("7. format_markdown_report")

# Build a minimal but complete AnalyticsReport
from src.strategies.contracts.analytics_contracts import (
    AnalyticsReport, ComparativeContext
)

def build_report():
    m = FakeMetrics(win_rate=18.5, profit_factor=1.6, total_pnl_points=120.0,
                    max_drawdown=-22.0, total_trades=200)
    tp = empty_time_perf([make_insight("warning", "High", "time")])
    q  = empty_quality([make_insight("critical", "High", "quality")])
    ra_r = make_risk(consistency=60.0, avg_ratio=1.6, recovery=1.8,
                     insights=[make_insight("success", "High", "risk")])
    comp = ComparativeContext(vs_baseline=None, statistical_flags=[], percentile_rank=None)
    es = TradeAnalytics._generate_executive_summary(m, tp, q, ra_r, comp)
    return AnalyticsReport(
        executive_summary=es, time_performance=tp, trade_quality=q,
        risk_adjusted=ra_r, comparative=comp, input_metrics=m,
        analysis_timestamp="2026-02-17T12:00:00",
        analysis_duration_ms=5.2,
    )

report = build_report()
md = TradeAnalytics.format_markdown_report(report)

check(isinstance(md, str),                          "Returns a string")
check(len(md) > 200,                                "Non-trivial length")
check("STRATEGY PERFORMANCE ANALYSIS" in md,        "Has header")
check("KEY INSIGHTS" in md,                         "Has KEY INSIGHTS section")
check("STRENGTHS" in md,                            "Has STRENGTHS section")
check("IMPROVEMENT AREAS" in md,                    "Has IMPROVEMENT AREAS section")
check("TIME-BASED PERFORMANCE" in md,               "Has TIME section")
check("TRADE QUALITY" in md,                        "Has TRADE QUALITY section")
check("RISK-ADJUSTED METRICS" in md,                "Has RISK section")
check("PERFORMANCE GRADE" in md,                    "Has GRADE section")
check(report.executive_summary.performance_grade in md, "Grade appears in report")
check("18.5" in md or "18.50" in md,                "Win rate appears in report")
check("5.2" in md or "5.1" in md,                   "Duration appears in report")


# ============================================================
# SECTION 8: _save_report
# ============================================================
section("8. _save_report")

with tempfile.TemporaryDirectory() as tmpdir:
    output_path = Path(tmpdir) / "analytics_out"
    report = build_report()
    TradeAnalytics._save_report(report, output_path)

    saved_files = list(output_path.glob("analytics_*"))
    json_files  = list(output_path.glob("analytics_*.json"))
    md_files    = list(output_path.glob("analytics_*.md"))

    check(len(saved_files) == 2,    f"Exactly 2 files saved (got {len(saved_files)})")
    check(len(json_files)  == 1,    "JSON file created")
    check(len(md_files)    == 1,    "Markdown file created")

    if json_files:
        import json
        content = json_files[0].read_text(encoding="utf-8")
        parsed  = json.loads(content)
        check("executive_summary" in parsed, "JSON has executive_summary")
        check("time_performance"  in parsed, "JSON has time_performance")
        check("risk_adjusted"     in parsed, "JSON has risk_adjusted")
        check("metadata"          in parsed, "JSON has metadata")

    if md_files:
        md_content = md_files[0].read_text(encoding="utf-8")
        check("STRATEGY PERFORMANCE ANALYSIS" in md_content, "MD file has header")
        check(len(md_content) > 100,                         "MD file non-trivial")

# Default output_dir (None) — should not crash and creates outputs/analytics
try:
    # We mock this to avoid writing to disk in CI
    import unittest.mock as um
    with um.patch("pathlib.Path.mkdir"), um.patch("pathlib.Path.write_text"):
        TradeAnalytics._save_report(build_report(), None)
    check(True, "save_report(report, None) does not crash")
except Exception as e:
    check(False, f"save_report(report, None) crashed: {e}")


# ============================================================
# SECTION 9: Full pipeline integration
# ============================================================
section("9. Full pipeline — analyze() end-to-end")

# Build 20 realistic trades
import random
random.seed(42)
pipeline_trades = []
for i in range(20):
    ts = f"2024-10-{7 + i//5:02d} {9 + (i % 8):02d}:00:00"
    pnl = random.choice([4.0, 7.0, -3.0, -2.0, 10.0, -5.0, 2.0, -1.0])
    bars = random.randint(2, 12)
    t = MagicMock()
    t.entry.entry_time = pd.Timestamp(ts)
    t.exit.pnl_points  = pnl
    t.exit.is_win      = pnl > 0
    t.exit.is_loss     = pnl <= 0
    t.exit.duration_bars = bars
    t.exit.exit_time   = pd.Timestamp(ts)
    t.pnl_points = pnl
    t.is_win     = pnl > 0
    t.is_loss    = pnl <= 0
    pipeline_trades.append(t)

tr_full = make_result(pipeline_trades)
m_full  = FakeMetrics(
    total_trades=20, winning_trades=12, losing_trades=8,
    win_rate=60.0, total_pnl_points=35.0, max_drawdown=-8.0,
    profit_factor=2.1, largest_win=10.0, largest_loss=-5.0,
)

report_full = TradeAnalytics.analyze(
    tr_full, object(), metrics=m_full, session_config=cfg
)

check(report_full.executive_summary.performance_grade in VALID_GRADES,
      f"Full pipeline: grade valid ({report_full.executive_summary.performance_grade})")
check(len(report_full.time_performance.by_session) > 0,
      "Full pipeline: sessions populated")
check(report_full.risk_adjusted.consistency_score >= 0,
      "Full pipeline: consistency score computed")
check(report_full.analysis_duration_ms > 0,
      f"Full pipeline: duration {report_full.analysis_duration_ms:.2f}ms")

# Markdown generation
md_full = TradeAnalytics.format_markdown_report(report_full)
check("STRATEGY PERFORMANCE ANALYSIS" in md_full, "Full pipeline: markdown generates")
check(len(md_full) > 300, "Full pipeline: markdown is substantial")

# JSON roundtrip
import json
json_str = report_full.to_json()
parsed = json.loads(json_str)
check("executive_summary" in parsed, "Full pipeline: JSON roundtrip OK")

print(f"\n  Grade: {report_full.executive_summary.performance_grade}")
print(f"  Assessment: {report_full.executive_summary.overall_assessment[:80]}...")
print(f"  Duration: {report_full.analysis_duration_ms:.2f}ms")
print(f"  Total insights: {len(report_full.get_all_insights())}")


# ============================================================
# SUMMARY
# ============================================================
print(f"\n{'='*60}")
print(f"  SESSION 16 TEST RESULTS")
print(f"{'='*60}")
print(f"  ✅ Passed: {_pass}")
print(f"  ❌ Failed: {_fail}")
print(f"  Total:   {_pass + _fail}")

if _failures:
    print("\nFAILURES:")
    for f in _failures:
        print(f"  • {f}")
    sys.exit(1)
else:
    print("\n  🎉 ALL TESTS PASSED")
    sys.exit(0)
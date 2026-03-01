"""
report_generator.py
────────────────────
Stage 7 — Report Generator.

Reads all stage results from CandidateStore and produces:
  - A self-contained HTML report (single file, no external deps, inline charts)
  - Per-candidate JSON output (all stage results flattened)
  - Per-candidate Parquet output (all stage results flattened, via pandas)
  - Per-borderline-candidate adversarial checklist HTML (separate file)

Charts are rendered with matplotlib (Agg backend) and embedded as base64 PNG.
The HTML template is built inline (no Jinja2 file dependency) using a template
string rendered with str.format_map or f-string substitution.

Scenario framing: report_emphasis from ScenarioProfile controls metric ordering
in the per-candidate detail section.

Public interface
────────────────
    generate_report(
        store, run_id, scenario, output_dir, formats
    ) -> None
"""

from __future__ import annotations

import base64
import io
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.backtesting.contracts import (
    ScenarioProfile,
    Verdict,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Public interface
# ─────────────────────────────────────────────────────────────────────────────

def generate_report(
    store: Any,  # CandidateStore — typed as Any to avoid circular import
    run_id: str,
    scenario: ScenarioProfile,
    output_dir: Path,
    formats: dict,  # {"html": True, "json": True, "parquet": True}
) -> None:
    """
    Generate all configured output formats for a completed pipeline run.

    Parameters
    ──────────
    store      : CandidateStore instance (reads all stage data from SQLite).
    run_id     : The pipeline run to report on.
    scenario   : Active ScenarioProfile (controls report framing).
    output_dir : Root output directory. Sub-dirs created as needed.
    formats    : Dict of format → bool, e.g. {"html": True, "json": True, "parquet": False}.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # ── Fetch all data from store ─────────────────────────────────────────────
    report_data = _collect_report_data(store, run_id)

    if not report_data["verdicts"]:
        logger.warning("No verdicts found for run %s — report will be empty.", run_id[:8])

    # ── HTML report ───────────────────────────────────────────────────────────
    if formats.get("html", True):
        html_path = output_dir / f"report_{run_id[:8]}.html"
        _write_html_report(report_data, scenario, run_id, html_path)
        logger.info("HTML report written: %s", html_path)

        # Per-borderline adversarial checklist
        _write_borderline_checklists(report_data, scenario, run_id, output_dir)

    # ── JSON output ───────────────────────────────────────────────────────────
    if formats.get("json", True):
        json_dir = output_dir / "json"
        json_dir.mkdir(exist_ok=True)
        for flat_record in report_data["flat_records"]:
            candidate_id = flat_record.get("candidate_id", "unknown")
            json_path = json_dir / f"{run_id[:8]}_{candidate_id[:12]}.json"
            json_path.write_text(
                json.dumps(flat_record, indent=2, default=str), encoding="utf-8"
            )
        logger.info("JSON records written to %s (%d files).", json_dir, len(report_data["flat_records"]))

    # ── Parquet output ────────────────────────────────────────────────────────
    if formats.get("parquet", True):
        _write_parquet_records(report_data, run_id, output_dir)


# ─────────────────────────────────────────────────────────────────────────────
# Data collection
# ─────────────────────────────────────────────────────────────────────────────

def _collect_report_data(store: Any, run_id: str) -> dict:
    """
    Pull all relevant data from the store for report generation.
    Returns a structured dict ready for rendering.
    """
    # Query verdicts
    verdicts = _safe_query(store, "query_verdicts", run_id) or []
    candidates = _safe_query(store, "query_candidates", run_id) or []
    wfo_scores = _safe_query(store, "query_wfo_consistency_scores", run_id) or []
    mc_results_deep = _safe_query(store, "query_mc_results", run_id, mode="deep") or []
    sensitivity_profiles = _safe_query(store, "query_sensitivity_profiles", run_id) or []
    run_meta = _safe_query(store, "get_run_metadata", run_id)

    # Build lookup maps
    verdict_map = {v.get("candidate_id"): v for v in verdicts}
    wfo_map = {w.get("candidate_id"): w for w in wfo_scores}
    mc_map = {m.get("candidate_id"): m for m in mc_results_deep}
    sens_map = {s.get("candidate_id"): s for s in sensitivity_profiles}

    # Build flat records for JSON/Parquet
    flat_records = []
    for cand in candidates:
        cid = cand.get("candidate_id")
        record = dict(cand)
        record.update(wfo_map.get(cid, {}))
        record.update(mc_map.get(cid, {}))
        record.update(sens_map.get(cid, {}))
        record.update(verdict_map.get(cid, {}))
        flat_records.append(record)

    # Funnel statistics
    funnel = _compute_funnel(candidates)

    return {
        "run_meta": run_meta,
        "verdicts": verdicts,
        "candidates": candidates,
        "wfo_scores": wfo_scores,
        "mc_results_deep": mc_results_deep,
        "sensitivity_profiles": sensitivity_profiles,
        "verdict_map": verdict_map,
        "wfo_map": wfo_map,
        "mc_map": mc_map,
        "sens_map": sens_map,
        "flat_records": flat_records,
        "funnel": funnel,
        "run_id": run_id,
        "generated_at": datetime.now(UTC).isoformat(),
    }


def _safe_query(store: Any, method_name: str, *args, **kwargs):
    """Call a store method if it exists, return None on any failure."""
    method = getattr(store, method_name, None)
    if method is None:
        return None
    try:
        return method(*args, **kwargs)
    except Exception as exc:
        logger.warning("Store query '%s' failed: %s", method_name, exc)
        return None


def _compute_funnel(candidates: list) -> dict:
    """Compute stage-level funnel counts from candidate records."""
    from collections import Counter
    stage_counts = Counter(c.get("stage") or c.get("origin_stage", "UNKNOWN") for c in candidates)
    return dict(stage_counts)


# ─────────────────────────────────────────────────────────────────────────────
# HTML report
# ─────────────────────────────────────────────────────────────────────────────

def _write_html_report(
    data: dict,
    scenario: ScenarioProfile,
    run_id: str,
    output_path: Path,
) -> None:
    """Render and write the full HTML report."""
    verdicts = data["verdicts"]
    go_candidates = [v for v in verdicts if v.get("verdict") == Verdict.AUTO_GO.value]
    borderline_candidates = [v for v in verdicts if v.get("verdict") == Verdict.BORDERLINE.value]
    no_go_candidates = [v for v in verdicts if v.get("verdict") == Verdict.NO_GO.value]

    run_summary_html = _render_run_summary(data, go_candidates, borderline_candidates, no_go_candidates)
    funnel_html = _render_funnel(data["funnel"])
    shortlist_html = _render_shortlist(go_candidates + borderline_candidates, data, scenario)
    candidate_details_html = _render_all_candidate_details(verdicts, data, scenario)

    generated_at = data["generated_at"]
    scenario_name = scenario.name

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Backtester Report — Run {run_id[:8]} — {scenario_name}</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         margin: 0; padding: 0; background: #f5f5f5; color: #222; }}
  .container {{ max-width: 1200px; margin: 0 auto; padding: 24px; }}
  h1 {{ color: #1a1a2e; border-bottom: 3px solid #0f3460; padding-bottom: 10px; }}
  h2 {{ color: #0f3460; margin-top: 40px; }}
  h3 {{ color: #16213e; }}
  .card {{ background: white; border-radius: 8px; padding: 20px; margin: 16px 0;
           box-shadow: 0 2px 8px rgba(0,0,0,0.08); }}
  .verdict-go {{ border-left: 5px solid #27ae60; }}
  .verdict-borderline {{ border-left: 5px solid #f39c12; }}
  .verdict-no_go {{ border-left: 5px solid #e74c3c; }}
  .badge {{ display: inline-block; padding: 3px 10px; border-radius: 12px;
            font-size: 12px; font-weight: 600; text-transform: uppercase; }}
  .badge-go {{ background: #27ae60; color: white; }}
  .badge-borderline {{ background: #f39c12; color: white; }}
  .badge-no_go {{ background: #e74c3c; color: white; }}
  .metric-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                  gap: 12px; margin: 12px 0; }}
  .metric-cell {{ background: #f8f9fa; border-radius: 6px; padding: 12px; }}
  .metric-label {{ font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: 0.5px; }}
  .metric-value {{ font-size: 20px; font-weight: 700; color: #1a1a2e; margin-top: 4px; }}
  .funnel-bar {{ display: flex; align-items: center; margin: 8px 0; }}
  .funnel-label {{ width: 180px; font-size: 13px; }}
  .funnel-fill {{ height: 20px; background: #0f3460; border-radius: 3px; min-width: 4px; }}
  .funnel-count {{ margin-left: 8px; font-size: 13px; color: #666; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th {{ background: #1a1a2e; color: white; padding: 10px 12px; text-align: left; }}
  td {{ padding: 8px 12px; border-bottom: 1px solid #eee; }}
  tr:hover td {{ background: #f8f9fa; }}
  .chart-img {{ max-width: 100%; border-radius: 6px; margin: 8px 0; }}
  .flag {{ display: inline-block; background: #e74c3c; color: white;
           font-size: 11px; padding: 2px 8px; border-radius: 10px; margin: 2px; }}
  .evidence {{ background: #f8f9fa; border-radius: 6px; padding: 12px;
               font-size: 13px; line-height: 1.6; margin-top: 8px; }}
  .deployment {{ background: #fff3cd; border: 1px solid #ffc107; border-radius: 6px;
                 padding: 10px 14px; font-size: 13px; margin-top: 8px; }}
  .meta-info {{ color: #888; font-size: 12px; margin-top: 4px; }}
</style>
</head>
<body>
<div class="container">
  <h1>📊 Backtester Report</h1>
  <p class="meta-info">Run ID: <code>{run_id}</code> | Scenario: <strong>{scenario_name}</strong> | Generated: {generated_at} (UTC)</p>

  <h2>Run Summary</h2>
  {run_summary_html}

  <h2>Pipeline Funnel</h2>
  {funnel_html}

  <h2>Ranked Shortlist (Go &amp; Borderline)</h2>
  {shortlist_html}

  <h2>Per-Candidate Detail</h2>
  {candidate_details_html}

</div>
</body>
</html>"""

    output_path.write_text(html, encoding="utf-8")


def _render_run_summary(data: dict, go: list, borderline: list, no_go: list) -> str:
    total = len(data["verdicts"])
    return f"""
<div class="card">
  <div class="metric-grid">
    <div class="metric-cell">
      <div class="metric-label">Total Evaluated</div>
      <div class="metric-value">{len(data['candidates'])}</div>
    </div>
    <div class="metric-cell">
      <div class="metric-label">Auto Go</div>
      <div class="metric-value" style="color:#27ae60">{len(go)}</div>
    </div>
    <div class="metric-cell">
      <div class="metric-label">Borderline</div>
      <div class="metric-value" style="color:#f39c12">{len(borderline)}</div>
    </div>
    <div class="metric-cell">
      <div class="metric-label">No Go</div>
      <div class="metric-value" style="color:#e74c3c">{len(no_go)}</div>
    </div>
    <div class="metric-cell">
      <div class="metric-label">Total Verdicts</div>
      <div class="metric-value">{total}</div>
    </div>
  </div>
</div>"""


def _render_funnel(funnel: dict) -> str:
    if not funnel:
        return "<div class='card'>No funnel data available.</div>"
    total = max(funnel.values()) if funnel else 1
    rows = ""
    stage_order = ["RANDOM", "MC_PREFILTER_PASS", "GA", "WFO", "MC_DEEP", "SENSITIVITY"]
    ordered_stages = [s for s in stage_order if s in funnel] + [
        s for s in funnel if s not in stage_order
    ]
    for stage in ordered_stages:
        count = funnel.get(stage, 0)
        pct = (count / total) * 100 if total > 0 else 0
        rows += f"""
    <div class="funnel-bar">
      <div class="funnel-label">{stage}</div>
      <div class="funnel-fill" style="width:{max(pct, 0.5):.1f}%"></div>
      <div class="funnel-count">{count}</div>
    </div>"""
    return f"<div class='card'>{rows}</div>"


def _render_shortlist(candidates: list, data: dict, scenario: ScenarioProfile) -> str:
    if not candidates:
        return "<div class='card'>No go or borderline candidates found.</div>"

    # Sort: AUTO_GO first, then by wfo_consistency_score DESC
    def sort_key(v):
        is_go = 1 if v.get("verdict") == Verdict.AUTO_GO.value else 0
        wfo = v.get("wfo_consistency_score") or 0.0
        return (-is_go, -wfo)

    sorted_cands = sorted(candidates, key=sort_key)

    rows = ""
    for v in sorted_cands:
        cid = v.get("candidate_id", "")
        verdict_val = v.get("verdict", "")
        badge_cls = f"badge-{verdict_val}"
        wfo = v.get("wfo_consistency_score")
        ruin = v.get("mc_deep_ruin_probability")
        spike = v.get("sensitivity_spike")
        rows += f"""
    <tr>
      <td><code>{cid[:12]}</code></td>
      <td><span class="badge {badge_cls}">{verdict_val}</span></td>
      <td>{f"{wfo:.3f}" if wfo is not None else "—"}</td>
      <td>{f"{ruin:.3f}" if ruin is not None else "—"}</td>
      <td>{"⚠ spike" if spike else "—"}</td>
      <td style="font-size:11px">{v.get("evidence_summary", "")[:120]}…</td>
    </tr>"""

    return f"""<div class="card">
  <table>
    <thead><tr>
      <th>Candidate</th><th>Verdict</th><th>WFO Score</th><th>Ruin Prob</th><th>Flags</th><th>Evidence</th>
    </tr></thead>
    <tbody>{rows}</tbody>
  </table>
</div>"""


def _render_all_candidate_details(verdicts: list, data: dict, scenario: ScenarioProfile) -> str:
    if not verdicts:
        return "<div class='card'>No candidates to display.</div>"

    html_parts = []
    for v in verdicts:
        cid = v.get("candidate_id", "")
        verdict_val = v.get("verdict", "no_go")
        card_cls = f"verdict-{verdict_val}"
        badge_cls = f"badge-{verdict_val}"

        wfo = v.get("wfo_consistency_score")
        ruin = v.get("mc_deep_ruin_probability")
        evidence = v.get("evidence_summary", "No evidence summary available.")

        flags_html = ""
        for flag_name in ["sensitivity_spike", "oos_gate_triggered", "window_collapse_flag", "sensitivity_profile_incomplete"]:
            if v.get(flag_name):
                flags_html += f'<span class="flag">{flag_name}</span> '

        # Scenario-emphasised metrics grid
        metric_cells = _render_scenario_metrics(v, data, cid, scenario)

        # Charts
        chart_html = _render_candidate_charts(cid, data)

        deployment_note = ""
        if verdict_val in (Verdict.AUTO_GO.value, Verdict.BORDERLINE.value):
            deployment_note = f"""<div class="deployment">
  ⚠️ <strong>Deployment Status: PAPER_TRADE_REQUIRED</strong><br>
  This candidate requires paper trading validation before live deployment.
  The operator must manually promote to LIVE_APPROVED after the paper trading period.
</div>"""

        html_parts.append(f"""
<div class="card {card_cls}">
  <h3>
    <span class="badge {badge_cls}">{verdict_val}</span>
    &nbsp;Candidate <code>{cid[:12]}</code>
  </h3>
  {flags_html}
  <div class="metric-grid">
    <div class="metric-cell">
      <div class="metric-label">WFO Consistency</div>
      <div class="metric-value">{f"{wfo:.3f}" if wfo is not None else "—"}</div>
    </div>
    <div class="metric-cell">
      <div class="metric-label">MC Ruin Prob</div>
      <div class="metric-value">{f"{ruin:.3f}" if ruin is not None else "—"}</div>
    </div>
    {metric_cells}
  </div>
  <div class="evidence"><strong>Evidence:</strong> {evidence}</div>
  {deployment_note}
  {chart_html}
</div>""")

    return "\n".join(html_parts)


def _render_scenario_metrics(
    verdict_row: dict, data: dict, candidate_id: str, scenario: ScenarioProfile
) -> str:
    """Render metric cells in scenario report_emphasis order."""
    emphasis_metrics = list(scenario.report_emphasis)
    cells = ""
    for metric_name in emphasis_metrics:
        value = verdict_row.get(metric_name)
        if value is None:
            # Try wfo_map or mc_map
            wfo_row = data["wfo_map"].get(candidate_id, {})
            mc_row = data["mc_map"].get(candidate_id, {})
            value = wfo_row.get(metric_name) or mc_row.get(metric_name)

        label = metric_name.replace("_", " ").title()
        value_str = f"{value:.3f}" if isinstance(value, float) else (str(value) if value is not None else "—")
        cells += f"""<div class="metric-cell">
      <div class="metric-label">{label}</div>
      <div class="metric-value">{value_str}</div>
    </div>"""
    return cells


def _render_candidate_charts(candidate_id: str, data: dict) -> str:
    """Render inline base64 charts for the candidate. Returns HTML string."""
    charts_html = ""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        # WFO window bar chart
        wfo_chart = _make_wfo_bar_chart(candidate_id, data)
        if wfo_chart:
            charts_html += f'<img class="chart-img" src="data:image/png;base64,{wfo_chart}" alt="WFO Window Performance">'

        # Sensitivity delta chart
        sens_chart = _make_sensitivity_chart(candidate_id, data)
        if sens_chart:
            charts_html += f'<img class="chart-img" src="data:image/png;base64,{sens_chart}" alt="Sensitivity Delta">'

    except Exception as exc:
        logger.warning("Chart generation failed for candidate %s: %s", candidate_id[:12], exc)

    return charts_html


def _make_wfo_bar_chart(candidate_id: str, data: dict) -> Optional[str]:
    """Generate WFO window bar chart. Returns base64 PNG string or None."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        store = data.get("_store")
        if store is None:
            return None
        window_results = _safe_query(store, "query_wfo_window_results", candidate_id) or []
        if not window_results:
            return None

        window_ids = [r.get("window_id", "") for r in window_results]
        net_pnls = [r.get("net_pnl") or 0.0 for r in window_results]

        fig, ax = plt.subplots(figsize=(8, 3))
        colors = ["#27ae60" if p >= 0 else "#e74c3c" for p in net_pnls]
        ax.bar(window_ids, net_pnls, color=colors, edgecolor="white", linewidth=0.5)
        ax.axhline(0, color="#333", linewidth=0.8)
        ax.set_title(f"WFO Window Net P&L — {candidate_id[:12]}", fontsize=11)
        ax.set_xlabel("Window")
        ax.set_ylabel("Net P&L")
        plt.tight_layout()

        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=100)
        plt.close(fig)
        buf.seek(0)
        return base64.b64encode(buf.read()).decode("ascii")
    except Exception:
        return None


def _make_sensitivity_chart(candidate_id: str, data: dict) -> Optional[str]:
    """Generate sensitivity delta chart. Returns base64 PNG string or None."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        store = data.get("_store")
        if store is None:
            return None
        sens_results = _safe_query(store, "query_sensitivity_results", candidate_id) or []
        if not sens_results:
            return None

        # Build pivot: param_name → list of deltas (across steps)
        from collections import defaultdict
        param_deltas: dict = defaultdict(list)
        for r in sens_results:
            delta = r.get("fitness_delta")
            if delta is not None:
                param_deltas[r.get("parameter_name", "?")].append(abs(delta))

        if not param_deltas:
            return None

        params = list(param_deltas.keys())
        max_deltas = [max(v) for v in param_deltas.values()]

        fig, ax = plt.subplots(figsize=(8, max(3, len(params) * 0.4 + 1)))
        colors = ["#e74c3c" if d > 0.15 else "#0f3460" for d in max_deltas]
        bars = ax.barh(params, max_deltas, color=colors, edgecolor="white", linewidth=0.5)
        ax.axvline(0.15, color="#f39c12", linewidth=1.2, linestyle="--", label="spike threshold")
        ax.set_title(f"Sensitivity Max |Δ Fitness| — {candidate_id[:12]}", fontsize=11)
        ax.set_xlabel("|Fitness Delta|")
        ax.legend(fontsize=9)
        plt.tight_layout()

        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=100)
        plt.close(fig)
        buf.seek(0)
        return base64.b64encode(buf.read()).decode("ascii")
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Adversarial borderline checklist
# ─────────────────────────────────────────────────────────────────────────────

def _write_borderline_checklists(
    data: dict,
    scenario: ScenarioProfile,
    run_id: str,
    output_dir: Path,
) -> None:
    """Write one adversarial checklist HTML per borderline candidate."""
    borderline = [
        v for v in data["verdicts"]
        if v.get("verdict") == Verdict.BORDERLINE.value
    ]
    if not borderline:
        return

    checklist_dir = output_dir / "checklists"
    checklist_dir.mkdir(exist_ok=True)

    for v in borderline:
        cid = v.get("candidate_id", "")
        checklist_path = checklist_dir / f"checklist_{run_id[:8]}_{cid[:12]}.html"
        _write_single_checklist(v, data, scenario, run_id, checklist_path)
        logger.info("Adversarial checklist written: %s", checklist_path)


def _write_single_checklist(
    verdict_row: dict,
    data: dict,
    scenario: ScenarioProfile,
    run_id: str,
    output_path: Path,
) -> None:
    """Write a single adversarial borderline checklist HTML file."""
    cid = verdict_row.get("candidate_id", "")
    wfo = verdict_row.get("wfo_consistency_score")
    ruin = verdict_row.get("mc_deep_ruin_probability")
    evidence = verdict_row.get("evidence_summary", "")
    sens_spike = verdict_row.get("sensitivity_spike")
    profile_incomplete = verdict_row.get("sensitivity_profile_incomplete")

    flags_section = ""
    if sens_spike:
        flags_section += "<li>⚠️ <strong>Sensitivity spike detected</strong> — verify robustness of spiking parameters manually.</li>"
    if profile_incomplete:
        flags_section += "<li>⚠️ <strong>Sensitivity profile incomplete</strong> — &gt;50% perturbation evaluations failed.</li>"
    if verdict_row.get("oos_gate_triggered"):
        flags_section += "<li>⚠️ <strong>IS/OOS gate triggered</strong> — examine forward-test performance degradation.</li>"
    if verdict_row.get("window_collapse_flag"):
        flags_section += "<li>⚠️ <strong>Window collapse detected</strong> — at least one WFO window showed extreme drawdown.</li>"

    checklist_items = [
        ("Market regime", "Does the evaluation period cover representative market conditions (trending + ranging)?"),
        ("Data quality", "Were any data gaps, outliers, or feed errors present during the test period?"),
        ("Overfitting risk", "Is the parameter set in a densely explored region (potential fitness landscape overfitting)?"),
        ("Sensitivity review", "Have all parameters with |Δ fitness| > 0.10 been reviewed for economic rationale?"),
        ("WFO window coverage", "Do the WFO windows cover multiple distinct market periods?"),
        ("MC stress test", "Does the ruin probability remain acceptable under a worse perturbation profile?"),
        ("Execution assumptions", "Are the spread, slippage, and execution delay assumptions realistic for eToro?"),
        ("Risk sizing", "Is the risk_percentile parameter appropriate for the current account balance?"),
        ("Correlated positions", "Would this strategy be traded alongside other correlated strategies?"),
        ("Paper trading plan", "Is there a defined paper trading duration and success criteria before live deployment?"),
    ]

    items_html = ""
    for item_title, item_desc in checklist_items:
        items_html += f"""
    <div style="border:1px solid #ddd; border-radius:6px; padding:12px; margin:8px 0; background:white">
      <label style="display:flex; align-items:flex-start; gap:10px; cursor:pointer">
        <input type="checkbox" style="margin-top:3px; flex-shrink:0">
        <div>
          <strong>{item_title}</strong><br>
          <span style="font-size:13px; color:#555">{item_desc}</span>
          <br><textarea style="width:100%; margin-top:6px; border:1px solid #ddd; border-radius:4px;
                         padding:6px; font-size:12px; resize:vertical; min-height:40px"
                    placeholder="Notes…"></textarea>
        </div>
      </label>
    </div>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Adversarial Checklist — {cid[:12]}</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         max-width: 800px; margin: 40px auto; padding: 0 20px; color: #222; }}
  h1 {{ color: #e74c3c; border-bottom: 2px solid #e74c3c; padding-bottom: 8px; }}
  .evidence {{ background: #fff3cd; border: 1px solid #ffc107; border-radius: 6px;
               padding: 14px; font-size: 13px; line-height: 1.6; margin: 16px 0; }}
  .flags {{ background: #fdecea; border: 1px solid #e74c3c; border-radius: 6px;
            padding: 14px; margin: 16px 0; }}
  .sign-off {{ border: 2px solid #333; border-radius: 8px; padding: 20px; margin-top: 32px; }}
  input[type=text], textarea {{ font-family: inherit; }}
</style>
</head>
<body>
<h1>⚠️ Adversarial Borderline Checklist</h1>
<p><strong>Run ID:</strong> <code>{run_id}</code><br>
   <strong>Candidate:</strong> <code>{cid}</code><br>
   <strong>Scenario:</strong> {scenario.name}</p>

<div class="evidence">
  <strong>Pipeline Evidence:</strong><br>{evidence}
  <br><br>
  WFO Consistency Score: <strong>{f"{wfo:.3f}" if wfo is not None else "N/A"}</strong>
  &nbsp;|&nbsp; MC Deep Ruin Probability: <strong>{f"{ruin:.3f}" if ruin is not None else "N/A"}</strong>
</div>

{"<div class='flags'><strong>Active Flags:</strong><ul>" + flags_section + "</ul></div>" if flags_section else ""}

<h2>Review Checklist</h2>
<p style="font-size:13px;color:#666">
  Complete all items before making a deployment decision.
  This checklist must be reviewed by the operator and retained as part of the trade log.
</p>
{items_html}

<div class="sign-off">
  <h3>Operator Sign-Off</h3>
  <p><strong>Decision:</strong>
    <label><input type="radio" name="decision" value="deploy"> Deploy to paper trading</label>
    &nbsp;&nbsp;
    <label><input type="radio" name="decision" value="reject"> Reject</label>
    &nbsp;&nbsp;
    <label><input type="radio" name="decision" value="hold"> Hold for further review</label>
  </p>
  <p><strong>Notes:</strong><br>
    <textarea style="width:100%; min-height:80px; border:1px solid #ccc; border-radius:4px; padding:8px; font-size:13px"
              placeholder="Operator notes…"></textarea>
  </p>
  <p>
    <strong>Operator:</strong>
    <input type="text" style="border:1px solid #ccc; border-radius:4px; padding:4px 8px" placeholder="Name">
    &nbsp;&nbsp;
    <strong>Date:</strong>
    <input type="date" style="border:1px solid #ccc; border-radius:4px; padding:4px 8px">
  </p>
</div>
</body>
</html>"""

    output_path.write_text(html, encoding="utf-8")


# ─────────────────────────────────────────────────────────────────────────────
# Parquet output
# ─────────────────────────────────────────────────────────────────────────────

def _write_parquet_records(data: dict, run_id: str, output_dir: Path) -> None:
    """Write per-candidate Parquet files using pandas."""
    try:
        import pandas as pd

        parquet_dir = output_dir / "parquet"
        parquet_dir.mkdir(exist_ok=True)

        for flat_record in data["flat_records"]:
            candidate_id = flat_record.get("candidate_id", "unknown")
            df = pd.DataFrame([flat_record])
            parquet_path = parquet_dir / f"{run_id[:8]}_{candidate_id[:12]}.parquet"
            df.to_parquet(parquet_path, index=False)

        logger.info(
            "Parquet records written to %s (%d files).",
            parquet_dir, len(data["flat_records"])
        )
    except ImportError:
        logger.warning("pandas not available — Parquet output skipped.")
    except Exception as exc:
        logger.error("Parquet write failed: %s", exc)
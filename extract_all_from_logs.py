#!/usr/bin/env python3
"""
Extract candidate metrics from backtest query logs.
Usage:
    python extract_all_from_logs.py <logs_directory> [--output <dir>] [--mapping <file>]
"""

import os
import re
import sys
import argparse
import csv
from collections import defaultdict

# ----------------------------------------------------------------------
# Mapping from run_id to timeframe (you must provide this)
# If not provided, we'll try to infer from file name, but better to supply.
# You can also pass a CSV file with columns: run_id, timeframe
# ----------------------------------------------------------------------
DEFAULT_MAPPING = {
    # 1-minute runs
    'cd67ceb0': '1min',
    # 5-minute runs
    'b8b6f21a-9c8f-4738-a418-950217463540': '5min',
    # 10-minute runs
    '822f1889-1810-4a22-8393-eaa4792a759e': '10min',
    # 15-minute runs
    '3990fa3c': '15min',
    # Add more if needed
}

def parse_table(lines, start_idx, expected_headers=None):
    """
    Parse a table from a list of lines starting at start_idx.
    Returns (table_rows, next_idx) where table_rows is list of dicts.
    Assumes table is preceded by a header line, then a separator line (---),
    then data rows, then blank line or end of section.
    """
    i = start_idx
    # Find header line (contains column names, maybe multiple words)
    header_line = None
    while i < len(lines) and not re.match(r'[-\s]{3,}', lines[i]):
        # if line looks like it has column headers (alphanumeric with spaces)
        if re.search(r'[a-z]', lines[i].lower()):
            header_line = lines[i]
            break
        i += 1
    if header_line is None:
        return [], start_idx

    # Skip separator line (---)
    i += 1
    # Now parse data rows until we hit a blank line or a new section header
    # (lines starting with uppercase letters and spaces only)
    rows = []
    while i < len(lines):
        line = lines[i].strip()
        if not line:
            break
        # Stop if next section header appears (all caps, no digits)
        if re.match(r'^[A-Z][A-Z\s]+$', line):
            break
        # Split on 2+ spaces to preserve columns
        parts = re.split(r'\s{2,}', line)
        if len(parts) > 1:
            # Map to header columns
            headers = re.split(r'\s{2,}', header_line.strip())
            # headers may have more elements than parts due to missing values
            row_dict = {}
            for j, h in enumerate(headers):
                if j < len(parts):
                    val = parts[j].strip()
                    # Try to convert numeric if possible
                    if val.replace('.', '', 1).isdigit() or (val.startswith('-') and val[1:].replace('.', '', 1).isdigit()):
                        try:
                            val = float(val)
                        except:
                            pass
                    row_dict[h] = val
                else:
                    row_dict[h] = None
            rows.append(row_dict)
        i += 1
    return rows, i

def extract_wfo_table(lines, start_idx):
    """Extract the WFO consistency scores table."""
    # The section header is "STAGE 4 - WFO CONSISTENCY SCORES"
    # Find the line where the table starts
    # It's easier to search for the header line and then call parse_table
    for i in range(start_idx, min(start_idx+20, len(lines))):
        if re.search(r'candidate\s+wfo_score', lines[i], re.I):
            header_line = i
            break
    else:
        return [], start_idx
    rows, next_idx = parse_table(lines, header_line)
    return rows, next_idx

def extract_per_window_top5(lines, start_idx):
    """Extract per-window details for top 5 candidates."""
    # Section header: "STAGE 4 - PER-WINDOW DETAIL (top 5 WFO candidates)"
    # Each candidate block starts with "Candidate <id>  WFO=..."
    # We'll parse block by block
    i = start_idx
    per_window = []
    while i < len(lines):
        # Look for "Candidate <id>  WFO=" pattern
        m = re.search(r'Candidate\s+([a-f0-9]+)\s+WFO=', lines[i])
        if m:
            candidate_id = m.group(1)
            # Now parse the table that follows
            # Find the header line (window_id ...)
            j = i+1
            while j < len(lines) and not re.search(r'window_id\s+ga_win', lines[j], re.I):
                j += 1
            if j < len(lines):
                table, next_idx = parse_table(lines, j)
                for row in table:
                    row['candidate_id'] = candidate_id
                    per_window.append(row)
                i = next_idx
            else:
                i += 1
        else:
            i += 1
    return per_window, len(lines)

def extract_stage1_top10(lines, start_idx):
    """Extract Stage 1 top 10 passed candidates."""
    # Find the table header line
    for i in range(start_idx, min(start_idx+20, len(lines))):
        if re.search(r'candidate\s+zone\s+win_rate', lines[i], re.I):
            header_line = i
            break
    else:
        return [], start_idx
    rows, next_idx = parse_table(lines, header_line)
    return rows, next_idx

def extract_mc_deep(lines, start_idx):
    """Extract MC Deep results."""
    for i in range(start_idx, min(start_idx+20, len(lines))):
        if re.search(r'candidate\s+ruin_prob', lines[i], re.I):
            header_line = i
            break
    else:
        return [], start_idx
    rows, next_idx = parse_table(lines, header_line)
    return rows, next_idx

def extract_sensitivity_summary(lines, start_idx):
    """Extract sensitivity summary (spike_detected)."""
    # The summary table is after "STAGE 6 - SENSITIVITY PROFILES" and before the detailed deltas
    # It has columns: candidate, base_fitness, spike_detected, profile_complete, spike_params
    for i in range(start_idx, min(start_idx+20, len(lines))):
        if re.search(r'candidate\s+base_fitness\s+spike_detected', lines[i], re.I):
            header_line = i
            break
    else:
        return [], start_idx
    rows, next_idx = parse_table(lines, header_line)
    return rows, next_idx

def extract_verdicts(lines, start_idx):
    """Extract final verdicts."""
    for i in range(start_idx, min(start_idx+20, len(lines))):
        if re.search(r'candidate\s+verdict\s+wfo_score', lines[i], re.I):
            header_line = i
            break
    else:
        return [], start_idx
    rows, next_idx = parse_table(lines, header_line)
    return rows, next_idx

def parse_log_file(log_path, run_id=None, timeframe=None):
    """Parse a single log file and return a dict of candidate data."""
    with open(log_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    # Remove trailing newlines
    lines = [l.rstrip() for l in lines]

    # If run_id not provided, try to extract from first lines
    if run_id is None:
        # Look for "run_id  : <id>" pattern
        for line in lines[:20]:
            m = re.search(r'run_id\s*:\s*([a-f0-9\-]+)', line)
            if m:
                run_id = m.group(1)
                break

    # Gather data
    candidate_data = defaultdict(dict)

    # Process each section
    idx = 0
    while idx < len(lines):
        line = lines[idx].upper()
        if 'STAGE 4 - WFO CONSISTENCY SCORES' in line:
            rows, next_idx = extract_wfo_table(lines, idx)
            for row in rows:
                cid = row.get('candidate', '').strip()
                if cid:
                    candidate_data[cid]['wfo_score'] = row.get('wfo_score')
                    candidate_data[cid]['windows_evaluated'] = row.get('windows_evaluated')
                    candidate_data[cid]['median_ret'] = row.get('median_ret')
                    candidate_data[cid]['worst_dd'] = row.get('worst_dd')
                    candidate_data[cid]['frac_pos'] = row.get('frac_pos')
                    candidate_data[cid]['collapsed'] = row.get('collapsed')
            idx = next_idx
        elif 'STAGE 4 - PER-WINDOW DETAIL' in line:
            rows, next_idx = extract_per_window_top5(lines, idx)
            # rows are per-window, we'll store them separately
            # We'll keep them in a separate dict for later
            # For now, we'll store in candidate_data under 'per_window'
            if not candidate_data.get('_per_window'):
                candidate_data['_per_window'] = []
            candidate_data['_per_window'].extend(rows)
            idx = next_idx
        elif 'STAGE 1 - TOP 10 PASSED CANDIDATES' in line:
            rows, next_idx = extract_stage1_top10(lines, idx)
            for row in rows:
                cid = row.get('candidate', '').strip()
                if cid:
                    candidate_data[cid]['stage1_win_rate'] = row.get('win_rate')
                    candidate_data[cid]['stage1_drawdown'] = row.get('drawdown')
                    candidate_data[cid]['stage1_expectancy'] = row.get('expectancy')
                    candidate_data[cid]['stage1_profit_factor'] = row.get('pf')
                    candidate_data[cid]['stage1_losing_streak'] = row.get('losing_streak')
                    candidate_data[cid]['stage1_trades_per_week'] = row.get('tpw')
            idx = next_idx
        elif 'STAGE 5 - MC DEEP RESULTS' in line:
            rows, next_idx = extract_mc_deep(lines, idx)
            for row in rows:
                cid = row.get('candidate', '').strip()
                if cid:
                    candidate_data[cid]['ruin_prob'] = row.get('ruin_prob')
            idx = next_idx
        elif 'STAGE 6 - SENSITIVITY PROFILES' in line:
            rows, next_idx = extract_sensitivity_summary(lines, idx)
            for row in rows:
                cid = row.get('candidate', '').strip()
                if cid:
                    candidate_data[cid]['spike_detected'] = row.get('spike_detected')
            idx = next_idx
        elif 'STAGE 7 - FINAL VERDICTS' in line:
            rows, next_idx = extract_verdicts(lines, idx)
            for row in rows:
                cid = row.get('candidate', '').strip()
                if cid:
                    candidate_data[cid]['verdict'] = row.get('verdict')
            idx = next_idx
        else:
            idx += 1

    # Post-process per-window data: aggregate totals for candidates that appear
    per_window_data = candidate_data.pop('_per_window', [])
    # Aggregate per candidate
    per_window_agg = defaultdict(list)
    for row in per_window_data:
        cid = row.get('candidate_id')
        if cid:
            per_window_agg[cid].append(row)

    for cid, rows in per_window_agg.items():
        # Compute total net P&L (sum of net_pnl)
        total_pnl = sum(float(r.get('net_pnl', 0)) for r in rows if r.get('net_pnl') is not None)
        candidate_data[cid]['total_net_pnl'] = total_pnl
        # Average expectancy per trade across windows (weighted by trades)
        total_expectancy = 0
        total_trades = 0
        for r in rows:
            exp = r.get('expectancy')
            trades = r.get('total_trades')
            if exp is not None and trades is not None:
                total_expectancy += float(exp) * float(trades)
                total_trades += float(trades)
        avg_exp = total_expectancy / total_trades if total_trades else None
        candidate_data[cid]['avg_expectancy'] = avg_exp
        # Max drawdown among windows
        max_dd = max(float(r.get('drawdown', 0)) for r in rows if r.get('drawdown') is not None)
        candidate_data[cid]['max_drawdown_window'] = max_dd

    # Add run_id and timeframe to each candidate
    for cid in candidate_data:
        candidate_data[cid]['run_id'] = run_id
        candidate_data[cid]['timeframe'] = timeframe

    # Return candidate data and per-window data
    return candidate_data, per_window_data

def main():
    parser = argparse.ArgumentParser(description='Extract candidate metrics from backtest logs.')
    parser.add_argument('logs_dir', help='Directory containing query log files')
    parser.add_argument('--output', '-o', default='outputs/metrics', help='Output directory for CSV files')
    parser.add_argument('--mapping', '-m', help='CSV file with run_id,timeframe mapping')
    args = parser.parse_args()

    # Load mapping if provided
    runid_to_tf = {}
    if args.mapping and os.path.exists(args.mapping):
        with open(args.mapping, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            for row in reader:
                if len(row) >= 2:
                    runid_to_tf[row[0].strip()] = row[1].strip()
    else:
        runid_to_tf = DEFAULT_MAPPING

    # Gather all log files
    log_files = []
    for root, dirs, files in os.walk(args.logs_dir):
        for f in files:
            if f.endswith('.log'):
                log_files.append(os.path.join(root, f))

    all_candidates = {}
    all_per_window = []

    for log_file in log_files:
        # Try to get run_id from file name or content
        run_id = None
        # Check if run_id is in filename (maybe it contains the uuid)
        base = os.path.basename(log_file)
        # Look for 8-4-4-4-12 pattern
        m = re.search(r'([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})', base)
        if m:
            run_id = m.group(1)
        else:
            # We'll parse the log to find run_id
            pass
        timeframe = runid_to_tf.get(run_id, 'unknown')
        candidates, per_window = parse_log_file(log_file, run_id, timeframe)
        for cid, data in candidates.items():
            if cid not in all_candidates:
                all_candidates[cid] = data
            else:
                # Merge (shouldn't have duplicates across runs)
                all_candidates[cid].update(data)
        all_per_window.extend(per_window)

    # Write candidate metrics CSV
    os.makedirs(args.output, exist_ok=True)
    candidate_csv = os.path.join(args.output, 'candidates_metrics.csv')
    fieldnames = [
        'candidate_id', 'timeframe', 'run_id',
        'wfo_score', 'windows_evaluated', 'median_ret', 'worst_dd', 'frac_pos', 'collapsed',
        'ruin_prob', 'spike_detected', 'verdict',
        'stage1_win_rate', 'stage1_drawdown', 'stage1_expectancy', 'stage1_profit_factor',
        'stage1_losing_streak', 'stage1_trades_per_week',
        'total_net_pnl', 'avg_expectancy', 'max_drawdown_window'
    ]
    with open(candidate_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for cid, data in all_candidates.items():
            row = {'candidate_id': cid}
            for fn in fieldnames[1:]:
                row[fn] = data.get(fn)
            writer.writerow(row)

    # Write per-window CSV
    if all_per_window:
        per_window_csv = os.path.join(args.output, 'per_window_metrics.csv')
        # Get all possible column names from all rows
        all_keys = set()
        for row in all_per_window:
            all_keys.update(row.keys())
        # Ensure candidate_id is first
        fieldnames = ['candidate_id'] + sorted([k for k in all_keys if k != 'candidate_id'])
        with open(per_window_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in all_per_window:
                writer.writerow(row)

    print(f"Extracted data for {len(all_candidates)} candidates.")
    print(f"Candidate metrics written to: {candidate_csv}")
    if all_per_window:
        print(f"Per-window metrics written to: {per_window_csv}")

if __name__ == '__main__':
    main()
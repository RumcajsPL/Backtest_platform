#!/usr/bin/env python3
"""
Extract candidate metrics from JSON files (one per candidate stage).
Usage:
    python extract_from_json.py --input outputs/backtesting/json --output outputs/metrics
"""

import os
import json
import argparse
import csv
from collections import defaultdict

# Mapping from run_id to timeframe (from BACKTESTING_TRACKER.md)
RUN_TO_TIMEFRAME = {
    # 1-minute series
    'cd67ceb0': '1min',
    # 5-minute series
    'b8b6f21a-9c8f-4738-a418-950217463540': '5min',
    # 10-minute series
    '822f1889-1810-4a22-8393-eaa4792a759e': '10min',
    # 15-minute series
    '3990fa3c': '15min',
    # Add any other runs you need
}

def parse_json_file(filepath):
    """Parse a single JSON file and return a dict with candidate_id, run_id, stage, and metrics."""
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    # Extract basic identifiers
    candidate_id = data.get('candidate_id')
    run_id = data.get('run_id')
    stage = data.get('stage')
    if not candidate_id or not run_id:
        return None

    # Build a flat dict of all fields that are not None (except large nested ones)
    metrics = {}
    # All fields except possibly large nested ones; we'll keep everything that is not None
    for key, value in data.items():
        if value is not None and key not in ['parameters_json', 'evidence_summary']:
            metrics[key] = value
    # Also include parameters if needed? We might not need them for tables, but could be useful.
    # We'll keep them separate for now.

    return {
        'candidate_id': candidate_id,
        'run_id': run_id,
        'stage': stage,
        'metrics': metrics,
        'parameters': data.get('parameters_json')
    }

def main():
    parser = argparse.ArgumentParser(description='Extract candidate metrics from JSON files.')
    parser.add_argument('--input', '-i', default='outputs/backtesting/json',
                        help='Directory containing JSON files (default: outputs/backtesting/json)')
    parser.add_argument('--output', '-o', default='outputs/metrics',
                        help='Output directory for CSV file (default: outputs/metrics)')
    args = parser.parse_args()

    # Build a dict keyed by (run_id, candidate_id) -> aggregated metrics
    candidates = defaultdict(dict)

    # Walk through all JSON files in input directory
    for root, dirs, files in os.walk(args.input):
        for file in files:
            if not file.endswith('.json'):
                continue
            filepath = os.path.join(root, file)
            parsed = parse_json_file(filepath)
            if parsed is None:
                continue
            key = (parsed['run_id'], parsed['candidate_id'])
            stage = parsed['stage']
            metrics = parsed['metrics']
            # Merge metrics into the candidate's record
            # We'll keep the most recent value for each field (overwriting)
            candidates[key].update(metrics)
            # Also store parameters separately if needed
            if 'parameters' not in candidates[key] and parsed['parameters']:
                candidates[key]['parameters'] = parsed['parameters']
            # Store run_id and candidate_id
            candidates[key]['run_id'] = parsed['run_id']
            candidates[key]['candidate_id'] = parsed['candidate_id']

    # Now build a list of candidate records for CSV
    records = []
    for (run_id, candidate_id), data in candidates.items():
        # Add timeframe
        timeframe = RUN_TO_TIMEFRAME.get(run_id, 'unknown')
        # Create a flat record with all fields we want
        record = {
            'candidate_id': candidate_id,
            'run_id': run_id,
            'timeframe': timeframe,
            'verdict': data.get('verdict'),
            'wfo_score': data.get('wfo_consistency_score'),
            'windows_evaluated': data.get('wfo_windows_evaluated'),
            'median_return': data.get('wfo_median_window_return'),
            'worst_drawdown': data.get('wfo_worst_window_drawdown'),
            'frac_pos': data.get('wfo_fraction_positive_windows'),
            'collapsed': data.get('wfo_window_collapse_flag'),
            'ruin_prob': data.get('mc_deep_ruin_probability'),
            'spike_detected': data.get('sensitivity_spike_detected'),
            'stage1_win_rate': data.get('actual_win_rate'),
            'stage1_drawdown': data.get('actual_max_drawdown'),
            'stage1_expectancy': data.get('actual_expectancy'),
            'stage1_profit_factor': data.get('actual_profit_factor'),
            'stage1_losing_streak': data.get('actual_losing_streak'),
            'stage1_trades_per_week': data.get('actual_trades_per_week'),
            'parameters': data.get('parameters'),
        }
        records.append(record)

    # Write to CSV
    os.makedirs(args.output, exist_ok=True)
    output_file = os.path.join(args.output, 'candidates_metrics_from_json.csv')
    fieldnames = [
        'candidate_id', 'run_id', 'timeframe',
        'verdict', 'wfo_score', 'windows_evaluated', 'median_return',
        'worst_drawdown', 'frac_pos', 'collapsed', 'ruin_prob', 'spike_detected',
        'stage1_win_rate', 'stage1_drawdown', 'stage1_expectancy',
        'stage1_profit_factor', 'stage1_losing_streak', 'stage1_trades_per_week',
        'parameters'
    ]
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            writer.writerow(rec)

    print(f"Extracted {len(records)} candidate records.")
    print(f"Output written to: {output_file}")

if __name__ == '__main__':
    main()
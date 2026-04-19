import csv
import re
from datetime import datetime, timezone
from pathlib import Path
from loguru import logger

LOG_DIR = Path("outputs/broker_support/logs")
OUTPUT_FILE = Path("outputs/broker_support/diagnostics/live_signals_extracted.csv")

# Patterns based on verified log formats
POLL_HEADER_PATTERN = re.compile(r"── \[(?P<instance>[\w]+)\] Poll #(?P<number>\d+) at (?P<time>\d{2}:\d{2}:\d{2}) UTC ──")
# SignalBridge lines are extracted via pipe split, but these regexes handle the "message" part
NO_SIGNAL_PATTERN = re.compile(r"SignalBridge: no signal on last bar \((?P<timestamp>[^)]+)\)")
SIGNAL_FOUND_PATTERN = re.compile(r"SignalBridge: signal found at last bar — (?P<direction>BUY|SELL) @ (?P<timestamp>[^,]+), bid=(?P<bid>[^ ]+)")
TRADE_PARAMS_PATTERN = re.compile(
    r"SignalBridge: trade params — entry=(?P<entry>[^,]+), sl=[^ ]+ \(dist=(?P<sl_dist>[^pts]+)pts\), tp=[^ ]+ \(dist=(?P<tp_dist>[^pts]+)pts\), rr=(?P<rr>[^x]+)x, atr=(?P<atr>[^ ]+)"
)
RISK_REJECT_PATTERN = re.compile(r"SignalBridge: RiskManager rejected trade at (?P<timestamp>[^.]+)\. Risk summary: (?P<reason>.*)")
RESULT_PATTERN = re.compile(r"SignalBridge: result=(?P<result>\w+)")
SUMMARY_PATTERN = re.compile(r"SignalBridge: OrderSignal \| (?P<direction>BUY|SELL) ")

def parse_log_line(line):
    """Splits loguru format: timestamp | level | module:line | message"""
    parts = line.split(" | ", 3)
    if len(parts) < 4:
        return None, None
    return parts[0].strip(), parts[3].strip()

def process_logs():
    if not LOG_DIR.exists():
        logger.error(f"Log directory {LOG_DIR} not found.")
        return

    log_files = list(LOG_DIR.glob("demo_trading_*.log"))
    if not log_files:
        logger.warning("No matching log files found in logs directory.")
        return

    all_extracted_rows = []
    stats = {
        "total_polls": 0,
        "stage_5_polls": 0,
        "results": {"NO_SIGNAL": 0, "RISK_REJECTED": 0, "SIGNAL": 0},
        "dates": set(),
        "instances": set()
    }

    for log_file in log_files:
        # Extraction of instance and log_date from filename: demo_trading_240166_2026-04-01.log
        try:
            parts = log_file.stem.split("_")
            filename_instance = parts[2]
            log_date = parts[3]
        except IndexError:
            logger.warning(f"Unexpected filename format: {log_file}")
            continue

        stats["instances"].add(filename_instance)
        stats["dates"].add(log_date)

        current_block = None
        
        try:
            with open(log_file, "r", encoding="utf-8", errors="replace") as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue

                    timestamp, message = parse_log_line(line)
                    
                    # Check for Poll Header (found in __main__:741 or similar)
                    # Note: Poll header often doesn't follow the pipe format strictly or is split differently
                    # We check the raw line for the header pattern first
                    header_match = POLL_HEADER_PATTERN.search(line)
                    if header_match:
                        if current_block and current_block.get("bar_timestamp"):
                            all_extracted_rows.append(finalize_row(current_block, log_date))
                            stats["stage_5_polls"] += 1
                            update_result_stats(stats, current_block["result"])
                        
                        if current_block:
                            stats["total_polls"] += 1

                        current_block = {
                            "date": log_date,
                            "instance": header_match.group("instance"),
                            "poll_number": header_match.group("number"),
                            "poll_timestamp": header_match.group("time"),
                            "bar_timestamp": None,
                            "result": None,
                            "direction": None,
                            "bid_price": None,
                            "entry_price_mid": None,
                            "sl_distance": None,
                            "tp_distance": None,
                            "atr_value": None,
                            "risk_reward_ratio": None,
                            "rejection_reason": None,
                        }
                        continue

                    if not current_block or not message:
                        continue

                    # Only process SignalBridge lines
                    if "SignalBridge:" not in message:
                        continue

                    # 1. No Signal / Bar Timestamp
                    ns_match = NO_SIGNAL_PATTERN.search(message)
                    if ns_match:
                        current_block["bar_timestamp"] = ns_match.group("timestamp")
                        current_block["result"] = "NO_SIGNAL"

                    # 2. Signal Found
                    sf_match = SIGNAL_FOUND_PATTERN.search(message)
                    if sf_match:
                        current_block["bar_timestamp"] = sf_match.group("timestamp")
                        current_block["direction"] = sf_match.group("direction")
                        current_block["bid_price"] = sf_match.group("bid")
                        current_block["result"] = "SIGNAL"

                    # 3. Trade Params
                    tp_match = TRADE_PARAMS_PATTERN.search(message)
                    if tp_match:
                        current_block["entry_price_mid"] = tp_match.group("entry")
                        current_block["sl_distance"] = tp_match.group("sl_dist")
                        current_block["tp_distance"] = tp_match.group("tp_dist")
                        current_block["risk_reward_ratio"] = tp_match.group("rr")
                        current_block["atr_value"] = tp_match.group("atr")

                    # 4. Risk Rejection
                    rr_match = RISK_REJECT_PATTERN.search(message)
                    if rr_match:
                        current_block["result"] = "RISK_REJECTED"
                        current_block["rejection_reason"] = rr_match.group("reason")
                        if not current_block["bar_timestamp"]:
                            current_block["bar_timestamp"] = rr_match.group("timestamp")

                    # 5. Direct Result
                    res_match = RESULT_PATTERN.search(message)
                    if res_match:
                        current_block["result"] = res_match.group("result")
                    
                    # 6. Order Summary (confirms SIGNAL)
                    if SUMMARY_PATTERN.search(message):
                        current_block["result"] = "SIGNAL"

        except Exception as e:
            logger.warning(f"Error reading {log_file} at line {line_num}: {e}")

        # Handle the last block of the file
        if current_block and current_block.get("bar_timestamp"):
            all_extracted_rows.append(finalize_row(current_block, log_date))
            stats["stage_5_polls"] += 1
            update_result_stats(stats, current_block["result"])
        if current_block:
            stats["total_polls"] += 1

    # Write CSV
    headers = [
        "date", "instance", "poll_number", "poll_timestamp", "bar_timestamp", 
        "result", "direction", "bid_price", "entry_price_mid", "sl_distance", 
        "tp_distance", "atr_value", "risk_reward_ratio", "rejection_reason"
    ]
    
    try:
        OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(all_extracted_rows)
    except Exception as e:
        logger.error(f"Failed to write CSV: {e}")
        return

    # Log Summary
    sorted_dates = sorted(list(stats["dates"]))
    date_range = f"{sorted_dates[0]} to {sorted_dates[-1]}" if sorted_dates else "N/A"
    
    logger.info("--- Signal Extraction Summary ---")
    logger.info(f"Total Polls Parsed: {stats['total_polls']}")
    logger.info(f"Stage-5 Polls: {stats['stage_5_polls']}")
    logger.info(f"NO_SIGNAL: {stats['results']['NO_SIGNAL']}")
    logger.info(f"RISK_REJECTED: {stats['results']['RISK_REJECTED']}")
    logger.info(f"SIGNAL: {stats['results']['SIGNAL']}")
    logger.info(f"Date Range: {date_range}")
    logger.info(f"Instances Processed: {len(stats['instances'])}")
    logger.info(f"CSV saved to: {OUTPUT_FILE}")

def finalize_row(block, log_date):
    """Ensures all fields exist for CSV writer"""
    row = {k: (v if v is not None else "") for k, v in block.items()}
    return row

def update_result_stats(stats, result):
    if result in stats["results"]:
        stats["results"][result] += 1

if __name__ == "__main__":
    process_logs()

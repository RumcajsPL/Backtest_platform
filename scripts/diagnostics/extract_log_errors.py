# Save this as: scripts\diagnostics\extract_log_errors.py
# Then run it from scripts\diagnostics\ with: python extract_log_errors.py

from pathlib import Path
import re

def main():
    # Resolve paths correctly when running from scripts\diagnostics\
    # __file__ -> diagnostics\ -> scripts\ -> project_root
    diagnostics_dir = Path(__file__).parent
    project_root = diagnostics_dir.parent.parent
    
    logs_dir = project_root / "outputs" / "broker_support" / "logs"
    
    # Exact list of logs you provided (hard-coded so we only scan what you care about)
    log_filenames = [
        "run_signal_loop_7ffbc5_2026-03-24.log",
        "run_signal_loop_7ffbc5_2026-03-25.log",
        "run_signal_loop_7ffbc5_2026-03-26.log",
        "run_signal_loop_7ffbc5_2026-03-27.log",
        "run_signal_loop_61875_2026-03-24.log",
        "run_signal_loop_61875_2026-03-25.log",
        "run_signal_loop_61875_2026-03-26.log",
        "run_signal_loop_61875_2026-03-27.log",
        "run_signal_loop_240166_2026-03-24.log",
        "run_signal_loop_240166_2026-03-25.log",
        "run_signal_loop_240166_2026-03-26.log",
        "run_signal_loop_240166_2026-03-27.log",
        "run_signal_loop_c424_2026-03-24.log",
        "run_signal_loop_c424_2026-03-25.log",
        "run_signal_loop_c424_2026-03-26.log",
        "run_signal_loop_c424_2026-03-27.log",
    ]
    
    # Regex to match the exact ERROR level (handles the spacing you showed: " | ERROR    | ")
    error_pattern = re.compile(r'\|\s*ERROR\s*\|')
    
    found_any = False
    
    for filename in log_filenames:
        log_path = logs_dir / filename
        
        if not log_path.exists():
            print(f"⚠️  File not found (skipped): {filename}")
            continue
        
        errors = []
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                for line in f:
                    if error_pattern.search(line):
                        errors.append(line.rstrip())   # keep original line but remove trailing newline
        except Exception as e:
            print(f"❌ Error reading {filename}: {e}")
            continue
        
        if errors:
            found_any = True
            # Show exactly the path style you used in your request
            relative_path = log_path.relative_to(project_root)
            print(f"\n{'='*100}")
            print(f"LOG FILE: {relative_path}")
            print(f"{'='*100}")
            for err_line in errors:
                print(err_line)
            print()  # extra blank line between log groups
    
    if not found_any:
        print("✅ No ERROR lines found in any of the specified log files.")
    else:
        print(f"✅ Extraction complete. All ERROR lines grouped by log file above.")

if __name__ == "__main__":
    main()
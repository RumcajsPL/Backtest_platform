#!/usr/bin/env python
"""
Quick portfolio inspection — shows raw field names from live API response.

Usage:
    python scripts/broker_support/inspect_portfolio.py
    python scripts/broker_support/inspect_portfolio.py --instance 240166
    python scripts/broker_support/inspect_portfolio.py --all-positions

The --instance flag loads the matching open_positions.json and annotates
which portfolio positions are CTP-placed vs external.

The --all-positions flag prints all open positions (default: first only).
"""
import argparse
import json
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.broker_support.client.client import EToroClient

JOURNAL_BASE_DIR = "outputs/broker_support/journal"


def _load_ctp_ids(instance_id: str | None) -> set[int]:
    """Load CTP-placed positionIDs from open_positions.json for this instance."""
    base = _PROJECT_ROOT / JOURNAL_BASE_DIR
    if instance_id:
        path = base / instance_id / "open_positions.json"
    else:
        path = base / "open_positions.json"

    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return {int(pid) for pid in data.get("position_ids", [])}
    except Exception as exc:
        print(f"Warning: could not read {path}: {exc}")
        return set()


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect live demo portfolio.")
    parser.add_argument(
        "--instance", "-i",
        default=None,
        help="Instance ID (e.g. c424, 240166). Annotates CTP vs external positions.",
    )
    parser.add_argument(
        "--all-positions",
        action="store_true",
        help="Print all open positions (default: first position only).",
    )
    args = parser.parse_args()

    ctp_ids = _load_ctp_ids(args.instance)
    if ctp_ids:
        print(f"\nCTP open positionIDs (from open_positions.json): {sorted(ctp_ids)}")
    elif args.instance:
        print(f"\nNo open_positions.json found for instance '{args.instance}'.")

    client = EToroClient()
    # get_portfolio() returns the unwrapped clientPortfolio sub-dict
    portfolio = client.get_portfolio()

    credit = portfolio.get("credit")
    positions = portfolio.get("positions", [])
    orders = portfolio.get("ordersForOpen", [])

    print(f"\nCredit:     {credit}")
    print(f"Positions:  {len(positions)}")
    print(f"Orders:     {len(orders)}")

    if not positions:
        print("\nNo open positions currently.")
        print("(Open a demo trade on eToro and re-run to see position fields.)")
        return

    show_positions = positions if args.all_positions else positions[:1]

    for idx, pos in enumerate(show_positions):
        pos_id = pos.get("positionID")
        order_id = pos.get("orderID")
        is_ctp = int(pos_id) in ctp_ids if pos_id is not None else False
        tag = "  ← CTP" if is_ctp else "  ← external"

        print(f"\n--- Position {idx + 1}/{len(positions)}{tag} ---")

        # Key fields first for quick diagnosis
        print(f"  positionID   : {pos_id}")
        print(f"  orderID      : {order_id}  ← use for pending-order reconciliation")
        print(f"  instrumentID : {pos.get('instrumentID')}")
        print(f"  isBuy        : {pos.get('isBuy')}")
        print(f"  openRate     : {pos.get('openRate')}")
        print(f"  openDateTime : {pos.get('openDateTime')}")
        print(f"  stopLossRate : {pos.get('stopLossRate')} (isNoStopLoss={pos.get('isNoStopLoss')})")
        print(f"  takeProfitRate: {pos.get('takeProfitRate')} (isNoTakeProfit={pos.get('isNoTakeProfit')})")

        if args.all_positions or idx == 0:
            print("\n  --- Full raw fields ---")
            print(json.dumps(pos, indent=4, default=str))

    if not args.all_positions and len(positions) > 1:
        print(f"\n(Use --all-positions to show all {len(positions)} positions)")

if __name__ == "__main__":
    main()
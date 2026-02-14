"""
Quick Smoke Test for TradeSimulator v4.5
Tests basic contract integration without full data pipeline
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

# Add project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

print("=" * 70)
print("TRADE SIMULATOR V4.5 - SMOKE TEST")
print("=" * 70)

# Test 1: Import contracts
print("\nTest 1: Import Contracts")
print("-" * 70)
try:
    from src.strategies.contracts.trade_contracts import (
        Trade,
        TradeEntry,
        TradeExit,
        TradeDirection,
        ExitReason,
        TradeParameters,
    )
    print("✅ All contracts imported successfully")
except ImportError as e:
    print(f"❌ Import failed: {e}")
    sys.exit(1)

# Test 2: Create TradeParameters
print("\nTest 2: Create TradeParameters")
print("-" * 70)
try:
    params = TradeParameters(
        entry_price_mid=100.0,
        entry_price_executed=100.05,
        stop_loss_raw=99.0,
        stop_loss_trigger=99.0,
        take_profit=102.0,
        position_size=1.0,
        sl_distance=1.05,
        tp_distance=1.95,
        risk_reward_ratio=1.86,
        comment="Test trade",
    )
    print(f"✅ TradeParameters created: {params.entry_price_executed}")
    print(f"   SL: {params.stop_loss_trigger}, TP: {params.take_profit}")
except Exception as e:
    print(f"❌ TradeParameters creation failed: {e}")
    sys.exit(1)

# Test 3: Create TradeEntry from TradeParameters
print("\nTest 3: Create TradeEntry from TradeParameters")
print("-" * 70)
try:
    entry = TradeEntry.from_trade_parameters(
        entry_id="E1",
        timestamp=pd.Timestamp.now(),
        direction=TradeDirection.LONG,
        params=params,
        trade_manager_id=1,
        signal_id=10,
    )
    print(f"✅ TradeEntry created: {entry.entry_id}")
    print(f"   Direction: {entry.direction.to_string()}")
    print(f"   Entry: {entry.entry_price}")
    print(f"   is_long: {entry.is_long}")
except Exception as e:
    print(f"❌ TradeEntry creation failed: {e}")
    sys.exit(1)

# Test 4: Create Trade (open)
print("\nTest 4: Create Open Trade")
print("-" * 70)
try:
    trade = Trade(entry=entry, exit=None)
    print(f"✅ Trade created: {trade.trade_id}")
    print(f"   Status: {trade.status}")
    print(f"   is_open: {trade.is_open}")
    print(f"   is_closed: {trade.is_closed}")
except Exception as e:
    print(f"❌ Trade creation failed: {e}")
    sys.exit(1)

# Test 5: Create TradeExit
print("\nTest 5: Create TradeExit")
print("-" * 70)
try:
    exit_time = entry.entry_time + timedelta(minutes=30)
    exit_price = 101.5
    
    trade_exit = TradeExit.create(
        entry=entry,
        exit_time=exit_time,
        exit_price=exit_price,
        exit_reason=ExitReason.TAKE_PROFIT,
    )
    print(f"✅ TradeExit created: {trade_exit.exit_id}")
    print(f"   Exit price: {trade_exit.exit_price}")
    print(f"   P&L: {trade_exit.pnl_points:.2f} points")
    print(f"   P&L %: {trade_exit.pnl_percent:.2f}%")
    print(f"   is_win: {trade_exit.is_win}")
    print(f"   Duration: {trade_exit.duration_minutes:.1f} min")
except Exception as e:
    print(f"❌ TradeExit creation failed: {e}")
    sys.exit(1)

# Test 6: Create closed Trade
print("\nTest 6: Create Closed Trade")
print("-" * 70)
try:
    closed_trade = Trade(entry=entry, exit=trade_exit)
    print(f"✅ Closed Trade created: {closed_trade.trade_id}")
    print(f"   Status: {closed_trade.status}")
    print(f"   is_open: {closed_trade.is_open}")
    print(f"   is_closed: {closed_trade.is_closed}")
    print(f"   P&L: {closed_trade.pnl_points:.2f} points")
except Exception as e:
    print(f"❌ Closed Trade creation failed: {e}")
    sys.exit(1)

# Test 7: Convert to dict (legacy format)
print("\nTest 7: Convert to Dict (Legacy Format)")
print("-" * 70)
try:
    trade_dict = closed_trade.to_dict()
    
    # Extract numeric ID
    numeric_id = int(closed_trade.entry.entry_id.replace("E", ""))
    trade_dict["trade_id"] = numeric_id
    
    print(f"✅ Trade converted to dict")
    print(f"   Keys: {list(trade_dict.keys())}")
    
    # Verify required fields
    required_fields = [
        "trade_id", "status", "entry_time", "direction",
        "entry_price", "sl_price", "tp_price",
        "exit_time", "exit_price", "exit_reason",
        "pnl_points", "pnl_percent", "is_win", "is_loss"
    ]
    
    missing = [f for f in required_fields if f not in trade_dict]
    if missing:
        print(f"❌ Missing fields: {missing}")
    else:
        print(f"✅ All required fields present")
        
    # Check types
    assert isinstance(trade_dict["trade_id"], int), "trade_id must be int"
    assert isinstance(trade_dict["direction"], str), "direction must be str"
    assert trade_dict["direction"] in ["BUY", "SELL"], "direction must be BUY/SELL"
    assert isinstance(trade_dict["pnl_points"], float), "pnl_points must be float"
    
    print(f"   trade_id type: {type(trade_dict['trade_id'])}")
    print(f"   direction: {trade_dict['direction']}")
    print(f"   status: {trade_dict['status']}")
    print(f"✅ All type checks passed")
    
except Exception as e:
    print(f"❌ Dict conversion failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 8: Test TradeDirection enum
print("\nTest 8: Test TradeDirection Enum")
print("-" * 70)
try:
    long_dir = TradeDirection.from_string("BUY")
    short_dir = TradeDirection.from_string("SELL")
    
    assert long_dir.to_string() == "BUY"
    assert short_dir.to_string() == "SELL"
    assert long_dir.is_long
    assert short_dir.is_short
    
    print(f"✅ TradeDirection enum works correctly")
    print(f"   BUY -> {long_dir.to_string()}")
    print(f"   SELL -> {short_dir.to_string()}")
except Exception as e:
    print(f"❌ TradeDirection test failed: {e}")
    sys.exit(1)

# Test 9: Test ExitReason enum
print("\nTest 9: Test ExitReason Enum")
print("-" * 70)
try:
    reasons = [
        ExitReason.STOP_LOSS,
        ExitReason.TAKE_PROFIT,
        ExitReason.OPPOSITE_SIGNAL,
        ExitReason.END_OF_DATA,
    ]
    
    for reason in reasons:
        assert reason.to_string() == reason.name
        print(f"   {reason.name} -> {reason.to_string()}")
    
    print(f"✅ ExitReason enum works correctly")
except Exception as e:
    print(f"❌ ExitReason test failed: {e}")
    sys.exit(1)

print("\n" + "=" * 70)
print("SMOKE TEST RESULTS")
print("=" * 70)
print("✅ ALL TESTS PASSED")
print("\nContract integration verified:")
print("  ✅ TradeParameters creation")
print("  ✅ TradeEntry from TradeParameters")
print("  ✅ TradeExit with auto P&L calculation")
print("  ✅ Trade (open and closed)")
print("  ✅ to_dict() with legacy format")
print("  ✅ TradeDirection enum")
print("  ✅ ExitReason enum")
print("\nReady to proceed with full test suite!")
print("=" * 70)
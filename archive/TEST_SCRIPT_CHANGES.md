================================================================================
STEP 3: TEST SUITE MIGRATION TO TradeResult CONTRACTS
================================================================================

FILE: tests/migration/test_trade_simulator.py

================================================================================
PATTERN 1: Update Trade Count Assertions
================================================================================

TEST: test_legacy_vs_new_trade_count_parity

OLD (v4.5.1 - dict access):
----------------------------
result_new = new.simulate_trades(...)

# NEW: Separate tracks for trades and rejects
new_total_signals = len(result_new["all_trades"]) + len(result_new.get("rejected_trades", []))

print(f"  New:    {new_total_signals} total signals")
print(f"    - Actual trades: {len(result_new['all_trades'])}")
print(f"    - Rejected signals: {len(result_new.get('rejected_trades', []))}")


NEW (v4.6 - contract access):
------------------------------
result_new = new.simulate_trades(...)  # Returns TradeResult

# Access properties directly
new_total_signals = len(result_new.trades) + len(result_new.rejected_signals)

print(f"  New:    {new_total_signals} total signals")
print(f"    - Actual trades: {len(result_new.trades)}")
print(f"    - Rejected signals: {len(result_new.rejected_signals)}")


================================================================================
PATTERN 2: Update Exit Stats Access
================================================================================

TEST: test_legacy_vs_new_metrics_parity

OLD (v4.5.1 - dict access):
----------------------------
for reason in ["STOP_LOSS", "TAKE_PROFIT", "OPPOSITE_SIGNAL", "END_OF_DATA"]:
    legacy_count = result_legacy["exit_stats"].get(reason, 0)
    new_count = result_new["exit_stats"].get(reason, 0)
    assert legacy_count == new_count


NEW (v4.6 - property access):
------------------------------
for reason in ["STOP_LOSS", "TAKE_PROFIT", "OPPOSITE_SIGNAL", "END_OF_DATA"]:
    legacy_count = result_legacy["exit_stats"].get(reason, 0)
    new_count = result_new.exits_by_reason.get(reason, 0)  # ← Changed
    assert legacy_count == new_count


================================================================================
PATTERN 3: Update Risk Stats Access
================================================================================

TEST: test_legacy_vs_new_metrics_parity

OLD (v4.5.1 - dict access):
----------------------------
legacy_approved = result_legacy["risk_stats"].get("total_approved", 0)
new_approved = result_new["risk_stats"].get("total_approved", 0)
legacy_rejected = result_legacy["risk_stats"].get("total_rejected", 0)
new_rejected = result_new["risk_stats"].get("total_rejected", 0)


NEW (v4.6 - property access):
------------------------------
legacy_approved = result_legacy["risk_stats"].get("total_approved", 0)
new_approved = result_new.risk_approved  # ← Direct property
legacy_rejected = result_legacy["risk_stats"].get("total_rejected", 0)
new_rejected = result_new.risk_rejected  # ← Direct property


================================================================================
PATTERN 4: Update Position Rejection Access
================================================================================

TEST: test_legacy_vs_new_metrics_parity

OLD (v4.5.1 - dict access):
----------------------------
new_position_rejects = (
    result_new.get("position_rejected_count", {}).get("buy", 0) +
    result_new.get("position_rejected_count", {}).get("sell", 0)
)


NEW (v4.6 - property access):
------------------------------
new_position_rejects = (
    result_new.position_rejected.get("buy", 0) +
    result_new.position_rejected.get("sell", 0)
)


================================================================================
PATTERN 5: Update Trade List Access
================================================================================

TEST: test_simulator_speed_comparison

OLD (v4.5.1 - dict access):
----------------------------
print(f"  Trades processed: {len(result_new['all_trades'])}")


NEW (v4.6 - property access):
------------------------------
print(f"  Trades processed: {len(result_new.trades)}")


================================================================================
PATTERN 6: Backward Compatibility Testing
================================================================================

NEW TEST: Verify to_dict() works for legacy compatibility

def test_trade_result_backward_compatibility(config_core, test_data, test_signals):
    """Verify TradeResult.to_dict() provides legacy format"""
    sim = NewTradeSimulator(config_core, test_data["full"])
    
    result = sim.simulate_trades(
        df_strategy=test_data["strategy"],
        filtered_signals=test_signals,
        df_ltf=test_data["ltf"],
        verbose=False,
    )
    
    # Should return TradeResult contract
    from src.strategies.contracts.trade_contracts import TradeResult
    assert isinstance(result, TradeResult)
    
    # to_dict() should provide legacy format
    result_dict = result.to_dict()
    assert "all_trades" in result_dict
    assert "closed_trades" in result_dict
    assert "open_trades" in result_dict
    assert "rejected_trades" in result_dict
    assert "exit_stats" in result_dict
    assert "risk_stats" in result_dict
    
    # Verify counts match
    assert len(result_dict["all_trades"]) == len(result.trades)
    assert len(result_dict["rejected_trades"]) == len(result.rejected_signals)


================================================================================
COMPLETE TEST FILE CHANGE SUMMARY
================================================================================

Tests to Update:
1. ✅ test_legacy_vs_new_trade_count_parity
2. ✅ test_legacy_vs_new_metrics_parity
3. ✅ test_simulator_speed_comparison
4. ✅ test_core_vs_debug_speed_improvement
5. ✅ test_throughput_benchmark
6. ✅ test_legacy_vs_new_speed_benchmark

Changes Per Test:
- Replace dict key access with property access
- result_new["all_trades"] → result_new.trades
- result_new["rejected_trades"] → result_new.rejected_signals
- result_new["exit_stats"] → result_new.exits_by_reason
- result_new["risk_stats"]["total_approved"] → result_new.risk_approved
- result_new["position_rejected_count"] → result_new.position_rejected

New Tests to Add:
- test_trade_result_backward_compatibility (verify to_dict())
- test_trade_result_contract_types (verify contract types)

Estimated Lines Changed: ~30-40 lines across 6 tests
Complexity: LOW (simple property access changes)

================================================================================
TEST UPDATE VERIFICATION CHECKLIST
================================================================================
[ ] All result_new["..."] replaced with result_new.property
[ ] Trade count assertions use .trades
[ ] Rejected signal assertions use .rejected_signals
[ ] Exit stats use .exits_by_reason
[ ] Risk stats use .risk_approved, .risk_rejected
[ ] Position rejected use .position_rejected
[ ] Legacy comparison still uses result_legacy["..."]
[ ] All tests pass with new contracts
[ ] Backward compatibility test added

================================================================================
### The Problem
In trade_simulator.py, the _reject_signal method creates a TradeEntry with entry_price=0.0:
````python
def _reject_signal(self, timestamp, direction, signal_id, reason, verbose):
    self.trade_counter += 1
    
    # Create minimal TradeEntry for rejected signal
    # Note: Rejected trades don't have valid prices/SL/TP
    entry = TradeEntry(
        entry_id=f"E{self.trade_counter}",
        trade_manager_id=None,
        signal_id=signal_id,
        entry_time=timestamp,
        direction=TradeDirection.from_string(direction),
        entry_price=0.0,  # <-- PROBLEM: This violates validation
        stop_loss=0.0,
        take_profit=0.0,
        position_size=0.0,
        sl_distance=0.0,
        tp_distance=0.0,
        risk_reward_ratio=0.0,
        atr_value=None,
        spread_enabled=False,
        spread_points=None,
        sl_adjusted=False,
        comment=f"Rejected: {reason}",
    )

But in trade_contracts.py, the validation requires**:
def __post_init__(self):
    """Validate entry data"""
    if self.entry_price <= 0:  # <-- This fails with entry_price=0.0
        raise ValueError("Entry price must be positive")

I think it can maybe somehow also reference the CLOSE ON OPPOSIT topic which we yet need to discuss
Gerenaly so I see follwing options (not exhaustive to discuss with you) 

Option 1 (not nice): Use a special sentinel value for rejected trades
Modify the TradeEntry contract to allow a special value for rejected trades:
# In trade_contracts.py
def __post_init__(self):
    """Validate entry data"""
    # Allow entry_price = -1.0 for rejected trades
    if self.entry_price <= 0 and self.entry_price != -1.0:
        raise ValueError("Entry price must be positive (or -1 for rejected trades)")
And in trade_simulator.py:

python
entry = TradeEntry(
    # ... other fields ...
    entry_price=-1.0,  # Special sentinel for rejected
    # ...
)
Option 2: Don't create TradeEntry for rejected signals
Instead, store rejected signals in a separate list or use a different mechanism:

python
def __init__(self, config: Dict, df_full: pd.DataFrame):
    # ... existing code ...
    self.rejected_signals: List[Dict] = []  # New list for rejected signals

def _reject_signal(self, timestamp, direction, signal_id, reason, verbose):
    self.trade_counter += 1
    
    # Store as dict instead of TradeEntry
    self.rejected_signals.append({
        "entry_id": f"E{self.trade_counter}",
        "signal_id": signal_id,
        "entry_time": timestamp,
        "direction": direction,
        "comment": f"Rejected: {reason}",
        "status": "REJECTED"
    })
    
    # Don't add to all_trades
Then modify the output conversion to include rejected signals separately:

python
# In simulate_trades return block
rejected_trades_dict = [
    {
        **r,
        "entry_price": 0.0,  # Legacy format expects 0.0
        "stop_loss": 0.0,
        "take_profit": 0.0,
        # ... other required fields
    }
    for r in self.rejected_signals
]
Option 3 (Recommended): Create a separate RejectedTrade contract/class/method
Add a new contract type for rejected signals:
python
# In trade_contracts.py
@dataclass
class RejectedTrade:
    """Represents a rejected signal (doesn't require price validation)"""
    entry_id: str
    signal_id: Optional[int]
    entry_time: pd.Timestamp
    direction: str
    reason: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "entry_id": self.entry_id,
            "signal_id": self.signal_id,
            "entry_time": self.entry_time,
            "direction": self.direction,
            "reject_reason": self.reason,
            "status": "REJECTED",
            # Legacy compatibility fields
            "entry_price": 0.0,
            "stop_loss": 0.0,
            "take_profit": 0.0,
            # ...
        }
To summ up: 
The TradeSimulator is trying to violate that constraint by using 0.0 as a sentinel
Maybe it is to alligne to legacy system but let me to clarify and enphasise here:
We need to compare to legacy for instant only for Parily and Perf but we don't need to keep any legacy compatibility atrifact. What I mean our new architecture is fully parrallel it is not making part of any existing legacy system. We don't need to keep any compatibility because for exemple our new TradeSimulator is used in some other legacy components. We migrate basing on legacy codes and experience but we create a completly new separate tool.Ath this level TradeSimulator and having already DataLoader, SignalGenerator, FilterPipline we have everything we even don't need to keep any metricks calculation because in nest steps we will fulle refactor ProgressiveTracker etc. At this stage it is still importen to compare the results we have in termo of parity and perf because legacy is the only existing already working and heavily tested which can give us a comparison baseline but we are not pushed to stay "compatible" at any level.
Hope this clarifies and you can put it in any documentation so we are not loosing this important point when moving form one session to another.  
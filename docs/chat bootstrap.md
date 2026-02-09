# Backtesting Platform — Chat Bootstrap Document
**Purpose:** Restore full project context in a new chat window  
**Owner:** Krzysztof  
**Version:** 1.0  

---

## 1. Project Summary

This project is a high‑precision backtesting engine designed to complement TradingView.  
It supports:
- tick‑based OHLCV generation  
- multi‑timeframe execution  
- LTF (1‑second) SL/TP execution  
- modular strategy architecture  
- TradingView → Python strategy translation  

We are migrating to a new architecture based on:
- `SignalFrame`
- `TradeDecision`
- `TradeParameters`
- `TradeRecord`
- `Simulator Core v1.0`

The old system remains frozen and is used as a reference.

---

## 2. Completed Modules (New Architecture)

The following modules are **finished and stable**:
- src/strategies/trade_management/position.py
- src/strategies/trade_management/signal_frame.py
- src/strategies/trade_management/simulator.py
- src/strategies/trade_management/trade_decision.py
- src/strategies/trade_management/trade_direction.py
- src/strategies/trade_management/trade_parameters.py
- src/strategies/trade_management/trade_record.py
- src/strategies/trade_management/decision_type.py


These form the foundation of the new system.

---

## 3. Next Module to Migrate

### **WBWSStrategy**

We will:
- rebuild it using SignalFrame  
- integrate WBWSTrigger  
- integrate FilterPipeline logic  
- integrate TradeManager logic  
- integrate RiskManager logic  
- output TradeDecision objects  

This will be done slowly and safely.

---

## 4. Files to Upload in This New Chat

Please upload the following files:

### **Strategy Logic**
- `WBWSStrategy` (current version)

### **Signal Logic**
- `wbws_trigger.py`

### **Filter Logic**
- `filter_pipeline.py`

### **Risk Logic**
- `risk_manager.py`

### **Position Logic**
- `trade_manager.py`

These are required to reconstruct WBWSStrategy v1.1.

---

## 5. Working Principles

- Old system is frozen (reference only).
- New system is built in parallel.
- Migration is incremental and validated step‑by‑step.
- Every module is tested before moving on.
- No breaking changes to old code.
- Folder structure and `__init__.py` files must remain consistent.

---

## 6. Current Task

**Start WBWSStrategy Migration — Step 1**  
Rebuild the strategy using the new architecture.

---

## 7. What I Expect From Copilot

- Maintain project plan  
- Track progress  
- Anticipate chat limits  
- Provide continuity  
- Keep me at the “owner” level  
- Ensure safe, incremental migration  
- Validate each module  
- Protect old system  

---

## 8. Ready to Begin

Once the above files are uploaded, we continue with:

**WBWSStrategy v1.1 — Raw Signal Integration**

# ============================================================
# L1 PATCH FILE — Type annotation & import hygiene
# All changes are annotation-only (zero runtime impact).
# Apply with: patch -p0 < L1_patches.patch
# Or apply manually — each hunk is labelled with the target file.
# ============================================================

# ── metrics_calculator.py ─────────────────────────────────────────────
# Dead import: List was used by old getattr patterns removed in Block 4.

--- src/strategies/specific/modules/metrics_calculator.py (original)
+++ src/strategies/specific/modules/metrics_calculator.py (patched)
@@ typing import @@
-from typing import List
+from typing import List   # <-- DELETE this entire import line
# Result: file has no remaining typing import needed (all standard types used inline)
# If any future method adds a List annotation, re-add it then.


# ── analytics_contracts.py ────────────────────────────────────────────
# 1. Add Any to imports (needed for Dict[str, Any] return types)
# 2. All to_dict() -> Dict → -> Dict[str, Any]
# 3. vs_baseline: Optional[Dict] → Optional[Dict[str, Any]]

--- src/strategies/contracts/analytics_contracts.py (original)
+++ src/strategies/contracts/analytics_contracts.py (patched)
@@ typing import @@
-from typing import TYPE_CHECKING, Dict, List, Optional
+from typing import TYPE_CHECKING, Any, Dict, List, Optional

@@ SessionMetrics.to_dict @@
-    def to_dict(self) -> Dict:
+    def to_dict(self) -> Dict[str, Any]:
# (apply to ALL to_dict() methods in the file — 10 occurrences)

@@ ComparativeContext.vs_baseline field @@
-    vs_baseline:       Optional[Dict]
+    vs_baseline:       Optional[Dict[str, Any]]


# ── trade_simulator.py ────────────────────────────────────────────────
# 1. timings annotation: Dict[str, list] → Dict[str, List[float]]
# 2. _ltf_windows: Dict → Dict[str, Any]

--- src/strategies/specific/modules/trade_simulator.py (original v5.2.0)
+++ src/strategies/specific/modules/trade_simulator.py (patched)
@@ TradeSimulatorProfiler.__init__ @@
-        self.timings: Dict[str, list] = defaultdict(list)
+        self.timings: Dict[str, List[float]] = defaultdict(list)

@@ TradeSimulator.__init__ @@
-        self._ltf_windows: Dict = {}
+        self._ltf_windows: Dict[str, Any] = {}
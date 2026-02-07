"""
Performance Metrics Calculator v2
Aligned with ProgressiveTracker v2 and optimized for speed & clarity.
"""

import pandas as pd
from typing import Dict, Optional


# ----------------------------------------------------------------------
# DRAW DOWN
# ----------------------------------------------------------------------
def calculate_drawdown(trades_df: pd.DataFrame) -> Dict:
    """
    Compute drawdown metrics from closed trades.
    Uses cumulative PnL curve.
    """
    if trades_df.empty:
        return {
            "max_drawdown_points": 0,
            "max_drawdown_percent": 0,
            "current_drawdown_points": 0,
            "recovery_factor": 0,
        }

    df = trades_df.sort_values("exit_time", ascending=True)
    cum = df["pnl_points"].cumsum()
    peak = cum.cummax()
    dd = cum - peak

    max_dd = dd.min()
    max_dd_pct = (max_dd / peak.max() * 100) if peak.max() > 0 else 0
    current_dd = dd.iloc[-1]

    total_pnl = df["pnl_points"].sum()
    recovery = abs(total_pnl / max_dd) if max_dd < 0 else 0

    return {
        "max_drawdown_points": float(max_dd),
        "max_drawdown_percent": float(max_dd_pct),
        "current_drawdown_points": float(current_dd),
        "recovery_factor": float(recovery),
    }


# ----------------------------------------------------------------------
# LOSING STREAK
# ----------------------------------------------------------------------
def calculate_losing_streak(trades_df: pd.DataFrame) -> Dict:
    """
    Compute losing streak metrics.
    """
    if trades_df.empty:
        return {
            "max_losing_streak": 0,
            "current_streak": 0,
            "avg_losing_streak_length": 0,
            "total_losing_streaks": 0,
        }

    df = trades_df.sort_values("exit_time", ascending=True)
    is_win = df["pnl_points"] > 0

    max_ls = 0
    current_ls = 0
    losing_streaks = []
    current_streak = 0

    for win in is_win:
        if win:
            if current_ls > 0:
                losing_streaks.append(current_ls)
                current_ls = 0
            current_streak = current_streak + 1 if current_streak >= 0 else 1
        else:
            current_ls += 1
            max_ls = max(max_ls, current_ls)
            current_streak = current_streak - 1 if current_streak <= 0 else -1

    if current_ls > 0:
        losing_streaks.append(current_ls)

    avg_ls = sum(losing_streaks) / len(losing_streaks) if losing_streaks else 0

    return {
        "max_losing_streak": int(max_ls),
        "current_streak": int(current_streak),
        "avg_losing_streak_length": float(avg_ls),
        "total_losing_streaks": len(losing_streaks),
    }


# ----------------------------------------------------------------------
# MAIN METRICS CALCULATOR
# ----------------------------------------------------------------------
def calculate_performance_metrics(
    trades_df: pd.DataFrame,
    ohlcv_df: Optional[pd.DataFrame] = None,
    detailed: bool = True,
) -> Dict:
    """
    Compute all performance metrics.
    - detailed=False → core mode (fast, minimal)
    - detailed=True → debug mode (full metrics)
    """

    if trades_df.empty:
        return {
            "total_trades": 0,
            "message": "No trades to analyze",
            "spread_analysis": {
                "total_spread_cost": 0,
                "avg_spread_per_trade": 0,
                "spread_impact_on_pnl": 0,
            },
        }

    closed = trades_df.loc[trades_df["status"] == "CLOSED"].copy()
    closed["month"] = pd.to_datetime(closed["exit_time"]).dt.to_period("M")
    open_trades = trades_df[trades_df["status"] == "OPEN"]
    rejected = trades_df[trades_df["status"] == "REJECTED"]

    # ------------------------------------------------------------------
    # BASIC METRICS
    # ------------------------------------------------------------------
    metrics = {
        "total_signals": len(trades_df),
        "total_trades": len(closed),
        "open_trades": len(open_trades),
        "rejected_trades": len(rejected),
    }

    if closed.empty:
        return metrics

    pnl = closed["pnl_points"]
    pnl_pct = closed["pnl_percent"]

    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]

    metrics.update(
        {
            "winning_trades": len(wins),
            "losing_trades": len(losses),
            "breakeven_trades": len(pnl[pnl == 0]),
            "win_rate": (len(wins) / len(closed) * 100) if len(closed) else 0,
            "loss_rate": (len(losses) / len(closed) * 100) if len(closed) else 0,
            "total_pnl_points": float(pnl.sum()),
            "total_pnl_percent": float(pnl_pct.sum()),
            "avg_pnl_points": float(pnl.mean()),
            "avg_pnl_percent": float(pnl_pct.mean()),
            "avg_win_points": float(wins.mean()) if len(wins) else 0,
            "avg_loss_points": float(losses.mean()) if len(losses) else 0,
            "largest_win": float(pnl.max()),
            "largest_loss": float(pnl.min()),
        }
    )

    # ------------------------------------------------------------------
    # PROFIT FACTOR & EXPECTANCY
    # ------------------------------------------------------------------
    win_sum = wins.sum()
    loss_sum = abs(losses.sum())

    metrics["profit_factor"] = float(win_sum / loss_sum) if loss_sum > 0 else 0

    win_rate = metrics["win_rate"] / 100
    metrics["expectancy_points"] = float(
        (win_rate * metrics["avg_win_points"])
        - ((1 - win_rate) * abs(metrics["avg_loss_points"]))
    )

    # ------------------------------------------------------------------
    # DRAW DOWN & LOSING STREAK
    # ------------------------------------------------------------------
    dd = calculate_drawdown(closed)
    ls = calculate_losing_streak(closed)

    metrics["max_drawdown_points"] = dd["max_drawdown_points"]
    metrics["max_losing_streak"] = ls["max_losing_streak"]

    if detailed:
        metrics["drawdown_analysis"] = dd
        metrics["losing_streak_analysis"] = ls

    # ------------------------------------------------------------------
    # EARLY EXIT FOR CORE MODE
    # ------------------------------------------------------------------
    if not detailed:
        return metrics

    # ------------------------------------------------------------------
    # DETAILED METRICS (DEBUG MODE)
    # ------------------------------------------------------------------

    # Exit reasons
    metrics["exit_reasons"] = (
        closed["exit_reason"].value_counts().to_dict()
        if "exit_reason" in closed.columns
        else {}
    )

    # Long/short breakdown
    longs = closed[closed["direction"] == "BUY"]
    shorts = closed[closed["direction"] == "SELL"]

    metrics["long_short_breakdown"] = {
        "long_trades": len(longs),
        "short_trades": len(shorts),
        "long_win_rate": (len(longs[longs["pnl_points"] > 0]) / len(longs) * 100)
        if len(longs)
        else 0,
        "short_win_rate": (len(shorts[shorts["pnl_points"] > 0]) / len(shorts) * 100)
        if len(shorts)
        else 0,
        "long_pnl_points": float(longs["pnl_points"].sum()) if len(longs) else 0,
        "short_pnl_points": float(shorts["pnl_points"].sum()) if len(shorts) else 0,
    }

    # Monthly performance
    try:
        closed["month"] = pd.to_datetime(closed["exit_time"]).dt.to_period("M")
        monthly = closed.groupby("month").agg(
            pnl_points=("pnl_points", "sum"),
            trades=("trade_id", "count"),
        )
        metrics["monthly_performance"] = {
            str(k): v.to_dict() for k, v in monthly.iterrows()
        }
    except Exception:
        metrics["monthly_performance"] = {}

    # Spread analysis
    if "spread_cost_points" in closed.columns:
        sc = closed["spread_cost_points"]
        metrics["spread_analysis"] = {
            "total_spread_cost_points": float(sc.sum()),
            "avg_spread_per_trade_points": float(sc.mean()),
            "max_spread_points": float(sc.max()),
            "min_spread_points": float(sc.min()),
            "trades_with_spread": int((sc > 0).sum()),
            "spread_penetration_rate": float((sc > 0).sum() / len(closed) * 100),
            "spread_impact_on_pnl": float(sc.sum() / abs(pnl.sum()) * 100)
            if pnl.sum() != 0
            else 0,
        }

    # Realized RR
    winning = closed[closed["pnl_points"] > 0]
    if len(winning) and winning["sl_distance"].mean() > 0:
        metrics["avg_risk_reward_realized"] = float(
            winning["tp_distance"].mean() / winning["sl_distance"].mean()
        )
    else:
        metrics["avg_risk_reward_realized"] = 0

    # Rejection analysis
    if len(rejected) and "reject_reason" in rejected.columns:
        metrics["rejection_analysis"] = {
            "total_rejected": len(rejected),
            "rejection_rate": float(len(rejected) / len(trades_df) * 100),
            "rejection_reasons": rejected["reject_reason"]
            .value_counts()
            .to_dict(),
        }

    return metrics
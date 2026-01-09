import numpy as np

def max_drawdown(curve):
    peak = curve[0]
    max_dd = 0

    for v in curve:
        if v > peak:
            peak = v
        dd = (peak - v)
        max_dd = max(max_dd, dd)

    return max_dd


def mc_summary(equity_curves):
    final_balances = [c[-1] for c in equity_curves]
    drawdowns = [max_drawdown(c) for c in equity_curves]

    return {
        "avg_final_balance": float(np.mean(final_balances)),
        "worst_drawdown": float(max(drawdowns)),
        "avg_drawdown": float(np.mean(drawdowns)),
        "ruin_probability": sum(b <= 0 for b in final_balances) / len(final_balances)
    }
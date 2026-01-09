class FitnessEvaluator:
    def __init__(self, constraints: dict, weights: dict):
        self.constraints = constraints
        self.weights = weights

    def passes_constraints(self, m: dict) -> bool:
        return (
            m["winrate"] >= self.constraints["min_winrate"] and
            m["max_drawdown"] <= self.constraints["max_drawdown"] and
            m["losing_streak"] <= self.constraints["max_losing_streak"] and
            m["trades_per_day"] >= self.constraints["min_trades_per_day"] and
            m["expectancy"] >= self.constraints["min_expectancy"] and
            m["profit_factor"] >= self.constraints["min_profit_factor"]
        )

    def score(self, m: dict) -> float:
        return (
            self.weights["net_pnl"] * m["net_pnl"] +
            self.weights["expectancy"] * m["expectancy"] -
            self.weights["drawdown"] * m["max_drawdown"] -
            self.weights["losing_streak"] * m["losing_streak"] +
            self.weights["winrate"] * m["winrate"]
        )
class FitnessEvaluator:
    def __init__(self, constraints: dict, weights: dict):
        self.constraints = constraints
        self.weights = weights

    def passes_constraints(self, m: dict) -> bool:
        """Check if metrics pass all constraints"""
        # Note: max_drawdown and losing_streak might not be in your reports yet
        # We'll handle them as optional for now
        checks = [
            m.get("winrate", 0) >= self.constraints.get("min_winrate", 0),
            m.get("max_drawdown", 0) <= self.constraints.get("max_drawdown", 999),  # Default high if missing
            m.get("losing_streak", 0) <= self.constraints.get("max_losing_streak", 999),  # Default high if missing
            m.get("trades_per_day", 0) >= self.constraints.get("min_trades_per_day", 0),
            m.get("expectancy", 0) >= self.constraints.get("min_expectancy", -999),  # Default low if missing
            m.get("profit_factor", 0) >= self.constraints.get("min_profit_factor", 0)
        ]
        
        return all(checks)

    def score(self, m: dict) -> float:
        """Calculate fitness score"""
        # Use drawdown if available, otherwise 0
        drawdown = m.get("max_drawdown", 0)
        
        return (
            self.weights["net_pnl"] * m.get("net_pnl", 0) +
            self.weights["expectancy"] * m.get("expectancy", 0) -
            self.weights["drawdown"] * drawdown -
            self.weights["losing_streak"] * m.get("losing_streak", 0) +
            self.weights["winrate"] * m.get("winrate", 0)
        )
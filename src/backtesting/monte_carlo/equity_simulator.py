import random

class EquitySimulator:
    def __init__(self, initial_balance=10000):
        self.initial_balance = initial_balance

    def simulate(self, returns):
        equity = self.initial_balance
        curve = [equity]

        for r in returns:
            equity += r
            curve.append(equity)

        return curve
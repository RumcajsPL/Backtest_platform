import random

class TournamentSelection:
    def __init__(self, k=3):
        self.k = k

    def select(self, evaluated):
        competitors = random.sample(evaluated, self.k)
        return max(competitors, key=lambda x: x["fitness"])
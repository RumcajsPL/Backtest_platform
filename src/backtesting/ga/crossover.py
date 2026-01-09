import random
import copy

class UniformCrossover:
    def crossover(self, p1, p2):
        child = copy.deepcopy(p1)

        for key in p1:
            if random.random() < 0.5:
                child[key] = p2[key]

        return child
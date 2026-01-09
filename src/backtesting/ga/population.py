import random

class Population:
    def __init__(self, sampler, size):
        self.sampler = sampler
        self.size = size

    def generate(self):
        return [self.sampler.sample() for _ in range(self.size)]
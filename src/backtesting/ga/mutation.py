import random

class ParameterMutation:
    def __init__(self, sampler, rate=0.1):
        self.sampler = sampler
        self.rate = rate

    def mutate(self, params):
        new_params = params.copy()

        for key in params:
            if random.random() < self.rate:
                new_params[key] = self.sampler.sample()[key]

        return new_params
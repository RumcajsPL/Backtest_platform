from ga.population import Population
from ga.selection import TournamentSelection
from ga.crossover import UniformCrossover
from ga.mutation import ParameterMutation

class GeneticOptimizer:
    def __init__(self, sampler, orchestrator, fitness_engine, config):
        self.population = Population(sampler, config["population_size"])
        self.selector = TournamentSelection(config["tournament_k"])
        self.crossover = UniformCrossover()
        self.mutator = ParameterMutation(sampler, config["mutation_rate"])
        self.orchestrator = orchestrator
        self.fitness = fitness_engine
        self.config = config

    def evaluate(self, params):
        metrics = self.orchestrator.run_single(params)
        fitness = self.fitness.score(metrics)
        return {"params": params, "metrics": metrics, "fitness": fitness}

    def run(self):
        population = self.population.generate()
        evaluated = [self.evaluate(p) for p in population]

        for gen in range(self.config["generations"]):
            print(f"🧬 Generation {gen+1}")

            new_population = []

            while len(new_population) < self.config["population_size"]:
                p1 = self.selector.select(evaluated)
                p2 = self.selector.select(evaluated)

                child = self.crossover.crossover(p1["params"], p2["params"])
                child = self.mutator.mutate(child)

                new_population.append(child)

            evaluated = [self.evaluate(p) for p in new_population]

        return sorted(evaluated, key=lambda x: x["fitness"], reverse=True)
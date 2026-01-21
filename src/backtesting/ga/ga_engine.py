import random
from ga.selection import TournamentSelection
from ga.crossover import UniformCrossover
from ga.mutation import ParameterMutation
from ga.population import Population  

class GeneticOptimizer:
    def __init__(self, sampler, orchestrator, fitness_engine, config, zone_name, zone_dir):
        self.sampler = sampler
        self.orchestrator = orchestrator
        self.fitness = fitness_engine
        self.config = config
        self.zone_name = zone_name
        self.zone_dir = zone_dir
        
        # GA configuration
        self.population_size = config.get("population_size", 4)
        self.generations = config.get("generations", 2)
        self.mutation_rate = config.get("mutation_rate", 0.15)
        self.crossover_rate = config.get("crossover_rate", 0.6)
        self.elite_fraction = config.get("elite_fraction", 0.2)
        self.tournament_k = config.get("tournament_k", 3)
        
        # Initialize components
        self.selector = TournamentSelection(self.tournament_k)
        self.crossover_op = UniformCrossover()
        self.mutation_op = ParameterMutation(sampler, self.mutation_rate)
        self.population_generator = Population(sampler, self.population_size)
    
    def get_random_parameters(self):
        return self.population_generator.get_random_parameters()
    
    def evaluate_individual(self, params, individual_index):
        """Evaluate a single parameter set using Orchestrator's cached methods"""
        
        # 1. Create config (Cached by Orchestrator)
        temp_yaml = self.orchestrator.create_temp_yaml(
            params, 
            self.zone_name, 
            individual_index, 
            "ga"
        )
        
        # 2. Run Strategy (Cached by Orchestrator - Result Cache)
        report_path = self.orchestrator.run_strategy(temp_yaml, self.zone_dir, individual_index)
        
        if report_path:
            # 3. Extract Metrics (Cached by Orchestrator - Metric Cache)
            # This replaces: metrics_extractor = OptimizationMetrics(str(report_path))
            real_metrics = self.orchestrator.get_cached_metrics(report_path)
            
            if self.fitness.passes_constraints(real_metrics):
                # 4. Calculate Fitness (Cached by Orchestrator - Fitness Cache)
                # This replaces: score = self.fitness.score(real_metrics)
                score = self.orchestrator.get_cached_fitness(real_metrics)
                return (params, score, real_metrics)
            else:
                return (params, -1000, real_metrics)
        
        return (params, -1000, {})  # Fallback for no-report cases
    
    def run(self, initial_population=None):
        """Run GA optimization"""
        print(f"🧬 Starting Genetic Algorithm optimization")
        print(f"   Population: {self.population_size}")
        print(f"   Generations: {self.generations}")
        
        # Initialize population
        if initial_population:
            population = initial_population[:self.population_size]
            if len(population) < self.population_size:
                population.extend(self.population_generator.generate()[:self.population_size - len(population)])
        else:
            population = self.population_generator.generate()
        
        # Ensure population size
        while len(population) < self.population_size:
            population.append(self.get_random_parameters())
        population = population[:self.population_size]
        
        # Evaluate initial population
        evaluated = []
        for i, params in enumerate(population):
            print(f"   Evaluating individual {i+1}/{len(population)}")
            result = self.evaluate_individual(params, i)
            evaluated.append({"params": params, "fitness": result[1], "metrics": result[2]})
        
        # Evolution loop
        for gen in range(self.generations):
            print(f"\n   Generation {gen+1}/{self.generations}")
            
            evaluated.sort(key=lambda x: x["fitness"], reverse=True)
            
            # Elitism
            elite_count = max(1, int(self.population_size * self.elite_fraction))
            new_population = [evaluated[i]["params"] for i in range(elite_count)]
            
            # Breed
            while len(new_population) < self.population_size:
                parent1 = self.selector.select(evaluated)
                parent2 = self.selector.select(evaluated)
                child = self.crossover_op.crossover(parent1["params"], parent2["params"]) if random.random() < self.crossover_rate else parent1["params"].copy()
                child = self.mutation_op.mutate(child)
                new_population.append(child)
            
            # Re-evaluate (Elites will hit cache instantly!)
            new_evaluated = []
            for i, params in enumerate(new_population):
                # Unique index logic could be improved, but uniqueness ensures safe filenames
                unique_idx = i + (gen + 1) * self.population_size 
                result = self.evaluate_individual(params, unique_idx)
                new_evaluated.append({"params": params, "fitness": result[1], "metrics": result[2]})
            
            evaluated = new_evaluated
            
            best_fitness = max(x["fitness"] for x in evaluated)
            avg_fitness = sum(x["fitness"] for x in evaluated) / len(evaluated)
            print(f"   Best: {best_fitness:.4f}, Avg: {avg_fitness:.4f}")
        
        # Final Sort
        evaluated.sort(key=lambda x: x["fitness"], reverse=True)
        results = [(e["params"], e["fitness"], e["metrics"]) for e in evaluated[:10]]
        
        print(f"\n✅ GA optimization completed with {len(results)} candidates")
        return results
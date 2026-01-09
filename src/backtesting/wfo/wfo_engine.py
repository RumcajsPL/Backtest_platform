from wfo.window_generator import WalkForwardWindowGenerator
from wfo.wfo_evaluator import WFOEvaluator

class WalkForwardEngine:
    def __init__(self, orchestrator, fitness_engine, config):
        self.orchestrator = orchestrator
        self.config = config
        self.evaluator = WFOEvaluator(fitness_engine)

    def run(self, params, date_range):
        generator = WalkForwardWindowGenerator(
            start=date_range["start"],
            end=date_range["end"],
            train_days=self.config["train_days"],
            test_days=self.config["test_days"]
        )

        windows = generator.generate()
        results = []

        for i, w in enumerate(windows):
            print(f"🔁 WFO Window {i+1}")

            metrics = self.orchestrator.run_single(
                params=params,
                date_range=w["test"]
            )

            results.append({
                "window": w,
                "metrics": metrics
            })

        return self.evaluator.evaluate(results), results
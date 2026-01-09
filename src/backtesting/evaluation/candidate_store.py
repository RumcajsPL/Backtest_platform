import json
from pathlib import Path

class CandidateStore:
    def __init__(self, output_dir: Path):
        self.file_path = output_dir / "candidates.json"
        self.candidates = []

        if self.file_path.exists():
            with open(self.file_path, "r") as f:
                self.candidates = json.load(f)

    def add(self, params: dict, metrics: dict, fitness: float):
        self.candidates.append({
            "params": params,
            "metrics": metrics,
            "fitness": fitness
        })

    def save(self):
        with open(self.file_path, "w") as f:
            json.dump(self.candidates, f, indent=2)
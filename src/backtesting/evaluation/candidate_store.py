import json
from pathlib import Path
from typing import Optional
import numpy as np

class CandidateStore:
    def __init__(self, output_dir: Path):
        self.file_path = output_dir / "candidates.json"
        self.candidates = []

        if self.file_path.exists():
            with open(self.file_path, "r") as f:
                self.candidates = json.load(f)

    def add(self, 
            params: dict, 
            metrics: dict, 
            fitness: float,
            zone_name: Optional[str] = None,
            sample_index: Optional[int] = None,
            source: Optional[str] = None):
        """Add a candidate with optional metadata"""
        
        # Clean numpy types from params and metrics
        cleaned_params = self.clean_numpy_types(params)
        cleaned_metrics = self.clean_numpy_types(metrics)
        
        candidate = {
            "params": cleaned_params,
            "metrics": cleaned_metrics,
            "fitness": float(fitness)  # Ensure fitness is float
        }
        
        # Add optional metadata if provided
        if zone_name is not None:
            candidate["zone_name"] = zone_name
        if sample_index is not None:
            candidate["sample_index"] = int(sample_index)  # Ensure int
        if source is not None:
            candidate["source"] = source
            
        self.candidates.append(candidate)

    def clean_numpy_types(self, obj):
        """Recursively convert numpy types to Python native types"""
        if isinstance(obj, dict):
            return {k: self.clean_numpy_types(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.clean_numpy_types(v) for v in obj]
        elif isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        elif hasattr(obj, 'dtype'):  # numpy array
            return obj.tolist()  # Convert to Python list
        else:
            return obj

    def save(self):
        """Save candidates to JSON file"""
        with open(self.file_path, "w") as f:
            json.dump(self.candidates, f, indent=2)
            
    def get_top_n(self, n: int = 10):
        """Get top N candidates by fitness score"""
        return sorted(self.candidates, key=lambda x: x["fitness"], reverse=True)[:n]
    
    def count(self):
        """Return number of stored candidates"""
        return len(self.candidates)
class CandidateRanker:
    def __init__(self, candidates: list):
        self.candidates = candidates

    def top_n(self, n=10):
        return sorted(
            self.candidates,
            key=lambda x: x["fitness"],
            reverse=True
        )[:n]
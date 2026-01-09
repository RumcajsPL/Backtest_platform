from datetime import timedelta

class WalkForwardWindowGenerator:
    def __init__(self, start, end, train_days, test_days):
        self.start = start
        self.end = end
        self.train_days = train_days
        self.test_days = test_days

    def generate(self):
        windows = []
        current = self.start

        while current + timedelta(days=self.train_days + self.test_days) <= self.end:
            train_start = current
            train_end = current + timedelta(days=self.train_days)
            test_start = train_end
            test_end = train_end + timedelta(days=self.test_days)

            windows.append({
                "train": (train_start, train_end),
                "test": (test_start, test_end)
            })

            current += timedelta(days=self.test_days)

        return windows
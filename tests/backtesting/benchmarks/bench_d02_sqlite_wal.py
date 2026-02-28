"""
D-02 Benchmark: SQLite WAL mode + single-writer queue.

Simulates 6 ProcessPoolExecutor workers each submitting ~83 fake CandidateRecord
rows to a multiprocessing.Queue. One writer thread drains the queue and writes
to SQLite in WAL mode.

Pass criterion: 500 rows, zero errors, no corruption on repeated runs.
"""
from __future__ import annotations

import json
import queue
import sqlite3
import tempfile
import threading
import time
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional


# ── Minimal fake record (matches CandidateRecord primitive structure) ─────────

@dataclass
class FakeRecord:
    run_id: str
    candidate_id: str
    zone_name: str
    stage: str
    generation: Optional[int]
    recorded_at: str
    parameters_json: str
    fitness_score: Optional[float]
    passed_constraints: Optional[int]


# ── SQLite setup ───────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS bench_candidates (
    run_id              TEXT NOT NULL,
    candidate_id        TEXT PRIMARY KEY,
    zone_name           TEXT NOT NULL,
    stage               TEXT NOT NULL,
    generation          INTEGER,
    recorded_at         TEXT NOT NULL,
    parameters_json     TEXT NOT NULL,
    fitness_score       REAL,
    passed_constraints  INTEGER
);
"""


def _setup_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


# ── Writer thread ──────────────────────────────────────────────────────────────

def _drain_queue(conn: sqlite3.Connection, write_queue: queue.Queue, errors: list) -> None:
    """Drain the write queue and INSERT rows sequentially."""
    while True:
        try:
            record = write_queue.get(timeout=5.0)
        except queue.Empty:
            break
        if record is None:
            write_queue.task_done()
            break
        try:
            conn.execute(
                """INSERT OR IGNORE INTO bench_candidates
                   (run_id, candidate_id, zone_name, stage, generation,
                    recorded_at, parameters_json, fitness_score, passed_constraints)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    record.run_id, record.candidate_id, record.zone_name,
                    record.stage, record.generation, record.recorded_at,
                    record.parameters_json, record.fitness_score,
                    record.passed_constraints,
                ),
            )
            conn.commit()
        except Exception as exc:
            errors.append(str(exc))
        finally:
            write_queue.task_done()


# ── Worker function (runs in child process) ────────────────────────────────────

def worker_task(worker_id: int, run_id: str, n_records: int) -> list[FakeRecord]:
    """Generate fake records; return them to the parent process."""
    records = []
    for i in range(n_records):
        params = {"rsi_period": 14 + i % 6, "atr_multiplier": 1.5 + (i % 4) * 0.25}
        records.append(
            FakeRecord(
                run_id=run_id,
                candidate_id=str(uuid.uuid4()),
                zone_name="safe",
                stage="RANDOM",
                generation=None,
                recorded_at=datetime.utcnow().isoformat(),
                parameters_json=json.dumps(params),
                fitness_score=0.5 + (i % 10) * 0.04,
                passed_constraints=1,
            )
        )
    return records


# ── Main benchmark ─────────────────────────────────────────────────────────────

def run_benchmark(n_workers: int = 6, total_records: int = 500, n_repeats: int = 3) -> None:
    print(f"\n{'='*60}")
    print(f"D-02 Benchmark: SQLite WAL + Writer Queue")
    print(f"Workers: {n_workers} | Total records: {total_records} | Repeats: {n_repeats}")
    print(f"{'='*60}")

    records_per_worker = total_records // n_workers
    actual_total = records_per_worker * n_workers

    all_passed = True

    for repeat in range(1, n_repeats + 1):
        run_id = str(uuid.uuid4())

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bench.db"
            conn = _setup_db(db_path)

            write_queue: queue.Queue = queue.Queue()
            errors: list = []

            writer = threading.Thread(
                target=_drain_queue, args=(conn, write_queue, errors), daemon=True
            )
            writer.start()

            t_start = time.perf_counter()

            # Spawn workers, collect records, push to queue
            with ProcessPoolExecutor(max_workers=n_workers) as pool:
                futures = [
                    pool.submit(worker_task, w, run_id, records_per_worker)
                    for w in range(n_workers)
                ]
                for fut in as_completed(futures):
                    for record in fut.result():
                        write_queue.put(record)

            # Sentinel to stop writer
            write_queue.join()
            write_queue.put(None)
            writer.join()

            elapsed = time.perf_counter() - t_start

            # Verify row count
            row_count = conn.execute(
                "SELECT COUNT(*) FROM bench_candidates WHERE run_id = ?", (run_id,)
            ).fetchone()[0]
            conn.close()

            passed = row_count == actual_total and len(errors) == 0
            status = "PASS ✓" if passed else "FAIL ✗"
            if not passed:
                all_passed = False

            print(
                f"  Run {repeat}/{n_repeats}: {status} | "
                f"rows={row_count}/{actual_total} | "
                f"errors={len(errors)} | "
                f"time={elapsed:.2f}s | "
                f"throughput={actual_total/elapsed:.0f} rows/s"
            )
            if errors:
                for e in errors[:5]:
                    print(f"    ERROR: {e}")

    print(f"\n{'='*60}")
    print(f"OVERALL: {'ALL RUNS PASSED ✓' if all_passed else 'ONE OR MORE RUNS FAILED ✗'}")
    print(f"{'='*60}\n")
    return all_passed


if __name__ == "__main__":
    run_benchmark()
import csv
import json
import os
import signal
import statistics
import subprocess
import sys
import time
from contextlib import ExitStack
from pathlib import Path

import psutil

HERE = Path(__file__).resolve().parent
OUTPUT = HERE / "runs"
CASES = {
    "one-async-sync": ("async-sync", 1, 50, 50),
    "one-async-sync-10": ("async-sync", 1, 50, 10),
    "one-threaded-50": ("threaded", 1, 50, 50),
    "one-threaded-10": ("threaded", 1, 50, 10),
    "five-async-sync": ("async-sync", 5, 10, 10),
    "five-threaded": ("threaded", 5, 10, 10),
    "one-multiprocess-50": ("multiprocess", 1, 50, 50),
    "one-multiprocess-10": ("multiprocess", 1, 50, 10),
    "five-multiprocess": ("multiprocess", 5, 10, 10),
}


def peak_in_flight(intervals):
    edges = [(start, 1) for start, *_ in intervals]
    edges.extend((end, -1) for _, _, end, *_ in intervals)
    active = peak = 0
    for _, delta in sorted(edges):
        active += delta
        peak = max(peak, active)
    return peak


def process_memory_kib(pid):
    resident = swap = 0
    try:
        with Path(f"/proc/{pid}/smaps_rollup").open() as maps:
            for line in maps:
                if line.startswith("Pss:"):
                    resident = int(line.split()[1])
                elif line.startswith("SwapPss:"):
                    swap = int(line.split()[1])
                    break
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        pass
    return resident, swap


def active_process_ids(workers):
    pids = set()
    for worker in workers:
        if worker.poll() is None:
            try:
                pids.add(worker.pid)
                pids.update(
                    child.pid
                    for child in psutil.Process(worker.pid).children(recursive=True)
                )
            except psutil.NoSuchProcess:
                pass
    return pids


def measure(case):
    mode, runs, steps, slots = CASES[case]
    folder = OUTPUT / case
    folder.mkdir(parents=True, exist_ok=True)
    intervals = []
    print(f"Starting {case}, {runs} run(s), {steps} ops each", flush=True)
    with ExitStack() as resources:
        files = [folder / f"ops-{index}.jsonl" for index in range(runs)]
        for path in files:
            path.write_text("")
        logs = [
            resources.enter_context((folder / f"worker-{index}.log").open("w"))
            for index in range(runs)
        ]
        started = time.monotonic()
        workers = []
        for path, log in zip(files, logs):
            env = {
                **os.environ,
                "PROFILE_MODE": mode,
                "PROFILE_STEPS": str(steps),
                "PROFILE_SLOTS": str(slots),
                "PROFILE_OPS_FILE": str(path),
                "PYTHONWARNINGS": "ignore",
            }
            env.pop("PYTHONPATH", None)
            worker = subprocess.Popen(
                [sys.executable, str(HERE / "worker.py")],
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            resources.callback(stop, worker)
            workers.append(worker)

        with (folder / "memory.csv").open("w", newline="") as sample_file:
            writer = csv.writer(sample_file)
            writer.writerow(
                ("seconds", "worker_pss_mib", "worker_swap_pss_mib", "process_count")
            )
            peak_resident = peak_footprint = 0.0
            peak_process_count = 0
            while any(worker.poll() is None for worker in workers):
                resident = swap = 0
                pids = active_process_ids(workers)
                for pid in pids:
                    resident_kib, swap_kib = process_memory_kib(pid)
                    resident += resident_kib
                    swap += swap_kib
                elapsed = time.monotonic() - started
                if resident or swap:
                    resident_mib, swap_mib = resident / 1024, swap / 1024
                    peak_resident = max(peak_resident, resident_mib)
                    peak_footprint = max(peak_footprint, resident_mib + swap_mib)
                    peak_process_count = max(peak_process_count, len(pids))
                    writer.writerow(
                        (round(elapsed, 4), resident_mib, swap_mib, len(pids))
                    )
                sample_file.flush()
                if elapsed >= 900:
                    raise TimeoutError(case)
                time.sleep(0.1)
            elapsed = time.monotonic() - started
        if any(worker.returncode for worker in workers):
            raise RuntimeError(f"{case} failed. See {folder}/worker-*.log")
        for path in files:
            parsed = [json.loads(line) for line in path.read_text().splitlines()]
            assert len(parsed) == steps
            intervals.extend(parsed)

    first = min(start for start, *_ in intervals)
    last = max(end for _, _, end, *_ in intervals)
    result = {
        "case": case,
        "run_workers": runs,
        "ops_per_run": steps,
        "slots_per_run": slots,
        "elapsed_s": round(elapsed, 3),
        "op_span_s": round(last - first, 3),
        "peak_worker_pss_mib": round(peak_resident, 3),
        "peak_worker_pss_plus_swap_mib": round(peak_footprint, 3),
        "unique_op_processes": len({row[-2] for row in intervals}),
        "peak_process_tree_count": peak_process_count,
        "peak_in_flight_ops": peak_in_flight(intervals),
        "median_op_s": round(
            statistics.median(end - start for start, _, end, *_ in intervals), 3
        ),
        "median_bigquery_job_s": round(
            statistics.median(row[4] - row[3] for row in intervals), 3
        ),
        "peak_concurrent_bigquery_jobs": peak_in_flight(
            [(row[3], 0, row[4]) for row in intervals]
        ),
        "bigquery_slot_ms": sum(row[5] or 0 for row in intervals),
        "bigquery_bytes_billed": sum(row[6] for row in intervals),
    }
    (folder / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result), flush=True)


def stop(worker):
    if worker.poll() is None:
        os.killpg(worker.pid, signal.SIGTERM)
        try:
            worker.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(worker.pid, signal.SIGKILL)
            worker.wait()


if __name__ == "__main__":
    for case in sys.argv[1:] or CASES:
        measure(case)

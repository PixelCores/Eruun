"""Run five concurrent sleeps with a deterministic per-Pod duration."""

import argparse
import hashlib
import json
import socket
import threading
import time
from pathlib import Path


def worker_durations(hostname: str, minimum: int, maximum: int) -> list[float]:
    seed = int.from_bytes(hashlib.sha256(hostname.encode()).digest()[:8], "big")
    longest = minimum + seed % (maximum - minimum + 1)
    return [round(longest * fraction / 5, 3) for fraction in range(1, 6)]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    parser.add_argument("--min-seconds", type=int, default=60)
    parser.add_argument("--max-seconds", type=int, default=300)
    args = parser.parse_args()
    if args.min_seconds < 1 or args.max_seconds < args.min_seconds:
        parser.error("expected 1 <= min-seconds <= max-seconds")

    hostname = socket.gethostname()
    durations = worker_durations(hostname, args.min_seconds, args.max_seconds)
    completed = [False] * len(durations)

    def sleep_worker(index: int, duration: float) -> None:
        time.sleep(duration)
        completed[index] = True

    started = time.monotonic()
    threads = [threading.Thread(target=sleep_worker, args=(index, duration))
               for index, duration in enumerate(durations)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    elapsed = time.monotonic() - started

    result = {
        "threadCount": len(threads),
        "threadSleepSeconds": durations,
        "plannedJobSeconds": max(durations),
        "elapsedSeconds": round(elapsed, 3),
        "completedThreads": sum(completed),
        "podHash": hashlib.sha256(hostname.encode()).hexdigest()[:12],
    }
    args.output.write_text(json.dumps(result, sort_keys=True) + "\n")
    print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()

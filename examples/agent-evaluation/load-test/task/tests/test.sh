#!/bin/sh
set -eu

python3 - <<'PY'
import json
from pathlib import Path

try:
    primary = Path("/app/load-timing.json").read_bytes()
    artifact = Path("/logs/artifacts/load-timing.json").read_bytes()
    report = json.loads(primary)
    sleeps = report["threadSleepSeconds"]
    passed = (
        primary == artifact
        and report["threadCount"] == report["completedThreads"] == 5
        and len(sleeps) == len(set(sleeps)) == 5
        and 1 <= report["plannedJobSeconds"] <= 300
        and report["plannedJobSeconds"] == max(sleeps)
        and report["elapsedSeconds"] >= report["plannedJobSeconds"] - 0.1
    )
except (OSError, ValueError, KeyError, TypeError):
    passed = False

Path("/logs/verifier/reward.txt").write_text("1\n" if passed else "0\n")
PY

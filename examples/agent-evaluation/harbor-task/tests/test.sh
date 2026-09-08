#!/bin/sh
set -eu
python3 - <<'PY'
from pathlib import Path

expected = b"Hello, Eruun!\n"
paths = (Path("/app/greeting.txt"), Path("/logs/artifacts/greeting.txt"))
passed = all(path.is_file() and path.read_bytes() == expected for path in paths)
Path("/logs/verifier/reward.txt").write_text("1\n" if passed else "0\n")
PY

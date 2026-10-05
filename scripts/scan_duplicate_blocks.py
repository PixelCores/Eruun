#!/usr/bin/env python3
"""Report repeated text blocks in a frozen Git revision; candidates need review.

This is a text heuristic, not a parser: whitespace inside literals is removed,
comment-like lines may be skipped, and renamed or reordered code can be missed.
Generated files are inventoried but excluded; source, test, and configuration
categories are compared separately. Candidate counts are not a defect metric.
"""

import argparse
import collections
import hashlib
import itertools
import json
from pathlib import Path
import re
import subprocess

SOURCE_EXTENSIONS = {".go", ".py", ".sh", ".sql", ".proto"}
CONFIG_EXTENSIONS = {".yaml", ".yml", ".tpl", ".json", ".toml"}
SOURCE_NAMES = {"Dockerfile", "Makefile"}


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args])


def scan(root, ref, width, maximum):
    revision = git(root, "rev-parse", "--verify", ref + "^{commit}").decode().strip()
    names = git(root, "ls-tree", "-r", "--name-only", "-z", revision).decode().split("\0")
    files, inventory, physical = {}, collections.Counter(), collections.Counter()
    for name in names:
        path = Path(name)
        if path.suffix not in SOURCE_EXTENSIONS | CONFIG_EXTENSIONS and path.name not in SOURCE_NAMES:
            continue
        raw = git(root, "show", revision + ":" + name).decode()
        lines = raw.split("\n")
        if lines[-1] == "":
            lines.pop()
        language = path.suffix or path.name
        if re.search(r"Code generated .*DO NOT EDIT", raw[:3000]) or "/pb/" in name:
            category = "generated"
        elif name.endswith(("_test.go", "_test.sh")) or path.name.startswith("test_"):
            category = "test"
        elif path.suffix in CONFIG_EXTENSIONS:
            category = "configuration"
        else:
            category = "source"
        inventory[category + ":" + language] += 1
        physical[category + ":" + language] += len(lines)
        if category == "generated":
            continue
        useful, block = [], False
        for number, line in enumerate(lines, 1):
            stripped = line.strip()
            if block:
                if "*/" in stripped:
                    block = False
                continue
            if stripped.startswith("/*"):
                block = "*/" not in stripped
                continue
            if not stripped or stripped.startswith(("//", "#", "--")):
                continue
            if not re.search(r"[\w\"'`]", stripped):
                continue
            useful.append((re.sub(r"\s+", "", stripped), number))
        files[name] = (category, language, useful)

    index = collections.defaultdict(list)
    for name, (category, language, rows) in files.items():
        for start in range(len(rows) - width + 1):
            fingerprint = hashlib.sha256("\n".join(row[0] for row in rows[start:start + width]).encode()).digest()
            index[category, language, fingerprint].append((name, start))

    blocks, skipped = {}, 0
    for (category, language, _), locations in index.items():
        if len(locations) < 2:
            continue
        if len(locations) > maximum:
            skipped += 1
            continue
        for (a, ai), (b, bi) in itertools.combinations(locations, 2):
            if a == b and abs(ai - bi) < width:
                continue
            ar, br = files[a][2], files[b][2]
            if [row[0] for row in ar[ai:ai + width]] != [row[0] for row in br[bi:bi + width]]:
                continue
            left, right = 0, width
            while (ai > left and bi > left and (a != b or left + right < bi - ai)
                   and ar[ai - left - 1][0] == br[bi - left - 1][0]):
                left += 1
            while (ai + right < len(ar) and bi + right < len(br)
                   and (a != b or left + right < bi - ai)
                   and ar[ai + right][0] == br[bi + right][0]):
                right += 1
            blocks[a, ai - left, b, bi - left] = {
                "category": category, "language": language,
                "significant_lines": left + right,
                "a": {"file": a, "start": ar[ai - left][1], "end": ar[ai + right - 1][1]},
                "b": {"file": b, "start": br[bi - left][1], "end": br[bi + right - 1][1]},
            }
    candidates = sorted(blocks.values(), key=lambda item: (
        -item["significant_lines"], item["a"]["file"], item["a"]["start"],
        item["b"]["file"], item["b"]["start"]))
    return {
        "revision": revision, "min_significant_lines": width,
        "max_window_occurrences": maximum,
        "files": dict(sorted(inventory.items())), "physical_lines": dict(sorted(physical.items())),
        "high_frequency_windows_skipped": skipped,
        "candidate_pairs": len(candidates), "candidates": candidates,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".", help="Git checkout path")
    parser.add_argument("--ref", default="HEAD", help="Committed revision; working changes are excluded")
    parser.add_argument("--min-lines", type=int, default=8, help="Minimum lines retained by the text heuristic")
    parser.add_argument("--max-occurrences", type=int, default=20, help="Skip windows occurring more often")
    args = parser.parse_args()
    if args.min_lines < 1 or args.max_occurrences < 2:
        parser.error("--min-lines must be positive and --max-occurrences must be at least 2")
    result = scan(args.root, args.ref, args.min_lines, args.max_occurrences)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

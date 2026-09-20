"""Observe submitted Harbor Jobs through terminal execution and result delivery."""

import argparse
import heapq
import http.client
import json
import math
import os
import queue
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path


SUCCESS = {"completed", "passed"}
FAILED = {"failed", "timeout", "cancelled", "reject", "notRun", "skipped"}
ACTIVE = {"created", "running", "pause", "waiting", "queued", "blocked", "pending",
          "prepare", "distributed", "wait_for_approval", "disabled", "changed"}
COLLECTION = {"pending", "collected", "incomplete", "unavailable", "expired"}
DELIVERY = {"pending", "running", "succeeded", "failed"}
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,254}\Z")


def load_submissions(path):
    """Keep only accepted IDs and timestamps, never arbitrary submission fields."""
    jobs = {}
    counts = Counter(records=0, acceptedRecords=0, duplicateTaskIds=0, unacceptedRecords=0)
    with path.open(encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            try:
                value = json.loads(line)
                if not isinstance(value, dict) or type(value.get("accepted")) is not bool:
                    raise ValueError
                counts["records"] += 1
                if not value["accepted"]:
                    counts["unacceptedRecords"] += 1
                    continue
                task_id, submitted = value.get("taskId"), value.get("completedAt")
                if (value.get("httpStatus") != 202 or type(value.get("businessCode")) is not int
                        or value["businessCode"] != 0 or not isinstance(task_id, str)
                        or not ID.fullmatch(task_id) or type(submitted) not in (int, float)
                        or not math.isfinite(submitted) or submitted <= 0):
                    raise ValueError
                counts["acceptedRecords"] += 1
                if task_id in jobs:
                    counts["duplicateTaskIds"] += 1
                    jobs[task_id] = min(jobs[task_id], submitted)
                else:
                    jobs[task_id] = submitted
            except (ValueError, TypeError):
                raise ValueError(f"invalid submission record at line {line_number}") from None
    if not jobs:
        raise ValueError("input contains no accepted task IDs")
    return jobs, dict(counts)


def snapshot(data, task_id):
    """Project the actual Job API onto a small allowlist, validating success inputs."""
    if not isinstance(data, dict) or data.get("taskId") != task_id or data.get("type") != "job":
        raise ValueError("invalid job detail")
    status, collection = data.get("status"), data.get("collectionState")
    if not isinstance(status, str) or status not in SUCCESS | FAILED | ACTIVE:
        raise ValueError("invalid job status")
    if not isinstance(collection, str) or collection not in COLLECTION:
        raise ValueError("invalid collection state")
    result = {"status": status, "collectionState": collection}
    # Failed executions can legitimately have no result, policy snapshot, or Runner.
    if status in FAILED:
        return result
    try:
        selected = data["job"]["traits"]["eval"]["resultPolicy"]["targets"]
        if not isinstance(selected, list) or not 1 <= len(selected) <= 2:
            raise ValueError
        targets = {}
        for target in selected:
            name, mode = target["type"], target["mode"]
            if (name not in {"minio", "database"} or name in targets
                    or mode not in ({"full"} if name == "minio" else {"full", "metadata"})):
                raise ValueError
            targets[name] = mode
        if targets.get("database") == "metadata" and "minio" not in targets:
            raise ValueError
        rows = data["deliveries"]
        if rows is None:
            rows = []
        if not isinstance(rows, list):
            raise ValueError
        deliveries = {}
        for row in rows:
            name, state = row["target"], row["state"]
            if (name not in targets or name in deliveries or state not in DELIVERY
                    or row["mode"] != targets[name]):
                raise ValueError
            deliveries[name] = state
        result["deliveries"] = {name: deliveries.get(name, "missing") for name in sorted(targets)}
    except (KeyError, TypeError, ValueError):
        raise ValueError("invalid result policy or deliveries") from None
    runner = data.get("runnerStatus")
    if isinstance(runner, dict):
        phase = runner.get("phase")
        if isinstance(phase, str) and phase in {"preparing", "running", "finalizing"}:
            result["runnerPhase"] = phase
        if type(runner.get("stale")) is bool:
            result["runnerStale"] = runner["stale"]
        terminal = runner.get("terminal")
        if isinstance(terminal, dict):
            outcome = terminal.get("outcome")
            if isinstance(outcome, str) and outcome in {"succeeded", "failed", "cancelled", "timed_out"}:
                result["runnerOutcome"] = outcome
    return result


def outcome(state):
    if state["status"] in FAILED:
        return "failed", "execution_failed"
    if state["status"] not in SUCCESS:
        return None
    if state.get("runnerOutcome") in {"failed", "cancelled", "timed_out"}:
        return "failed", "runner_failed"
    if state["collectionState"] in {"incomplete", "unavailable", "expired"}:
        return "failed", "collection_" + state["collectionState"]
    if "failed" in state["deliveries"].values():
        return "failed", "delivery_failed"
    if state["collectionState"] == "collected" and all(
            value == "succeeded" for value in state["deliveries"].values()):
        return "succeeded", "collected_and_delivered"
    return None


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, new_url):
        return None


def fetch_job(api_url, token, workspace_id, task_id, timeout):
    request = urllib.request.Request(
        api_url.rstrip("/") + "/api/v1/jobs/" + urllib.parse.quote(task_id, safe=""),
        headers={"Authorization": "Bearer " + token, "X-Eruun-Workspace-ID": workspace_id},
    )
    try:
        # Never forward credentials through a redirect; HTTP status/body messages
        # and private result/manifest contents are deliberately not returned.
        with urllib.request.build_opener(NoRedirect).open(request, timeout=timeout) as response:
            if response.status != 200:
                return None, "http_" + str(response.status)
            raw = response.read(MAX_RESPONSE_BYTES + 1)
            if len(raw) > MAX_RESPONSE_BYTES:
                return None, "response_too_large"
            envelope = json.loads(raw)
            if (not isinstance(envelope, dict) or type(envelope.get("code")) is not int
                    or envelope["code"] != 0):
                return None, "api_business_error"
            return snapshot(envelope.get("data"), task_id), None
    except urllib.error.HTTPError as error:
        return None, "http_" + str(error.code)
    except TimeoutError:
        return None, "request_timeout"
    except urllib.error.URLError as error:
        return None, "request_timeout" if isinstance(error.reason, TimeoutError) else "request_failed"
    except (OSError, http.client.HTTPException):
        return None, "request_failed"
    except (ValueError, TypeError):
        return None, "invalid_response"


def observe(jobs, fetch, emit, *, rate=10, workers=8, interval=30, deadline=1800, timeout=15):
    """Bound queued/in-flight requests and stop reporting at the overall deadline."""
    started_at, started = time.time(), time.monotonic()
    expires = started + deadline
    stop = threading.Event()
    requests, results = queue.Queue(maxsize=workers), queue.Queue(maxsize=workers * 2)
    rate_lock = threading.Lock()
    next_request = started
    records = {task_id: {"taskId": task_id, "submittedAt": submitted, "samples": 0,
                         "requests": 0, "apiErrors": 0, "maxSampleGapSeconds": 0}
               for task_id, submitted in jobs.items()}

    def worker():
        nonlocal next_request
        while not stop.is_set():
            try:
                task_id = requests.get(timeout=0.05)
            except queue.Empty:
                continue
            with rate_lock:
                wait = max(0, min(next_request, expires) - time.monotonic())
                if stop.wait(wait) or time.monotonic() >= expires:
                    return
                began = time.monotonic()
                next_request = began + 1 / rate
                results.put(("started", task_id, time.time(), began, None, None))
            try:
                state, error = fetch(task_id, min(timeout, expires - began))
            except Exception:
                # Unexpected transport errors must not leak URLs, tokens or bodies.
                state, error = None, "request_failed"
            if not stop.is_set():
                results.put(("finished", task_id, time.time(), time.monotonic(), state, error))

    for _ in range(workers):
        # A DNS/socket implementation can outlive its timeout. Daemon workers let
        # the CLI honor the overall deadline without waiting for such requests.
        threading.Thread(target=worker, daemon=True).start()
    pending = [(started, index, task_id) for index, task_id in enumerate(jobs)]
    heapq.heapify(pending)
    sequence, in_flight, active, peak = len(pending), 0, 0, 0
    last_samples, last_states, finished = {}, {}, set()
    errors = Counter()
    emit({"kind": "observation", "startedAt": started_at, "uniqueTaskIds": len(jobs),
          "rateLimitPerSecond": rate, "workers": workers, "pollIntervalSeconds": interval,
          "deadlineSeconds": deadline, "requestTimeoutSeconds": timeout})
    try:
        while len(finished) < len(jobs) and time.monotonic() < expires:
            now = time.monotonic()
            while pending and pending[0][0] <= now and in_flight < workers:
                _, _, task_id = heapq.heappop(pending)
                requests.put_nowait(task_id)
                in_flight += 1
            wait = expires - time.monotonic()
            if pending and in_flight < workers:
                wait = min(wait, max(0, pending[0][0] - time.monotonic()))
            try:
                event, task_id, at, monotonic_at, state, error = results.get(timeout=max(0, wait))
            except queue.Empty:
                continue
            record = records[task_id]
            if event == "started":
                record["requests"] += 1
                record["firstRequestAt"] = record.get("firstRequestAt", at)
                record["lastRequestAt"] = at
                active += 1
                peak = max(peak, active)
                continue
            active -= 1
            in_flight -= 1
            if error:
                record["apiErrors"] += 1
                record["lastAPIError"] = error
                errors[error] += 1
                emit({"kind": "api_error", "taskId": task_id, "observedAt": at, "error": error})
            else:
                record["samples"] += 1
                record["firstObservedAt"] = record.get("firstObservedAt", at)
                record["lastObservedAt"] = at
                if task_id in last_samples:
                    record["maxSampleGapSeconds"] = max(
                        record["maxSampleGapSeconds"], monotonic_at - last_samples[task_id])
                last_samples[task_id] = monotonic_at
                if state != last_states.get(task_id):
                    emit({"kind": "state", "taskId": task_id, "observedAt": at, **state})
                    last_states[task_id] = state
                if state["status"] in SUCCESS | FAILED:
                    record.setdefault("terminalObservedAt", at)
                if state["collectionState"] == "collected":
                    record.setdefault("collectedObservedAt", at)
                if state.get("deliveries") and all(v == "succeeded" for v in state["deliveries"].values()):
                    record.setdefault("deliveredObservedAt", at)
                result = outcome(state)
                if result:
                    record["outcome"], record["reason"] = result
                    record["finishedAt"] = at
                    finished.add(task_id)
                    continue
            sequence += 1
            heapq.heappush(pending, (monotonic_at + interval, sequence, task_id))
    finally:
        stop.set()
        # Ensure no further request can pass the start gate before counting the
        # final start events. Late response bodies never become success evidence.
        with rate_lock:
            pass

    while True:
        try:
            event, task_id, at, _, _, _ = results.get_nowait()
        except queue.Empty:
            break
        if event == "started":
            record = records[task_id]
            record["requests"] += 1
            record["firstRequestAt"] = record.get("firstRequestAt", at)
            record["lastRequestAt"] = at
            active += 1
            peak = max(peak, active)
        else:
            active -= 1

    ended = time.monotonic()
    totals = Counter(succeeded=0, failed=0, unknown=0)
    for task_id, record in records.items():
        if task_id not in finished:
            record.update(outcome="unknown", reason="deadline", finishedAt=time.time())
        if task_id in last_samples:
            record["lastSampleAgeSeconds"] = ended - last_samples[task_id]
        totals[record["outcome"]] += 1
        emit({"kind": "task_summary", **record})
    summary = {"kind": "summary", "startedAt": started_at, "finishedAt": time.time(),
               "elapsedSeconds": ended - started, "uniqueTaskIds": len(jobs), **totals,
               "observedTaskIds": sum(record["samples"] > 0 for record in records.values()),
               "requests": sum(record["requests"] for record in records.values()),
               "successfulSamples": sum(record["samples"] for record in records.values()),
               "apiErrors": dict(errors), "peakInFlightRequests": peak,
               "maxSampleGapSeconds": max(record["maxSampleGapSeconds"] for record in records.values())}
    emit(summary)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path, help="completed submit.py JSONL")
    parser.add_argument("--output", required=True, type=Path, help="new observation JSONL file")
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--rate", type=float, default=10, help="global GET requests per second")
    parser.add_argument("--workers", type=int, default=8, help="maximum queued/in-flight requests")
    parser.add_argument("--interval", type=float, default=30, help="minimum seconds between a Job's requests")
    parser.add_argument("--deadline", type=float, default=1800, help="overall observation duration in seconds")
    parser.add_argument("--timeout", type=float, default=15, help="per-request socket timeout in seconds")
    args = parser.parse_args()
    if args.workers < 1 or any(not math.isfinite(value) or value <= 0 for value in
                              (args.rate, args.interval, args.deadline, args.timeout)):
        parser.error("workers, rate, interval, deadline and timeout must be finite and positive")
    try:
        parsed_url = urllib.parse.urlsplit(args.api_url)
        parsed_url.port
    except ValueError:
        parser.error("invalid api-url")
    if (parsed_url.scheme not in {"http", "https"} or not parsed_url.hostname
            or parsed_url.username or parsed_url.password or parsed_url.query or parsed_url.fragment):
        parser.error("api-url must be an HTTP(S) base URL without credentials, query or fragment")
    token = os.environ.get("ERUUN_TOKEN")
    if not token:
        parser.error("set ERUUN_TOKEN in the environment")
    try:
        jobs, submissions = load_submissions(args.input)
        descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except ValueError as error:
        parser.error(str(error))
    except (OSError, UnicodeError):
        parser.error("cannot read input or create a new output file")
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        def emit(record):
            output.write(json.dumps(record, sort_keys=True) + "\n")
            output.flush()

        emit({"kind": "submissions", **submissions, "uniqueTaskIds": len(jobs)})
        summary = observe(jobs, lambda task_id, timeout: fetch_job(
            args.api_url, token, args.workspace_id, task_id, timeout), emit,
            rate=args.rate, workers=args.workers, interval=args.interval,
            deadline=args.deadline, timeout=args.timeout)
    print(json.dumps(summary, sort_keys=True))
    return int(summary["failed"] > 0 or summary["unknown"] > 0 or submissions["unacceptedRecords"] > 0)


if __name__ == "__main__":
    raise SystemExit(main())

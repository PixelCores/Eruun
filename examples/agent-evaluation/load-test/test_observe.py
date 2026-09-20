import contextlib
import copy
import io
import json
import os
import stat
import tempfile
import threading
import time
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

import observe


def detail(task_id="job-1", status="completed", collection="collected", deliveries=None):
    return {
        "taskId": task_id, "type": "job", "status": status, "collectionState": collection,
        "job": {"traits": {"eval": {"resultPolicy": {
            "targets": [{"type": "database", "mode": "full"}]}}}},
        "deliveries": ([{"target": "database", "mode": "full", "state": "succeeded"}]
                       if deliveries is None else deliveries),
    }


def accepted(task_id="job-1"):
    return {"accepted": True, "httpStatus": 202, "businessCode": 0,
            "taskId": task_id, "completedAt": 1000}


class ObserverTests(unittest.TestCase):
    def observe(self, jobs, fetch, **options):
        records = []
        config = dict(rate=1000, workers=2, interval=0.001, deadline=1, timeout=0.05)
        config.update(options)
        summary = observe.observe(jobs, fetch, records.append, **config)
        return summary, records

    def test_waits_for_terminal_collection_and_all_selected_targets(self):
        initial = detail(status="running", collection="pending", deliveries=[])
        collected = detail(status="running", deliveries=[
            {"target": "database", "mode": "full", "state": "pending"}])
        saving = detail(deliveries=[{"target": "database", "mode": "full", "state": "running"}])
        states = [initial, collected, saving, detail()]
        summary, records = self.observe({"job-1": 1}, lambda task, timeout: (
            observe.snapshot(states.pop(0), task), None))
        self.assertEqual((summary["succeeded"], summary["successfulSamples"]), (1, 4))
        task = next(row for row in records if row["kind"] == "task_summary")
        self.assertLess(task["collectedObservedAt"], task["terminalObservedAt"])
        self.assertLessEqual(task["terminalObservedAt"], task["deliveredObservedAt"])
        self.assertGreater(task["maxSampleGapSeconds"], 0)

    def test_failed_execution_never_waits_for_unavailable_collection(self):
        for status in observe.FAILED:
            with self.subTest(status=status):
                failed = detail(status=status, collection="unavailable")
                del failed["job"]
                summary, _ = self.observe({"job-1": 1}, lambda task, timeout: (
                    observe.snapshot(failed, task), None))
                self.assertEqual((summary["failed"], summary["requests"]), (1, 1))

    def test_collection_and_delivery_failures_remain_distinct(self):
        for collection in ("incomplete", "unavailable", "expired"):
            state = observe.snapshot(detail(collection=collection), "job-1")
            self.assertEqual(observe.outcome(state), ("failed", "collection_" + collection))
        state = observe.snapshot(detail(deliveries=[
            {"target": "database", "mode": "full", "state": "failed"}]), "job-1")
        self.assertEqual(observe.outcome(state), ("failed", "delivery_failed"))

    def test_multiple_targets_and_missing_targets_cannot_succeed(self):
        value = detail()
        value["job"]["traits"]["eval"]["resultPolicy"]["targets"] = [
            {"type": "minio", "mode": "full"}, {"type": "database", "mode": "metadata"}]
        value["deliveries"][0]["mode"] = "metadata"
        self.assertIsNone(observe.outcome(observe.snapshot(value, "job-1")))
        value["deliveries"].append({"target": "minio", "mode": "full", "state": "succeeded"})
        self.assertEqual(observe.outcome(observe.snapshot(value, "job-1"))[0], "succeeded")
        value["deliveries"].append(copy.deepcopy(value["deliveries"][0]))
        with self.assertRaises(ValueError):
            observe.snapshot(value, "job-1")

    def test_mixed_phases_and_transient_errors(self):
        sequences = {
            "job-1": ["http_503", "request_failed", "running", "completed"],
            "job-2": ["failed"],
            "job-3": ["running", "completed"],
        }

        def fetch(task_id, timeout):
            status = sequences[task_id].pop(0)
            if status in {"http_503", "request_failed"}:
                return None, status
            value = detail(task_id, status=status)
            value["runnerStatus"] = {"phase": "preparing" if task_id == "job-1" else "finalizing", "stale": False}
            return observe.snapshot(value, task_id), None

        summary, records = self.observe(dict.fromkeys(sequences, 1), fetch)
        self.assertEqual((summary["succeeded"], summary["failed"], summary["unknown"]), (2, 1, 0))
        self.assertEqual(summary["observedTaskIds"], 3)
        self.assertEqual(summary["apiErrors"], {"http_503": 1, "request_failed": 1})
        self.assertEqual({row.get("runnerPhase") for row in records if row["kind"] == "state"}
                         - {None}, {"preparing", "finalizing"})

    def test_deadline_marks_unobserved_and_unfinished_unknown(self):
        summary, records = self.observe({"job-1": 1, "job-2": 1},
                                        lambda task, timeout: (None, "request_failed"),
                                        rate=1, deadline=0.025)
        self.assertEqual(summary["unknown"], 2)
        self.assertEqual(summary["observedTaskIds"], 0)
        self.assertEqual(summary["requests"], 1)
        self.assertTrue(all(row["reason"] == "deadline" for row in records if row["kind"] == "task_summary"))

    def test_overall_deadline_does_not_join_stuck_requests(self):
        release = threading.Event()
        budgets = []

        def fetch(task_id, timeout):
            budgets.append(timeout)
            release.wait(1)
            return observe.snapshot(detail(task_id), task_id), None

        try:
            began = time.monotonic()
            summary, _ = self.observe({"job-1": 1}, fetch, deadline=0.035, timeout=0.02)
            self.assertLess(time.monotonic() - began, 0.25)
            self.assertEqual(summary["unknown"], 1)
            self.assertTrue(0 < budgets[0] <= 0.02)
        finally:
            release.set()

    def test_global_rate_and_inflight_are_bounded(self):
        lock = threading.Lock()
        starts, active, peak = [], 0, 0

        def fetch(task_id, timeout):
            nonlocal active, peak
            with lock:
                starts.append(time.monotonic())
                active += 1
                peak = max(peak, active)
            time.sleep(0.025)
            with lock:
                active -= 1
            return observe.snapshot(detail(task_id), task_id), None

        summary, _ = self.observe({f"job-{index}": 1 for index in range(8)}, fetch,
                                  workers=2, rate=100)
        self.assertEqual(summary["succeeded"], 8)
        self.assertLessEqual(peak, 2)
        self.assertEqual(summary["peakInFlightRequests"], peak)
        self.assertTrue(all(b - a >= 0.008 for a, b in zip(starts, starts[1:])))

    def test_snapshot_drops_private_fields(self):
        value = detail()
        value.update(results=[{"manifest": "private-result", "summary": {"secret": "private-result"}}],
                     executions=[{"token": "private-token"}], executionKey="private-execution-key")
        value["deliveries"][0].update(reference="private-location", error="private-error")
        value["runnerStatus"] = {"phase": "running", "stale": False,
                                 "terminal": {"outcome": "succeeded", "message": "private-message"}}
        summary, records = self.observe({"job-1": 1}, lambda task, timeout: (
            observe.snapshot(value, task), None))
        self.assertEqual(summary["succeeded"], 1)
        self.assertNotIn("private-", json.dumps(records))

    def test_invalid_response_fields_cannot_succeed(self):
        for field, value in (("taskId", "different"), ("type", "command"),
                             ("status", "secret-status"), ("collectionState", {}),
                             ("job", {}), ("deliveries", "secret-delivery")):
            with self.subTest(field=field):
                response = detail()
                response[field] = value
                with self.assertRaises(ValueError):
                    observe.snapshot(response, "job-1")


class IOTests(unittest.TestCase):
    def test_input_deduplicates_and_counts_unaccepted(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.jsonl"
            path.write_text("\n".join(json.dumps(row) for row in
                                      [accepted(), accepted(), {"accepted": False, "error": "secret"}]))
            jobs, counts = observe.load_submissions(path)
            self.assertEqual(jobs, {"job-1": 1000})
            self.assertEqual(counts, dict(records=3, acceptedRecords=2, duplicateTaskIds=1, unacceptedRecords=1))

    def test_bad_input_does_not_echo_contents(self):
        bad = ["{private-json", "null", "[]", "{}", json.dumps({"accepted": False}),
               json.dumps({**accepted(), "taskId": "secret/token"}),
               json.dumps({**accepted(), "completedAt": float("nan")}),
               json.dumps({**accepted(), "businessCode": False})]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.jsonl"
            for row in bad:
                with self.subTest(row=row):
                    path.write_text(row)
                    with self.assertRaises(ValueError) as raised:
                        observe.load_submissions(path)
                    self.assertNotIn("private", str(raised.exception))
                    self.assertNotIn("secret", str(raised.exception))

    def test_http_and_timeout_errors_are_sanitized(self):
        for error, expected in ((TimeoutError("private-token"), "request_timeout"),
                                (urllib.error.URLError("private-token"), "request_failed"),
                                (urllib.error.HTTPError("private-url", 503, "private-body", {}, None), "http_503")):
            with self.subTest(error=type(error).__name__), mock.patch("observe.urllib.request.build_opener") as opener:
                opener.return_value.open.side_effect = error
                self.assertEqual(observe.fetch_job("http://localhost", "private-token", "workspace", "job-1", 1),
                                 (None, expected))

    def test_body_is_bounded_and_envelope_validated(self):
        cases = [(b"{", "invalid_response"), (b'{"code":false}', "api_business_error"),
                 (b'{"code":1,"message":"private-error"}', "api_business_error"),
                 (b"x" * (observe.MAX_RESPONSE_BYTES + 1), "response_too_large")]
        for body, error in cases:
            with self.subTest(error=error), mock.patch("observe.urllib.request.build_opener") as opener:
                response = opener.return_value.open.return_value.__enter__.return_value
                response.status = 200
                response.read.return_value = body
                self.assertEqual(observe.fetch_job("http://localhost", "secret", "workspace", "job-1", 1), (None, error))
                response.read.assert_called_once_with(observe.MAX_RESPONSE_BYTES + 1)

    def test_cli_private_file_no_overwrite_and_exit_status(self):
        with tempfile.TemporaryDirectory() as directory:
            source, output = Path(directory) / "input.jsonl", Path(directory) / "output.jsonl"
            source.write_text(json.dumps(accepted()))
            arguments = ["observe.py", "--input", str(source), "--output", str(output),
                         "--api-url", "http://localhost", "--workspace-id", "workspace"]
            with mock.patch.dict(os.environ, ERUUN_TOKEN="private-token"), mock.patch("sys.argv", arguments), \
                    mock.patch("observe.fetch_job", return_value=(observe.snapshot(detail(), "job-1"), None)) as fetch, \
                    contextlib.redirect_stdout(io.StringIO()) as stdout:
                self.assertEqual(observe.main(), 0)
                self.assertEqual(stat.S_IMODE(output.stat().st_mode), 0o600)
                self.assertNotIn("private", output.read_text() + stdout.getvalue())
                original = output.read_bytes()
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
                    observe.main()
                self.assertEqual(raised.exception.code, 2)
                self.assertEqual(output.read_bytes(), original)
                fetch.assert_called_once()


if __name__ == "__main__":
    unittest.main()

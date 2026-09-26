import asyncio
from contextlib import contextmanager, redirect_stdout
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import io
import json
import logging
import os
from pathlib import Path
from types import SimpleNamespace
import shutil
import subprocess
import tarfile
import tempfile
import threading
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import runner


EXAMPLE = Path(__file__).resolve().parents[5] / "examples/agent-evaluation/harbor-task"


def config():
    return {
        "taskId": "task-test", "namespace": "workspace-test",
        "datasetURL": "http://platform.test/input", "resultURL": "http://platform.test/result",
        "eventURL": "http://platform.test/events",
        "token": "private-transfer-token", "datasetDigest": "a" * 64,
        "agent": {"name": "oracle"}, "options": {"attempts": 1, "concurrency": 1},
        "resources": {"cpu": "1", "memory": "1Gi", "cpuLimit": "2", "memoryLimit": "2Gi"},
        "sandboxServiceAccount": "task-sandbox", "timeoutSeconds": 60,
    }


def result():
    return {"n_total_trials": 1, "finished_at": "2026-09-08T00:00:00Z", "stats": {
        "n_completed_trials": 1, "n_errored_trials": 0, "n_pending_trials": 0,
        "n_running_trials": 0, "n_cancelled_trials": 0,
        "evals": {"oracle__uploaded": {"metrics": [{"mean": 0.0}]}},
    }}


def collected_trial(output, name="trial", **changes):
    trial = output / "run" / name
    (trial / "artifacts").mkdir(parents=True, exist_ok=True)
    (trial / "artifacts/manifest.json").write_text(json.dumps([
        {"source": "/logs/artifacts", "destination": "artifacts/logs/artifacts", "type": "directory", "status": "ok"}]))
    states = output.parent / "collection"
    states.mkdir(exist_ok=True)
    (states / (name + ".json")).write_text(json.dumps({
        "podName": name, "namespace": "workspace-test", "trialDirectory": str(trial),
        "started": True, "stopped": True, "pending": 0, "errorCount": 0, "errors": [], **changes}))


@contextmanager
def local_http_server(handler):
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    server.daemon_threads = False
    server.stop = threading.Event()
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
    thread.start()
    try:
        yield server
    finally:
        server.stop.set()
        server.shutdown()
        thread.join()
        server.server_close()


class RunnerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        self.environment = patch.dict(os.environ, {"POD_NAME": "runner-pod", "POD_UID": "runner-uid"})
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def package(self, members):
        path = self.root / "package.tar.gz"
        with tarfile.open(path, "w:gz") as archive:
            for name, contents, kind in members:
                info = tarfile.TarInfo(name)
                info.type = kind
                info.size = len(contents) if kind == tarfile.REGTYPE else 0
                if kind in (tarfile.SYMTYPE, tarfile.LNKTYPE):
                    info.linkname = contents.decode()
                archive.addfile(info, io.BytesIO(contents) if kind == tarfile.REGTYPE else None)
        return path

    def example_package(self):
        path = self.root / "package.tar.gz"
        with tarfile.open(path, "w:gz") as archive:
            archive.add(EXAMPLE, arcname="nested/greeting")
        return path

    def test_strict_public_options_and_agent(self):
        self.assertEqual(runner.validate_config(config())["agent"]["name"], "oracle")
        for edit in (
            {"agent": {"name": "module:UserAgent"}},
            {"agent": {"name": "codex"}},
            {"agent": {"name": "oracle", "import_path": "module:UserAgent"}},
            {"options": {"namespace": "other-workspace"}},
            {"options": {"attempts": True}},
            {"options": {"concurrency": 33}},
            {"datasetDigest": "not-a-digest"},
            {"resultURL": "http://user:password@platform.test/result"},
            {"token": "value\r\nInjected: header"},
        ):
            with self.subTest(edit=edit), self.assertRaises(runner.RunnerError):
                runner.validate_config(config() | edit)

    def test_execution_and_finalization_budget_boundaries(self):
        for seconds in (86400, 86401, 14 * 24 * 60 * 60):
            with self.subTest(seconds=seconds):
                self.assertEqual(runner.validate_config(config() | {"timeoutSeconds": seconds})["timeoutSeconds"], seconds)
        for seconds in (0, True, 14 * 24 * 60 * 60 + 1):
            with self.subTest(seconds=seconds), self.assertRaises(runner.RunnerError):
                runner.validate_config(config() | {"timeoutSeconds": seconds})
        for seconds in (360, 960):
            with self.subTest(finalization=seconds):
                self.assertEqual(runner.validate_config(config() | {"finalizationTimeoutSeconds": seconds})["finalizationTimeoutSeconds"], seconds)
        self.assertEqual(runner.FINALIZATION_SECONDS, 960)

    def test_archive_rejects_path_escape_links_duplicates_and_expansion(self):
        cases = [
            [("../outside", b"x", tarfile.REGTYPE)],
            [("/outside", b"x", tarfile.REGTYPE)],
            [("a\\b", b"x", tarfile.REGTYPE)],
            [("link", b"../outside", tarfile.SYMTYPE)],
            [("hardlink", b"a", tarfile.LNKTYPE)],
            [("a", b"x", tarfile.REGTYPE), ("./a", b"y", tarfile.REGTYPE)],
            [("pipe", b"", tarfile.FIFOTYPE)],
        ]
        for index, members in enumerate(cases):
            with self.subTest(index=index), self.assertRaises(runner.RunnerError):
                runner.extract_package(self.package(members), self.root / str(index))
        with patch.object(runner, "MAX_EXPANDED_BYTES", 1), self.assertRaises(runner.RunnerError):
            runner.extract_package(self.package([("file", b"too big", tarfile.REGTYPE)]), self.root / "large")

    def test_native_package_root_nested_and_prebuilt_validation(self):
        source = self.example_package()
        extracted = self.root / "dataset"
        runner.extract_package(source, extracted)
        tasks = runner.native_tasks(extracted)
        self.assertEqual(tasks, [extracted / "nested/greeting"])
        self.assertEqual(runner.native_tasks(tasks[0]), tasks)
        task_file = tasks[0] / "task.toml"
        content = task_file.read_text()
        for changed in (content.replace(":1.0.0", ":latest"), content.replace(":1.0.0", ""), content + "\nuser = 'root'\n"):
            task_file.write_text(changed)
            with self.assertRaises(runner.RunnerError):
                runner.native_tasks(extracted)
        task_file.write_text(content)
        (tasks[0] / "environment/docker-compose.yaml").write_text("services: {}")
        with self.assertRaises(runner.RunnerError):
            runner.native_tasks(extracted)

    def test_download_verifies_digest_and_sends_pod_identity(self):
        payload = b"native bundle"
        cfg = config() | {"datasetDigest": hashlib.sha256(payload).hexdigest()}
        response = MagicMock()
        response.status = 200
        response.read1.side_effect = [payload, b""]
        connection = MagicMock()
        connection.getresponse.return_value = response
        with patch.object(runner.http.client, "HTTPConnection", return_value=connection):
            destination = self.root / "download"
            runner.download_package(cfg, destination)
        self.assertEqual(destination.read_bytes(), payload)
        headers = connection.request.call_args.kwargs["headers"]
        self.assertEqual(headers["Authorization"], "Bearer private-transfer-token")
        self.assertEqual(headers["X-Eruun-Runner-Pod-UID"], "runner-uid")
        response.read1.side_effect = [payload, b""]
        with patch.object(runner.http.client, "HTTPConnection", return_value=connection), self.assertRaisesRegex(runner.RunnerError, "digest"):
            runner.download_package(config(), destination)

    def test_download_retries_temporary_failures_and_replaces_partial_content(self):
        payload = b"complete native bundle"
        cfg = config() | {"datasetDigest": hashlib.sha256(payload).hexdigest()}

        def success():
            response = MagicMock(status=200)
            response.read1.side_effect = [payload, b""]
            connection = MagicMock()
            connection.getresponse.return_value = response
            return connection

        for failure in (408, 429, 503, "connection"):
            with self.subTest(failure=failure):
                destination = self.root / f"download-{failure}"
                destination.write_bytes(b"stale content")
                first = MagicMock()
                if failure == "connection":
                    response = MagicMock(status=200)
                    response.read1.side_effect = [b"partial", ConnectionResetError("connection reset")]
                    first.getresponse.return_value = response
                else:
                    first.getresponse.return_value = MagicMock(status=failure)
                second = success()
                with patch.object(runner.http.client, "HTTPConnection", side_effect=[first, second]), \
                        patch.object(runner.time, "sleep") as sleep, redirect_stdout(io.StringIO()):
                    runner.download_package(cfg, destination)
                self.assertEqual(destination.read_bytes(), payload)
                sleep.assert_called_once()
                first.close.assert_called_once()
                second.close.assert_called_once()

        rejected = MagicMock()
        rejected.getresponse.return_value = MagicMock(status=401)
        with patch.object(runner.http.client, "HTTPConnection", return_value=rejected), \
                patch.object(runner.time, "sleep") as sleep, \
                self.assertRaisesRegex(runner.RunnerError, "HTTP 401"):
            runner.download_package(cfg, self.root / "rejected")
        sleep.assert_not_called()

    def test_download_retries_share_one_total_time_budget(self):
        deadlines = []

        def connect(_url, deadline):
            deadlines.append(deadline)
            connection = MagicMock()
            connection.request.side_effect = ConnectionResetError("connection reset")
            return connection, "/input"

        with patch.object(runner.time, "monotonic", side_effect=[10, 10, 11, 12, 311]), \
                patch.object(runner.time, "sleep"), \
                patch.object(runner, "transfer_connection", side_effect=connect), \
                redirect_stdout(io.StringIO()), self.assertRaises(TimeoutError):
            runner.download_package(config(), self.root / "download-timeout")
        self.assertEqual(deadlines, [310, 310])

    def test_zero_reward_is_execution_success_but_framework_errors_fail(self):
        path = self.root / "result.json"
        path.write_text(json.dumps(result()))
        self.assertEqual(runner.framework_status(path, 0), "succeeded")
        self.assertEqual(runner.framework_status(path, 1), "failed")
        variants = []
        for key in ("n_errored_trials", "n_pending_trials", "n_running_trials", "n_cancelled_trials"):
            value = result()
            value["stats"][key] = 1
            variants.append(value)
        variants.extend([result() | {"finished_at": None}, result() | {"n_total_trials": 0}, result() | {"n_total_trials": True}, {}])
        for value in variants:
            with self.subTest(result=value):
                path.write_text(json.dumps(value))
                self.assertEqual(runner.framework_status(path, 0), "failed")
        path.write_text("broken json")
        self.assertEqual(runner.framework_status(path, 0), "failed")

    def test_raw_archive_keeps_binary_hidden_files_and_safe_links(self):
        output = self.root / "outputs"
        output.mkdir()
        (output / "trial").mkdir()
        original = b"\x00\xff\x10raw file\n"
        (output / "trial/artifact.bin").write_bytes(original)
        (output / ".hidden").write_bytes(b"hidden")
        (output / "trial/link").symlink_to("artifact.bin")
        (output / "trial/chained").symlink_to("link")
        os.link(output / "trial/artifact.bin", output / "trial/hardlinked")
        (output / "unsafe").symlink_to("/etc/passwd")
        archive_path = self.root / "result.tar.gz"
        report = {"executionStatus": "succeeded"}
        runner.archive_results(output, archive_path, report)
        with tarfile.open(archive_path) as archive:
            self.assertEqual(archive.extractfile("outputs/trial/artifact.bin").read(), original)
            self.assertEqual(archive.extractfile("outputs/.hidden").read(), b"hidden")
            self.assertTrue(archive.getmember("outputs/trial/link").issym())
            self.assertTrue(archive.getmember("outputs/trial/hardlinked").isfile())
            self.assertEqual(archive.extractfile("outputs/trial/hardlinked").read(), original)
            self.assertNotIn("outputs/trial/chained", archive.getnames())
            self.assertNotIn("outputs/unsafe", archive.getnames())
            saved = json.load(archive.extractfile("result.json"))
            self.assertFalse(saved["collectionComplete"])
            self.assertEqual({issue["path"] for issue in saved["collectionErrors"]}, {"unsafe", "trial/chained"})

    def test_upload_streams_archive_and_requires_durable_ack(self):
        source = self.root / "result.tar.gz"
        source.write_bytes(b"complete raw archive")
        connection = MagicMock()
        connection.getresponse.return_value.status = 201
        connection.getresponse.return_value.read.return_value = json.dumps({"data": {"id": "a" * 64, "digest": "b" * 64}}).encode()
        with patch.object(runner.http.client, "HTTPConnection", return_value=connection):
            artifact = runner.upload_results(config(), source, "failed")
        self.assertEqual(artifact["id"], "a" * 64)
        connection.putrequest.assert_called_once_with("POST", "/result")
        connection.putheader.assert_any_call("X-Eruun-Runner-Pod-UID", "runner-uid")
        connection.putheader.assert_any_call("X-Eruun-Evaluation-Status", "failed")
        connection.send.assert_called_once_with(b"complete raw archive")
        connection.getresponse.return_value.status = 409
        with patch.object(runner.http.client, "HTTPConnection", return_value=connection), self.assertRaises(runner.RunnerError):
            runner.upload_results(config(), source, "succeeded")

    def test_runner_event_ack_retry_replays_exact_sequence(self):
        observed = []

        def post(cfg, event, deadline):
            observed.append(dict(event))
            if len(observed) == 1:
                raise ConnectionResetError("ack lost")
            return "continue", None

        reporter = runner.StatusReporter(config(), threading.Event(), time.monotonic() + 5)
        with patch.object(runner, "post_runner_event", side_effect=post), patch.object(reporter.stop, "wait", return_value=False):
            reporter.claim()
            reporter.close()
        self.assertEqual([event["sequence"] for event in observed], [1, 1])
        self.assertEqual(observed[0], observed[1])

    def test_runner_event_retries_temporary_platform_failure(self):
        observed = []

        def post(cfg, event, deadline):
            observed.append(dict(event))
            if len(observed) < 3:
                raise runner.RetryableTransferError("temporary API or database failure")
            return "continue", None

        reporter = runner.StatusReporter(config(), threading.Event(), time.monotonic() + 5)
        with patch.object(runner, "post_runner_event", side_effect=post), patch.object(reporter.stop, "wait", return_value=False):
            reporter.claim()
            reporter.close()
        self.assertEqual([event["sequence"] for event in observed], [1, 1, 1])
        self.assertTrue(all(event == observed[0] for event in observed))

    def test_runner_event_retries_incomplete_success_ack(self):
        incomplete = MagicMock(status=200)
        incomplete.read.return_value = b'{"data":'
        accepted = MagicMock(status=200)
        accepted.read.return_value = json.dumps({"data": {
            "acceptedSequence": 1, "action": "continue",
        }}).encode()
        connection = MagicMock()
        connection.getresponse.side_effect = [incomplete, accepted]
        reporter = runner.StatusReporter(config(), threading.Event(), time.monotonic() + 5)

        with patch.object(runner.http.client, "HTTPConnection", return_value=connection), \
                patch.object(reporter.stop, "wait", return_value=False):
            reporter.claim()
            reporter.close()

        bodies = [call.kwargs["body"] for call in connection.request.call_args_list]
        self.assertEqual(len(bodies), 2)
        self.assertEqual(bodies[0], bodies[1])

    def test_runner_event_does_not_retry_invalid_success_ack(self):
        invalid = MagicMock(status=200)
        invalid.read.return_value = json.dumps({"data": {"action": "continue"}}).encode()
        connection = MagicMock()
        connection.getresponse.return_value = invalid
        reporter = runner.StatusReporter(config(), threading.Event(), time.monotonic() + 5)

        with patch.object(runner.http.client, "HTTPConnection", return_value=connection), \
                self.assertRaises(runner.RunnerError):
            reporter.claim()

        connection.request.assert_called_once()

    def test_runner_event_rejects_inconsistent_stop_outcome(self):
        for data in (
                {"acceptedSequence": 1, "action": "stop"},
                {"acceptedSequence": 1, "action": "stop", "stopOutcome": "failed"},
                {"acceptedSequence": 1, "action": "continue", "stopOutcome": "timed_out"},
        ):
            with self.subTest(data=data):
                invalid = MagicMock(status=200)
                invalid.read.return_value = json.dumps({"data": data}).encode()
                connection = MagicMock()
                connection.getresponse.return_value = invalid
                reporter = runner.StatusReporter(config(), threading.Event(), time.monotonic() + 5)

                with patch.object(runner.http.client, "HTTPConnection", return_value=connection), \
                        self.assertRaises(runner.RunnerError):
                    reporter.claim()

                connection.request.assert_called_once()

    def test_runner_event_stop_interrupts_retry_backoff(self):
        reporter = runner.StatusReporter(config(), threading.Event(), time.monotonic() + 5)
        attempts = []

        def post(cfg, event, deadline):
            attempts.append(dict(event))
            reporter.stop.set()
            raise ConnectionRefusedError("platform unavailable")

        with patch.object(runner, "post_runner_event", side_effect=post), \
                patch.object(runner, "log_runner_event"), \
                self.assertRaisesRegex(runner.RunnerError, "reporter stopped"):
            reporter._send_with_retry({"protocolVersion": "v1", "sequence": 1, "kind": "claim"})
        self.assertEqual(len(attempts), 1)

    def test_heartbeat_stop_cancels_framework_control(self):
        cancel = threading.Event()
        kinds = []

        def post(cfg, event, deadline):
            kinds.append(event["kind"])
            return ("stop", "timed_out") if event["kind"] == "heartbeat" else ("continue", None)

        reporter = runner.StatusReporter(config(), cancel, time.monotonic() + 5)
        with patch.object(runner, "HEARTBEAT_SECONDS", 0.01), patch.object(runner, "post_runner_event", side_effect=post):
            reporter.claim()
            self.assertTrue(cancel.wait(1))
            reporter.close()
        self.assertIn("heartbeat", kinds)

    def test_heartbeat_stop_preserves_authoritative_terminal_outcome(self):
        cancel = threading.Event()
        reporter = runner.StatusReporter(config(), cancel, time.monotonic() + 5)
        observed = []

        def post(cfg, event, deadline):
            observed.append(event)
            return "stop", "timed_out"

        terminal = {"protocolVersion": "v1", "sequence": 3, "kind": "terminal", "terminal": {
            "outcome": "succeeded", "artifactId": "a" * 64, "artifactDigest": "b" * 64,
            "collectionComplete": True, "reason": "evaluation_succeeded",
        }}
        with patch.object(runner, "post_runner_event", side_effect=post):
            reporter._send_with_retry({"protocolVersion": "v1", "sequence": 2, "kind": "heartbeat"})
            reporter._send_with_retry(terminal)

        self.assertTrue(cancel.is_set())
        self.assertEqual(reporter.stop_outcome, "timed_out")
        self.assertEqual(observed[-1]["terminal"]["outcome"], "timed_out")
        self.assertEqual(observed[-1]["terminal"]["reason"], "evaluation_timed_out")

    def test_terminal_cancel_race_retries_cancelled_outcome(self):
        cancel = threading.Event()
        reporter = runner.StatusReporter(config(), cancel, time.monotonic() + 5)
        observed = []

        def post(cfg, event, deadline):
            observed.append(event)
            if len(observed) == 1:
                cancel.set()
                raise runner.RunnerEventConflictError("platform rejected runner event with HTTP 409")
            return "stop", "cancelled"

        event = {"protocolVersion": "v1", "sequence": 2, "kind": "terminal", "terminal": {
            "outcome": "succeeded", "artifactId": "a" * 64, "artifactDigest": "b" * 64,
            "collectionComplete": True, "reason": "evaluation_succeeded",
        }}
        with patch.object(runner, "post_runner_event", side_effect=post):
            reporter._send_with_retry(event)
        self.assertEqual([item["terminal"]["outcome"] for item in observed], ["succeeded", "cancelled"])
        self.assertEqual(observed[-1]["terminal"]["reason"], "evaluation_cancelled")

    def test_terminal_stop_conflict_retries_authoritative_outcome(self):
        for stop_outcome, reason in (("cancelled", "evaluation_cancelled"),
                                     ("timed_out", "evaluation_timed_out")):
            with self.subTest(stop_outcome=stop_outcome):
                conflict = MagicMock(status=409)
                conflict.read.return_value = json.dumps({
                    "code": 34004, "message": "runner conflict", "data": {"stopOutcome": stop_outcome},
                }).encode()
                accepted = MagicMock(status=200)
                accepted.read.return_value = json.dumps({"data": {
                    "acceptedSequence": 2, "action": "stop", "stopOutcome": stop_outcome,
                }}).encode()
                connection = MagicMock()
                connection.getresponse.side_effect = [conflict, accepted]
                cancel = threading.Event()
                reporter = runner.StatusReporter(config(), cancel, time.monotonic() + 5)
                event = {"protocolVersion": "v1", "sequence": 2, "kind": "terminal", "terminal": {
                    "outcome": "succeeded", "artifactId": "a" * 64, "artifactDigest": "b" * 64,
                    "collectionComplete": True, "reason": "evaluation_succeeded",
                }}

                with patch.object(runner.http.client, "HTTPConnection", return_value=connection):
                    reporter._send_with_retry(event)

                sent = [json.loads(call.kwargs["body"])["terminal"]
                        for call in connection.request.call_args_list]
                self.assertEqual([item["outcome"] for item in sent], ["succeeded", stop_outcome])
                self.assertEqual(sent[-1]["reason"], reason)
                self.assertTrue(cancel.is_set())

    def test_terminal_generic_conflict_does_not_guess_stop_outcome(self):
        conflict = MagicMock(status=409)
        conflict.read.return_value = json.dumps({
            "code": 34004, "message": "runner conflict", "data": None,
        }).encode()
        connection = MagicMock()
        connection.getresponse.return_value = conflict
        reporter = runner.StatusReporter(config(), threading.Event(), time.monotonic() + 5)
        event = {"protocolVersion": "v1", "sequence": 2, "kind": "terminal", "terminal": {
            "outcome": "succeeded", "artifactId": "a" * 64, "artifactDigest": "b" * 64,
            "collectionComplete": True, "reason": "evaluation_succeeded",
        }}

        with patch.object(runner.http.client, "HTTPConnection", return_value=connection), \
                self.assertRaises(runner.RunnerEventConflictError):
            reporter._send_with_retry(event)

        connection.request.assert_called_once()

    def test_terminal_lost_ack_replays_original_after_local_cancel(self):
        cancel = threading.Event()
        reporter = runner.StatusReporter(config(), cancel, time.monotonic() + 5)
        observed = []

        def post(cfg, event, deadline):
            observed.append(json.dumps(event, sort_keys=True))
            if len(observed) == 1:
                cancel.set()
                raise ConnectionResetError("acknowledgment lost after commit")
            return "stop", "cancelled"

        event = {"protocolVersion": "v1", "sequence": 2, "kind": "terminal", "terminal": {
            "outcome": "succeeded", "artifactId": "a" * 64, "artifactDigest": "b" * 64,
            "collectionComplete": True, "reason": "evaluation_succeeded",
        }}
        with patch.object(runner, "post_runner_event", side_effect=post), \
                patch.object(reporter.stop, "wait", return_value=False):
            reporter._send_with_retry(event)
        self.assertEqual(len(observed), 2)
        self.assertEqual(observed[0], observed[1])
        self.assertEqual(reporter.stop_outcome, "cancelled")

    def test_terminal_event_ends_reporter_before_due_heartbeat(self):
        reporter = runner.StatusReporter(config(), threading.Event(), time.monotonic() + 60)
        completed = threading.Event()
        reporter.events.put(({"kind": "terminal", "terminal": {
            "outcome": "succeeded", "artifactId": "a" * 64, "artifactDigest": "b" * 64,
            "collectionComplete": True, "reason": "evaluation_succeeded",
        }}, completed))
        observed = []

        def send(event):
            observed.append(event["kind"])
            if event["kind"] == "heartbeat":
                reporter.stop.set()

        with patch.object(runner.time, "monotonic", side_effect=[0, 0, 16, 16]), \
                patch.object(reporter, "_send_with_retry", side_effect=send):
            reporter._run()

        self.assertEqual(observed, ["terminal"])
        self.assertTrue(completed.is_set())
        self.assertIsNone(reporter.failure)

    def test_terminal_supersedes_uncertain_heartbeat_with_higher_sequence(self):
        reporter = runner.StatusReporter(config(), threading.Event(), time.monotonic() + 10)
        reporter.events.put(({"kind": "heartbeat"}, None))
        reporter.events.put(({"kind": "phase", "phase": "finalizing"}, None))
        completed = threading.Event()
        observed = []

        def post(cfg, event, deadline):
            observed.append(event)
            if event["kind"] == "heartbeat":
                reporter.events.put(({"kind": "terminal", "terminal": {
                    "outcome": "failed", "artifactId": "a" * 64, "artifactDigest": "b" * 64,
                    "collectionComplete": False, "reason": "evaluation_failed",
                }}, completed))
                reporter.terminal_ready.set()
                raise ConnectionResetError("heartbeat accepted but acknowledgment lost")
            return "continue", None

        with patch.object(runner, "post_runner_event", side_effect=post), \
                patch.object(reporter.stop, "wait") as backoff:
            reporter._run()
        self.assertEqual([(item["kind"], item["sequence"]) for item in observed],
                         [("heartbeat", 2), ("terminal", 4)])
        self.assertTrue(completed.is_set())
        self.assertIsNone(reporter.failure)
        backoff.assert_not_called()

    def test_terminal_priority_cannot_skip_claim(self):
        reporter = runner.StatusReporter(config(), threading.Event(), time.monotonic() + 5)
        reporter.terminal_ready.set()
        event = {"protocolVersion": "v1", "sequence": 1, "kind": "claim"}
        with patch.object(runner, "post_runner_event", side_effect=[ConnectionResetError(), ("continue", None)]) as post, \
                patch.object(reporter.stop, "wait", return_value=False):
            reporter._send_with_retry(event)
        self.assertEqual(post.call_count, 2)
        self.assertEqual(post.call_args_list[0].args[1], post.call_args_list[1].args[1])

    def test_snapshot_attempt_cannot_consume_terminal_reserved_window(self):
        now = [929.0]
        reporter = runner.StatusReporter(config(), threading.Event(), 960)
        reporter.snapshot_deadline = 930
        observed = []

        def post(cfg, event, deadline):
            observed.append((event["kind"], deadline))
            if event["kind"] == "heartbeat":
                now[0] = deadline
                raise TimeoutError()
            return "continue", None

        with patch.object(runner.time, "monotonic", side_effect=lambda: now[0]), \
                patch.object(runner, "post_runner_event", side_effect=post):
            reporter._send_with_retry({"kind": "heartbeat", "sequence": 2})
            reporter._send_with_retry({"kind": "terminal", "sequence": 3, "terminal": {"outcome": "failed"}})
        self.assertEqual(observed, [("heartbeat", 930), ("terminal", 960)])
        self.assertEqual(960 - now[0], 30)

    def test_main_claims_before_starting_evaluation(self):
        claimed = []
        reporter = MagicMock()
        reporter.claim.side_effect = lambda: claimed.append(True)

        def execute(*args, **kwargs):
            self.assertTrue(claimed)
            return 0

        with patch.dict(os.environ, {"ERUUN_JOB_CONFIG": json.dumps(config())}), \
                patch.object(runner, "StatusReporter", return_value=reporter), \
                patch.object(runner.Path, "mkdir"), \
                patch.object(runner.tempfile, "mkdtemp", return_value=str(self.root)), \
                patch.object(runner, "execute", side_effect=execute):
            self.assertEqual(runner.main(), 0)
        reporter.claim.assert_called_once()

    def test_conflicting_claim_stops_before_dataset_or_harbor(self):
        reporter = MagicMock()
        reporter.claim.side_effect = runner.RunnerError("platform rejected runner event with HTTP 409")
        with patch.dict(os.environ, {"ERUUN_JOB_CONFIG": json.dumps(config())}), \
                patch.object(runner, "StatusReporter", return_value=reporter), \
                patch.object(runner, "execute") as execute:
            self.assertEqual(runner.main(), 1)
        execute.assert_not_called()

    def test_result_is_uploaded_before_terminal_ack(self):
        package = self.example_package()
        calls = []
        reporter = MagicMock()
        reporter.shorten_deadline.side_effect = lambda deadline: deadline
        reporter.emit.side_effect = lambda kind, **fields: calls.append((kind, fields))
        reporter.terminal.side_effect = lambda terminal: calls.append(("terminal", terminal))

        def framework(cfg, output, cancel, timeout):
            (output / "run").mkdir()
            (output / "run/result.json").write_text(json.dumps(result()))
            collected_trial(output)
            return 0, None

        def upload(cfg, path, status, **kwargs):
            calls.append(("upload", status))
            return {"id": "a" * 64, "digest": "b" * 64}

        with patch.object(runner.importlib.metadata, "version", return_value=runner.FRAMEWORK_VERSION), \
                patch.object(runner, "download_package", side_effect=lambda cfg, dest: shutil.copyfile(package, dest)), \
                patch.object(runner, "run_framework", side_effect=framework), \
                patch.object(runner, "upload_results", side_effect=upload):
            code = runner.execute(config(), self.root, threading.Event(), reporter=reporter, final_deadline=time.monotonic() + 10)
        self.assertEqual(code, 0)
        names = [call[0] for call in calls]
        self.assertLess(names.index("upload"), names.index("terminal"))
        self.assertEqual(calls[-1][1]["artifactId"], "a" * 64)

    def test_sandbox_capability_stays_outside_harbor_config_and_result_archive(self):
        package = self.example_package()
        cfg = config() | {"sandboxURL": "http://platform.test/api/v1/job-runners/task-test/sandboxes",
                          "timeoutSeconds": 14 * 24 * 3600}
        reporter = runner.StatusReporter(cfg, threading.Event(), time.monotonic() + cfg["timeoutSeconds"] + 960)

        def framework(config_path, output, cancel, timeout):
            contents = config_path.read_text()
            generated = json.loads(contents)
            self.assertNotIn(cfg["token"], contents)
            profile = Path(generated["environment"]["kwargs"]["sandbox_control_file"])
            self.assertEqual(profile.stat().st_mode & 0o777, 0o600)
            self.assertFalse(profile.is_relative_to(output))
            self.assertEqual(json.loads(profile.read_text())["token"], cfg["token"])
            admission = Path(json.loads(profile.read_text())["admissionStateFile"])
            self.assertEqual(admission.stat().st_mode & 0o777, 0o600)
            self.assertTrue(json.loads(admission.read_text())["healthy"])
            self.assertGreater(generated["environment_build_timeout_multiplier"], 2000)
            self.assertNotIn("agent_timeout_multiplier", generated)
            self.assertNotIn("verifier_timeout_multiplier", generated)
            (output / "run").mkdir()
            (output / "run/result.json").write_text(json.dumps(result()))
            collected_trial(output)
            return 0, None

        def upload(cfg, path, status, **kwargs):
            with tarfile.open(path) as archive:
                self.assertNotIn("sandbox-control.json", archive.getnames())
                self.assertNotIn("admission-state.json", archive.getnames())
                for member in archive:
                    if member.isfile():
                        self.assertNotIn(cfg["token"].encode(), archive.extractfile(member).read())
            return {"id": "a" * 64, "digest": "b" * 64}

        with patch.object(runner.importlib.metadata, "version", return_value=runner.FRAMEWORK_VERSION), \
                patch.object(runner, "download_package", side_effect=lambda cfg, dest: shutil.copyfile(package, dest)), \
                patch.object(runner, "run_framework", side_effect=framework), \
                patch.object(reporter, "terminal"), \
                patch.object(runner, "upload_results", side_effect=upload):
            self.assertEqual(runner.execute(cfg, self.root, reporter.cancel, reporter=reporter), 0)

    def test_sandbox_preparation_consumes_framework_execution_budget(self):
        package = self.example_package()
        cfg = config() | {"sandboxURL": "http://platform.test/api/v1/job-runners/task-test/sandboxes"}
        reporter = MagicMock()
        reporter.shorten_deadline.side_effect = lambda deadline: deadline
        now = [100.0]
        observed_timeouts = []

        def download(_cfg, destination):
            shutil.copyfile(package, destination)
            now[0] = 120.0

        def framework(_config_path, output, _cancel, timeout):
            observed_timeouts.append(timeout)
            (output / "run").mkdir()
            (output / "run/result.json").write_text(json.dumps(result()))
            collected_trial(output)
            return 0, None

        with patch.object(runner.time, "monotonic", side_effect=lambda: now[0]), \
                patch.object(runner.importlib.metadata, "version", return_value=runner.FRAMEWORK_VERSION), \
                patch.object(runner, "download_package", side_effect=download), \
                patch.object(runner, "run_framework", side_effect=framework), \
                patch.object(runner, "upload_results", return_value={"id": "a" * 64, "digest": "b" * 64}):
            code = runner.execute(cfg, self.root, threading.Event(), reporter=reporter, final_deadline=1120.0)

        self.assertEqual(code, 0)
        self.assertEqual(observed_timeouts, [40.0])
        control = json.loads((self.root / "sandbox-control.json").read_text())
        self.assertEqual(control["executionDeadline"], 160.0)
        reporter.terminal.assert_called_once()

    def test_stop_snapshot_rereads_cause_after_racing_cancel(self):
        cases = (
            ("reporter failure", TimeoutError("control reporter failed"), False, None, "failed"),
            ("control outage", None, True, None, "failed"),
            ("authoritative timeout", None, False, "timed_out", "timed_out"),
            ("authoritative cancellation", None, False, "cancelled", "cancelled"),
            ("signal cancellation", None, False, None, "cancelled"),
        )
        original_cause = runner._reporter_stop_outcome
        for name, failure, outage_exhausted, stop_outcome, expected in cases:
            with self.subTest(name=name):
                reporter = SimpleNamespace(failure=None, outage_exhausted=False, stop_outcome=None)
                cancel = threading.Event()
                first_read = threading.Barrier(2)
                cause_published = threading.Barrier(2)
                cause_reads = []

                def read_cause(current_reporter):
                    cause = original_cause(current_reporter)
                    cause_reads.append(cause)
                    if len(cause_reads) == 1:
                        first_read.wait(timeout=2)
                        cause_published.wait(timeout=2)
                    return cause

                def publish_stop():
                    first_read.wait(timeout=2)
                    reporter.failure = failure
                    reporter.outage_exhausted = outage_exhausted
                    reporter.stop_outcome = stop_outcome
                    cancel.set()
                    cause_published.wait(timeout=2)

                publisher = threading.Thread(target=publish_stop)
                publisher.start()
                with patch.object(runner, "_reporter_stop_outcome", side_effect=read_cause):
                    observed = runner._stop_snapshot(reporter, cancel)
                publisher.join(timeout=2)

                self.assertFalse(publisher.is_alive())
                self.assertEqual(observed, expected)
                self.assertEqual(len(cause_reads), 2)

    def test_execute_stops_before_framework_and_preserves_cancel_source(self):
        package = self.example_package()
        cases = (
            ("control outage before shared cancel", None, True, None, False,
             "failed", "evaluation_failed", "control_outage",
             "runner control outage exceeded its time budget"),
            ("reporter failure before derived cause", TimeoutError("control reporter failed"), False, None, False,
             "failed", "evaluation_failed", "control_outage",
             "runner control outage exceeded its time budget"),
            ("authoritative cancellation before shared cancel", None, True, "cancelled", False,
             "cancelled", "evaluation_cancelled", "cancelled",
             "cancelled before framework start"),
            ("authoritative timeout before shared cancel", None, True, "timed_out", False,
             "timed_out", "evaluation_timed_out", "timed_out",
             "timed out before framework start"),
            ("signal cancellation", None, False, None, True,
             "cancelled", "evaluation_cancelled", "cancelled",
             "cancelled before framework start"),
        )
        for (name, failure, outage_exhausted, stop_outcome, set_cancel,
             outcome, reason, interruption, error) in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                work = Path(directory)
                cancel = threading.Event()
                reporter = MagicMock()
                reporter.failure = None
                reporter.outage_exhausted = False
                reporter.stop_outcome = None
                reporter.shorten_deadline.side_effect = lambda deadline: deadline
                archived = []

                def download(_cfg, destination):
                    shutil.copyfile(package, destination)
                    reporter.failure = failure
                    reporter.outage_exhausted = outage_exhausted
                    reporter.stop_outcome = stop_outcome
                    if set_cancel:
                        cancel.set()

                def upload(_cfg, path, status, **_kwargs):
                    with tarfile.open(path) as archive:
                        archived.append(json.load(archive.extractfile("result.json")))
                    self.assertEqual(status, "failed")
                    return {"id": "a" * 64, "digest": "b" * 64}

                with patch.object(runner.importlib.metadata, "version", return_value=runner.FRAMEWORK_VERSION), \
                        patch.object(runner, "download_package", side_effect=download), \
                        patch.object(runner, "run_framework") as framework, \
                        patch.object(runner, "upload_results", side_effect=upload):
                    code = runner.execute(config(), work, cancel, reporter=reporter,
                                          final_deadline=time.monotonic() + 10)

                self.assertEqual(code, 1)
                framework.assert_not_called()
                terminal = reporter.terminal.call_args.args[0]
                self.assertEqual((terminal["outcome"], terminal["reason"]), (outcome, reason))
                self.assertEqual(archived[0]["interruption"], interruption)
                self.assertEqual(archived[0]["error"], error)

    def test_execute_runtime_control_outage_reports_failed_terminal(self):
        package = self.example_package()
        cancel = threading.Event()
        reporter = MagicMock()
        reporter.outage_exhausted = False
        reporter.stop_outcome = None
        reporter.shorten_deadline.side_effect = lambda deadline: deadline

        def framework(_config_path, output, _cancel, _timeout):
            (output / "partial.log").write_bytes(b"stopped by local control outage")
            reporter.outage_exhausted = True
            cancel.set()
            return -signal.SIGTERM, "cancelled"

        with patch.object(runner.importlib.metadata, "version", return_value=runner.FRAMEWORK_VERSION), \
                patch.object(runner, "download_package", side_effect=lambda _cfg, dest: shutil.copyfile(package, dest)), \
                patch.object(runner, "run_framework", side_effect=framework), \
                patch.object(runner, "upload_results", return_value={"id": "a" * 64, "digest": "b" * 64}):
            code = runner.execute(config(), self.root, cancel, reporter=reporter,
                                  final_deadline=time.monotonic() + 10)

        self.assertEqual(code, 1)
        terminal = reporter.terminal.call_args.args[0]
        self.assertEqual(terminal["outcome"], "failed")
        self.assertEqual(terminal["reason"], "evaluation_failed")

    def test_execute_stop_before_upload_cannot_mark_artifact_succeeded(self):
        package = self.example_package()
        archive_results = runner.archive_results
        cases = (
            ("control outage", None, True, None, "failed", "evaluation_failed"),
            ("reporter failure before derived cause", TimeoutError("control reporter failed"), False, None,
             "failed", "evaluation_failed"),
            ("authoritative cancellation", None, True, "cancelled", "cancelled", "evaluation_cancelled"),
            ("authoritative timeout", None, True, "timed_out", "timed_out", "evaluation_timed_out"),
        )
        for name, failure, outage_exhausted, stop_outcome, outcome, reason in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                work = Path(directory)
                cancel = threading.Event()
                reporter = MagicMock()
                reporter.failure = None
                reporter.outage_exhausted = False
                reporter.stop_outcome = None
                reporter.shorten_deadline.side_effect = lambda deadline: deadline
                upload_statuses = []

                def framework(_config_path, output, _cancel, _timeout):
                    (output / "run").mkdir()
                    (output / "run/result.json").write_text(json.dumps(result()))
                    collected_trial(output)
                    return 0, None

                def archive(output, destination, report, **kwargs):
                    archive_results(output, destination, report, **kwargs)
                    reporter.failure = failure
                    reporter.outage_exhausted = outage_exhausted
                    reporter.stop_outcome = stop_outcome

                def upload(_cfg, _path, status, **_kwargs):
                    upload_statuses.append(status)
                    return {"id": "a" * 64, "digest": "b" * 64}

                with patch.object(runner.importlib.metadata, "version", return_value=runner.FRAMEWORK_VERSION), \
                        patch.object(runner, "download_package",
                                     side_effect=lambda _cfg, dest: shutil.copyfile(package, dest)), \
                        patch.object(runner, "run_framework", side_effect=framework), \
                        patch.object(runner, "archive_results", side_effect=archive), \
                        patch.object(runner, "upload_results", side_effect=upload):
                    code = runner.execute(config(), work, cancel, reporter=reporter,
                                          final_deadline=time.monotonic() + 10)

                self.assertFalse(cancel.is_set())
                self.assertEqual(code, 1)
                self.assertEqual(upload_statuses, ["failed"])
                terminal = reporter.terminal.call_args.args[0]
                self.assertEqual((terminal["outcome"], terminal["reason"]), (outcome, reason))

    def test_cancel_during_result_upload_overrides_success_terminal(self):
        package = self.example_package()
        cancel = threading.Event()
        reporter = MagicMock()
        reporter.shorten_deadline.side_effect = lambda deadline: deadline

        def framework(cfg, output, cancel, timeout):
            (output / "run").mkdir()
            (output / "run/result.json").write_text(json.dumps(result()))
            collected_trial(output)
            return 0, None

        def upload(cfg, path, status, **kwargs):
            cancel.set()
            return {"id": "a" * 64, "digest": "b" * 64}

        with patch.object(runner.importlib.metadata, "version", return_value=runner.FRAMEWORK_VERSION), \
                patch.object(runner, "download_package", side_effect=lambda cfg, dest: shutil.copyfile(package, dest)), \
                patch.object(runner, "run_framework", side_effect=framework), \
                patch.object(runner, "upload_results", side_effect=upload):
            code = runner.execute(config(), self.root, cancel, reporter=reporter, final_deadline=time.monotonic() + 10)
        self.assertEqual(code, 1)
        terminal = reporter.terminal.call_args.args[0]
        self.assertEqual(terminal["outcome"], "cancelled")
        self.assertEqual(terminal["reason"], "evaluation_cancelled")

    def test_network_retry_reuses_exact_archive_bytes(self):
        path = self.root / "result.tar.gz"
        path.write_bytes(b"immutable source bytes")
        observed = []
        def upload(cfg, source, status, **kwargs):
            observed.append(source.read_bytes())
            if len(observed) < 3:
                raise ConnectionResetError("lost acknowledgment")
        with patch.object(runner, "upload_results", side_effect=upload), patch.object(runner.time, "sleep"):
            runner.upload_with_retry(config(), path, "succeeded")
        self.assertEqual(observed, [b"immutable source bytes"] * 3)

    def test_upload_recovers_after_more_than_three_failures_with_bounded_jitter(self):
        path = self.root / "result.tar.gz"
        path.write_bytes(b"same source after response loss")
        now = [0.0]
        waits = []
        observed = []

        def upload(cfg, source, status, deadline):
            observed.append((source.read_bytes(), status, deadline))
            if len(observed) <= 5:
                raise ConnectionResetError("private-transfer-token http://platform.test/result")
            return {"id": "a" * 64, "digest": "b" * 64}

        def sleep(delay):
            waits.append(delay)
            now[0] += delay

        output = io.StringIO()
        with patch.object(runner, "upload_results", side_effect=upload), \
                patch.object(runner.time, "monotonic", side_effect=lambda: now[0]), \
                patch.object(runner.time, "sleep", side_effect=sleep), \
                patch.object(runner.random, "uniform", side_effect=lambda low, high: (low + high) / 2), \
                redirect_stdout(output):
            artifact = runner.upload_with_retry(config(), path, "succeeded", deadline=30)
        self.assertEqual(artifact["id"], "a" * 64)
        self.assertEqual(observed, [(path.read_bytes(), "succeeded", 30)] * 6)
        self.assertEqual(waits, [0.375, 0.75, 1.5, 3.0, 3.75])
        self.assertNotIn("private-transfer-token", output.getvalue())
        self.assertNotIn("platform.test", output.getvalue())

    def test_http_deadline_interrupts_trickling_headers_and_response_body(self):
        for operation in ("event", "upload"):
            for stage in ("headers", "body"):
                with self.subTest(operation=operation, stage=stage):
                    if operation == "event":
                        body = json.dumps({"data": {"acceptedSequence": 1, "action": "continue"}}).encode()
                    else:
                        body = json.dumps({"data": {"id": "a" * 64, "digest": "b" * 64}}).encode()
                    headers = (f"HTTP/1.1 200 OK\r\nContent-Length: {len(body)}\r\n"
                               "Connection: close\r\n\r\n").encode()

                    class Handler(BaseHTTPRequestHandler):
                        def log_message(self, *args):
                            pass

                        def do_POST(self):
                            self.rfile.read(int(self.headers["Content-Length"]))
                            try:
                                if stage == "body":
                                    self.wfile.write(headers)
                                    self.wfile.flush()
                                for byte in headers + body if stage == "headers" else body:
                                    self.wfile.write(bytes([byte]))
                                    self.wfile.flush()
                                    if self.server.stop.wait(0.01):
                                        break
                            except OSError:
                                pass

                    timers = []
                    make_timer = threading.Timer

                    def timer(*args, **kwargs):
                        created = make_timer(*args, **kwargs)
                        timers.append(created)
                        return created

                    with local_http_server(Handler) as server:
                        cfg = config()
                        url = f"http://127.0.0.1:{server.server_port}/test"
                        cfg["eventURL"] = cfg["resultURL"] = url
                        source = self.root / "source"
                        source.write_bytes(b"archive")
                        started = time.monotonic()
                        with patch.object(runner.threading, "Timer", side_effect=timer), \
                                patch.object(runner, "EVENT_ATTEMPT_SECONDS", 0.15), \
                                self.assertRaises(TimeoutError):
                            if operation == "event":
                                runner.post_runner_event(cfg, {"kind": "claim", "sequence": 1}, started + 5)
                            else:
                                runner.upload_results(cfg, source, "failed", deadline=started + 0.15)
                        self.assertLess(time.monotonic() - started, 1)
                        self.assertTrue(timers)
                        self.assertFalse(any(item.is_alive() for item in timers))

    def test_upload_attempt_deadline_interrupts_blocked_send(self):
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                self.server.stop.wait(2)

        source = self.root / "large-archive"
        source.write_bytes(b"x" * (16 * 1024 * 1024))
        with local_http_server(Handler) as server:
            cfg = config()
            cfg["resultURL"] = f"http://127.0.0.1:{server.server_port}/upload"
            cfg["transferTimeoutSeconds"] = 0.15
            started = time.monotonic()
            with self.assertRaises(TimeoutError):
                runner.upload_results(cfg, source, "failed", deadline=started + 5)
            self.assertLess(time.monotonic() - started, 1)

    def test_upload_retries_incomplete_success_ack_with_same_archive(self):
        path = self.root / "result.tar.gz"
        path.write_bytes(b"immutable source bytes")
        incomplete = MagicMock(status=201)
        incomplete.read.return_value = b'{"data":'
        accepted = MagicMock(status=201)
        accepted.read.return_value = json.dumps({"data": {
            "id": "a" * 64, "digest": "b" * 64,
        }}).encode()
        connection = MagicMock()
        connection.getresponse.side_effect = [incomplete, accepted]

        with patch.object(runner.http.client, "HTTPConnection", return_value=connection), \
                patch.object(runner.time, "sleep"):
            artifact = runner.upload_with_retry(config(), path, "succeeded")

        self.assertEqual(artifact["id"], "a" * 64)
        self.assertEqual([call.args[0] for call in connection.send.call_args_list],
                         [b"immutable source bytes", b"immutable source bytes"])

    def test_upload_does_not_retry_invalid_success_ack(self):
        path = self.root / "result.tar.gz"
        path.write_bytes(b"immutable source bytes")
        invalid = MagicMock(status=201)
        invalid.read.return_value = json.dumps({"data": {"id": "a" * 64}}).encode()
        connection = MagicMock()
        connection.getresponse.return_value = invalid

        with patch.object(runner.http.client, "HTTPConnection", return_value=connection), \
                self.assertRaises(runner.RunnerError):
            runner.upload_with_retry(config(), path, "succeeded")

        connection.send.assert_called_once_with(b"immutable source bytes")

    def test_upload_retries_share_one_total_time_budget(self):
        path = self.root / "archive"
        path.write_bytes(b"data")
        deadlines = []
        def upload(cfg, source, status, deadline):
            deadlines.append(deadline)
            raise ConnectionResetError()
        with patch.object(runner.time, "monotonic", side_effect=[10, 10, 11, 12, 311]), patch.object(runner.time, "sleep"), patch.object(runner, "upload_results", side_effect=upload), self.assertRaises(TimeoutError):
            runner.upload_with_retry(config(), path, "failed")
        self.assertEqual(deadlines, [310, 310])

    def test_slow_archive_preserves_upload_and_terminal_budget(self):
        for total in (360, 960):
            with self.subTest(total=total):
                work = self.root / str(total)
                work.mkdir()
                cfg = config() | {"finalizationTimeoutSeconds": total}
                now = [0.0]
                reporter = MagicMock()
                reporter.shorten_deadline.side_effect = lambda deadline: deadline

                def archive(output, path, report, deadline):
                    (output / "raw.log").write_bytes(b"preserved original")
                    self.assertEqual(deadline, 60)
                    now[0] = deadline
                    raise TimeoutError("archive preparation exceeded its budget")

                def upload(cfg, path, status, deadline):
                    self.assertEqual(deadline, total - 30)
                    with tarfile.open(path) as source:
                        report = json.load(source.extractfile("result.json"))
                    self.assertTrue(report["diagnosticOnly"])
                    self.assertFalse(report["collectionComplete"])
                    self.assertEqual(status, "failed")
                    now[0] = deadline - 1
                    return {"id": "a" * 64, "digest": "b" * 64}

                def terminal(event):
                    self.assertGreaterEqual(total - now[0], 30)
                    self.assertFalse(event["collectionComplete"])
                    self.assertEqual(event["outcome"], "failed")

                reporter.terminal.side_effect = terminal
                with patch.object(runner.time, "monotonic", side_effect=lambda: now[0]), \
                        patch.object(runner.importlib.metadata, "version", side_effect=runner.RunnerError("preparation failed")), \
                        patch.object(runner, "archive_results", side_effect=archive), \
                        patch.object(runner, "upload_results", side_effect=upload):
                    self.assertEqual(runner.execute(cfg, work, threading.Event(), reporter=reporter), 1)
                reporter.terminal.assert_called_once()
                self.assertEqual((work / "outputs/raw.log").read_bytes(), b"preserved original")

    def test_upload_deadline_never_reports_unconfirmed_artifact_terminal(self):
        now = [0.0]
        reporter = MagicMock()
        reporter.shorten_deadline.side_effect = lambda deadline: deadline
        attempts = []

        def upload(cfg, path, status, deadline):
            attempts.append(deadline)
            now[0] = deadline
            raise ConnectionResetError("acknowledgment lost")

        with patch.object(runner.time, "monotonic", side_effect=lambda: now[0]), \
                patch.object(runner.importlib.metadata, "version", side_effect=runner.RunnerError("preparation failed")), \
                patch.object(runner, "upload_results", side_effect=upload), \
                self.assertRaises(TimeoutError):
            runner.execute(config() | {"finalizationTimeoutSeconds": 360}, self.root, threading.Event(), reporter=reporter)
        self.assertEqual(attempts, [330])
        reporter.terminal.assert_not_called()
        self.assertTrue((self.root / "results.tar.gz").exists())

    def test_extended_budget_allows_terminal_to_retry_near_ten_minute_outage(self):
        now = [330.0]
        reporter = runner.StatusReporter(config(), threading.Event(), 960)
        observed = []

        def post(cfg, event, deadline):
            observed.append(json.dumps(event, sort_keys=True))
            if now[0] < 925:
                raise runner.RetryableTransferError("temporary database failure")
            return "continue", None

        def wait(delay):
            now[0] += delay
            return False

        event = {"protocolVersion": "v1", "sequence": 2, "kind": "terminal", "terminal": {
            "outcome": "failed", "artifactId": "a" * 64, "artifactDigest": "b" * 64,
            "collectionComplete": False, "reason": "evaluation_failed",
        }}
        with patch.object(runner.time, "monotonic", side_effect=lambda: now[0]), \
                patch.object(runner.random, "uniform", side_effect=lambda low, high: high), \
                patch.object(reporter.stop, "wait", side_effect=wait), \
                patch.object(runner, "log_runner_event"), \
                patch.object(runner, "post_runner_event", side_effect=post):
            reporter._send_with_retry(event)
        self.assertGreaterEqual(now[0], 925)
        self.assertLess(now[0], 960)
        self.assertGreater(len(observed), 3)
        self.assertTrue(all(item == observed[0] for item in observed))

    def test_control_outage_stops_claim_and_running_reporter_at_six_hundred_seconds(self):
        for kind in ("claim", "heartbeat"):
            with self.subTest(kind=kind):
                now = [0.0]
                cancel = threading.Event()
                reporter = runner.StatusReporter(config(), cancel, 14 * 24 * 3600)

                def wait(delay):
                    now[0] += delay
                    return False

                with patch.object(runner.time, "monotonic", side_effect=lambda: now[0]), \
                        patch.object(reporter.stop, "wait", side_effect=wait), \
                        patch.object(runner, "log_runner_event"), \
                        patch.object(runner, "post_runner_event", side_effect=ConnectionRefusedError()):
                    if kind == "claim":
                        with self.assertRaises(TimeoutError):
                            reporter.claim()
                    else:
                        reporter.events.put(({"kind": "heartbeat"}, None))
                        reporter._run()
                        self.assertIsInstance(reporter.failure, TimeoutError)
                        self.assertTrue(cancel.is_set())
                self.assertEqual(now[0], 600)

    def test_control_outage_budget_resets_only_after_acknowledgment(self):
        now = [500.0]
        reporter = runner.StatusReporter(config(), threading.Event(), 2000)
        reporter.outage_started = 0
        with patch.object(runner.time, "monotonic", side_effect=lambda: now[0]), \
                patch.object(runner, "post_runner_event", return_value=("continue", None)) as post:
            reporter._send_with_retry({"kind": "heartbeat", "sequence": 2})
            self.assertEqual(post.call_args.args[2], 600)
            self.assertIsNone(reporter.outage_started)
            reporter._send_with_retry({"kind": "heartbeat", "sequence": 3})
            self.assertEqual(post.call_args.args[2], 2000)

    def test_terminal_superseding_snapshot_keeps_existing_outage_budget(self):
        reporter = runner.StatusReporter(config(), threading.Event(), 2000)
        reporter.outage_started = 0

        def lost_snapshot(cfg, event, deadline):
            reporter.terminal_ready.set()
            raise ConnectionRefusedError()

        with patch.object(runner.time, "monotonic", return_value=590), \
                patch.object(runner, "post_runner_event", side_effect=lost_snapshot):
            reporter._send_with_retry({"kind": "heartbeat", "sequence": 2})
        self.assertEqual(reporter.outage_started, 0)
        with patch.object(runner.time, "monotonic", return_value=600), \
                patch.object(runner, "post_runner_event") as post, self.assertRaises(TimeoutError):
            reporter._send_with_retry({"kind": "terminal", "sequence": 3, "terminal": {"outcome": "failed"}})
        post.assert_not_called()

    def test_finalization_event_recovery_uses_fixed_delivery_window(self):
        for recover in (True, False):
            with self.subTest(recover=recover):
                now = [0.0]
                reporter = runner.StatusReporter(config(), threading.Event(), 2000)
                reporter.shorten_deadline(960)
                seen = []

                def post(cfg, event, deadline):
                    seen.append((json.dumps(event, sort_keys=True), deadline))
                    if not recover or now[0] < 601:
                        raise ConnectionRefusedError()
                    return "continue", None

                def wait(delay):
                    now[0] += delay
                    return False

                event = {"kind": "terminal", "sequence": 4, "terminal": {"outcome": "succeeded"}}
                with patch.object(runner.time, "monotonic", side_effect=lambda: now[0]), \
                        patch.object(reporter.stop, "wait", side_effect=wait), \
                        patch.object(runner, "log_runner_event"), \
                        patch.object(runner, "post_runner_event", side_effect=post):
                    if recover:
                        reporter._send_with_retry(event)
                        self.assertGreaterEqual(now[0], 601)
                        self.assertLess(now[0], 960)
                    else:
                        with self.assertRaises(TimeoutError):
                            reporter._send_with_retry(event)
                        self.assertEqual(now[0], 960)
                self.assertTrue(all(item == seen[0] for item in seen))
                self.assertEqual(seen[0][1], 960)
                self.assertFalse(reporter.cancel.is_set())

    def test_runtime_outage_stops_execution_but_allows_failed_terminal_delivery(self):
        now = [0.0]
        reporter = runner.StatusReporter(config(), threading.Event(), 2000)

        def wait(delay):
            now[0] += delay
            return False

        with patch.object(runner.time, "monotonic", side_effect=lambda: now[0]), \
                patch.object(reporter.stop, "wait", side_effect=wait), \
                patch.object(runner, "log_runner_event"), \
                patch.object(runner, "post_runner_event", side_effect=ConnectionRefusedError()):
            reporter.configure_admission(self.root / "admission.json")
            reporter.events.put(({"kind": "heartbeat"}, None))
            reporter._run()
            self.assertEqual(now[0], 600)
            self.assertTrue(reporter.cancel.is_set())
            self.assertTrue(reporter.outage_exhausted)
            self.assertEqual(reporter.shorten_deadline(now[0] + 960), 1560)
        now[0] = 601
        with patch.object(runner.time, "monotonic", side_effect=lambda: now[0]), \
                patch.object(runner, "post_runner_event", return_value=("continue", None)) as post:
            reporter.terminal({"outcome": "failed", "reason": "evaluation_failed"})
        self.assertEqual(post.call_args.args[1]["sequence"], 3)
        self.assertEqual(post.call_args.args[1]["terminal"]["outcome"], "failed")
        self.assertEqual(post.call_args.args[2], 1560)
        self.assertIsNone(reporter.failure)
        self.assertTrue(reporter.cancel.is_set())
        self.assertTrue(json.loads(reporter.admission_path.read_text())["stopped"])

    def test_collection_limits_emit_incomplete_diagnostic(self):
        output = self.root / "output"
        output.mkdir()
        (output / "file").write_bytes(b"original")
        report = {"executionStatus": "succeeded"}
        with patch.object(runner, "MAX_FILES", 2):
            runner.archive_results(output, self.root / "archive", report)
        self.assertFalse(report["collectionComplete"])
        self.assertEqual(report["collectionErrors"][0]["reason"], "too_many_entries")

    def test_archive_deadline_expires_while_copying_large_file(self):
        output = self.root / "outputs"
        output.mkdir()
        payload = output / "large.bin"
        payload.write_bytes(b"x" * (1024 * 1024 + 1))
        expired = threading.Event()
        original_open = Path.open

        class ExpiringSource:
            def __init__(self, source):
                self.source = source

            def __enter__(self):
                return self

            def __exit__(self, *args):
                self.source.close()

            def read(self, size=-1):
                contents = self.source.read(size)
                expired.set()
                return contents

        def open_path(path, *args, **kwargs):
            source = original_open(path, *args, **kwargs)
            if path == payload and args and args[0] == "rb":
                return ExpiringSource(source)
            return source

        report = {"executionStatus": "succeeded"}
        with patch.object(runner.Path, "open", new=open_path), \
                patch.object(runner.time, "monotonic", side_effect=lambda: 1 if expired.is_set() else 0), \
                self.assertRaisesRegex(TimeoutError, "total time budget"):
            runner.archive_results(output, self.root / "archive", report, deadline=0.5)

    def test_sandbox_collection_observes_finalization_deadline(self):
        output = self.root / "outputs"
        output.mkdir()
        with self.assertRaisesRegex(TimeoutError, "total time budget"):
            runner.check_sandbox_collection(output, 0, {}, deadline=time.monotonic() - 1)

    def test_many_long_paths_obey_the_api_manifest_budget(self):
        output = self.root / "outputs"
        parent = output / ("a" * 240) / ("b" * 240) / ("c" * 240)
        parent.mkdir(parents=True)
        for index in range(2300):
            (parent / (f"{index:04d}-" + "d" * 160)).touch()
        report = {"executionStatus": "succeeded"}
        archive_path = self.root / "archive"
        runner.archive_results(output, archive_path, report)
        self.assertFalse(report["collectionComplete"])
        self.assertIn("manifest_size_limit", {error["reason"] for error in report["collectionErrors"]})
        with tarfile.open(archive_path) as archive:
            sizes = [runner.manifest_entry_size(member.name, member.size) for member in archive if member.isfile()]
        self.assertLessEqual(2 + sum(sizes) + len(sizes) - 1, runner.MAX_MANIFEST_BYTES)
        self.assertEqual(runner.manifest_entry_size("<&>\u2028\u2029", 0), 126)

    def test_archive_failure_uploads_diagnostic_and_preserves_local_output(self):
        def framework(cfg, output, cancel, timeout):
            (output / "partial.log").write_bytes(b"raw retained locally")
            return 1, None
        def upload(cfg, path, status, **kwargs):
            with tarfile.open(path) as archive:
                report = json.load(archive.extractfile("result.json"))
                self.assertTrue(report["diagnosticOnly"])
                self.assertFalse(report["collectionComplete"])
            self.assertEqual(status, "failed")
        package = self.example_package()
        with patch.object(runner.importlib.metadata, "version", return_value=runner.FRAMEWORK_VERSION), patch.object(runner, "download_package", side_effect=lambda cfg, dest: shutil.copyfile(package, dest)), patch.object(runner, "run_framework", side_effect=framework), patch.object(runner, "archive_results", side_effect=runner.RunnerError("too large")), patch.object(runner, "upload_results", side_effect=upload):
            self.assertEqual(runner.execute(config(), self.root, threading.Event()), 1)
        self.assertEqual((self.root / "outputs/partial.log").read_bytes(), b"raw retained locally")

    def test_complete_run_archives_native_failure_diagnostics(self):
        package = self.example_package()
        def download(cfg, dest):
            shutil.copyfile(package, dest)
        captured = []
        def upload(cfg, path, status, **kwargs):
            with tarfile.open(path) as archive:
                captured.append((status, json.load(archive.extractfile("result.json")), archive.getnames()))
                self.assertEqual(archive.extractfile("outputs/run/trial/diagnostic.log").read(), b"original diagnostics")
        def framework(cfg, output, cancel, timeout):
            (output / "run/trial").mkdir(parents=True)
            (output / "run/trial/diagnostic.log").write_bytes(b"original diagnostics")
            collected_trial(output)
            failed = result()
            failed["stats"]["n_errored_trials"] = 1
            (output / "run/result.json").write_text(json.dumps(failed))
            return 0, None
        with patch.object(runner.importlib.metadata, "version", return_value=runner.FRAMEWORK_VERSION), patch.object(runner, "download_package", side_effect=download), patch.object(runner, "run_framework", side_effect=framework), patch.object(runner, "upload_results", side_effect=upload):
            code = runner.execute(config(), self.root, threading.Event())
        self.assertEqual(code, 1)
        self.assertEqual(captured[0][0], "failed")
        self.assertTrue(captured[0][1]["collectionComplete"])
        self.assertIn("outputs/run/result.json", captured[0][2])

    def test_cancel_preserves_collected_output(self):
        package = self.example_package()
        def download(cfg, dest):
            shutil.copyfile(package, dest)
        def framework(cfg, output, cancel, timeout):
            (output / "partial.log").write_bytes(b"before termination")
            return -15, "cancelled"
        def upload(cfg, path, status, **kwargs):
            with tarfile.open(path) as archive:
                report = json.load(archive.extractfile("result.json"))
                self.assertEqual(report["interruption"], "cancelled")
                self.assertFalse(report["collectionComplete"])
                self.assertEqual(archive.extractfile("outputs/partial.log").read(), b"before termination")
        with patch.object(runner.importlib.metadata, "version", return_value=runner.FRAMEWORK_VERSION), patch.object(runner, "download_package", side_effect=download), patch.object(runner, "run_framework", side_effect=framework), patch.object(runner, "upload_results", side_effect=upload):
            self.assertEqual(runner.execute(config(), self.root, threading.Event()), 1)

    def test_unfinished_or_missing_collection_state_is_not_complete(self):
        output = self.root / "outputs"
        output.mkdir()
        for changes in ({"stopped": False}, {"pending": 1}, {"started": False}, {"errorCount": 1}):
            with self.subTest(changes=changes):
                collected_trial(output, **changes)
                report = {"executionStatus": "failed"}
                runner.check_sandbox_collection(output, 1, report)
                runner.archive_results(output, self.root / "result.tar.gz", report)
                self.assertFalse(report["collectionComplete"])
        collected_trial(output)
        report = {}
        runner.check_sandbox_collection(output, 2, report)
        self.assertIn("missing_trial_collection_state", {e["reason"] for e in report["collectionErrors"]})

    def test_framework_process_cannot_expand_transfer_token(self):
        output = self.root / "outputs"
        output.mkdir()
        process = MagicMock()
        process.poll.return_value = 0
        process.returncode = 0
        with patch.dict(os.environ, {"ERUUN_JOB_CONFIG": "private-token", "OPENAI_API_KEY": "selected-secret"}), patch.object(runner.subprocess, "Popen", return_value=process) as start:
            runner.run_framework(self.root / "job.json", output, threading.Event(), 60)
        child_env = start.call_args.kwargs["env"]
        self.assertNotIn("ERUUN_JOB_CONFIG", child_env)
        self.assertEqual(child_env["OPENAI_API_KEY"], "selected-secret")


@unittest.skipUnless(importlib.util.find_spec("harbor") and importlib.util.find_spec("kubernetes"), "install pinned requirements to exercise Harbor")
class NativeHarborTest(unittest.TestCase):
    def environment(self, directory, name="trial", sandbox=False):
        from harbor.models.task.config import EnvironmentConfig
        from harbor.models.trial.paths import TrialPaths
        from eruun_environment import WorkspaceEnvironment

        output = Path(directory) / "outputs"
        output.mkdir(exist_ok=True)
        cfg = config()
        if sandbox:
            cfg["sandboxURL"] = "http://platform.test/api/v1/job-runners/task-test/sandboxes"
            control = {key: cfg[key] for key in ("sandboxURL", "token", "taskId", "namespace")}
            control.update(executionDeadline=time.monotonic() + 2000, finalizationDeadline=time.monotonic() + 2960)
            control["admissionStateFile"] = str(Path(directory) / "admission-state.json")
            profile = Path(directory) / "sandbox-control.json"
            profile.write_text(json.dumps(control))
            profile.chmod(0o600)
        generated = runner.harbor_config(cfg, [EXAMPLE], output, "runner-pod", "runner-uid")
        environment = WorkspaceEnvironment(
            environment_dir=EXAMPLE / "environment", environment_name="greeting",
            session_id=name, trial_paths=TrialPaths(output / "run" / name),
            task_env_config=EnvironmentConfig(docker_image="example.com/task:1.0.0", storage_mb=2048),
            **generated["environment"]["kwargs"],
        )
        environment._ensure_client = AsyncMock()
        environment._wait_for_admission = AsyncMock(return_value=0)
        environment._collection["started"] = not sandbox
        environment._save_collection()
        return environment, output

    @staticmethod
    def sandbox_reply(**changes):
        return {"trialId": "trial", "state": "ready", "admitted": True,
                "namespace": "workspace-test", "sandboxName": "allocated", "sandboxUID": "sandbox-uid",
                "podName": "actual-pod", "podUID": "pod-uid", "containerName": "main", **changes}

    def test_sandbox_start_uses_api_identity_and_release_waits_for_deletion(self):
        import eruun_environment
        from harbor.environments.ack import ACKEnvironment
        from harbor.environments.base import ExecResult

        with tempfile.TemporaryDirectory() as directory:
            environment, _ = self.environment(directory, sandbox=True)
            environment.exec = AsyncMock(return_value=ExecResult(return_code=0))
            environment._upload_environment_dir_after_start = AsyncMock()
            replies = [self.sandbox_reply(state="pending", admitted=False, sandboxUID="", podUID=""),
                       self.sandbox_reply(), self.sandbox_reply(state="pending", reason="release_pending"),
                       self.sandbox_reply(state="released")]
            with patch.object(eruun_environment, "sandbox_request", side_effect=replies) as request, \
                    patch.object(eruun_environment.asyncio, "sleep", new_callable=AsyncMock), \
                    patch.object(ACKEnvironment, "start", new_callable=AsyncMock) as pod_start, \
                    patch.object(ACKEnvironment, "stop", new_callable=AsyncMock) as pod_stop:
                asyncio.run(environment.start(False))
                artifact_dir = environment.trial_paths.artifacts_dir
                artifact_dir.mkdir(parents=True)
                (artifact_dir / "manifest.json").write_text('[{"status":"ok"}]')
                asyncio.run(environment.stop(False))
            pod_start.assert_not_called()
            pod_stop.assert_not_called()
            self.assertEqual(environment.pod_name, "actual-pod")
            self.assertEqual(environment._collection["sandboxUID"], "sandbox-uid")
            self.assertTrue(environment._collection["stopped"])
            calls = request.call_args_list
            self.assertEqual([(call.args[1], call.args[2]) for call in calls],
                             [("POST", ""), ("GET", "/trial"), ("POST", "/trial/release"), ("POST", "/trial/release")])
            self.assertEqual(calls[0].args[3], {"trialId": "trial", "image": "example.com/task:1.0.0", "storageMiB": 2048})
            self.assertEqual(calls[2].args[3], {"sandboxUID": "sandbox-uid", "podUID": "pod-uid", "collectionComplete": True})
            self.assertEqual(calls[2].args[3], calls[3].args[3])
            environment._upload_environment_dir_after_start.assert_awaited_once()

    def test_projected_token_rotation_reaches_core_and_sandbox_exec_clients(self):
        import datetime
        import harbor.environments.ack as ack
        import eruun_environment
        from kubernetes.client import Configuration
        from kubernetes.config import incluster_config

        real_datetime = datetime.datetime
        baseline = real_datetime(2026, 1, 1)
        with tempfile.TemporaryDirectory() as directory:
            token, cert = Path(directory) / "token", Path(directory) / "ca.crt"
            token.write_text("test-token-0")
            cert.write_text("test-ca")
            loader = incluster_config.InClusterConfigLoader(str(token), str(cert), environ={
                "KUBERNETES_SERVICE_HOST": "127.0.0.1", "KUBERNETES_SERVICE_PORT": "443"})
            manager = ack.KubernetesClientManager()
            with patch.object(Configuration, "_default", None), \
                    patch.object(incluster_config.datetime, "datetime", wraps=real_datetime) as clock, \
                    patch.object(ack.k8s_config, "load_kube_config", side_effect=ack.k8s_config.ConfigException()), \
                    patch.object(ack.k8s_config, "load_incluster_config", side_effect=loader.load_and_set), \
                    patch.object(ack, "DynamicClient"):
                clock.now.return_value = baseline
                manager._init_client(None)
                api = eruun_environment.SandboxExecAPI(self.sandbox_reply(), manager._core_api)
                clients = [manager._core_api.api_client, api.api_client]
                try:
                    for hours in (0, 1, 24, 14 * 24):
                        clock.now.return_value = baseline + datetime.timedelta(hours=hours)
                        token.write_text(f"test-token-{hours}")
                        for client in clients:
                            self.assertEqual(client.configuration.auth_settings()["BearerToken"]["value"],
                                             f"bearer test-token-{hours}")
                finally:
                    for client in clients + [manager._api_client, manager._batch_api.api_client]:
                        client.close()

    def test_event_outage_gates_new_sandbox_until_ack_and_never_reopens_after_stop(self):
        import eruun_environment
        with tempfile.TemporaryDirectory() as directory:
            environment, _ = self.environment(directory, sandbox=True)
            del environment._wait_for_admission
            environment._sandbox_control["executionDeadline"] = 2000
            now = [0.0]
            reporter = runner.StatusReporter(config(), threading.Event(), 2000)
            reporter.outage_started = 0

            async def advance(delay):
                self.assertFalse(environment._sandbox_requested)
                self.assertFalse(reporter.cancel.is_set())
                now[0] += delay
                if now[0] >= 2:
                    reporter._send_with_retry({"kind": "heartbeat", "sequence": 2})

            with patch.object(runner.time, "monotonic", side_effect=lambda: now[0]), \
                    patch.object(eruun_environment.asyncio, "sleep", side_effect=advance), \
                    patch.object(runner, "post_runner_event", return_value=("continue", None)), \
                    patch.object(eruun_environment, "sandbox_request", return_value=self.sandbox_reply()) as request:
                reporter.configure_admission(Path(environment._sandbox_control["admissionStateFile"]))
                asyncio.run(environment._start_sandbox())
                self.assertEqual(now[0], 2)
                request.assert_called_once()
                reporter.cancel.set()
                reporter._send_with_retry({"kind": "heartbeat", "sequence": 3})
                with self.assertRaisesRegex(runner.RunnerError, "stopped new sandbox"):
                    asyncio.run(environment._wait_for_admission(2000))
            self.assertEqual(reporter.admission_path.stat().st_mode & 0o777, 0o600)
            self.assertNotIn(config()["token"], reporter.admission_path.read_text())

    def test_missing_corrupt_or_expired_admission_state_fails_closed(self):
        import eruun_environment
        with tempfile.TemporaryDirectory() as directory:
            environment, _ = self.environment(directory, sandbox=True)
            del environment._wait_for_admission
            path = Path(environment._sandbox_control["admissionStateFile"])
            for contents in (None, "{}", json.dumps({"healthy": True, "stopped": False, "updatedAt": 0})):
                if contents is not None:
                    path.write_text(contents)
                    path.chmod(0o600)
                with self.subTest(contents=contents), \
                        patch.object(runner.time, "monotonic", return_value=61), \
                        patch.object(eruun_environment, "sandbox_request") as request, \
                        self.assertRaises(runner.RunnerError):
                    asyncio.run(environment._start_sandbox())
                request.assert_not_called()
                self.assertFalse(environment._sandbox_requested)

    def test_harbor_finally_retains_failed_collection_without_direct_pod_deletion(self):
        import eruun_environment
        from harbor.environments.ack import ACKEnvironment
        from harbor.trial.trial import Trial
        with tempfile.TemporaryDirectory() as directory:
            environment, _ = self.environment(directory, sandbox=True)
            environment._sandbox_requested = True
            environment._accept_sandbox_identity(self.sandbox_reply())
            trial = SimpleNamespace(_is_agent_environment_stopped=False, agent_environment=environment,
                                    config=SimpleNamespace(environment=SimpleNamespace(delete=True)), logger=MagicMock(),
                                    _record_exception=MagicMock())
            with patch.object(eruun_environment, "sandbox_request", return_value=self.sandbox_reply(
                    state="retained", retainUntil="2026-09-21T00:00:00Z")) as request, \
                    patch.object(ACKEnvironment, "stop", new_callable=AsyncMock) as direct_stop:
                asyncio.run(Trial._stop_agent_environment(trial))
                asyncio.run(Trial._stop_agent_environment(trial))
            request.assert_called_once()
            self.assertFalse(request.call_args.args[3]["collectionComplete"])
            self.assertEqual(environment._collection["sandboxState"], "retained")
            self.assertTrue(environment._collection["stopped"])
            direct_stop.assert_not_awaited()
            trial._record_exception.assert_not_called()

    def test_uncertain_creation_release_polls_same_intent_within_deadline(self):
        import eruun_environment
        for recover in (True, False):
            with self.subTest(recover=recover), tempfile.TemporaryDirectory() as directory:
                environment, _ = self.environment(directory, sandbox=True)
                environment._sandbox_requested = True
                environment._sandbox_control["finalizationDeadline"] = 3
                now = [0.0]

                async def advance(delay):
                    now[0] += delay

                def reply(*args):
                    if recover and now[0] >= 2:
                        return self.sandbox_reply(state="retained")
                    return self.sandbox_reply(state="pending", admitted=False, sandboxUID="", podUID="",
                                              reason="creation_outcome_unknown")

                with patch.object(eruun_environment.time, "monotonic", side_effect=lambda: now[0]), \
                        patch.object(eruun_environment.asyncio, "sleep", side_effect=advance), \
                        patch.object(eruun_environment, "sandbox_request", side_effect=reply) as request:
                    if recover:
                        asyncio.run(environment._release_sandbox(False))
                        self.assertFalse(environment._collection["releasePending"])
                        self.assertEqual(environment._collection["sandboxState"], "retained")
                    else:
                        with self.assertRaises(TimeoutError):
                            asyncio.run(environment._release_sandbox(False))
                        self.assertTrue(environment._collection["releasePending"])
                        self.assertEqual(now[0], 3)
                self.assertEqual(request.call_count, 3)
                for call in request.call_args_list:
                    self.assertEqual(call.args[1:4], ("POST", "/trial/release",
                                                      {"sandboxUID": "", "podUID": "", "collectionComplete": False}))

    def test_sandbox_missing_storage_fails_before_allocation_without_pod_fallback(self):
        import eruun_environment
        from harbor.environments.ack import ACKEnvironment
        with tempfile.TemporaryDirectory() as directory:
            environment, _ = self.environment(directory, sandbox=True)
            environment.task_env_config.storage_mb = None
            with patch.object(eruun_environment, "sandbox_request") as request, \
                    patch.object(ACKEnvironment, "start", new_callable=AsyncMock) as pod_start, \
                    self.assertRaisesRegex(runner.RunnerError, "storage_mb"):
                asyncio.run(environment.start(False))
            request.assert_not_called()
            pod_start.assert_not_called()

    def test_sandbox_outage_retries_identical_allocation_and_is_bounded(self):
        import eruun_environment
        for recovered in (True, False):
            with self.subTest(recovered=recovered), tempfile.TemporaryDirectory() as directory:
                environment, _ = self.environment(directory, sandbox=True)
                now = [0.0]
                seen = []

                def request(control, method, path, body, deadline):
                    seen.append(json.dumps(body, sort_keys=True))
                    if recovered and len(seen) == 6:
                        return self.sandbox_reply()
                    raise ConnectionResetError("uncertain create acknowledgment")

                async def advance(delay):
                    now[0] += delay

                with patch.object(eruun_environment.time, "monotonic", side_effect=lambda: now[0]), \
                        patch.object(eruun_environment.asyncio, "sleep", side_effect=advance), \
                        patch.object(eruun_environment, "sandbox_request", side_effect=request):
                    operation = environment._sandbox_operation("POST", "", {"trialId": "trial"}, 2000)
                    if recovered:
                        data, paused = asyncio.run(operation)
                        self.assertEqual(data["state"], "ready")
                        self.assertGreater(paused, 0)
                    else:
                        with self.assertRaises(TimeoutError):
                            asyncio.run(operation)
                        self.assertEqual(now[0], 600)
                self.assertGreater(len(seen), 3)
                self.assertTrue(all(item == seen[0] for item in seen))

    def test_sandbox_queue_and_api_outage_do_not_consume_author_build_budget(self):
        import eruun_environment
        with tempfile.TemporaryDirectory() as directory:
            environment, _ = self.environment(directory, sandbox=True)
            environment._sandbox_control["executionDeadline"] = 2000
            environment.task_env_config.build_timeout_sec = 2
            now = [0.0]
            queued = [True]
            admitted = [False]

            def request(control, method, path, body, deadline):
                if queued[0]:
                    queued[0] = False
                    return self.sandbox_reply(state="pending", admitted=False, sandboxUID="", podUID="")
                if not admitted[0]:
                    admitted[0] = True
                    return self.sandbox_reply(state="pending", sandboxUID="", podUID="")
                if now[0] < 1290:
                    raise ConnectionRefusedError()
                return self.sandbox_reply()

            async def advance(delay):
                now[0] += 700 if not admitted[0] else delay

            with patch.object(eruun_environment.time, "monotonic", side_effect=lambda: now[0]), \
                    patch.object(eruun_environment.asyncio, "sleep", side_effect=advance), \
                    patch.object(eruun_environment, "sandbox_request", side_effect=request):
                remaining = asyncio.run(environment._start_sandbox())
            self.assertGreaterEqual(now[0], 1290)
            self.assertAlmostEqual(remaining, 1)

    def test_admitted_pending_startup_stops_at_author_build_budget(self):
        import eruun_environment
        with tempfile.TemporaryDirectory() as directory:
            environment, _ = self.environment(directory, sandbox=True)
            environment._sandbox_control["executionDeadline"] = 2000
            environment.task_env_config.build_timeout_sec = 2
            now = [0.0]

            async def advance(delay):
                now[0] += delay

            with patch.object(eruun_environment.time, "monotonic", side_effect=lambda: now[0]), \
                    patch.object(eruun_environment.asyncio, "sleep", side_effect=advance), \
                    patch.object(eruun_environment, "sandbox_request", return_value=self.sandbox_reply(
                        state="pending", podUID="")) as request, \
                    self.assertRaisesRegex(TimeoutError, "task build timeout"):
                asyncio.run(environment._start_sandbox())
            self.assertEqual(now[0], 2)
            self.assertEqual(request.call_count, 2)

    def test_first_admitted_response_charges_server_startup_not_queue_or_gate(self):
        import eruun_environment
        for queued in (False, True):
            with self.subTest(queued=queued), tempfile.TemporaryDirectory() as directory:
                environment, _ = self.environment(directory, sandbox=True)
                environment._sandbox_control["executionDeadline"] = 2000
                environment.task_env_config.build_timeout_sec = 2
                now = [0.0]
                calls = [0]

                async def wait_for_admission(_deadline):
                    if queued and calls[0] == 1:
                        now[0] += 100  # a closed Runner admission gate is paused

                async def advance(delay):
                    now[0] += delay

                def request(*_args):
                    calls[0] += 1
                    if queued and calls[0] == 1:
                        now[0] += 8  # server-side capacity wait before admission
                        return self.sandbox_reply(state="pending", admitted=False, sandboxUID="", podUID="")
                    now[0] += 5
                    return self.sandbox_reply(admissionAgeSeconds=1 if queued else 3)

                with patch.object(eruun_environment.time, "monotonic", side_effect=lambda: now[0]), \
                        patch.object(eruun_environment.asyncio, "sleep", side_effect=advance), \
                        patch.object(environment, "_wait_for_admission", side_effect=wait_for_admission), \
                        patch.object(eruun_environment, "sandbox_request", side_effect=request):
                    if queued:
                        self.assertAlmostEqual(asyncio.run(environment._start_sandbox()), 1)
                        self.assertEqual(now[0], 114)
                    else:
                        with self.assertRaisesRegex(TimeoutError, "task build timeout"):
                            asyncio.run(environment._start_sandbox())
                        self.assertEqual(now[0], 5)

    def test_first_admission_after_lost_ack_pauses_api_outage(self):
        import eruun_environment
        with tempfile.TemporaryDirectory() as directory:
            environment, _ = self.environment(directory, sandbox=True)
            environment._sandbox_control["executionDeadline"] = 2000
            environment.task_env_config.build_timeout_sec = 2
            now = [0.0]
            calls = [0]

            async def advance(delay):
                now[0] += delay

            def request(*_args):
                calls[0] += 1
                if calls[0] == 1:
                    now[0] += 3
                    raise ConnectionResetError("response lost after admission")
                return self.sandbox_reply(admissionAgeSeconds=now[0])

            with patch.object(eruun_environment.time, "monotonic", side_effect=lambda: now[0]), \
                    patch.object(eruun_environment.asyncio, "sleep", side_effect=advance), \
                    patch.object(eruun_environment, "sandbox_request", side_effect=request):
                self.assertAlmostEqual(asyncio.run(environment._start_sandbox()), 2)
            self.assertEqual(calls[0], 2)

    def test_cancelled_sandbox_allocation_releases_unconfirmed_intent(self):
        import eruun_environment
        from harbor.environments.ack import ACKEnvironment
        with tempfile.TemporaryDirectory() as directory:
            environment, _ = self.environment(directory, sandbox=True)
            with patch.object(eruun_environment, "sandbox_request", side_effect=[asyncio.CancelledError(), self.sandbox_reply(state="released")]) as request, \
                    patch.object(ACKEnvironment, "start", new_callable=AsyncMock) as pod_start, \
                    self.assertRaises(asyncio.CancelledError):
                asyncio.run(environment.start(False))
            self.assertEqual(request.call_args_list[-1].args[2], "/trial/release")
            self.assertEqual(request.call_args_list[-1].args[3], {"sandboxUID": "", "podUID": "", "collectionComplete": False})
            self.assertFalse(environment._collection["releasePending"])
            pod_start.assert_not_called()

    def test_incomplete_collection_retains_sandbox_and_never_deletes_pod(self):
        import eruun_environment
        from harbor.environments.ack import ACKEnvironment
        with tempfile.TemporaryDirectory() as directory:
            environment, _ = self.environment(directory, sandbox=True)
            environment._sandbox_requested = True
            environment._accept_sandbox_identity(self.sandbox_reply())
            with patch.object(eruun_environment, "sandbox_request", return_value=self.sandbox_reply(state="retained", retainUntil="2026-09-21T00:00:00Z")) as request, \
                    patch.object(ACKEnvironment, "stop", new_callable=AsyncMock) as pod_stop:
                asyncio.run(environment.stop(True))
            self.assertFalse(request.call_args.args[3]["collectionComplete"])
            self.assertEqual(environment._collection["sandboxState"], "retained")
            pod_stop.assert_not_called()

    def test_sandbox_exec_checks_uid_and_selects_main_container(self):
        import eruun_environment
        core = MagicMock()
        core.read_namespaced_pod.return_value.metadata.uid = "pod-uid"
        api = eruun_environment.SandboxExecAPI(self.sandbox_reply(), core)
        try:
            self.assertIs(api.connect_get_namespaced_pod_exec.__self__, api)
            with patch.object(eruun_environment.k8s_client.CoreV1Api, "connect_get_namespaced_pod_exec", return_value="response") as execute:
                self.assertEqual(api.connect_get_namespaced_pod_exec("actual-pod", "workspace-test", command=["true"]), "response")
                self.assertEqual(execute.call_args.kwargs["container"], "main")
                core.read_namespaced_pod.return_value.metadata.uid = "replacement"
                with self.assertRaisesRegex(runner.RunnerError, "identity changed"):
                    api.connect_get_namespaced_pod_exec("actual-pod", "workspace-test", command=["true"])
                self.assertEqual(execute.call_count, 1)
        finally:
            api.api_client.close()

    def test_sandbox_http_rejects_unknown_states_and_wrong_ready_identity(self):
        import eruun_environment
        for data in (self.sandbox_reply(state="unknown"), self.sandbox_reply(admitted=None),
                     self.sandbox_reply(admissionAgeSeconds=-1), self.sandbox_reply(admissionAgeSeconds=True),
                     self.sandbox_reply(admissionAgeSeconds="1"),
                     self.sandbox_reply(admitted=False, admissionAgeSeconds=1)):
            connection = MagicMock()
            connection.getresponse.return_value.status = 200
            connection.getresponse.return_value.read.return_value = json.dumps({"code": 0, "data": data}).encode()
            control = config() | {"sandboxURL": "http://platform.test/sandboxes"}
            with patch.dict(os.environ, {"POD_NAME": "runner", "POD_UID": "runner-uid"}), \
                    patch.object(runner.http.client, "HTTPConnection", return_value=connection), \
                    self.assertRaisesRegex(runner.RunnerError, "invalid sandbox"):
                eruun_environment.sandbox_request(control, "POST", "", {"trialId": "trial"}, time.monotonic() + 5)
        with tempfile.TemporaryDirectory() as directory:
            environment, _ = self.environment(directory, sandbox=True)
            for changes in ({"trialId": "other"}, {"namespace": "other"}, {"containerName": "sidecar"},
                            {"podUID": ""}, {"admitted": False}):
                with self.subTest(changes=changes), self.assertRaises(runner.RunnerError):
                    environment._accept_sandbox_identity(self.sandbox_reply(**changes))
            environment._accept_sandbox_identity(self.sandbox_reply())
            with self.assertRaisesRegex(runner.RunnerError, "identity changed"):
                environment._accept_sandbox_identity(self.sandbox_reply(podUID="replacement"))

    def test_harbor_swallowed_download_failure_retains_source_and_marks_incomplete(self):
        from harbor.environments.ack import ACKEnvironment
        from harbor.trial.artifact_handler import ArtifactHandler

        with tempfile.TemporaryDirectory() as directory:
            environment, output = self.environment(directory)
            environment.service_is_dir = AsyncMock(return_value=True)
            environment._download_tar = MagicMock(side_effect=RuntimeError("transfer failed"))
            manifest = asyncio.run(ArtifactHandler(artifacts=[], logger=logging.getLogger("test")).download_artifacts(
                environment, environment.trial_paths.artifacts_dir, source_artifacts_dir=Path("/logs/artifacts")))
            self.assertEqual(manifest.entries[0].status, "failed")
            # A later successful call cannot erase an earlier missing source.
            environment._download_tar.side_effect = None
            asyncio.run(environment.download_dir("/logs/artifacts", output / "later"))
            with patch.object(ACKEnvironment, "stop", new_callable=AsyncMock) as stop:
                asyncio.run(environment.stop(delete=True))
                stop.assert_awaited_once_with(delete=False)
            (output / "run/result.json").write_text(json.dumps(result()))
            report = {"executionStatus": runner.framework_status(output / "run/result.json", 0)}
            runner.check_sandbox_collection(output, 1, report)
            runner.archive_results(output, Path(directory) / "result.tar.gz", report)
            self.assertEqual(report["executionStatus"], "succeeded")  # reward=0 is valid.
            self.assertFalse(report["collectionComplete"])
            reasons = {error["reason"] for error in report["collectionErrors"]}
            self.assertIn("sandbox_download_failed", reasons)
            self.assertIn("native_artifact_collection_failed", reasons)

    def test_harbor_swallowed_agent_log_and_filtered_transfer_failures_are_recorded(self):
        from harbor.environments.base import ExecResult
        from harbor.trial.trial import Trial

        with tempfile.TemporaryDirectory() as directory:
            environment, output = self.environment(directory)
            environment._download_tar = MagicMock(side_effect=RuntimeError("disconnected"))
            trial = SimpleNamespace(agent_environment=environment, logger=MagicMock())
            asyncio.run(Trial._download_role_logs(trial,
                agent_config=SimpleNamespace(include_logs=[], exclude_logs=[]),
                source_dir=Path("/logs/agent"), target_dir=output / "agent"))
            self.assertEqual(environment._collection["errorCount"], 1)
            # Fail before download_file, inside Harbor's filtering/tar setup.
            environment.exec = AsyncMock(return_value=ExecResult(return_code=1))
            for operation in (environment.download_dir_filtered, environment.download_dir_with_exclusions):
                with self.subTest(operation=operation.__name__), self.assertRaises(RuntimeError):
                    asyncio.run(operation(source_dir="/logs/artifacts", target_dir=output / "filtered", exclude=["*.tmp"]))
            self.assertEqual(environment._collection["errorCount"], 3)

    def test_complete_trials_release_pods_and_empty_artifacts_are_valid(self):
        from harbor.environments.ack import ACKEnvironment
        from harbor.trial.artifact_handler import ArtifactHandler

        with tempfile.TemporaryDirectory() as directory:
            for name in ("first", "second"):
                environment, output = self.environment(directory, name)
                environment.service_is_dir = AsyncMock(return_value=True)
                environment._download_tar = MagicMock()
                manifest = asyncio.run(ArtifactHandler(artifacts=[], logger=logging.getLogger("test")).download_artifacts(
                    environment, environment.trial_paths.artifacts_dir, source_artifacts_dir=Path("/logs/artifacts")))
                self.assertEqual(manifest.entries[0].status, "ok")
                with patch.object(ACKEnvironment, "stop", new_callable=AsyncMock) as stop:
                    asyncio.run(environment.stop(delete=False))
                    stop.assert_awaited_once_with(delete=True)
            report = {"executionStatus": "succeeded"}
            runner.check_sandbox_collection(output, 2, report)
            runner.archive_results(output, Path(directory) / "result.tar.gz", report)
            self.assertTrue(report["collectionComplete"])

    def test_missing_or_invalid_manifest_prevents_source_deletion(self):
        from harbor.environments.ack import ACKEnvironment

        with tempfile.TemporaryDirectory() as directory:
            environment, output = self.environment(directory)
            manifest = environment.trial_paths.artifacts_dir / "manifest.json"
            manifest.parent.mkdir(parents=True)
            for contents in (None, "not json", "[]", '[{"status":"failed","source":"/logs/artifacts"}]'):
                if contents is not None:
                    manifest.write_text(contents)
                with self.subTest(contents=contents), patch.object(ACKEnvironment, "stop", new_callable=AsyncMock) as stop:
                    asyncio.run(environment.stop(delete=True))
                    stop.assert_awaited_once_with(delete=False)
                    report = {}
                    runner.check_sandbox_collection(output, 1, report)
                    self.assertGreater(report["collectionErrorCount"], 0)

    def test_failed_state_write_and_cancelled_transfer_fail_closed(self):
        from harbor.environments.ack import ACKEnvironment

        with tempfile.TemporaryDirectory() as directory:
            environment, output = self.environment(directory)
            environment._ensure_client.side_effect = asyncio.CancelledError()
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(environment.download_file("/logs/agent/log", output / "log"))
            self.assertEqual(environment._collection["errorCount"], 1)
            with patch.object(Path, "write_text", side_effect=OSError("disk full")), self.assertRaises(OSError):
                environment._save_collection()
            with patch.object(ACKEnvironment, "stop", new_callable=AsyncMock) as stop:
                asyncio.run(environment.stop(delete=True))
                stop.assert_awaited_once_with(delete=False)
            persisted = json.loads(environment._collection_path.read_text())
            self.assertEqual(persisted["errorCount"], 2)
            self.assertEqual(persisted["pending"], 0)

    def test_kubernetes_download_preserves_binary_frames(self):
        from eruun_environment import WorkspaceEnvironment
        import eruun_environment

        payload = bytes(range(256)) * 17
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:") as archive:
            info = tarfile.TarInfo("artifact.bin")
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))
        response = MagicMock()
        response.is_open.side_effect = [True, False]
        response.peek_stdout.return_value = True
        response.peek_stderr.return_value = False
        response.read_stdout.return_value = buffer.getvalue()
        response.returncode = 0
        environment = object.__new__(WorkspaceEnvironment)
        environment._exec_api = MagicMock()
        environment.pod_name, environment.namespace = "trial", "workspace"
        with tempfile.TemporaryDirectory() as directory, patch.object(eruun_environment, "stream", return_value=response) as connect:
            environment._download_tar(["tar", "cf", "-", "."], Path(directory))
            self.assertEqual((Path(directory) / "artifact.bin").read_bytes(), payload)
            self.assertTrue(connect.call_args.kwargs["binary"])
            response.close.assert_called_once()

    def test_real_job_config_and_ack_never_build_or_escalate(self):
        from harbor.models.job.config import JobConfig
        from harbor.models.task.config import EnvironmentConfig
        from harbor.models.trial.paths import TrialPaths
        from harbor.environments.base import ExecResult
        from eruun_environment import WorkspaceEnvironment

        with tempfile.TemporaryDirectory() as directory:
            generated = runner.harbor_config(config(), [EXAMPLE], Path(directory) / "outputs", "runner-pod", "runner-uid")
            model = JobConfig.model_validate(generated)
            self.assertEqual(model.environment.import_path, "eruun_environment:WorkspaceEnvironment")
            environment = WorkspaceEnvironment(
                environment_dir=EXAMPLE / "environment", environment_name="greeting",
                session_id="trial-test", trial_paths=TrialPaths(Path(directory) / "trial"),
                task_env_config=EnvironmentConfig(docker_image="example.com/task:1.0.0"),
                **generated["environment"]["kwargs"],
            )
            core_api = MagicMock()
            environment._core_api = core_api
            environment._batch_api = MagicMock()
            environment._ensure_client = AsyncMock()
            environment._wait_for_pod_ready = AsyncMock()
            environment.exec = AsyncMock(return_value=ExecResult(return_code=0))
            environment._upload_environment_dir_after_start = AsyncMock()
            environment._build_and_push_image = AsyncMock()
            asyncio.run(environment.start(False))
            environment._build_and_push_image.assert_not_awaited()
            pod = core_api.create_namespaced_pod.call_args.kwargs["body"]
            self.assertEqual(pod["metadata"]["namespace"], "workspace-test")
            self.assertEqual(pod["metadata"]["ownerReferences"][0]["uid"], "runner-uid")
            security = pod["spec"]["containers"][0]["securityContext"]
            self.assertFalse(security["privileged"])
            self.assertTrue(security["runAsNonRoot"])
            self.assertEqual(security["runAsUser"], 1000)
            self.assertEqual(security["capabilities"], {"drop": ["ALL"]})
            self.assertFalse(pod["spec"]["automountServiceAccountToken"])
            self.assertEqual(pod["spec"]["activeDeadlineSeconds"], 60)
            self.assertEqual(pod["spec"]["serviceAccountName"], "task-sandbox")
            self.assertIsNone(environment._resolve_user("root"))
            self.assertIsNone(environment._resolve_user(1000))

    def test_real_cli_accepts_generated_config(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            generated = runner.harbor_config(config(), [EXAMPLE], root / "output", "runner-pod", "runner-uid")
            file = root / "job.json"
            file.write_text(json.dumps(generated))
            cli = Path(os.sys.executable).parent / "harbor"
            completed = subprocess.run([str(cli), "run", "-c", str(file), "--print-config"],
                                       capture_output=True, text=True, timeout=30)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            resolved = json.loads(completed.stdout)
            self.assertEqual(resolved["environment"]["import_path"], "eruun_environment:WorkspaceEnvironment")
            self.assertFalse(resolved["environment"].get("force_build", False))


if __name__ == "__main__":
    unittest.main()

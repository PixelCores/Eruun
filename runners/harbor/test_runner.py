import asyncio
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import threading
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import runner


EXAMPLE = Path(__file__).resolve().parents[2] / "examples/agent-evaluation/harbor-task"


def config():
    return {
        "taskId": "task-test", "namespace": "workspace-test",
        "datasetURL": "http://platform.test/input", "resultURL": "http://platform.test/result",
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
        with patch.object(runner.http.client, "HTTPConnection", return_value=connection):
            runner.upload_results(config(), source, "failed")
        connection.putrequest.assert_called_once_with("POST", "/result")
        connection.putheader.assert_any_call("X-Eruun-Runner-Pod-UID", "runner-uid")
        connection.putheader.assert_any_call("X-Eruun-Evaluation-Status", "failed")
        connection.send.assert_called_once_with(b"complete raw archive")
        connection.getresponse.return_value.status = 409
        with patch.object(runner.http.client, "HTTPConnection", return_value=connection), self.assertRaises(runner.RunnerError):
            runner.upload_results(config(), source, "succeeded")

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

    def test_collection_limits_emit_incomplete_diagnostic(self):
        output = self.root / "output"
        output.mkdir()
        (output / "file").write_bytes(b"original")
        report = {"executionStatus": "succeeded"}
        with patch.object(runner, "MAX_FILES", 2):
            runner.archive_results(output, self.root / "archive", report)
        self.assertFalse(report["collectionComplete"])
        self.assertEqual(report["collectionErrors"][0]["reason"], "too_many_entries")

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
                self.assertEqual(archive.extractfile("outputs/partial.log").read(), b"before termination")
        with patch.object(runner.importlib.metadata, "version", return_value=runner.FRAMEWORK_VERSION), patch.object(runner, "download_package", side_effect=download), patch.object(runner, "run_framework", side_effect=framework), patch.object(runner, "upload_results", side_effect=upload):
            self.assertEqual(runner.execute(config(), self.root, threading.Event()), 1)

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
            generated = runner.harbor_config(config(), [EXAMPLE], Path(directory), "runner-pod", "runner-uid")
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

import asyncio
import io
from http.server import BaseHTTPRequestHandler
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch
import uuid

os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "True")

import native_checkpoint as native
import recovery_runtime as recovery
import runner
from test_runner import config as base_config, local_http_server


def config(agent="codex"):
    return base_config() | {"agent": {"name": agent, "model": "fixture-model"},
                            "sandboxURL": "http://platform.test/sandbox", "checkpointURL": "http://platform.test/checkpoints",
                            "recovery": {"agentVersion": recovery.AGENT_VERSIONS[agent], "replaySafe": True, "checkpointIntervalSeconds": 60},
                            "executionDeadlineMonotonic": time.monotonic() + 90}


def manifest():
    session = str(uuid.uuid4())
    return {"version": 1, "harborVersion": "0.22.0", "agent": "codex", "agentVersion": "0.154.0", "datasetDigest": "a" * 64,
            "remainingSeconds": 60, "completedTrialIds": [],
            "members": [{"trialId": "trial-a__env", "sessionId": session, "stage": "agent"}],
            "trials": [{"config": {"trial_name": "trial-a", "task": {"path": "dataset/a"}}, "stage": "agent", "sessionId": session,
                        "remainingAgentSeconds": 50, "agentDeadline": time.time() + 50,
                        "collection": {"started": True, "stopped": False, "pending": 0, "errorCount": 0, "errors": []}}], "files": []}


class RecoveryMaterialTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def test_opt_in_requires_replay_and_exact_agent_version(self):
        for agent in recovery.AGENT_VERSIONS:
            runner.validate_config(config(agent))
        for change in ({"replaySafe": False}, {"agentVersion": "latest"}, {"checkpointIntervalSeconds": True}, {"checkpointIntervalSeconds": 59}):
            value = config()
            value["recovery"].update(change)
            with self.subTest(change=change), self.assertRaises(runner.RunnerError):
                runner.validate_config(value)
        with self.assertRaises(runner.RunnerError):
            runner.validate_config(base_config() | {"resumeCheckpointId": "id"})

    def test_bundle_survives_source_removal_and_excludes_control(self):
        source = self.root / "source"
        path = source / "outputs/run/trial-a/agent/session.jsonl"
        path.parent.mkdir(parents=True)
        path.write_text('{"history":"kept"}\n')
        (source / "sandbox-control.json").write_text('{"token":"old-secret"}')
        (path.parent / "eruun-native").mkdir()
        (path.parent / "eruun-native/auth.json").write_text('{"token":"old-secret"}')
        material = self.root / "material.tar.gz"
        recovery.create_bundle(source, manifest(), material)
        shutil.rmtree(source)
        restored = self.root / "new-runner"
        restored.mkdir()
        loaded = recovery.load_bundle(material, restored, config())
        self.assertEqual(len(loaded["files"]), 1)
        self.assertEqual((restored / path.relative_to(source)).read_text(), '{"history":"kept"}\n')
        with tarfile.open(material) as archive:
            self.assertFalse(any("old-secret" in archive.extractfile(entry).read().decode() for entry in archive))

    def test_user_artifact_basenames_survive_and_credentials_fail_closed(self):
        work = self.root / "source"
        artifacts = work / "outputs/run/trial-a/artifacts"
        artifacts.mkdir(parents=True)
        (artifacts / "config.json").write_text('{"user":1}')
        (artifacts / "result.json").write_text('{"reward":1}')
        archive = self.root / "material.tar.gz"
        recovery.create_bundle(work, manifest(), archive)
        with tarfile.open(archive) as bundle:
            names = bundle.getnames()
        self.assertIn("outputs/run/trial-a/artifacts/config.json", names)
        self.assertIn("outputs/run/trial-a/artifacts/result.json", names)
        (artifacts / "result.json").write_text("sensitive-runtime-key")
        with self.assertRaisesRegex(runner.RunnerError, "credentials"):
            recovery.create_bundle(work, manifest(), archive, {"sensitive-runtime-key"})

    def test_manifest_size_boundary_matches_server_one_mib_limit(self):
        work = self.root / "source"
        (work / "outputs/run").mkdir(parents=True)
        value = manifest()
        value["padding"] = ""
        base_size = len(json.dumps(value, separators=(",", ":")).encode())
        value["padding"] = "x" * (1024 * 1024 - base_size)
        material = self.root / "material.tar.gz"
        recovery.create_bundle(work, value, material)
        with tarfile.open(material) as archive:
            self.assertEqual(archive.getmember("checkpoint.json").size, 1024 * 1024)
        restored = self.root / "restored"
        restored.mkdir()
        recovery.load_bundle(material, restored, config())
        value["padding"] += "x"
        with self.assertRaisesRegex(runner.RunnerError, "manifest exceeds size limit"):
            recovery.create_bundle(work, value, material)
        oversized = self.archive([("checkpoint.json", json.dumps(value, separators=(",", ":")).encode(), tarfile.REGTYPE)])
        with self.assertRaisesRegex(runner.RunnerError, "manifest exceeds size limit"):
            recovery.load_bundle(oversized, restored, config())

    def test_oversized_file_rejected_before_reading_into_memory(self):
        work = self.root / "source"
        output = work / "outputs/run/trial-a/agent/data"
        output.parent.mkdir(parents=True)
        output.write_bytes(b"large")
        with patch.object(recovery, "MAX_CHECKPOINT_EXPANDED_BYTES", 1), patch.object(Path, "read_bytes", side_effect=AssertionError("read before limit")):
            with self.assertRaisesRegex(runner.RunnerError, "material limit"):
                recovery.create_bundle(work, manifest(), self.root / "archive")

    def archive(self, entries):
        output = self.root / (uuid.uuid4().hex + ".tar.gz")
        with tarfile.open(output, "w:gz") as archive:
            for name, raw, kind in entries:
                info = tarfile.TarInfo(name)
                info.size, info.type = len(raw), kind
                archive.addfile(info, io.BytesIO(raw) if kind == tarfile.REGTYPE else None)
        return output

    def test_material_rejects_corruption_links_and_member_mismatch_before_writes(self):
        valid = manifest()
        cases = [
            [("../escape", b"bad", tarfile.REGTYPE)],
            [("checkpoint.json", b"{}", tarfile.SYMTYPE)],
            [("checkpoint.json", b"{}", tarfile.REGTYPE), ("checkpoint.json", b"{}", tarfile.REGTYPE)],
            [("checkpoint.json", json.dumps(valid | {"members": []}).encode(), tarfile.REGTYPE)],
            [("checkpoint.json", json.dumps(valid | {"agentVersion": "changed"}).encode(), tarfile.REGTYPE)],
            [("checkpoint.json", json.dumps(valid | {"files": [{"path": "outputs/run/test", "size": 3, "digest": "0" * 64}]}).encode(), tarfile.REGTYPE),
             ("outputs/run/test", b"bad", tarfile.REGTYPE)],
        ]
        for case in cases:
            destination = self.root / uuid.uuid4().hex
            destination.mkdir()
            with self.subTest(case=case), self.assertRaises(runner.RunnerError):
                recovery.load_bundle(self.archive(case), destination, config())
            self.assertEqual(list(destination.iterdir()), [])

    def test_uncertain_upload_reuses_exact_id_and_bytes_then_polls(self):
        archive = self.root / "archive"
        archive.write_bytes(b"immutable")
        calls = []
        def request(config, identity, method, deadline, source=None):
            calls.append((identity, method, source.read_bytes() if source else None))
            if len(calls) == 1:
                raise OSError("response lost")
            return {"id": identity, "state": "pending" if method == "POST" else "ready"}
        with patch.object(recovery, "checkpoint_request", side_effect=request), patch.object(recovery.time, "sleep"):
            recovery.publish_checkpoint(config(), "same-id", archive, time.monotonic() + 10)
        self.assertEqual(calls, [("same-id", "POST", b"immutable"), ("same-id", "POST", b"immutable"), ("same-id", "GET", None)])

    def test_failed_snapshot_is_not_ready(self):
        with patch.object(recovery, "checkpoint_request", return_value={"state": "failed"}), self.assertRaises(runner.RunnerError):
            recovery.publish_checkpoint(config(), "id", self.root / "unused", time.monotonic() + 10)

    def test_cancelled_publication_stops_after_a_bounded_in_flight_request(self):
        stop = threading.Event()
        requests = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_POST(self):
                self.rfile.read(int(self.headers["Content-Length"]))
                requests.append(self.path)
                stop.set()
                self.server.stop.wait(1)
        archive = self.root / "archive"
        archive.write_bytes(b"fixture")
        with local_http_server(Handler) as server:
            cfg = config() | {"checkpointURL": f"http://127.0.0.1:{server.server_port}/checkpoints"}
            started = time.monotonic()
            with patch.dict(os.environ, {"POD_NAME": "fixture-runner", "POD_UID": "fixture-uid"}), \
                    patch.object(recovery, "EVENT_ATTEMPT_SECONDS", 0.05), \
                    self.assertRaisesRegex(runner.RunnerError, "publication cancelled"):
                recovery.publish_checkpoint(cfg, "id", archive, started + 3600, stop)
            self.assertLess(time.monotonic() - started, 1)
            self.assertEqual(requests, ["/checkpoints/id"])

    def test_quiescence_rejects_detached_process_and_pid_reuse(self):
        baseline = {1: (0, "1"), 20: (1, "200")}
        for processes in (baseline | {33: (1, "300")}, {1: (0, "1"), 20: (1, "999")}):
            with patch.object(native, "processes", return_value=processes), self.assertRaises(native.BoundaryError):
                native.assert_quiescent(baseline)
        with patch.object(native, "processes", return_value=baseline):
            native.assert_quiescent(baseline)

    def test_session_boundary_requires_complete_records_and_exact_identity(self):
        identity = str(uuid.uuid4())
        directory = self.root / "state/sessions"
        directory.mkdir(parents=True)
        session = directory / (identity + ".jsonl")
        session.write_text('{"type":"session_meta"}\n')
        output = self.root / "output"
        output.write_text(json.dumps({"thread_id": identity}) + "\n")
        with patch.object(native, "STATE", directory.parent):
            self.assertEqual(native.saved_session("codex", output, identity), identity)
            with self.assertRaises(native.BoundaryError):
                native.saved_session("codex", output, str(uuid.uuid4()))
            session.write_text('{"partial":')
            with self.assertRaises(native.BoundaryError):
                native.saved_session("codex", output)

    def test_resume_commands_use_explicit_session_never_last(self):
        identity = str(uuid.uuid4())
        for agent in recovery.AGENT_VERSIONS:
            command = native.command(agent, "model", "continue", identity)
            self.assertIn(identity, command)
            self.assertNotIn("--last", command)
            self.assertNotIn("--continue", command)


class RecoveryLifecycleTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        for task in ("completed", "interrupted"):
            directory = self.root / "source/dataset" / task
            (directory / "environment").mkdir(parents=True)
            (directory / "tests").mkdir()
            (directory / "task.toml").write_text('version="1.0"\n[environment]\ndocker_image="alpine:3.22"\n')
            (directory / "instruction.md").write_text("fixture")
            (directory / "environment/Dockerfile").write_text("FROM alpine:3.22")
            (directory / "tests/test.sh").write_text("exit 0")
        (self.root / "source/outputs").mkdir()
        (self.root / "source/collection").mkdir()

    def generated(self, root):
        cfg = config()
        generated = runner.harbor_config(cfg, sorted((root / "dataset").iterdir()), root / "outputs", "new-runner", "new-pod-uid")
        generated["environment"]["kwargs"].pop("sandbox_control_file", None)
        return generated

    async def test_completed_trial_skipped_original_ids_and_paths_preserved_after_source_deleted(self):
        from harbor.models.trial.result import AgentInfo, TrialResult
        source = self.root / "source"
        first = recovery.RecoveryJob(config(), self.generated(source), source)
        await first.initialize()
        completed, interrupted = first.records
        for record, stage in ((completed, "completed"), (interrupted, "agent")):
            record["stage"] = stage
            trial_config = recovery.restored_config(record["config"], first.generated, source)
            result = TrialResult(task_name=trial_config.task.path.name, trial_name=trial_config.trial_name,
                                 trial_uri="fixture://trial", task_id=trial_config.task.get_task_id(), task_checksum="fixture",
                                 config=trial_config, source="uploaded", agent_info=AgentInfo(name="codex", version="0.154.0"),
                                 started_at="2026-09-24T00:00:00Z", finished_at="2026-09-24T00:01:00Z" if stage == "completed" else None)
            record["result"] = recovery.stored_result(result, record["config"])
        completed["collection"] = {"started": True, "stopped": True, "pending": 0, "errorCount": 0, "errors": []}
        interrupted.update(sessionId=str(uuid.uuid4()), remainingAgentSeconds=50, agentDeadline=time.time() + 50,
                           collection={"started": True, "stopped": False, "pending": 0, "errorCount": 0, "errors": []})
        header = manifest() | {"trials": first.records, "completedTrialIds": [completed["config"]["trial_name"] + "__env"],
                               "members": [{"trialId": interrupted["config"]["trial_name"] + "__env", "sessionId": interrupted["sessionId"], "stage": "agent"}]}
        archive = self.root / "checkpoint.tar.gz"
        recovery.create_bundle(source, header, archive)
        restored = self.root / "restored"
        shutil.copytree(source / "dataset", restored / "dataset")
        (restored / "outputs").mkdir()
        (restored / "collection").mkdir()
        shutil.rmtree(source)
        cfg = config() | {"resumeCheckpointId": "checkpoint"}
        resumed = recovery.RecoveryJob(cfg, self.generated(restored), restored)
        with patch.object(recovery, "download_checkpoint", side_effect=lambda cfg, path, deadline: shutil.copyfile(archive, path)):
            await resumed.initialize()
        self.assertEqual([record["config"]["trial_name"] for record in resumed.records], [record["config"]["trial_name"] for record in first.records])
        self.assertEqual([record["stage"] for record in resumed.records], ["completed", "agent"])
        observed = []
        async def finish(record):
            observed.append(record["config"]["trial_name"])
            trial_config = recovery.restored_config(record["config"], resumed.generated, restored)
            result = recovery.restored_result(record["result"], trial_config)
            result.finished_at = result.started_at
            record["result"] = recovery.stored_result(result, record["config"])
            record["stage"] = "completed"
            resumed.active[trial_config.trial_name]["state"] = "completed"
            resumed.changed.set()
            return result
        with patch.object(resumed, "initialize", AsyncMock()), patch.object(resumed, "run_trial", side_effect=finish):
            await resumed.run()
        self.assertEqual(observed, [interrupted["config"]["trial_name"]])
        self.assertFalse(source.exists())
        final = json.loads((restored / "outputs/run/result.json").read_text())
        self.assertEqual(final["n_total_trials"], 2)
        self.assertTrue(all(result["config"]["task"]["path"].startswith(str(restored)) for result in final["trial_results"]))

    async def test_real_harbor_lifecycle_clones_and_resumes_without_restarting_completed_trial(self):
        from harbor.trial.trial import Trial
        from harbor.models.verifier.result import VerifierResult
        original_create = Trial.create
        source = self.root / "source"
        durable = self.root / "durable.tar.gz"
        requests = []
        restored_mode = False
        class SourceLost(Exception):
            pass
        async def create(trial_config):
            trial = await original_create(trial_config)
            environment = trial.agent_environment
            async def start(force_build):
                environment._restored = restored_mode and environment._require_restored
                environment._collection["started"] = True
                environment._save_collection()
            async def stop(delete):
                environment._collection["stopped"] = True
                environment._save_collection()
            async def execute(command, **kwargs):
                request = json.loads(kwargs["env"]["ERUUN_NATIVE_REQUEST"])
                requests.append((restored_mode, trial.task.name, request))
                sid = request.get("sessionId") or str(uuid.uuid4())
                paused = trial.task.name == "interrupted" and not restored_mode
                return SimpleNamespace(return_code=0, stdout=json.dumps({"sessionId": sid, "paused": paused, "quiescent": True}))
            async def download(remote, local):
                Path(local).mkdir(parents=True, exist_ok=True)
            async def artifacts(**kwargs):
                trial.paths.artifacts_dir.mkdir(parents=True, exist_ok=True)
                (trial.paths.artifacts_dir / "manifest.json").write_text('[{"status":"ok"}]')
            async def verifier():
                trial.result.verifier_result = VerifierResult(rewards={"reward": 1})
            environment.start, environment.stop, environment.exec = start, stop, execute
            environment.upload_file = AsyncMock()
            environment.upload_dir = AsyncMock()
            environment.download_dir = download
            environment.run_healthcheck = AsyncMock()
            trial.agent.setup = AsyncMock()
            trial.agent._extra_env = {"OPENAI_API_KEY": "loopback-fixture-only"}
            trial._collect_artifacts = artifacts
            trial._run_verifier = verifier
            return trial
        def persist_and_lose(cfg, identity, archive, deadline, stop):
            shutil.copyfile(archive, durable)
            raise SourceLost()
        with patch.object(Trial, "create", side_effect=create), patch.object(recovery, "publish_checkpoint", side_effect=persist_and_lose):
            job = recovery.RecoveryJob(config() | {"options": {"attempts": 1, "concurrency": 2}}, self.generated(source), source)
            try:
                await job.run()
            except SourceLost:
                pass
            else:
                self.fail(str([record.get("result", {}).get("exception_info") for record in job.records]))
        with tarfile.open(durable) as archive:
            saved = json.load(archive.extractfile("checkpoint.json"))
        self.assertEqual(len(saved["completedTrialIds"]), 1)
        self.assertEqual(len(saved["members"]), 1)
        restored = self.root / "fresh-work"
        shutil.copytree(source / "dataset", restored / "dataset")
        (restored / "outputs").mkdir()
        (restored / "collection").mkdir()
        shutil.rmtree(source)
        restored_mode = True
        resumed = recovery.RecoveryJob(config() | {"resumeCheckpointId": "point", "options": {"attempts": 1, "concurrency": 2}}, self.generated(restored), restored)
        with patch.object(Trial, "create", side_effect=create), patch.object(recovery, "download_checkpoint", side_effect=lambda cfg, path, deadline: shutil.copyfile(durable, path)):
            await resumed.run()
        resumed_requests = [request for restored_call, task, request in requests if restored_call]
        self.assertEqual(len(resumed_requests), 1)
        self.assertEqual(resumed_requests[0]["sessionId"], saved["members"][0]["sessionId"])
        self.assertEqual(resumed_requests[0]["prompt"], recovery.RESUME_PROMPT)
        self.assertEqual(runner.framework_status(restored / "outputs/run/result.json", 0), "succeeded")
        final = json.loads((restored / "outputs/run/result.json").read_text())
        self.assertEqual(next(iter(final["stats"]["evals"].values()))["metrics"], [{"mean": 1.0}])
        self.assertFalse(source.exists())

    async def test_pending_snapshot_survives_agent_timeout_publication_failure_and_cancellation(self):
        from harbor.trial.trial import Trial
        from harbor.models.verifier.result import VerifierResult
        original_create = Trial.create
        for outcome in ("ready", "failed", "cancelled"):
            with self.subTest(outcome=outcome):
                work = self.root / outcome
                task = work / "dataset/task"
                shutil.copytree(self.root / "source/dataset/completed", task)
                with (task / "task.toml").open("a") as target:
                    target.write("\n[agent]\ntimeout_sec=0.1\n")
                (work / "outputs").mkdir()
                (work / "collection").mkdir()
                cfg = config() | {"options": {"attempts": 1, "concurrency": 1}}
                generated = self.generated(work)
                job = recovery.RecoveryJob(cfg, generated, work)
                entered = asyncio.Event()
                finish, publisher_done = threading.Event(), threading.Event()
                loop = asyncio.get_running_loop()
                timeline, requests = [], []

                async def create(trial_config):
                    trial = await original_create(trial_config)
                    environment = trial.agent_environment
                    async def start(force_build):
                        environment._collection["started"] = True
                        environment._save_collection()
                    async def stop(delete):
                        timeline.append("stop")
                        environment._collection["stopped"] = True
                        environment._save_collection()
                    async def execute(command, **kwargs):
                        requests.append(json.loads(kwargs["env"]["ERUUN_NATIVE_REQUEST"]))
                        return SimpleNamespace(return_code=0, stdout=json.dumps({"sessionId": str(uuid.uuid4()), "paused": True, "quiescent": True}))
                    async def download(remote, local):
                        if entered.is_set():
                            timeline.append("download")
                        Path(local).mkdir(parents=True, exist_ok=True)
                    async def artifacts(**kwargs):
                        timeline.append("artifacts")
                        trial.paths.artifacts_dir.mkdir(parents=True, exist_ok=True)
                        (trial.paths.artifacts_dir / "manifest.json").write_text('[{"status":"ok"}]')
                    async def verifier():
                        timeline.append("verifier")
                        trial.result.verifier_result = VerifierResult(rewards={"reward": 1})
                    environment.start, environment.stop, environment.exec = start, stop, execute
                    environment.upload_file = AsyncMock()
                    environment.upload_dir = AsyncMock(side_effect=lambda *args: timeline.append("upload"))
                    environment.download_dir = download
                    environment.run_healthcheck = AsyncMock()
                    trial.agent.setup = AsyncMock()
                    trial.agent._extra_env = {"OPENAI_API_KEY": "loopback-fixture-only"}
                    trial._collect_artifacts, trial._run_verifier = artifacts, verifier
                    return trial

                def publish(cfg, identity, archive, deadline, stop):
                    loop.call_soon_threadsafe(entered.set)
                    try:
                        while not finish.is_set():
                            if stop.wait(0.01):
                                return
                            runner.remaining_time(deadline)
                        if outcome == "failed":
                            raise runner.RunnerError("snapshot publication failed")
                    finally:
                        publisher_done.set()

                with patch.object(Trial, "create", side_effect=create), patch.object(recovery, "publish_checkpoint", side_effect=publish):
                    operation = asyncio.create_task(job.run())
                    try:
                        await asyncio.wait_for(entered.wait(), timeout=2)
                        await asyncio.sleep(0.15)
                        self.assertEqual(timeline, [], "no source I/O may cross the pending barrier")
                        self.assertFalse(operation.done())
                        if outcome == "cancelled":
                            operation.cancel()
                            with self.assertRaises(asyncio.CancelledError):
                                await asyncio.wait_for(operation, timeout=1)
                        else:
                            finish.set()
                            if outcome == "failed":
                                with self.assertRaisesRegex(runner.RunnerError, "publication failed"):
                                    await asyncio.wait_for(operation, timeout=1)
                            else:
                                await asyncio.wait_for(operation, timeout=1)
                        self.assertTrue(await asyncio.to_thread(publisher_done.wait, 1))
                        if outcome == "ready":
                            self.assertIn("verifier", timeline)
                            self.assertIn("stop", timeline)
                            self.assertEqual(job.records[0]["result"]["exception_info"]["exception_type"], "AgentTimeoutError")
                        else:
                            self.assertEqual(timeline, [], "failed or uncertain snapshots must retain their source")
                        self.assertEqual(len(requests), 1, "snapshot delay must consume the original agent budget")
                    finally:
                        finish.set()
                        operation.cancel()
                        await asyncio.gather(operation, return_exceptions=True)

    async def test_metrics_include_completed_resumed_pending_and_failed_trials(self):
        from harbor.models.trial.result import AgentInfo, ExceptionInfo, TrialResult
        from harbor.models.verifier.result import VerifierResult
        work = self.root / "source"
        generated = self.generated(work) | {"n_attempts": 2}
        job = recovery.RecoveryJob(config() | {"options": {"attempts": 2, "concurrency": 2}}, generated, work)
        await job.initialize()
        for index, record in enumerate(job.records):
            trial_config = recovery.restored_config(record["config"], generated, work)
            result = TrialResult(task_name=trial_config.task.path.name, trial_name=trial_config.trial_name,
                                 trial_uri="fixture://trial", task_id=trial_config.task.get_task_id(), task_checksum="fixture",
                                 config=trial_config, source="uploaded", agent_info=AgentInfo(name="codex", version="0.154.0"))
            # Both tasks have a successful first attempt. The other attempts
            # exercise a zero reward and an execution failure with no reward.
            if index == 3:
                result.exception_info = ExceptionInfo.from_exception(RuntimeError("fixture failure"))
            else:
                result.verifier_result = VerifierResult(rewards={"reward": int(index < 2)})
            record["result"] = recovery.stored_result(result, record["config"])
            record["stage"] = ("completed", "agent", "pending", "completed")[index]
        ran = []
        async def finish(record):
            ran.append(record["stage"])
            record["stage"] = "completed"
            job.active[record["config"]["trial_name"]]["state"] = "completed"
            job.changed.set()
        with patch.object(job, "initialize", AsyncMock()), patch.object(job, "run_trial", side_effect=finish):
            await job.run()
        self.assertCountEqual(ran, ["agent", "pending"])
        final = json.loads((work / "outputs/run/result.json").read_text())
        evaluation = final["stats"]["evals"]["codex__uploaded"]
        self.assertEqual(evaluation["metrics"], [{"mean": 0.5}])
        self.assertEqual(evaluation["pass_at_k"], {"2": 1.0})
        self.assertEqual(final["stats"]["n_errored_trials"], 1)

    async def test_resume_does_not_reset_expired_agent_deadline(self):
        job = recovery.RecoveryJob(config(), {}, self.root / "source")
        record = manifest()["trials"][0]
        record["agentDeadline"] = time.time() - 1
        trial = SimpleNamespace(config=SimpleNamespace(trial_name="trial-a"))
        environment = SimpleNamespace(upload_file=AsyncMock(), exec=AsyncMock())
        job.active["trial-a"] = {}
        with self.assertRaises(TimeoutError):
            await job.native_run(record, trial, "initial", environment, None)
        environment.exec.assert_not_called()

    async def test_native_io_keeps_the_original_agent_timeout(self):
        job = recovery.RecoveryJob(config(), {}, self.root / "source")
        job.active["trial-a"] = {"agentTimeoutSeconds": 0.02}
        trial = SimpleNamespace(config=SimpleNamespace(trial_name="trial-a"),
                                agent=SimpleNamespace(model_connection=SimpleNamespace(api_key="fixture-key", configured_base_url=None)))
        cancelled = asyncio.Event()
        requests = []
        async def execute(command, **kwargs):
            requests.append(json.loads(kwargs["env"]["ERUUN_NATIVE_REQUEST"]))
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        environment = SimpleNamespace(upload_file=AsyncMock(), exec=execute)
        started = time.monotonic()
        with self.assertRaises(TimeoutError):
            await job.native_run({"stage": "pending"}, trial, "instruction", environment, None)
        self.assertLess(time.monotonic() - started, 0.5)
        self.assertTrue(cancelled.is_set())
        self.assertLessEqual(requests[0]["seconds"], 0.02)
        self.assertNotIn("release", job.active["trial-a"])

    async def test_native_launch_resolves_nvm_and_standard_cli_installations(self):
        for agent, install in (("codex", ".nvm/versions/node/v22/bin"),
                               ("codex", ".npm-global/bin"), ("claude-code", ".local/bin")):
            with self.subTest(agent=agent, install=install):
                directory = self.root / (agent + install.replace("/", "-"))
                home = directory / "home"
                binaries = directory / "bin"
                binaries.mkdir(parents=True)
                (binaries / "python3").symlink_to(sys.executable)
                installed = home / install
                installed.mkdir(parents=True)
                if install.startswith(".nvm"):
                    (home / ".nvm/nvm.sh").write_text('export PATH="$HOME/.nvm/versions/node/v22/bin:$PATH"\n')
                    (home / ".bash_profile").write_text('. "$HOME/.nvm/nvm.sh"\n')
                elif agent == "claude-code":
                    (home / ".nvm").mkdir()
                    (home / ".nvm/nvm.sh").write_text("exit 71\n")
                identity = str(uuid.uuid4())
                state_var, session_dir, key = (("CODEX_HOME", "sessions", "thread_id") if agent == "codex"
                                               else ("CLAUDE_CONFIG_DIR", "projects", "session_id"))
                cli = installed / ("codex" if agent == "codex" else "claude")
                cli.write_text(
                    "#!/bin/sh\n"
                    f"if [ \"$1\" = --version ]; then printf '%s\\n' {shlex.quote(native.VERSIONS[agent])}; exit; fi\n"
                    f'mkdir -p "${state_var}/{session_dir}"\n'
                    f"printf '{{}}\\n' > \"${state_var}/{session_dir}/{identity}.jsonl\"\n"
                    f"printf '%s\\n' '{json.dumps({key: identity})}'\n")
                cli.chmod(0o755)
                supervisor = directory / "supervisor.py"
                # Exercise the real supervisor/CLI lookup without requiring a
                # Linux PID namespace; quiescence has separate boundary tests.
                supervisor.write_text(
                    "import sys\nfrom pathlib import Path\nfrom unittest.mock import patch\n"
                    f"sys.path.insert(0, {str(Path(native.__file__).parent)!r})\n"
                    "import native_checkpoint as native\n"
                    f"native.STATE = Path({str(directory / 'logs/native')!r})\n"
                    "with patch.object(native, 'baseline_processes', return_value={}), "
                    "patch.object(native, 'assert_quiescent'), patch.object(native.os, 'sync', create=True):\n"
                    "    sys.exit(native.main())\n")
                async def execute(command, env, timeout_sec):
                    command = command.replace("/tmp/eruun-native-checkpoint.py", shlex.quote(str(supervisor)))
                    # ACK executes sh -c wrapping bash -ic, so .bash_profile is
                    # not loaded and cannot make the NVM fixture pass by itself.
                    result = await asyncio.to_thread(
                        subprocess.run, ["sh", "-c", "bash -ic " + shlex.quote(command)],
                        env={"HOME": str(home), "PATH": str(binaries) + ":/usr/bin:/bin", **env},
                        capture_output=True, text=True, timeout=timeout_sec)
                    return SimpleNamespace(return_code=result.returncode, stdout=result.stdout)
                environment = SimpleNamespace(upload_file=AsyncMock(), exec=execute)
                trial = SimpleNamespace(config=SimpleNamespace(trial_name="trial-a"), agent=SimpleNamespace(
                    model_connection=SimpleNamespace(api_key="fixture-key", configured_base_url=None),
                    _resolve_auth_env=lambda: {"ANTHROPIC_API_KEY": "fixture-key"}))
                job = recovery.RecoveryJob(config(agent), {}, directory)
                job.active["trial-a"] = {"agentTimeoutSeconds": 10}
                record = {"stage": "pending"}
                await job.native_run(record, trial, "instruction", environment, None)
                self.assertEqual(record["sessionId"], identity)
                self.assertEqual(job.active["trial-a"]["state"], "finalizing")

    async def test_pending_checkpoint_keeps_all_native_agents_parked(self):
        job = recovery.RecoveryJob(config(), {}, self.root / "source")
        record = manifest()["trials"][0]
        job.records = [record]
        release = asyncio.Event()
        job.active = {"trial-a": {"state": "parked", "release": release}}
        entered, finish = asyncio.Event(), asyncio.Event()
        async def publish(*args):
            entered.set()
            await finish.wait()
        async def threaded(function, *args):
            return await publish(*args)
        with patch.object(recovery, "create_bundle"), patch.object(recovery.asyncio, "to_thread", side_effect=threaded):
            operation = asyncio.create_task(job.checkpoint())
            await entered.wait()
            self.assertFalse(release.is_set())
            finish.set()
            await operation
        self.assertTrue(release.is_set())

    async def test_failed_checkpoint_never_releases_barrier(self):
        job = recovery.RecoveryJob(config(), {}, self.root / "source")
        job.records = [manifest()["trials"][0]]
        release = asyncio.Event()
        job.active = {"trial-a": {"state": "parked", "release": release}}
        with patch.object(recovery, "create_bundle"), patch.object(recovery, "publish_checkpoint", side_effect=runner.RunnerError("snapshot failed")):
            with self.assertRaises(runner.RunnerError):
                await job.checkpoint()
        self.assertFalse(release.is_set())


@unittest.skipUnless(os.environ.get("ERUUN_TEST_NATIVE_CODEX") and os.environ.get("ERUUN_TEST_NATIVE_CLAUDE"),
                     "set both pinned CLI paths to run native loopback recovery fixtures")
class ProductionNativeCommandTest(unittest.TestCase):
    def test_production_commands_resume_both_pinned_clis_against_loopback(self):
        from native_resume_probe import _server, FIRST, INTERRUPTED, RESUMED, MARKER
        binaries = {"codex": Path(os.environ["ERUUN_TEST_NATIVE_CODEX"]).resolve(),
                    "claude-code": Path(os.environ["ERUUN_TEST_NATIVE_CLAUDE"]).resolve()}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for agent, binary in binaries.items():
                with self.subTest(agent=agent), _server() as server:
                    directory = root / agent
                    directory.mkdir()
                    (directory / "work").mkdir()
                    (directory / "home").mkdir()
                    (directory / "bin").mkdir()
                    (directory / "bin" / ("codex" if agent == "codex" else "claude")).symlink_to(binary)
                    base = f"http://127.0.0.1:{server.server_port}"
                    env = {"PATH": str(directory / "bin") + ":/usr/bin:/bin:/usr/sbin:/sbin", "HOME": str(directory / "home"),
                           "TMPDIR": str(directory), "LANG": "en_US.UTF-8", "HTTP_PROXY": base, "HTTPS_PROXY": base,
                           "ALL_PROXY": base, "NO_PROXY": "127.0.0.1,localhost", "OPENAI_API_KEY": "loopback-fixture-only",
                           "ANTHROPIC_API_KEY": "loopback-fixture-only", "ANTHROPIC_BASE_URL": base}
                    previous = Path.cwd()
                    os.chdir(directory / "work")
                    try:
                        # macOS cannot exercise Linux /proc. These two patches
                        # deliberately bound this evidence to actual production
                        # CLI args/session persistence, not the cloud barrier.
                        with patch.dict(os.environ, env, clear=True), patch.object(native, "STATE", directory / "logs/native"), \
                                patch.object(native, "baseline_processes", return_value={}), patch.object(native, "assert_quiescent"):
                            request = {"agent": agent, "model": "fixture-model" if agent == "codex" else "claude-sonnet-4-5",
                                       "baseURL": base + "/v1", "prompt": FIRST, "seconds": 30}
                            first = native.run(request)
                            self.assertFalse(first["paused"])
                            interrupted = native.run(request | {"prompt": INTERRUPTED, "sessionId": first["sessionId"], "seconds": 3})
                            self.assertTrue(interrupted["paused"])
                            server.release.set()
                            previous_count = len(server.requests)
                            resumed = native.run(request | {"prompt": RESUMED, "sessionId": first["sessionId"]})
                            self.assertFalse(resumed["paused"])
                            self.assertEqual(first["sessionId"], resumed["sessionId"])
                            transcript = json.dumps(server.requests[previous_count:])
                            for marker in (MARKER, INTERRUPTED, RESUMED):
                                self.assertIn(marker, transcript)
                    finally:
                        os.chdir(previous)


if __name__ == "__main__":
    unittest.main()

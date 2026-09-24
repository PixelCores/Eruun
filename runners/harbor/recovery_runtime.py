"""Pinned Harbor lifecycle with explicit native-session recovery.

Job.create is used only to derive an initial trial plan. Recovery never calls
Harbor's stock reconciliation, which deletes/reruns incomplete trial folders.
"""

import asyncio
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import http.client
import io
import json
from pathlib import Path, PurePosixPath
import re
import sys
import tarfile
import threading
import time
import uuid

from runner import (EVENT_ATTEMPT_SECONDS, FRAMEWORK_VERSION, MAX_FILES, MAX_EVENT_RESPONSE_BYTES, RetryableTransferError,
                    RunnerError, endpoint, integer, remaining_time, retry_delay, transfer_connection,
                    transfer_deadline, transfer_headers, transfer_timeout)

AGENT_VERSIONS = {"codex": "0.154.0", "claude-code": "2.1.281"}
MAX_CHECKPOINT_BYTES = 64 * 1024 * 1024
MAX_CHECKPOINT_EXPANDED_BYTES = 256 * 1024 * 1024
MAX_CHECKPOINT_MANIFEST = 1024 * 1024
RESUME_PROMPT = "Continue the original task from the saved session. An interrupted tool operation may be replayed; the task explicitly permits replay."


def validate_recovery(config):
    value = config.get("recovery")
    if value is None:
        if config.get("resumeCheckpointId") or config.get("checkpointURL"):
            raise RunnerError("checkpoint configuration requires recovery")
        return
    if (not isinstance(value, dict) or set(value) - {"agentVersion", "replaySafe", "checkpointIntervalSeconds"}
            or value.get("replaySafe") is not True
            or config["agent"]["name"] not in AGENT_VERSIONS
            or value.get("agentVersion") != AGENT_VERSIONS[config["agent"]["name"]]):
        raise RunnerError("recovery requires replaySafe and an explicitly supported agent version")
    integer(value.get("checkpointIntervalSeconds"), "checkpointIntervalSeconds", 60, 3600)
    endpoint(config.get("checkpointURL"))
    endpoint(config.get("sandboxURL"))
    identity = config.get("resumeCheckpointId")
    if identity is not None and (not isinstance(identity, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", identity)):
        raise RunnerError("invalid resume checkpoint identity")


def checkpoint_request(config, checkpoint_id, method, deadline, archive=None):
    deadline = min(deadline, time.monotonic() + EVENT_ATTEMPT_SECONDS)
    url = config["checkpointURL"].rstrip("/") + "/" + checkpoint_id
    connection, target = transfer_connection(url, deadline)
    with transfer_deadline(connection, deadline):
        headers = transfer_headers(config)
        if archive is None:
            connection.request(method, target, headers=headers)
        else:
            connection.putrequest(method, target)
            for key, value in (headers | {"Content-Type": "application/gzip", "Content-Length": str(archive.stat().st_size)}).items():
                connection.putheader(key, value)
            connection.endheaders()
            with archive.open("rb") as source:
                while chunk := source.read(1024 * 1024):
                    transfer_timeout(connection, deadline)
                    connection.send(chunk)
        transfer_timeout(connection, deadline)
        with closing(connection.getresponse()) as response:
            body = response.read(MAX_EVENT_RESPONSE_BYTES + 1)
        remaining_time(deadline)
        if response.status >= 500 or response.status in {408, 429}:
            raise RetryableTransferError("checkpoint control is temporarily unavailable")
        if not 200 <= response.status < 300:
            raise RunnerError(f"platform rejected checkpoint with HTTP {response.status}")
        try:
            data = json.loads(body)["data"]
            if (len(body) > MAX_EVENT_RESPONSE_BYTES or data["id"] != checkpoint_id
                    or data["state"] not in {"pending", "ready", "failed"}):
                raise ValueError()
        except (ValueError, KeyError, TypeError):
            raise RunnerError("invalid checkpoint acknowledgment") from None
        return data


def publish_checkpoint(config, checkpoint_id, archive, deadline, stop=None):
    # The immutable bytes and ID survive uncertain POST outcomes. Do not mint
    # a second cloud operation merely because its response was lost.
    method, attempt = "POST", 0
    stop = stop or threading.Event()
    while True:
        if stop.is_set():
            raise RunnerError("checkpoint publication cancelled")
        remaining_time(deadline)
        try:
            data = checkpoint_request(config, checkpoint_id, method, deadline,
                                      archive if method == "POST" else None)
            if data["state"] == "ready":
                return
            if data["state"] == "failed":
                raise RunnerError("checkpoint was rejected or a required snapshot failed")
            method = "GET"
            stop.wait(min(1, remaining_time(deadline)))
        except (OSError, http.client.HTTPException, RetryableTransferError):
            attempt += 1
            stop.wait(retry_delay(attempt, deadline))


def download_checkpoint(config, path, deadline):
    connection, target = transfer_connection(config["checkpointURL"].rstrip("/") + "/" + config["resumeCheckpointId"] + "/material", deadline)
    with transfer_deadline(connection, deadline):
        connection.request("GET", target, headers=transfer_headers(config))
        transfer_timeout(connection, deadline)
        with closing(connection.getresponse()) as response, path.open("xb") as output:
            if response.status != 200:
                raise RunnerError(f"platform rejected checkpoint material with HTTP {response.status}")
            size = 0
            while True:
                transfer_timeout(connection, deadline)
                chunk = response.read1(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_CHECKPOINT_BYTES:
                    raise RunnerError("checkpoint material exceeds compressed size limit")
                output.write(chunk)
        remaining_time(deadline)


def safe_path(raw):
    if not isinstance(raw, str):
        raise RunnerError("checkpoint path is not a string")
    path = PurePosixPath(raw)
    if (not path.parts or path.is_absolute() or ".." in path.parts or "\\" in raw or "\x00" in raw
            or path.as_posix() != raw or len(raw.encode()) > 1024):
        raise RunnerError("checkpoint contains an unsafe path")
    return path


def validate_manifest(manifest, config):
    if (not isinstance(manifest, dict) or manifest.get("version") != 1
            or manifest.get("harborVersion") != FRAMEWORK_VERSION
            or manifest.get("agent") != config["agent"]["name"]
            or manifest.get("agentVersion") != config["recovery"]["agentVersion"]
            or manifest.get("datasetDigest") != config["datasetDigest"]):
        raise RunnerError("checkpoint version or task identity does not match")
    trials, completed, members = manifest.get("trials"), manifest.get("completedTrialIds"), manifest.get("members")
    if not isinstance(trials, list) or not trials or len(trials) > MAX_FILES or not isinstance(completed, list) or not isinstance(members, list):
        raise RunnerError("checkpoint trial set is invalid")
    ids, completed_ids, member_ids = set(), set(), set()
    for trial in trials:
        name = trial.get("config", {}).get("trial_name", "")
        identity = name + "__env"
        if (not re.fullmatch(r"[a-zA-Z0-9_.-]{1,120}", name) or identity in ids
                or trial.get("stage") not in {"pending", "agent", "completed"}):
            raise RunnerError("checkpoint trial identity or stage is invalid")
        ids.add(identity)
        task_path = safe_path(trial["config"]["task"]["path"])
        if task_path.parts[0] != "dataset":
            raise RunnerError("checkpoint task path is outside the dataset")
        if trial["stage"] != "pending":
            collection = trial.get("collection")
            if (not isinstance(collection, dict) or collection.get("started") is not True
                    or collection.get("pending") != 0 or collection.get("errorCount") != 0
                    or collection.get("stopped") is not (trial["stage"] == "completed")):
                raise RunnerError("checkpoint trial collection is not at a complete boundary")
        if trial["stage"] == "completed":
            completed_ids.add(identity)
            if not isinstance(trial.get("result"), dict) or not trial["result"].get("finished_at"):
                raise RunnerError("checkpoint completed trial result is missing")
        if trial["stage"] == "agent":
            member_ids.add(identity)
            try:
                if str(uuid.UUID(trial["sessionId"])) != trial["sessionId"]:
                    raise ValueError()
                remaining = trial["remainingAgentSeconds"]
                if not isinstance(trial.get("agentDeadline"), (int, float)) or isinstance(trial["agentDeadline"], bool) or not 0 < trial["agentDeadline"] < 1e12:
                    raise ValueError()
                if isinstance(remaining, bool) or not isinstance(remaining, (int, float)) or not 0 < remaining <= 14 * 86400:
                    raise ValueError()
            except (KeyError, ValueError, TypeError):
                raise RunnerError("checkpoint native session or agent budget is invalid") from None
    if len(completed) != len(completed_ids) or set(completed) != completed_ids:
        raise RunnerError("checkpoint completed trial set does not match")
    if len(members) != len(member_ids) or {member.get("trialId") for member in members} != member_ids:
        raise RunnerError("checkpoint snapshot member set does not match")
    indexed = {trial["config"]["trial_name"] + "__env": trial for trial in trials}
    if any(member.get("stage") != "agent" or member.get("sessionId") != indexed[member["trialId"]]["sessionId"] for member in members):
        raise RunnerError("checkpoint snapshot sessions do not match")
    return manifest


def load_bundle(path, destination, config):
    """Validate every byte before material becomes an executable recovery plan."""
    seen, contents, total = set(), {}, 0
    with tarfile.open(path, "r:gz") as archive:
        for index, member in enumerate(archive):
            safe_path(member.name)
            if index >= MAX_FILES or not member.isfile() or member.name in seen:
                raise RunnerError("checkpoint contains duplicate or unsupported entries")
            seen.add(member.name)
            total += member.size
            if member.size < 0 or total > MAX_CHECKPOINT_EXPANDED_BYTES:
                raise RunnerError("checkpoint exceeds expanded size limit")
            if member.name == "checkpoint.json" and member.size > MAX_CHECKPOINT_MANIFEST:
                raise RunnerError("checkpoint manifest exceeds size limit")
            with archive.extractfile(member) as source:
                contents[member.name] = source.read()
    try:
        manifest = validate_manifest(json.loads(contents.pop("checkpoint.json")), config)
        files = manifest["files"]
        if not isinstance(files, list) or len(files) != len(contents):
            raise ValueError()
        expected = set()
        for entry in files:
            name = entry["path"]
            safe_path(name)
            if name in expected or not name.startswith("outputs/run/"):
                raise ValueError()
            expected.add(name)
            raw = contents[name]
            if entry["size"] != len(raw) or entry["digest"] != hashlib.sha256(raw).hexdigest():
                raise ValueError()
        if expected != contents.keys():
            raise ValueError()
    except (KeyError, TypeError, ValueError):
        raise RunnerError("checkpoint content index or digest is invalid") from None
    for name, raw in contents.items():
        target = destination.joinpath(*safe_path(name).parts)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("xb") as output:
            output.write(raw)
        target.chmod(0o600)
    return manifest


def stored_config(config, work):
    value = config.model_dump(mode="json")
    value["task"]["path"] = config.task.path.relative_to(work).as_posix()
    # Regenerate transport URLs, ownership, Pod UID, namespace and capability
    # paths from the new execution. They are never recovery authority.
    value.pop("environment", None)
    value.pop("trials_dir", None)
    return value


def restored_config(value, generated, work):
    from harbor.models.trial.config import TrialConfig
    value = json.loads(json.dumps(value))
    relative = safe_path(value["task"]["path"])
    if relative.parts[0] != "dataset":
        raise RunnerError("checkpoint task path is outside the dataset")
    value["task"]["path"] = str(work.joinpath(*relative.parts))
    value["trials_dir"] = str(work / "outputs" / "run")
    value["environment"] = generated["environment"]
    if value.get("agent", {}).get("name") != generated["agents"][0]["name"] or value.get("agent", {}).get("model_name") != generated["agents"][0].get("model_name"):
        raise RunnerError("checkpoint agent configuration changed")
    parsed = TrialConfig.model_validate(value)
    expected = TrialConfig(task={"path": value["task"]["path"], "source": "uploaded"},
                           trial_name=parsed.trial_name, trials_dir=parsed.trials_dir, job_id=parsed.job_id,
                           agent=generated["agents"][0], environment=generated["environment"],
                           environment_build_timeout_multiplier=parsed.environment_build_timeout_multiplier)
    if parsed != expected:
        raise RunnerError("checkpoint trial configuration is outside the supported recovery contract")
    return parsed


def stored_result(result, config):
    value = result.model_dump(mode="json")
    value["config"] = config
    value["task_id"] = {"path": config["task"]["path"]}
    value["trial_uri"] = ""
    return value


def restored_result(value, config):
    from harbor.models.trial.result import TrialResult
    value = dict(value)
    value["config"] = config
    value["task_id"] = config.task.get_task_id()
    value["trial_uri"] = (config.trials_dir / config.trial_name).as_uri()
    return TrialResult.model_validate(value)


def create_bundle(work, manifest, archive_path, forbidden_values=()):
    files, data, total = [], [], 0
    root = work / "outputs" / "run"
    # Trial configs/results are serialized in the manifest with explicit path
    # fields. Do not copy obsolete Pod ownership, capabilities or log locks.
    excluded = {root / name for name in ("config.json", "result.json", "lock.json")}
    for record in manifest["trials"]:
        trial = root / record["config"]["trial_name"]
        excluded.update(trial / name for name in ("config.json", "result.json", "lock.json"))
        excluded.update(trial / "agent" / "eruun-native" / name for name in ("auth.json", ".credentials.json"))
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise RunnerError("checkpoint cannot persist symlinks")
        if path.is_dir() or path in excluded:
            continue
        if not path.is_file():
            raise RunnerError("checkpoint cannot persist special files")
        if path.stat().st_size + total > MAX_CHECKPOINT_EXPANDED_BYTES:
            raise RunnerError("checkpoint exceeds material limit")
        raw = path.read_bytes()
        if any(secret and secret.encode() in raw for secret in forbidden_values):
            raise RunnerError("checkpoint output contains runtime credentials")
        total += len(raw)
        if total > MAX_CHECKPOINT_EXPANDED_BYTES or len(files) >= MAX_FILES - 1:
            raise RunnerError("checkpoint exceeds material limit")
        name = path.relative_to(work).as_posix()
        files.append({"path": name, "size": len(raw), "digest": hashlib.sha256(raw).hexdigest()})
        data.append((name, raw))
    manifest["files"] = files
    encoded = json.dumps(manifest, separators=(",", ":")).encode()
    if any(secret and secret.encode() in encoded for secret in forbidden_values):
        raise RunnerError("checkpoint manifest contains runtime credentials")
    if len(encoded) > MAX_CHECKPOINT_MANIFEST:
        raise RunnerError("checkpoint manifest exceeds size limit")
    with tarfile.open(archive_path, "w:gz") as target:
        for name, raw in [("checkpoint.json", encoded), *data]:
            info = tarfile.TarInfo(name)
            info.size, info.mode = len(raw), 0o600
            target.addfile(info, io.BytesIO(raw))
    if archive_path.stat().st_size > MAX_CHECKPOINT_BYTES:
        raise RunnerError("checkpoint exceeds compressed size limit")


class RecoveryJob:
    def __init__(self, config, generated, work):
        self.config, self.generated, self.work = config, generated, work
        self.deadline = config["executionDeadlineMonotonic"]
        self.records = []
        self.active = {}
        self.changed = asyncio.Event()
        self.secret_values = {config["token"]}

    async def initialize(self):
        from harbor.job import Job
        from harbor.models.job.config import JobConfig
        if self.config.get("resumeCheckpointId"):
            material = self.work / "resume.tar.gz"
            await asyncio.to_thread(download_checkpoint, self.config, material, self.deadline)
            manifest = load_bundle(material, self.work, self.config)
            self.records = manifest["trials"]
            expected = {str(Path(task["path"]).relative_to(self.work)) for task in self.generated["tasks"]}
            observed = [record["config"]["task"]["path"] for record in self.records]
            if set(observed) != expected or any(observed.count(path) != self.config["options"]["attempts"] for path in expected):
                raise RunnerError("checkpoint trial plan does not match submitted task attempts")
        else:
            job = await Job.create(JobConfig.model_validate(self.generated))
            try:
                self.records = [{"config": stored_config(value, self.work), "stage": "pending"} for value in job._trial_configs]
            finally:
                job._close_logger_handlers()
        for record in self.records:
            config = restored_config(record["config"], self.generated, self.work)
            directory = config.trials_dir / config.trial_name
            directory.mkdir(parents=True, exist_ok=True)
            (directory / "config.json").write_text(config.model_dump_json())
            if record["stage"] == "completed":
                result = restored_result(record["result"], config)
                (directory / "result.json").write_text(result.model_dump_json())
                if "lock" in record:
                    from harbor.models.job.lock import TrialLock
                    lock = dict(record["lock"])
                    lock["environment"] = self.generated["environment"]
                    lock["task"] = dict(lock["task"], path=str(config.task.path))
                    (directory / "lock.json").write_text(TrialLock.model_validate(lock).model_dump_json())
                collection = dict(record["collection"])
                collection["trialDirectory"] = str(directory)
                (self.work / "collection" / (config.trial_name + ".json")).write_text(json.dumps(collection))

    async def native_run(self, record, trial, instruction, environment, context):
        identity = trial.config.trial_name
        active = self.active[identity]
        if record["stage"] == "agent":
            seconds = min(record["remainingAgentSeconds"], record["agentDeadline"] - time.time())
            # elapsed time outside the native agent phase, including failure
            # recovery and snapshot publication, also consumes the original job.
            active["agentDeadline"] = min(self.deadline, time.monotonic() + seconds)
        else:
            timeout = active["agentTimeoutSeconds"]
            active["agentDeadline"] = (min(self.deadline, time.monotonic() + timeout)
                                       if timeout is not None else self.deadline)
        budget = remaining_time(active["agentDeadline"])
        await asyncio.wait_for(
            environment.upload_file(Path(__file__).with_name("native_checkpoint.py"), "/tmp/eruun-native-checkpoint.py"),
            timeout=budget)
        while True:
            budget = remaining_time(active["agentDeadline"])
            request = {"agent": self.config["agent"]["name"], "model": self.config["agent"]["model"].split("/")[-1],
                       "prompt": RESUME_PROMPT if record.get("sessionId") else instruction,
                       "sessionId": record.get("sessionId"), "seconds": min(budget, self.config["recovery"]["checkpointIntervalSeconds"])}
            access = trial.agent.model_connection
            if self.config["agent"]["name"] == "codex":
                native_env = {"OPENAI_API_KEY": access.api_key or ""}
                request["baseURL"] = access.configured_base_url
            else:
                native_env = trial.agent._resolve_auth_env()
            required_key = "OPENAI_API_KEY" if self.config["agent"]["name"] == "codex" else "ANTHROPIC_API_KEY"
            if not native_env.get(required_key):
                raise RunnerError("native recovery requires a fresh provider API key")
            self.secret_values.add(native_env[required_key])
            native_env["ERUUN_NATIVE_REQUEST"] = json.dumps(request)
            active["state"] = "running"
            result = await asyncio.wait_for(
                environment.exec('export PATH="$HOME/.local/bin:$HOME/.npm-global/bin:$PATH"; python3 /tmp/eruun-native-checkpoint.py',
                                 env=native_env, timeout_sec=remaining_time(active["agentDeadline"])),
                timeout=remaining_time(active["agentDeadline"]))
            try:
                value = json.loads(result.stdout)
                if (result.return_code != 0 or value.get("quiescent") is not True
                        or not isinstance(value.get("paused"), bool)
                        or str(uuid.UUID(value["sessionId"])) != value["sessionId"]):
                    raise ValueError()
            except (ValueError, KeyError, TypeError):
                raise RunnerError("native agent did not establish a recoverable boundary") from None
            record["sessionId"] = value["sessionId"]
            if not value["paused"]:
                active["state"] = "finalizing"
                self.changed.set()
                return
            remaining_time(active["agentDeadline"])
            record["stage"] = "agent"
            # This copy is read only after the supervisor proved all writers
            # stopped. Snapshot membership never includes pending collection.
            await asyncio.wait_for(environment.download_dir("/logs/agent", trial.paths.agent_dir),
                                   timeout=remaining_time(active["agentDeadline"]))
            record["remainingAgentSeconds"] = remaining_time(active["agentDeadline"])
            record["agentDeadline"] = time.time() + record["remainingAgentSeconds"]
            record["result"] = stored_result(trial.result, record["config"])
            collection = environment._collection
            if collection["pending"] != 0 or collection["errorCount"] != 0:
                raise RunnerError("checkpoint collection is not complete")
            record["collection"] = {key: collection[key] for key in ("started", "stopped", "pending", "errorCount", "errors")}
            active["release"] = asyncio.Event()
            active["state"] = "parked"
            self.changed.set()
            await active["release"].wait()

    async def run_trial(self, record):
        from harbor.trial.trial import Trial
        config = restored_config(record["config"], self.generated, self.work)
        trial = await Trial.create(config)
        active = self.active[config.trial_name]
        # Harbor's outer agent timer also covers the parked wait. Let native_run
        # enforce the same absolute budget around agent I/O, then check it again
        # after publication; an expired budget must not unfreeze a pending source.
        active["agentTimeoutSeconds"] = trial._agent_timeout_sec
        trial._agent_timeout_sec = None
        sync_output, finalize = trial._sync_agent_output, trial._finalize

        async def sync_agent_output(target):
            release = active.get("release")
            if release is not None and not release.is_set():
                # SingleStepTrial calls this from both its agent finally block
                # and _recover_outputs. Raising, rather than skipping it, also
                # prevents subsequent artifact collection and verification.
                raise asyncio.CancelledError("checkpoint source remains frozen")
            await sync_output(target)

        async def finalize_trial():
            release = active.get("release")
            if release is None or release.is_set():
                await finalize()
            # Unknown/failed publication retains the source. The control plane
            # owns cleanup; closing an HTTP request does not cancel a snapshot.

        trial._sync_agent_output = sync_agent_output
        trial._finalize = finalize_trial
        lock = trial._trial_lock.model_dump(mode="json")
        lock.pop("environment", None)
        lock["task"]["path"] = record["config"]["task"]["path"]
        record["lock"] = lock
        restoring = record["stage"] == "agent"
        trial.agent_environment._require_restored = restoring
        if restoring:
            async def prepare():
                # Install/healthcheck/task setup must not overwrite a clone.
                await trial.agent_environment.start(force_build=False)
                if not trial.agent_environment._restored:
                    raise RunnerError("recovery did not attach the required cloned sandbox")
                if trial.task.checksum != record["result"].get("task_checksum"):
                    raise RunnerError("checkpoint task checksum changed")
                trial._result = restored_result(record["result"], config)
                trial._result.finished_at = None
            trial._prepare = prepare
        async def run(instruction, environment, context):
            return await self.native_run(record, trial, instruction, environment, context)
        trial.agent.run = run
        def session_directory():
            if not record.get("sessionId"):
                return None
            paths = list((trial.paths.agent_dir / "eruun-native").rglob("*" + record["sessionId"] + ".jsonl"))
            return paths[0].parent if len(paths) == 1 else None
        trial.agent._get_session_dir = session_directory
        try:
            result = await trial.run()
            record["stage"] = "completed"
            record["result"] = stored_result(result, record["config"])
            collection = dict(trial.agent_environment._collection)
            for key in ("podName", "podUID", "sandboxName", "sandboxUID", "namespace", "trialDirectory"):
                collection.pop(key, None)
            record["collection"] = collection
            return result
        finally:
            active["state"] = "completed"
            self.changed.set()

    async def checkpoint(self):
        parked = [value for value in self.active.values() if value["state"] == "parked"]
        for record in self.records:
            if record["stage"] == "agent":
                record["remainingAgentSeconds"] = min(record["remainingAgentSeconds"], record["agentDeadline"] - time.time())
                if record["remainingAgentSeconds"] <= 0:
                    raise TimeoutError("agent deadline elapsed before checkpoint publication")
        if not parked:
            return
        # All in-flight trials have either stopped all native writers or fully
        # finished verification and collection. No next batch starts here.
        manifest = {"version": 1, "harborVersion": FRAMEWORK_VERSION,
                    "agent": self.config["agent"]["name"], "agentVersion": self.config["recovery"]["agentVersion"],
                    "datasetDigest": self.config["datasetDigest"], "trials": self.records,
                    "remainingSeconds": remaining_time(self.deadline),
                    "completedTrialIds": [record["config"]["trial_name"] + "__env" for record in self.records if record["stage"] == "completed"],
                    "members": [{"trialId": record["config"]["trial_name"] + "__env", "sessionId": record["sessionId"], "stage": "agent"}
                                for record in self.records if record["stage"] == "agent"]}
        checkpoint_id = uuid.uuid4().hex
        archive = self.work / ("checkpoint-" + checkpoint_id + ".tar.gz")
        validate_manifest(manifest, self.config)
        create_bundle(self.work, manifest, archive, self.secret_values)
        stop = threading.Event()
        try:
            await asyncio.to_thread(publish_checkpoint, self.config, checkpoint_id, archive, self.deadline, stop)
        finally:
            stop.set()
            archive.unlink(missing_ok=True)
        for active in parked:
            active["state"] = "running"
            active["release"].set()

    async def run(self):
        from harbor.metrics.mean import Mean
        from harbor.models.job.result import JobResult, JobStats
        from harbor.utils.pass_at_k import compute_pass_at_k_by_evals
        await self.initialize()
        started = datetime.now(timezone.utc)
        remaining = [record for record in self.records if record["stage"] != "completed"]
        width = self.config["options"]["concurrency"]
        for offset in range(0, len(remaining), width):
            batch = remaining[offset:offset + width]
            self.active = {record["config"]["trial_name"]: {"state": "preparing"} for record in batch}
            tasks = [asyncio.create_task(self.run_trial(record)) for record in batch]
            for task in tasks:
                task.add_done_callback(lambda _: self.changed.set())
            try:
                while not all(task.done() for task in tasks):
                    self.changed.clear()
                    for task in tasks:
                        if task.done() and task.exception() is not None:
                            raise task.exception()
                    states = {value["state"] for value in self.active.values()}
                    if states <= {"parked", "completed"} and "parked" in states:
                        await self.checkpoint()
                    if not all(task.done() for task in tasks):
                        await asyncio.wait_for(self.changed.wait(), timeout=remaining_time(self.deadline))
                await asyncio.gather(*tasks)
            except BaseException:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise
        results = [restored_result(record["result"], restored_config(record["config"], self.generated, self.work)) for record in self.records]
        stats = JobStats.from_trial_results(results, n_total_trials=len(results))
        rewards = {key: [] for key in stats.evals}
        for result in results:
            info = result.agent_info
            key = JobStats.format_agent_evals_key(info.name, info.model_info.name if info.model_info else None,
                                                  result.source or "adhoc")
            rewards[key].append(result.verifier_result.rewards if result.verifier_result is not None else None)
        for key, values in rewards.items():
            stats.evals[key].metrics = [Mean().compute(values)]
        for key, values in compute_pass_at_k_by_evals(results).items():
            stats.evals[key].pass_at_k = values
        result = JobResult(id=uuid.uuid4(), started_at=started, finished_at=datetime.now(timezone.utc), n_total_trials=len(results),
                           stats=stats, trial_results=results)
        (self.work / "outputs" / "run" / "result.json").write_text(result.model_dump_json())


def main():
    try:
        generated = json.loads(Path(sys.argv[1]).read_text())
        config = json.loads(Path(sys.argv[2]).read_text())
        validate_recovery(config)
        asyncio.run(RecoveryJob(config, generated, Path(sys.argv[1]).parent).run())
        return 0
    except Exception as exc:
        print("Recovery runtime failed: " + (str(exc) if isinstance(exc, RunnerError) else type(exc).__name__), flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

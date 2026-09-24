"""Harbor's native Kubernetes backend constrained to the workspace user.

Harbor uses user='root' for ordinary setup operations such as chmod of its own
log directory. Eruun's prepared task images make those paths writable by UID
1000, so all operations run as that fixed UID without invoking su or sudo.
Commands which actually require root fail normally in the restricted Pod.
"""

import asyncio
from contextlib import asynccontextmanager, closing
import http.client
import json
import math
from pathlib import Path
import re
import shlex
import stat
import tarfile
import tempfile
import time
import urllib.parse
import uuid

from harbor.environments.ack import ACKEnvironment
from harbor.models.trial.paths import EnvironmentPaths
from kubernetes import client as k8s_client
from kubernetes.stream import stream

from runner import (CONTROL_OUTAGE_SECONDS, CONTROL_STATE_MAX_AGE_SECONDS, EVENT_ATTEMPT_SECONDS, MAX_EVENT_RESPONSE_BYTES, RetryableTransferError,
                    RunnerError, endpoint, integer, remaining_time, retry_delay, transfer_connection,
                    transfer_deadline, transfer_headers, transfer_timeout)

MAX_TRANSFER_BYTES = 2 * 1024 * 1024 * 1024


class SandboxExecAPI(k8s_client.CoreV1Api):
    """Keep Kubernetes stream's bound API method while pinning its target."""

    def __init__(self, identity, core_api):
        super().__init__(k8s_client.ApiClient())
        self.identity = identity
        self.core_api = core_api

    def connect_get_namespaced_pod_exec(self, name, namespace, **kwargs):
        if (name, namespace) != (self.identity["podName"], self.identity["namespace"]):
            raise RunnerError("sandbox execution target changed")
        # stream() temporarily replaces this API client's request transport;
        # identity reads must use the separate ordinary CoreV1 client.
        pod = self.core_api.read_namespaced_pod(name, namespace, _request_timeout=EVENT_ATTEMPT_SECONDS)
        if pod.metadata.uid != self.identity["podUID"]:
            raise RunnerError("sandbox Pod identity changed")
        kwargs["container"] = self.identity["containerName"]
        return super().connect_get_namespaced_pod_exec(name, namespace, **kwargs)


def sandbox_request(control, method, suffix, payload, deadline):
    deadline = min(deadline, time.monotonic() + EVENT_ATTEMPT_SECONDS)
    connection, target = transfer_connection(control["sandboxURL"].rstrip("/") + suffix, deadline)
    encoded = None if payload is None else json.dumps(payload, separators=(",", ":")).encode()
    with transfer_deadline(connection, deadline):
        connection.request(method, target, body=encoded,
                           headers=transfer_headers(control) | {"Content-Type": "application/json"})
        transfer_timeout(connection, deadline)
        with closing(connection.getresponse()) as response:
            body = response.read(MAX_EVENT_RESPONSE_BYTES + 1)
        remaining_time(deadline)
        if response.status >= 500 or response.status in {408, 429}:
            raise RetryableTransferError("sandbox control is temporarily unavailable")
        if not 200 <= response.status < 300:
            raise RunnerError(f"platform rejected sandbox operation with HTTP {response.status}")
        if len(body) > MAX_EVENT_RESPONSE_BYTES:
            raise RunnerError("sandbox acknowledgment is too large")
        try:
            envelope = json.loads(body)
        except ValueError:
            raise RetryableTransferError("sandbox acknowledgment could not be decoded") from None
        data = envelope.get("data") if isinstance(envelope, dict) else None
        if (not isinstance(data, dict) or data.get("state") not in {"pending", "ready", "released", "retained", "failed"}
                or not isinstance(data.get("admitted"), bool)):
            raise RunnerError("invalid sandbox acknowledgment")
        age = data.get("admissionAgeSeconds")
        if age is not None and (not data["admitted"] or isinstance(age, bool)
                                or not isinstance(age, (int, float)) or not math.isfinite(age) or age < 0):
            raise RunnerError("invalid sandbox admission age")
        return data


class WorkspaceEnvironment(ACKEnvironment):
    def __init__(self, *, collection_state_dir, sandbox_control_file=None, **kwargs):
        super().__init__(**kwargs)
        self._sandbox_control = None
        self._sandbox_identity = None
        self._sandbox_requested = False
        self._restored = False
        self._require_restored = False
        if sandbox_control_file is not None:
            path = Path(sandbox_control_file)
            if not path.is_file() or stat.S_IMODE(path.stat().st_mode) != 0o600 or path.stat().st_size > 16 * 1024:
                raise RunnerError("invalid sandbox control capability file")
            control = json.loads(path.read_text())
            endpoint(control.get("sandboxURL"))
            if (not isinstance(control.get("token"), str) or not control["token"]
                    or control.get("namespace") != self.namespace
                    or not isinstance(control.get("executionDeadline"), (int, float))
                    or not isinstance(control.get("finalizationDeadline"), (int, float))
                    or not isinstance(control.get("admissionStateFile"), str)
                    or not re.fullmatch(r"[a-zA-Z0-9_.-]{1,128}", self.session_id)):
                raise RunnerError("invalid sandbox control capability")
            if self.use_sandbox_claim:
                raise RunnerError("sandbox control does not use a warm pool")
            self._sandbox_control = control
        # This directory is outside every task and downloaded trial directory.
        # Persist before each operation so a killed process or failed write
        # cannot turn an unfinished transfer into complete collection.
        directory = Path(collection_state_dir)
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._collection_path = directory / (uuid.uuid4().hex + ".json")
        self._collection = {"podName": self.pod_name, "namespace": self.namespace,
                            "trialDirectory": str(self.trial_paths.trial_dir),
                            "started": False, "stopped": False, "pending": 0,
                            "errorCount": 0, "errors": []}
        self._save_collection()

    def _save_collection(self):
        temporary = self._collection_path.with_suffix(".tmp")
        try:
            temporary.write_text(json.dumps(self._collection))
            temporary.replace(self._collection_path)
        except OSError:
            self._collection["errorCount"] += 1
            if len(self._collection["errors"]) < 100:
                self._collection["errors"].append({"path": "collection", "reason": "state_write_failed"})
            raise

    @asynccontextmanager
    async def _collecting(self, source):
        self._collection["pending"] += 1
        self._save_collection()
        try:
            yield
        except BaseException as exc:
            self._collection["errorCount"] += 1
            if len(self._collection["errors"]) < 100:
                self._collection["errors"].append({"path": str(source)[:1024], "reason": type(exc).__name__})
            raise
        finally:
            self._collection["pending"] -= 1
            self._save_collection()

    async def _ensure_client(self):
        await super()._ensure_client()
        if self._sandbox_identity is not None and not isinstance(self._exec_api, SandboxExecAPI):
            if self._exec_api is not None:
                self._exec_api.api_client.close()
            self._exec_api = SandboxExecAPI(self._sandbox_identity, self._core_api)

    async def _sandbox_operation(self, method, suffix, payload, deadline):
        outage_started = None
        attempt = 0
        paused = 0.0
        while True:
            remaining_time(deadline)
            started = time.monotonic()
            attempt_deadline = deadline if outage_started is None else min(deadline, outage_started + CONTROL_OUTAGE_SECONDS)
            try:
                data = await asyncio.to_thread(sandbox_request, self._sandbox_control, method, suffix, payload, attempt_deadline)
                if outage_started is not None:
                    paused = time.monotonic() - outage_started
                return data, paused
            except (OSError, http.client.HTTPException, RetryableTransferError):
                if outage_started is None:
                    outage_started = started
                attempt += 1
                await asyncio.sleep(retry_delay(attempt, min(deadline, outage_started + CONTROL_OUTAGE_SECONDS)))

    async def _wait_for_admission(self, deadline):
        path = Path(self._sandbox_control["admissionStateFile"])
        while True:
            remaining_time(deadline)
            try:
                if stat.S_IMODE(path.stat().st_mode) != 0o600 or path.stat().st_size > 4096:
                    raise ValueError()
                state = json.loads(path.read_text())
                now = time.monotonic()
                if (not isinstance(state, dict) or not isinstance(state.get("healthy"), bool)
                        or not isinstance(state.get("stopped"), bool)
                        or isinstance(state.get("updatedAt"), bool)
                        or not isinstance(state.get("updatedAt"), (float, int))
                        or not math.isfinite(state["updatedAt"]) or state["updatedAt"] > now):
                    raise ValueError()
                if state["stopped"]:
                    raise RunnerError("runner stopped new sandbox admission")
                if state["healthy"]:
                    if now - state["updatedAt"] > CONTROL_STATE_MAX_AGE_SECONDS:
                        raise RunnerError("runner admission state expired")
                    return
                outage = state.get("outageStarted")
                if (isinstance(outage, bool) or not isinstance(outage, (float, int))
                        or not math.isfinite(outage) or outage > now):
                    raise ValueError()
            except (OSError, ValueError, TypeError):
                raise RunnerError("runner admission state is unavailable") from None
            await asyncio.sleep(min(0.25, remaining_time(min(deadline, outage + CONTROL_OUTAGE_SECONDS))))

    def _accept_sandbox_identity(self, data):
        if data.get("trialId") != self.session_id:
            raise RunnerError("sandbox acknowledgment belongs to a different trial")
        if data["state"] != "ready" and not all(data.get(key) for key in ("sandboxUID", "podUID")):
            return
        if (not data["admitted"] or data.get("namespace") != self.namespace or data.get("containerName") != "main"
                or any(not isinstance(data.get(key), str) or not data[key]
                       for key in ("sandboxName", "sandboxUID", "podName", "podUID"))):
            raise RunnerError("invalid ready sandbox identity")
        identity = {key: data[key] for key in
                    ("namespace", "sandboxName", "sandboxUID", "podName", "podUID", "containerName")}
        if self._sandbox_identity is not None and self._sandbox_identity != identity:
            raise RunnerError("sandbox execution identity changed")
        self._sandbox_identity = identity
        self.pod_name = data["podName"]
        self._collection.update(self._sandbox_identity)
        self._save_collection()

    async def _start_sandbox(self):
        control = self._sandbox_control
        deadline = control["executionDeadline"]
        path = "/" + urllib.parse.quote(self.session_id, safe="")
        payload = {"trialId": self.session_id, "image": self._get_image_url(),
                   "storageMiB": integer(self.task_env_config.storage_mb, "task environment.storage_mb", 1, 1048576)}
        method, suffix = "POST", ""
        startup_remaining = float(self.task_env_config.build_timeout_sec)
        admitted = False
        while True:
            if admitted and startup_remaining <= 0:
                raise TimeoutError("sandbox startup exceeded task build timeout")
            await self._wait_for_admission(deadline)
            if not self._sandbox_requested:
                self._sandbox_requested = True
                self._collection["sandboxRequested"] = True
                self._collection["trialId"] = self.session_id
                self._save_collection()
            started = time.monotonic()
            data, outage = await self._sandbox_operation(method, suffix, payload, deadline)
            elapsed = time.monotonic() - started
            if admitted:
                startup_remaining -= max(0, elapsed - outage)
            elif data["admitted"] and data.get("admissionAgeSeconds") is not None:
                # The server measures from its durable UID admission. Limit the
                # charge to this operation so an earlier queue wait or closed
                # admission gate is never mistaken for startup time.
                startup_remaining -= max(0, min(elapsed, data["admissionAgeSeconds"]) - outage)
            if startup_remaining <= 0:
                raise TimeoutError("sandbox startup exceeded task build timeout")
            self._accept_sandbox_identity(data)
            if data["state"] == "ready":
                if self._require_restored and data.get("restored") is not True:
                    raise RunnerError("recovery sandbox was not cloned from the bound checkpoint")
                self._restored = data.get("restored") is True
                return startup_remaining
            if data["state"] != "pending":
                raise RunnerError("sandbox did not become ready")
            if admitted and not data["admitted"]:
                raise RunnerError("sandbox admission moved backwards")
            admitted = data["admitted"]
            pause = min(1, remaining_time(deadline), startup_remaining if admitted else 1)
            await asyncio.sleep(pause)
            if admitted:
                startup_remaining -= pause
            method, suffix, payload = "GET", path, None

    async def _release_sandbox(self, complete, deadline=None):
        if not self._sandbox_requested:
            return
        identity = self._sandbox_identity or {}
        payload = {"sandboxUID": identity.get("sandboxUID", ""), "podUID": identity.get("podUID", ""),
                   "collectionComplete": complete}
        deadline = deadline or self._sandbox_control["finalizationDeadline"]
        self._collection["releasePending"] = True
        self._save_collection()
        while True:
            data, _ = await self._sandbox_operation("POST", "/" + urllib.parse.quote(self.session_id, safe="") + "/release",
                                                    payload, deadline)
            if data.get("trialId") != self.session_id:
                raise RunnerError("sandbox release belongs to a different trial")
            if data["state"] in {"released", "retained"}:
                break
            if data["state"] != "pending" or data.get("reason") not in {"release_pending", "creation_outcome_unknown"}:
                raise RunnerError("sandbox release was not confirmed")
            await asyncio.sleep(min(1, remaining_time(deadline)))
        self._collection["releasePending"] = False
        self._collection["sandboxState"] = data["state"]
        self._save_collection()

    async def start(self, force_build):
        if self._sandbox_control is None:
            await super().start(force_build)
        else:
            if force_build:
                raise RunnerError("sandbox tasks require a prebuilt image")
            try:
                startup_remaining = await self._start_sandbox()
                async with asyncio.timeout(min(startup_remaining, remaining_time(self._sandbox_control["executionDeadline"]))):
                    await self._ensure_client()
                    for command in (f"mkdir -p {EnvironmentPaths.agent_dir} {EnvironmentPaths.verifier_dir}",
                                    f"chmod 777 {EnvironmentPaths.agent_dir} {EnvironmentPaths.verifier_dir}"):
                        if (await self.exec(command)).return_code != 0:
                            raise RunnerError("cannot prepare sandbox log directories")
                    if not self._restored:
                        await self._upload_environment_dir_after_start()
            except BaseException:
                try:
                    await self._release_sandbox(False, min(self._sandbox_control["finalizationDeadline"],
                                                           time.monotonic() + EVENT_ATTEMPT_SECONDS))
                except Exception as exc:
                    self._collection["errorCount"] += 1
                    self._collection["errors"].append({"path": "sandbox", "reason": type(exc).__name__})
                    self._save_collection()
                raise
        # Harbor's implicit artifact source is optional. An empty convention
        # directory is a successful collection, including task images which
        # only pre-create /logs; absent user-declared artifacts still fail.
        result = await self.exec("mkdir -p /logs/artifacts")
        if result.return_code != 0:
            raise RuntimeError("cannot prepare sandbox artifact directory")
        self._collection["started"] = True
        self._save_collection()

    async def stop(self, delete):
        # Free completed trials promptly so a large dataset cannot exhaust the
        # namespace's running-Pod quota. A failed transfer retains its source
        # Pod; successful transfers retain all original bytes in the Runner
        # even if the later final archive or API upload fails.
        complete = self._collection["started"] and self._collection["pending"] == 0 and self._collection["errorCount"] == 0
        try:
            manifest = self.trial_paths.artifacts_dir / "manifest.json"
            if manifest.stat().st_size > 1024 * 1024:
                raise ValueError("oversized artifact manifest")
            entries = json.loads(manifest.read_text())
            if not isinstance(entries, list) or not entries:
                raise ValueError("invalid artifact manifest")
            complete = complete and all(entry["status"] in {"ok", "empty"} for entry in entries)
        except (OSError, ValueError, KeyError, TypeError):
            complete = False
        if self._sandbox_control is None:
            await super().stop(delete=complete)
        else:
            try:
                await self._release_sandbox(complete)
            finally:
                if self._exec_api is not None:
                    self._exec_api.api_client.close()
                    self._exec_api = None
                if self._client_manager is not None:
                    await self._client_manager.release_client()
                    self._client_manager = self._core_api = self._dynamic_client = None
        self._collection["stopped"] = True
        self._save_collection()

    @staticmethod
    def type():
        return "eruun-kubernetes"

    def _resolve_user(self, user):
        # The generated Pod's runAsUser is authoritative; returning None avoids
        # ACK's `su` wrapper, including BaseEnvironment.default_user fallback.
        return None

    def _download_tar(self, command, destination, filename=None):
        # ACK 0.22.0 uses text websocket frames for tar downloads. Kubernetes'
        # UTF-8 replacement decoding irreversibly corrupts binary artifacts.
        response = stream(
            self._exec_api.connect_get_namespaced_pod_exec,
            self.pod_name, self.namespace, command=command,
            stderr=True, stdin=False, stdout=True, tty=False,
            _preload_content=False, binary=True,
        )
        with tempfile.TemporaryFile() as data:
            size = 0
            try:
                while response.is_open():
                    response.update(timeout=1)
                    if response.peek_stdout():
                        chunk = response.read_stdout()
                        if not isinstance(chunk, bytes):
                            raise RuntimeError("sandbox transfer must use binary websocket frames")
                        size += len(chunk)
                        if size > MAX_TRANSFER_BYTES:
                            raise RuntimeError("sandbox transfer exceeds 2 GiB")
                        data.write(chunk)
                    if response.peek_stderr():
                        response.read_stderr()  # Do not retain an unbounded stderr buffer.
                    # Kubernetes 32's public stream wrapper does not accept
                    # capture_all=False. Clear its duplicate aggregate buffer,
                    # preserving separate stdout/stderr/exit-status channels.
                    response._all.seek(0)
                    response._all.truncate(0)
                if response.returncode != 0:
                    raise RuntimeError("sandbox tar command failed")
            finally:
                response.close()
            data.seek(0)
            destination.mkdir(parents=True, exist_ok=True)
            with tarfile.open(fileobj=data, mode="r:") as archive:
                members = archive.getmembers()
                if len(members) > 10_000 or sum(member.size for member in members) > MAX_TRANSFER_BYTES:
                    raise RuntimeError("expanded sandbox transfer exceeds collection limits")
                if filename is not None:
                    if len(members) != 1 or not members[0].isfile():
                        raise RuntimeError("sandbox file transfer must contain one regular file")
                    members[0].name = filename
                    archive.extract(members[0], destination, filter="data")
                else:
                    archive.extractall(destination, members=members, filter="data")

    async def download_file(self, source_path, target_path):
        async with self._collecting(source_path):
            await self._ensure_client()
            target = Path(target_path)
            await asyncio.to_thread(self._download_tar, ["tar", "cf", "-", source_path], target.parent, target.name)

    async def download_dir(self, source_dir, target_dir):
        async with self._collecting(source_dir):
            await self._ensure_client()
            await asyncio.to_thread(self._download_tar,
                                    ["sh", "-c", f"cd {shlex.quote(source_dir)} && tar cf - ."], Path(target_dir))

    async def download_dir_with_exclusions(self, *, source_dir, target_dir, exclude):
        # Include archive creation/extraction failures which happen outside
        # download_file in Harbor's higher-level transfer implementation.
        async with self._collecting(source_dir):
            await super().download_dir_with_exclusions(source_dir=source_dir, target_dir=target_dir, exclude=exclude)

    async def download_dir_filtered(self, *, source_dir, target_dir, include=None, exclude=None, protect=None):
        async with self._collecting(source_dir):
            await super().download_dir_filtered(source_dir=source_dir, target_dir=target_dir,
                                                 include=include, exclude=exclude, protect=protect)

"""Run a pinned Harbor evaluation and transfer its complete local output archive."""

from __future__ import annotations

from contextlib import closing, contextmanager
import hashlib
import http.client
import importlib.metadata
import io
import json
import math
import os
from pathlib import Path, PurePosixPath
import queue
import random
import re
import signal
import socket
import stat
import subprocess
import tarfile
import tempfile
import threading
import time
import tomllib
import urllib.parse

FRAMEWORK_VERSION = "0.22.0"
MAX_PACKAGE_BYTES = 64 * 1024 * 1024
MAX_EXPANDED_BYTES = 256 * 1024 * 1024
MAX_RESULT_BYTES = 512 * 1024 * 1024
MAX_RESULT_EXPANDED_BYTES = 2 * 1024 * 1024 * 1024
MAX_FILES = 10_000
MAX_MANIFEST_BYTES = 2 * 1024 * 1024
TRANSFER_SECONDS = 300
FINALIZATION_SECONDS = 960
MAX_EXECUTION_SECONDS = 14 * 24 * 60 * 60
EVENT_ATTEMPT_SECONDS = 30
COLLECTION_SECONDS = 60
TERMINAL_RESERVE_SECONDS = 30
CONTROL_OUTAGE_SECONDS = 600
CONTROL_STATE_MAX_AGE_SECONDS = 60
HEARTBEAT_SECONDS = 15
MAX_EVENT_RESPONSE_BYTES = 64 * 1024
# Installed adapters execute in the trial sandbox, rather than importing user code.
AGENTS = frozenset({"claude-code", "codex", "terminus-2", "oracle"})


class RunnerError(Exception):
    pass


class RetryableTransferError(RunnerError):
    pass


class RunnerEventConflictError(RunnerError):
    def __init__(self, message, stop_outcome=None):
        super().__init__(message)
        self.stop_outcome = stop_outcome


def integer(value, name, minimum, maximum):
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise RunnerError(f"{name} must be an integer from {minimum} to {maximum}")
    return value


def endpoint(value):
    if not isinstance(value, str):
        raise RunnerError("platform endpoint is missing")
    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password or parsed.fragment:
        raise RunnerError("invalid platform endpoint")
    return value


def validate_config(config):
    if not isinstance(config, dict):
        raise RunnerError("runner configuration must be an object")
    for key in ("taskId", "namespace", "sandboxServiceAccount", "token"):
        if not isinstance(config.get(key), str) or not config[key] or any(c in config[key] for c in "\r\n"):
            raise RunnerError(f"invalid {key}")
    endpoint(config.get("datasetURL"))
    endpoint(config.get("resultURL"))
    endpoint(config.get("eventURL"))
    if "sandboxURL" in config:
        endpoint(config["sandboxURL"])
    digest = config.get("datasetDigest", "")
    if not isinstance(digest, str):
        raise RunnerError("datasetDigest must be SHA-256")
    digest = digest.removeprefix("sha256:")
    if not re.fullmatch(r"[a-f0-9]{64}", digest):
        raise RunnerError("datasetDigest must be SHA-256")
    config["datasetDigest"] = digest
    agent = config.get("agent")
    if not isinstance(agent, dict) or set(agent) - {"name", "model"} or agent.get("name") not in AGENTS:
        raise RunnerError("unsupported Harbor agent")
    if agent["name"] != "oracle" and not isinstance(agent.get("model"), str):
        raise RunnerError("model is required for the selected agent")
    if agent.get("model") is not None and (not isinstance(agent["model"], str) or not agent["model"].strip() or any(c in agent["model"] for c in "\r\n")):
        raise RunnerError("invalid model")
    options = config.get("options", {})
    if not isinstance(options, dict) or set(options) - {"attempts", "concurrency"}:
        raise RunnerError("unsupported Harbor option")
    config["options"] = {
        "attempts": integer(options.get("attempts", 1), "attempts", 1, 10),
        "concurrency": integer(options.get("concurrency", 1), "concurrency", 1, 32),
    }
    integer(config.get("timeoutSeconds"), "timeoutSeconds", 1, MAX_EXECUTION_SECONDS)
    integer(config.get("transferTimeoutSeconds", TRANSFER_SECONDS), "transferTimeoutSeconds", 1, 3600)
    integer(config.get("finalizationTimeoutSeconds", FINALIZATION_SECONDS), "finalizationTimeoutSeconds", 1, 3600)
    resources = config.get("resources")
    if not isinstance(resources, dict) or set(resources) != {"cpu", "memory", "cpuLimit", "memoryLimit"}:
        raise RunnerError("resources must contain requests and limits")
    if any(not isinstance(v, str) or not v for v in resources.values()):
        raise RunnerError("invalid resources")
    return config


def download_package(config, destination):
    deadline = time.monotonic() + TRANSFER_SECONDS
    attempt = 0
    while True:
        remaining_time(deadline)
        connection, target = transfer_connection(config["datasetURL"], deadline)
        digest = hashlib.sha256()
        size = 0
        try:
            output = destination.open("wb")
        except OSError:
            connection.close()
            raise
        try:
            # Every attempt owns a fresh target and digest; a partial response
            # can never be appended to or mistaken for a later successful one.
            with output:
                connection.request("GET", target, headers=transfer_headers(config))
                transfer_timeout(connection, deadline)
                with closing(connection.getresponse()) as response:
                    if response.status >= 500 or response.status in {408, 429}:
                        raise RetryableTransferError("platform temporarily rejected dataset download")
                    if response.status != 200:
                        raise RunnerError(f"platform rejected dataset download with HTTP {response.status}")
                    while True:
                        transfer_timeout(connection, deadline)
                        chunk = response.read1(1024 * 1024)
                        if not chunk:
                            break
                        size += len(chunk)
                        if size > MAX_PACKAGE_BYTES:
                            raise RunnerError("task package exceeds 64 MiB")
                        digest.update(chunk)
                        try:
                            output.write(chunk)
                        except OSError as exc:
                            raise RunnerError("cannot write task package") from exc
                    try:
                        output.flush()
                    except OSError as exc:
                        raise RunnerError("cannot write task package") from exc
            if digest.hexdigest() != config["datasetDigest"]:
                raise RunnerError("task package digest does not match submitted content")
            return
        except (OSError, http.client.HTTPException, RetryableTransferError):
            attempt += 1
            log_runner_event("dataset_retry", attempt=attempt)
            time.sleep(retry_delay(attempt, deadline))
        finally:
            connection.close()


def extract_package(source, destination):
    """Reject aliases, links, devices, duplicate members and decompression bombs."""
    destination.mkdir()
    seen = set()
    total = 0
    with tarfile.open(source, "r:gz") as archive:
        for index, member in enumerate(archive):
            if index >= MAX_FILES:
                raise RunnerError("task package contains too many entries")
            raw = member.name
            path = PurePosixPath(raw)
            # A tar made with `tar -C task .` has an optional root directory entry.
            if raw in {".", "./"} and member.isdir():
                continue
            if path.is_absolute() or ".." in path.parts or "\\" in raw or "\x00" in raw or not path.parts or len(raw.encode()) > 1024:
                raise RunnerError("task package contains an unsafe path")
            name = path.as_posix()
            if name in seen:
                raise RunnerError("task package contains duplicate paths")
            seen.add(name)
            if not (member.isfile() or member.isdir()):
                raise RunnerError("task package may contain only files and directories")
            total += member.size
            if member.size < 0 or total > MAX_EXPANDED_BYTES:
                raise RunnerError("expanded task package exceeds 256 MiB")
            target = destination.joinpath(*path.parts)
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                if target.exists():
                    raise RunnerError("task package paths conflict")
                input_file = archive.extractfile(member)
                if input_file is None:
                    raise RunnerError("task package file is unreadable")
                with input_file, target.open("xb") as output:
                    while chunk := input_file.read(1024 * 1024):
                        output.write(chunk)
                target.chmod(0o700 if member.mode & 0o111 else 0o600)


def native_tasks(directory):
    roots = sorted(p.parent for p in directory.rglob("task.toml"))
    if not roots:
        raise RunnerError("task package contains no native Harbor tasks")
    for root in roots:
        if any(parent in roots for parent in root.parents):
            raise RunnerError("nested task definitions overlap")
        for required in ("instruction.md", "task.toml", "environment/Dockerfile", "tests/test.sh"):
            if not (root / required).is_file():
                raise RunnerError(f"Harbor task is missing {required}")
        data = tomllib.loads((root / "task.toml").read_text())
        environment = data.get("environment", {})
        image = environment.get("docker_image")
        if not isinstance(image, str) or not image or image.endswith(":latest"):
            raise RunnerError("each task must specify a prebuilt image with an explicit version or digest")
        last = image.rsplit("/", 1)[-1]
        if ":" not in last or not last.rsplit(":", 1)[-1] or any(c.isspace() for c in image):
            raise RunnerError("task image must have an explicit version or digest")
        if environment.get("os", "linux") != "linux" or list(root.rglob("docker-compose.y*ml")):
            raise RunnerError("only single-container Linux Harbor tasks are supported")
        if data.get("steps") or data.get("verifier", {}).get("environment") or data.get("verifier", {}).get("environment_mode", "shared") != "shared":
            raise RunnerError("separate verifier environments and multi-step tasks are not supported")
        for phase in ("agent", "verifier"):
            if data.get(phase, {}).get("user") not in {None, 1000, "1000"}:
                raise RunnerError("task agent and verifier must use the fixed workspace user 1000")
        # Arbitrary task content runs in the sandbox. Host-sensitive integrations are excluded.
        if environment.get("gpus", 0) or environment.get("tpu") or environment.get("mcp_servers"):
            raise RunnerError("GPU, TPU and task MCP integrations are not supported")
    return roots


def harbor_config(config, tasks, output, pod_name, pod_uid):
    if not pod_name or not pod_uid:
        raise RunnerError("POD_NAME and POD_UID are required for sandbox ownership")
    resources = config["resources"]
    return {
        "job_name": "run", "jobs_dir": str(output),
        "n_attempts": config["options"]["attempts"],
        "n_concurrent_trials": config["options"]["concurrency"],
        "retry": {"max_retries": 0}, "quiet": True,
        "agents": [{"name": config["agent"]["name"], "model_name": config["agent"].get("model")}],
        "tasks": [{"path": str(p), "source": "uploaded"} for p in tasks],
        "environment": {
            "import_path": "eruun_environment:WorkspaceEnvironment", "force_build": False, "delete": False,
            "kwargs": {
                "collection_state_dir": str(output.parent / "collection"),
                **({"sandbox_control_file": str(output.parent / "sandbox-control.json")}
                   if config.get("sandboxURL") else {}),
                "namespace": config["namespace"], "skip_image_check": True,
                "use_sandbox_claim": False, "service_account": config["sandboxServiceAccount"],
                "pod_overrides": {
                    "metadata": {
                        "ownerReferences": [{"apiVersion": "v1", "kind": "Pod", "name": pod_name, "uid": pod_uid}],
                        "labels": {"eruun.io/task-id": config["taskId"]},
                    },
                    "spec": {
                        "automountServiceAccountToken": False,
                        "activeDeadlineSeconds": config["timeoutSeconds"],
                        "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}},
                        "containers": [{
                            "securityContext": {
                                "privileged": False, "allowPrivilegeEscalation": False,
                                "runAsNonRoot": True, "runAsUser": 1000, "runAsGroup": 1000,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "env": [{"name": "HOME", "value": "/home/agent"},
                                    {"name": "NPM_CONFIG_PREFIX", "value": "/home/agent/.npm-global"}],
                            "resources": {
                                "requests": {"cpu": resources["cpu"], "memory": resources["memory"]},
                                "limits": {"cpu": resources["cpuLimit"], "memory": resources["memoryLimit"]},
                            },
                        }],
                    },
                },
            },
        },
    }


def framework_status(result_path, exit_code):
    if exit_code != 0:
        return "failed"
    try:
        result = json.loads(result_path.read_text())
        stats = result["stats"]
        total = result["n_total_trials"]
        if isinstance(total, bool) or not isinstance(total, int) or total <= 0 or not result.get("finished_at"):
            return "failed"
        counts = {name: stats[name] for name in ("n_completed_trials", "n_errored_trials", "n_running_trials", "n_pending_trials", "n_cancelled_trials")}
        if any(isinstance(v, bool) or not isinstance(v, int) or v < 0 for v in counts.values()):
            return "failed"
        if counts["n_completed_trials"] != total or any(counts[name] for name in counts if name != "n_completed_trials"):
            return "failed"
        return "succeeded"
    except (OSError, ValueError, KeyError, TypeError):
        return "failed"


def run_framework(config_path, output, cancel, timeout):
    # Never expose Eruun's transfer capability to task-controlled ${VAR} expansion.
    child_env = {key: value for key, value in os.environ.items() if not key.startswith("ERUUN_")}
    child_env["HARBOR_TELEMETRY"] = "0"
    with (output / "harbor-console.log").open("wb") as log:
        process = subprocess.Popen(["harbor", "run", "--config", str(config_path), "--yes"],
                                   cwd=config_path.parent, env=child_env, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        deadline = time.monotonic() + timeout
        while process.poll() is None:
            if cancel.wait(0.2) or time.monotonic() >= deadline:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                return process.returncode, "cancelled" if cancel.is_set() else "timed_out"
        return process.returncode, None


def manifest_entry_size(path, size, link=None):
    """Match the API's compact encoding/json ManifestEntry wire budget."""
    entry = {"path": path, "size": size}
    if link is None:
        entry["digest"] = "0" * 64
    else:
        entry["linkTarget"] = link
    encoded = json.dumps(entry, ensure_ascii=False, separators=(",", ":"))
    for raw, escaped in (("<", "\\u003c"), (">", "\\u003e"), ("&", "\\u0026"), ("\u2028", "\\u2028"), ("\u2029", "\\u2029")):
        encoded = encoded.replace(raw, escaped)
    return len(encoded.encode())


def collection_error(report, path, reason, count=1):
    report["collectionErrorCount"] = report.get("collectionErrorCount", 0) + count
    errors = report.setdefault("collectionErrors", [])
    if len(errors) < 100:
        errors.append({"path": str(path)[:1024], "reason": reason})


def remaining_time(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("runner operation exceeded its total time budget")
    return remaining


def ensure_deadline(deadline):
    if deadline is not None:
        remaining_time(deadline)


class _DeadlineReader:
    """Make tarfile's otherwise blocking copy loop observe the shared deadline."""

    def __init__(self, source, deadline):
        self.source = source
        self.deadline = deadline

    def read(self, size=-1):
        ensure_deadline(self.deadline)
        contents = self.source.read(size)
        ensure_deadline(self.deadline)
        return contents


def check_sandbox_collection(output, expected_trials, report, deadline=None):
    """Account for data which Harbor failed to transfer into local outputs."""
    try:
        records = []
        for path in (output.parent / "collection").glob("*.json"):
            ensure_deadline(deadline)
            records.append(path)
        if len(records) != expected_trials:
            collection_error(report, "trials", "missing_trial_collection_state")
        for path in records:
            ensure_deadline(deadline)
            try:
                if path.stat().st_size > 1024 * 1024:
                    raise ValueError("oversized collection state")
                state_contents = path.read_text()
                ensure_deadline(deadline)
                state = json.loads(state_contents)
                ensure_deadline(deadline)
                if state["started"] is not True or state["stopped"] is not True or state["pending"] != 0:
                    collection_error(report, state.get("podName", "trial"), "unfinished_sandbox_collection")
                if state["errorCount"]:
                    collection_error(report, state.get("podName", "trial"), "sandbox_download_failed", state["errorCount"])
                trial = Path(state["trialDirectory"])
                trial.resolve().relative_to(output.resolve())
                manifest_path = trial / "artifacts" / "manifest.json"
                if manifest_path.stat().st_size > 1024 * 1024:
                    raise ValueError("oversized artifact manifest")
                manifest_contents = manifest_path.read_text()
                ensure_deadline(deadline)
                entries = json.loads(manifest_contents)
                ensure_deadline(deadline)
                if not isinstance(entries, list) or not entries:
                    raise ValueError("invalid artifact manifest")
                for entry in entries:
                    ensure_deadline(deadline)
                    if entry["status"] not in {"ok", "empty"}:
                        collection_error(report, entry["source"], "native_artifact_collection_failed")
            except TimeoutError:
                raise
            except (OSError, ValueError, KeyError, TypeError):
                collection_error(report, "trial", "unreadable_collection_state_or_manifest")
    except TimeoutError:
        raise
    except OSError:
        collection_error(report, "trials", "unreadable_collection_state")
    ensure_deadline(deadline)


def archive_results(output, archive_path, report, deadline=None):
    """Keep original bytes and safe links, surfacing every omitted entry."""
    ensure_deadline(deadline)
    entries = [output]
    issues = list(report.get("collectionErrors", []))
    issue_count = report.get("collectionErrorCount", 0)
    total = 0
    # JSON array delimiters and the reserved platform summary entry. Report
    # contents do not enter the manifest; only its size and digest do.
    manifest_size = 2 + manifest_entry_size("result.json", 1024 * 1024)

    def issue(path, reason):
        nonlocal issue_count
        issue_count += 1
        if len(issues) < 100:
            issues.append({"path": str(path)[:1024], "reason": reason})

    def walk_error(error):
        ensure_deadline(deadline)
        issue(error.filename or ".", "unreadable_directory")

    for directory, directories, files in os.walk(output, onerror=walk_error, followlinks=False):
        ensure_deadline(deadline)
        for name in sorted(directories + files):
            ensure_deadline(deadline)
            path = Path(directory) / name
            relative = path.relative_to(output).as_posix()
            try:
                mode = path.lstat().st_mode
                if len(("outputs/" + relative).encode()) > 1024:
                    issue(relative, "path_too_long")
                    continue
                if stat.S_ISLNK(mode):
                    link = os.readlink(path)
                    if PurePosixPath(link).is_absolute() or "\\" in link or "\x00" in link:
                        raise ValueError("unsafe link")
                    target = Path(os.path.normpath(path.parent / link))
                    target.relative_to(output)
                    if any(p.is_symlink() for p in [target, *target.parents] if p != output and output in p.parents):
                        raise ValueError("chained link")
                    path.resolve(strict=True).relative_to(output.resolve())
                elif not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
                    issue(relative, "unsupported_file_type")
                    continue
                size = path.stat().st_size if stat.S_ISREG(mode) else 0
                # Reserve one archive member and 1 MiB for the platform report.
                if len(entries) >= MAX_FILES - 1:
                    issue(relative, "too_many_entries")
                    continue
                if total + size > MAX_RESULT_EXPANDED_BYTES - 1024 * 1024:
                    issue(relative, "expanded_size_limit")
                    continue
                if not stat.S_ISDIR(mode):
                    entry_size = 1 + manifest_entry_size("outputs/" + relative, size,
                                                        os.readlink(path) if stat.S_ISLNK(mode) else None)
                    if manifest_size + entry_size > MAX_MANIFEST_BYTES:
                        issue(relative, "manifest_size_limit")
                        continue
                    manifest_size += entry_size
                total += size
                entries.append(path)
            except TimeoutError:
                raise
            except (ValueError, OSError, RuntimeError):
                issue(relative, "unsafe_or_unreadable_entry")

    ensure_deadline(deadline)
    with tarfile.open(archive_path, "w:gz", dereference=False) as archive:
        saved = set()
        # Links follow all ordinary entries, so they cannot refer to an omitted file.
        def link_order(path):
            ensure_deadline(deadline)
            return path.is_symlink()

        entries.sort(key=link_order)
        for path in entries:
            ensure_deadline(deadline)
            relative = path.relative_to(output).as_posix()
            try:
                info = archive.gettarinfo(str(path), arcname="outputs" if path == output else "outputs/" + relative)
                if info.issym():
                    target = Path(os.path.normpath(path.parent / info.linkname))
                    if target not in saved:
                        issue(relative, "link_target_not_collected")
                        continue
                if info.isfile() or info.islnk():
                    source = path.open("rb")
                    # OS hardlinks become regular entries with the same original bytes.
                    info.type, info.linkname, info.size = tarfile.REGTYPE, "", path.stat().st_size
                    with source:
                        archive.addfile(info, _DeadlineReader(source, deadline))
                else:
                    archive.addfile(info)
                saved.add(path)
            except TimeoutError:
                raise
            except OSError:
                issue(relative, "unreadable_file")
        ensure_deadline(deadline)
        report["collectionComplete"] = issue_count == 0
        if issue_count:
            report["collectionErrors"] = issues
            report["collectionErrorCount"] = issue_count
        report_bytes = json.dumps(report, ensure_ascii=False, allow_nan=False).encode()
        ensure_deadline(deadline)
        info = tarfile.TarInfo("result.json")
        info.size, info.mode = len(report_bytes), 0o600
        archive.addfile(info, _DeadlineReader(io.BytesIO(report_bytes), deadline))
        ensure_deadline(deadline)
    ensure_deadline(deadline)
    if archive_path.stat().st_size > MAX_RESULT_BYTES:
        raise RunnerError("complete result archive exceeds 512 MiB")
    ensure_deadline(deadline)


def transfer_headers(config):
    pod_name, pod_uid = os.environ.get("POD_NAME"), os.environ.get("POD_UID")
    if not pod_name or not pod_uid or any(c in pod_name + pod_uid for c in "\r\n"):
        raise RunnerError("POD_NAME and POD_UID are required for source transfer")
    return {"Authorization": "Bearer " + config["token"],
            "X-Eruun-Runner-Pod-Name": pod_name, "X-Eruun-Runner-Pod-UID": pod_uid}


def log_runner_event(event, **fields):
    record = {"component": "harbor-runner", "event": event, **fields}
    print(json.dumps(record, sort_keys=True, separators=(",", ":")), flush=True)


def transfer_connection(endpoint_url, deadline):
    url = urllib.parse.urlsplit(endpoint_url)
    cls = http.client.HTTPSConnection if url.scheme == "https" else http.client.HTTPConnection
    connection = cls(url.hostname, url.port, timeout=min(30, remaining_time(deadline)))
    target = urllib.parse.urlunsplit(("", "", url.path or "/", url.query, ""))
    return connection, target


def transfer_timeout(connection, deadline):
    remaining = remaining_time(deadline)
    if connection.sock is not None:
        connection.sock.settimeout(remaining)


@contextmanager
def transfer_deadline(connection, deadline):
    """Interrupt socket I/O at an absolute deadline, including trickling peers.

    Keep the connected socket: getresponse() can detach it from the connection
    before reading a Connection: close response. DNS resolution remains subject
    to the system resolver; the deadline is checked before sending any request.
    """
    connected_socket = None
    expired = threading.Event()

    def expire():
        expired.set()
        active_socket = connected_socket or connection.sock
        if active_socket is not None:
            try:
                active_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    timer = threading.Timer(remaining_time(deadline), expire)
    timer.daemon = True
    timer.start()
    try:
        connection.connect()
        connected_socket = connection.sock
        remaining_time(deadline)
        yield
    finally:
        timer.cancel()
        timer.join()
        connection.close()
        if expired.is_set():
            raise TimeoutError("runner HTTP attempt exceeded its total time budget") from None
    remaining_time(deadline)


def retry_delay(attempt, deadline):
    # Equal jitter keeps retries spaced out, with a fixed five-second ceiling.
    ceiling = min(5.0, 0.25 * 2 ** min(attempt, 5))
    return min(random.uniform(ceiling / 2, ceiling), remaining_time(deadline))


def post_runner_event(config, event, deadline):
    deadline = min(deadline, time.monotonic() + EVENT_ATTEMPT_SECONDS)
    connection, target = transfer_connection(config["eventURL"], deadline)
    encoded = json.dumps(event, separators=(",", ":")).encode()
    with transfer_deadline(connection, deadline):
        connection.request("POST", target, body=encoded,
                           headers=transfer_headers(config) | {"Content-Type": "application/json"})
        transfer_timeout(connection, deadline)
        with closing(connection.getresponse()) as response:
            body = response.read(MAX_EVENT_RESPONSE_BYTES + 1)
        remaining_time(deadline)
        if response.status >= 500 or response.status in {408, 429}:
            raise RetryableTransferError("platform temporarily rejected runner event")
        if response.status == 409:
            stop_outcome = None
            if len(body) <= MAX_EVENT_RESPONSE_BYTES:
                try:
                    conflict = json.loads(body)
                    candidate = conflict["data"]["stopOutcome"]
                    if conflict["code"] == 34004 and candidate in {"cancelled", "timed_out"}:
                        stop_outcome = candidate
                except (ValueError, KeyError, TypeError):
                    pass
            raise RunnerEventConflictError("platform rejected runner event with HTTP 409", stop_outcome)
        if not 200 <= response.status < 300:
            raise RunnerError(f"platform rejected runner event with HTTP {response.status}")
        if len(body) > MAX_EVENT_RESPONSE_BYTES:
            raise RunnerError("runner event acknowledgment is too large")
        try:
            response_data = json.loads(body)
        except ValueError:
            raise RetryableTransferError("runner event acknowledgment could not be decoded") from None
        try:
            data = response_data["data"]
            accepted = data["acceptedSequence"]
            action = data["action"]
            stop_outcome = data.get("stopOutcome")
        except (KeyError, TypeError):
            raise RunnerError("invalid runner event acknowledgment") from None
        if isinstance(accepted, bool) or not isinstance(accepted, int) or accepted < event["sequence"] or action not in {"continue", "stop"}:
            raise RunnerError("invalid runner event acknowledgment")
        if ((action == "stop" and stop_outcome not in {"cancelled", "timed_out"})
                or (action == "continue" and stop_outcome is not None)):
            raise RunnerError("invalid runner event acknowledgment")
        return action, stop_outcome


def collection_progress(collection, total):
    completed = 0
    try:
        for path in list(collection.glob("*.json"))[:MAX_FILES]:
            try:
                if path.stat().st_size <= 1024 * 1024 and json.loads(path.read_text()).get("stopped") is True:
                    completed += 1
            except (OSError, ValueError, TypeError):
                continue
    except OSError:
        pass
    return {"completedTrials": min(completed, total), "totalTrials": total}


class StatusReporter:
    """Serialize event sequence allocation and retry without supervising Harbor."""

    def __init__(self, config, cancel, deadline):
        self.config = config
        self.cancel = cancel
        self.deadline = deadline
        self.snapshot_deadline = deadline
        self.events = queue.Queue(maxsize=16)
        self.terminal_ready = threading.Event()
        self.stop = threading.Event()
        self.failure = None
        self.sequence = 1
        self.progress_source = None
        self.last_progress = None
        self.thread = None
        self.stop_outcome = None
        self.outage_started = None
        self.outage_exhausted = False
        self.finalizing = False
        self.admission_path = None
        self.admission_lock = threading.Lock()
        self.admission_stopped = False

    def configure_admission(self, path):
        self.admission_path = path
        self._publish_admission()

    def _publish_admission(self):
        if self.admission_path is None:
            return
        with self.admission_lock:
            self.admission_stopped |= self.cancel.is_set() or self.failure is not None or self.finalizing
            state = {"healthy": self.outage_started is None and not self.admission_stopped,
                     "stopped": self.admission_stopped, "updatedAt": time.monotonic(),
                     "outageStarted": self.outage_started}
            temporary = self.admission_path.with_suffix(".tmp")
            try:
                descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
                with os.fdopen(descriptor, "w") as target:
                    os.fchmod(target.fileno(), 0o600)
                    json.dump(state, target)
                temporary.replace(self.admission_path)
            except OSError:
                raise RunnerError("cannot persist runner admission state") from None

    def _send_with_retry(self, event):
        attempt = 0
        may_update_terminal = True
        while True:
            if self.stop.is_set():
                raise RunnerError("runner event reporter stopped")
            if self.terminal_ready.is_set() and event["kind"] not in {"claim", "terminal"}:
                return
            deadline = self.deadline
            if self.outage_started is not None and not self.finalizing:
                deadline = min(deadline, self.outage_started + CONTROL_OUTAGE_SECONDS)
            if event["kind"] not in {"claim", "terminal"}:
                if time.monotonic() >= self.snapshot_deadline:
                    return
                deadline = min(deadline, self.snapshot_deadline)
            if (may_update_terminal and event["kind"] == "terminal" and self.stop_outcome is None
                    and self.cancel.is_set() and not self.outage_exhausted):
                self.stop_outcome = "cancelled"
            if (may_update_terminal and event["kind"] == "terminal" and self.stop_outcome is not None
                    and event["terminal"]["outcome"] != self.stop_outcome):
                reason = "evaluation_cancelled" if self.stop_outcome == "cancelled" else "evaluation_timed_out"
                terminal = {**event["terminal"], "outcome": self.stop_outcome, "reason": reason}
                event = {**event, "terminal": terminal}
            remaining_time(deadline)
            try:
                # A lost response may follow a committed terminal. Only an
                # explicit rejection permits changing its digest at this sequence.
                may_update_terminal = False
                started = time.monotonic()
                action, stop_outcome = post_runner_event(self.config, event, deadline)
                self.outage_started = None
                if action == "stop":
                    self.stop_outcome = stop_outcome
                    self.cancel.set()
                self._publish_admission()
                return
            except (OSError, http.client.HTTPException, RetryableTransferError):
                if self.outage_started is None:
                    self.outage_started = started
                self._publish_admission()
                if self.terminal_ready.is_set() and event["kind"] not in {"claim", "terminal"}:
                    return
                if self.finalizing:
                    deadline = min(self.deadline, self.snapshot_deadline) if event["kind"] not in {"claim", "terminal"} else self.deadline
                if event["kind"] not in {"claim", "terminal"} and time.monotonic() >= deadline:
                    if deadline == self.snapshot_deadline:
                        return
                    raise TimeoutError("runner control outage exceeded its time budget") from None
                # The framework's outage tolerance stops execution at 600s;
                # result delivery uses the already fixed finalization window.
                if not self.finalizing:
                    deadline = min(deadline, self.outage_started + CONTROL_OUTAGE_SECONDS)
                attempt += 1
                log_runner_event("event_retry", kind=event["kind"], attempt=attempt)
                if self.stop.wait(retry_delay(attempt, deadline)):
                    raise RunnerError("runner event reporter stopped")
            except RunnerEventConflictError as exc:
                # An authoritative stop may race the first terminal request. A rejected
                # event did not advance the server sequence, so retry it once
                # with the authoritative stop outcome.
                if event["kind"] == "terminal" and exc.stop_outcome is not None:
                    self.cancel.set()
                    self.stop_outcome = exc.stop_outcome
                elif event["kind"] == "terminal" and self.stop_outcome is None and self.cancel.is_set():
                    self.stop_outcome = "cancelled"
                else:
                    raise
                if event["terminal"]["outcome"] != self.stop_outcome:
                    may_update_terminal = True
                    continue
                raise

    def claim(self):
        event = {"protocolVersion": "v1", "sequence": 1, "kind": "claim"}
        self._send_with_retry(event)
        if self.cancel.is_set():
            raise RunnerError("platform stopped evaluation after claim")
        self.thread = threading.Thread(target=self._run, name="runner-events", daemon=True)
        self.thread.start()

    def emit(self, kind, **fields):
        if self.failure is not None:
            return
        try:
            self.events.put_nowait(({"kind": kind, **fields}, None))
        except queue.Full:
            self.failure = RunnerError("runner event queue is full")

    def track_progress(self, collection, total):
        self.progress_source = (collection, total)

    def shorten_deadline(self, deadline):
        self.finalizing = True
        self.deadline = min(self.deadline, deadline)
        self._publish_admission()
        return self.deadline

    def _next_event(self, event):
        self.sequence += 1
        return {"protocolVersion": "v1", "sequence": self.sequence, **event}

    def _send_progress(self):
        if self.progress_source is None:
            return
        progress = collection_progress(*self.progress_source)
        if progress == self.last_progress:
            return
        self._send_with_retry(self._next_event({"kind": "progress", "progress": progress}))
        self.last_progress = progress

    def _run(self):
        next_heartbeat = time.monotonic() + HEARTBEAT_SECONDS
        try:
            while not self.stop.is_set():
                timeout = max(0, min(1, next_heartbeat - time.monotonic()))
                try:
                    event, completed = self.events.get(timeout=timeout)
                except queue.Empty:
                    event = completed = None
                if event is not None:
                    self._send_with_retry(self._next_event(event))
                    if completed is not None:
                        completed.set()
                    if event["kind"] == "terminal":
                        return
                if time.monotonic() >= next_heartbeat and not self.stop.is_set() and not self.terminal_ready.is_set():
                    self._send_with_retry(self._next_event({"kind": "heartbeat"}))
                    self._send_progress()
                    next_heartbeat = time.monotonic() + HEARTBEAT_SECONDS
        except Exception as exc:
            self.failure = exc
            self.outage_exhausted = (isinstance(exc, TimeoutError) and self.outage_started is not None
                                     and time.monotonic() >= self.outage_started + CONTROL_OUTAGE_SECONDS)
            self.cancel.set()
            try:
                self._publish_admission()
            except RunnerError:
                pass  # A stale admission record also fails closed in Harbor.
            while True:
                try:
                    _, completed = self.events.get_nowait()
                    if completed is not None:
                        completed.set()
                except queue.Empty:
                    break

    def terminal(self, terminal):
        if self.failure is not None:
            if not self.finalizing or not self.outage_exhausted:
                raise self.failure
            # The reporter stopped execution after a runtime outage. Join its
            # sequence owner before delivering the failed final fact directly.
            if self.thread is not None:
                self.thread.join(timeout=min(EVENT_ATTEMPT_SECONDS, remaining_time(self.deadline)))
                if self.thread.is_alive():
                    raise self.failure
            self._send_with_retry(self._next_event({"kind": "terminal", "terminal": terminal}))
            self.failure = None
            return
        completed = threading.Event()
        try:
            self.events.put_nowait(({"kind": "terminal", "terminal": terminal}, completed))
        except queue.Full:
            raise RunnerError("runner event queue is full") from None
        # Phase/progress are snapshots, not an audit log. Once this fact is
        # queued, finish the current HTTP attempt then skip older snapshots.
        # _next_event still allocates a higher sequence for the terminal.
        self.terminal_ready.set()
        while not completed.wait(min(1, remaining_time(self.deadline))):
            if self.failure is not None:
                raise self.failure
        if self.failure is not None:
            raise self.failure

    def close(self):
        self.stop.set()
        if self.thread is not None:
            self.thread.join(timeout=max(0, min(5, self.deadline - time.monotonic())))


def upload_results(config, archive_path, status, deadline=None):
    if deadline is None:
        deadline = time.monotonic() + TRANSFER_SECONDS
    deadline = min(deadline, time.monotonic() + config.get("transferTimeoutSeconds", TRANSFER_SECONDS))
    connection, target = transfer_connection(config["resultURL"], deadline)
    with transfer_deadline(connection, deadline):
        connection.putrequest("POST", target)
        for name, value in transfer_headers(config).items():
            connection.putheader(name, value)
        connection.putheader("Content-Type", "application/gzip")
        connection.putheader("Content-Length", str(archive_path.stat().st_size))
        connection.putheader("X-Eruun-Evaluation-Status", status)
        connection.endheaders()
        with archive_path.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                transfer_timeout(connection, deadline)
                connection.send(chunk)
        transfer_timeout(connection, deadline)
        with closing(connection.getresponse()) as response:
            body = response.read(MAX_EVENT_RESPONSE_BYTES + 1)
        remaining_time(deadline)
        if not 200 <= response.status < 300:
            if response.status >= 500 or response.status in {408, 429}:
                raise RetryableTransferError("platform temporarily rejected source upload")
            raise RunnerError(f"platform rejected source archive with HTTP {response.status}")
        if len(body) > MAX_EVENT_RESPONSE_BYTES:
            raise RunnerError("source acknowledgment is too large")
        try:
            response_data = json.loads(body)
        except ValueError:
            raise RetryableTransferError("source acknowledgment could not be decoded") from None
        try:
            artifact = response_data["data"]
        except (KeyError, TypeError):
            raise RunnerError("invalid source acknowledgment") from None
        if not isinstance(artifact, dict) or not isinstance(artifact.get("id"), str) or not isinstance(artifact.get("digest"), str):
            raise RunnerError("invalid source acknowledgment")
        return artifact


def upload_with_retry(config, archive, status, deadline=None):
    # The first durable source is immutable. Retries must send this same file,
    # without rebuilding gzip (which could change metadata and its digest).
    if deadline is None:
        deadline = time.monotonic() + TRANSFER_SECONDS
    attempt = 0
    while True:
        remaining_time(deadline)
        try:
            return upload_results(config, archive, status, deadline=deadline)
        except (OSError, http.client.HTTPException, RetryableTransferError):
            attempt += 1
            log_runner_event("upload_retry", attempt=attempt)
            time.sleep(retry_delay(attempt, deadline))


def execute(config, work, cancel, reporter=None, final_deadline=None):
    output = work / "outputs"
    output.mkdir()
    (work / "collection").mkdir(mode=0o700)
    expected_trials = 0
    execution_deadline = None
    report = {"taskId": config["taskId"], "framework": {"name": "harbor", "version": FRAMEWORK_VERSION},
              "datasetDigest": config["datasetDigest"], "executionStatus": "failed", "frameworkExitCode": None}
    try:
        if reporter is not None:
            reporter.emit("phase", phase="preparing")
        if importlib.metadata.version("harbor") != FRAMEWORK_VERSION:
            raise RunnerError("installed Harbor version does not match the pinned runner")
        source = work / "tasks.tar.gz"
        download_package(config, source)
        dataset = work / "dataset"
        extract_package(source, dataset)
        tasks = native_tasks(dataset)
        expected_trials = len(tasks) * config["options"]["attempts"]
        if reporter is not None:
            reporter.track_progress(work / "collection", expected_trials)
        generated = harbor_config(config, tasks, output, os.environ.get("POD_NAME"), os.environ.get("POD_UID"))
        if config.get("sandboxURL"):
            if reporter is None:
                raise RunnerError("sandbox execution requires a status reporter")
            admission_path = work / "admission-state.json"
            reporter.configure_admission(admission_path)
            execution_deadline = time.monotonic() + config["timeoutSeconds"]
            grace = config.get("finalizationTimeoutSeconds", FINALIZATION_SECONDS)
            if final_deadline is not None:
                execution_deadline = min(execution_deadline, final_deadline - grace)
            budget = remaining_time(execution_deadline)
            build_timeouts = [tomllib.loads((task / "task.toml").read_text()).get("environment", {}).get("build_timeout_sec", 600)
                              for task in tasks]
            if any(isinstance(value, bool) or not isinstance(value, (int, float))
                   or not math.isfinite(value) or value <= 0 for value in build_timeouts):
                raise RunnerError("task environment build timeout must be positive")
            # Harbor wraps start() in a build timeout. Queue admission belongs
            # to the Job budget; the adapter enforces each original build
            # timeout only after the server admits that environment.
            generated["environment_build_timeout_multiplier"] = max(1, budget / min(build_timeouts))
            control = {key: config[key] for key in ("sandboxURL", "token", "taskId", "namespace")}
            control["executionDeadline"] = execution_deadline
            control["finalizationDeadline"] = execution_deadline + grace
            control["admissionStateFile"] = str(admission_path)
            descriptor = os.open(work / "sandbox-control.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w") as target:
                json.dump(control, target)
        config_path = work / "harbor-config.json"
        config_path.write_text(json.dumps(generated))
        if cancel.is_set() and reporter.outage_exhausted is True:
            succeeded = False
            outcome = "failed"
        elif cancel.is_set():
            raise RunnerError("cancelled before framework start")
        if reporter is not None:
            reporter.emit("phase", phase="running")
        log_runner_event("harbor_start")
        framework_timeout = config["timeoutSeconds"]
        if execution_deadline is not None:
            framework_timeout = min(framework_timeout, remaining_time(execution_deadline))
        code, interrupted = run_framework(config_path, output, cancel, framework_timeout)
        report["frameworkExitCode"] = code
        report["executionStatus"] = framework_status(output / "run" / "result.json", code)
        if interrupted:
            report["executionStatus"] = "failed"
            report["interruption"] = interrupted
            collection_error(report, "trials", "framework_interrupted")
    except Exception as exc:
        # Exception values may contain environment-expanded credentials; retain the category only.
        report["error"] = type(exc).__name__
        if isinstance(exc, RunnerError):
            report["error"] = str(exc)
        if cancel.is_set() and "interruption" not in report:
            report["interruption"] = "cancelled"
            collection_error(report, "trials", "framework_interrupted")
    finalization_deadline = time.monotonic() + config.get("finalizationTimeoutSeconds", FINALIZATION_SECONDS)
    if final_deadline is not None:
        finalization_deadline = min(finalization_deadline, final_deadline)
    if reporter is not None:
        finalization_deadline = reporter.shorten_deadline(finalization_deadline)
        reporter.emit("phase", phase="finalizing")
    budget = remaining_time(finalization_deadline)
    terminal_reserve = min(TERMINAL_RESERVE_SECONDS, budget / 3)
    upload_deadline = finalization_deadline - terminal_reserve
    if reporter is not None:
        # A snapshot HTTP retry must not occupy the terminal's reserved window.
        reporter.snapshot_deadline = upload_deadline
    collection_deadline = time.monotonic() + min(COLLECTION_SECONDS, budget / 6)
    archive = work / "results.tar.gz"
    try:
        check_sandbox_collection(output, expected_trials, report, deadline=collection_deadline)
        archive_results(output, archive, report, deadline=collection_deadline)
    except (RunnerError, OSError, tarfile.TarError) as exc:
        # Preserve the local originals for Pod retention and make missing source
        # explicit; never label a diagnostic-only archive as full collection.
        report["collectionComplete"] = False
        report["collectionErrors"] = [{"path": "outputs", "reason": type(exc).__name__}]
        report["collectionErrorCount"] = 1
        report["diagnosticOnly"] = True
        contents = json.dumps(report, ensure_ascii=False).encode()
        diagnostic_deadline = min(upload_deadline, time.monotonic() + 5)
        ensure_deadline(diagnostic_deadline)
        with tarfile.open(archive, "w:gz") as target:
            info = tarfile.TarInfo("result.json")
            info.size, info.mode = len(contents), 0o600
            target.addfile(info, _DeadlineReader(io.BytesIO(contents), diagnostic_deadline))
        ensure_deadline(diagnostic_deadline)
    status = report["executionStatus"] if report["collectionComplete"] and not cancel.is_set() else "failed"
    artifact = upload_with_retry(config, archive, status, deadline=upload_deadline)
    succeeded = not cancel.is_set() and report["executionStatus"] == "succeeded" and report["collectionComplete"]
    if reporter is not None:
        if cancel.is_set():
            succeeded = False
            outcome = "cancelled"
        else:
            outcome = "succeeded" if succeeded else report.get("interruption", "failed")
        if outcome not in {"timed_out", "cancelled", "succeeded"}:
            outcome = "failed"
        if succeeded:
            reason = "evaluation_succeeded"
        elif outcome == "cancelled":
            reason = "evaluation_cancelled"
        else:
            reason = "evaluation_failed"
        exit_code = report.get("frameworkExitCode")
        terminal = {
            "outcome": outcome,
            "artifactId": artifact["id"],
            "artifactDigest": artifact["digest"],
            "collectionComplete": bool(report["collectionComplete"]),
            "reason": reason,
        }
        if isinstance(exit_code, int) and exit_code >= 0:
            terminal["exitCode"] = min(exit_code, 255)
        elif isinstance(exit_code, int) and exit_code < 0:
            try:
                terminal["signal"] = signal.Signals(-exit_code).name
            except ValueError:
                terminal["signal"] = "UNKNOWN"
        reporter.terminal(terminal)
    return 0 if succeeded and not cancel.is_set() else 1


def main():
    cancel = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: cancel.set())
    signal.signal(signal.SIGINT, lambda *_: cancel.set())
    try:
        config = validate_config(json.loads(os.environ["ERUUN_JOB_CONFIG"]))
        started = time.monotonic()
        final_deadline = started + config["timeoutSeconds"] + config.get("finalizationTimeoutSeconds", FINALIZATION_SECONDS)
        reporter = StatusReporter(config, cancel, final_deadline)
        reporter.claim()
        Path("/work/home").mkdir(parents=True, exist_ok=True)
        # Leave raw output and the exact final archive in the Pod until the
        # platform's retention policy removes it, including failed uploads.
        directory = tempfile.mkdtemp(prefix="evaluation-", dir="/work")
        try:
            return execute(config, Path(directory), cancel, reporter=reporter, final_deadline=final_deadline)
        finally:
            reporter.close()
    except Exception as exc:
        # Do not print transfer URLs, tokens, request snapshots or untrusted exception bodies.
        print(f"Harbor runner failed: {type(exc).__name__}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

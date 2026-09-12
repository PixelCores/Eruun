"""Run a pinned Harbor evaluation and transfer its complete local output archive."""

from __future__ import annotations

import hashlib
import http.client
import importlib.metadata
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import signal
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
# Installed adapters execute in the trial sandbox, rather than importing user code.
AGENTS = frozenset({"claude-code", "codex", "terminus-2", "oracle"})


class RunnerError(Exception):
    pass


class RetryableTransferError(RunnerError):
    pass


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
    integer(config.get("timeoutSeconds"), "timeoutSeconds", 1, 86400)
    resources = config.get("resources")
    if not isinstance(resources, dict) or set(resources) != {"cpu", "memory", "cpuLimit", "memoryLimit"}:
        raise RunnerError("resources must contain requests and limits")
    if any(not isinstance(v, str) or not v for v in resources.values()):
        raise RunnerError("invalid resources")
    return config


def download_package(config, destination):
    deadline = time.monotonic() + TRANSFER_SECONDS
    connection, target = transfer_connection(config["datasetURL"], deadline)
    digest = hashlib.sha256()
    size = 0
    try:
        connection.request("GET", target, headers=transfer_headers(config))
        transfer_timeout(connection, deadline)
        response = connection.getresponse()
        if response.status != 200:
            raise RunnerError(f"platform rejected dataset download with HTTP {response.status}")
        with destination.open("wb") as output:
            while True:
                transfer_timeout(connection, deadline)
                chunk = response.read1(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_PACKAGE_BYTES:
                    raise RunnerError("task package exceeds 64 MiB")
                digest.update(chunk)
                output.write(chunk)
    finally:
        connection.close()
    if digest.hexdigest() != config["datasetDigest"]:
        raise RunnerError("task package digest does not match submitted content")


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


def check_sandbox_collection(output, expected_trials, report):
    """Account for data which Harbor failed to transfer into local outputs."""
    try:
        records = list((output.parent / "collection").glob("*.json"))
        if len(records) != expected_trials:
            collection_error(report, "trials", "missing_trial_collection_state")
        for path in records:
            try:
                if path.stat().st_size > 1024 * 1024:
                    raise ValueError("oversized collection state")
                state = json.loads(path.read_text())
                if state["started"] is not True or state["stopped"] is not True or state["pending"] != 0:
                    collection_error(report, state.get("podName", "trial"), "unfinished_sandbox_collection")
                if state["errorCount"]:
                    collection_error(report, state.get("podName", "trial"), "sandbox_download_failed", state["errorCount"])
                trial = Path(state["trialDirectory"])
                trial.resolve().relative_to(output.resolve())
                manifest_path = trial / "artifacts" / "manifest.json"
                if manifest_path.stat().st_size > 1024 * 1024:
                    raise ValueError("oversized artifact manifest")
                entries = json.loads(manifest_path.read_text())
                if not isinstance(entries, list) or not entries:
                    raise ValueError("invalid artifact manifest")
                for entry in entries:
                    if entry["status"] not in {"ok", "empty"}:
                        collection_error(report, entry["source"], "native_artifact_collection_failed")
            except (OSError, ValueError, KeyError, TypeError):
                collection_error(report, "trial", "unreadable_collection_state_or_manifest")
    except OSError:
        collection_error(report, "trials", "unreadable_collection_state")


def archive_results(output, archive_path, report):
    """Keep original bytes and safe links, surfacing every omitted entry."""
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
        issue(error.filename or ".", "unreadable_directory")

    for directory, directories, files in os.walk(output, onerror=walk_error, followlinks=False):
        for name in sorted(directories + files):
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
            except (ValueError, OSError, RuntimeError):
                issue(relative, "unsafe_or_unreadable_entry")

    with tarfile.open(archive_path, "w:gz", dereference=False) as archive:
        saved = set()
        # Links follow all ordinary entries, so they cannot refer to an omitted file.
        entries.sort(key=lambda p: p.is_symlink())
        for path in entries:
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
                        archive.addfile(info, source)
                else:
                    archive.addfile(info)
                saved.add(path)
            except OSError:
                issue(relative, "unreadable_file")
        report["collectionComplete"] = issue_count == 0
        if issue_count:
            report["collectionErrors"] = issues
            report["collectionErrorCount"] = issue_count
        report_bytes = json.dumps(report, ensure_ascii=False, allow_nan=False).encode()
        info = tarfile.TarInfo("result.json")
        info.size, info.mode = len(report_bytes), 0o600
        archive.addfile(info, io.BytesIO(report_bytes))
    if archive_path.stat().st_size > MAX_RESULT_BYTES:
        raise RunnerError("complete result archive exceeds 512 MiB")


def transfer_headers(config):
    pod_name, pod_uid = os.environ.get("POD_NAME"), os.environ.get("POD_UID")
    if not pod_name or not pod_uid or any(c in pod_name + pod_uid for c in "\r\n"):
        raise RunnerError("POD_NAME and POD_UID are required for source transfer")
    return {"Authorization": "Bearer " + config["token"],
            "X-Eruun-Runner-Pod-Name": pod_name, "X-Eruun-Runner-Pod-UID": pod_uid}


def remaining_time(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("platform transfer exceeded its total time budget")
    return remaining


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


def upload_results(config, archive_path, status, deadline=None):
    if deadline is None:
        deadline = time.monotonic() + TRANSFER_SECONDS
    connection, target = transfer_connection(config["resultURL"], deadline)
    try:
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
        response = connection.getresponse()
        if not 200 <= response.status < 300:
            if response.status >= 500 or response.status in {408, 429}:
                raise RetryableTransferError("platform temporarily rejected source upload")
            raise RunnerError(f"platform rejected source archive with HTTP {response.status}")
        response.read(4096)
    finally:
        connection.close()


def upload_with_retry(config, archive, status):
    # The first durable source is immutable. Retries must send this same file,
    # without rebuilding gzip (which could change metadata and its digest).
    deadline = time.monotonic() + TRANSFER_SECONDS
    for attempt in range(3):
        try:
            remaining_time(deadline)
            upload_results(config, archive, status, deadline=deadline)
            return
        except (OSError, http.client.HTTPException, RetryableTransferError):
            if attempt == 2:
                raise
            time.sleep(min(attempt + 1, remaining_time(deadline)))


def execute(config, work, cancel):
    output = work / "outputs"
    output.mkdir()
    (work / "collection").mkdir(mode=0o700)
    expected_trials = 0
    report = {"taskId": config["taskId"], "framework": {"name": "harbor", "version": FRAMEWORK_VERSION},
              "datasetDigest": config["datasetDigest"], "executionStatus": "failed", "frameworkExitCode": None}
    try:
        if importlib.metadata.version("harbor") != FRAMEWORK_VERSION:
            raise RunnerError("installed Harbor version does not match the pinned runner")
        source = work / "tasks.tar.gz"
        download_package(config, source)
        dataset = work / "dataset"
        extract_package(source, dataset)
        tasks = native_tasks(dataset)
        expected_trials = len(tasks) * config["options"]["attempts"]
        generated = harbor_config(config, tasks, output, os.environ.get("POD_NAME"), os.environ.get("POD_UID"))
        config_path = work / "harbor-config.json"
        config_path.write_text(json.dumps(generated))
        if cancel.is_set():
            raise RunnerError("cancelled before framework start")
        code, interrupted = run_framework(config_path, output, cancel, config["timeoutSeconds"])
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
    check_sandbox_collection(output, expected_trials, report)
    archive = work / "results.tar.gz"
    try:
        archive_results(output, archive, report)
    except (RunnerError, OSError, tarfile.TarError) as exc:
        # Preserve the local originals for Pod retention and make missing source
        # explicit; never label a diagnostic-only archive as full collection.
        report["collectionComplete"] = False
        report["collectionErrors"] = [{"path": "outputs", "reason": type(exc).__name__}]
        report["collectionErrorCount"] = 1
        report["diagnosticOnly"] = True
        contents = json.dumps(report, ensure_ascii=False).encode()
        with tarfile.open(archive, "w:gz") as target:
            info = tarfile.TarInfo("result.json")
            info.size, info.mode = len(contents), 0o600
            target.addfile(info, io.BytesIO(contents))
    status = report["executionStatus"] if report["collectionComplete"] else "failed"
    upload_with_retry(config, archive, status)
    return 0 if report["executionStatus"] == "succeeded" and report["collectionComplete"] else 1


def main():
    cancel = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: cancel.set())
    signal.signal(signal.SIGINT, lambda *_: cancel.set())
    try:
        config = validate_config(json.loads(os.environ["ERUUN_JOB_CONFIG"]))
        Path("/work/home").mkdir(parents=True, exist_ok=True)
        # Leave raw output and the exact final archive in the Pod until the
        # platform's retention policy removes it, including failed uploads.
        directory = tempfile.mkdtemp(prefix="evaluation-", dir="/work")
        return execute(config, Path(directory), cancel)
    except Exception as exc:
        # Do not print transfer URLs, tokens, request snapshots or untrusted exception bodies.
        print(f"Harbor runner failed: {type(exc).__name__}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

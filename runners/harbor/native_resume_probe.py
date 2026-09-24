"""Exercise native CLI session recovery against loopback model stubs only.

This verifies conversation history across SIGKILL and a new session directory.
It does not execute tools, quiesce an agent, test Harbor's lifecycle, or call ACS.
Run with the exact CLI versions below; no account or model API key is needed.
"""

import argparse
from contextlib import contextmanager, ExitStack
import http.server
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import threading
import time
import uuid

CLI_VERSIONS = {"codex": "codex-cli 0.154.0", "claude": "2.1.281 (Claude Code)"}
MARKER = "PERSISTED_NATIVE_RESUME_PROBE_MARKER"
FIRST = "FIRST_LOCAL_NATIVE_RESUME_PROBE"
INTERRUPTED = "INTERRUPTED_LOCAL_NATIVE_RESUME_PROBE"
RESUMED = "RESUMED_LOCAL_NATIVE_RESUME_PROBE"
MAX_REQUEST_BYTES = 2 * 1024 * 1024


class ProbeError(RuntimeError):
    pass


def _write(path, value):
    with open(path, "x", opener=lambda name, flags: os.open(name, flags, 0o600)) as output:
        output.write(value)


@contextmanager
def _process(command, *, directory, env, label):
    with open(directory / f"{label}.stdout", "x", opener=lambda name, flags: os.open(name, flags, 0o600)) as stdout, \
            open(directory / f"{label}.stderr", "x", opener=lambda name, flags: os.open(name, flags, 0o600)) as stderr:
        process = subprocess.Popen(command, cwd=directory / "work", env=env, stdin=subprocess.DEVNULL,
                                   stdout=stdout, stderr=stderr, start_new_session=True)
        try:
            yield process
        finally:
            # Also kill descendants when their CLI parent has already exited.
            # The process group belongs exclusively to this probe invocation.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=5)


def _run(command, *, directory, env, label, timeout=40):
    with _process(command, directory=directory, env=env, label=label) as process:
        try:
            code = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            raise ProbeError(f"{label} timed out; process group terminated") from None
    return code, (directory / f"{label}.stdout").read_text()


def _check_version(agent, output):
    if output.strip() != CLI_VERSIONS[agent]:
        raise ProbeError(f"requires {CLI_VERSIONS[agent]}; installed CLI version differs")


def _session_id(agent, output):
    key = "thread_id" if agent == "codex" else "session_id"
    for line in output.splitlines():
        if line.startswith("{"):
            event = json.loads(line)
            if event.get(key):
                return str(uuid.UUID(event[key]))
    raise ProbeError(f"{agent} did not emit a session identity")


def _wait_for_saved_prompt(state, session, process):
    # Receiving a model request does not prove its user message is on disk:
    # Claude Code can flush it after sending the request. Select an observable
    # file boundary explicitly; this is not an Agent pause/flush protocol.
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        for path in state.rglob(f"*{session}.jsonl"):
            with path.open("rb") as transcript:
                limit = 1024 * 1024
                offset = max(0, os.fstat(transcript.fileno()).st_size - limit)
                transcript.seek(max(0, offset - 1))
                clipped_first = offset > 0 and transcript.read(1) != b"\n"
                lines = transcript.read(limit).split(b"\n")[:-1]
                if clipped_first:
                    lines = lines[1:]
            for line in lines:
                try:
                    record = json.loads(line)
                except (ValueError, UnicodeDecodeError):
                    continue
                if isinstance(record, dict) and INTERRUPTED in json.dumps(record):
                    return
        if process.poll() is not None:
            raise ProbeError("interrupted CLI exited before its user prompt was saved")
        time.sleep(0.02)
    raise ProbeError("interrupted user prompt did not appear in the native session file")


class ModelStub(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_CONNECT(self):
        # The CLIs' outbound proxy points here. Never tunnel external traffic.
        self.send_error(403)

    def do_GET(self):
        self.send_error(404)

    def _event(self, name, data):
        self.wfile.write(f"event: {name}\ndata: {json.dumps(data)}\n\n".encode())
        self.wfile.flush()

    def do_POST(self):
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 < size <= MAX_REQUEST_BYTES:
                raise ValueError()
            body = json.loads(self.rfile.read(size))
            if not isinstance(body, dict):
                raise ValueError()
        except (ValueError, OSError):
            self.send_error(400)
            return
        if self.path not in {"/v1/responses", "/v1/messages?beta=true", "/v1/messages/count_tokens?beta=true"}:
            self.send_error(404)
            return
        with self.server.capture_lock:
            self.server.requests.append({"path": self.path, "body": body})
        if "count_tokens" in self.path:
            response = b'{"input_tokens":100}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Connection", "close")
        self.end_headers()
        encoded = json.dumps(body)
        interrupted = INTERRUPTED in encoded and RESUMED not in encoded
        try:
            if self.path == "/v1/responses":
                self._responses(interrupted)
            else:
                self._messages(body["model"], interrupted)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _wait_for_kill(self):
        self.server.interrupted.set()
        self.server.release.wait(45)

    def _responses(self, interrupted):
        response = {"id": "resp_" + uuid.uuid4().hex, "object": "response", "status": "in_progress", "output": []}
        self._event("response.created", {"type": "response.created", "response": response})
        item = {"id": "msg_" + uuid.uuid4().hex, "type": "message", "role": "assistant", "status": "in_progress", "content": []}
        self._event("response.output_item.added", {"type": "response.output_item.added", "output_index": 0, "item": item})
        if interrupted:
            self._wait_for_kill()
            return
        item.update(status="completed", content=[{"type": "output_text", "text": MARKER, "annotations": []}])
        self._event("response.output_text.delta", {"type": "response.output_text.delta", "item_id": item["id"],
                    "output_index": 0, "content_index": 0, "delta": MARKER})
        self._event("response.output_item.done", {"type": "response.output_item.done", "output_index": 0, "item": item})
        response.update(status="completed", output=[item], usage={"input_tokens": 10, "output_tokens": 4, "total_tokens": 14})
        self._event("response.completed", {"type": "response.completed", "response": response})

    def _messages(self, model, interrupted):
        message = {"id": "msg_" + uuid.uuid4().hex, "type": "message", "role": "assistant", "model": model,
                   "content": [], "stop_reason": None, "stop_sequence": None, "usage": {"input_tokens": 10, "output_tokens": 0}}
        self._event("message_start", {"type": "message_start", "message": message})
        self._event("content_block_start", {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}})
        if interrupted:
            self._wait_for_kill()
            return
        self._event("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": MARKER}})
        self._event("content_block_stop", {"type": "content_block_stop", "index": 0})
        self._event("message_delta", {"type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                    "usage": {"output_tokens": 4}})
        self._event("message_stop", {"type": "message_stop"})


@contextmanager
def _server():
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), ModelStub)
    server.daemon_threads = True
    server.requests = []
    server.capture_lock = threading.Lock()
    server.interrupted = threading.Event()
    server.release = threading.Event()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.release.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _environment(directory):
    # Construct from scratch: never inherit real credentials or agent settings.
    for name in ("work", "home", "state", "tmp"):
        (directory / name).mkdir(mode=0o700)
    return {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "HOME": str(directory / "home"),
            "TMPDIR": str(directory / "tmp"), "LANG": "en_US.UTF-8",
            "XDG_CONFIG_HOME": str(directory / "home" / ".config"),
            "XDG_CACHE_HOME": str(directory / "home" / ".cache")}


def _check_report(report):
    agent = report.get("agent")
    paths = report.get("modelRequestPaths")
    allowed = {"/v1/responses"} if agent == "codex" else {"/v1/messages?beta=true", "/v1/messages/count_tokens?beta=true"}
    if (agent not in CLI_VERSIONS or report.get("version") != CLI_VERSIONS[agent]
            or not isinstance(paths, list) or len([path for path in paths if "count_tokens" not in path]) < 3
            or any(path not in allowed for path in paths)
            or report.get("firstExit") != 0 or report.get("interruptedExit") != -signal.SIGKILL or report.get("resumeExit") != 0
            or not report.get("sessionId") or report.get("resumedSessionId") != report["sessionId"]
            or any(report.get(key) is not True for key in ("interruptedPromptObservedOnDiskBeforeKill", "sourceStateDeleted",
                                                          "completedMarkerPreserved", "interruptedPromptPreserved", "resumePromptSent"))):
        raise ProbeError(f"{agent} native session recovery evidence is incomplete")


def _probe_agent(agent, binary, directory, env):
    state = directory / "state"
    with _server() as server:
        base = f"http://127.0.0.1:{server.server_port}"
        env.update(HTTP_PROXY=base, HTTPS_PROXY=base, ALL_PROXY=base, NO_PROXY="127.0.0.1,localhost")
        if agent == "codex":
            env.update(CODEX_HOME=str(state), LOCAL_STUB_KEY="local-fixture-not-a-real-key")
            config = f'''model = "offline-probe"
model_provider = "offline"
check_for_update_on_startup = false
[analytics]
enabled = false
[feedback]
enabled = false
[model_providers.offline]
name = "Offline loopback fixture"
base_url = "{base}/v1"
wire_api = "responses"
requires_openai_auth = false
env_key = "LOCAL_STUB_KEY"
request_max_retries = 0
stream_max_retries = 0
'''
            _write(state / "config.toml", config)
            command = [binary, "exec", "--skip-git-repo-check", "--json", "--ignore-rules"]
            def arguments(prompt, session=None):
                return command + (["resume", session] if session else []) + [prompt]
        else:
            env.update(CLAUDE_CONFIG_DIR=str(state), ANTHROPIC_BASE_URL=base,
                       ANTHROPIC_API_KEY="local-fixture-not-a-real-key", CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC="1",
                       DISABLE_AUTOUPDATER="1", DISABLE_TELEMETRY="1", DISABLE_ERROR_REPORTING="1")
            command = [binary, "--bare", "--print", "--verbose", "--output-format", "stream-json", "--model", "claude-sonnet-4-5",
                       "--tools", "", "--strict-mcp-config", "--mcp-config", '{"mcpServers":{}}', "--setting-sources", ""]
            def arguments(prompt, session=None):
                return command + (["--resume", session] if session else []) + [prompt]
        first_code, first_output = _run(arguments(FIRST), directory=directory, env=env, label="first")
        if first_code != 0:
            raise ProbeError(f"{agent} initial CLI exited {first_code}")
        session = _session_id(agent, first_output)
        with _process(arguments(INTERRUPTED, session), directory=directory, env=env, label="interrupted") as process:
            if not server.interrupted.wait(30):
                raise ProbeError(f"{agent} did not reach the interrupted model request")
            _wait_for_saved_prompt(state, session, process)
            os.killpg(process.pid, signal.SIGKILL)
            interrupted_code = process.wait(timeout=5)
        server.release.set()
        restored = directory / "restored-state"
        restored.mkdir(mode=0o700)
        if agent == "codex":
            shutil.copytree(state / "sessions", restored / "sessions")
            _write(restored / "config.toml", config)
            env["CODEX_HOME"] = str(restored)
        else:
            shutil.copytree(state / "projects", restored / "projects")
            env["CLAUDE_CONFIG_DIR"] = str(restored)
        shutil.rmtree(state)
        with server.capture_lock:
            previous = len(server.requests)
        resume_code, resume_output = _run(arguments(RESUMED, session), directory=directory, env=env, label="resumed")
        with server.capture_lock:
            requests = list(server.requests)
        _write(directory / "requests.json", json.dumps(requests, indent=2))
        request_text = json.dumps(requests[previous:])
        report = {"agent": agent, "version": CLI_VERSIONS[agent], "sessionId": session,
                  "resumedSessionId": _session_id(agent, resume_output),
                  "firstExit": first_code, "interruptedExit": interrupted_code, "resumeExit": resume_code,
                  "interruptedPromptObservedOnDiskBeforeKill": True,
                  "sourceStateDeleted": not state.exists(), "completedMarkerPreserved": MARKER in request_text,
                  "interruptedPromptPreserved": INTERRUPTED in request_text, "resumePromptSent": RESUMED in request_text,
                  "modelRequestPaths": [request["path"] for request in requests]}
        _check_report(report)
        return report


def probe(root, binaries):
    agents = {}
    for agent, binary in binaries.items():
        directory = root / agent
        directory.mkdir(mode=0o700)
        env = _environment(directory)
        if agent == "codex":
            env["CODEX_HOME"] = str(directory / "state")
        else:
            env["CLAUDE_CONFIG_DIR"] = str(directory / "state")
        code, output = _run([binary, "--version"], directory=directory, env=env, label="version", timeout=10)
        if code != 0:
            raise ProbeError(f"{agent} version check failed")
        _check_version(agent, output)
        agents[agent] = (directory, env)
    return {
        "evidenceLevel": "native-cli-conversation-resume-with-loopback-model-stub",
        "passed": True, "realModelCalls": False, "toolCallsExercised": False, "cloudValidated": False,
        "barrierValidated": False, "harborLifecycleValidated": False,
        "checkpointBoundary": "native session file visibly contains the interrupted user prompt before SIGKILL; no arbitrary-kill zero-loss guarantee",
        "agents": [_probe_agent(agent, binaries[agent], directory, env) for agent, (directory, env) in agents.items()],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codex", default=shutil.which("codex"), help="Path to Codex CLI 0.154.0")
    parser.add_argument("--claude", default=shutil.which("claude"), help="Path to Claude Code 2.1.281")
    parser.add_argument("--output-dir", type=Path, help="New directory to retain private evidence; must not already exist")
    args = parser.parse_args(argv)
    if not args.codex or not args.claude:
        print(json.dumps({"passed": False, "error": "both pinned CLIs must be installed or supplied explicitly"}))
        return 1
    with ExitStack() as stack:
        if args.output_dir:
            root = args.output_dir.resolve()
            try:
                root.mkdir(mode=0o700, parents=True)
            except OSError:
                print(json.dumps({"passed": False, "error": "cannot create new output directory"}))
                return 1
        else:
            root = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="eruun-native-resume-")))
        try:
            report = probe(root, {"codex": str(Path(args.codex).resolve()), "claude": str(Path(args.claude).resolve())})
        except ProbeError as exc:
            report = {"passed": False, "error": str(exc)}
        except Exception as exc:
            # Raw CLI errors and request bodies stay in the private artifact directory.
            report = {"passed": False, "error": type(exc).__name__}
        _write(root / "summary.json", json.dumps(report, indent=2))
        for path in root.rglob("*"):
            if not path.is_symlink():
                path.chmod(0o700 if path.is_dir() else 0o600)
        print(json.dumps(report, indent=2))
        return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

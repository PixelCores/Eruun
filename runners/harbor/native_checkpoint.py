"""Sandbox-side native CLI supervisor for the opt-in recovery runtime.

All native processes are stopped before returning a recoverable boundary. This
requires Linux /proc and python3 in the prepared image; it never trusts an exec
stream closing or only the CLI parent exiting as proof of quiescence.
"""

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import uuid

VERSIONS = {"codex": "codex-cli 0.154.0", "claude-code": "2.1.281 (Claude Code)"}
STATE = Path("/logs/agent/eruun-native")


class BoundaryError(RuntimeError):
    pass


def processes(proc=Path("/proc")):
    result = {}
    for path in proc.iterdir():
        if not path.name.isdecimal():
            continue
        try:
            # comm may contain spaces and ')' characters.
            fields = (path / "stat").read_text().rsplit(")", 1)[1].split()
            if fields[0] != "Z":
                result[int(path.name)] = (int(fields[1]), fields[19])
        except FileNotFoundError:
            continue
    return result


def baseline_processes():
    current = processes()
    if Path("/proc/1/comm").read_text().strip() != "sleep":
        raise BoundaryError("sandbox init is not the managed idle process")
    ancestors, pid = set(), os.getpid()
    while pid in current and pid not in ancestors:
        ancestors.add(pid)
        pid = current[pid][0]
    ancestors.add(1)
    if set(current) - ancestors:
        raise BoundaryError("sandbox has unmanaged processes before agent start")
    return current


def assert_quiescent(baseline):
    # Checking all visible tasks also detects double-fork/setsid descendants.
    # Identity includes starttime so a reused PID is never a baseline process.
    current = processes()
    if any(pid not in baseline or baseline[pid] != identity for pid, identity in current.items()):
        raise BoundaryError("sandbox has remaining background writers")


def stop_process_group(process):
    try:
        os.killpg(process.pid, signal.SIGINT)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass
    # Kill descendants even when the CLI parent already returned.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=5)


def command(agent, model, prompt, session):
    if agent == "codex":
        args = ["codex", "exec", "--skip-git-repo-check", "--json", "--dangerously-bypass-approvals-and-sandbox", "-m", model]
        return args + (["resume", session] if session else []) + [prompt]
    args = ["claude", "--bare", "--print", "--verbose", "--output-format", "stream-json",
            "--permission-mode", "bypassPermissions", "--model", model,
            "--strict-mcp-config", "--mcp-config", '{"mcpServers":{}}', "--setting-sources", ""]
    return args + (["--resume", session] if session else []) + [prompt]


def saved_session(agent, output, expected=None):
    key = "thread_id" if agent == "codex" else "session_id"
    identities = set()
    with output.open() as source:
        for line in source:
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if isinstance(event, dict) and event.get(key):
                identities.add(str(uuid.UUID(event[key])))
    if len(identities) != 1 or (expected and identities != {expected}):
        raise BoundaryError("native CLI did not confirm exactly one expected session")
    session = identities.pop()
    root = STATE / ("sessions" if agent == "codex" else "projects")
    records = list(root.rglob(f"*{session}.jsonl"))
    if len(records) != 1:
        raise BoundaryError("native session transcript is missing or ambiguous")
    for path in root.rglob("*"):
        if path.is_symlink() or (not path.is_file() and not path.is_dir()):
            raise BoundaryError("native session contains unsupported filesystem entries")
        if path.is_file():
            with path.open("rb") as source:
                if path.suffix == ".jsonl":
                    for line in source:
                        if not line.endswith(b"\n"):
                            raise BoundaryError("native session contains an unflushed record")
                        json.loads(line)
                os.fsync(source.fileno())
    return session


def run(request):
    agent = request["agent"]
    if agent not in VERSIONS:
        raise BoundaryError("unsupported native agent")
    binary = "codex" if agent == "codex" else "claude"
    actual = subprocess.run([binary, "--version"], check=True, capture_output=True, text=True, timeout=10)
    if actual.stdout.strip() != VERSIONS[agent]:
        raise BoundaryError("native agent version does not match recovery material")
    STATE.mkdir(parents=True, exist_ok=True, mode=0o700)
    env = dict(os.environ)
    env.pop("ERUUN_NATIVE_REQUEST", None)
    env.update(DISABLE_AUTOUPDATER="1", DISABLE_TELEMETRY="1", DISABLE_ERROR_REPORTING="1",
               CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC="1", IS_SANDBOX="1")
    if agent == "codex":
        env["CODEX_HOME"] = str(STATE)
        base = request.get("baseURL") or "https://api.openai.com/v1"
        # env_key authentication avoids persistent auth.json and never copies
        # credentials into checkpoint material or a sandbox filesystem.
        (STATE / "config.toml").write_text(
            'model_provider = "eruun"\ncheck_for_update_on_startup = false\n'
            '[analytics]\nenabled = false\n[feedback]\nenabled = false\n'
            '[model_providers.eruun]\nname = "Eruun"\nwire_api = "responses"\n'
            'env_key = "OPENAI_API_KEY"\nrequires_openai_auth = false\nbase_url = ' + json.dumps(base) + '\n')
    else:
        env["CLAUDE_CONFIG_DIR"] = str(STATE)
    baseline = baseline_processes()
    output = STATE.parent / ("eruun-native-" + uuid.uuid4().hex + ".jsonl")
    interval = float(request["seconds"])
    if not 0 < interval <= 3600:
        raise BoundaryError("invalid agent slice budget")
    paused = False
    with output.open("wb") as log:
        process = subprocess.Popen(command(agent, request["model"], request["prompt"], request.get("sessionId")),
                                   env=env, stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
        try:
            try:
                process.wait(timeout=interval)
            except subprocess.TimeoutExpired:
                paused = True
        finally:
            stop_process_group(process)
        log.flush()
        os.fsync(log.fileno())
    assert_quiescent(baseline)
    if not paused and process.returncode != 0:
        raise BoundaryError("native CLI failed")
    session = saved_session(agent, output, request.get("sessionId"))
    # Settings are regenerated with the new Runner's environment before every
    # launch. Only native conversation/project data is retained for recovery.
    (STATE / "config.toml").unlink(missing_ok=True)
    if (STATE / "auth.json").exists() or (STATE / ".credentials.json").exists():
        raise BoundaryError("native CLI persisted credentials")
    os.sync()
    assert_quiescent(baseline)
    return {"sessionId": session, "paused": paused, "quiescent": True, "agentVersion": VERSIONS[agent]}


def main():
    try:
        result = run(json.loads(os.environ["ERUUN_NATIVE_REQUEST"]))
    except Exception as exc:
        # Raw CLI output remains in the sandbox. Never echo credentials.
        print(json.dumps({"error": str(exc) if isinstance(exc, BoundaryError) else type(exc).__name__}))
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())

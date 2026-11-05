#!/usr/bin/env python3
"""S0 probe for one-shot Agent tasks; see docs/agent-task-design.md section 9.1.

Run inside the Sandbox container as UID 1000:

    python3 /tmp/eruun-agent/probe.py <codex|claude-code> <model>

The report contains booleans, counts and file paths only. It never includes
credential values or model output text.
"""

import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path("/tmp/eruun-agent")
BIN = ROOT / "bin"
HOME = ROOT / "home"
VERSIONS = {"codex": "codex-cli 0.154.0", "claude-code": "2.1.281 (Claude Code)"}
BINARIES = {"codex": "codex", "claude-code": "claude"}
KEYS = {"codex": "OPENAI_API_KEY", "claude-code": "ANTHROPIC_API_KEY"}
SESSION_KEYS = {"codex": "thread_id", "claude-code": "session_id"}
WRITE_PROMPT = ("Create a file named hello.txt in the current directory containing exactly the word hi "
                "followed by a newline. Do not change anything else.")
STOP_PROMPT = "Run the shell command `sleep 600` in the current directory and wait for it to finish before replying."
RUN_TIMEOUT = 600
STOP_WAIT = 180
ALLOWED_WRITE_PREFIXES = ("/tmp/", "/workspace/", "/proc/", "/sys/", "/dev/")


def processes():
    """Return pid -> (comm, ppid, starttime) for every live task, as native_checkpoint.py does."""
    result = {}
    for path in Path("/proc").iterdir():
        if not path.name.isdecimal():
            continue
        try:
            raw = (path / "stat").read_text()
        except (FileNotFoundError, ProcessLookupError):
            continue
        comm = raw[raw.index("(") + 1:raw.rindex(")")]
        fields = raw.rsplit(")", 1)[1].split()
        if fields[0] != "Z":
            result[int(path.name)] = (comm, int(fields[1]), fields[19])
    return result


def new_processes(baseline):
    return {pid: value for pid, value in processes().items()
            if pid not in baseline or baseline[pid][2] != value[2]}


def git(repo, *args):
    return subprocess.run(["git", "-c", "user.name=s0", "-c", "user.email=s0@example.invalid",
                           "-c", "commit.gpgsign=false", *args],
                          cwd=repo, check=True, capture_output=True, timeout=60)


def check_environment():
    missing = [tool for tool in ("sh", "git", "tar", "python3") if shutil.which(tool) is None]
    writable = {}
    for directory in ("/workspace", "/tmp"):
        probe = Path(directory) / ".s0-write-check"
        try:
            probe.write_text("ok")
            probe.unlink()
            writable[directory] = True
        except OSError:
            writable[directory] = False
    pid1 = Path("/proc/1/comm").read_text().strip()
    report = {"arch": os.uname().machine, "uid": os.getuid(), "missingTools": missing,
              "writable": writable, "pid1": pid1}
    report["ok"] = not missing and os.getuid() == 1000 and all(writable.values()) and pid1 == "sleep"
    return report


def agent_env(harness, native):
    env = dict(os.environ)
    env["PATH"] = str(BIN) + ":" + env.get("PATH", "/usr/bin:/bin")
    env["HOME"] = str(HOME)
    env.update(DISABLE_AUTOUPDATER="1", DISABLE_TELEMETRY="1", DISABLE_ERROR_REPORTING="1",
               CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC="1", IS_SANDBOX="1")
    native.mkdir(parents=True, exist_ok=True, mode=0o700)
    if harness == "codex":
        env["CODEX_HOME"] = str(native)
        base = os.environ.get("S0_OPENAI_BASE_URL") or "https://api.openai.com/v1"
        (native / "config.toml").write_text(
            'model_provider = "eruun"\ncheck_for_update_on_startup = false\n'
            '[analytics]\nenabled = false\n[feedback]\nenabled = false\n'
            '[model_providers.eruun]\nname = "Eruun"\nwire_api = "responses"\n'
            'env_key = "OPENAI_API_KEY"\nrequires_openai_auth = false\nbase_url = ' + json.dumps(base) + '\n')
    else:
        env["CLAUDE_CONFIG_DIR"] = str(native)
    return env


def command(harness, model, prompt):
    # Same flags as native_checkpoint.py command(), without resume.
    if harness == "codex":
        return ["codex", "exec", "--skip-git-repo-check", "--json",
                "--dangerously-bypass-approvals-and-sandbox", "-m", model, prompt]
    return ["claude", "--bare", "--print", "--verbose", "--output-format", "stream-json",
            "--permission-mode", "bypassPermissions", "--model", model,
            "--strict-mcp-config", "--mcp-config", '{"mcpServers":{}}', "--setting-sources", "", prompt]


def check_version(harness, env):
    try:
        actual = subprocess.run([BINARIES[harness], "--version"], env=env, capture_output=True,
                                text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"ok": False, "error": type(exc).__name__}
    output = actual.stdout.strip()
    return {"ok": actual.returncode == 0 and output == VERSIONS[harness], "exitCode": actual.returncode,
            "output": output[:200], "expected": VERSIONS[harness]}


def stop_group(process):
    for sig in (signal.SIGINT, signal.SIGKILL):
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass


def start(harness, model, prompt, repo, logs, env):
    logs.mkdir(parents=True, exist_ok=True)
    stdout = (logs / "trajectory.jsonl").open("wb")
    stderr = (logs / "stderr.log").open("wb")
    process = subprocess.Popen(command(harness, model, prompt), cwd=repo, env=env, stdin=subprocess.DEVNULL,
                               stdout=stdout, stderr=stderr, start_new_session=True)
    return process, (stdout, stderr)


def close(files):
    for handle in files:
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()


def trajectory(harness, path, native):
    events, invalid, sessions = 0, 0, set()
    for line in path.read_bytes().splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            invalid += 1
            continue
        events += 1
        if isinstance(event, dict) and event.get(SESSION_KEYS[harness]):
            sessions.add(str(event[SESSION_KEYS[harness]]))
    session_files = len(list(native.rglob("*" + next(iter(sessions)) + ".jsonl"))) if len(sessions) == 1 else 0
    return {"events": events, "invalidLines": invalid, "sessionIds": len(sessions), "sessionFiles": session_files}


def writes_outside(marker):
    """Regular files modified after marker outside /tmp and /workspace on the root filesystem."""
    since, device, found = marker.stat().st_mtime, os.stat("/").st_dev, []
    for directory, subdirs, files in os.walk("/"):
        subdirs[:] = [name for name in subdirs
                      if not (os.path.join(directory, name) + "/").startswith(ALLOWED_WRITE_PREFIXES)]
        for name in files:
            path = os.path.join(directory, name)
            try:
                status = os.lstat(path)
            except OSError:
                continue
            if status.st_dev == device and status.st_mtime > since:
                found.append(path)
                if len(found) >= 50:
                    return found
    return found


def run_write(harness, model, env, work):
    repo = Path("/workspace") / ("s0-" + harness)
    shutil.rmtree(repo, ignore_errors=True)
    repo.mkdir(parents=True)
    git(repo, "init", "-q")
    (repo / "README.md").write_text("S0 probe repository\n")
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", "base")
    base = git(repo, "rev-parse", "HEAD").stdout.decode().strip()

    marker = work / "marker"
    marker.write_text("")
    time.sleep(1)
    baseline = processes()
    started = time.monotonic()
    process, files = start(harness, model, WRITE_PROMPT, repo, work, env)
    timed_out = False
    try:
        process.wait(timeout=RUN_TIMEOUT)
    except subprocess.TimeoutExpired:
        timed_out = True
    finally:
        stop_group(process)
        close(files)
    leftovers = new_processes(baseline)
    report = {"exitCode": process.returncode, "timedOut": timed_out,
              "durationSeconds": round(time.monotonic() - started, 1),
              "leftoverProcesses": sorted(value[0] for value in leftovers.values())}
    report["trajectory"] = trajectory(harness, work / "trajectory.jsonl", work / "native")
    report["writesOutsideAllowed"] = writes_outside(marker)

    git(repo, "add", "-A")
    patch = git(repo, "diff", "--cached", "--binary", base).stdout
    (work / "diff.patch").write_bytes(patch)
    changed = [line.split("\t")[-1] for line in
               git(repo, "diff", "--cached", "--numstat", base).stdout.decode().splitlines()]
    hello = repo / "hello.txt"
    report["diff"] = {"bytes": len(patch), "files": changed,
                      "helloMatches": hello.is_file() and hello.read_text() == "hi\n"}
    report["ok"] = (process.returncode == 0 and not timed_out and not leftovers
                    and report["trajectory"]["sessionIds"] == 1 and report["trajectory"]["sessionFiles"] == 1
                    and not report["writesOutsideAllowed"] and "hello.txt" in changed)
    return report, repo


def run_stop(harness, model, env, work, repo):
    baseline = processes()
    process, files = start(harness, model, STOP_PROMPT, repo, work, env)
    observed_sleep = False
    deadline = time.monotonic() + STOP_WAIT
    try:
        while time.monotonic() < deadline and process.poll() is None:
            if any(value[0] == "sleep" for value in new_processes(baseline).values()):
                observed_sleep = True
                break
            time.sleep(1)
        exited_early = process.poll() is not None
        stop_group(process)
    finally:
        close(files)
    time.sleep(2)
    leftovers = new_processes(baseline)
    # Leave the container clean for the next run whatever the verdict is.
    for pid in leftovers:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    report = {"observedToolProcess": observed_sleep, "exitedBeforeStop": exited_early,
              "leftoverProcesses": sorted(value[0] for value in leftovers.values())}
    report["ok"] = observed_sleep and not leftovers
    return report


def check_credentials(harness, key, work):
    secret = key.encode()
    leaked = []
    for root in (work, Path("/workspace") / ("s0-" + harness), HOME):
        for path in root.rglob("*"):
            try:
                if path.is_file() and not path.is_symlink() and secret in path.read_bytes():
                    leaked.append(str(path))
            except OSError:
                continue
    auth_files = [str(path) for path in (work / "native" / "auth.json", work / "native" / ".credentials.json",
                                         HOME / ".codex" / "auth.json", HOME / ".claude" / ".credentials.json")
                  if path.exists()]
    return {"filesContainingKey": leaked, "authFiles": auth_files, "ok": not leaked and not auth_files}


def main():
    if len(sys.argv) != 3 or sys.argv[1] not in VERSIONS:
        print("usage: probe.py <codex|claude-code> <model>", file=sys.stderr)
        return 2
    harness, model = sys.argv[1], sys.argv[2]
    key = os.environ.get(KEYS[harness], "")
    if not key:
        print(KEYS[harness] + " is not set", file=sys.stderr)
        return 2
    work = ROOT / "logs" / harness
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    HOME.mkdir(parents=True, exist_ok=True)
    env = agent_env(harness, work / "native")

    report = {"harness": harness, "environment": check_environment(), "version": check_version(harness, env)}
    if report["version"]["ok"]:
        report["write"], repo = run_write(harness, model, env, work)
        if os.environ.get("S0_SKIP_STOP") != "1":
            report["stop"] = run_stop(harness, model, env, work / "stop", repo)
    report["credentials"] = check_credentials(harness, key, work)
    report["ok"] = all(section.get("ok") for name, section in report.items() if isinstance(section, dict))
    output = json.dumps(report, indent=2, ensure_ascii=False)
    (ROOT / ("s0-report-" + harness + ".json")).write_text(output + "\n")
    print(output)
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())

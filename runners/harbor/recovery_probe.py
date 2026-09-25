"""Offline Harbor recovery capability gate; never starts a sandbox or an agent.

Install requirements.txt, then run ``python runners/harbor/recovery_probe.py``.
Exit 2 means the experiment succeeded but the in-trial recovery gate failed;
exit 1 means the experiment could not establish its evidence. This is a fixture
experiment against Harbor's real job reconciliation, not a cloud recovery test.
"""

import asyncio
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile

PINNED_VERSIONS = {"harbor": "0.22.0", "kubernetes": "32.0.1"}


class ProbeError(RuntimeError):
    pass


def require_versions():
    for name, expected in PINNED_VERSIONS.items():
        try:
            actual = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            raise ProbeError(f"install requirements.txt: missing {name}=={expected}") from None
        if actual != expected:
            raise ProbeError(f"requires {name}=={expected}; found {actual}")


def _relocate_fixture(value, source, destination):
    """Rebase only generated fixture paths, not arbitrary recovery material."""
    if isinstance(value, dict):
        return {key: _relocate_fixture(item, source, destination) for key, item in value.items()}
    if isinstance(value, list):
        return [_relocate_fixture(item, source, destination) for item in value]
    if isinstance(value, str) and value.startswith(str(source) + "/"):
        return str(destination) + value[len(str(source)):]
    return value


async def _experiment(root, interrupted_result):
    from harbor.job import Job
    from harbor.models.job.config import JobConfig
    from harbor.models.trial.result import AgentInfo, TrialResult
    from harbor.models.verifier.result import VerifierResult

    source, restored = root / "source", root / "restored"
    tasks = []
    for name in ("completed", "interrupted"):
        task = source / "dataset" / name
        (task / "environment").mkdir(parents=True)
        (task / "tests").mkdir()
        (task / "instruction.md").write_text("Offline reconciliation fixture; do not execute.\n")
        (task / "task.toml").write_text('version = "1.0"\n[environment]\ndocker_image = "alpine:3.22"\n')
        (task / "environment" / "Dockerfile").write_text("FROM alpine:3.22\n")
        (task / "tests" / "test.sh").write_text("exit 1\n")
        tasks.append({"path": str(task), "source": "recovery-probe"})
    config = JobConfig.model_validate({
        "job_name": "probe", "jobs_dir": str(source / "outputs"),
        "tasks": tasks, "agents": [{"name": "oracle"}],
        "n_concurrent_trials": 2, "retry": {"max_retries": 0}, "quiet": True,
    })
    original = await Job.create(config)
    try:
        trial_configs = {trial.task.path.name: trial for trial in original._trial_configs}
        original._job_config_path.write_text(config.model_dump_json())
        for name, trial in trial_configs.items():
            directory = original.job_dir / trial.trial_name
            directory.mkdir()
            (directory / "config.json").write_text(trial.model_dump_json())
            (directory / "progress.json").write_text('{"completedSteps":7}')
            if name == "completed":
                result = TrialResult(
                    task_name=name, trial_name=trial.trial_name,
                    trial_uri="fixture://completed", task_id=trial.task.get_task_id(),
                    task_checksum="fixture", config=trial, source="recovery-probe",
                    agent_info=AgentInfo(name="oracle", version="fixture"),
                    verifier_result=VerifierResult(rewards={"reward": 1.0}),
                    started_at=datetime.now(timezone.utc), finished_at=datetime.now(timezone.utc),
                )
                (directory / "result.json").write_text(result.model_dump_json())
            elif interrupted_result != "missing":
                (directory / "result.json").write_text("" if interrupted_result == "empty" else '{"id":')
    finally:
        original._close_logger_handlers()

    # Simulate a different Runner filesystem. Harbor stores absolute paths, so
    # the fixture explicitly rebases all generated JSON before removing source.
    # This rebasing is experimental setup, not an implemented Eruun protocol.
    shutil.copytree(source, restored)
    for path in restored.rglob("*.json"):
        if path.name == "result.json" and path.parent.name == trial_configs["interrupted"].trial_name:
            continue
        value = _relocate_fixture(json.loads(path.read_text()), source, restored)
        path.write_text(json.dumps(value))
    shutil.rmtree(source)
    restored_job_dir = restored / "outputs" / "probe"
    completed_name = trial_configs["completed"].trial_name
    interrupted_name = trial_configs["interrupted"].trial_name
    result_path = restored_job_dir / completed_name / "result.json"
    result_digest = hashlib.sha256(result_path.read_bytes()).hexdigest()
    restored_config = JobConfig.model_validate_json((restored_job_dir / "config.json").read_text())
    resumed = await Job.create(restored_config)
    try:
        remaining = resumed._remaining_trial_configs
        completed_reused = len(resumed._existing_trial_results) == 1 and resumed._existing_trial_results[0].trial_name == completed_name
        rerun_planned = len(remaining) == 1 and remaining[0].task.path.name == "interrupted" and remaining[0].trial_name != interrupted_name
        result_preserved = hashlib.sha256(result_path.read_bytes()).hexdigest() == result_digest
        if source.exists() or not completed_reused or not rerun_planned or not result_preserved:
            raise ProbeError("pinned Harbor reconciliation behavior differs from the recovery gate baseline")
        progress_retained = (restored_job_dir / interrupted_name / "progress.json").exists()
        if interrupted_result == "missing" and progress_retained:
            raise ProbeError("pinned Harbor no longer removes the incomplete trial directory")
        return {
            "interruptedResult": interrupted_result,
            "sourceDirectoryRemoved": True,
            "completedTrialSkipped": completed_reused,
            "completedResultUnchanged": result_preserved,
            "incompleteTrialScheduledWithNewIdentity": rerun_planned,
            "oldProgressFileRetained": progress_retained,
            "oldProgressLoaded": False,
        }
    finally:
        resumed._close_logger_handlers()


def probe():
    require_versions()
    # LiteLLM imports otherwise fetch a price table. This experiment needs no
    # network, model, account, Kubernetes configuration, or credentials.
    os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"
    from harbor.agents.installed.claude_code import ClaudeCode
    from harbor.agents.installed.codex import Codex
    from harbor.agents.oracle import OracleAgent
    from harbor.agents.terminus_2.terminus_2 import Terminus2

    with tempfile.TemporaryDirectory(prefix="eruun-harbor-recovery-probe-") as directory:
        experiments = [asyncio.run(_experiment(Path(directory) / state, state))
                       for state in ("missing", "empty", "truncated")]
    return {
        "versions": PINNED_VERSIONS,
        "evidenceLevel": "offline-harbor-job-reconciliation-fixture",
        "recoveryGranularity": "completed-trials-only; incomplete-trials-restart",
        "experiments": experiments,
        "agentNativeResumeDeclared": {agent.name(): agent.SUPPORTS_RESUME
                                      for agent in (Codex, ClaudeCode, Terminus2, OracleAgent)},
        "gatePassed": False,
        "blockers": [
            "job reconciliation does not restore incomplete trial progress",
            "native Agent resume declarations do not establish crash recovery or a write barrier",
            "Runner persistence, clone attachment, fencing and ACS recovery were not exercised",
        ],
    }


def main():
    try:
        report = probe()
    except ProbeError as exc:
        print(json.dumps({"gatePassed": False, "experimentError": str(exc)}))
        return 1
    except Exception as exc:
        print(json.dumps({"gatePassed": False, "experimentError": type(exc).__name__}))
        return 1
    print(json.dumps(report, indent=2))
    return 0 if report["gatePassed"] else 2


if __name__ == "__main__":
    sys.exit(main())

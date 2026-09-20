"""Read bounded, selected Kubernetes identity snapshots; never infer trial execution."""

import argparse
import json
import math
import os
import subprocess
import time
from pathlib import Path

from observe import ID, load_submissions


def quoted(expression, scope):
    return ('{{with ' + scope + '}}{{with ' + expression + '}}{{printf "%q" .}}'
            '{{else}}""{{end}}{{else}}""{{end}}')


# Projection occurs inside kubectl: Pod env (including ERUUN_JOB_CONFIG), full
# annotations, specs, condition messages and kubeconfig never reach our output.
TEMPLATE = '{{range .items}}{' + ','.join(
    '"' + key + '":' + quoted(expression, scope) for key, expression, scope in (
        ("name", ".name", ".metadata"), ("namespace", ".namespace", ".metadata"),
        ("uid", ".uid", ".metadata"),
        ("taskId", 'index . "eruun.io/task-id"', ".metadata.labels"),
        ("annotatedTaskId", 'index . "eruun.job/taskId"', ".metadata.annotations"),
        ("sandboxRunnerUID", 'index . "eruun.io/sandbox-runner-uid"', ".metadata.annotations"),
        ("runner", 'index . "eruun.io/evaluation-runner"', ".metadata.labels"),
        ("phase", ".phase", ".status"))) + ',"owners":[' + (
    '{{range $index, $owner := .metadata.ownerReferences}}{{if $index}},{{end}}'
    '{"kind":{{printf "%q" .kind}},"name":{{printf "%q" .name}},'
    '"uid":{{printf "%q" .uid}},"controller":{{if .controller}}true{{else}}false{{end}}}'
    '{{end}}],"conditions":['
    '{{with .status}}{{range $index, $condition := .conditions}}{{if $index}},{{end}}'
    '{"type":{{printf "%q" .type}},"status":{{printf "%q" .status}}}{{end}}{{end}}]}'
    '{{"\\n"}}{{end}}')


def clean_objects(objects, namespace, task_ids):
    """Validate selected identities and discard unrelated tasks and private fields."""
    selected = {}
    for value in objects:
        if not isinstance(value, dict):
            raise ValueError("invalid object")
        kind, name, uid = value.get("kind"), value.get("name"), value.get("uid")
        if (kind not in {"Job", "Pod", "Sandbox"} or not isinstance(name, str)
                or not ID.fullmatch(name) or not isinstance(uid, str) or not ID.fullmatch(uid)):
            raise ValueError("invalid object identity")
        if value.get("namespace") != namespace:
            continue
        task_id = value.get("taskId") or value.get("annotatedTaskId")
        if task_id not in task_ids:
            continue
        if value.get("taskId") and value.get("annotatedTaskId") not in (None, "", task_id):
            raise ValueError("conflicting task identity")
        role = "RunnerPod" if kind == "Pod" and value.get("runner") == "true" else (
            "TrialPod" if kind == "Pod" else kind)
        owners = []
        for owner in value.get("owners", []):
            if (not isinstance(owner, dict) or owner.get("kind") not in {"Pod", "Job", "Sandbox"}
                    or not all(isinstance(owner.get(key), str) and ID.fullmatch(owner[key]) for key in ("name", "uid"))):
                continue
            owners.append({key: owner[key] for key in ("kind", "name", "uid")} | {
                "controller": owner.get("controller") is True})
        state = {"kind": kind, "role": role, "name": name, "namespace": namespace,
                 "uid": uid, "taskId": task_id, "owners": owners}
        runner_uid = value.get("sandboxRunnerUID")
        if kind == "Sandbox" and isinstance(runner_uid, str) and ID.fullmatch(runner_uid):
            state["sandboxRunnerUID"] = runner_uid
        # Unknown CRD phases remain unknown; condition text is never copied.
        phase = value.get("phase")
        state["phase"] = phase if phase in {"Pending", "Running", "Succeeded", "Failed", "Unknown"} else "Unknown"
        state["conditions"] = [{"type": item["type"], "status": item["status"]}
                               for item in value.get("conditions", []) if isinstance(item, dict)
                               and item.get("type") in {"Complete", "Failed", "Ready", "PodScheduled", "ContainersReady", "Initialized"}
                               and item.get("status") in {"True", "False", "Unknown"}]
        key = kind, namespace, name
        if key in selected:
            raise ValueError("duplicate object identity")
        selected[key] = state
    return selected


def link_objects(selected):
    """Join only owner kind/name/UID and task matches, never name-only guesses."""
    # Old trial Pods use an owner chain. Retained Sandboxes intentionally have
    # no Runner ownerReference: their explicit binding is reported separately.
    for role in ("Job", "RunnerPod", "Sandbox", "TrialPod"):
        for state in selected.values():
            if state["role"] != role:
                continue
            if role == "Job":
                state["jobUID"] = state["uid"]
                continue
            if role == "Sandbox" and "sandboxRunnerUID" in state:
                matches = [parent for parent in selected.values()
                           if parent["role"] == "RunnerPod" and parent["namespace"] == state["namespace"]
                           and parent["uid"] == state["sandboxRunnerUID"] and parent["taskId"] == state["taskId"]]
                state["ownerLink"] = "unknown"
                state["runnerBinding"] = "unconfirmed"
                if len(matches) == 1:
                    state["runnerBinding"] = "annotation_uid_matched"
                    state["runnerUID"] = matches[0]["uid"]
                    if "jobUID" in matches[0]:
                        state["jobUID"] = matches[0]["jobUID"]
                continue
            candidates = [owner for owner in state["owners"] if
                          (role == "RunnerPod" and owner["kind"] == "Job" and owner["controller"])
                          or (role != "RunnerPod" and owner["kind"] in {"Pod", "Sandbox"})]
            state["ownerLink"] = "unknown"
            if len(candidates) != 1:
                continue
            owner = candidates[0]
            parent = selected.get((owner["kind"], state["namespace"], owner["name"]))
            if parent is None:
                state["ownerLink"] = "missing"
                continue
            if parent["uid"] != owner["uid"]:
                state["ownerLink"] = "uid_mismatch"
                continue
            if parent["taskId"] != state["taskId"]:
                state["ownerLink"] = "task_mismatch"
                continue
            if role != "RunnerPod" and parent["role"] not in {"RunnerPod", "Sandbox"}:
                continue
            state["ownerLink"] = "matched"
            if role == "RunnerPod":
                state["runnerUID"] = state["uid"]
            elif parent["role"] == "RunnerPod":
                state["runnerUID"] = parent["uid"]
            elif "runnerUID" in parent:
                state["runnerUID"] = parent["runnerUID"]
            if "jobUID" in parent:
                state["jobUID"] = parent["jobUID"]
    return selected


def identity_changes(previous, current):
    changes = []
    for key, old in previous.items():
        new = current.get(key)
        if new is None:
            changes.append({"change": "missing_since_previous_sample", **{
                field: old[field] for field in ("kind", "name", "namespace", "taskId", "uid")}})
        elif old["uid"] != new["uid"]:
            changes.append({"change": "recreated", "previousUID": old["uid"], **{
                field: new[field] for field in ("kind", "name", "namespace", "taskId", "uid")}})
    return changes


def read_objects(context, namespace, resource, kind, selector, timeout):
    command = ["kubectl", "--context", context, "--namespace", namespace,
               "--request-timeout", f"{timeout:.3f}s", "get", resource,
               "--selector", selector, "--chunk-size=200", "-o", "go-template=" + TEMPLATE]
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired:
        return None, "kubectl_timeout"
    except OSError:
        return None, "kubectl_unavailable"
    if result.returncode:
        return None, "kubectl_failed"
    try:
        return [json.loads(line) | {"kind": kind} for line in result.stdout.splitlines()], None
    except (ValueError, TypeError):
        return None, "invalid_projection"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path, help="completed submit.py JSONL")
    parser.add_argument("--output", required=True, type=Path, help="new private JSONL file")
    parser.add_argument("--context", required=True, help="explicit kubectl context")
    parser.add_argument("--namespace", required=True, help="one workload namespace")
    parser.add_argument("--job-selector", required=True, help="explicit Job label selector")
    parser.add_argument("--pod-selector", default="eruun.io/task-id")
    parser.add_argument("--sandbox-resource", help="optional discovered plural.version.group for Sandbox")
    parser.add_argument("--sandbox-selector", help="required with sandbox-resource")
    parser.add_argument("--duration", type=float, default=300)
    parser.add_argument("--interval", type=float, default=30)
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--rate", type=float, default=2, help="maximum kubectl starts per second")
    args = parser.parse_args()
    if (any(not math.isfinite(value) or value <= 0 for value in
            (args.duration, args.interval, args.timeout, args.rate))
            or not args.context.strip() or not args.namespace.strip()
            or not args.job_selector.strip() or not args.pod_selector.strip()
            or bool(args.sandbox_resource) != bool(args.sandbox_selector)
            or (args.sandbox_selector is not None and not args.sandbox_selector.strip())):
        parser.error("positive finite budgets, explicit scope and nonempty selectors are required")
    try:
        jobs, _ = load_submissions(args.input)
        descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except ValueError as error:
        parser.error(str(error))
    except (OSError, UnicodeError):
        parser.error("cannot read input or create a new output file")
    resources = [("jobs.batch", "Job", args.job_selector), ("pods", "Pod", args.pod_selector)]
    if args.sandbox_resource:
        resources.append((args.sandbox_resource, "Sandbox", args.sandbox_selector))
    started = time.monotonic()
    expires, next_call = started + args.duration, started
    previous, calls, samples, failed = {}, 0, 0, False
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        def emit(value):
            output.write(json.dumps(value, sort_keys=True) + "\n")
            output.flush()

        emit({"record": "configuration", "startedAt": time.time(), "namespace": args.namespace,
              "uniqueTaskIds": len(jobs), "durationSeconds": args.duration,
              "intervalSeconds": args.interval, "kubectlRateLimit": args.rate,
              "requestTimeoutSeconds": args.timeout, "sandboxRequested": bool(args.sandbox_resource)})
        while time.monotonic() < expires:
            rows, error = [], None
            began = time.time()
            for resource, kind, selector in resources:
                time.sleep(max(0, min(next_call, expires) - time.monotonic()))
                if time.monotonic() >= expires:
                    error = "deadline"
                    break
                next_call = time.monotonic() + 1 / args.rate
                calls += 1
                values, error = read_objects(args.context, args.namespace, resource, kind, selector,
                                             min(args.timeout, expires - time.monotonic()))
                if error:
                    break
                rows.extend(values)
            if error is None:
                try:
                    current = link_objects(clean_objects(rows, args.namespace, jobs))
                except (ValueError, TypeError, KeyError):
                    error = "invalid_identity"
            if error:
                failed = True
                emit({"record": "snapshot_error", "startedAt": began, "finishedAt": time.time(), "error": error})
            else:
                samples += 1
                at = time.time()
                by_task = {task_id: [] for task_id in jobs}
                for state in current.values():
                    by_task[state["taskId"]].append(state)
                emit({"record": "snapshot", "sample": samples, "startedAt": began, "finishedAt": at,
                      "objects": len(current), "kubectlCalls": calls})
                for change in identity_changes(previous, current):
                    emit({"record": "identity_change", "sample": samples, "observedAt": at, **change})
                for state in current.values():
                    emit({"record": "object", "sample": samples, "observedAt": at, **state})
                for task_id in jobs:
                    objects = by_task[task_id]
                    emit({"record": "task", "sample": samples, "observedAt": at, "taskId": task_id,
                          "jobs": sum(state["role"] == "Job" for state in objects),
                          "runnerPods": sum(state["role"] == "RunnerPod" for state in objects),
                          "linkedTrialPods": sum(state["role"] == "TrialPod" and "jobUID" in state for state in objects),
                          "sandboxes": sum(state["role"] == "Sandbox" for state in objects),
                          "actualTrialExecution": "unknown"})
                previous = current
            time.sleep(max(0, min(args.interval, expires - time.monotonic())))
        summary = {"record": "summary", "finishedAt": time.time(), "samples": samples,
                   "kubectlCalls": calls, "elapsedSeconds": time.monotonic() - started,
                   "observationIncomplete": failed or samples == 0}
        emit(summary)
    print(json.dumps(summary, sort_keys=True))
    return int(summary["observationIncomplete"])


if __name__ == "__main__":
    raise SystemExit(main())

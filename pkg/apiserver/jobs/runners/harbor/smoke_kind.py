"""Opt-in real oracle evaluation in a disposable kind cluster (no model calls)."""

import argparse
import base64
import hashlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import time
import uuid


EXAMPLE = Path(__file__).resolve().parents[5] / "examples/agent-evaluation/harbor-task"
SECURITY = {"runAsUser": 1000, "runAsGroup": 1000, "runAsNonRoot": True,
            "allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}}
SERVER = '''from http.server import BaseHTTPRequestHandler, HTTPServer
import hashlib
import json
from pathlib import Path
claims = {}
phase_failures = {}
class Handler(BaseHTTPRequestHandler):
    def authorized(self):
        return (self.headers.get('Authorization') == 'Bearer local-smoke-capability'
                and self.headers.get('X-Eruun-Runner-Pod-Name', '').startswith('runner')
                and bool(self.headers.get('X-Eruun-Runner-Pod-UID')))
    def reply(self, status, data):
        body = json.dumps({'code': 0, 'message': '', 'data': data}).encode()
        self.send_response(status); self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body))); self.end_headers(); self.wfile.write(body)
    def do_GET(self):
        if not self.authorized():
            self.send_error(403); return
        with Path('/work/datasets').open('a') as log: log.write(self.path + '\\n')
        data = Path('/data/tasks.tar.gz').read_bytes()
        self.send_response(200); self.end_headers(); self.wfile.write(data)
    def do_POST(self):
        if not self.authorized():
            self.send_error(403); return
        length = int(self.headers['Content-Length'])
        if self.path.startswith('/events/'):
            body = self.rfile.read(length)
            event = json.loads(body)
            task = self.path.removeprefix('/events/')
            owner = (self.headers['X-Eruun-Runner-Pod-Name'], self.headers['X-Eruun-Runner-Pod-UID'])
            if task in claims and claims[task] != owner:
                self.send_error(409); return
            if event['kind'] == 'claim': claims[task] = owner
            elif task not in claims:
                self.send_error(409); return
            failures = phase_failures.get(task, 0)
            if task == 'oracle-smoke' and event['kind'] == 'phase' and failures < 2:
                phase_failures[task] = failures + 1
                if failures == 0:
                    self.close_connection = True; return
                self.send_error(503); return
            with Path('/work/events.jsonl').open('a') as log:
                log.write(json.dumps({'task': task, 'owner': owner[0], **event}) + '\\n')
            self.reply(200, {'acceptedSequence': event['sequence'], 'action': 'continue'}); return
        Path('/work/received-status').write_text(self.headers.get('X-Eruun-Evaluation-Status', ''))
        with Path('/work/raw.tar.gz').open('wb') as output:
            while length:
                chunk = self.rfile.read(min(length, 1024 * 1024))
                if not chunk: raise RuntimeError('incomplete upload')
                output.write(chunk); length -= len(chunk)
        raw = Path('/work/raw.tar.gz').read_bytes()
        self.reply(201, {'id': hashlib.sha256(('source\\0' + self.path).encode()).hexdigest(),
                         'digest': hashlib.sha256(raw).hexdigest()})
HTTPServer(('0.0.0.0', 8080), Handler).serve_forever()
'''


def smoke(kubeconfig, runner_image, task_image, result_path, fail_verifier=False, fail_collection=False):
    namespace = "harbor-smoke-" + uuid.uuid4().hex[:8]

    def kubectl(*args, data=None, check=True):
        return subprocess.run(["kubectl", "--kubeconfig", str(kubeconfig), "-n", namespace, *args],
                              input=data, capture_output=True, check=check, timeout=180).stdout

    def apply(value):
        kubectl("apply", "-f", "-", data=json.dumps(value).encode())

    with tempfile.TemporaryDirectory() as directory:
        task = Path(directory) / "greeting"
        shutil.copytree(EXAMPLE, task)
        task_file = task / "task.toml"
        task_file.write_text(task_file.read_text().replace("example.com/your-team/eruun-greeting-task:1.0.0", task_image))
        with (task / "solution/solve.sh").open("a") as solution:
            solution.write("\ntest \"$(id -u)\" = 1000\ntest ! -e /var/run/secrets/kubernetes.io/serviceaccount/token\n"
                           "python -c \"from pathlib import Path; Path('/logs/artifacts/binary.bin').write_bytes(bytes(range(256))*17)\"\n")
        if fail_verifier:
            (task / "tests/test.sh").write_text("#!/bin/sh\nprintf 'intentional verifier failure\\n'\nexit 7\n")
        if fail_collection:
            with (task / "solution/solve.sh").open("a") as solution:
                solution.write("\nln -s /etc/passwd /logs/artifacts/unsafe\n")
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:gz") as bundle:
            bundle.add(task, arcname="greeting")
        package = buffer.getvalue()

    apply({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": namespace, "labels": {
        "pod-security.kubernetes.io/enforce": "restricted"}}})
    for name in ("runner", "sandbox"):
        apply({"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": name}})
    apply({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": {"name": "runner"},
           "rules": [{"apiGroups": [""], "resources": ["pods"], "verbs": ["create", "get", "list", "watch", "delete"]},
                     {"apiGroups": [""], "resources": ["pods/exec"], "verbs": ["get", "create"]}]})
    apply({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": {"name": "runner"},
           "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "runner"},
           "subjects": [{"kind": "ServiceAccount", "name": "runner", "namespace": namespace}]})
    apply({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "source"},
           "data": {"server.py": SERVER}, "binaryData": {"tasks.tar.gz": base64.b64encode(package).decode()}})
    resources = {"requests": {"cpu": "100m", "memory": "128Mi"}, "limits": {"cpu": "2", "memory": "2Gi"}}

    def pod(name, container, volumes, service_account=None):
        return {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name, "labels": {"app": name}},
                "spec": {"restartPolicy": "Never", "activeDeadlineSeconds": 600,
                         "serviceAccountName": service_account or "sandbox",
                         "automountServiceAccountToken": service_account == "runner",
                         "securityContext": {"fsGroup": 1000, "seccompProfile": {"type": "RuntimeDefault"}},
                         "volumes": volumes, "containers": [{"name": "main", "image": runner_image,
                             "imagePullPolicy": "Never", "securityContext": SECURITY,
                             "resources": resources, **container}]}}

    apply(pod("source", {"command": ["python", "/data/server.py"], "volumeMounts": [
        {"name": "source", "mountPath": "/data"}, {"name": "work", "mountPath": "/work"}]},
        [{"name": "source", "configMap": {"name": "source"}}, {"name": "work", "emptyDir": {}}]))
    apply({"apiVersion": "v1", "kind": "Service", "metadata": {"name": "source"},
           "spec": {"selector": {"app": "source"}, "ports": [{"port": 8080}]}})
    kubectl("wait", "--for=condition=Ready", "pod/source", "--timeout=120s")
    config = {"taskId": "oracle-smoke", "namespace": namespace,
              "datasetURL": "http://source:8080/dataset/oracle-smoke", "resultURL": "http://source:8080/results/oracle-smoke",
              "eventURL": "http://source:8080/events/oracle-smoke",
              "datasetDigest": hashlib.sha256(package).hexdigest(), "token": "local-smoke-capability",
              "agent": {"name": "oracle"}, "options": {"attempts": 1, "concurrency": 1},
              "resources": {"cpu": "100m", "memory": "128Mi", "cpuLimit": "1", "memoryLimit": "512Mi"},
              "sandboxServiceAccount": "sandbox", "timeoutSeconds": 300}
    apply(pod("runner", {"env": [
        {"name": "ERUUN_JOB_CONFIG", "value": json.dumps(config)},
        {"name": "POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
        {"name": "POD_UID", "valueFrom": {"fieldRef": {"fieldPath": "metadata.uid"}}}],
        "volumeMounts": [{"name": "work", "mountPath": "/work"}]},
        [{"name": "work", "emptyDir": {}}], "runner"))
    deadline = time.monotonic() + 360
    while time.monotonic() < deadline:
        state = json.loads(kubectl("get", "pod/runner", "-o", "json"))
        if state["status"]["phase"] in {"Succeeded", "Failed"}:
            break
        time.sleep(2)
    else:
        raise RuntimeError("runner did not finish within 360 seconds")
    raw = kubectl("exec", "source", "--", "cat", "/work/raw.tar.gz", check=False)
    result_path.write_bytes(raw)
    expected_phase = "Failed" if fail_verifier or fail_collection else "Succeeded"
    if state["status"]["phase"] != expected_phase:
        logs = kubectl("logs", "runner", check=False).decode(errors="replace")
        raise RuntimeError(f"runner failed; raw diagnostics saved to {result_path}: {logs}")
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as archive:
        report = json.load(archive.extractfile("result.json"))
        native = json.load(archive.extractfile("outputs/run/result.json"))
        assert report["collectionComplete"] == (not fail_collection), report
        expected_status = "failed" if fail_verifier else "succeeded"
        assert report["executionStatus"] == expected_status, report
        if fail_verifier:
            assert native["stats"]["n_errored_trials"] == 1, native
            assert report["frameworkExitCode"] == 0, report
            assert any(member.name.endswith("exception.txt") for member in archive)
        else:
            assert native["stats"]["n_completed_trials"] == 1 and native["stats"]["n_errored_trials"] == 0, native
        if fail_collection:
            assert "sandbox_download_failed" in {entry["reason"] for entry in report["collectionErrors"]}, report
            manifests = [json.load(archive.extractfile(member)) for member in archive if member.name.endswith("/artifacts/manifest.json")]
            assert any(entry["status"] == "failed" for manifest in manifests for entry in manifest)
        else:
            binary = next(member for member in archive if member.name.endswith("/binary.bin"))
            assert archive.extractfile(binary).read() == bytes(range(256)) * 17
        if not fail_verifier:
            reward = next(member for member in archive if member.name.endswith("/reward.txt"))
            assert archive.extractfile(reward).read().strip() == b"1"
    transfer_status = "failed" if fail_collection else expected_status
    assert kubectl("exec", "source", "--", "cat", "/work/received-status") == transfer_status.encode()
    events = [json.loads(line) for line in kubectl("exec", "source", "--", "cat", "/work/events.jsonl").splitlines()]
    task_events = [event for event in events if event["task"] == "oracle-smoke"]
    assert task_events[0]["kind"] == "claim" and task_events[0]["sequence"] == 1, task_events
    assert [event.get("phase") for event in task_events if event["kind"] == "phase"] == ["preparing", "running", "finalizing"], task_events
    assert task_events[-1]["kind"] == "terminal" and task_events[-1]["terminal"]["artifactDigest"] == hashlib.sha256(raw).hexdigest(), task_events
    trial_pods = json.loads(kubectl("get", "pods", "-l", "eruun.io/task-id=oracle-smoke", "-o", "json"))
    if fail_collection:
        assert len(trial_pods["items"]) == 1, "failed collection must retain its source Pod"
        retained = trial_pods["items"][0]
        assert retained["spec"]["activeDeadlineSeconds"] == 300
        assert retained["metadata"]["ownerReferences"][0]["uid"] == state["metadata"]["uid"]
        assert kubectl("exec", retained["metadata"]["name"], "--", "cat", "/logs/artifacts/binary.bin") == bytes(range(256)) * 17
        outcome = "incomplete collection reported and source Pod retained"
    else:
        assert not trial_pods["items"], "native trial Pods were not cleaned up"
        outcome = "native verifier failure correctly detected" if fail_verifier else "reward=1"
    print(f"Harbor 0.22.0 oracle smoke passed: {outcome}, restricted Pod, original binary preserved; archive: {result_path}")

    if not fail_verifier and not fail_collection:
        oom_config = config | {"taskId": "oom-smoke",
                               "datasetURL": "http://source:8080/dataset/oom-smoke",
                               "resultURL": "http://source:8080/results/oom-smoke",
                               "eventURL": "http://source:8080/events/oom-smoke"}
        downward = [
            {"name": "ERUUN_JOB_CONFIG", "value": json.dumps(oom_config)},
            {"name": "POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
            {"name": "POD_UID", "valueFrom": {"fieldRef": {"fieldPath": "metadata.uid"}}},
        ]
        oom_code = "import json,os,time; from runner import StatusReporter; import threading; c=json.loads(os.environ['ERUUN_JOB_CONFIG']); r=StatusReporter(c,threading.Event(),time.monotonic()+120); r.claim(); x=[]; [(x.append(bytearray(8*1024*1024)),time.sleep(.02)) for _ in range(64)]"
        apply(pod("runner-oom", {"command": ["python", "-c", oom_code], "env": downward,
                                  "resources": {"requests": {"cpu": "100m", "memory": "64Mi"}, "limits": {"cpu": "1", "memory": "96Mi"}}},
                  [{"name": "work", "emptyDir": {}}], "runner"))
        kubectl("wait", "--for=jsonpath={.status.phase}=Failed", "pod/runner-oom", "--timeout=120s")
        oom_state = json.loads(kubectl("get", "pod/runner-oom", "-o", "json"))
        assert oom_state["status"]["containerStatuses"][0]["state"]["terminated"]["reason"] == "OOMKilled", oom_state["status"]
        apply(pod("runner-replacement", {"env": downward, "volumeMounts": [{"name": "work", "mountPath": "/work"}]},
                  [{"name": "work", "emptyDir": {}}], "runner"))
        kubectl("wait", "--for=jsonpath={.status.phase}=Failed", "pod/runner-replacement", "--timeout=120s")
        datasets = kubectl("exec", "source", "--", "cat", "/work/datasets", check=False).decode()
        assert "/dataset/oom-smoke" not in datasets, "replacement downloaded data after conflicting claim"
        print("Runner OOM and replacement claim conflict smoke passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", default="kind")
    parser.add_argument("--runner-image", default="eruun-harbor-runner:0.22.0-local")
    parser.add_argument("--task-image", default="eruun-harbor-task:1.0.0-local")
    parser.add_argument("--result", type=Path, required=True)
    args = parser.parse_args()
    if shutil.which(args.kind) is None:
        raise RuntimeError(f"kind executable not found: {args.kind}")
    name = "eruun-harbor-" + uuid.uuid4().hex[:8]
    with tempfile.TemporaryDirectory() as directory:
        kubeconfig = Path(directory) / "kubeconfig"
        created = False
        try:
            subprocess.run([args.kind, "create", "cluster", "--name", name, "--kubeconfig", str(kubeconfig), "--wait", "120s"], check=True)
            created = True
            subprocess.run([args.kind, "load", "docker-image", args.runner_image, args.task_image, "--name", name], check=True)
            smoke(kubeconfig, args.runner_image, args.task_image, args.result)
            failure_result = args.result.with_name(args.result.name.removesuffix(".tar.gz") + "-failed.tar.gz")
            smoke(kubeconfig, args.runner_image, args.task_image, failure_result, fail_verifier=True)
            incomplete_result = args.result.with_name(args.result.name.removesuffix(".tar.gz") + "-incomplete.tar.gz")
            smoke(kubeconfig, args.runner_image, args.task_image, incomplete_result, fail_collection=True)
        finally:
            if created:
                subprocess.run([args.kind, "delete", "cluster", "--name", name, "--kubeconfig", str(kubeconfig)], check=True)


if __name__ == "__main__":
    main()

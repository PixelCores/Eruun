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


EXAMPLE = Path(__file__).resolve().parents[2] / "examples/agent-evaluation/harbor-task"
SECURITY = {"runAsUser": 1000, "runAsGroup": 1000, "runAsNonRoot": True,
            "allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}}
SERVER = '''from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
class Handler(BaseHTTPRequestHandler):
    def authorized(self):
        return (self.headers.get('Authorization') == 'Bearer local-smoke-capability'
                and self.headers.get('X-Eruun-Runner-Pod-Name') == 'runner'
                and bool(self.headers.get('X-Eruun-Runner-Pod-UID')))
    def do_GET(self):
        if not self.authorized():
            self.send_error(403); return
        data = Path('/data/tasks.tar.gz').read_bytes()
        self.send_response(200); self.end_headers(); self.wfile.write(data)
    def do_POST(self):
        if not self.authorized():
            self.send_error(403); return
        Path('/work/received-status').write_text(self.headers.get('X-Eruun-Evaluation-Status', ''))
        length = int(self.headers['Content-Length'])
        with Path('/work/raw.tar.gz').open('wb') as output:
            while length:
                chunk = self.rfile.read(min(length, 1024 * 1024))
                if not chunk: raise RuntimeError('incomplete upload')
                output.write(chunk); length -= len(chunk)
        self.send_response(201); self.end_headers()
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
              "datasetURL": "http://source:8080/dataset", "resultURL": "http://source:8080/results",
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", default="kind")
    parser.add_argument("--runner-image", default="eruun-harbor-runner:0.22.0-local")
    parser.add_argument("--task-image", default="eruun-harbor-task:1.0.0-local")
    parser.add_argument("--result", type=Path, required=True)
    args = parser.parse_args()
    name = "eruun-harbor-" + uuid.uuid4().hex[:8]
    with tempfile.TemporaryDirectory() as directory:
        kubeconfig = Path(directory) / "kubeconfig"
        try:
            subprocess.run([args.kind, "create", "cluster", "--name", name, "--kubeconfig", str(kubeconfig), "--wait", "120s"], check=True)
            subprocess.run([args.kind, "load", "docker-image", args.runner_image, args.task_image, "--name", name], check=True)
            smoke(kubeconfig, args.runner_image, args.task_image, args.result)
            failure_result = args.result.with_name(args.result.name.removesuffix(".tar.gz") + "-failed.tar.gz")
            smoke(kubeconfig, args.runner_image, args.task_image, failure_result, fail_verifier=True)
            incomplete_result = args.result.with_name(args.result.name.removesuffix(".tar.gz") + "-incomplete.tar.gz")
            smoke(kubeconfig, args.runner_image, args.task_image, incomplete_result, fail_collection=True)
        finally:
            subprocess.run([args.kind, "delete", "cluster", "--name", name, "--kubeconfig", str(kubeconfig)], check=True)


if __name__ == "__main__":
    main()

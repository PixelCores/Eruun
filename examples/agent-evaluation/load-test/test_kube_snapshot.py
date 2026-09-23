import copy
import json
import subprocess
import unittest
from unittest import mock

import kube_snapshot as snapshot


def objects():
    return [
        {"kind": "Job", "namespace": "space", "name": "job", "uid": "job-uid", "annotatedTaskId": "task-1"},
        {"kind": "Pod", "namespace": "space", "name": "runner", "uid": "runner-uid", "taskId": "task-1",
         "runner": "true", "phase": "Running", "owners": [
             {"kind": "Job", "name": "job", "uid": "job-uid", "controller": True}]},
        {"kind": "Pod", "namespace": "space", "name": "trial", "uid": "trial-uid", "taskId": "task-1",
         "phase": "Running", "owners": [{"kind": "Pod", "name": "runner", "uid": "runner-uid"}]},
    ]


class SnapshotTests(unittest.TestCase):
    def linked(self, rows):
        return snapshot.link_objects(snapshot.clean_objects(rows, "space", {"task-1", "task-2"}))

    def test_owner_uid_chain_and_no_execution_claim(self):
        linked = self.linked(objects())
        trial = linked[("Pod", "space", "trial")]
        self.assertEqual(trial["ownerLink"], "matched")
        self.assertEqual((trial["jobUID"], trial["runnerUID"]), ("job-uid", "runner-uid"))
        self.assertNotIn("actualTrialExecution", trial)

    def test_selector_missing_job_remains_unknown(self):
        linked = self.linked(objects()[1:])
        self.assertEqual(linked[("Pod", "space", "runner")]["ownerLink"], "missing")
        trial = linked[("Pod", "space", "trial")]
        self.assertEqual(trial["runnerUID"], "runner-uid")
        self.assertNotIn("jobUID", trial)

    def test_recreated_owner_cannot_link_by_name(self):
        rows = objects()
        rows[1]["uid"] = "replacement-runner"
        linked = self.linked(rows)
        trial = linked[("Pod", "space", "trial")]
        self.assertEqual(trial["ownerLink"], "uid_mismatch")
        self.assertNotIn("runnerUID", trial)
        self.assertNotIn("jobUID", trial)

    def test_cross_task_and_noncontroller_job_owner_fail_link(self):
        rows = objects()
        rows[0]["annotatedTaskId"] = "task-2"
        self.assertEqual(self.linked(rows)[("Pod", "space", "runner")]["ownerLink"], "task_mismatch")
        rows = objects()
        rows[1]["owners"][0]["controller"] = False
        self.assertEqual(self.linked(rows)[("Pod", "space", "runner")]["ownerLink"], "unknown")

    def test_namespace_task_filter_and_private_field_allowlist(self):
        rows = objects()
        rows[0].update(spec={"env": [{"value": "private-token"}]}, annotations={"secret": "private"})
        rows[2]["conditions"] = [{"type": "Ready", "status": "True", "message": "private-message"}]
        rows += [{**copy.deepcopy(rows[0]), "namespace": "other"},
                 {**copy.deepcopy(rows[0]), "annotatedTaskId": "unrelated", "name": "unrelated"}]
        linked = self.linked(rows)
        self.assertEqual(len(linked), 3)
        self.assertNotIn("private", json.dumps(list(linked.values())))

    def test_missing_and_rebuilt_objects_are_distinct(self):
        previous = self.linked(objects())
        rows = objects()[:2]
        rows[1]["uid"] = "new-runner"
        changes = snapshot.identity_changes(previous, self.linked(rows))
        self.assertEqual({row["change"] for row in changes}, {"missing_since_previous_sample", "recreated"})
        recreated = next(row for row in changes if row["change"] == "recreated")
        self.assertEqual(recreated["previousUID"], "runner-uid")

    def test_optional_sandbox_uses_actual_owner_chain(self):
        rows = objects()
        rows.append({"kind": "Sandbox", "namespace": "space", "name": "sandbox", "uid": "sandbox-uid",
                     "taskId": "task-1", "owners": [{"kind": "Pod", "name": "runner", "uid": "runner-uid"}]})
        rows[2]["owners"] = [{"kind": "Sandbox", "name": "sandbox", "uid": "sandbox-uid"}]
        trial = self.linked(rows)[("Pod", "space", "trial")]
        self.assertEqual((trial["runnerUID"], trial["jobUID"]), ("runner-uid", "job-uid"))

    def test_duplicate_and_conflicting_identity_rejected(self):
        rows = objects()
        with self.assertRaises(ValueError):
            self.linked(rows + [rows[0]])
        rows[0]["taskId"] = "task-2"
        with self.assertRaises(ValueError):
            self.linked(rows)

    def test_retained_sandbox_binding_has_no_runner_owner_reference(self):
        rows = objects()
        rows.append({"kind": "Sandbox", "namespace": "space", "name": "sandbox", "uid": "sandbox-uid",
                     "taskId": "task-1", "sandboxRunnerUID": "runner-uid"})
        rows[2]["owners"] = [{"kind": "Sandbox", "name": "sandbox", "uid": "sandbox-uid", "controller": True}]
        linked = self.linked(rows)
        self.assertEqual(linked[("Sandbox", "space", "sandbox")]["runnerBinding"], "annotation_uid_matched")
        self.assertEqual(linked[("Pod", "space", "trial")]["jobUID"], "job-uid")
        rows[1]["uid"] = "replacement-runner"
        linked = self.linked(rows)
        self.assertEqual(linked[("Sandbox", "space", "sandbox")]["runnerBinding"], "unconfirmed")
        self.assertNotIn("jobUID", linked[("Pod", "space", "trial")])

    def test_kubectl_is_read_only_scoped_projected_and_time_bounded(self):
        result = subprocess.CompletedProcess([], 0, json.dumps(objects()[1]), "")
        with mock.patch("kube_snapshot.subprocess.run", return_value=result) as run:
            values, error = snapshot.read_objects("test-context", "space", "pods", "Pod", "eruun.io/task-id", 3)
        self.assertIsNone(error)
        self.assertEqual(values[0]["uid"], "runner-uid")
        command = run.call_args.args[0]
        self.assertEqual(command[:5], ["kubectl", "--context", "test-context", "--namespace", "space"])
        self.assertIn("--selector", command)
        self.assertIn("get", command)
        self.assertEqual(run.call_args.kwargs["timeout"], 3)
        self.assertNotIn(".spec", command[-1])
        self.assertNotIn(".env", command[-1])

    def test_kubectl_errors_never_echo_stderr(self):
        with mock.patch("kube_snapshot.subprocess.run", return_value=subprocess.CompletedProcess([], 1, "", "private-token")):
            self.assertEqual(snapshot.read_objects("ctx", "space", "pods", "Pod", "x=y", 1), (None, "kubectl_failed"))
        with mock.patch("kube_snapshot.subprocess.run", side_effect=subprocess.TimeoutExpired("private", 1)):
            self.assertEqual(snapshot.read_objects("ctx", "space", "pods", "Pod", "x=y", 1), (None, "kubectl_timeout"))


if __name__ == "__main__":
    unittest.main()

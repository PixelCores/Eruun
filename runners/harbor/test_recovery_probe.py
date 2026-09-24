import contextlib
import importlib.metadata
import io
import json
import unittest
from unittest.mock import patch

import recovery_probe


class RecoveryProbeTests(unittest.TestCase):
    def test_real_pinned_harbor_reconciliation_cannot_resume_incomplete_trials(self):
        # Missing/wrong dependencies fail this test; the capability gate must
        # never look verified because the meaningful test silently skipped.
        report = recovery_probe.probe()
        self.assertFalse(report["gatePassed"])
        self.assertEqual(report["versions"], {"harbor": "0.22.0", "kubernetes": "32.0.1"})
        self.assertEqual(report["agentNativeResumeDeclared"], {
            "codex": True, "claude-code": True, "terminus-2": False, "oracle": False,
        })
        for experiment in report["experiments"]:
            with self.subTest(result=experiment["interruptedResult"]):
                self.assertTrue(experiment["sourceDirectoryRemoved"])
                self.assertTrue(experiment["completedTrialSkipped"])
                self.assertTrue(experiment["completedResultUnchanged"])
                self.assertTrue(experiment["incompleteTrialScheduledWithNewIdentity"])
                self.assertFalse(experiment["oldProgressLoaded"])
                self.assertEqual(experiment["oldProgressFileRetained"], experiment["interruptedResult"] != "missing")

    def test_wrong_or_missing_dependency_is_an_error_not_a_skip(self):
        for version in ("0.23.0", importlib.metadata.PackageNotFoundError("harbor")):
            with self.subTest(version=str(version)):
                options = {"side_effect": version} if isinstance(version, Exception) else {"return_value": version}
                with patch.object(recovery_probe.importlib.metadata, "version", **options):
                    with self.assertRaises(recovery_probe.ProbeError):
                        recovery_probe.probe()

    def test_cli_separates_failed_gate_from_failed_experiment(self):
        with patch.object(recovery_probe, "probe", return_value={"gatePassed": False}), contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(recovery_probe.main(), 2)
            self.assertFalse(json.loads(output.getvalue())["gatePassed"])
        with patch.object(recovery_probe, "probe", side_effect=recovery_probe.ProbeError("missing dependency")), contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(recovery_probe.main(), 1)
            self.assertEqual(json.loads(output.getvalue())["experimentError"], "missing dependency")


if __name__ == "__main__":
    unittest.main()

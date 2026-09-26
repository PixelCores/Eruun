import copy
import json
from pathlib import Path
import signal
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

import native_resume_probe as probe


class NativeResumeProbeTests(unittest.TestCase):
    def test_saved_prompt_requires_a_complete_newline_terminated_json_object(self):
        complete = json.dumps({"message": probe.INTERRUPTED}).encode() + b"\n"
        tail = complete + b" " * (1024 * 1024 - len(complete) - 1) + b"\n"
        cases = [
            ("complete", complete, True),
            ("complete_before_partial_last_line", complete + b'{"next":', True),
            ("partial_json", b'{"message":"' + probe.INTERRUPTED.encode() + b"\n", False),
            ("missing_newline", complete[:-1], False),
            ("scalar", json.dumps(probe.INTERRUPTED).encode() + b"\n", False),
            ("clipped_first_line", b'{"prefix":' + tail, False),
            ("tail_starts_at_line_boundary", b"{}\n" + tail, True),
        ]
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            for name, content, accepted in cases:
                with self.subTest(name=name):
                    (state / "session.jsonl").write_bytes(content)
                    process = Mock()
                    process.poll.return_value = None
                    with patch.object(probe.time, "monotonic", side_effect=[100, 100.1, 106]), patch.object(probe.time, "sleep"):
                        if accepted:
                            probe._wait_for_saved_prompt(state, "session", process)
                        else:
                            with self.assertRaisesRegex(probe.ProbeError, "did not appear"):
                                probe._wait_for_saved_prompt(state, "session", process)

    def test_saved_prompt_wait_rejects_process_exit_and_times_out(self):
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            for exit_code, message in ((None, "did not appear"), (0, "exited before")):
                with self.subTest(exit_code=exit_code):
                    process = Mock()
                    process.poll.return_value = exit_code
                    with patch.object(probe.time, "monotonic", side_effect=[100, 100.1, 106]), patch.object(probe.time, "sleep"):
                        with self.assertRaisesRegex(probe.ProbeError, message):
                            probe._wait_for_saved_prompt(state, "session", process)

    def test_only_verified_cli_versions_are_accepted(self):
        for agent, version in probe.CLI_VERSIONS.items():
            with self.subTest(agent=agent):
                probe._check_version(agent, version + "\n")
                with self.assertRaises(probe.ProbeError):
                    probe._check_version(agent, "different CLI version")

    def test_incomplete_session_recovery_evidence_is_rejected(self):
        report = {
            "agent": "codex", "version": probe.CLI_VERSIONS["codex"],
            "sessionId": "fixture-session", "resumedSessionId": "fixture-session",
            "firstExit": 0, "interruptedExit": -signal.SIGKILL, "resumeExit": 0,
            "interruptedPromptObservedOnDiskBeforeKill": True,
            "sourceStateDeleted": True, "completedMarkerPreserved": True,
            "interruptedPromptPreserved": True, "resumePromptSent": True,
            "modelRequestPaths": ["/v1/responses"] * 3,
        }
        probe._check_report(report)
        for key in report:
            with self.subTest(missing=key):
                incomplete = copy.deepcopy(report)
                incomplete.pop(key)
                with self.assertRaises(probe.ProbeError):
                    probe._check_report(incomplete)
        for key, value in (("resumeExit", 1), ("interruptedExit", 0), ("resumedSessionId", "different"),
                           ("completedMarkerPreserved", False), ("sourceStateDeleted", False),
                           ("modelRequestPaths", ["https://example.invalid/v1/responses"] * 3)):
            with self.subTest(invalid=key):
                invalid = copy.deepcopy(report)
                invalid[key] = value
                with self.assertRaises(probe.ProbeError):
                    probe._check_report(invalid)

    def test_timeout_terminates_cli_and_descendant_process(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "work").mkdir()
            marker = directory / "descendant-survived"
            ready = directory / "descendant-started"
            child = f"import time; from pathlib import Path; Path({str(ready)!r}).touch(); time.sleep(0.8); Path({str(marker)!r}).touch()"
            parent = f"import subprocess, sys, time; subprocess.Popen([sys.executable, '-c', {child!r}]); time.sleep(30)"
            with self.assertRaises(probe.ProbeError):
                probe._run([sys.executable, "-c", parent], directory=directory,
                           env={"PATH": "/usr/bin:/bin"}, label="timeout", timeout=0.4)
            self.assertTrue(ready.exists(), "the child must have started to exercise descendant cleanup")
            time.sleep(0.8)
            self.assertFalse(marker.exists())
            self.assertEqual((directory / "timeout.stdout").stat().st_mode & 0o777, 0o600)
            self.assertEqual((directory / "timeout.stderr").stat().st_mode & 0o777, 0o600)

    def test_environment_does_not_inherit_credentials(self):
        with tempfile.TemporaryDirectory() as temporary:
            environment = probe._environment(Path(temporary))
            self.assertNotIn("OPENAI_API_KEY", environment)
            self.assertNotIn("ANTHROPIC_API_KEY", environment)
            self.assertNotIn("CODEX_HOME", environment)
            self.assertEqual(environment["HOME"], str(Path(temporary) / "home"))


if __name__ == "__main__":
    unittest.main()

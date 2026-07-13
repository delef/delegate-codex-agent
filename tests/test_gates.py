from pathlib import Path
import stat
import tempfile
import unittest

from delegate_agent.gates import check_diff_scope, run_checks, run_command_check, validate_result


VALID = {
    "result": "done", "evidence": "test.py:1", "changes": "none",
    "verification": "ok", "risks": "none", "recommended_next_action": "review",
}


class GateTests(unittest.TestCase):
    def test_result_schema_requires_writer_fields_for_writers(self):
        read = validate_result({key: value for key, value in VALID.items() if key not in {"changes", "verification"}})
        writer = validate_result({key: value for key, value in VALID.items() if key != "changes"}, writer=True)
        self.assertEqual(read.status, "accepted")
        self.assertEqual(writer.status, "rejected")
        self.assertIn("changes", writer.evidence["missing"])

    def test_command_check_uses_argv_and_writes_artifact(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = run_command_check({"type": "command", "argv": ["python3", "-c", "print('ok')"], "timeout_seconds": 10}, cwd=tmp, artifact_dir=tmp)
            self.assertEqual(result.status, "accepted")
            self.assertIn("ok", Path(result.artifact).read_text(encoding="utf-8"))

    def test_command_failure_and_timeout_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            failed = run_command_check({"type": "command", "argv": ["python3", "-c", "raise SystemExit(3)"], "timeout_seconds": 10}, cwd=tmp, artifact_dir=tmp)
            timed_out = run_command_check({"type": "command", "argv": ["python3", "-c", "import time; time.sleep(2)"], "timeout_seconds": 1}, cwd=tmp, artifact_dir=tmp)
        self.assertEqual(failed.reason, "verification_failed")
        self.assertEqual(timed_out.reason, "verification_timeout")

    def test_run_checks_stops_at_first_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            checks = [{"type": "result_schema"}, {"type": "command", "argv": ["python3", "-c", "raise SystemExit(1)"], "timeout_seconds": 10}, {"type": "approval"}]
            results = run_checks(checks, result=VALID, cwd=tmp, artifact_dir=tmp)
        self.assertEqual([item.gate_type for item in results], ["result_schema", "command"])


if __name__ == "__main__":
    unittest.main()

import json
from pathlib import Path
import os
import stat
import sys
import tempfile
import unittest

from delegate_agent.worker import WorkerRequest, build_command, check_capabilities, run_worker


FAKE_CODEX = r'''#!/usr/bin/env python3
import json
import sys
from pathlib import Path

if sys.argv[-1] == "--help":
    print("--json --output-schema --sandbox")
    raise SystemExit(0)
output = Path(sys.argv[sys.argv.index("-o") + 1])
print(json.dumps({"type": "thread.started", "thread_id": "thread-1"}), flush=True)
print(json.dumps({"type": "turn.completed", "usage": {"input_tokens": 4, "cached_input_tokens": 1, "output_tokens": 2, "reasoning_output_tokens": 1}}), flush=True)
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text("result", encoding="utf-8")
'''


class WorkerTests(unittest.TestCase):
    def test_build_command_uses_argv_and_schema(self):
        request = WorkerRequest(
            binary=("codex",), cwd=Path("/repo"), model="gpt-luna", sandbox="read-only",
            prompt="hello", result_path=Path("/tmp/result.md"), events_path=Path("/tmp/events"),
            output_schema_path=Path("/schemas/result.json"),
        )
        command = build_command(request)
        self.assertIn("--output-schema", command)
        self.assertIn("read-only", command)
        self.assertEqual(command[-1], "-")

    def test_resume_command_keeps_thread_id(self):
        request = WorkerRequest(
            binary=("codex",), cwd=Path("/repo"), model="gpt-luna", sandbox="read-only",
            prompt="feedback", result_path=Path("/tmp/result.md"), events_path=Path("/tmp/events"),
            resume_thread_id="thread-1",
        )
        command = build_command(request)
        self.assertIn("resume", command)
        self.assertIn("thread-1", command)

    def test_run_worker_persists_events_and_usage(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binary = root / "fake-codex"
            binary.write_text(FAKE_CODEX, encoding="utf-8")
            binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
            request = WorkerRequest(
                binary=(str(binary),), cwd=root, model="gpt-luna", sandbox="read-only",
                prompt="hello", result_path=root / "result.md", events_path=root / "events.jsonl",
            )
            processes = []
            lines = []
            outcome = run_worker(request, on_process=processes.append, on_line=lines.append)

            self.assertEqual(outcome.exit_code, 0)
            self.assertEqual(outcome.thread_id, "thread-1")
            self.assertEqual(outcome.event_count, 2)
            self.assertEqual(outcome.usage["total_tokens"], 6)
            self.assertEqual(outcome.usage["uncached_input_tokens"], 3)
            self.assertEqual(len(processes), 1)
            self.assertTrue(request.result_path.is_file())
            self.assertEqual(len(request.events_path.read_text(encoding="utf-8").splitlines()), 2)
            self.assertEqual(len(lines), 2)

    def test_hard_token_limit_terminates_after_usage_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binary = root / "fake-codex"
            binary.write_text(
                FAKE_CODEX.replace("output.write_text", "import time; time.sleep(5); output.write_text"),
                encoding="utf-8",
            )
            binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
            request = WorkerRequest(
                binary=(str(binary),), cwd=root, model="gpt-luna", sandbox="read-only",
                prompt="hello", result_path=root / "result.md", events_path=root / "events.jsonl",
                hard_tokens=5,
            )
            outcome = run_worker(request)
        self.assertTrue(outcome.budget_exhausted)
        self.assertGreaterEqual(outcome.usage["total_tokens"], 5)

    def test_capability_detection(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binary = root / "fake-codex"
            binary.write_text(FAKE_CODEX, encoding="utf-8")
            binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
            self.assertEqual(check_capabilities((str(binary),)), {"json", "output_schema", "sandbox"})


if __name__ == "__main__":
    unittest.main()

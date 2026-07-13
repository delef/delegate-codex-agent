import json
from pathlib import Path
import stat
import tempfile
import unittest

from delegate_agent.hooks import run_hook
from delegate_agent.schema import normalize_workflow
from delegate_agent.store import JournalStateStore
from delegate_agent.workflow import WorkflowRuntime


class HookTests(unittest.TestCase):
    def script(self, root: Path, code: str) -> Path:
        path = root / "hook.py"
        path.write_text("#!/usr/bin/env python3\n" + code, encoding="utf-8")
        path.chmod(path.stat().st_mode | stat.S_IXUSR)
        return path

    def test_hook_receives_versioned_json_and_supports_block(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = self.script(root, "import json, sys; payload=json.load(sys.stdin); print(payload['event']); raise SystemExit(2)")
            result = run_hook(
                {"argv": [str(path)], "timeout_seconds": 5, "output_limit": 100},
                event="task_created", payload={"task_id": "a"}, cwd=root,
            )
        self.assertFalse(result.allowed)
        self.assertEqual(result.reason, "hook_blocked")
        self.assertIn("task_created", result.output)

    def test_hook_timeout_can_be_fail_open(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = self.script(root, "import time; time.sleep(2)")
            result = run_hook(
                {"argv": [str(path)], "timeout_seconds": 1, "failure_policy": "fail_open"},
                event="worker_idle", payload={}, cwd=root,
            )
        self.assertTrue(result.allowed)
        self.assertEqual(result.reason, "hook_timeout_fail_open")

    def test_task_created_hook_blocks_runtime_task(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo"
            repo.mkdir()
            spec = root / "task.json"
            spec.write_text(json.dumps({"name": "task", "objective": "run"}), encoding="utf-8")
            hook = self.script(root, "raise SystemExit(2)")
            workflow = normalize_workflow({
                "version": 1, "name": "hook-demo", "cwd": str(repo),
                "budget": {"total_tokens": 10, "max_workers": 1},
                "hooks": {"task_created": [{"argv": [str(hook)]}]},
                "nodes": [{"id": "task", "kind": "agent", "spec": str(spec)}],
            })
            runtime = WorkflowRuntime(root / "run", workflow)
            status = runtime.run()
            runtime.close()
            store = JournalStateStore.open(root / "run")
            task_state = store.snapshot["tasks"]["task"]["state"]
            store.close()
        self.assertEqual(status["state"], "failed")
        self.assertEqual(task_state, "blocked")


if __name__ == "__main__":
    unittest.main()

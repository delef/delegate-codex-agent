import json
from pathlib import Path
import tempfile
import unittest

from orchestrator_agent.cli import inspect_workflow, request_control
from orchestrator_agent.schema import normalize_workflow
from orchestrator_agent.store import JournalStateStore


class CliTests(unittest.TestCase):
    def test_inspect_and_control_request_are_machine_readable(self):
        workflow = normalize_workflow({
            "version": 1, "name": "cli", "cwd": ".",
            "budget": {"total_tokens": 10, "max_workers": 1},
            "nodes": [{"id": "a", "kind": "agent", "spec": "a.json"}],
        })
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "run"
            store = JournalStateStore.create(path, workflow)
            store.close()
            status = inspect_workflow(path)
            request = request_control(path, "pause", {"reason": "operator"}, request_id="cli-1")
        self.assertEqual(status["state"], "created")
        self.assertTrue(request.name == "cli-1.json")


if __name__ == "__main__":
    unittest.main()

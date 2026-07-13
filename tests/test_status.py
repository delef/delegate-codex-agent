from pathlib import Path
import tempfile
import unittest

from delegate_agent.schema import normalize_workflow
from delegate_agent.status import build_status, status_json
from delegate_agent.store import JournalStateStore


def workflow_value():
    return normalize_workflow({
        "version": 1, "name": "status", "cwd": ".",
        "budget": {"total_tokens": 10, "max_workers": 1},
        "nodes": [{"id": "a", "kind": "agent", "spec": "a.json"}],
    })


class StatusTests(unittest.TestCase):
    def test_status_is_aggregate_and_payload_free(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JournalStateStore.create(Path(tmp) / "run", workflow_value())
            store.transition_workflow("running", expected_state="created")
            store.transition_task("a", "ready", expected_state="pending")
            value = build_status(store)
            encoded = status_json(store)
            store.close()
        self.assertEqual(value["next"], "a")
        self.assertEqual(value["budget"]["remaining_tokens"], 10)
        self.assertNotIn("prompt", encoded)
        self.assertIn('"schema_version": 1', encoded)


if __name__ == "__main__":
    unittest.main()

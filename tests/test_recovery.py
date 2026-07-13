from pathlib import Path
import tempfile
import unittest

from delegate_agent.recovery import reconcile
from delegate_agent.schema import normalize_workflow
from delegate_agent.store import JournalStateStore


def workflow_value():
    return normalize_workflow({
        "version": 1, "name": "recovery", "cwd": ".",
        "budget": {"total_tokens": 20, "max_workers": 1},
        "nodes": [{"id": "a", "kind": "agent", "spec": "a.json", "budget": {
            "reserve_tokens": 5, "hard_tokens": 10,
        }}],
    })


class RecoveryTests(unittest.TestCase):
    def test_missing_worker_is_interrupted_and_reservation_released(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JournalStateStore.create(Path(tmp) / "run", workflow_value())
            store.transition_workflow("running", expected_state="created")
            store.transition_task("a", "ready", expected_state="pending")
            store.reserve_budget(5, task_id="a")
            store.transition_task("a", "running", expected_state="ready", fields={"pid": 999})
            self.assertEqual(reconcile(store, is_alive=lambda pid: False), ["a"])
            snapshot = store.snapshot
            store.close()
        self.assertEqual(snapshot["tasks"]["a"]["state"], "interrupted")
        self.assertEqual(snapshot["budget"]["reserved_tokens"], 0)
        self.assertEqual(snapshot["state"], "interrupted")

    def test_live_worker_is_not_touched(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JournalStateStore.create(Path(tmp) / "run", workflow_value())
            store.transition_workflow("running", expected_state="created")
            store.transition_task("a", "ready", expected_state="pending")
            store.transition_task("a", "running", expected_state="ready", fields={"pid": 123})
            self.assertEqual(reconcile(store, is_alive=lambda pid: True), [])
            self.assertEqual(store.snapshot["tasks"]["a"]["state"], "running")
            store.close()


if __name__ == "__main__":
    unittest.main()

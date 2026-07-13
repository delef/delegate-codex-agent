from pathlib import Path
import tempfile
import unittest

from orchestrator_agent.schema import normalize_workflow
from orchestrator_agent.scheduler import DurableScheduler
from orchestrator_agent.store import JournalStateStore


def workflow_value():
    return normalize_workflow({
        "version": 1, "name": "scheduler", "cwd": ".",
        "budget": {"total_tokens": 100, "max_workers": 2},
        "nodes": [
            {"id": "a", "kind": "agent", "spec": "a.json"},
            {"id": "b", "kind": "agent", "spec": "b.json", "depends_on": ["a"]},
        ],
    })


class SchedulerTests(unittest.TestCase):
    def test_only_accepted_dependencies_unblock_children(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JournalStateStore.create(Path(tmp) / "run", workflow_value())
            scheduler = DurableScheduler(store)
            self.assertEqual(scheduler.refresh_ready(), ["a"])
            scheduler.start("a", reserve_tokens=1)
            scheduler.complete("a")
            scheduler.reject("a", reason="check failed")
            self.assertEqual(scheduler.refresh_ready(), [])
            self.assertEqual(store.snapshot["tasks"]["b"]["state"], "blocked")
            store.close()

    def test_accepted_dependency_unblocks_child(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JournalStateStore.create(Path(tmp) / "run", workflow_value())
            scheduler = DurableScheduler(store)
            scheduler.refresh_ready()
            scheduler.start("a", reserve_tokens=1)
            scheduler.complete("a")
            scheduler.accept("a")
            self.assertEqual(scheduler.refresh_ready(), ["b"])
            store.close()

    def test_budget_exhaustion_blocks_dependents_and_releases_reservation(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JournalStateStore.create(Path(tmp) / "run", workflow_value())
            scheduler = DurableScheduler(store)
            scheduler.refresh_ready()
            scheduler.start("a", reserve_tokens=4)
            scheduler.budget_exhausted("a", observed_tokens=8)
            self.assertEqual(store.snapshot["tasks"]["a"]["state"], "budget_exhausted")
            self.assertEqual(store.snapshot["budget"]["reserved_tokens"], 0)
            scheduler.refresh_ready()
            self.assertEqual(store.snapshot["tasks"]["b"]["state"], "blocked")
            store.close()

    def test_manual_retry_requeues_terminal_task_within_attempt_limit(self):
        value = workflow_value()
        value["nodes"][0]["retry"] = {"max_attempts": 2}
        with tempfile.TemporaryDirectory() as tmp:
            store = JournalStateStore.create(Path(tmp) / "run", value)
            scheduler = DurableScheduler(store)
            scheduler.refresh_ready()
            scheduler.start("a", reserve_tokens=1)
            scheduler.complete("a")
            scheduler.reject("a", reason="failed")
            scheduler.manual_retry("a", reason="operator")
            self.assertEqual(store.snapshot["tasks"]["a"]["state"], "ready")
            self.assertEqual(store.snapshot["tasks"]["a"]["retry_count"], 1)
            store.close()


if __name__ == "__main__":
    unittest.main()

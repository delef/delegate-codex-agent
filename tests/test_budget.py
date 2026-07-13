from pathlib import Path
import tempfile
import unittest

from delegate_agent.budget import BudgetController
from delegate_agent.errors import StateError
from delegate_agent.schema import normalize_workflow
from delegate_agent.store import JournalStateStore


def workflow_value():
    return normalize_workflow({
        "version": 1, "name": "budget", "cwd": ".",
        "budget": {"total_tokens": 10, "max_workers": 1},
        "nodes": [{"id": "a", "kind": "agent", "spec": "a.json", "budget": {
            "reserve_tokens": 6, "hard_tokens": 7,
        }}],
    })


class BudgetTests(unittest.TestCase):
    def test_reservation_cannot_exceed_remaining_budget(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JournalStateStore.create(Path(tmp) / "run", workflow_value())
            budget = BudgetController(store)
            self.assertTrue(budget.reserve("a", 6).allowed)
            decision = budget.decide(5)
            with self.assertRaises(StateError):
                store.reserve_budget(5, task_id="a")
            self.assertFalse(decision.allowed)
            store.close()

    def test_usage_is_recorded_and_hard_limit_is_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JournalStateStore.create(Path(tmp) / "run", workflow_value())
            budget = BudgetController(store)
            store.transition_task("a", "ready", expected_state="pending")
            store.transition_task("a", "running", expected_state="ready")
            self.assertFalse(budget.record("a", {"input_tokens": 3, "output_tokens": 1}))
            self.assertTrue(budget.record("a", {"input_tokens": 3, "output_tokens": 1}))
            store.close()


if __name__ == "__main__":
    unittest.main()

from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

from orchestrator_agent.gates import GateResult
from orchestrator_agent.retry import classify_failure
from orchestrator_agent.schema import normalize_workflow
from orchestrator_agent.scheduler import DurableScheduler
from orchestrator_agent.store import JournalStateStore


def workflow_value(max_attempts=2):
    return normalize_workflow({
        "version": 1, "name": "retry", "cwd": ".",
        "budget": {"total_tokens": 100, "max_workers": 1},
        "nodes": [{"id": "a", "kind": "agent", "spec": "a.json",
                    "retry": {"max_attempts": max_attempts}}],
    })


class RetryTests(unittest.TestCase):
    def test_failure_class_and_fingerprint_are_deterministic(self):
        outcome = SimpleNamespace(exit_code=0, budget_exhausted=False)
        gates = [GateResult("command", "rejected", "verification_failed", {"exit_code": 1})]
        first = classify_failure(outcome=outcome, gates=gates, result={"result": "same"})
        second = classify_failure(outcome=outcome, gates=gates, result={"result": " same "})
        self.assertEqual(first.failure_class, "verification")
        self.assertEqual(first.fingerprint, second.fingerprint)

    def test_new_evidence_retries_once_and_same_fingerprint_stops(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JournalStateStore.create(Path(tmp) / "run", workflow_value())
            scheduler = DurableScheduler(store)
            scheduler.refresh_ready()
            scheduler.start("a", reserve_tokens=1)
            scheduler.complete("a")
            scheduler.reject("a", reason="verification_failed")
            self.assertTrue(scheduler.maybe_retry(
                "a", failure_class="verification", fingerprint="first",
                evidence={"result_sha256": "one"}, allowed_classes=["verification"],
            ))
            scheduler.start("a", reserve_tokens=1)
            scheduler.complete("a")
            scheduler.reject("a", reason="verification_failed")
            self.assertFalse(scheduler.maybe_retry(
                "a", failure_class="verification", fingerprint="first",
                evidence={"result_sha256": "one"}, allowed_classes=["verification"],
                previous_fingerprint="first",
            ))
            self.assertEqual(store.snapshot["tasks"]["a"]["state"], "rejected")
            self.assertEqual(store.snapshot["tasks"]["a"]["attempt"], 2)
            store.close()

    def test_budget_and_scope_failures_are_not_retryable_by_default(self):
        budget = classify_failure(
            outcome=SimpleNamespace(exit_code=-15, budget_exhausted=True), gates=[], result={},
        )
        scope = classify_failure(
            outcome=SimpleNamespace(exit_code=0, budget_exhausted=False),
            gates=[GateResult("diff_scope", "rejected", "scope_violation")], result={},
        )
        self.assertEqual(budget.failure_class, "budget")
        self.assertEqual(scope.failure_class, "scope")


if __name__ == "__main__":
    unittest.main()

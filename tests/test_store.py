import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from delegate_agent.errors import CorruptJournalError, StateError
from delegate_agent.schema import normalize_workflow
from delegate_agent.store import JournalStateStore, submit_control_request


def workflow_value():
    return normalize_workflow({
        "version": 1,
        "name": "store-demo",
        "cwd": ".",
        "budget": {"total_tokens": 100, "max_workers": 2},
        "nodes": [
            {"id": "inspect", "kind": "agent", "spec": "inspect.json", "budget": {
                "reserve_tokens": 10, "hard_tokens": 20,
            }},
            {"id": "check", "kind": "check", "depends_on": ["inspect"]},
        ],
    })


class JournalStateStoreTests(unittest.TestCase):
    def test_create_transition_usage_budget_and_replay(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "workflow"
            store = JournalStateStore.create(path, workflow_value())
            store.transition_workflow("running", expected_state="created")
            store.transition_task("inspect", "ready", expected_state="pending")
            store.reserve_budget(10, task_id="inspect", idempotency_key="reserve-1")
            store.transition_task("inspect", "running", expected_state="ready")
            store.record_usage({"input_tokens": 3, "output_tokens": 2}, task_id="inspect")
            store.transition_task("inspect", "completed", expected_state="running")
            store.transition_task("inspect", "verifying", expected_state="completed")
            store.transition_task("inspect", "accepted", expected_state="verifying")
            store.release_budget(10, task_id="inspect")
            before = store.snapshot
            store.close()

            reopened = JournalStateStore.open(path)
            after = reopened.snapshot
            reopened.close()

        self.assertEqual(after, before)
        self.assertEqual(after["state"], "running")
        self.assertEqual(after["tasks"]["inspect"]["state"], "accepted")
        self.assertEqual(after["usage"]["total_tokens"], 5)
        self.assertEqual(after["budget"]["reserved_tokens"], 0)

    def test_idempotency_returns_original_event_before_stale_precondition(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JournalStateStore.create(Path(tmp) / "workflow", workflow_value())
            first = store.transition_task(
                "inspect", "ready", expected_state="pending", idempotency_key="ready-1",
            )
            second = store.transition_task(
                "inspect", "ready", expected_state="pending", idempotency_key="ready-1",
            )
            store.close()
        self.assertEqual(first, second)

    def test_competing_runtime_locks_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "workflow"
            first = JournalStateStore.create(path, workflow_value())
            second = JournalStateStore.open(path)
            with self.assertRaisesRegex(StateError, "already running"):
                second.acquire()
            first.close()
            second.close()

    def test_replay_recovers_event_written_before_snapshot_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "workflow"
            store = JournalStateStore.create(path, workflow_value())
            original = __import__("delegate_agent.store", fromlist=["_atomic_json"])._atomic_json

            def fail_snapshot(snapshot_path, value):
                if Path(snapshot_path).name == "snapshot.json":
                    raise OSError("simulated snapshot failure")
                return original(snapshot_path, value)

            with mock.patch("delegate_agent.store._atomic_json", side_effect=fail_snapshot):
                with self.assertRaises(OSError):
                    store.transition_task("inspect", "ready", expected_state="pending")
            store.close()

            reopened = JournalStateStore.open(path)
            self.assertEqual(reopened.snapshot["tasks"]["inspect"]["state"], "ready")
            reopened.close()

    def test_incomplete_final_line_is_ignored_but_mid_journal_corruption_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "workflow"
            store = JournalStateStore.create(path, workflow_value())
            store.close()
            events = path / "state" / "events.jsonl"
            with events.open("ab") as handle:
                handle.write(b'{"seq":999')
            reopened = JournalStateStore.open(path)
            reopened.close()

            data = events.read_bytes()
            events.write_bytes(data + b"\nnot-json\n")
            with self.assertRaises(CorruptJournalError):
                JournalStateStore.open(path)

    def test_control_requests_are_archived_after_handler(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "workflow"
            store = JournalStateStore.create(path, workflow_value())
            request_path = submit_control_request(path, "pause", {"reason": "operator"}, request_id="req-1")
            seen = []
            store.consume_control_requests(lambda request: seen.append(request["type"]) or {"accepted": True})
            processed_path = path / "control" / "processed" / "req-1.json"
            self.assertTrue(processed_path.exists())
            events = (path / "state" / "events.jsonl").read_text(encoding="utf-8")
            store.close()

        self.assertEqual(seen, ["pause"])
        self.assertFalse(request_path.exists())
        self.assertIn("control.processed", events)

    def test_usage_totals_cannot_be_falsified(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JournalStateStore.create(Path(tmp) / "workflow", workflow_value())
            with self.assertRaisesRegex(StateError, "total_tokens"):
                store.record_usage({"input_tokens": 3, "output_tokens": 2, "total_tokens": 1})
            store.close()

    def test_dynamic_task_addition_replays_from_journal(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "workflow"
            store = JournalStateStore.create(path, workflow_value())
            store.add_task({
                "id": "map::one", "kind": "agent", "spec": "child.json",
                "model": "luna", "sandbox": "read-only", "isolation": "shared",
                "depends_on": ["inspect"], "budget": {"reserve_tokens": 1, "hard_tokens": 2},
                "retry": {"max_attempts": 1}, "checks": [], "map_parent": "map", "map_key": "one",
            })
            before = store.snapshot
            store.close()
            reopened = JournalStateStore.open(path)
            after = reopened.snapshot
            reopened.close()
        self.assertEqual(after, before)
        self.assertEqual(after["tasks"]["map::one"]["map_key"], "one")


if __name__ == "__main__":
    unittest.main()

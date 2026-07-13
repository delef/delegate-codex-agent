import json
from pathlib import Path
import tempfile
import unittest

from delegate_agent.cli import prepare_workflow


class PrepareWorkflowTests(unittest.TestCase):
    def spec(self, root: Path, name: str = "task") -> Path:
        path = root / f"{name}.json"
        path.write_text(json.dumps({"name": name, "objective": "inspect"}), encoding="utf-8")
        return path

    def test_preview_is_deterministic_and_does_not_start_workers(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = self.spec(root, "first")
            second = self.spec(root, "second")
            workflow = root / "workflow.json"
            workflow.write_text(json.dumps({
                "version": 1, "name": "preview", "cwd": str(root),
                "budget": {"total_tokens": 20, "max_workers": 2},
                "nodes": [
                    {"id": "a", "kind": "agent", "spec": str(first),
                     "budget": {"reserve_tokens": 3, "hard_tokens": 5},
                     "checks": [{"type": "result_schema"}]},
                    {"id": "b", "kind": "agent", "spec": str(second),
                     "depends_on": ["a"],
                     "budget": {"reserve_tokens": 2, "hard_tokens": 4},
                     "checks": [{"type": "result_schema"}]},
                ],
            }), encoding="utf-8")
            preview = prepare_workflow(workflow)
        self.assertTrue(preview["ready"])
        self.assertEqual(preview["phases"], [["a"], ["b"]])
        self.assertEqual(preview["estimates"]["reserve_tokens"], 5)
        self.assertEqual(preview["estimates"]["expanded_tasks"], 2)
        self.assertTrue(preview["preview_only"])

    def test_preview_rejects_reservations_over_workflow_budget(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = self.spec(root)
            workflow = root / "workflow.json"
            workflow.write_text(json.dumps({
                "version": 1, "name": "over", "cwd": str(root),
                "budget": {"total_tokens": 5, "max_workers": 1},
                "nodes": [{"id": "a", "kind": "agent", "spec": str(spec),
                            "budget": {"reserve_tokens": 6, "hard_tokens": 6}}],
            }), encoding="utf-8")
            preview = prepare_workflow(workflow)
        self.assertFalse(preview["ready"])
        self.assertEqual(preview["errors"][0]["type"], "reservation_over_budget")


if __name__ == "__main__":
    unittest.main()

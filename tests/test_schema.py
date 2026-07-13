import json
from pathlib import Path
import tempfile
import unittest

from orchestrator_agent.errors import SchemaError
from orchestrator_agent.models import TaskState, WorkflowState, can_transition_task, can_transition_workflow
from orchestrator_agent.schema import load_workflow, normalize_workflow


def workflow_value(**overrides):
    value = {
        "version": 1,
        "name": "demo",
        "cwd": ".",
        "budget": {"total_tokens": 10_000, "max_workers": 2},
        "nodes": [{
            "id": "inspect",
            "kind": "agent",
            "spec": "task.json",
            "checks": [{"type": "result_schema"}],
        }],
    }
    value.update(overrides)
    return value


class WorkflowSchemaTests(unittest.TestCase):
    def test_normalizes_relative_paths_and_defaults(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "workflow.json"
            normalized = normalize_workflow(workflow_value(), source=source)

        self.assertEqual(normalized["version"], 1)
        self.assertEqual(normalized["budget"]["max_terra_tasks"], 0)
        self.assertEqual(normalized["nodes"][0]["model"], "luna")
        self.assertEqual(normalized["nodes"][0]["sandbox"], "read-only")
        self.assertEqual(normalized["nodes"][0]["budget"]["reserve_tokens"], 1)
        self.assertTrue(Path(normalized["nodes"][0]["spec"]).is_absolute())

    def test_loads_json_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "workflow.json"
            path.write_text(json.dumps(workflow_value()), encoding="utf-8")
            loaded = load_workflow(path)
        self.assertEqual(loaded["name"], "demo")

    def test_rejects_version_cycle_duplicate_and_missing_dependency(self):
        cases = [
            (workflow_value(version=2), "version"),
            (workflow_value(nodes=[
                {"id": "a", "spec": "a.json", "depends_on": ["b"]},
                {"id": "b", "spec": "b.json", "depends_on": ["a"]},
            ]), "cycle"),
            (workflow_value(nodes=[
                {"id": "a", "spec": "a.json"}, {"id": "a", "spec": "b.json"},
            ]), "duplicate"),
            (workflow_value(nodes=[
                {"id": "a", "spec": "a.json", "depends_on": ["missing"]},
            ]), "missing"),
        ]
        for value, message in cases:
            with self.subTest(message=message), self.assertRaisesRegex(SchemaError, message):
                normalize_workflow(value)

    def test_rejects_unsafe_models_and_worktrees(self):
        cases = [
            ({"model": "terra"}, "model_reason"),
            ({"model": "sol", "model_reason": "compare", "sandbox": "workspace-write"}, "read-only"),
            ({"isolation": "worktree"}, "workspace-write"),
        ]
        for fields, message in cases:
            value = workflow_value(nodes=[{"id": "a", "spec": "a.json", **fields}])
            with self.subTest(message=message), self.assertRaisesRegex(SchemaError, message):
                normalize_workflow(value)

    def test_validates_command_checks_without_shell_strings(self):
        value = workflow_value(nodes=[{
            "id": "check",
            "kind": "check",
            "checks": [{
                "type": "command",
                "argv": ["python3", "-m", "unittest"],
                "timeout_seconds": 20,
                "inherit_env": ["PATH"],
            }],
        }])
        normalized = normalize_workflow(value)
        self.assertEqual(normalized["nodes"][0]["checks"][0]["argv"][0], "python3")

    def test_retry_policy_normalizes_and_rejects_unknown_failure_classes(self):
        value = workflow_value()
        value["nodes"][0]["retry"] = {"max_attempts": 2, "retry_on": ["verification", "transport"]}
        normalized = normalize_workflow(value)
        self.assertEqual(normalized["nodes"][0]["retry"]["retry_on"], ["verification", "transport"])
        value["nodes"][0]["retry"]["retry_on"] = ["model_magic"]
        with self.assertRaises(SchemaError):
            normalize_workflow(value)

    def test_escalation_requires_reason_and_only_allows_luna_to_terra(self):
        value = workflow_value()
        value["nodes"][0]["retry"] = {
            "max_attempts": 2, "retry_on": ["invalid_result"],
            "escalate_to": "terra", "escalate_on": ["invalid_result"],
        }
        with self.assertRaisesRegex(SchemaError, "escalation_reason"):
            normalize_workflow(value)
        value["nodes"][0]["retry"]["escalation_reason"] = "invalid result after schema gate"
        normalized = normalize_workflow(value)
        self.assertEqual(normalized["nodes"][0]["retry"]["escalate_to"], "terra")

    def test_condition_requires_deterministic_source_operator_and_branches(self):
        value = workflow_value()
        value["nodes"] = [
            {"id": "source", "kind": "agent", "spec": "source.json"},
            {"id": "decide", "kind": "condition", "depends_on": ["source"],
             "source": "source", "pointer": "/result", "operator": "equals", "value": "ok",
             "on_true": ["yes"], "on_false": ["no"]},
            {"id": "yes", "kind": "agent", "spec": "yes.json", "depends_on": ["decide"]},
            {"id": "no", "kind": "agent", "spec": "no.json", "depends_on": ["decide"]},
        ]
        normalized = normalize_workflow(value)
        self.assertEqual(normalized["nodes"][1]["operator"], "equals")
        value["nodes"][1]["operator"] = "eval"
        with self.assertRaises(SchemaError):
            normalize_workflow(value)

    def test_map_and_reduce_require_bounded_template_and_dependency(self):
        value = workflow_value()
        value["nodes"] = [
            {"id": "source", "kind": "agent", "spec": "source.json"},
            {"id": "map", "kind": "map", "depends_on": ["source"], "source": "source",
             "pointer": "/items", "item_key": "/id", "max_items": 3,
             "template": {"kind": "agent", "spec": "child.json"}, "reduce": "reduce"},
            {"id": "reduce", "kind": "reduce", "depends_on": ["map"], "source": "map"},
        ]
        normalized = normalize_workflow(value)
        self.assertEqual(normalized["nodes"][1]["max_items"], 3)
        self.assertEqual(normalized["nodes"][1]["template"]["model"], "luna")
        value["nodes"][1]["max_items"] = 0
        with self.assertRaises(SchemaError):
            normalize_workflow(value)

    def test_repeat_until_requires_iteration_cap_condition_and_template(self):
        value = workflow_value()
        value["nodes"] = [{
            "id": "repeat", "kind": "repeat_until", "max_iterations": 3,
            "template": {"kind": "agent", "spec": "child.json"},
            "condition": {"pointer": "/result", "operator": "equals", "value": "done"},
        }]
        normalized = normalize_workflow(value)
        self.assertEqual(normalized["nodes"][0]["max_iterations"], 3)
        value["nodes"][0]["max_iterations"] = 0
        with self.assertRaises(SchemaError):
            normalize_workflow(value)

        unsafe = workflow_value(nodes=[{
            "id": "check", "kind": "check",
            "checks": [{"type": "command", "argv": "python3 -m unittest"}],
        }])
        with self.assertRaisesRegex(SchemaError, "argv"):
            normalize_workflow(unsafe)


class TransitionTests(unittest.TestCase):
    def test_task_transition_contract(self):
        self.assertTrue(can_transition_task(TaskState.PENDING, TaskState.READY))
        self.assertFalse(can_transition_task(TaskState.PENDING, TaskState.ACCEPTED))
        self.assertTrue(can_transition_task(TaskState.VERIFYING, TaskState.REJECTED))
        self.assertFalse(can_transition_task(TaskState.ACCEPTED, TaskState.RUNNING))

    def test_workflow_transition_contract(self):
        self.assertTrue(can_transition_workflow(WorkflowState.CREATED, WorkflowState.RUNNING))
        self.assertFalse(can_transition_workflow(WorkflowState.SUCCEEDED, WorkflowState.RUNNING))


if __name__ == "__main__":
    unittest.main()

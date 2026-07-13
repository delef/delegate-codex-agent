import json
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest

from orchestrator_agent.schema import normalize_workflow
from orchestrator_agent.store import JournalStateStore, submit_control_request
from orchestrator_agent.conditions import evaluate_condition
from orchestrator_agent.workflow import WorkflowRuntime, build_task_prompt


FAKE_CODEX = """#!/usr/bin/env python3
import json
import pathlib
import sys
args = sys.argv[1:]
out = pathlib.Path(args[args.index('-o') + 1])
print(json.dumps({'type': 'thread.started', 'thread_id': 'workflow'}), flush=True)
print(json.dumps({'type': 'turn.completed', 'usage': {'input_tokens': 2, 'cached_input_tokens': 1, 'output_tokens': 2, 'reasoning_output_tokens': 0}}), flush=True)
out.write_text(json.dumps({'result': 'done', 'evidence': 'fake:1', 'changes': 'none', 'verification': 'ok', 'risks': 'none', 'recommended_next_action': 'stop'}), encoding='utf-8')
"""


def retry_codex_script(counter: Path, *, always_invalid: bool = False) -> str:
    return f'''#!/usr/bin/env python3
import json
from pathlib import Path
import sys
counter = Path({str(counter)!r})
count = int(counter.read_text()) if counter.exists() else 0
counter.write_text(str(count + 1))
out = Path(sys.argv[sys.argv.index('-o') + 1])
print(json.dumps({{"type": "turn.completed", "usage": {{"input_tokens": 1, "output_tokens": 1}}}}), flush=True)
if {always_invalid!r} or count == 0:
    out.write_text(json.dumps({{"result": "bad"}}), encoding="utf-8")
else:
    out.write_text(json.dumps({{"result": "done", "evidence": "fake:1", "changes": "none", "verification": "ok", "risks": "none", "recommended_next_action": "stop"}}), encoding="utf-8")
'''


class WorkflowTests(unittest.TestCase):
    def test_prompt_contains_objective_and_dependency_results(self):
        with tempfile.TemporaryDirectory() as tmp:
            spec = Path(tmp) / "task.json"
            spec.write_text(json.dumps({"name": "task", "objective": "inspect", "scope": ["src"]}), encoding="utf-8")
            prompt = build_task_prompt(spec, dependency_results={"previous": {"result": "ok"}})
        self.assertIn("inspect", prompt)
        self.assertIn("previous", prompt)

    def test_static_agent_workflow_runs_to_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo"
            repo.mkdir()
            spec = root / "task.json"
            spec.write_text(json.dumps({"name": "task", "objective": "run", "scope": []}), encoding="utf-8")
            binary = root / "codex"
            binary.write_text(FAKE_CODEX, encoding="utf-8")
            binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
            workflow = normalize_workflow({
                "version": 1, "name": "demo", "cwd": str(repo),
                "budget": {"total_tokens": 100, "max_workers": 1},
                "nodes": [{
                    "id": "task", "kind": "agent", "spec": str(spec),
                    "checks": [{"type": "result_schema"}],
                }],
            })
            runtime = WorkflowRuntime(root / "run", workflow)
            import os
            old_path = os.environ.get("PATH", "")
            os.environ["PATH"] = f"{root}{os.pathsep}{old_path}"
            try:
                status = runtime.run()
            finally:
                os.environ["PATH"] = old_path
                runtime.close()
        self.assertEqual(status["state"], "succeeded")
        self.assertEqual(status["counts"], {"accepted": 1})

    def test_check_node_runs_independent_verification_without_worker(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo"
            repo.mkdir()
            workflow = normalize_workflow({
                "version": 1, "name": "check-demo", "cwd": str(repo),
                "budget": {"total_tokens": 100, "max_workers": 1},
                "nodes": [{"id": "check", "kind": "check", "checks": [
                    {"type": "command", "argv": ["python3", "-c", "raise SystemExit(0)"]},
                ]}],
            })
            runtime = WorkflowRuntime(root / "run", workflow)
            status = runtime.run()
            runtime.close()
        self.assertEqual(status["state"], "succeeded")
        self.assertEqual(status["counts"], {"accepted": 1})

    def test_cancel_control_is_applied_before_scheduling(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo"
            repo.mkdir()
            spec = root / "task.json"
            spec.write_text(json.dumps({"name": "task", "objective": "run"}), encoding="utf-8")
            workflow = normalize_workflow({
                "version": 1, "name": "cancel-demo", "cwd": str(repo),
                "budget": {"total_tokens": 100, "max_workers": 1},
                "nodes": [{"id": "task", "kind": "agent", "spec": str(spec)}],
            })
            run_dir = root / "run"
            submit_control_request(run_dir, "cancel", {"reason": "test"}, request_id="cancel-1")
            runtime = WorkflowRuntime(run_dir, workflow)
            status = runtime.run()
            runtime.close()
            archived = (run_dir / "control" / "processed" / "cancel-1.json").exists()
        self.assertEqual(status["state"], "cancelled")
        self.assertEqual(status["counts"], {"cancelled": 1})
        self.assertTrue(archived)

    def test_approval_control_gates_dependents_and_reject_blocks_them(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo"
            repo.mkdir()
            spec = root / "task.json"
            spec.write_text(json.dumps({"name": "task", "objective": "run"}), encoding="utf-8")
            binary = root / "codex"
            binary.write_text(FAKE_CODEX, encoding="utf-8")
            binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
            workflow = normalize_workflow({
                "version": 1, "name": "approval-demo", "cwd": str(repo),
                "budget": {"total_tokens": 100, "max_workers": 1},
                "nodes": [
                    {"id": "gate", "kind": "approval"},
                    {"id": "task", "kind": "agent", "spec": str(spec), "depends_on": ["gate"],
                     "checks": [{"type": "result_schema"}]},
                ],
            })
            run_dir = root / "run"
            submit_control_request(run_dir, "approve", {"task_id": "gate"}, request_id="approve-1")
            runtime = WorkflowRuntime(run_dir, workflow)
            import os
            old_path = os.environ.get("PATH", "")
            os.environ["PATH"] = f"{root}{os.pathsep}{old_path}"
            try:
                status = runtime.run()
            finally:
                os.environ["PATH"] = old_path
                runtime.close()
            store = JournalStateStore.open(run_dir)
            gate = store.snapshot["tasks"]["gate"]
            task = store.snapshot["tasks"]["task"]
            store.close()
        self.assertEqual(status["state"], "succeeded")
        self.assertEqual(gate["approval_status"], "approved")
        self.assertEqual(task["state"], "accepted")

    def test_rejected_approval_does_not_run_dependents(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo"
            repo.mkdir()
            spec = root / "task.json"
            spec.write_text(json.dumps({"name": "task", "objective": "run"}), encoding="utf-8")
            workflow = normalize_workflow({
                "version": 1, "name": "reject-demo", "cwd": str(repo),
                "budget": {"total_tokens": 100, "max_workers": 1},
                "nodes": [
                    {"id": "gate", "kind": "approval"},
                    {"id": "task", "kind": "agent", "spec": str(spec), "depends_on": ["gate"]},
                ],
            })
            run_dir = root / "run"
            submit_control_request(run_dir, "reject", {"task_id": "gate", "reason": "not ready"}, request_id="reject-1")
            runtime = WorkflowRuntime(run_dir, workflow)
            status = runtime.run()
            runtime.close()
            store = JournalStateStore.open(run_dir)
            states = {task_id: task["state"] for task_id, task in store.snapshot["tasks"].items()}
            store.close()
        self.assertEqual(status["state"], "failed")
        self.assertEqual(states, {"gate": "blocked", "task": "blocked"})

    def test_hard_budget_ends_workflow_without_unblocking_dependents(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo"
            repo.mkdir()
            spec = root / "task.json"
            spec.write_text(json.dumps({"name": "task", "objective": "run"}), encoding="utf-8")
            binary = root / "codex"
            binary.write_text(FAKE_CODEX, encoding="utf-8")
            binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
            workflow = normalize_workflow({
                "version": 1, "name": "budget-demo", "cwd": str(repo),
                "budget": {"total_tokens": 100, "max_workers": 1},
                "nodes": [
                    {"id": "task", "kind": "agent", "spec": str(spec),
                     "budget": {"reserve_tokens": 2, "hard_tokens": 3}},
                    {"id": "child", "kind": "agent", "spec": str(spec), "depends_on": ["task"]},
                ],
            })
            runtime = WorkflowRuntime(root / "run", workflow)
            import os
            old_path = os.environ.get("PATH", "")
            os.environ["PATH"] = f"{root}{os.pathsep}{old_path}"
            try:
                status = runtime.run()
            finally:
                os.environ["PATH"] = old_path
                runtime.close()
        self.assertEqual(status["state"], "budget_exhausted")
        self.assertEqual(status["counts"], {"budget_exhausted": 1, "blocked": 1})

    def test_invalid_result_retries_with_new_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo"
            repo.mkdir()
            spec = root / "task.json"
            spec.write_text(json.dumps({"name": "task", "objective": "run"}), encoding="utf-8")
            counter = root / "counter"
            binary = root / "codex"
            binary.write_text(retry_codex_script(counter), encoding="utf-8")
            binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
            workflow = normalize_workflow({
                "version": 1, "name": "retry-demo", "cwd": str(repo),
                "budget": {"total_tokens": 100, "max_workers": 1},
                "nodes": [{"id": "task", "kind": "agent", "spec": str(spec),
                           "retry": {"max_attempts": 2}, "checks": [{"type": "result_schema"}]}],
            })
            runtime = WorkflowRuntime(root / "run", workflow)
            import os
            old_path = os.environ.get("PATH", "")
            os.environ["PATH"] = f"{root}{os.pathsep}{old_path}"
            try:
                status = runtime.run()
            finally:
                os.environ["PATH"] = old_path
                runtime.close()
            task_dir = root / "run" / "tasks" / "task"
            persisted_store = JournalStateStore.open(root / "run")
            persisted_attempt = persisted_store.snapshot["tasks"]["task"]["attempt"]
            persisted_store.close()
            attempt_artifacts = [
                (task_dir / "attempt-1" / "result.json").is_file(),
                (task_dir / "attempt-2" / "result.json").is_file(),
            ]
        self.assertEqual(status["state"], "succeeded")
        self.assertEqual(persisted_attempt, 2)
        self.assertEqual(attempt_artifacts, [True, True])

    def test_retry_can_escalate_luna_to_terra_with_explicit_reason(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo"
            repo.mkdir()
            spec = root / "task.json"
            spec.write_text(json.dumps({"name": "task", "objective": "run"}), encoding="utf-8")
            counter = root / "counter"
            binary = root / "codex"
            binary.write_text(retry_codex_script(counter), encoding="utf-8")
            binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
            workflow = normalize_workflow({
                "version": 1, "name": "escalate-demo", "cwd": str(repo),
                "budget": {"total_tokens": 100, "max_workers": 1, "max_terra_tasks": 1},
                "nodes": [{"id": "task", "kind": "agent", "spec": str(spec),
                           "retry": {
                               "max_attempts": 2, "retry_on": ["invalid_result"],
                               "escalate_to": "terra", "escalate_on": ["invalid_result"],
                               "escalation_reason": "schema failure needs stronger repair",
                           }, "checks": [{"type": "result_schema"}]}],
            })
            runtime = WorkflowRuntime(root / "run", workflow)
            import os
            old_path = os.environ.get("PATH", "")
            os.environ["PATH"] = f"{root}{os.pathsep}{old_path}"
            try:
                status = runtime.run()
            finally:
                os.environ["PATH"] = old_path
                runtime.close()
            store = JournalStateStore.open(root / "run")
            task = store.snapshot["tasks"]["task"]
            store.close()
        self.assertEqual(status["state"], "succeeded")
        self.assertEqual(task["model"], "terra")
        self.assertEqual(task["escalated_from"], "luna")
        self.assertEqual(task["attempt"], 2)

    def test_identical_invalid_result_stops_before_max_attempts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo"
            repo.mkdir()
            spec = root / "task.json"
            spec.write_text(json.dumps({"name": "task", "objective": "run"}), encoding="utf-8")
            counter = root / "counter"
            binary = root / "codex"
            binary.write_text(retry_codex_script(counter, always_invalid=True), encoding="utf-8")
            binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
            workflow = normalize_workflow({
                "version": 1, "name": "retry-stop", "cwd": str(repo),
                "budget": {"total_tokens": 100, "max_workers": 1},
                "nodes": [{"id": "task", "kind": "agent", "spec": str(spec),
                           "retry": {"max_attempts": 4}, "checks": [{"type": "result_schema"}]}],
            })
            runtime = WorkflowRuntime(root / "run", workflow)
            import os
            old_path = os.environ.get("PATH", "")
            os.environ["PATH"] = f"{root}{os.pathsep}{old_path}"
            try:
                status = runtime.run()
            finally:
                os.environ["PATH"] = old_path
                runtime.close()
            reopened = JournalStateStore.open(root / "run")
            task_state = reopened.snapshot["tasks"]["task"]
            reopened.close()
        self.assertEqual(status["state"], "failed")
        self.assertEqual(task_state["state"], "rejected")
        self.assertEqual(task_state["attempt"], 2)

    def test_condition_selects_branch_and_blocks_the_other(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo"
            repo.mkdir()
            spec = root / "task.json"
            spec.write_text(json.dumps({"name": "task", "objective": "run"}), encoding="utf-8")
            binary = root / "codex"
            binary.write_text(FAKE_CODEX, encoding="utf-8")
            binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
            workflow = normalize_workflow({
                "version": 1, "name": "condition-demo", "cwd": str(repo),
                "budget": {"total_tokens": 100, "max_workers": 1},
                "cache": {"enabled": False},
                "nodes": [
                    {"id": "discover", "kind": "agent", "spec": str(spec),
                     "checks": [{"type": "result_schema"}]},
                    {"id": "decide", "kind": "condition", "depends_on": ["discover"],
                     "source": "discover", "pointer": "/result", "operator": "equals", "value": "done",
                     "on_true": ["yes"], "on_false": ["no"]},
                    {"id": "yes", "kind": "agent", "spec": str(spec), "depends_on": ["decide"],
                     "checks": [{"type": "result_schema"}]},
                    {"id": "no", "kind": "agent", "spec": str(spec), "depends_on": ["decide"],
                     "checks": [{"type": "result_schema"}]},
                ],
            })
            runtime = WorkflowRuntime(root / "run", workflow)
            import os
            old_path = os.environ.get("PATH", "")
            os.environ["PATH"] = f"{root}{os.pathsep}{old_path}"
            try:
                status = runtime.run()
            finally:
                os.environ["PATH"] = old_path
                runtime.close()
        self.assertEqual(status["state"], "succeeded")
        self.assertEqual(status["counts"], {"accepted": 3, "blocked": 1})

    def test_condition_rejects_wrong_numeric_type(self):
        with self.assertRaisesRegex(Exception, "numeric"):
            evaluate_condition({
                "source": "source", "pointer": "/value", "operator": "greater_than", "value": 2,
            }, {"source": {"value": "2"}})

    def test_bounded_map_expands_children_and_reduce_waits_for_all(self):
        map_codex = """#!/usr/bin/env python3
import json
from pathlib import Path
import sys
args = sys.argv
out = Path(args[args.index('-o') + 1])
prompt = sys.stdin.read()
if '## Map item' in prompt:
    result = 'child'
else:
    result = {'items': [{'id': 'a'}, {'id': 'b'}, {'id': 'c'}]}
print(json.dumps({'type': 'turn.completed', 'usage': {'input_tokens': 1, 'output_tokens': 1}}), flush=True)
out.write_text(json.dumps({'result': result, 'evidence': 'fake:1', 'changes': 'none', 'verification': 'ok', 'risks': 'none', 'recommended_next_action': 'stop'}), encoding='utf-8')
"""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo"
            repo.mkdir()
            spec = root / "task.json"
            spec.write_text(json.dumps({"name": "task", "objective": "run"}), encoding="utf-8")
            binary = root / "codex"
            binary.write_text(map_codex, encoding="utf-8")
            binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
            workflow = normalize_workflow({
                "version": 1, "name": "map-demo", "cwd": str(repo),
                "budget": {"total_tokens": 100, "max_workers": 2}, "cache": {"enabled": False},
                "nodes": [
                    {"id": "source", "kind": "agent", "spec": str(spec),
                     "checks": [{"type": "result_schema"}]},
                    {"id": "map", "kind": "map", "depends_on": ["source"], "source": "source",
                     "pointer": "/result/items", "item_key": "/id", "max_items": 3,
                     "template": {"kind": "agent", "spec": str(spec),
                                  "checks": [{"type": "result_schema"}]}, "reduce": "reduce"},
                    {"id": "reduce", "kind": "reduce", "depends_on": ["map"], "source": "map"},
                ],
            })
            runtime = WorkflowRuntime(root / "run", workflow)
            import os
            old_path = os.environ.get("PATH", "")
            os.environ["PATH"] = f"{root}{os.pathsep}{old_path}"
            try:
                status = runtime.run()
            finally:
                os.environ["PATH"] = old_path
                runtime.close()
        self.assertEqual(status["state"], "succeeded")
        self.assertEqual(status["counts"], {"accepted": 6})

    def test_repeat_until_stops_on_condition_and_persists_iterations(self):
        repeat_codex = """#!/usr/bin/env python3
import json
from pathlib import Path
import sys
out = Path(sys.argv[sys.argv.index('-o') + 1])
prompt = sys.stdin.read()
result = 'done' if '## Repeat iteration\\n\\n2' in prompt else 'continue'
print(json.dumps({'type': 'turn.completed', 'usage': {'input_tokens': 1, 'output_tokens': 1}}), flush=True)
out.write_text(json.dumps({'result': result, 'evidence': 'fake:1', 'changes': 'none', 'verification': 'ok', 'risks': 'none', 'recommended_next_action': 'stop'}), encoding='utf-8')
"""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo"
            repo.mkdir()
            spec = root / "task.json"
            spec.write_text(json.dumps({"name": "task", "objective": "run"}), encoding="utf-8")
            binary = root / "codex"
            binary.write_text(repeat_codex, encoding="utf-8")
            binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
            workflow = normalize_workflow({
                "version": 1, "name": "repeat-demo", "cwd": str(repo),
                "budget": {"total_tokens": 100, "max_workers": 1}, "cache": {"enabled": False},
                "nodes": [{
                    "id": "repeat", "kind": "repeat_until", "max_iterations": 3,
                    "template": {"kind": "agent", "spec": str(spec), "checks": [{"type": "result_schema"}]},
                    "condition": {"pointer": "/result", "operator": "equals", "value": "done"},
                }],
            })
            runtime = WorkflowRuntime(root / "run", workflow)
            import os
            old_path = os.environ.get("PATH", "")
            os.environ["PATH"] = f"{root}{os.pathsep}{old_path}"
            try:
                status = runtime.run()
            finally:
                os.environ["PATH"] = old_path
                runtime.close()
            store = JournalStateStore.open(root / "run")
            task_ids = sorted(task_id for task_id in store.snapshot["tasks"] if task_id.startswith("repeat::"))
            store.close()
        self.assertEqual(status["state"], "succeeded")
        self.assertEqual(status["counts"], {"accepted": 3})
        self.assertEqual(task_ids, ["repeat::1", "repeat::2"])

    def test_writer_uses_isolated_worktree_and_captures_patch(self):
        writer_codex = """#!/usr/bin/env python3
import json
from pathlib import Path
import sys
Path('generated.txt').write_text('writer\\n', encoding='utf-8')
out = Path(sys.argv[sys.argv.index('-o') + 1])
print(json.dumps({'type': 'turn.completed', 'usage': {'input_tokens': 1, 'output_tokens': 1}}), flush=True)
out.write_text(json.dumps({'result': 'done', 'evidence': 'generated.txt', 'changes': 'generated.txt', 'verification': 'ok', 'risks': 'none', 'recommended_next_action': 'review'}), encoding='utf-8')
"""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo"
            repo.mkdir()
            subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
            subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
            subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)
            (repo / "README.md").write_text("base\n", encoding="utf-8")
            subprocess.run(["git", "add", "README.md"], cwd=repo, check=True)
            subprocess.run(["git", "commit", "-qm", "base"], cwd=repo, check=True)
            spec = root / "task.json"
            spec.write_text(json.dumps({"name": "writer", "objective": "write"}), encoding="utf-8")
            binary = root / "codex"
            binary.write_text(writer_codex, encoding="utf-8")
            binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
            workflow = normalize_workflow({
                "version": 1, "name": "writer-demo", "cwd": str(repo),
                "budget": {"total_tokens": 100, "max_workers": 1}, "cache": {"enabled": False},
                "nodes": [{"id": "writer", "kind": "agent", "spec": str(spec),
                           "sandbox": "workspace-write", "isolation": "worktree",
                           "checks": [{"type": "result_schema"}, {"type": "diff_scope", "paths": ["generated.txt"]}]}],
            })
            runtime = WorkflowRuntime(root / "run", workflow)
            import os
            old_path = os.environ.get("PATH", "")
            os.environ["PATH"] = f"{root}{os.pathsep}{old_path}"
            try:
                status = runtime.run()
            finally:
                os.environ["PATH"] = old_path
                runtime.close()
            store = JournalStateStore.open(root / "run")
            task = store.snapshot["tasks"]["writer"]
            worktree_exists = Path(task["worktree"]).is_dir()
            patch_exists = Path(task["patch_path"]).is_file()
            patch_text = Path(task["patch_path"]).read_text(encoding="utf-8")
            store.close()
        self.assertEqual(status["state"], "succeeded")
        self.assertTrue(worktree_exists)
        self.assertTrue(patch_exists)
        self.assertIn("generated.txt", patch_text)


if __name__ == "__main__":
    unittest.main()

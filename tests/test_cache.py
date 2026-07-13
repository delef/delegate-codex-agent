import json
from pathlib import Path
import stat
import tempfile
import unittest

from orchestrator_agent.cache import ResultCache, task_fingerprint
from orchestrator_agent.errors import StateError
from orchestrator_agent.schema import normalize_workflow
from orchestrator_agent.store import JournalStateStore
from orchestrator_agent.workflow import WorkflowRuntime


class CacheTests(unittest.TestCase):
    def test_fingerprint_changes_with_spec_and_dependencies(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = root / "task.json"
            spec.write_text('{"objective":"one"}', encoding="utf-8")
            node = {"kind": "agent", "spec": str(spec), "model": "luna",
                    "sandbox": "read-only", "isolation": "shared", "depends_on": [], "checks": []}
            first = task_fingerprint(node, cwd=root)
            second = task_fingerprint(node, cwd=root, dependency_results={"dep": {"result": "ok"}})
            explicit_model = task_fingerprint({**node, "model_id": "available-model"}, cwd=root)
            spec.write_text('{"objective":"two"}', encoding="utf-8")
            third = task_fingerprint(node, cwd=root)
        self.assertNotEqual(first, second)
        self.assertNotEqual(first, explicit_model)
        self.assertNotEqual(first, third)

    def test_cache_round_trip_and_tamper_detection(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = ResultCache(Path(tmp) / "cache")
            entry = cache.put(
                "a" * 64, result={"result": "ok"}, source_workflow="wf", source_task="task",
                sandbox="read-only",
            )
            self.assertEqual(cache.get(entry.fingerprint).result, {"result": "ok"})
            cache._result_path(entry.fingerprint).write_text("tampered", encoding="utf-8")
            self.assertIsNone(cache.get(entry.fingerprint))
            with self.assertRaises(StateError):
                cache.put("b" * 64, result={}, source_workflow="wf", source_task="writer", sandbox="workspace-write")

    def test_runtime_cache_hit_skips_second_worker_launch(self):
        fake = """#!/usr/bin/env python3
import json
from pathlib import Path
import sys
counter = Path(sys.argv[0] + '.count')
count = int(counter.read_text()) if counter.exists() else 0
counter.write_text(str(count + 1))
out = Path(sys.argv[sys.argv.index('-o') + 1])
print(json.dumps({'type': 'turn.completed', 'usage': {'input_tokens': 1, 'output_tokens': 1}}), flush=True)
out.write_text(json.dumps({'result': 'done', 'evidence': 'fake:1', 'changes': 'none', 'verification': 'ok', 'risks': 'none', 'recommended_next_action': 'stop'}), encoding='utf-8')
"""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo"
            repo.mkdir()
            spec = root / "task.json"
            spec.write_text(json.dumps({"name": "task", "objective": "inspect"}), encoding="utf-8")
            binary = root / "codex"
            binary.write_text(fake, encoding="utf-8")
            binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
            workflow = normalize_workflow({
                "version": 1, "name": "cache-demo", "cwd": str(repo),
                "budget": {"total_tokens": 100, "max_workers": 1},
                "nodes": [{"id": "task", "kind": "agent", "spec": str(spec),
                           "checks": [{"type": "result_schema"}]}],
            })
            import os
            old_path = os.environ.get("PATH", "")
            os.environ["PATH"] = f"{root}{os.pathsep}{old_path}"
            try:
                first_runtime = WorkflowRuntime(root / "run-1", workflow)
                self.assertEqual(first_runtime.run()["state"], "succeeded")
                first_runtime.close()
                second_runtime = WorkflowRuntime(root / "run-2", workflow)
                self.assertEqual(second_runtime.run()["state"], "succeeded")
                second_runtime.close()
            finally:
                os.environ["PATH"] = old_path
            count = int((Path(str(binary) + ".count")).read_text())
            store = JournalStateStore.open(root / "run-2")
            task = store.snapshot["tasks"]["task"]
            store.close()
        self.assertEqual(count, 1)
        self.assertTrue(task["cache_hit"])
        self.assertEqual(task["usage"]["total_tokens"], 0)


if __name__ == "__main__":
    unittest.main()

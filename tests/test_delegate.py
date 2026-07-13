import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "delegate.py"


def load_delegate():
    spec = importlib.util.spec_from_file_location("delegate", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class ManifestTests(unittest.TestCase):
    def setUp(self):
        self.delegate = load_delegate()

    def write_manifest(self, directory, value):
        path = Path(directory) / "manifest.json"
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    def test_manifest_defaults_each_task_to_luna_read_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest = self.write_manifest(tmp, {
                "tasks": [{"id": "inspect", "spec": "inspect.json"}],
            })
            tasks = self.delegate.load_manifest(manifest)

        self.assertEqual(tasks[0]["model"], "luna")
        self.assertEqual(tasks[0]["sandbox"], "read-only")
        self.assertEqual(tasks[0]["depends_on"], [])
        self.assertEqual(tasks[0]["isolation"], "shared")

    def test_manifest_rejects_dependency_cycles(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest = self.write_manifest(tmp, {"tasks": [
                {"id": "a", "spec": "a.json", "depends_on": ["b"]},
                {"id": "b", "spec": "b.json", "depends_on": ["a"]},
            ]})
            with self.assertRaisesRegex(self.delegate.SpecError, "cycle"):
                self.delegate.load_manifest(manifest)

    def test_manifest_rejects_duplicate_ids_and_missing_dependencies(self):
        cases = [
            ({"tasks": [{"id": "a", "spec": "a.json"}, {"id": "a", "spec": "b.json"}]}, "duplicate"),
            ({"tasks": [{"id": "a", "spec": "a.json", "depends_on": ["missing"]}]}, "missing"),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            for value, message in cases:
                manifest = self.write_manifest(tmp, value)
                with self.subTest(message=message), self.assertRaisesRegex(self.delegate.SpecError, message):
                    self.delegate.load_manifest(manifest)

    def test_worktree_isolation_requires_a_writer(self):
        with tempfile.TemporaryDirectory() as tmp:
            invalid = self.write_manifest(tmp, {"tasks": [
                {"id": "read", "spec": "read.json", "isolation": "worktree"},
            ]})
            with self.assertRaisesRegex(self.delegate.SpecError, "worktree.*workspace-write"):
                self.delegate.load_manifest(invalid)

            valid = self.write_manifest(tmp, {"tasks": [{
                "id": "write", "spec": "write.json", "sandbox": "workspace-write",
                "isolation": "worktree", "base_ref": "HEAD",
            }]})
            task = self.delegate.load_manifest(valid)[0]
            self.assertEqual(task["isolation"], "worktree")
            self.assertEqual(task["base_ref"], "HEAD")

    def test_batch_defaults_are_budget_conservative(self):
        args = self.delegate.parser().parse_args([
            "batch", "--manifest", "tasks.json", "--cwd", "/tmp/repo",
        ])
        self.assertEqual(args.max_workers, 2)
        self.assertEqual(args.max_dependency_chars, 2_000)
        self.assertEqual(args.max_terra_tasks, 1)
        self.assertTrue(hasattr(args, "max_sol_tasks"))
        self.assertEqual(args.max_sol_tasks, 0)
        self.assertIsNone(args.stop_after_total_tokens)

    def test_terra_requires_reason_and_respects_batch_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            missing_reason = self.write_manifest(tmp, {"tasks": [{
                "id": "expensive", "spec": "task.json", "model": "terra",
            }]})
            with self.assertRaisesRegex(self.delegate.SpecError, "Terra.*reason"):
                self.delegate.load_manifest(missing_reason)

            manifest = self.write_manifest(tmp, {"tasks": [
                {"id": "a", "spec": "a.json", "model": "terra", "model_reason": "migration"},
                {"id": "b", "spec": "b.json", "model": "terra", "model_reason": "debugging"},
            ]})
            tasks = self.delegate.load_manifest(manifest)
            with self.assertRaisesRegex(self.delegate.SpecError, "Terra task limit"):
                self.delegate.validate_model_budget(tasks, max_terra_tasks=1)
            self.delegate.validate_model_budget(tasks, max_terra_tasks=2)

    def test_sol_model_id(self):
        self.assertIn("sol", self.delegate.MODEL_IDS)
        self.assertEqual(self.delegate.MODEL_IDS["sol"], "gpt-5.6-sol")

    def test_sol_manifest_requires_reason(self):
        with tempfile.TemporaryDirectory() as tmp:
            missing_reason = self.write_manifest(tmp, {"tasks": [{
                "id": "think", "spec": "task.json", "model": "sol",
            }]})
            with self.assertRaisesRegex(self.delegate.SpecError, "Sol.*reason"):
                self.delegate.load_manifest(missing_reason)

    def test_sol_manifest_is_read_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            writable = self.write_manifest(tmp, {"tasks": [{
                "id": "think", "spec": "task.json", "model": "sol",
                "model_reason": "compare architecture tradeoffs",
                "sandbox": "workspace-write",
            }]})
            with self.assertRaisesRegex(self.delegate.SpecError, "Sol.*read-only"):
                self.delegate.load_manifest(writable)

    def test_sol_requires_explicit_batch_budget(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest = self.write_manifest(tmp, {"tasks": [{
                "id": "think", "spec": "task.json", "model": "sol",
                "model_reason": "compare architecture tradeoffs outside supervisor context",
            }]})
            tasks = self.delegate.load_manifest(manifest)
            with self.assertRaisesRegex(self.delegate.SpecError, "Sol task limit"):
                self.delegate.validate_model_budget(
                    tasks, max_terra_tasks=1, max_sol_tasks=0,
                )
            self.delegate.validate_model_budget(
                tasks, max_terra_tasks=1, max_sol_tasks=1,
            )

    def test_single_sol_run_accepts_an_explicit_reason(self):
        args = self.delegate.parser().parse_args([
            "run", "--spec", "task.json", "--cwd", "/tmp/repo",
            "--model", "sol", "--model-reason", "analyze competing designs",
        ])
        self.assertEqual(args.model, "sol")
        self.assertEqual(args.model_reason, "analyze competing designs")

    def test_single_terra_run_remains_compatible_without_reason(self):
        self.delegate.validate_model_use(
            "terra", None, "workspace-write", "delegate",
        )

    def test_sol_use_requires_reason_and_read_only(self):
        with self.assertRaisesRegex(self.delegate.SpecError, "Sol.*reason"):
            self.delegate.validate_model_use("sol", None, "read-only", "delegate")
        with self.assertRaisesRegex(self.delegate.SpecError, "Sol.*read-only"):
            self.delegate.validate_model_use(
                "sol", "analyze competing designs", "workspace-write", "delegate",
            )
        self.delegate.validate_model_use(
            "sol", "analyze competing designs", "read-only", "delegate",
        )


class ResultCompactionTests(unittest.TestCase):
    def setUp(self):
        self.delegate = load_delegate()

    def test_structures_full_result_but_bounds_dependency_context(self):
        raw = """## Result
Implemented compact handoff.
## Evidence
scripts/delegate.py: structured_result
## Changes
Added local parsing.
## Verification
EXPENSIVE_DETAIL """ + ("x" * 500) + """
## Risks
None.
## Recommended next action
Review the diff.
"""
        structured = self.delegate.structured_result(raw)
        compact = self.delegate.dependency_summary(structured, max_chars=180)

        self.assertEqual(structured["result"], "Implemented compact handoff.")
        self.assertIn("EXPENSIVE_DETAIL", structured["verification"])
        self.assertLessEqual(len(compact), 180)
        self.assertNotIn("EXPENSIVE_DETAIL", compact)
        self.assertIn("Result: Implemented", compact)


class UsageTests(unittest.TestCase):
    def setUp(self):
        self.delegate = load_delegate()

    def test_sums_turn_usage_without_double_counting_reasoning(self):
        with tempfile.TemporaryDirectory() as tmp:
            events = Path(tmp) / "events.jsonl"
            events.write_text("\n".join([
                json.dumps({"type": "thread.started", "thread_id": "x"}),
                json.dumps({"type": "turn.completed", "usage": {
                    "input_tokens": 100, "cached_input_tokens": 40,
                    "output_tokens": 20, "reasoning_output_tokens": 7,
                }}),
                json.dumps({"type": "turn.completed", "usage": {
                    "input_tokens": 30, "cached_input_tokens": 10,
                    "output_tokens": 5, "reasoning_output_tokens": 2,
                }}),
            ]) + "\n", encoding="utf-8")

            usage = self.delegate.usage_from_events(events)

        self.assertEqual(usage["input_tokens"], 130)
        self.assertEqual(usage["cached_input_tokens"], 50)
        self.assertEqual(usage["uncached_input_tokens"], 80)
        self.assertEqual(usage["output_tokens"], 25)
        self.assertEqual(usage["reasoning_output_tokens"], 9)
        self.assertEqual(usage["total_tokens"], 155)


class SkillContractTests(unittest.TestCase):
    def test_skill_is_compact_and_preserves_budget_safety_rules(self):
        content = (ROOT / "SKILL.md").read_text(encoding="utf-8")
        self.assertLessEqual(len(content.split()), 650)
        for required in (
            "Prefer one Luna", "model_reason", "workspace-write", "worktree",
            "result.json", "max-terra-tasks", "max-sol-tasks",
            "stop-after-total-tokens", "thinking delegate", "supervisor verification",
        ):
            with self.subTest(required=required):
                self.assertIn(required, content)


class BatchIntegrationTests(unittest.TestCase):
    def test_reads_overlap_but_writer_is_exclusive(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            repo = tmp / "repo"
            repo.mkdir()
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            bin_dir = tmp / "bin"
            bin_dir.mkdir()
            fake_codex = bin_dir / "codex"
            fake_codex.write_text(textwrap.dedent("""\
                #!/usr/bin/env python3
                import json, os, pathlib, sys, time
                args = sys.argv[1:]
                out = pathlib.Path(args[args.index('-o') + 1])
                packet = sys.stdin.read()
                name = next(line[6:] for line in packet.splitlines() if line.startswith('Name: '))
                log = pathlib.Path(os.environ['DELEGATE_TEST_LOG'])
                with log.open('a') as f:
                    f.write(f'{name} start {time.monotonic()}\\n')
                print(json.dumps({'type': 'thread.started', 'thread_id': name}), flush=True)
                print(json.dumps({'type': 'turn.completed', 'usage': {'input_tokens': 100, 'cached_input_tokens': 40, 'output_tokens': 20, 'reasoning_output_tokens': 5}}), flush=True)
                time.sleep(float(os.environ.get('DELEGATE_SLEEP', '0.25')))
                if not os.environ.get('DELEGATE_NO_RESULT'):
                    out.write_text('Result\\nOK\\n', encoding='utf-8')
                with log.open('a') as f:
                    f.write(f'{name} end {time.monotonic()}\\n')
            """), encoding="utf-8")
            fake_codex.chmod(0o755)

            tasks = []
            for name in ("read-a", "read-b", "write"):
                spec = tmp / f"{name}.json"
                spec.write_text(json.dumps({
                    "name": name, "objective": name, "scope": [], "context": [],
                    "constraints": [], "acceptance": [], "commands": [], "output": [],
                }), encoding="utf-8")
                tasks.append({
                    "id": name, "spec": str(spec),
                    "sandbox": "workspace-write" if name == "write" else "read-only",
                    "depends_on": ["read-a", "read-b"] if name == "write" else [],
                })
            manifest = tmp / "manifest.json"
            manifest.write_text(json.dumps({"tasks": tasks}), encoding="utf-8")
            log = tmp / "timeline.log"
            env = os.environ.copy()
            env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
            env["DELEGATE_CODEX_BIN"] = str(fake_codex)
            env["DELEGATE_TEST_LOG"] = str(log)

            completed = subprocess.run([
                sys.executable, str(SCRIPT), "batch", "--manifest", str(manifest),
                "--cwd", str(repo), "--max-workers", "3", "--runs-dir", str(tmp / "runs"),
            ], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)

            debug_events = ""
            if completed.returncode:
                batch_candidates = [
                    Path(line.removeprefix("BATCH_DIR=")) for line in completed.stdout.splitlines()
                    if line.startswith("BATCH_DIR=")
                ]
                if batch_candidates:
                    debug_events = "\n".join(
                        path.read_text(encoding="utf-8")
                        for path in batch_candidates[0].glob("runs/*/events.jsonl")
                    )
            self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout + debug_events)
            intervals = {}
            for line in log.read_text(encoding="utf-8").splitlines():
                name, event, raw_time = line.split()
                intervals.setdefault(name, {})[event] = float(raw_time)
            self.assertLess(intervals["read-a"]["start"], intervals["read-b"]["end"])
            self.assertLess(intervals["read-b"]["start"], intervals["read-a"]["end"])
            reads_end = max(intervals["read-a"]["end"], intervals["read-b"]["end"])
            self.assertGreaterEqual(intervals["write"]["start"], reads_end)
            batch_line = next(line for line in completed.stdout.splitlines() if line.startswith("BATCH_DIR="))
            batch_dir = Path(batch_line.removeprefix("BATCH_DIR="))
            summary = json.loads((batch_dir / "batch-status.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["status"], "succeeded")
            self.assertTrue(all(task["model"] == "gpt-5.6-luna" for task in summary["tasks"]))
            self.assertEqual(summary["usage"]["total_tokens"], 360)
            self.assertTrue(all(task["usage"]["total_tokens"] == 120 for task in summary["tasks"]))
            writer = next(task for task in summary["tasks"] if task["id"] == "write")
            writer_packet = (Path(writer["run_dir"]) / "packet.md").read_text(encoding="utf-8")
            self.assertIn("## Dependency results", writer_packet)
            self.assertIn("### read-a", writer_packet)
            self.assertIn("### read-b", writer_packet)

            cutoff = subprocess.run([
                sys.executable, str(SCRIPT), "batch", "--manifest", str(manifest),
                "--cwd", str(repo), "--max-workers", "2",
                "--stop-after-total-tokens", "100",
                "--runs-dir", str(tmp / "cutoff-runs"),
            ], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
            self.assertEqual(cutoff.returncode, 3, cutoff.stderr + cutoff.stdout)
            cutoff_dir = Path(next(
                line.removeprefix("BATCH_DIR=") for line in cutoff.stdout.splitlines()
                if line.startswith("BATCH_DIR=")
            ))
            cutoff_status = json.loads((cutoff_dir / "batch-status.json").read_text(encoding="utf-8"))
            self.assertEqual(cutoff_status["status"], "budget_exhausted")
            self.assertGreaterEqual(cutoff_status["usage"]["total_tokens"], 100)
            cutoff_writer = next(task for task in cutoff_status["tasks"] if task["id"] == "write")
            self.assertEqual(cutoff_writer["status"], "skipped")
            self.assertEqual(cutoff_writer["blocked_by"], ["token budget reached"])

            interrupt_env = env.copy()
            interrupt_env["DELEGATE_SLEEP"] = "10"
            started = time.monotonic()
            process = subprocess.Popen([
                sys.executable, str(SCRIPT), "batch", "--manifest", str(manifest),
                "--cwd", str(repo), "--max-workers", "3", "--runs-dir", str(tmp / "interrupt-runs"),
            ], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=interrupt_env)
            assert process.stdout is not None
            while True:
                line = process.stdout.readline()
                self.assertTrue(line, "batch exited before starting a delegate")
                if line.startswith("DELEGATE_STARTED"):
                    break
            process.send_signal(signal.SIGINT)
            process.communicate(timeout=5)
            self.assertEqual(process.returncode, 130)
            self.assertLess(time.monotonic() - started, 5)

            missing_env = env.copy()
            missing_env["DELEGATE_NO_RESULT"] = "1"
            missing = subprocess.run([
                sys.executable, str(SCRIPT), "run", "--spec", tasks[0]["spec"],
                "--cwd", str(repo), "--runs-dir", str(tmp / "missing-runs"),
            ], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=missing_env)
            self.assertEqual(missing.returncode, 2)
            missing_run = Path(next(
                line.removeprefix("RUN_DIR=") for line in missing.stdout.splitlines()
                if line.startswith("RUN_DIR=")
            ))
            missing_status = json.loads((missing_run / "status.json").read_text(encoding="utf-8"))
            self.assertEqual(missing_status["status"], "failed")

    def test_isolated_writers_run_in_parallel_worktrees(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            repo = tmp / "repo"
            repo.mkdir()
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            (repo / "base.txt").write_text("base\n", encoding="utf-8")
            subprocess.run(["git", "add", "base.txt"], cwd=repo, check=True)
            subprocess.run([
                "git", "-c", "user.name=Test", "-c", "user.email=test@example.com",
                "commit", "-qm", "base",
            ], cwd=repo, check=True)

            bin_dir = tmp / "bin"
            bin_dir.mkdir()
            fake_codex = bin_dir / "codex"
            fake_codex.write_text(textwrap.dedent("""\
                #!/usr/bin/env python3
                import json, os, pathlib, sys, time
                args = sys.argv[1:]
                out = pathlib.Path(args[args.index('-o') + 1])
                cwd = pathlib.Path(args[args.index('-C') + 1])
                packet = sys.stdin.read()
                name = next(line[6:] for line in packet.splitlines() if line.startswith('Name: '))
                log = pathlib.Path(os.environ['DELEGATE_TEST_LOG'])
                with log.open('a') as f: f.write(f'{name} start {time.monotonic()} {cwd}\\n')
                (cwd / f'{name}.txt').write_text(name, encoding='utf-8')
                time.sleep(0.25)
                out.write_text('## Result\\nDone.\\n## Risks\\nNone.\\n', encoding='utf-8')
                print(json.dumps({'type': 'thread.started', 'thread_id': name}), flush=True)
                print(json.dumps({'type': 'turn.completed', 'usage': {'input_tokens': 50, 'cached_input_tokens': 20, 'output_tokens': 10, 'reasoning_output_tokens': 3}}), flush=True)
                with log.open('a') as f: f.write(f'{name} end {time.monotonic()} {cwd}\\n')
            """), encoding="utf-8")
            fake_codex.chmod(0o755)

            tasks = []
            for name in ("writer-a", "writer-b"):
                spec = tmp / f"{name}.json"
                spec.write_text(json.dumps({
                    "name": name, "objective": name, "scope": [], "context": [],
                    "constraints": [], "acceptance": [], "commands": [], "output": [],
                }), encoding="utf-8")
                tasks.append({
                    "id": name, "spec": str(spec), "sandbox": "workspace-write",
                    "isolation": "worktree",
                })
            manifest = tmp / "manifest.json"
            manifest.write_text(json.dumps({"tasks": tasks}), encoding="utf-8")
            log = tmp / "timeline.log"
            env = os.environ.copy()
            env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
            env["DELEGATE_CODEX_BIN"] = str(fake_codex)
            env["DELEGATE_TEST_LOG"] = str(log)

            completed = subprocess.run([
                sys.executable, str(SCRIPT), "batch", "--manifest", str(manifest),
                "--cwd", str(repo), "--max-workers", "2", "--runs-dir", str(tmp / "runs"),
            ], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)

            batch_candidates = [
                Path(line.removeprefix("BATCH_DIR=")) for line in completed.stdout.splitlines()
                if line.startswith("BATCH_DIR=")
            ]
            debug_events = ""
            if completed.returncode and batch_candidates:
                debug_events = "\n".join(
                    path.read_text(encoding="utf-8")
                    for path in batch_candidates[0].glob("runs/*/events.jsonl")
                )
            self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout + debug_events)
            intervals = {}
            worktrees = {}
            for line in log.read_text(encoding="utf-8").splitlines():
                name, event, raw_time, cwd = line.split(maxsplit=3)
                intervals.setdefault(name, {})[event] = float(raw_time)
                worktrees[name] = cwd
            self.assertLess(intervals["writer-a"]["start"], intervals["writer-b"]["end"])
            self.assertLess(intervals["writer-b"]["start"], intervals["writer-a"]["end"])
            self.assertNotEqual(worktrees["writer-a"], worktrees["writer-b"])
            self.assertNotEqual(worktrees["writer-a"], str(repo))

            batch_dir = Path(next(
                line.removeprefix("BATCH_DIR=") for line in completed.stdout.splitlines()
                if line.startswith("BATCH_DIR=")
            ))
            status = json.loads((batch_dir / "batch-status.json").read_text(encoding="utf-8"))
            for task in status["tasks"]:
                self.assertEqual(task["status"], "succeeded")
                self.assertEqual(task["integration_status"], "ready")
                self.assertTrue(Path(task["worktree"]).is_dir())


if __name__ == "__main__":
    unittest.main()

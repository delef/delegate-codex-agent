from pathlib import Path
import subprocess
import tempfile
import unittest

from orchestrator_agent.errors import StateError
from orchestrator_agent.integration import apply_integration_plan, build_integration_plan, capture_writer_changes, plan_digest, write_integration_plan


def git(cwd: Path, *args: str) -> str:
    completed = subprocess.run(["git", *args], cwd=cwd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    return completed.stdout.strip()


class IntegrationTests(unittest.TestCase):
    def repo(self, root: Path) -> tuple[Path, str]:
        repo = root / "repo"
        repo.mkdir()
        git(repo, "init", "-q")
        git(repo, "config", "user.email", "test@example.com")
        git(repo, "config", "user.name", "Test")
        (repo / "README.md").write_text("base\n", encoding="utf-8")
        git(repo, "add", "README.md")
        git(repo, "commit", "-qm", "base")
        return repo, git(repo, "rev-parse", "HEAD")

    def worktree(self, repo: Path, root: Path, name: str) -> Path:
        path = root / name
        git(repo, "worktree", "add", "--detach", "-q", str(path), "HEAD")
        return path

    def test_clean_independent_writers_produce_non_mutating_plan(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo, base = self.repo(root)
            first = self.worktree(repo, root, "first")
            second = self.worktree(repo, root, "second")
            (first / "one.txt").write_text("one\n", encoding="utf-8")
            (second / "two.txt").write_text("two\n", encoding="utf-8")
            before = git(repo, "status", "--porcelain")
            plan = build_integration_plan(repo, [
                {"id": "a", "worktree": str(first), "base_sha": base, "scope": ["one.txt"]},
                {"id": "b", "worktree": str(second), "base_sha": base, "scope": ["two.txt"]},
            ])
            destination = write_integration_plan(plan, root / "integration-plan.json")
            destination_exists = destination.is_file()
            after = git(repo, "status", "--porcelain")
        self.assertTrue(plan["ready"])
        self.assertEqual(plan["order"], ["a", "b"])
        self.assertFalse(plan["mutated"])
        self.assertEqual(before, after)
        self.assertTrue(destination_exists)

    def test_overlapping_paths_are_conflicts_without_ordering(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo, base = self.repo(root)
            first = self.worktree(repo, root, "first")
            second = self.worktree(repo, root, "second")
            (first / "same.txt").write_text("one\n", encoding="utf-8")
            (second / "same.txt").write_text("two\n", encoding="utf-8")
            plan = build_integration_plan(repo, [
                {"id": "a", "worktree": str(first), "base_sha": base},
                {"id": "b", "worktree": str(second), "base_sha": base},
            ])
        self.assertFalse(plan["ready"])
        self.assertEqual(plan["conflicts"][0]["path"], "same.txt")

    def test_scope_and_symlink_changes_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo, base = self.repo(root)
            worktree = self.worktree(repo, root, "writer")
            (worktree / "outside.txt").write_text("outside\n", encoding="utf-8")
            plan = build_integration_plan(repo, [
                {"id": "writer", "worktree": str(worktree), "base_sha": base, "scope": ["allowed"]},
            ])
            self.assertFalse(plan["ready"])
            with self.assertRaises(StateError):
                (worktree / "link").symlink_to(root / "secret")
                capture_writer_changes(worktree, base_ref="HEAD")

    def test_capture_records_rename_and_deletion(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo, base = self.repo(root)
            writer = self.worktree(repo, root, "writer")
            git(writer, "mv", "README.md", "RENAMED.md")
            changes = capture_writer_changes(writer, base_ref=base)
        statuses = {item["path"]: item["status"] for item in changes["files"]}
        self.assertEqual(statuses["README.md"], "renamed_from")
        self.assertEqual(statuses["RENAMED.md"], "renamed")

    def test_approved_plan_applies_patch_and_rejects_stale_or_missing_approval(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo, base = self.repo(root)
            writer = self.worktree(repo, root, "writer")
            (writer / "README.md").write_text("changed\n", encoding="utf-8")
            patch_path = root / "writer.patch"
            patch_path.write_text(
                subprocess.run(["git", "diff", "--binary", base, "--"], cwd=writer, text=True, stdout=subprocess.PIPE, check=True).stdout,
                encoding="utf-8",
            )
            plan = build_integration_plan(repo, [{
                "id": "writer", "worktree": str(writer), "base_sha": base,
                "patch": str(patch_path),
            }], checks=[{"argv": ["python3", "-c", "raise SystemExit(0)"], "timeout_seconds": 10}])
            with self.assertRaises(StateError):
                apply_integration_plan(plan, approval="wrong")
            result = apply_integration_plan(plan, approval={"approved": True, "plan_sha256": plan_digest(plan)})
            self.assertEqual(result["applied"], ["writer"])
            self.assertEqual(result["verification"][0]["exit_code"], 0)
            self.assertEqual((repo / "README.md").read_text(encoding="utf-8"), "changed\n")


if __name__ == "__main__":
    unittest.main()

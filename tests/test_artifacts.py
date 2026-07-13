from pathlib import Path
import tempfile
import unittest

from delegate_agent.artifacts import build_manifest, write_manifest
from delegate_agent.errors import StateError


class ArtifactTests(unittest.TestCase):
    def test_manifest_contains_sorted_hashes_and_sizes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "b.txt").write_text("b", encoding="utf-8")
            (root / "a.txt").write_text("a", encoding="utf-8")
            manifest = build_manifest(root, [root / "b.txt", root / "a.txt", root / "a.txt"])
            destination = write_manifest(root, [root / "a.txt", root / "b.txt"])
        self.assertEqual([item["path"] for item in manifest["artifacts"]], ["a.txt", "b.txt"])
        self.assertTrue(destination.name == "artifact_manifest.json")
        self.assertEqual(manifest["artifacts"][0]["size"], 1)

    def test_manifest_rejects_escape_and_directories(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            outside = root.parent / "outside-artifact.txt"
            outside.write_text("x", encoding="utf-8")
            with self.assertRaises(StateError):
                build_manifest(root, [outside])
            with self.assertRaises(StateError):
                build_manifest(root, [root])
            outside.unlink()


if __name__ == "__main__":
    unittest.main()
